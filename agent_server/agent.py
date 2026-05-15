import json
import logging
import os
import re
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, AsyncGenerator

import mlflow
from agents import Agent, Runner, function_tool, set_default_openai_api, set_default_openai_client
from agents.tracing import set_trace_processors
from databricks_openai import AsyncDatabricksOpenAI
from mlflow.genai.agent_server import invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)
from neo4j import GraphDatabase
from neo4j.graph import Node, Path as Neo4jPath, Relationship
from neo4j.time import Date, DateTime, Duration, Time

from agent_server.utils import get_session_id, process_agent_stream_events

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "databricks-claude-sonnet-4-5"
DEFAULT_MAX_ROWS = 25
REQUIRED_NEO4J_ENV = ("NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD")
MUTATING_CYPHER = re.compile(
    r"\b("
    r"CREATE|MERGE|DELETE|DETACH\s+DELETE|SET|REMOVE|DROP|ALTER|RENAME|"
    r"LOAD\s+CSV|START\s+DATABASE|STOP\s+DATABASE|CALL\s+dbms\.|"
    r"CALL\s+apoc\.periodic|CALL\s+apoc\.load|CALL\s+apoc\.export"
    r")\b",
    re.IGNORECASE,
)
READ_ONLY_PREFIXES = ("MATCH", "RETURN", "WITH", "CALL", "SHOW", "EXPLAIN", "PROFILE", "UNWIND")

# Databricks-hosted models are exposed through the OpenAI-compatible client.
set_default_openai_client(AsyncDatabricksOpenAI())
set_default_openai_api("chat_completions")
set_trace_processors([])  # MLflow handles trace processing for the app.
mlflow.openai.autolog()
logging.getLogger("mlflow.utils.autologging_utils").setLevel(logging.ERROR)
logging.getLogger("neo4j").setLevel(logging.ERROR)


def _load_neo4j_env_from_file() -> None:
    """Load local Neo4j credentials from a user-provided file when env vars are absent."""
    credential_file = os.getenv("NEO4J_CREDENTIAL_FILE")
    if not credential_file:
        return

    path = Path(credential_file).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"NEO4J_CREDENTIAL_FILE does not exist: {path}")

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        if "=" not in raw_line or raw_line.lstrip().startswith("#"):
            continue
        key, value = raw_line.split("=", 1)
        key = key.strip()
        if key.startswith("NEO4J_") and key not in os.environ:
            os.environ[key] = value.strip().strip('"')


def _redact_known_secrets(message: str) -> str:
    redacted = message
    for key in ("NEO4J_URI", "NEO4J_PASSWORD", "CLIENT_SECRET", "DATABRICKS_TOKEN"):
        value = os.getenv(key)
        if value:
            redacted = redacted.replace(value, "[REDACTED]")
    return redacted


def _require_neo4j_config() -> None:
    _load_neo4j_env_from_file()
    missing = [key for key in REQUIRED_NEO4J_ENV if not os.getenv(key)]
    if missing:
        raise RuntimeError(
            "Missing Neo4j configuration: "
            + ", ".join(missing)
            + ". In Databricks, map these from app secret resources. Locally, set env vars "
            + "or set NEO4J_CREDENTIAL_FILE."
        )


@lru_cache(maxsize=1)
def _neo4j_driver():
    _require_neo4j_config()
    return GraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ["NEO4J_USERNAME"], os.environ["NEO4J_PASSWORD"]),
        max_transaction_retry_time=5,
    )


def _neo4j_database() -> str | None:
    database = os.getenv("NEO4J_DATABASE", "").strip()
    return database or None


def _row_limit(limit: int | None = None) -> int:
    configured = int(os.getenv("NEO4J_MAX_ROWS", DEFAULT_MAX_ROWS))
    requested = configured if limit is None else limit
    return max(1, min(int(requested), 100))


def _validate_read_only_cypher(cypher: str) -> str:
    query = cypher.strip()
    if not query:
        raise ValueError("Cypher query is empty.")
    if ";" in query.rstrip(";"):
        raise ValueError("Only one Cypher statement can be executed at a time.")

    query = query.rstrip(";").strip()
    upper = query.upper()
    if not upper.startswith(READ_ONLY_PREFIXES):
        raise ValueError(
            "Only read-only Cypher is allowed. Start with MATCH, RETURN, WITH, CALL, SHOW, "
            "EXPLAIN, PROFILE, or UNWIND."
        )
    if MUTATING_CYPHER.search(query):
        raise ValueError("Mutating Cypher is blocked. Use read-only queries only.")
    return query


def _jsonable(value: Any) -> Any:
    if isinstance(value, Node):
        return {
            "element_id": value.element_id,
            "labels": sorted(value.labels),
            "properties": {key: _jsonable(val) for key, val in dict(value).items()},
        }
    if isinstance(value, Relationship):
        return {
            "element_id": value.element_id,
            "type": value.type,
            "start_node": value.start_node.element_id,
            "end_node": value.end_node.element_id,
            "properties": {key: _jsonable(val) for key, val in dict(value).items()},
        }
    if isinstance(value, Neo4jPath):
        return {
            "nodes": [_jsonable(node) for node in value.nodes],
            "relationships": [_jsonable(rel) for rel in value.relationships],
        }
    if isinstance(value, (Date, Time, DateTime, Duration)):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _jsonable(val) for key, val in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(item) for item in value]
    return value


def _query_neo4j(cypher: str, parameters: dict[str, Any] | None = None, limit: int | None = None) -> str:
    row_limit = _row_limit(limit)
    query = _validate_read_only_cypher(cypher)

    def run(tx):
        result = tx.run(query, parameters or {})
        rows = []
        for index, record in enumerate(result):
            if index >= row_limit:
                break
            rows.append({key: _jsonable(record[key]) for key in record.keys()})
        return rows

    try:
        with _neo4j_driver().session(database=_neo4j_database()) as session:
            rows = session.execute_read(run)
    except Exception as exc:
        logger.warning("Neo4j query failed: %s", _redact_known_secrets(f"{type(exc).__name__}: {exc}"))
        return json.dumps(
            {"error": _redact_known_secrets(f"{type(exc).__name__}: {exc}")},
            indent=2,
            default=str,
        )

    return json.dumps(
        {"row_count": len(rows), "max_rows": row_limit, "rows": rows},
        indent=2,
        default=str,
    )


@function_tool
def get_current_time() -> str:
    """Get the current date and time."""
    return datetime.now().isoformat()


@function_tool
def get_neo4j_schema(limit: int = 20) -> str:
    """Inspect the Neo4j graph schema: labels, relationship types, and common patterns."""
    row_limit = _row_limit(limit)
    schema_queries = {
        "labels": """
            MATCH (n)
            UNWIND labels(n) AS label
            RETURN label, count(*) AS count
            ORDER BY count DESC
            LIMIT $limit
        """,
        "relationship_types": """
            MATCH ()-[r]->()
            RETURN type(r) AS relationship_type, count(*) AS count
            ORDER BY count DESC
            LIMIT $limit
        """,
        "relationship_patterns": """
            MATCH (a)-[r]->(b)
            RETURN labels(a) AS start_labels, type(r) AS relationship_type,
                   labels(b) AS end_labels, count(*) AS count
            ORDER BY count DESC
            LIMIT $limit
        """,
        "property_keys": """
            CALL db.propertyKeys() YIELD propertyKey
            RETURN collect(propertyKey)[0..$limit] AS property_keys
        """,
    }

    schema: dict[str, Any] = {}
    for name, query in schema_queries.items():
        schema[name] = json.loads(_query_neo4j(query, {"limit": row_limit}, limit=row_limit))
    return json.dumps(schema, indent=2, default=str)


@function_tool
def run_read_only_cypher(cypher: str, limit: int = DEFAULT_MAX_ROWS) -> str:
    """Run one read-only Cypher query against Neo4j and return JSON rows."""
    return _query_neo4j(cypher, limit=limit)


def create_agent() -> Agent:
    model = os.getenv("DATABRICKS_AGENT_MODEL", DEFAULT_MODEL)
    return Agent(
        name="Neo4j Graph Agent",
        instructions=(
            "You are a Databricks-hosted Neo4j graph analysis agent. "
            "Answer questions by inspecting the graph and running precise read-only Cypher. "
            "Use get_neo4j_schema before querying when labels, relationships, or properties are unknown. "
            "Use run_read_only_cypher for graph questions, keep queries narrow, and include LIMIT clauses. "
            "Never expose credentials, tokens, client secrets, or raw connection strings. "
            "If a query fails, explain the likely issue and try a corrected read-only query when possible."
        ),
        model=model,
        tools=[get_current_time, get_neo4j_schema, run_read_only_cypher],
    )


@invoke()
async def invoke_handler(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    if session_id := get_session_id(request):
        mlflow.update_current_trace(metadata={"mlflow.trace.session": session_id})

    messages = [item.model_dump() for item in request.input]
    result = await Runner.run(create_agent(), messages)
    return ResponsesAgentResponse(output=[item.to_input_item() for item in result.new_items])


@stream()
async def stream_handler(
    request: ResponsesAgentRequest,
) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    if session_id := get_session_id(request):
        mlflow.update_current_trace(metadata={"mlflow.trace.session": session_id})

    messages = [item.model_dump() for item in request.input]
    result = Runner.run_streamed(create_agent(), input=messages)
    async for event in process_agent_stream_events(result.stream_events()):
        yield event
