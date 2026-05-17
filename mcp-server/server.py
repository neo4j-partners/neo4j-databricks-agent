"""FastMCP-based Neo4j Cypher MCP server with a demo landing page.

Surface:
- GET  /         -> HTML landing page (server identity, tool catalog, sample request)
- GET  /health   -> 200 JSON, for the Databricks Apps health probe
- POST /mcp/     -> MCP Streamable HTTP transport (FastMCP)

The MCP layer exposes two read-only tools backed by a single Neo4j Aura
instance read from the standard NEO4J_* environment variables (mapped from
the `mcp-neo4j` Databricks secret scope).
"""

from __future__ import annotations

import json
import logging
import os
import re
from contextlib import asynccontextmanager
from functools import lru_cache
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from fastmcp import FastMCP
from neo4j import GraphDatabase
from neo4j.graph import Node, Path as Neo4jPath, Relationship
from neo4j.time import Date, DateTime, Duration, Time

logger = logging.getLogger("mcp-neo4j-cypher")
logging.getLogger("neo4j").setLevel(logging.WARNING)

REQUIRED_ENV = ("NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD")
DEFAULT_MAX_ROWS = int(os.environ.get("NEO4J_MAX_ROWS", "50"))
READ_PREFIXES = ("MATCH", "RETURN", "WITH", "CALL", "SHOW", "EXPLAIN", "PROFILE", "UNWIND")
MUTATING = re.compile(
    r"\b(CREATE|MERGE|DELETE|DETACH\s+DELETE|SET|REMOVE|DROP|ALTER|RENAME|"
    r"LOAD\s+CSV|START\s+DATABASE|STOP\s+DATABASE)\b",
    re.IGNORECASE,
)


@lru_cache(maxsize=1)
def _driver():
    missing = [k for k in REQUIRED_ENV if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Missing Neo4j env vars: {', '.join(missing)}")
    return GraphDatabase.driver(
        os.environ["NEO4J_URI"],
        auth=(os.environ["NEO4J_USERNAME"], os.environ["NEO4J_PASSWORD"]),
        max_transaction_retry_time=5,
    )


def _database() -> str | None:
    return os.getenv("NEO4J_DATABASE", "").strip() or None


def _validate_read_only(cypher: str) -> str:
    q = (cypher or "").strip().rstrip(";").strip()
    if not q:
        raise ValueError("Cypher query is empty.")
    if ";" in q:
        raise ValueError("Only one Cypher statement can be executed at a time.")
    if not q.upper().startswith(READ_PREFIXES):
        raise ValueError(
            "Only read-only Cypher is allowed. Start with "
            "MATCH/RETURN/WITH/CALL/SHOW/EXPLAIN/PROFILE/UNWIND."
        )
    if MUTATING.search(q):
        raise ValueError("Mutating Cypher is blocked. Use read-only queries only.")
    return q


def _to_json(value: Any) -> Any:
    if isinstance(value, Node):
        return {
            "element_id": value.element_id,
            "labels": sorted(value.labels),
            "properties": {k: _to_json(v) for k, v in dict(value).items()},
        }
    if isinstance(value, Relationship):
        return {
            "element_id": value.element_id,
            "type": value.type,
            "start_node": value.start_node.element_id,
            "end_node": value.end_node.element_id,
            "properties": {k: _to_json(v) for k, v in dict(value).items()},
        }
    if isinstance(value, Neo4jPath):
        return {
            "nodes": [_to_json(n) for n in value.nodes],
            "relationships": [_to_json(r) for r in value.relationships],
        }
    if isinstance(value, (Date, Time, DateTime, Duration)):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _to_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_to_json(v) for v in value]
    return value


def _run_read(cypher: str, params: dict[str, Any] | None, row_limit: int) -> dict[str, Any]:
    query = _validate_read_only(cypher)
    limit = max(1, min(int(row_limit), 200))

    def _runner(tx):
        rows: list[dict[str, Any]] = []
        for i, rec in enumerate(tx.run(query, params or {})):
            if i >= limit:
                break
            rows.append({k: _to_json(rec[k]) for k in rec.keys()})
        return rows

    with _driver().session(database=_database()) as session:
        rows = session.execute_read(_runner)
    return {"row_count": len(rows), "max_rows": limit, "rows": rows}


# ---------------------------------------------------------------------------
# FastMCP server with two tools
# ---------------------------------------------------------------------------

mcp = FastMCP(
    name="neo4j-cypher",
    instructions=(
        "Read-only Cypher and schema introspection over a single Neo4j Aura "
        "instance. Always call get_neo4j_schema first when labels, relationship "
        "types, or property keys are unknown."
    ),
)


@mcp.tool
def get_neo4j_schema(sample_size: int = 100) -> dict[str, Any]:
    """Return labels, relationship types, property keys, and common patterns.

    Uses APOC (`apoc.meta.schema`) when available for a precise, indexed view,
    and falls back to native queries against `db.labels()` / `db.relationshipTypes()`
    / `db.propertyKeys()` otherwise.
    """
    try:
        apoc = _run_read(
            "CALL apoc.meta.schema({sample: $sample}) YIELD value RETURN value AS schema",
            {"sample": int(sample_size)},
            row_limit=1,
        )
        if apoc["rows"]:
            return {"source": "apoc.meta.schema", "schema": apoc["rows"][0]["schema"]}
    except Exception as exc:
        logger.info("apoc.meta.schema unavailable, falling back: %s", exc)

    labels = _run_read("CALL db.labels() YIELD label RETURN label ORDER BY label", None, 1000)
    rels = _run_read(
        "CALL db.relationshipTypes() YIELD relationshipType "
        "RETURN relationshipType ORDER BY relationshipType",
        None,
        1000,
    )
    keys = _run_read(
        "CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey ORDER BY propertyKey",
        None,
        1000,
    )
    patterns = _run_read(
        "MATCH (a)-[r]->(b) "
        "RETURN labels(a) AS start, type(r) AS rel, labels(b) AS end, count(*) AS count "
        "ORDER BY count DESC LIMIT 25",
        None,
        25,
    )
    return {
        "source": "native",
        "labels": [r["label"] for r in labels["rows"]],
        "relationship_types": [r["relationshipType"] for r in rels["rows"]],
        "property_keys": [r["propertyKey"] for r in keys["rows"]],
        "patterns": patterns["rows"],
    }


@mcp.tool
def read_neo4j_cypher(
    query: str,
    params: dict[str, Any] | None = None,
    row_limit: int = DEFAULT_MAX_ROWS,
) -> dict[str, Any]:
    """Execute one read-only Cypher query and return JSON rows.

    The server rejects multi-statement input and any mutating clause
    (CREATE/MERGE/DELETE/SET/REMOVE/DROP/ALTER/RENAME/LOAD CSV/...). For
    schema discovery prefer `get_neo4j_schema`.
    """
    return _run_read(query, params, row_limit)


# ---------------------------------------------------------------------------
# FastAPI parent with landing page + health, MCP mounted at /mcp/
# ---------------------------------------------------------------------------

mcp_app = mcp.http_app(path="/")
LANDING_HTML = (Path(__file__).parent / "landing.html").read_text(encoding="utf-8")


@asynccontextmanager
async def lifespan(api: FastAPI):
    """Forward the FastMCP lifespan so the session manager initializes."""
    async with mcp_app.lifespan(api):
        yield


app = FastAPI(
    title="Neo4j Cypher MCP server",
    description="Read-only Cypher and Neo4j schema tools over MCP. Served on Databricks Apps.",
    lifespan=lifespan,
    docs_url=None,
    redoc_url=None,
    openapi_url=None,
)


@app.get("/", include_in_schema=False)
async def landing() -> HTMLResponse:
    return HTMLResponse(LANDING_HTML)


@app.get("/health", include_in_schema=False)
async def health() -> JSONResponse:
    return JSONResponse({"status": "healthy", "server": "neo4j-cypher", "transport": "streamable-http"})


app.mount("/mcp", mcp_app)
