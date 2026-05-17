# Creating a Neo4j Agent in Databricks

A step-by-step guide for evolving this repo's agent from a self-contained FastAPI app (running its own `openai-agents` loop) into a **Databricks-orchestrated agent** that uses Unity Catalog functions, Databricks-managed MCP servers, or external MCP servers as hosted tools.

This is the architecture the **Supervisor API** enables: instead of your FastAPI app driving the LLM <-> tool loop, you declare hosted tools and Databricks runs the loop server-side. You keep your Databricks App as the surface — it just calls `responses.create(...)` and streams the result back to the browser.

```
+-----------+        +-------------------------+        +---------------------+
| Browser   | <----> | Databricks App (FastAPI)| <----> | Supervisor API      |
| (chat UI) |   SSE  | /responses                       | (tool loop)         |
+-----------+        +-------------------------+        +----+--------+-------+
                                                             |        |
                                                             v        v
                                                  +----------+   +----------------+
                                                  | UC funcs |   | MCP servers    |
                                                  | (Bolt    |   | (managed /     |
                                                  |  driver) |   |  app / ext UC) |
                                                  +----------+   +----------------+
```

---

## TL;DR — pick a path

| Path | Tool type | Auth | Best for |
|------|-----------|------|----------|
| **A. UC Functions** | `uc_function` | Service principal queries UC functions; UC function uses Aura Bolt creds from secrets | Production, governed, no extra app. **Recommended starting point for this repo.** |
| **B. Databricks-managed MCP** | `uc_function` exposed via the managed UC functions MCP server | Same as A | Same as A; useful if you want to reuse the functions outside this agent |
| **C. Self-hosted MCP App** | `app` | App-to-app `CAN_USE` (no per-user OAuth) | If you already maintain a custom MCP server and want it bundled. **Live in this repo — see `mcp-server/` and Step 6.** |
| **D. External MCP via UC Connection** | `uc_connection` | OAuth M2M (`client_credentials`) on the external server | If your provider issues an M2M-capable client. **Currently blocked for Aura — see Appendix.** |

---

## Prerequisites

- Working app from this repo, deployed (this guide assumes `app.yaml` already points to `uvicorn agent_server.start_server:app`).
- Databricks CLI **v0.298.0 or newer** (the bundle schema for `app` resources requires it).
- A profile in `~/.databrickscfg` for the target workspace.
- Unity Catalog enabled, plus a writable catalog/schema (this guide uses `neo4j_agent.tools` — replace with yours).
- `databricks-openai>=0.14.0`, `databricks-sdk>=0.55.0`, `mlflow>=3.10.0` in `pyproject.toml` (already present in this repo).
- A SQL warehouse if you create UC SQL functions (or serverless if you create Python UDFs).
- The `neo4j` secret scope, populated with `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, `NEO4J_DATABASE` (already present in this repo).

```bash
# Verify
databricks --version                            # v0.298.0+
databricks auth profiles
databricks secrets list-secrets neo4j --profile <profile>
```

---

## Step 1 — Choose the agent loop architecture

Today this repo runs the `openai-agents` SDK Runner *inside* the FastAPI app. The Supervisor API replaces that loop with a Databricks-hosted one. The trade-offs:

- **Keep the local loop** (status quo). Lower complexity, no Supervisor API dependency, no AI Gateway V2 requirement. Tools must be Python callables inside the app.
- **Move to Supervisor API**. Databricks runs the loop. You declare hosted tools by reference (UC function name, app name, UC connection name). End-to-end traces in MLflow. Built-in retry, parallel tool calls, server-side observability.

The rest of this guide assumes you are moving to the Supervisor API.

---

## Step 2 — Path A: Expose Neo4j as Unity Catalog Functions

A UC Python function is the cleanest way to wrap your Bolt queries. It runs on serverless compute, uses secrets for credentials, returns JSON the LLM can read.

### 2.1 Create the schema and grant ownership

```bash
databricks api post /api/2.0/sql/statements --json '{
  "warehouse_id": "<your-warehouse-id>",
  "statement": "CREATE SCHEMA IF NOT EXISTS neo4j_agent.tools COMMENT \"Tools the Neo4j agent calls via UC functions.\""
}' --profile <profile>
```

### 2.2 Create a `run_read_only_cypher` UC function

The function reads the Bolt URL/user/password from the `neo4j` secret scope using `secret(...)` (UC SQL function) or `dbutils.secrets.get` (Python UDF). Python is required because we need the `neo4j` driver.

```sql
CREATE OR REPLACE FUNCTION neo4j_agent.tools.run_read_only_cypher(
  cypher STRING COMMENT 'A single read-only Cypher query. Must start with MATCH/RETURN/WITH/CALL/SHOW/EXPLAIN/PROFILE/UNWIND. Mutating clauses (CREATE/MERGE/DELETE/SET/REMOVE/DROP/...) are rejected.',
  row_limit INT DEFAULT 25 COMMENT 'Maximum rows to return (1-100). Defaults to 25.'
)
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Executes a read-only Cypher query against the configured Neo4j Aura instance and returns JSON {row_count, rows}. Use this for any graph data lookup. For schema discovery (labels, relationships, properties), prefer get_neo4j_schema.'
AS $$
import json, os, re
from neo4j import GraphDatabase

MUTATING = re.compile(r"\b(CREATE|MERGE|DELETE|DETACH\s+DELETE|SET|REMOVE|DROP|ALTER|RENAME|LOAD\s+CSV)\b", re.IGNORECASE)
READ_PREFIXES = ("MATCH","RETURN","WITH","CALL","SHOW","EXPLAIN","PROFILE","UNWIND")

q = (cypher or "").strip().rstrip(";")
if not q or not q.upper().startswith(READ_PREFIXES):
    return json.dumps({"error": "Only read-only Cypher is allowed."})
if MUTATING.search(q):
    return json.dumps({"error": "Mutating Cypher is blocked."})

limit = max(1, min(int(row_limit or 25), 100))
uri = dbutils.secrets.get("neo4j", "NEO4J_URI")
user = dbutils.secrets.get("neo4j", "NEO4J_USERNAME")
pw   = dbutils.secrets.get("neo4j", "NEO4J_PASSWORD")
db   = dbutils.secrets.get("neo4j", "NEO4J_DATABASE") or None

with GraphDatabase.driver(uri, auth=(user, pw)) as driver, driver.session(database=db) as session:
    rows = []
    for i, rec in enumerate(session.execute_read(lambda tx: list(tx.run(q)))):
        if i >= limit: break
        rows.append({k: rec[k] for k in rec.keys()})
return json.dumps({"row_count": len(rows), "rows": rows}, default=str)
$$;
```

Repeat for `get_neo4j_schema` (re-using the four queries from `agent_server/agent.py:200-225`).

> **Note on the driver**: UC Python UDFs install packages declared in environment dependencies. Make sure `neo4j>=6.0.0` is available on the serverless compute environment, or use an SQL warehouse with a custom environment.

### 2.3 Verify

```bash
databricks functions get neo4j_agent.tools.run_read_only_cypher --profile <profile>
```

```sql
SELECT neo4j_agent.tools.run_read_only_cypher('MATCH (n) RETURN count(n) AS c', 1);
```

---

## Step 3 — Rewrite `agent_server/agent.py` to use the Supervisor API

Replace the entire `openai-agents` Runner + `@function_tool` decorators with a `databricks_openai` Supervisor API call. The handlers stay; the body changes.

```python
import os
import logging
import mlflow
from databricks.sdk import WorkspaceClient
from databricks_openai import DatabricksOpenAI
from mlflow import MlflowClient
from mlflow.genai.agent_server import invoke, stream
from mlflow.tracing import get_tracing_context_headers_for_http_request
from mlflow.types.responses import ResponsesAgentRequest, ResponsesAgentResponse

logger = logging.getLogger(__name__)
mlflow.openai.autolog()

MODEL = os.getenv("DATABRICKS_AGENT_MODEL", "databricks-claude-sonnet-4-5")

TOOLS = [
    {
        "type": "uc_function",
        "uc_function": {
            "name": "neo4j_agent.tools.run_read_only_cypher",
            "description": (
                "Run a single read-only Cypher query against Neo4j Aura. "
                "Use for any graph data lookup. Pass a query starting with "
                "MATCH/RETURN/WITH/CALL/SHOW/EXPLAIN/PROFILE/UNWIND."
            ),
        },
    },
    {
        "type": "uc_function",
        "uc_function": {
            "name": "neo4j_agent.tools.get_neo4j_schema",
            "description": (
                "Inspect the Neo4j graph schema (labels, relationship types, "
                "patterns, property keys). Call before querying when unknown."
            ),
        },
    },
]

INSTRUCTIONS = (
    "You are a Databricks-hosted Neo4j graph analysis agent. "
    "Inspect the graph schema first when labels/relationships are unknown, "
    "then run precise read-only Cypher. Keep queries narrow with LIMIT. "
    "Never expose credentials, tokens, or raw connection strings."
)

_wc = WorkspaceClient()
_client = DatabricksOpenAI(workspace_client=_wc, use_ai_gateway=True)


def _trace_destination() -> dict:
    exp_id = os.environ.get("MLFLOW_EXPERIMENT_ID")
    if not exp_id:
        raise RuntimeError("MLFLOW_EXPERIMENT_ID is not set.")
    loc = MlflowClient().get_experiment(exp_id).trace_location
    if loc is None or not hasattr(loc, "catalog_name"):
        raise RuntimeError(
            f"Experiment {exp_id} trace_location is not a UC location. "
            "Enable 'MLflow traces in Unity Catalog'."
        )
    dest = {"catalog_name": loc.catalog_name, "schema_name": loc.schema_name}
    if loc.table_prefix:
        dest["table_prefix"] = loc.table_prefix
    return dest


_TRACE_DEST = _trace_destination()


def _build_input(request: ResponsesAgentRequest) -> list[dict]:
    return [{"role": "system", "content": INSTRUCTIONS}] + [
        item.model_dump() for item in request.input
    ]


@invoke()
def invoke_handler(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    if request.context and request.context.conversation_id:
        mlflow.update_current_trace(
            metadata={"mlflow.trace.session": request.context.conversation_id}
        )
    response = _client.responses.create(
        model=MODEL,
        input=_build_input(request),
        tools=TOOLS,
        stream=False,
        extra_headers=get_tracing_context_headers_for_http_request(),
        extra_body={"trace_destination": _TRACE_DEST},
    )
    return ResponsesAgentResponse(output=[item.model_dump() for item in response.output])


@stream()
def stream_handler(request: ResponsesAgentRequest):
    if request.context and request.context.conversation_id:
        mlflow.update_current_trace(
            metadata={"mlflow.trace.session": request.context.conversation_id}
        )
    return _client.responses.create(
        model=MODEL,
        input=_build_input(request),
        tools=TOOLS,
        stream=True,
        extra_headers=get_tracing_context_headers_for_http_request(),
        extra_body={"trace_destination": _TRACE_DEST},
    )
```

Key differences vs the original:
- No `@function_tool` decorators, no `Runner.run_streamed`, no `_neo4j_driver` inside the app.
- Tools are passed by **name** as a list of dicts; the Supervisor API resolves them.
- `extra_headers` + `trace_destination` link client and server spans into one MLflow trace.

---

## Step 4 — Update `databricks.yml` resource grants

The app's service principal needs `EXECUTE` on each UC function. The model serving endpoint needs `CAN_QUERY` (already present). You can drop the `NEO4J_*` secret resources from the **app** config, since Bolt creds now live inside the UC functions, but **keep them in the `neo4j` scope** because the UC functions read from there.

```yaml
resources:
  apps:
    neo4j_databricks_agent:
      # ...existing...
      resources:
        - name: experiment
          experiment:
            experiment_id: ${resources.experiments.neo4j_agent_experiment.id}
            permission: CAN_MANAGE
        - name: llm
          serving_endpoint:
            name: databricks-claude-sonnet-4-5
            permission: CAN_QUERY
        - name: cypher_tool
          uc_securable:
            type: FUNCTION
            full_name: neo4j_agent.tools.run_read_only_cypher
            permission: EXECUTE
        - name: schema_tool
          uc_securable:
            type: FUNCTION
            full_name: neo4j_agent.tools.get_neo4j_schema
            permission: EXECUTE
```

> **Grant on the secret scope:** the UC functions read secrets at execution time. Grant the **function owner** (or a group containing it) `READ` on the `neo4j` scope; the function then runs as the owner. The app's service principal does *not* need scope access in this design.

---

## Step 5 — Path B: Use the managed UC Functions MCP server (optional)

Databricks publishes a managed MCP server that exposes UC functions automatically. If you want to consume the same functions from external MCP clients (Claude Desktop, Cursor, your own LangGraph agent), you don't need to redeclare them — point any MCP client at:

```
https://<workspace-host>/api/2.0/mcp/functions/neo4j_agent/tools
```

OAuth scope: `unity-catalog`. No extra config in this repo.

This is **not** how the Supervisor API consumes UC functions (it uses `uc_function` directly), but it's useful for multi-client reuse.

---

## Step 6 — Path C: Bundle a custom MCP server as a Databricks App

> **Status: live in this repo.** The Neo4j Cypher MCP server is deployed as the Databricks App `mcp-neo4j-cypher` (URL: `https://mcp-neo4j-cypher-281440413997137.aws.databricksapps.com`, MCP endpoint at `/mcp/mcp`). Source lives under [`mcp-server/`](../mcp-server/README.md). The agent app does **not** yet consume it — wire it in by following the steps below when you migrate to the Supervisor API.

### How `mcp-server/` is structured

```
mcp-server/
├── server.py          # FastMCP + Starlette: tools + landing + MCP mount
├── landing.html       # HTML served on GET /
├── app.yaml           # uvicorn command + NEO4J_* env mappings
├── requirements.txt   # fastmcp, starlette, uvicorn, neo4j
└── README.md          # surface contract, smoke test, consumption notes
```

Why custom FastMCP instead of the upstream PyPI `mcp-neo4j-cypher` CLI: a real landing page on `/`, clean tool names without an upstream namespace prefix, a `/health` endpoint, and server-side read-only enforcement that's auditable in `server.py`.

### How it's wired in `databricks.yml`

```yaml
resources:
  secret_scopes:
    mcp_neo4j: { name: mcp-neo4j }

  apps:
    mcp_neo4j_cypher:
      name: mcp-neo4j-cypher
      source_code_path: ./mcp-server
      config:
        command: ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "$DATABRICKS_APP_PORT"]
        env:
          - name: NEO4J_MAX_ROWS
            value: "50"
          - name: NEO4J_URI
            value_from: "neo4j_uri"
          - name: NEO4J_USERNAME
            value_from: "neo4j_username"
          - name: NEO4J_PASSWORD
            value_from: "neo4j_password"
          - name: NEO4J_DATABASE
            value_from: "neo4j_database"
      resources:
        - name: neo4j_uri      ;  secret: { scope: mcp-neo4j, key: neo4j_uri,      permission: READ }
        - name: neo4j_username ;  secret: { scope: mcp-neo4j, key: neo4j_username, permission: READ }
        - name: neo4j_password ;  secret: { scope: mcp-neo4j, key: neo4j_password, permission: READ }
        - name: neo4j_database ;  secret: { scope: mcp-neo4j, key: neo4j_database, permission: READ }
```

### How to consume it from the agent app

```yaml
# In the agent app's resources block in databricks.yml
- name: mcp_neo4j_cypher
  app:
    name: mcp-neo4j-cypher
    permission: CAN_USE
```

```python
# In agent_server/agent.py (Supervisor API form)
TOOLS = [
    {
        "type": "app",
        "app": {
            "name": "mcp-neo4j-cypher",
            "description": "Read-only Cypher queries and Neo4j schema introspection via APOC.",
        },
    },
]
```

Requires Databricks CLI v0.298.0+. The bundle grants `CAN_USE` on deploy.

### MCP registry visibility

Any Databricks App with a name starting with `mcp-` is auto-listed in the workspace's **Agents > MCPs** registry. No "Register MCP Server" click is needed. The "Register MCP Server" button is only for registering **external** (non-Databricks-App) MCP endpoints.

### Smoke test (after deploy)

The Databricks Apps MCP registry treats the app URL as the MCP base and appends `/mcp` to construct the protocol endpoint, so the canonical URL is `{app_url}/mcp/mcp`.

```bash
TOKEN=$(databricks auth token --profile <profile> | jq -r .access_token)
URL=https://mcp-neo4j-cypher-<workspace-id>.aws.databricksapps.com/mcp/mcp

# MCP initialize -> SSE response with server capabilities
curl -sN -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -H "Accept: application/json,text/event-stream" \
  "$URL" -d '{
    "jsonrpc":"2.0","id":1,"method":"initialize",
    "params":{"protocolVersion":"2025-03-26","capabilities":{},
              "clientInfo":{"name":"smoke","version":"0.1"}}
  }'
```

Expected tools: `get_neo4j_schema` (APOC-based, falls back to native `db.labels()` / `db.relationshipTypes()` / `db.propertyKeys()`) and `read_neo4j_cypher` (read-only, single-statement validator). Hosted MCP tools trigger the multi-turn approval flow (Step 8) unless you auto-approve client-side.

---

## Step 7 — Deploy and verify

```bash
# Bind any pre-existing resources (one time per workspace), then deploy.
databricks bundle validate --profile <profile>
databricks bundle deploy --profile <profile>
databricks bundle run --profile <profile> neo4j_databricks_agent
```

Smoke-test the live app:

```bash
curl -sN -X POST \
  -H "Authorization: Bearer $(databricks auth token --profile <profile> | jq -r .access_token)" \
  -H "content-type: application/json" \
  "https://<app-host>/responses" \
  -d '{"input":[{"role":"user","content":"List the top 5 labels in the graph by count."}], "stream": true}'
```

You should see SSE chunks that include `response.output_item.done` items for the UC function call and the assistant's final message. Open `/` in a browser to use the chat UI.

---

## Step 8 — MCP approval flow (Paths C and D only)

When you use `app` or `uc_connection` tool types, the Supervisor API does **not** auto-execute the MCP call. It returns `mcp_approval_request` items and waits for `mcp_approval_response` on the next turn.

The chat UI in `agent_server/static/app.js` already round-trips Responses-API output items in `state.history`. To complete the flow, add an "Approve" button that the renderer shows when it sees an `mcp_approval_request` item, then send a follow-up `/responses` request with the approval item appended:

```js
state.history.push({
  type: "mcp_approval_response",
  id: approvalReq.id,
  approval_request_id: approvalReq.id,
  approve: true,
});
```

For UC functions (Path A/B), no approval flow is needed — they execute server-side.

---

## Step 9 — Verify distributed tracing in MLflow

After a few requests, the experiment configured in `MLFLOW_EXPERIMENT_ID` should show traces with both client-side spans (your FastAPI handler) and server-side spans (the Supervisor API tool calls).

```bash
databricks api get /api/2.0/mlflow/experiments/get \
  --json '{"experiment_id": "<MLFLOW_EXPERIMENT_ID>"}' \
  --profile <profile>
```

> Distributed tracing requires the experiment's `trace_location` to be a Unity Catalog location backed by customer-managed storage. If you see `Experiment <id> trace_location is not a UC location` on startup, enable "MLflow traces in Unity Catalog" for your workspace.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `403 access_denied` from the model | App SP missing `CAN_QUERY` on the serving endpoint | Add the `serving_endpoint` resource block in `databricks.yml`, redeploy |
| `403 access_denied` calling `run_read_only_cypher` | App SP missing `EXECUTE` on the function | Add the `uc_securable` resource block, redeploy |
| `RuntimeError: MLFLOW_EXPERIMENT_ID is not set` | Env var not propagated | Confirm `databricks.yml` declares the `experiment` resource with `valueFrom: experiment` in env block |
| `Parameter not supported when tools are provided` | Passing `temperature`/`top_p` with `tools=[...]` | Remove inference params |
| `Please ensure AI Gateway V2 is enabled` | Workspace missing AI Gateway V2 | Contact your Databricks account team |
| `unknown field: name` on the `app` resource | CLI too old | Upgrade to v0.298.0+ |
| MCP tool calls hang at "approval" | UI not handling `mcp_approval_request` | Implement the approval round-trip from Step 8 |

---

## Appendix — Why the external Neo4j Aura MCP server isn't usable as a UC connection (today)

If you read Neo4j Aura's docs and find the MCP endpoint
`https://mcp.neo4j.io/agent?project_id=...&agent_id=...` with a `CLIENT_ID`/`CLIENT_SECRET` from the agent key, you might expect to register it as a UC HTTP connection with OAuth M2M. Two reasons that doesn't work right now:

1. **The Aura API token (`https://api.neo4j.io/oauth/token`) is not accepted by the MCP server.** The MCP server is gated by an Auth0 tenant (`aura-mcp.eu.auth0.com`) per its `/.well-known/oauth-authorization-server` discovery doc.
2. **The Aura agent key's Auth0 client doesn't have the `client_credentials` grant enabled.** Hitting `https://aura-mcp.eu.auth0.com/oauth/token` with `grant_type=client_credentials` returns `access_denied (403)`. That client is provisioned for the interactive flows (device code / authorization code) that desktop MCP clients use, not the M2M flow that a UC connection requires.

When Neo4j enables M2M for Aura agent keys, you can register the connection like this:

```bash
databricks connections create --json '{
  "name": "neo4j_aura_mcp",
  "connection_type": "HTTP",
  "options": {
    "host": "https://mcp.neo4j.io",
    "base_path": "/agent?project_id=<pid>&agent_id=<aid>",
    "client_id": "<m2m-client-id>",
    "client_secret": "<m2m-client-secret>",
    "token_endpoint": "https://aura-mcp.eu.auth0.com/oauth/token",
    "oauth_scope": "openid profile email"
  }
}' --profile <profile>
```

…and reference it from `agent.py`:

```python
{"type": "uc_connection",
 "uc_connection": {"name": "neo4j_aura_mcp",
                   "description": "Neo4j Aura Agent MCP server"}}
```

…with this resource block in `databricks.yml`:

```yaml
- name: neo4j_aura_mcp
  uc_securable:
    type: CONNECTION
    full_name: neo4j_aura_mcp
    permission: USE_CONNECTION
```

Until then, **Path A (UC Functions)** is the right production choice for this repo.
