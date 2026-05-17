# mcp-neo4j-cypher Databricks App

This is a thin Databricks Apps wrapper around the open-source
[`mcp-neo4j-cypher`](https://pypi.org/project/mcp-neo4j-cypher/) MCP server. It
exposes the Cypher tools (`read_neo4j_cypher`, `get_neo4j_schema`, …) over
streamable HTTP at `/mcp/` so the Databricks Supervisor API — or any other
MCP-aware agent — can call them as a hosted tool.

## Deployment surface

| Property | Value |
|----------|-------|
| App name | `mcp-neo4j-cypher` (must begin with `mcp-` to be MCP-registry-eligible) |
| Transport | Streamable HTTP at `/mcp/` |
| Port | `$DATABRICKS_APP_PORT` (set by Databricks) |
| Auth to Neo4j | Bolt creds from the `mcp-neo4j` secret scope |
| Read-only | `NEO4J_READ_ONLY=true` so mutating Cypher is rejected at the server |

## How it is wired

- `app.yaml` — declares the `mcp-neo4j-cypher` CLI command and the four `NEO4J_*` env vars mapped from the `mcp-neo4j` secret scope.
- `requirements.txt` — pins the PyPI package.
- The bundle (in the parent repo's `databricks.yml`) declares
  `resources.apps.mcp_neo4j_cypher`, points `source_code_path` at `./mcp-server/`,
  and grants the app's service principal `READ` on each of the four secrets.

## Local smoke test (after deploy)

```bash
TOKEN=$(databricks auth token --profile <profile> | jq -r .access_token)
URL=https://mcp-neo4j-cypher-<workspace-id>.aws.databricksapps.com/mcp/

# MCP initialize
curl -sN -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -H "Accept: application/json,text/event-stream" \
  "$URL" -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
      "protocolVersion": "2025-03-26",
      "capabilities": {},
      "clientInfo": {"name": "smoke-test", "version": "0.1"}
    }
  }'

# tools/list — confirm read_neo4j_cypher and get_neo4j_schema appear
curl -sN -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -H "Accept: application/json,text/event-stream" \
  "$URL" -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'
```

## Registering as an MCP server in Databricks

Once the app is RUNNING, it can be registered in **Agents > MCPs > Register MCP Server** (or via the REST API) so it appears in the workspace's MCP registry and is callable from any agent (Supervisor API hosted tool with `type: "app"`, or external MCP clients via OAuth).

## Consuming from the chat agent

In `agent_server/agent.py` (after migration to Supervisor API):

```python
TOOLS = [
    {
        "type": "app",
        "app": {
            "name": "mcp-neo4j-cypher",
            "description": "Read-only Cypher and Neo4j schema introspection.",
        },
    },
]
```

Plus this grant in `databricks.yml` under the agent app's `resources:` block:

```yaml
- name: mcp_neo4j_cypher
  app:
    name: mcp-neo4j-cypher
    permission: CAN_USE
```
