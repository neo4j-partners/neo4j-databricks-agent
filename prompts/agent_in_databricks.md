# Prompt history — moving the agent into Databricks

These are the prompts and decision points that produced `docs/AGENT_IN_DATABRICKS.md`.

## Original ask

> "I want to create an agent within databricks? — like MCP or UC catalog?"

Two paths surfaced:

1. Keep the FastAPI app, register the **Neo4j Aura hosted MCP server** as a UC connection so Databricks proxies tool calls.
2. Replace the client-side `openai-agents` loop with the **Supervisor API**, declaring UC functions / managed MCP / app-as-MCP / external MCP as hosted tools.

User picked (2).

## Discovery that reshaped the guide

While preparing a UC connection for the Aura MCP endpoint (`https://mcp.neo4j.io/agent?project_id=...&agent_id=...`) using the `CLIENT_ID` / `CLIENT_SECRET` from the Aura agent key:

- The MCP server's `/.well-known/oauth-authorization-server` points at the Auth0 tenant `https://aura-mcp.eu.auth0.com/`.
- `WWW-Authenticate: Bearer resource_metadata=...` from a 401 reveals the protected-resource metadata: scopes `openid profile email`, audience equals the MCP URL itself.
- Hitting `https://aura-mcp.eu.auth0.com/oauth/token` with `grant_type=client_credentials` (with and without `audience`/`scope`) returns `{"error":"access_denied"} HTTP 403`.
- The Aura `https://api.neo4j.io/oauth/token` returns a valid JWT, but the MCP server rejects it (401) — it's a different OAuth realm.

Conclusion: the Aura agent key is provisioned for interactive flows (device code / auth code), which Claude Desktop, Cursor, and similar clients use. UC connections need machine-to-machine (`client_credentials`). Until Neo4j enables M2M on Aura agent keys, the external-MCP path is blocked.

## What the guide ends up recommending

- **Path A** — port the existing `get_neo4j_schema` / `run_read_only_cypher` tools into Unity Catalog Python functions and reference them from the Supervisor API by name. Production-grade, no extra app, governed by UC, secrets stay in the `neo4j` scope.
- **Path B** — same UC functions, exposed externally via the managed UC Functions MCP server (`/api/2.0/mcp/functions/<catalog>/<schema>`) for non-Databricks MCP clients.
- **Path C** — bundle a custom MCP server as a second Databricks App (`mcp-*` prefix), grant `CAN_USE` from the agent app.
- **Path D** — external MCP via UC connection. Documented for completeness; gated on Neo4j enabling M2M.

## Verification steps captured in the guide

- OAuth probe sequence: `/.well-known/oauth-authorization-server`, the `WWW-Authenticate` header on a 401, then `/.well-known/oauth-protected-resource/...?project_id=...&agent_id=...`.
- Local + deployed smoke tests using `curl -sN -X POST .../responses` with `stream: true`.
- MLflow distributed-tracing prerequisites (`MLFLOW_EXPERIMENT_ID`, UC-backed `trace_location`).

## Open follow-ups

- File request with Neo4j Aura to enable `client_credentials` on agent-key Auth0 clients (or expose an M2M-friendly endpoint).
- If Path A is chosen, add a `prompts/` entry capturing the UC function SQL and any environment dependency setup for the `neo4j` Python package on serverless.
