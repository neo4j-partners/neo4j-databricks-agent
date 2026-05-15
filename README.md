# Neo4j Databricks Agent

Databricks Apps agent that answers questions against a Neo4j Aura graph through
read-only Cypher tools. The app uses the Databricks OpenAI-compatible client,
MLflow AgentServer, and the built-in Databricks Apps chat UI.

## What It Does

- Runs as a Databricks App named `neo4j-databricks-agent`.
- Uses `databricks-claude-sonnet-4-5` by default.
- Reads Neo4j connection details from Databricks secret resources.
- Provides two graph tools to the agent:
  - `get_neo4j_schema`
  - `run_read_only_cypher`
- Blocks Cypher mutation keywords and caps returned rows.

## Local Files

The Neo4j credential file is expected at:

```bash
/Users/gsivaji/Documents/Neo4j/Databricks/agentbricks/Neo4j-27ad415a-Created-2025-11-12.txt
```

No secret values are committed to this project.

## Databricks Setup

Authenticate with OAuth for Databricks Apps work:

```bash
databricks auth login --host https://dbc-de840360-1abb.cloud.databricks.com
```

Load the Neo4j credentials into the Databricks secret scope:

```bash
cd /Users/gsivaji/Documents/Neo4j/Databricks/agentbricks/neo4j-databricks-agent
uv run setup-neo4j-secrets
```

Then validate and deploy the app:

```bash
databricks bundle validate
databricks bundle deploy -t dev
databricks bundle run neo4j_databricks_agent -t dev
```

Current workspace status:

- OAuth profile `neo4j-agent` has been created locally.
- Databricks app `neo4j-databricks-agent` exists.
- Neo4j secrets have been written to the `neo4j` secret scope.
- Source deployment is blocked until Workspace Files can access the workspace
  storage bucket. The Databricks API currently returns `Cannot access AWS bucket`
  on Workspace Files upload/export requests.

## Local Development

Use the credential file directly for local Neo4j access:

```bash
cd /Users/gsivaji/Documents/Neo4j/Databricks/agentbricks/neo4j-databricks-agent
export NEO4J_CREDENTIAL_FILE=/Users/gsivaji/Documents/Neo4j/Databricks/agentbricks/Neo4j-27ad415a-Created-2025-11-12.txt
uv run start-app --no-ui
```

Example backend request:

```bash
curl -X POST http://localhost:8000/invocations \
  -H "Content-Type: application/json" \
  -d '{"input":[{"role":"user","content":"What labels are in the graph?"}]}'
```

## Notes

The bundle also defines:

- MLflow experiment: `/Shared/neo4j-databricks-agent`
- Secret scope: `neo4j`
- Secret keys: `NEO4J_URI`, `NEO4J_USERNAME`, `NEO4J_PASSWORD`, `NEO4J_DATABASE`
- Serving endpoint permission: `CAN_QUERY` on `databricks-claude-sonnet-4-5`
