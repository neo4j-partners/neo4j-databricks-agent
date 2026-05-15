#!/usr/bin/env python3
"""Load Neo4j credentials into a Databricks secret scope without printing values."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


DEFAULT_CREDENTIAL_FILE = (
    "/Users/gsivaji/Documents/Neo4j/Databricks/agentbricks/"
    "Neo4j-27ad415a-Created-2025-11-12.txt"
)

REQUIRED_KEYS = ("NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD", "NEO4J_DATABASE")
OPTIONAL_KEY_ALIASES = {
    "AURA_INSTANCEID": "NEO4J_AURA_INSTANCE_ID",
    "AURA_INSTANCENAME": "NEO4J_AURA_INSTANCE_NAME",
    "CLIENT_ID": "NEO4J_AURA_CLIENT_ID",
    "CLIENT_SECRET": "NEO4J_AURA_CLIENT_SECRET",
    "CLIENT_NAME": "NEO4J_AURA_CLIENT_NAME",
}


def parse_credential_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("mcp endpoint:"):
            values["NEO4J_MCP_ENDPOINT"] = line.split(":", 1)[1].strip()
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"')
        values[key] = value
        if key in OPTIONAL_KEY_ALIASES:
            values[OPTIONAL_KEY_ALIASES[key]] = value
    return values


def run_databricks(args: list[str], *, profile: str | None = None, stdin: str | None = None):
    command = ["databricks"]
    if profile:
        command.extend(["--profile", profile])
    command.extend(args)
    return subprocess.run(command, input=stdin, text=True, capture_output=True)


def create_scope(scope: str, profile: str | None) -> None:
    result = run_databricks(["secrets", "create-scope", scope], profile=profile)
    if result.returncode == 0:
        print(f"Created secret scope: {scope}")
        return
    output = f"{result.stdout}\n{result.stderr}"
    if "RESOURCE_ALREADY_EXISTS" in output or "already exists" in output.lower():
        print(f"Secret scope already exists: {scope}")
        return
    raise RuntimeError(output.strip())


def put_secret(scope: str, key: str, value: str, profile: str | None) -> None:
    result = run_databricks(
        ["secrets", "put-secret", scope, key],
        profile=profile,
        stdin=value,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to store {key}: {result.stderr.strip() or result.stdout.strip()}")
    print(f"Stored secret: {scope}/{key}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credential-file", default=DEFAULT_CREDENTIAL_FILE)
    parser.add_argument("--scope", default="neo4j")
    parser.add_argument("--profile", default=None)
    parser.add_argument(
        "--include-optional",
        action="store_true",
        help="Also store Aura API and MCP endpoint values from the credential file.",
    )
    args = parser.parse_args()

    credential_path = Path(args.credential_file).expanduser()
    if not credential_path.exists():
        print(f"Credential file not found: {credential_path}", file=sys.stderr)
        return 2

    values = parse_credential_file(credential_path)
    missing = [key for key in REQUIRED_KEYS if not values.get(key)]
    if missing:
        print(f"Missing required keys in credential file: {', '.join(missing)}", file=sys.stderr)
        return 2

    keys_to_store = list(REQUIRED_KEYS)
    if args.include_optional:
        keys_to_store.extend(
            key
            for key in (
                "NEO4J_AURA_INSTANCE_ID",
                "NEO4J_AURA_INSTANCE_NAME",
                "NEO4J_MCP_ENDPOINT",
                "NEO4J_AURA_CLIENT_ID",
                "NEO4J_AURA_CLIENT_SECRET",
                "NEO4J_AURA_CLIENT_NAME",
            )
            if values.get(key)
        )

    try:
        create_scope(args.scope, args.profile)
        for key in keys_to_store:
            put_secret(args.scope, key, values[key], args.profile)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    print("Neo4j Databricks secrets are ready.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
