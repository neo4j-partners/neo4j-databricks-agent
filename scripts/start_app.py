#!/usr/bin/env python3
"""Local dev entrypoint for the agent server.

The UI is served directly by FastAPI (see agent_server/static/), so there is no
separate frontend process. This wrapper just resolves the port, ensures it is
free, and execs `start-server` with the right arguments.

Usage:
    uv run start-app [--port 8000] [--reload]
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
from pathlib import Path

from dotenv import load_dotenv


def _port_available(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        try:
            s.connect(("localhost", port))
        except (ConnectionRefusedError, OSError):
            return True
    return False


def main() -> int:
    load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env", override=True)

    parser = argparse.ArgumentParser(
        description="Run the Neo4j Databricks agent locally.",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("DATABRICKS_APP_PORT", "8000")),
        help="Port to bind (default: 8000 or $DATABRICKS_APP_PORT)",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="Reload the server on code changes",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of uvicorn workers",
    )
    args = parser.parse_args()

    if not os.environ.get("DATABRICKS_APP_NAME") and not _port_available(args.port):
        print(
            f"ERROR: Port {args.port} is already in use.\n"
            f"  To free it: lsof -ti :{args.port} | xargs kill -9",
            file=sys.stderr,
        )
        return 1

    import uvicorn

    print(f"Starting agent server on http://0.0.0.0:{args.port}")
    print(f"Open the chat UI at      http://localhost:{args.port}/")
    uvicorn.run(
        "agent_server.start_server:app",
        host="0.0.0.0",
        port=args.port,
        workers=args.workers,
        reload=args.reload,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
