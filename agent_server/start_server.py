from pathlib import Path

from dotenv import load_dotenv
from fastapi.responses import FileResponse, Response
from fastapi.staticfiles import StaticFiles
from mlflow.genai.agent_server import AgentServer, setup_mlflow_git_based_version_tracking

# Load env vars from .env before importing the agent for proper auth
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env", override=True)

# Need to import the agent to register the functions with the server
import agent_server.agent  # noqa: E402

STATIC_DIR = Path(__file__).parent / "static"

agent_server = AgentServer("ResponsesAgent", enable_chat_proxy=False)
app = agent_server.app  # noqa: F841

if STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/", include_in_schema=False)
    async def root() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon() -> Response:
        icon = STATIC_DIR / "favicon.ico"
        if icon.exists():
            return FileResponse(icon)
        return Response(status_code=204)


setup_mlflow_git_based_version_tracking()


def main():
    agent_server.run(app_import_string="agent_server.start_server:app")
