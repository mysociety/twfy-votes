from pathlib import Path

import typer
import uvicorn

app = typer.Typer(help="")

two_levels_above = Path(__file__).parent.parent


@app.command()
def render():
    from .main import app as fastapi_app

    fastapi_app.render()


@app.command()
def run_server(static: bool = False):
    if static:
        run_static_server()
    else:
        run_fastapi_server()


def run_fastapi_server():
    uvicorn.run(  # type: ignore
        "twfy_votes.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        reload_dirs=[str(two_levels_above)],
        reload_delay=2,
    )


def run_static_server():
    """
    Run a basic http server with the render_dir as the root
    """
    from .helpers.static_fastapi.serve import serve_folder
    from .internal.settings import settings

    serve_folder(settings.render_dir)


if __name__ == "__main__":
    app()
