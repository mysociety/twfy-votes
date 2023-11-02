import asyncio
from functools import wraps
from pathlib import Path
from typing import Awaitable, Callable, ParamSpec, TypeVar

import typer
import uvicorn

from .apps.core.db import db_lifespan

app = typer.Typer(help="")

two_levels_above = Path(__file__).parent.parent


# Create a type variable for the return type.
TReturn = TypeVar("TReturn")

# Create a ParamSpec variable for capturing argument types.
P = ParamSpec("P")


def coro(f: Callable[P, Awaitable[TReturn]]) -> Callable[P, TReturn]:
    @wraps(f)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> TReturn:
        return asyncio.run(f(*args, **kwargs))  # type: ignore

    return wrapper


def load_db(f: Callable[P, Awaitable[TReturn]]) -> Callable[P, Awaitable[TReturn]]:
    @wraps(f)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> TReturn:
        async with db_lifespan():
            return await f(*args, **kwargs)

    return wrapper


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


@app.command()
@coro
@load_db
async def update():
    from .apps.decisions.data_update import create_commons_cluster

    await create_commons_cluster()


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
