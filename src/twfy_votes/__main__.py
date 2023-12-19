import asyncio
import os
from functools import wraps
from pathlib import Path
from typing import Awaitable, Callable, Optional, ParamSpec, TypeVar

import typer
import uvicorn
from trogon import Trogon  # type: ignore
from typer.main import get_group

from .apps.core.db import db_lifespan
from .apps.policies.models import PolicyDirection, PolicyStrength

app = typer.Typer(help="")

two_levels_above = Path(__file__).parent.parent

PORT = int(os.environ.get("PORT", 8000))

# Create a type variable for the return type.
TReturn = TypeVar("TReturn")

# Create a ParamSpec variable for capturing argument types.
P = ParamSpec("P")


@app.command()
def ui(ctx: typer.Context):
    """
    Open terminal UI
    """
    Trogon(get_group(app), click_context=ctx).run()


def coroutine(f: Callable[P, Awaitable[TReturn]]) -> Callable[P, TReturn]:
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
    """
    Render views configured with static site parameters
    """
    from .main import app as fastapi_app

    fastapi_app.render()


@app.command()
def run_server(static: bool = False, live: bool = False):
    """
    Run the main fastapi server - or a static server on the
    _site directory if '--static' is passed.
    """
    if static:
        run_static_server()
    else:
        if live:
            print("Running in production mode")
            run_fastapi_prod_server()
        else:
            run_fastapi_server()


@app.command()
@coroutine
@load_db
async def update():
    """
    Create the cached tables that are used in views
    and are a bit too expensive to run on the fly.
    Roughly needs to be run each time the data is updated.
    """
    from .apps.decisions.data_update import (
        create_commons_cluster,
        process_cached_tables,
    )

    await process_cached_tables()
    await create_commons_cluster()


@app.command()
@coroutine
@load_db
async def update_motions_info(gid: Optional[str] = None, all: bool = False):
    """
    Create the cached tables that are used in views
    and are a bit too expensive to run on the fly.
    Roughly needs to be run each time the data is updated.

    Specific a specifc gid to refetch that one.
    """
    from .apps.decisions.motion_analysis import update_motion_yaml

    await update_motion_yaml(specific_gid=gid, run_all=all)


@app.command()
@coroutine
@load_db
async def create_voting_records(
    policy_id: Optional[int] = None, person_id: Optional[int] = None
):
    """
    Generate the big voting file.
    File can also be partially updated by specify as person_id or policy_id.
    Although the policy_id is smaller - it's not quick - because the generation is per person.
    This will limit to just the people who are affected by the policy.
    """
    from .apps.decisions.models import AllowedChambers
    from .apps.policies.vr_generator import (
        generate_voting_records_for_chamber,
    )

    await generate_voting_records_for_chamber(
        chamber=AllowedChambers.COMMONS, policy_id=policy_id, person_id=person_id
    )


@app.command()
@coroutine
@load_db
async def validate_voting_records(sample_size: int = 10):
    """
    Run a validation on a random sample of voting records.
    """
    from .apps.policies.vr_validator import test_policy_sample

    await test_policy_sample(sample_size)


@app.command()
def add_vote_to_policy(
    votes_url: str,
    policy_id: int,
    vote_alignment: PolicyDirection,
    strength: PolicyStrength = PolicyStrength.STRONG,
):
    """
    Add a vote to a policy based on a twfy-votes URL.
    """
    from .apps.policies.tools import add_vote_to_policy_from_url

    add_vote_to_policy_from_url(
        votes_url=votes_url,
        policy_id=policy_id,
        vote_alignment=vote_alignment,
        strength=strength,
    )


def run_fastapi_prod_server():
    uvicorn.run(
        "twfy_votes.main:app",
        host="0.0.0.0",
        port=PORT,
        proxy_headers=True,
        forwarded_allow_ips="*",
    )


def run_fastapi_server():
    uvicorn.run(
        "twfy_votes.main:app",
        host="0.0.0.0",
        port=PORT,
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
