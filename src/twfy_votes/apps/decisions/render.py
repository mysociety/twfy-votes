import asyncio
from typing import Any, AsyncIterator

from ...helpers.static_fastapi.static import StaticAPIRouter
from ...internal.db import duck_core
from ...internal.settings import settings

router = StaticAPIRouter(template_directory=settings.template_dir)


@router.render_for_path("/")
@router.render_for_path("/decisions")
@router.render_for_path("/decisions.json")
async def no_args() -> AsyncIterator[dict[Any, Any]]:
    yield {}


@router.render_for_path("/decisions/divisions/{chamber_slug}/{date}/{division_number}")
@router.render_for_path(
    "/decisions/divisions/{chamber_slug}/{date}/{division_number}.json"
)
async def decision_parameters():
    duck = await duck_core.child_query()

    query = """
    select house as chamber_slug,
           division_date as date,
           division_number
    from
        pw_division
    """

    for record in await duck.compile(query).records():
        await asyncio.sleep(0)
        yield record


@router.render_for_path("/decisions/divisions/{chamber_slug}/{year}")
@router.render_for_path("/decisions/divisions/{chamber_slug}/{year}.json")
async def chamber_parameters():
    duck = await duck_core.child_query()

    query = """
    select
        chamber_slug,
        earliest_year,
        latest_year
    from
        pw_chamber_division_span
    """
    for record in await duck.compile(query).records():
        for year_in_range in range(
            int(record["earliest_year"]), int(record["latest_year"]) + 1
        ):
            await asyncio.sleep(0)
            yield {
                "chamber_slug": record["chamber_slug"],
                "year": year_in_range,
            }
