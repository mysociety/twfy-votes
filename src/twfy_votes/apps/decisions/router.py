# Views

from ...helpers.static_fastapi.static import StaticAPIRouter
from ...internal.settings import settings
from ..core.dependencies import GetContext
from .dependencies import (
    GetChambersWithYearRange,
    GetDivisionAndVotes,
    GetDivisionListing,
)

router = StaticAPIRouter(template_directory=settings.template_dir)


@router.get_html("/")
@router.use_template("home.html")
async def home(context: GetContext):
    return context


@router.get_html("/decisions/")
@router.use_template("decisions.html")
async def decisions(
    context: GetContext, chambers_with_year_range: GetChambersWithYearRange
):
    context["chambers"] = chambers_with_year_range
    return context


@router.get("/decisions.json")
async def api_decisions(chambers_with_year_range: GetChambersWithYearRange):
    return chambers_with_year_range


@router.get("/decisions/division/{chamber_slug}/{date}/{division_number}.json")
async def api_division(division: GetDivisionAndVotes):
    return division


@router.get_html("/decisions/division/{chamber_slug}/{date}/{division_number}")
@router.use_template("division.html")
async def division(context: GetContext, division: GetDivisionAndVotes):
    context["item"] = division
    return context


@router.get("/decisions/divisions/{chamber_slug}/{year}.json")
async def api_divisions_list(division_list: GetDivisionListing):
    return division_list


@router.get_html("/decisions/divisions/{chamber_slug}/{year}/")
@router.use_template("division_list.html")
async def divisions_list(context: GetContext, division_list: GetDivisionListing):
    context["search"] = division_list
    return context
