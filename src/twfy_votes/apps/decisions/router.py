# Views

from ...helpers.static_fastapi.static import StaticAPIRouter
from ...internal.settings import settings
from ..core.dependencies import GetContext
from .dependencies import (
    GetAgreementAndVotes,
    GetChambersWithYearRange,
    GetDecisionListing,
    GetDivisionAndVotes,
    GetPeopleList,
    GetPersonAndRecords,
    GetPersonAndVotes,
)

router = StaticAPIRouter(template_directory=settings.template_dir)


@router.get_html("/")
@router.use_template("home.html")
async def home(context: GetContext):
    return context


@router.get("/people/{people_option}.json")
async def api_people(people: GetPeopleList) -> GetPeopleList:
    return people


@router.get_html("/people/{people_option}")
@router.use_template("people.html")
async def people(context: GetContext, people: GetPeopleList):
    context["people"] = people
    return context


@router.get_html("/person/{person_id}/votes")
@router.use_template("person_votes.html")
async def person_votes(context: GetContext, person_and_votes: GetPersonAndVotes):
    context["item"] = person_and_votes
    return context


@router.get("/person/{person_id}/votes.json")
async def api_person_votes(person_and_votes: GetPersonAndVotes) -> GetPersonAndVotes:
    return person_and_votes


@router.get_html("/decisions")
@router.use_template("decisions.html")
async def decisions(
    context: GetContext, chambers_with_year_range: GetChambersWithYearRange
):
    context["chambers"] = chambers_with_year_range
    return context


@router.get("/decisions.json")
async def api_decisions(
    chambers_with_year_range: GetChambersWithYearRange
) -> GetChambersWithYearRange:
    return chambers_with_year_range


@router.get("/decisions/division/{chamber_slug}/{date}/{division_number}.json")
async def api_division(division: GetDivisionAndVotes):
    return division


@router.get_html("/decisions/division/{chamber_slug}/{date}/{division_number}")
@router.use_template("division.html")
async def division(context: GetContext, division: GetDivisionAndVotes):
    context["item"] = division
    return context


@router.get("/decisions/agreement/{chamber_slug}/{date}/{decision_ref}.json")
async def api_agreement(agreement: GetAgreementAndVotes):
    return agreement


@router.get_html("/decisions/agreement/{chamber_slug}/{date}/{decision_ref}")
@router.use_template("agreement.html")
async def agreement(context: GetContext, agreement: GetAgreementAndVotes):
    context["item"] = agreement
    return context


@router.get("/decisions/{chamber_slug}/{year}/{month}.json")
@router.get("/decisions/{chamber_slug}/{year}.json")
async def api_decisions_list(division_list: GetDecisionListing) -> GetDecisionListing:
    return division_list


@router.get_html("/decisions/{chamber_slug}/{year}")
@router.use_template("division_list.html")
async def decisions_list(context: GetContext, division_list: GetDecisionListing):
    context["search"] = division_list
    return context


@router.get_html("/decisions/{chamber_slug}/{year}/{month}")
@router.use_template("division_list_month.html")
async def decisions_list_month(context: GetContext, division_list: GetDecisionListing):
    context["search"] = division_list
    return context


@router.get_html("/person/{person_id}/")
@router.use_template("person.html")
async def person(context: GetContext, person_and_parties: GetPersonAndRecords):
    context["item"] = person_and_parties
    return context


@router.get("/person/{person_id}.json")
async def api_person(person_and_parties: GetPersonAndRecords) -> GetPersonAndRecords:
    return person_and_parties
