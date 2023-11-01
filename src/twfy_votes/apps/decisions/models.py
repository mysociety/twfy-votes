from __future__ import annotations

import datetime
from calendar import month_name
from itertools import groupby
from typing import Any, Literal

import pandas as pd
from bs4 import BeautifulSoup
from fastapi import Request
from pydantic import AliasChoices, Field, computed_field
from starlette.datastructures import URL

from ...helpers.data.models import ProjectBaseModel as BaseModel
from ...helpers.data.models import StrEnum
from ...helpers.data.style import UrlColumn, style_df
from ...internal.common import absolute_url_for
from ...internal.db import duck_core
from .queries import (
    ChamberDivisionsQuery,
    DivisionBreakDownQuery,
    DivisionQueryKeys,
    DivisionVotesQuery,
    GetAllPersonsQuery,
    GetPersonQuery,
    GovDivisionBreakDownQuery,
    PartyDivisionBreakDownQuery,
    PersonVotesQuery,
)


class AllowedChambers(StrEnum):
    COMMONS = "commons"
    LORDS = "lords"
    SCOTLAND = "scotland"
    WALES = "wales"
    NI = "ni"


class VotePosition(StrEnum):
    AYE = "aye"
    NO = "no"
    ABSTENTION = "abstention"
    ABSENT = "absent"
    TELLNO = "tellno"
    TELLAYE = "tellaye"


def aliases(*args: str) -> Any:
    return Field(..., validation_alias=AliasChoices(*args))


class GovernmentParties(BaseModel):
    chamber: list[str]
    party: list[str]
    start_date: datetime.date
    end_date: datetime.date


class Chamber(BaseModel):
    slug: AllowedChambers

    @computed_field
    @property
    def name(self) -> str:
        match self.slug:
            case "commons":
                return "House of Commons"
            case "lords":
                return "House of Lords"
            case "scotland":
                return "Scottish Parliament"
            case "wales":
                return "Senedd"
            case "ni":
                return "Northern Ireland Assembly"
            case _:
                raise ValueError(f"Invalid house slug {self.slug}")

    def twfy_debate_link(self, gid: str) -> str:
        link_format = "https://www.theyworkforyou.com/{debate_slug}/?id={gid}"
        match self.slug:
            case "commons":
                return link_format.format(debate_slug="debates", gid=gid)
            case "lords":
                return link_format.format(debate_slug="lords", gid=gid)
            case "scotland":
                return link_format.format(debate_slug="sp", gid=gid)
            case "wales":
                return link_format.format(debate_slug="senedd", gid=gid)
            case "ni":
                return link_format.format(debate_slug="ni", gid=gid)
            case _:
                raise ValueError(f"Invalid house slug {self.slug}")


class ChamberWithYearRange(BaseModel):
    chamber: Chamber
    earliest_year: int
    latest_year: int

    def year_range(self):
        r = range(self.earliest_year, self.latest_year + 1)
        # latest year first
        return reversed(r)

    @classmethod
    async def fetch_all(cls):
        duck = await duck_core.child_query()

        items = await duck.compile("select * from pw_chamber_division_span").records()

        chambers = [
            ChamberWithYearRange(
                chamber=Chamber(slug=item["chamber_slug"]),
                earliest_year=item["earliest_year"],
                latest_year=item["latest_year"],
            )
            for item in items
        ]

        return chambers


class Person(BaseModel):
    person_id: int
    first_name: str = aliases("first_name", "given_name")
    last_name: str
    nice_name: str
    party: str | None = None

    def votes_url(self, request: Request) -> URL:
        return absolute_url_for(request, "person_votes", person_id=self.person_id)

    @computed_field
    @property
    def name(self) -> str:
        if self.party:
            return f"{self.first_name} {self.last_name} ({self.party})"
        return f"{self.first_name} {self.last_name}"

    @classmethod
    async def from_id(cls, person_id: int) -> Person:
        duck = await duck_core.child_query()
        return await GetPersonQuery(person_id=person_id).to_model_single(
            model=Person, duck=duck
        )

    @classmethod
    async def fetch_all(cls) -> list[Person]:
        duck = await duck_core.child_query()
        return await GetAllPersonsQuery().to_model_list(model=Person, duck=duck)


class Vote(BaseModel):
    person: Person
    membership_id: int
    division: DivisionInfo | None = None
    vote: VotePosition
    diff_from_party_average: float

    @computed_field
    @property
    def vote_desc(self) -> str:
        match self.vote:
            case VotePosition.AYE:
                return "With motion"
            case VotePosition.NO:
                return "Against motion"
            case VotePosition.ABSTENTION:
                return "Abstention"
            case VotePosition.ABSENT:
                return "Absent"
            case VotePosition.TELLNO:
                return "Against motion (Teller)"
            case VotePosition.TELLAYE:
                return "With motion (Teller)"
            case _:  # type: ignore
                raise ValueError(f"Invalid vote position {self.vote}")

    @computed_field
    @property
    def value(self) -> int:
        match self.vote:
            case VotePosition.AYE | VotePosition.TELLAYE:
                return 1
            case VotePosition.NO | VotePosition.TELLNO:
                return -1
            case VotePosition.ABSTENTION | VotePosition.ABSENT:
                return 0
            case _:  # type: ignore
                raise ValueError(f"Invalid vote position {self.vote}")

    def majority_desc(self, result: Literal["passed", "rejected"]) -> str:
        if self.value == 1 and result == "passed":
            return "Majority"
        if self.value == 1 and result == "rejected":
            return "Minority"
        if self.value == -1 and result == "passed":
            return "Minority"
        if self.value == -1 and result == "rejected":
            return "Majority"
        return "N/A"


class PartialAgreement(BaseModel):
    chamber_slug: AllowedChambers
    date: datetime.date = aliases("date", "division_date")
    decision_ref: str  # Anticpated this will be the ref style used in TWFY to refer to the speech containing the agreement minus the date e.g. "a.974.1#g991.0"

    @computed_field
    @property
    def key(self) -> str:
        return f"{self.chamber_slug}-{self.date.isoformat()}-{self.decision_ref}"


class PartialDivision(BaseModel):
    """
    Base instance of the properties needed to identify a division
    """

    chamber_slug: AllowedChambers
    date: datetime.date = aliases("date", "division_date")
    division_number: int

    @computed_field
    @property
    def key(self) -> str:
        return f"{self.chamber_slug}-{self.date.isoformat()}-{self.division_number}"


class AgreementInfo(BaseModel):
    key: str = aliases("key", "agreement_key")
    house: Chamber
    date: datetime.date = aliases("date", "division_date")
    decision_ref: str
    division_name: str
    motion: str

    def url(self, request: Request):
        return ""

    @classmethod
    async def from_partials(
        cls, partials: list[PartialAgreement]
    ) -> list[AgreementInfo]:
        return []


class DivisionInfo(BaseModel):
    key: str = aliases("key", "division_key")
    chamber: Chamber
    date: datetime.date = aliases("date", "division_date")
    division_id: int
    division_number: int
    division_name: str
    source_url: str
    motion: str
    debate_url: str
    source_gid: str
    debate_gid: str
    clock_time: str | None = None

    @computed_field
    @property
    def twfy_link(self) -> str:
        gid = self.source_gid.split("/")[-1]
        return self.chamber.twfy_debate_link(gid)

    def safe_motion(self) -> str:
        return self.motion

    def motion_twfy_link(self) -> str | None:
        soup = BeautifulSoup(self.motion, "html.parser")
        pid = None
        for p in soup.find_all("p"):
            # get the gid property from the ptag

            pid = p.get("pid", None)
            if pid:
                continue
        if not pid:
            return None

        pid = pid.split("/")[0]
        # insert a . between the first two characters
        pid = pid[:1] + "." + pid[1:]
        gid = self.date.isoformat() + pid
        return self.chamber.twfy_debate_link(gid)

    def url(self, request: Request):
        return absolute_url_for(
            request,
            "division",
            chamber_slug=self.chamber.slug,
            date=self.date,
            division_number=self.division_number,
        )

    @classmethod
    async def from_partials(cls, partials: list[PartialDivision]) -> list[DivisionInfo]:
        duck = await duck_core.child_query()
        if not partials:
            return []
        return await DivisionQueryKeys(keys=[x.key for x in partials]).to_model_list(
            model=DivisionInfo, duck=duck
        )

    @classmethod
    async def from_partial(cls, partial: PartialDivision) -> DivisionInfo:
        """
        Elevate to a division.
        """
        duck = await duck_core.child_query()
        return await DivisionQueryKeys(keys=[partial.key]).to_model_single(
            model=DivisionInfo, duck=duck
        )


class DivisionListing(BaseModel):
    chamber: Chamber
    start_date: datetime.date
    end_date: datetime.date
    divisions: list[DivisionInfo]

    def grouped_divisions(self):
        divisions = self.divisions
        divisions.sort(key=lambda x: x.date, reverse=True)
        for month_id, divs in groupby(divisions, lambda x: x.date.month):
            yield month_name[month_id], list(divs)

    @classmethod
    async def from_chamber_year(cls, chamber: Chamber, year: int) -> DivisionListing:
        start_date = datetime.date(year, 1, 1)
        end_date = datetime.date(year, 12, 31)
        return await cls.from_chamber(chamber, start_date, end_date)

    @classmethod
    async def from_chamber(
        cls,
        chamber: Chamber,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> DivisionListing:
        duck = await duck_core.child_query()
        divisions = await ChamberDivisionsQuery(
            start_date=start_date,
            end_date=end_date,
            chamber_slug=chamber.slug,
        ).to_model_list(model=DivisionInfo, duck=duck)

        return DivisionListing(
            chamber=chamber,
            start_date=start_date,
            end_date=end_date,
            divisions=divisions,
        )


class DivisionBreakdown(BaseModel):
    grouping: str | None = None
    vote_participant_count: int
    for_motion: int
    against_motion: int
    neutral_motion: int
    signed_votes: int
    motion_majority: int
    motion_majority_ratio: float
    motion_result_int: int


class PersonAndVotes(BaseModel):
    person: Person
    votes: list[Vote]

    @classmethod
    async def from_person(cls, person: Person) -> PersonAndVotes:
        duck = await duck_core.child_query()
        votes = await PersonVotesQuery(
            person_id=person.person_id,
        ).to_model_list(
            duck=duck, model=Vote, validate=DivisionVotesQuery.validate.NOT_ZERO
        )

        return PersonAndVotes(person=person, votes=votes)

    def votes_df(self, request: Request) -> str:
        data = [
            {
                "Date": v.division.date,
                "Division": UrlColumn(
                    url=v.division.url(request), text=v.division.division_name
                ),
                "Vote": v.vote_desc,
                "Party alignment": 1 - v.diff_from_party_average,
            }
            for v in self.votes
            if v.division is not None
        ]

        if len(data) != len(self.votes):
            raise ValueError("Some votes have no division associated with them")
        df = pd.DataFrame(data=data)
        return style_df(df, percentage_columns=["Party alignment"])


class DivisionAndVotes(BaseModel):
    house: Chamber
    date: datetime.date = aliases("date", "division_date")
    division_number: int
    details: DivisionInfo
    overall_breakdown: DivisionBreakdown
    party_breakdowns: list[DivisionBreakdown]
    gov_breakdowns: list[DivisionBreakdown]
    votes: list[Vote]

    @classmethod
    async def from_division(cls, division: DivisionInfo) -> DivisionAndVotes:
        duck = await duck_core.child_query()

        # Get people's votes associated with this division
        votes = await DivisionVotesQuery(
            division_date=division.date,
            division_number=division.division_number,
            chamber_slug=division.chamber.slug,
        ).to_model_list(
            duck=duck, model=Vote, validate=DivisionVotesQuery.validate.NOT_ZERO
        )

        # calculate the overall breakdown of how people voted and the result
        overall_breakdown = await DivisionBreakDownQuery(
            division_id=division.division_id
        ).to_model_single(
            duck=duck,
            model=DivisionBreakdown,
        )

        # do the same by party
        party_breakdowns = await PartyDivisionBreakDownQuery(
            division_id=division.division_id
        ).to_model_list(
            duck=duck,
            model=DivisionBreakdown,
            validate=PartyDivisionBreakDownQuery.validate.NOT_ZERO,
        )

        # get breakdown by gov/other
        gov_breakdowns = await GovDivisionBreakDownQuery(
            division_id=division.division_id
        ).to_model_list(
            duck=duck,
            model=DivisionBreakdown,
            validate=PartyDivisionBreakDownQuery.validate.NOT_ZERO,
        )

        return DivisionAndVotes(
            house=division.chamber,
            date=division.date,
            division_number=division.division_number,
            details=division,
            votes=votes,
            overall_breakdown=overall_breakdown,
            party_breakdowns=party_breakdowns,
            gov_breakdowns=gov_breakdowns,
        )

    def party_breakdown_df(self) -> str:
        all_breakdowns = [dict(x) for x in self.party_breakdowns]
        df = pd.DataFrame(data=all_breakdowns)
        banned_columns = ["signed_votes", "motion_majority", "motion_result_int"]
        df = df.drop(columns=banned_columns)  # type: ignore

        return style_df(df, percentage_columns=["motion majority ratio"])

    def gov_breakdown_df(self) -> str:
        self.overall_breakdown.grouping = "All MPs"

        all_breakdowns = [self.overall_breakdown]
        all_breakdowns += self.gov_breakdowns

        all_breakdowns = [dict(x) for x in all_breakdowns]
        df = pd.DataFrame(data=all_breakdowns)
        banned_columns = ["signed_votes", "motion_majority", "motion_result_int"]
        df = df.drop(columns=banned_columns)  # type: ignore

        return style_df(df, percentage_columns=["motion majority ratio"])

    def votes_df(self, request: Request) -> str:
        data = [
            {
                "Person": UrlColumn(
                    url=v.person.votes_url(request), text=v.person.nice_name
                ),
                "Party": v.person.party,
                "Vote": v.vote_desc,
                "Party alignment": 1 - v.diff_from_party_average,
            }
            for v in self.votes
        ]

        df = pd.DataFrame(data=data)
        return style_df(df, percentage_columns=["Party alignment"])
