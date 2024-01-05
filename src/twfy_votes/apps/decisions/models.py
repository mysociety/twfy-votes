from __future__ import annotations

import datetime
from calendar import month_name
from itertools import groupby
from operator import attrgetter
from typing import Any, Literal, TypeVar

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
from ..policies.queries import GetPersonParties
from .analysis import is_nonaction_vote
from .queries import (
    ChamberDivisionsQuery,
    DivisionBreakDownQuery,
    DivisionIdsVotesQuery,
    DivisionQueryKeys,
    DivisionVotesQuery,
    GetAllPersonsQuery,
    GetCurrentPeopleQuery,
    GetPersonQuery,
    GovDivisionBreakDownQuery,
    MotionQuery,
    PartyDivisionBreakDownQuery,
    PersonVotesQuery,
)

T = TypeVar("T")


def dataframe_to_dict_records(df: pd.DataFrame) -> list[dict[str, Any]]:
    """
    Convert a DataFrame into a list of dictionaries.

    This is a dumber but faster approach than then to_dict method.
    Because we know the columns are basic types, we can just iterate over the rows
    """
    cols = list(df)
    col_arr_map = {col: df[col].astype(object).to_numpy() for col in cols}
    records = []
    for i in range(len(df)):
        record = {col: col_arr_map[col][i] for col in cols}
        records.append(record)

    return records


def group_by_key(data_list: list[T], key: str) -> dict[str | int, list[T]]:
    # Function to create a dictionary from a list using a key
    key_func = attrgetter(key)
    return {
        k: list(g) for k, g in groupby(sorted(data_list, key=key_func), key=key_func)
    }


def aliases(*args: str) -> Any:
    return Field(..., validation_alias=AliasChoices(*args))


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


class VoteType(StrEnum):
    """
    Enum for different types of parlimentary vote.
    Not all of these are formal descriptions.
    Converging on 'stages' rather than readings across Parliament.

    """

    AMENDMENT = "amendment"
    TEN_MINUTE_RULE = "ten_minute_rule"
    LORDS_AMENDMENT = (
        "lords_amendment"  # not an amendment in the lords, but commons responding to it
    )
    FIRST_STAGE = "first_stage"
    SECOND_STAGE = "second_stage"
    COMMITEE_CLAUSE = "committee_clause"
    SECOND_STAGE_COMMITTEE = (
        "second_stage_committee"  # approval of clauses in committee
    )
    THIRD_STAGE = "third_stage"
    APPROVE_STATUTORY_INSTRUMENT = "approve_statutory_instrument"
    ADJOURNMENT = "adjournment"
    TIMETABLE_CHANGE = (
        "timetable_change"  # tracking motions that take control of the order paper
    )
    HUMBLE_ADDRESS = "humble_address"
    GOVERNMENT_AGENDA = "government_agenda"  # monarch's speech etc
    CONFIDENCE = "confidence"
    STANDING_ORDER_CHANGE = "standing_order_change"
    PRIVATE_SITTING = "private_sitting"
    OTHER = "other"

    def display_name(self):
        return self.replace("_", " ").title()


class VoteMotionAnalysis(BaseModel):
    debate_type: str
    gid: str
    question: str
    tidied_motion: str | None = None
    full_motion_speech: str
    full_motion_gid: str | None = None
    vote_type: VoteType

    def twfy_motion_url(self):
        # hardcode to commons votes for now
        link_format = "https://www.theyworkforyou.com/{debate_slug}/?id={gid}"
        return link_format.format(debate_slug="debates", gid=self.full_motion_gid)

    def motion_uses_powers(self):
        """
        We only need to do vote analysis for votes that aren't inherently using powers based on
        classification further up.
        """

        if self.vote_type in [
            VoteType.ADJOURNMENT,
            VoteType.OTHER,
            VoteType.GOVERNMENT_AGENDA,
        ]:
            non_action = is_nonaction_vote(self.full_motion_speech)
            return not non_action
        else:
            return True


class ManualMotion(BaseModel):
    chamber: AllowedChambers
    division_date: datetime.date
    division_number: int
    manual_motion: str


class GovernmentParties(BaseModel):
    chamber: list[str]
    party: list[str]
    start_date: datetime.date
    end_date: datetime.date


class Chamber(BaseModel):
    slug: AllowedChambers

    @computed_field
    @property
    def member_name(self) -> str:
        match self.slug:
            case "commons":
                return "MPs"
            case "lords":
                return "Lords"
            case "scotland":
                return "MSPs"
            case "wales":
                return "MSs"
            case "ni":
                return "AMs"
            case _:
                raise ValueError(f"Invalid house slug {self.slug}")

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
        person_objects = await GetPersonQuery(person_id=person_id).to_model_list(
            model=cls, validate=GetPersonQuery.validate.NOT_ZERO, duck=duck
        )
        return person_objects[0]

    @classmethod
    async def fetch_group(cls, option: Literal["all", "current"]) -> list[Person]:
        duck = await duck_core.child_query()
        if option == "all":
            models = await GetAllPersonsQuery().to_model_list(model=Person, duck=duck)
        elif option == "current":
            models = await GetCurrentPeopleQuery().to_model_list(
                model=Person, duck=duck
            )
        else:
            raise ValueError(f"Invalid option {option}")
        # make unique on person_id
        unique_models = {x.person_id: x for x in models}
        return list(unique_models.values())

    @classmethod
    async def fetch_all(cls) -> list[Person]:
        duck = await duck_core.child_query()
        models = await GetAllPersonsQuery().to_model_list(model=Person, duck=duck)
        # make unique on person_id
        unique_models = {x.person_id: x for x in models}
        return list(unique_models.values())


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


class VoteWithDivisionID(Vote):
    """
    Small helper object when fetch votes
    associated with multiple divisions
    """

    division_id: int


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
    voting_cluster: str = ""

    def url(self, request: Request):
        return ""

    def motion_uses_powers(self):
        return PowersAnalysis.INSUFFICENT_INFO

    @classmethod
    async def from_partials(
        cls, partials: list[PartialAgreement]
    ) -> list[AgreementInfo]:
        return []


class PowersAnalysis(StrEnum):
    USES_POWERS = "uses_powers"
    DOES_NOT_USE_POWERS = "does_not_use_powers"
    INSUFFICENT_INFO = "insufficent_info"

    def display_name(self):
        return self.value.replace("_", " ").title()


class DivisionInfo(BaseModel):
    key: str = aliases("key", "division_key")
    chamber: Chamber
    date: datetime.date = aliases("date", "division_date")
    division_id: int
    division_number: int
    division_name: str
    source_url: str
    motion: str
    manual_motion: str
    debate_url: str
    source_gid: str
    debate_gid: str
    clock_time: str | None = None
    voting_cluster: str | None = None
    vote_motion_analysis: VoteMotionAnalysis | None = None

    @computed_field
    @property
    def twfy_link(self) -> str:
        gid = self.source_gid.split("/")[-1]
        return self.chamber.twfy_debate_link(gid)

    def cluster_desc(self):
        match self.voting_cluster:
            case "Gov rejects, strong opp (M)":
                return "Government rejects, strong opposition (Fuzzy)"
            case "Gov proposes, strong opp":
                return "Government proposes, strong opposition"
            case "Opp proposes, low participation":
                return "Opposition proposes, low participation"
            case "Gov rejects, weak opp":
                return "Government rejects, weak opposition"
            case "Gov proposes, weak opp":
                return "Government proposes, weak opposition"
            case "Low participation":
                return "Low participation"
            case "Gov rejects, strong opp (H)":
                return "Government rejects, strong opposition (Clear)"
            case "Bipartisan support":
                return "Bipartisan support"
            case _:
                return self.voting_cluster

    def poor_quality_motion_text(self) -> bool:
        """
        Track based on motion size if we have good info in the database
        """
        text = self.motion_text_only()
        num_of_words = len(text.split(" "))
        if num_of_words < 20:
            return True
        return False

    def vote_type(self):
        if self.vote_motion_analysis:
            return VoteType(self.vote_motion_analysis.vote_type).display_name()
        return "Unknown"

    def motion_uses_powers(self) -> PowersAnalysis:
        """
        If either the motion or the manual motion triggers the non-action vote criteria.
        We're also now deferring to the motion calculated via the twfy wiki.
        At some point, we want to drop motion and manual motion from the main table.

        """
        result = PowersAnalysis.DOES_NOT_USE_POWERS
        if self.vote_motion_analysis:
            if self.vote_motion_analysis.motion_uses_powers():
                result = PowersAnalysis.USES_POWERS

            if self.vote_motion_analysis.vote_type == VoteType.AMENDMENT:
                poor_quality_motion = self.poor_quality_motion_text()
                if self.manual_motion and not poor_quality_motion:
                    if is_nonaction_vote(self.motion):
                        result = PowersAnalysis.DOES_NOT_USE_POWERS
                    else:
                        result = PowersAnalysis.USES_POWERS

            return result

        if self.manual_motion or self.motion:
            motion_based_nonaction = is_nonaction_vote(self.motion)
            manual_motion_based_nonaction = is_nonaction_vote(self.manual_motion)

            poor_quality_motion = self.poor_quality_motion_text()

            if poor_quality_motion and self.manual_motion:
                # if we have a poor quality motion and we have a manual motion, use that alone
                if manual_motion_based_nonaction:
                    return PowersAnalysis.DOES_NOT_USE_POWERS
                else:
                    return PowersAnalysis.USES_POWERS
            elif poor_quality_motion and not self.manual_motion:
                # if we have a poor quality motion and no manual motion, we assume a negative response (has powers)
                # is false and return insufficent info
                if motion_based_nonaction:
                    return PowersAnalysis.DOES_NOT_USE_POWERS
                else:
                    return PowersAnalysis.INSUFFICENT_INFO
            else:
                # we have both a good quality motion and a manual motion, if either one says non action, trust that
                if motion_based_nonaction | manual_motion_based_nonaction:
                    return PowersAnalysis.DOES_NOT_USE_POWERS
                else:
                    return PowersAnalysis.USES_POWERS

        return PowersAnalysis.INSUFFICENT_INFO

    def motion_text_only(self) -> str:
        soup = BeautifulSoup(self.motion, "html.parser")
        return soup.get_text()

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
        pid = pid.replace("..", ".")  # resolve inconsistent formats
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
    async def upgrade_with_motions(
        cls, items: list[DivisionInfo]
    ) -> list[DivisionInfo]:
        duck = await duck_core.child_query()
        # at this point we only have special info for commons divisions
        division_gids = [
            x.source_gid.split("/")[-1] for x in items if x.chamber.slug == "commons"
        ]
        if len(division_gids) > 0:
            # get the motion info
            motions = await MotionQuery(gids=division_gids).to_model_list(
                model=VoteMotionAnalysis, duck=duck, nan_to_none=True
            )
            # make lookup based on gid
            motion_lookup = {x.gid: x for x in motions}
        else:
            motion_lookup = {}

        # add the motion info to the division info
        for item in items:
            item.vote_motion_analysis = motion_lookup.get(
                item.source_gid.split("/")[-1], None
            )

        return items

    @classmethod
    async def from_partials(cls, partials: list[PartialDivision]) -> list[DivisionInfo]:
        duck = await duck_core.child_query()
        if len(partials) == 0:
            items = []
        else:
            items = await DivisionQueryKeys(
                keys=[x.key for x in partials]
            ).to_model_list(model=DivisionInfo, duck=duck)

        items = await cls.upgrade_with_motions(items)

        return items

    @classmethod
    async def from_partial(cls, partial: PartialDivision) -> DivisionInfo:
        """
        Elevate to a division.
        """
        items = await cls.from_partials([partial])
        if len(items) != 1:
            raise ValueError("Expected one division to be returned")
        return items[0]


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

    def division_df(self, request: Request):
        data = [
            {
                "Date": d.date,
                "Division": UrlColumn(url=d.url(request), text=d.division_name),
                "Vote Type": d.vote_type(),
                "Powers": d.motion_uses_powers().display_name(),
                "Voting Cluster": d.voting_cluster,
            }
            for d in self.divisions
        ]

        df = pd.DataFrame(data=data)
        return style_df(df)

    @classmethod
    async def from_chamber_year(cls, chamber: Chamber, year: int) -> DivisionListing:
        start_date = datetime.date(year, 1, 1)
        end_date = datetime.date(year, 12, 31)
        return await cls.from_chamber(chamber, start_date, end_date)

    @classmethod
    async def from_chamber_year_month(
        cls, chamber: Chamber, year: int, month: int
    ) -> DivisionListing:
        start_date = datetime.date(year, month, 1)
        next_month = month + 1
        if next_month > 12:
            next_month = 1
            year += 1
        end_date = datetime.date(year, next_month, 1) - datetime.timedelta(days=1)
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

        divisions = await DivisionInfo.upgrade_with_motions(divisions)

        return DivisionListing(
            chamber=chamber,
            start_date=start_date,
            end_date=end_date,
            divisions=divisions,
        )


class DivisionBreakdown(BaseModel):
    division_id: int
    grouping: str | None = None
    vote_participant_count: int
    for_motion: int
    against_motion: int
    neutral_motion: int
    signed_votes: int
    motion_majority: int
    motion_majority_ratio: float
    motion_result_int: int
    total_possible_members: int


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
    chamber: Chamber
    date: datetime.date = aliases("date", "division_date")
    division_number: int
    details: DivisionInfo
    overall_breakdown: DivisionBreakdown
    party_breakdowns: list[DivisionBreakdown]
    gov_breakdowns: list[DivisionBreakdown]
    votes: list[Vote]

    @classmethod
    async def from_divisions(
        cls,
        divisions: list[DivisionInfo],
        overall_breakdown_only: bool = False,
    ) -> list[DivisionAndVotes]:
        """
        Fetch multiple connected sets of divisions, votes, and breakdowns
        """
        duck = await duck_core.child_query()
        # get the division_ids
        division_ids = [d.division_id for d in divisions]

        """
        # This is a more direct method - but because we can end up with quite a lot of votes
        # method below is about twice as quick
        votes = await DivisionIdsVotesQuery(division_ids=division_ids).to_model_list(
            duck=duck,
            model=VoteWithDivisionID,
            validate=DivisionIdsVotesQuery.validate.NOT_ZERO,
        )
        """

        # get df of results
        df = await DivisionIdsVotesQuery(division_ids=division_ids).compile(duck).df()

        # create person objects first
        # limit df to just columns that start person__ and remove that prefix from the column names
        person_df = df.filter(regex="^person__").rename(columns=lambda x: x[8:])
        person_df["membership_id"] = df["membership_id"].astype(int)
        person_df = person_df.drop_duplicates()
        person_records = dataframe_to_dict_records(person_df)
        person_lookup = {
            x["membership_id"]: Person.model_validate(x) for x in person_records
        }

        # create vote objects
        df["person"] = df["membership_id"].astype(int).map(person_lookup)
        # drop all the columns that start with person__
        votes_df = df.drop(columns=df.filter(regex="^person__").columns)

        # create votes from dataframe
        votes = [
            VoteWithDivisionID.model_validate(x)
            for x in dataframe_to_dict_records(votes_df)
        ]

        # calculate the overall breakdown of how people voted and the result
        overall_breakdowns = await DivisionBreakDownQuery(
            division_ids=division_ids
        ).to_model_list(
            duck=duck,
            model=DivisionBreakdown,
            validate=DivisionBreakDownQuery.validate.NOT_ZERO,
        )

        if overall_breakdown_only:
            party_breakdowns = []
            gov_breakdowns = []
        else:
            # do the same by party
            party_breakdowns = await PartyDivisionBreakDownQuery(
                division_ids=division_ids
            ).to_model_list(
                duck=duck,
                model=DivisionBreakdown,
                validate=PartyDivisionBreakDownQuery.validate.NOT_ZERO,
            )

            # get breakdown by gov/other
            gov_breakdowns = await GovDivisionBreakDownQuery(
                division_ids=division_ids
            ).to_model_list(
                duck=duck,
                model=DivisionBreakdown,
                validate=PartyDivisionBreakDownQuery.validate.NOT_ZERO,
            )

        # Assembly a list of DivisionAndVotes objects based on the division_ids in all the above objects

        division_and_votes = []
        div_breakdown_lookup = group_by_key(overall_breakdowns, "division_id")
        div_party_breakdown_lookup = group_by_key(party_breakdowns, "division_id")
        div_gov_breakdown_lookup = group_by_key(gov_breakdowns, "division_id")
        votes_lookup = group_by_key(votes, "division_id")

        for division in divisions:
            div_votes = votes_lookup.get(division.division_id, [])
            div_breakdown = div_breakdown_lookup[division.division_id][0]
            div_party_breakdown = div_party_breakdown_lookup.get(
                division.division_id, []
            )
            div_gov_breakdown = div_gov_breakdown_lookup.get(division.division_id, [])

            division_and_votes.append(
                DivisionAndVotes(
                    chamber=division.chamber,
                    date=division.date,
                    division_number=division.division_number,
                    details=division,
                    votes=[v for v in div_votes],
                    overall_breakdown=div_breakdown,
                    party_breakdowns=div_party_breakdown,
                    gov_breakdowns=div_gov_breakdown,
                )
            )

        return division_and_votes

    @classmethod
    async def from_division(cls, division: DivisionInfo) -> DivisionAndVotes:
        divisions = await cls.from_divisions([division])
        if len(divisions) != 1:
            raise ValueError("Expected one division to be returned")

        return divisions[0]

    def party_breakdown_df(self) -> str:
        all_breakdowns = [dict(x) for x in self.party_breakdowns]
        df = pd.DataFrame(data=all_breakdowns)
        banned_columns = [
            "signed_votes",
            "motion_majority",
            "motion_result_int",
            "total_possible_members",
        ]
        df = df.drop(columns=banned_columns)

        return style_df(df, percentage_columns=["motion majority ratio"])

    def gov_breakdown_df(self) -> str:
        self.overall_breakdown.grouping = f"All {self.chamber.member_name}"

        all_breakdowns = [self.overall_breakdown]
        all_breakdowns += self.gov_breakdowns

        all_breakdowns = [dict(x) for x in all_breakdowns]
        df = pd.DataFrame(data=all_breakdowns)
        banned_columns = [
            "signed_votes",
            "motion_majority",
            "motion_result_int",
            "total_possible_members",
        ]
        df = df.drop(columns=banned_columns)

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


class VotingRecordLink(BaseModel):
    chamber: Chamber
    party: str


class PersonAndRecords(BaseModel):
    person: Person
    records: list[VotingRecordLink]

    @classmethod
    async def from_person(cls, person: Person):
        """
        Given a person ID get a list of the possible comparison party and chambers
        """

        duck = await duck_core.child_query()

        # broaden for any chambers that the person has been a member of if we add more voting records
        # using this version of the app
        allowed_chambers = [AllowedChambers.COMMONS]
        parties = []
        voting_record_links = []
        for a in allowed_chambers:
            party_df = (
                await GetPersonParties(chamber_slug=a, person_id=person.person_id)
                .compile(duck)
                .df()
            )
            parties = party_df["party"].tolist()
            for p in parties:
                voting_record_links.append(
                    VotingRecordLink(chamber=Chamber(slug=a), party=p)
                )

        return PersonAndRecords(person=person, records=voting_record_links)
