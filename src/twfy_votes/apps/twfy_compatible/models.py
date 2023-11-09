from datetime import date

from pydantic import BaseModel
from twfy_votes.apps.decisions.models import Vote, VotePosition

# from ...helpers.data.models import StrEnum
from twfy_votes.helpers.data.models import StrEnum


class ValidOrganizationType(StrEnum):
    COMMONS = "uk.parliament.commons"
    LORDS = "uk.parliament.lords"
    SCOTTISH_PARLIAMENT = "scottish.parliament"


class PopoloVoteOption(StrEnum):
    AYE = "aye"
    TELLNO = "tellno"
    TELLAYE = "tellaye"
    NO = "no"
    BOTH = "both"
    ABSENT = "absent"


class PopoloVoteType(StrEnum):
    AYE = "aye"
    NO = "no"
    BOTH = "both"
    AYE3 = "aye3"
    NO3 = "no3"
    BOTH3 = "both3"


class PopoloDirection(StrEnum):
    MAJORITY_STRONG = "Majority (strong)"
    MAJORITY_WEAK = "Majority"
    MINORITY_STRONG = "minority (strong)"
    MINORITY_WEAK = "minority"
    ABSTAIN = "abstention"


class PopoloVoteCount(BaseModel):
    option: PopoloVoteOption
    value: int


class PopoloVote(BaseModel):
    id: str
    option: PopoloVoteOption

    @classmethod
    def from_vote(cls, vote: Vote):
        if vote.vote == VotePosition.ABSTENTION:
            option = PopoloVoteOption.BOTH
        else:
            option = PopoloVoteOption(vote.vote)

        return cls(
            id=f"uk.org.publicwhip/person/{vote.person.person_id}", option=option
        )


class VoteEvent(BaseModel):
    counts: list[PopoloVoteCount]
    votes: list[PopoloVote]


class PopoloMotion(BaseModel):
    id: str
    organization_id: str
    text: str
    date: date
    vote_events: list[VoteEvent]


class PopoloSource(BaseModel):
    url: str


class PopoloAspect(BaseModel):
    """
    Aspects are associated votes
    """

    source: str
    direction: PopoloDirection
    motion: PopoloMotion


class PopoloPolicy(BaseModel):
    title: str
    text: str
    sources: PopoloSource
    aspects: list[PopoloAspect]
