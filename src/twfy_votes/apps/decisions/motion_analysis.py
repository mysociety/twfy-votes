from pathlib import Path
from typing import Any

import pandas as pd
import requests
import rich
import tqdm
from bs4 import BeautifulSoup
from pydantic import BaseModel

from ...helpers.data.models import StashableBase
from ...internal.db import duck_core
from ...internal.settings import settings
from .models import VoteMotionAnalysis, VoteType


def gids_from_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    assuming a df with column source_gid - return a list of gids
    minus the prefix stuff
    """
    gids = df["source_gid"].str.split("/", expand=True).drop(columns=[0])
    gids.columns = ["chamber", "gid"]
    if "division_date" in df.columns:
        gids["source"] = df["division_date"]
    gids = gids.sort_values(by=["gid"])
    return gids


async def get_new_gids(run_all: bool = False) -> list[str]:
    duck = await duck_core.child_query()

    query = """
    select * from policy_votes_with_id
    join pw_division using (division_id)
    """
    df = await duck.compile(query).df()
    gids = gids_from_df(df)
    gids = gids[gids["chamber"].isin(["debate"])]
    policy_gids = gids["gid"].to_list()

    # get all recent gids
    query = """
    select * from pw_division
    """
    df = await duck.compile(query).df()
    gids = gids_from_df(df)
    gids = gids[gids["chamber"].isin(["debate"])]
    gids = gids[gids["gid"].str[:4].astype(int) >= 2022]
    recent_gids = gids["gid"].to_list()

    # get all agreement gids
    query = """
    select * from pw_agreements
    """
    df = await duck.compile(query).df()
    gids = gids_from_df(df)
    gids = gids[gids["chamber"].isin(["debate"])]
    agreement_gids = gids["gid"].to_list()

    combined_gids = sorted(list(set(policy_gids + recent_gids + agreement_gids)))

    if run_all is False:
        collection = MotionCollection.from_path(
            Path("data", "processed", "motions.yaml")
        )
        existing_gids = set([m.gid for m in collection.items])
        combined_gids = [gid for gid in combined_gids if gid not in existing_gids]
    combined_gids = sorted(combined_gids)
    return combined_gids


async def update_motion_yaml(specific_gid: str | None = None, run_all: bool = False):
    collection = MotionCollection.from_path(Path("data", "processed", "motions.yaml"))
    if specific_gid:
        reduced_gids = [specific_gid]
        collection.items = [x for x in collection.items if x.gid != specific_gid]
    else:
        reduced_gids = await get_new_gids(run_all=run_all)

    twfy = TWFYMotionProcessor()
    # sort reduced_gids
    for gid in tqdm.tqdm(reduced_gids):
        try:
            result = twfy.get_motion(debate_type="commons", gid=gid).to_reduced()
            collection.items.append(result)
        except APIError as e:
            print(e)
            continue
        except Exception as e:
            print(f"Error with {gid}")
            raise e

    # sort collection.items by gid
    collection.items = sorted(collection.items, key=lambda x: x.gid)
    collection.to_path(Path("data", "processed", "motions.yaml"))


class APIError(Exception):
    pass


def all_present(search: str, items: list[str]) -> bool:
    """
    function that takes a search string, and a list of strings,
    and returns true if all are present
    """
    return all([x in search for x in items])


def any_present(search: str, items: list[str]) -> bool:
    """
    function that takes a search string, and a list of strings,
    and returns true if any are present
    """
    return any([x in search for x in items])


class SearchResponse(BaseModel):
    """
    Basic model to hold responses from TWFY API
    """

    epobject_id: str
    htype: str
    gid: str
    hpos: str
    section_id: str
    subsection_id: str
    hdate: str
    htime: str | None = None
    source_url: str
    major: str
    minor: str
    colnum: str
    body: str
    contentcount: None | str = None
    listurl: str
    commentsurl: str


class VoteSearch(BaseModel):
    vote: SearchResponse
    debate: SearchResponse


def catagorise_motion(motion: str) -> VoteType:
    l_motion = motion.lower()

    if all_present(l_motion, ["be approved", "laid before this house"]):
        return VoteType.APPROVE_STATUTORY_INSTRUMENT
    if all_present(l_motion, ["be revoked", "laid before this house"]):
        return VoteType.REVOKE_STATUTORY_INSTRUMENT
    elif any_present(l_motion, ["makes provision as set out in this order"]):
        return VoteType.TIMETABLE_CHANGE
    elif any_present(
        l_motion,
        ["following standing order be made", "orders be standing orders of the house"],
    ):
        return VoteType.STANDING_ORDER_CHANGE
    elif any_present(l_motion, ["first reading"]):
        return VoteType.FIRST_STAGE
    elif any_present(
        l_motion, ["second reading", "read a second time"]
    ) and any_present(l_motion, ["clause"]):
        return VoteType.SECOND_STAGE_COMMITTEE
    elif any_present(l_motion, ["clause stand part of the bill"]):
        return VoteType.COMMITEE_CLAUSE
    elif any_present(
        l_motion, ["third reading", "read a third time", "read the third time"]
    ):
        return VoteType.THIRD_STAGE
    elif any_present(l_motion, ["second reading", "read a second time"]):
        return VoteType.SECOND_STAGE
    elif all_present(l_motion, ["standing order", "23"]):
        return VoteType.TEN_MINUTE_RULE
    elif any_present(l_motion, ["do adjourn until"]):
        return VoteType.ADJOURNMENT
    elif any_present(
        l_motion,
        [
            "takes note of european union document",
            "takes note of european document",
            "takes note of draft european council decision",
        ],
    ) or all_present(
        l_motion, ["takes note of regulation", "of the european parliament"]
    ):
        return VoteType.EU_DOCUMENT_SCRUTINY
    elif all_present(l_motion, ["amendment", "lords"]):
        return VoteType.LORDS_AMENDMENT
    elif any_present(l_motion, ["gracious speech"]):
        return VoteType.GOVERNMENT_AGENDA
    elif any_present(l_motion, ["amendment", "clause be added to the bill"]):
        return VoteType.AMENDMENT
    elif any_present(l_motion, ["humble address be presented"]):
        return VoteType.HUMBLE_ADDRESS
    elif any_present(l_motion, ["that the house sit in private"]):
        return VoteType.PRIVATE_SITTING
    elif all_present(l_motion, ["confidence in", "government"]):
        return VoteType.CONFIDENCE
    else:
        return VoteType.OTHER


class VoteMotion(BaseModel):
    debate_type: str
    gid: str
    vote_type: VoteType
    question: str
    full_motion_speech: SearchResponse
    tidied_motion: str | None

    def to_reduced(self):
        return VoteMotionAnalysis(
            debate_type=self.debate_type,
            gid=self.gid,
            question=self.question,
            tidied_motion=self.tidied_motion,
            full_motion_speech=self.full_motion_speech.body,
            full_motion_gid=self.full_motion_speech.gid,
            vote_type=self.vote_type,
        )


class MotionCollection(StashableBase):
    items: list[VoteMotionAnalysis] = []


def get_item_in_isolation(body: str, trigger_terms: list[str]) -> str:
    soup = BeautifulSoup(body, "html.parser")
    ps = soup.find_all("p")
    text_content = [x.text for x in ps if any([y in x.text for y in trigger_terms])]
    return " ".join(text_content)


def get_motion_indent_in_isolation(body: str) -> str | None:
    soup = BeautifulSoup(body, "html.parser")
    search_phrase = ["I beg to move", "Moved by Lord", "Amendment proposed:"]

    ps = soup.find_all("p")
    motion_started = False
    for p in ps:
        # if motion_started and p has a class indent
        if motion_started:
            if "indent" in p.get("class", ""):
                return p.text
            else:
                return None
        if any([x in p.text for x in search_phrase]):
            motion_started = True


def revision_agnostic_gid(gid: str) -> str:
    # the revision is a,b,c after the iso date
    # we don't care about this for the purposes of comparisons

    # split by dot
    gid_parts = gid.split(".")
    if gid_parts[0][-1] in "abcd":
        gid_parts[0] = gid_parts[0][:-1]
    return ".".join(gid_parts)


class TWFYMotionProcessor:
    """
    Handler for fetching debates from the TheyWorkForYou API.
    """

    def __init__(self, api_key: str = settings.twfy_api_key, debug: bool = False):
        self.api_key = api_key
        self.debug = debug

    def fetch_debate(self, *, debate_type: str, gid: str):
        """
        Fetches a debate from the TheyWorkForYou API using the GID.
        This actually fetches a few things around a specific gid.
        Will return any speech parents of the gid, and the first layer of children.
        """
        url = "https://www.theyworkforyou.com/api/getDebates"
        params = {
            "key": self.api_key,
            "output": "json",
            "gid": gid,
            "type": debate_type,
        }

        response = requests.get(url, params=params)
        if response.status_code == 200:
            return [SearchResponse.model_validate(x) for x in response.json()]
        else:
            raise APIError(
                f"Invalid response code {response.status_code} from TheyWorkForYou API"
            )

    def fetch_vote_debate(self, *, debate_type: str, gid: str) -> VoteSearch:
        data = self.fetch_debate(debate_type=debate_type, gid=gid)
        if len(data) < 2:
            raise ValueError(f"Expected at least 2 items, got {len(data)}")
        vote_gids = [
            x
            for x in data
            if revision_agnostic_gid(x.gid) == revision_agnostic_gid(gid)
        ]
        debate_gids = [
            x
            for x in data
            if revision_agnostic_gid(x.gid) != revision_agnostic_gid(gid)
        ]
        if len(vote_gids) != 1:
            raise ValueError(f"Expected 1 vote, got {len(vote_gids)}")
        if len(debate_gids) < 1:
            raise ValueError(f"Expected at least debate, got {len(debate_gids)}")

        return VoteSearch(vote=vote_gids[0], debate=debate_gids[-1])

    def debug_print(self, message: Any):
        if self.debug:
            rich.print(message)

    def get_motion(self, *, debate_type: str, gid: str):
        """
        Different chambers are so different - let's just give in and
        do different parsers rather than trying to make one work for each.
        """
        match debate_type:
            case "commons":
                return self.get_commons_motion(debate_type=debate_type, gid=gid)
            case _:
                raise ValueError(f"Debate type {debate_type} not supported")

    def get_commons_motion(self, *, debate_type: str, gid: str):
        """
        Given the GID of a vote in a debate, returns the motion that was voted on.

        So the basic problem here is, we have a reference for a vote, but want to
        know what the vote was about.

        Generally, they are voting on a 'question' < but clues about what this is
        are spread in a few places.

        For many debates, the question is the motion raised in the first few speeches.
        Then at the end 'the question will be put'.

        So you fish out the motion, and associate that with the vote in the same debate.

        The problem is multiple votes in the same debate.

        For instance, when someone tries to amend the motion, you have a vote
        on the amendment, and then a vote on the motion.

        So you need to find the motion that is relevant to the vote.

        Sometimes, the question before the vote restates the amendment, sometimes it doesn't.

        The approach here is to identify all possible 'motions'.
        Then to look in the few speeches before the vote for the question.
        The question will contain a hint about if it's an amendment or other kind of vote.
        This can then help reconcile when we have multiple possible motions it could belong to.

        Generally (because of TWFY formatting issues), there are not more than two votes
        in a debate (there are in reality, but rogue headers usually split it up in longer amendments)
        """

        # First we get the vote and top level debate ids
        data = self.fetch_vote_debate(debate_type=debate_type, gid=gid)

        self.debug_print("Vote and debate data")
        self.debug_print(data)

        motion_phrases = ["I beg to move", "Moved by Lord", "Amendment proposed:"]
        question_phrases = [
            "Question put",
            "put the Question",
            "The House proceeded to a Division",
            "put forthwith the Question",
            "be approved",
            "do adjourn until",
            "Amendment proposed",
        ]

        # get all speeches associated with the debate
        self.debug_print(f"Fetching parent debate: {data.debate.gid}")
        debates = self.fetch_debate(debate_type=debate_type, gid=data.debate.gid)

        self.debug_print(f"There were {len(debates)} speeches in the debate")

        # find the index of the vote - and get the previous entry
        # allow this to be several previous - sometimes the speaker says something
        all_ids = [x.gid for x in debates]
        vote_index = all_ids.index(data.vote.gid)
        offset = 0
        question_debates = []
        while offset <= 3:
            # get the speech where someone actually puts the question
            end_debate_gid = debates[vote_index - offset].gid
            question_debates = [
                x
                for x in debates
                if any([y in x.body for y in question_phrases])
                and x.gid == end_debate_gid
            ]
            if len(question_debates) == 0:
                offset += 1
            else:
                break

        if len(question_debates) > 1:
            rich.print(debates)
            raise ValueError(f"Expected 1 question, got {len(question_debates)}")

        if len(question_debates) == 0:
            self.debug_print(
                "No question phrase found, using speech before vote instead."
            )
            # instead let's get the speech immediately before the vote
            # deferred divisions sometimes causes this
            question_debates = [debates[vote_index - 1]]

        # if possible from form of question, get vote type
        # this is the fall back if the motion detection is bad
        rel_question = get_item_in_isolation(question_debates[0].body, question_phrases)
        question_vote_type = catagorise_motion(rel_question)

        # find the speech that contains the motion
        motion_debates = [
            x for x in debates if any([y in x.body for y in motion_phrases])
        ]

        self.debug_print(f"There were {len(motion_debates)} speeches with motions")

        # now we need to limit down to the motion that is relevant if multiple
        # we do this by looking for coherence with the question_vote_type
        # this helps when we have a second reading and amendment in same debate
        rel_motions = []
        if len(motion_debates) > 1:
            motion_vote_type = None
            for m in motion_debates:
                rel_motion = get_item_in_isolation(m.body, motion_phrases)
                motion_vote_type = catagorise_motion(rel_motion)
                if motion_vote_type == question_vote_type:
                    self.debug_print(f"limiting to motion with type {motion_vote_type}")
                    rel_motions.append(m)
        else:
            rel_motions = motion_debates

        if len(rel_motions) > 1:
            # special casing for when we have multiple amendments
            # does the question also work as a motion
            self.debug_print("Multiple motions, trying question as motion")
            motion_text = get_item_in_isolation(
                question_debates[0].body, motion_phrases
            )
            if motion_text:
                self.debug_print("using question as motion")
                rel_motion = question_debates[0]
            else:
                # asssume the first motion is the one
                # this might be in error!
                rel_motion = rel_motions[0]
        elif len(rel_motions) == 0:
            rel_motion = question_debates[0]
        else:
            rel_motion = rel_motions[0]

        # It's nice to have a basic tidied up version of the motion
        # ... but this varies a lot depending on the type of motion
        # so a few different approaches
        # first, if the motion is nicely in an indent after the keyphrase
        tidied_motion = get_motion_indent_in_isolation(rel_motion.body)
        # next if the motion is short, just take the text
        if tidied_motion is None and len(rel_motion.body) < 1000:
            tidied_motion = BeautifulSoup(rel_motion.body, "html.parser").text
        if tidied_motion is None:
            # reverse order ps
            ps = BeautifulSoup(rel_motion.body, "html.parser").find_all("p")[::-1]
            pay_attention = False
            for p in ps:
                if pay_attention:
                    if "Amendment proposed" in p.text:
                        tidied_motion = p.text
                        break
                if "Question put" in p.text:
                    pay_attention = True
        # next if the motion is long, take the first paragraph (unless it's too short)
        if tidied_motion is None:
            soup = BeautifulSoup(rel_motion.body, "html.parser")
            # use the text of the first p tag
            if first_p := soup.find("p"):
                if len(first_p.text) > 50:
                    tidied_motion = first_p.text

        # based on the motion, we can get a vote type
        # ideally this should be the same as the question vote type
        motion_vote_type = catagorise_motion(rel_motion.body)
        if motion_vote_type == VoteType.OTHER:
            motion_vote_type = question_vote_type

        return VoteMotion(
            debate_type=debate_type,
            gid=gid,
            question=rel_question,
            full_motion_speech=rel_motion,
            tidied_motion=tidied_motion,
            vote_type=motion_vote_type,
        )
