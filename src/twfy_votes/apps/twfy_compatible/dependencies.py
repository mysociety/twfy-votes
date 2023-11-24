"""
Helper functions for bridging from our data to the current popolo output of public whip.
"""

from ...helpers.static_fastapi.dependencies import dependency_alias_for
from ..decisions.models import (
    AllowedChambers,
    Chamber,
    DivisionAndVotes,
    DivisionBreakdown,
    DivisionInfo,
)
from ..policies.models import (
    PersonPolicyLink,
    Policy,
    PolicyDirection,
    PolicyStrength,
    ReducedPersonPolicyLink,
)
from . import models as m


def pw_style_motion_description(
    motion_passed: int, direction: PolicyDirection, strength: PolicyStrength
) -> m.PopoloDirection:
    if motion_passed:
        # aye is the same as majority
        # no is the same as minority
        if direction == PolicyDirection.AGREE:
            if strength == PolicyStrength.STRONG:
                return m.PopoloDirection.MAJORITY_STRONG
            elif strength == PolicyStrength.WEAK:
                return m.PopoloDirection.MAJORITY_WEAK
        elif direction == PolicyDirection.AGAINST:
            if strength == PolicyStrength.STRONG:
                return m.PopoloDirection.MINORITY_STRONG
            elif strength == PolicyStrength.WEAK:
                return m.PopoloDirection.MINORITY_WEAK
    else:
        # aye is the same as minority
        # no is the same as majority
        if direction == PolicyDirection.AGREE:
            if strength == PolicyStrength.STRONG:
                return m.PopoloDirection.MINORITY_STRONG
            elif strength == PolicyStrength.WEAK:
                return m.PopoloDirection.MINORITY_WEAK
        elif direction == PolicyDirection.AGAINST:
            if strength == PolicyStrength.STRONG:
                return m.PopoloDirection.MAJORITY_STRONG
            elif strength == PolicyStrength.WEAK:
                return m.PopoloDirection.MAJORITY_WEAK
    # other option is that this was a abstain vote in pw
    # which in the long run we want want rid of
    return m.PopoloDirection.ABSTAIN


def org_type(chamber: Chamber) -> m.ValidOrganizationType:
    match chamber.slug:
        case AllowedChambers.COMMONS:
            return m.ValidOrganizationType.COMMONS
        case AllowedChambers.LORDS:
            return m.ValidOrganizationType.LORDS
        case AllowedChambers.SCOTLAND:
            return m.ValidOrganizationType.SCOTTISH_PARLIAMENT
        case _:
            raise ValueError(f"Unknown chamber {chamber.slug}")


def breakdown_to_vote_count(
    overall_breakdown: DivisionBreakdown
) -> list[m.PopoloVoteCount]:
    return [
        m.PopoloVoteCount(
            option=m.PopoloVoteOption.NO, value=overall_breakdown.against_motion
        ),
        m.PopoloVoteCount(
            option=m.PopoloVoteOption.AYE, value=overall_breakdown.for_motion
        ),
        m.PopoloVoteCount(
            option=m.PopoloVoteOption.BOTH, value=overall_breakdown.neutral_motion
        ),
        m.PopoloVoteCount(
            option=m.PopoloVoteOption.ABSENT,
            value=650 - overall_breakdown.vote_participant_count,
        ),
    ]


def get_division_url(div: DivisionInfo, policy_id: int) -> str:
    template = "http://www.publicwhip.org.uk/division.php?date={date}&number={number}&dmp={policy_id}&house=commons&display=allpossible"
    return template.format(
        date=div.date.strftime("%Y-%m-%d"),
        number=div.division_number,
        policy_id=policy_id,
    )


@dependency_alias_for(m.PopoloPolicy)
async def GetPopoloPolicy(policy_id: int) -> m.PopoloPolicy:
    """
    Create a replacement object for the https://www.publicwhip.org.uk/data/popolo/363.json
    view
    """

    policy = await Policy.from_id(policy_id)

    # just refer back to public whip for moment as we're not public
    url = f"https://www.publicwhip.org.uk/policy.php?id={policy_id}"

    divisions = [d.decision for d in policy.division_links]

    div_and_votes_collection: list[
        DivisionAndVotes
    ] = await DivisionAndVotes.from_divisions(divisions)
    div_and_votes_lookup = {x.details.division_id: x for x in div_and_votes_collection}

    aspects = []
    for link in policy.division_links:
        # for the moment, we only care about the commons
        if link.decision.chamber.slug != AllowedChambers.COMMONS:
            continue
        division = link.decision
        strength = link.strength
        id = f"pw-{division.date}-{division.chamber.slug}"
        div_and_votes = div_and_votes_lookup[division.division_id]
        motion_result = div_and_votes.overall_breakdown.motion_result_int
        motion_desc = pw_style_motion_description(
            motion_result, link.alignment, strength
        )
        motion_org = org_type(division.chamber)
        counts = breakdown_to_vote_count(div_and_votes.overall_breakdown)
        votes = [m.PopoloVote.from_vote(v) for v in div_and_votes.votes]
        vote_events = m.VoteEvent(counts=counts, votes=votes)
        division_url = get_division_url(division, policy_id)
        motion = m.PopoloMotion(
            id=id,
            organization_id=motion_org,
            text=division.division_name,
            date=division.date,
            vote_events=[vote_events],
        )
        aspect = m.PopoloAspect(
            source=division_url, direction=motion_desc, motion=motion
        )
        aspects.append(aspect)

    alignments = await PersonPolicyLink.from_policy_id(policy_id)

    reduced_alignments = [
        ReducedPersonPolicyLink.from_person_policy_link(x) for x in alignments
    ]

    policy = m.PopoloPolicy(
        title=policy.name,
        text=policy.policy_description,
        sources=m.PopoloSource(url=url),
        aspects=aspects,
        alignments=reduced_alignments,
    )
    return policy
