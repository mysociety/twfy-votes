"""
This module contains the 'slow' approach to calculating the policies.
It is used to validate the 'fast' approach in vr_generator.py.
"""

from math import isclose
from pathlib import Path
from typing import Any

import rich
from pydantic import BaseModel, Field, computed_field
from tqdm import tqdm
from typing_extensions import Self

from ...apps.core.db import duck_core
from ..decisions.models import DivisionInfo, VotePosition, VoteWithDivisionID
from ..decisions.queries import DivisionVotesQuery
from .models import (
    Policy,
    PolicyDecisionLink,
    PolicyDirection,
    PolicyStrength,
    PolicyTimePeriod,
    PolicyTimePeriodSlug,
)
from .vr_generator import get_pivot_df


class Score(BaseModel):
    num_votes_same: float = 0.0
    num_strong_votes_same: float = 0.0
    num_votes_different: float = 0.0
    num_strong_votes_different: float = 0.0
    num_votes_absent: float = 0.0
    num_strong_votes_absent: float = 0.0
    num_votes_abstained: float = 0.0
    num_strong_votes_abstained: float = 0.0
    num_agreements_same: float = 0.0
    num_strong_agreements_same: float = 0.0
    num_agreements_different: float = 0.0
    num_strong_agreements_different: float = 0.0
    num_comparators: list[int] = Field(default_factory=list)

    def __eq__(self, other: Self) -> bool:
        return not self.tol_errors(other)

    def tol_errors(self, other: Self) -> list[str]:
        """
        Allow 0.05 tolerance for floating point errors.
        """
        errors: list[str] = []

        def tol(a: float, b: float) -> bool:
            return isclose(a, b, abs_tol=0.05)

        fields = [
            "num_votes_same",
            "num_strong_votes_same",
            "num_votes_different",
            "num_strong_votes_different",
            "num_votes_absent",
            "num_strong_votes_absent",
            "num_votes_abstained",
            "num_strong_votes_abstained",
        ]

        for field in fields:
            if not tol((o := getattr(self, field)), (t := getattr(other, field))):
                errors.append(f"{field} is not equal - {o} != {t}")

        return errors

    @computed_field
    @property
    def total_votes(self) -> float:
        return (
            self.num_votes_same
            + self.num_votes_different
            + self.num_votes_absent
            + self.num_strong_votes_same
            + self.num_strong_votes_different
            + self.num_strong_votes_absent
            + self.num_votes_abstained
            + self.num_strong_votes_abstained
        )

    def reduce(self):
        """
        reduce each count to a fraction of the total vote.
        The new total should be 1.
        """
        new = self.model_copy()
        total = new.total_votes
        new.num_votes_same /= total
        new.num_votes_different /= total
        new.num_votes_absent /= total
        new.num_strong_votes_same /= total
        new.num_strong_votes_different /= total
        new.num_strong_votes_absent /= total
        new.num_votes_abstained /= total
        new.num_strong_votes_abstained /= total
        if not isclose(new.total_votes, 1.0):
            raise ValueError(f"Total votes should be 1, not {self.total_votes}")
        return new

    def __add__(self: Self, other: Self) -> Self:
        if isinstance(other, Score):
            self.num_votes_same += other.num_votes_same
            self.num_strong_votes_same += other.num_strong_votes_same
            self.num_votes_different += other.num_votes_different
            self.num_strong_votes_different += other.num_strong_votes_different
            self.num_votes_absent += other.num_votes_absent
            self.num_strong_votes_absent += other.num_strong_votes_absent
            self.num_votes_abstained += other.num_votes_abstained
            self.num_strong_votes_abstained += other.num_strong_votes_abstained
        else:
            raise TypeError(f"Cannot add {type(self)} and {type(other)}")
        return self


class PolicyComparison(BaseModel):
    target_distribution: Score
    other_distribution: Score

    def __eq__(self, other: Self) -> bool:
        target_eq = self.target_distribution == other.target_distribution
        other_eq = self.other_distribution == other.other_distribution

        return target_eq and other_eq


async def get_party_members(party_slug: str):
    duck = await duck_core.child_query()

    query = """
                select * from pd_memberships
                where chamber = 'commons'
                and party_reduced = {{ party }}
                order by start_date asc
    """

    df = await duck.compile(query, {"party": party_slug}).df()
    df["end_date"] = df["end_date"].fillna("9999-12-31")
    return df


async def get_party_members_or_person(party_slug: str, person_id: int):
    duck = await duck_core.child_query()

    query = """
        select * from pd_memberships
        where chamber = 'commons'
        and (
            party_reduced = {{ party }}
        or
            person_id = {{ person_id }}
            )
        order by start_date asc
    """

    df = await duck.compile(query, {"party": party_slug, "person_id": person_id}).df()
    df["end_date"] = df["end_date"].fillna("9999-12-31")
    return df


async def get_mp_dates(person_id: int):
    duck = await duck_core.child_query()

    query = """
    select 
        person_id,
        start_date,
        end_date,
        from pd_memberships
        where chamber = 'commons'
        and person_id = {{ person_id }}
    order by start_date asc
    """

    df = await duck.compile(query, {"person_id": person_id}).df()
    df["end_date"] = df["end_date"].fillna("9999-12-31")
    return df


async def votes_from_decision_link(
    decision_link: PolicyDecisionLink[DivisionInfo]
) -> list[VoteWithDivisionID]:
    duck = await duck_core.child_query()

    votes = await DivisionVotesQuery(
        division_date=decision_link.decision.date,
        division_number=decision_link.decision.division_number,
        chamber_slug=decision_link.decision.chamber.slug,
    ).to_model_list(
        duck=duck,
        model=VoteWithDivisionID,
        validate=DivisionVotesQuery.validate.NOT_ZERO,
    )
    return votes


async def get_scores_slow(
    *,
    person_id: int,
    policy_id: int,
    party: str,
    comparison_period_slug: PolicyTimePeriodSlug,
    debug: bool = False,
) -> PolicyComparison:
    """
    This calculates the score a much slower way than the SQL method.
    Wherever possible this has chosen a different approach to getting
    information and processing it.
    Ideally, this will agree with the less easy to read SQL method.
    """
    comparsion_period = PolicyTimePeriod(slug=comparison_period_slug)
    policy = await Policy.from_id(policy_id)
    chamber = policy.chamber.slug

    member_score = Score()
    other_score = Score()

    def debug_print(*args: Any, **kwargs: Any):
        if debug:
            print(*args, **kwargs)

    party_members = await get_party_members_or_person(
        party_slug=party, person_id=person_id
    )
    mp_dates = await get_mp_dates(person_id=person_id)

    def is_valid_date(date: str) -> bool:
        mask = (mp_dates["start_date"] <= date) & (mp_dates["end_date"] >= date)
        return mask.any()

    # iterate through all agreements
    for decision_link in policy.agreement_links:
        if not is_valid_date(decision_link.decision.date.isoformat()):
            continue
        if not comparsion_period.is_valid_date(decision_link.decision.date):
            continue
        if decision_link.decision.chamber.slug != chamber:
            continue
        if decision_link.alignment == PolicyDirection.NEUTRAL:
            continue
        if decision_link.strength == PolicyStrength.STRONG:
            if decision_link.alignment == PolicyDirection.AGREE:
                member_score.num_strong_agreements_same += 1
            else:
                member_score.num_strong_agreements_different += 1
        else:
            if decision_link.alignment == PolicyDirection.AGREE:
                member_score.num_agreements_same += 1
            else:
                member_score.num_agreements_different += 1

    # iterate through all divisions
    for decision_link in policy.division_links:
        # ignore neutral policies
        if decision_link.alignment == PolicyDirection.NEUTRAL:
            debug_print(
                f"policy neutral {decision_link.decision.division_id} - discarded"
            )
            continue
        if decision_link.decision.chamber.slug != chamber:
            debug_print("policy out of chamber - discarded")
            continue

        if not comparsion_period.is_valid_date(decision_link.decision.date):
            debug_print("policy not in comparison period - discarded")
            continue

        if not is_valid_date(decision_link.decision.date.isoformat()):
            debug_print("person not a member of this date")
            # this person was not a member on this date
            continue

        # get associated votes for this divisio
        votes = await votes_from_decision_link(decision_link)
        if votes[0].division_id != decision_link.decision.division_id:
            raise ValueError("votes and division_id do not match")
        vote_lookup = {v.person.person_id: v for v in votes}
        date = decision_link.decision.date.isoformat()

        # get all possible members on this date
        party_mask = (party_members["start_date"] <= date) & (
            party_members["end_date"] >= date
        )
        is_strong = decision_link.strength == PolicyStrength.STRONG
        rel_party_members = party_members[party_mask]
        other_this_vote_score = Score()

        person_ids = rel_party_members["person_id"].astype(int).tolist()

        debug_print(is_strong, decision_link.strength, decision_link.alignment)

        # iterate through members in votes
        for _, member_series in rel_party_members.iterrows():
            loop_person_id = int(member_series["person_id"])
            is_target = person_id == loop_person_id
            vote = vote_lookup.get(loop_person_id, None)

            if is_target:
                debug_print("this member is the target")
                debug_print(vote)
            if vote is None:
                # this member did not vote
                if is_strong:
                    if is_target:
                        member_score.num_strong_votes_absent += 1
                    else:
                        other_this_vote_score.num_strong_votes_absent += 1
                else:
                    if is_target:
                        member_score.num_votes_absent += 1
                    else:
                        other_this_vote_score.num_votes_absent += 1
            else:
                # this member did vote
                # alignment tests if the vote is in the same direction as the policy
                aligned = (
                    vote.vote in [VotePosition.AYE, VotePosition.TELLAYE]
                    and decision_link.alignment == PolicyDirection.AGREE
                ) or (
                    vote.vote in [VotePosition.NO, VotePosition.TELLNO]
                    and decision_link.alignment == PolicyDirection.AGAINST
                )
                is_abstention = vote.vote == VotePosition.ABSTENTION
                if is_target:
                    if is_strong:
                        if aligned:
                            member_score.num_strong_votes_same += 1
                        else:
                            if is_abstention:
                                member_score.num_strong_votes_abstained += 1
                            else:
                                member_score.num_strong_votes_different += 1
                    else:
                        if aligned:
                            member_score.num_votes_same += 1
                        else:
                            if is_abstention:
                                member_score.num_votes_abstained += 1
                            else:
                                member_score.num_votes_different += 1
                else:
                    if is_strong:
                        if aligned:
                            other_this_vote_score.num_strong_votes_same += 1
                        else:
                            if is_abstention:
                                other_this_vote_score.num_strong_votes_abstained += 1
                            else:
                                other_this_vote_score.num_strong_votes_different += 1
                    else:
                        if aligned:
                            other_this_vote_score.num_votes_same += 1
                        else:
                            if is_abstention:
                                other_this_vote_score.num_votes_abstained += 1
                            else:
                                other_this_vote_score.num_votes_different += 1

        # for this vote, reduce the score to a fraction - so 'all other mps' cast '1' vote in total.
        if other_this_vote_score.total_votes > 0:
            other_score += other_this_vote_score.reduce()
        debug_print("Appending vote score")
        debug_print(len(other_score.num_comparators))
        other_score.num_comparators.append(len(person_ids) - 1)
        member_score.num_comparators.append(1)
    return PolicyComparison(
        target_distribution=member_score, other_distribution=other_score
    )


class ValidationResult(BaseModel):
    slow: PolicyComparison
    fast: PolicyComparison
    results_match: bool


async def validate_approach(
    person_id: int,
    policy_id: int,
    party_id: str,
    chamber_slug: str,
    comparison_period_slug: PolicyTimePeriodSlug,
    debug: bool = False,
) -> ValidationResult:
    """
    This function validates that the fast SQL approach reaches the same conclusions as the easier to follow
    Python function.
    """

    period = PolicyTimePeriod(slug=comparison_period_slug)

    # get fast approach
    df = await get_pivot_df(
        person_id=person_id,
        party_id=party_id,
        chamber_slug=chamber_slug,
        start_date=period.start_date,
        end_date=period.end_date,
    )
    df = df[df["policy_id"] == policy_id]

    di = df.set_index("is_target").to_dict("index")

    fast_approach = PolicyComparison(
        target_distribution=(td := Score.model_validate(di[1]) if 1 in di else Score()),
        other_distribution=Score.model_validate(di.get(0, td)),
    )

    # get slow approach
    slow_approach = await get_scores_slow(
        person_id=person_id,
        policy_id=policy_id,
        party=party_id,
        comparison_period_slug=comparison_period_slug,
        debug=debug,
    )

    # validate if they're reaching the same conclusion
    result = slow_approach == fast_approach

    return ValidationResult(
        slow=slow_approach, fast=fast_approach, results_match=result
    )


async def test_policy_sample(sample: int = 50, policy_id: int | None = None) -> bool:
    """
    Pick a random sample of policies and people and validate that the fast and slow approaches
    reach the same conclusion.
    """

    chamber_slug = "commons"

    path = Path("data", "processed", "person_policies.parquet")

    duck = await duck_core.child_query()

    if policy_id is None:
        query = """
        select * from '{{ parquet_path | sqlsafe }}'
        USING sample {{ sample_size | sqlsafe }}
        """
        df = await duck.compile(
            query, {"parquet_path": path, "sample_size": sample}
        ).df()
    else:
        query = """
        SELECT * from
            (
            SELECT
                *
            FROM 
                '{{ parquet_path | sqlsafe }}'
            WHERE
                policy_id = {{ policy_id }}
            )
        USING sample {{ sample_size | sqlsafe }}

        """
        df = await duck.compile(
            query, {"parquet_path": path, "policy_id": policy_id, "sample_size": sample}
        ).df()

    run_random_check = True

    has_errors = False
    if run_random_check:
        for _, row in tqdm(df.iterrows(), total=len(df)):
            policy_id = int(row["policy_id"])
            person_id = int(row["person_id"])
            party_id = str(row["comparison_party"])
            period_slug = PolicyTimePeriodSlug(row["period_slug"])

            result = await validate_approach(
                person_id=person_id,
                policy_id=policy_id,
                chamber_slug=chamber_slug,
                party_id=party_id,
                comparison_period_slug=period_slug,
            )

            summary = {
                "policy_id": policy_id,
                "person_id": person_id,
                "party_id": party_id,
                "result": result.results_match,
            }

            if not result.results_match:
                rich.print(summary)
                has_errors = True

    if not has_errors:
        rich.print("[green]No errors found[/green]")
    return has_errors
