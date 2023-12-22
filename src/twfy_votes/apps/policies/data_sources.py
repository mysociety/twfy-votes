"""
This module contains the information needed to load 
the duckdb database from various sources.
"""

from pathlib import Path
from typing import Any

from ...helpers.duck import DuckQuery, YamlData
from .models import PartialPolicy, PolicyStrength

duck = DuckQuery(cached_dir=Path("data", "cached"))


@duck.as_python_source
class policies(YamlData[PartialPolicy]):
    yaml_source = Path("data", "policies", "*.yml")
    validation_model = PartialPolicy


@duck.as_python_source
class policy_votes(YamlData[PartialPolicy]):
    """
    Also add this as a seperate table to make query maths easier
    """

    yaml_source = Path("data", "policies", "*.yml")
    validation_model = PartialPolicy

    @classmethod
    def post_validation(cls, models: list[PartialPolicy]) -> list[dict[str, Any]]:
        data: list[dict[str, Any]] = []

        for policy in models:
            for decision in policy.division_links:
                if decision.decision:
                    data.append(
                        {
                            "policy_id": policy.id,
                            "division_date": decision.decision.date,
                            "chamber": decision.decision.chamber_slug,
                            "division_number": decision.decision.division_number,
                            "strength": decision.strength,
                            "strong_int": 1
                            if decision.strength == PolicyStrength.STRONG
                            else 0,
                            "alignment": decision.alignment,
                            "notes": decision.notes,
                        }
                    )

        return data


@duck.as_python_source
class policy_agreements(YamlData[PartialPolicy]):
    """
    Also add this as a seperate table to make query maths easier
    """

    yaml_source = Path("data", "policies", "*.yml")
    validation_model = PartialPolicy

    @classmethod
    def post_validation(cls, models: list[PartialPolicy]) -> list[dict[str, Any]]:
        data: list[dict[str, Any]] = []

        for policy in models:
            for decision in policy.agreement_links:
                if decision.decision:
                    data.append(
                        {
                            "policy_id": policy.id,
                            "division_date": decision.decision.date,
                            "chamber": decision.decision.chamber_slug,
                            "decision_ref": decision.decision.decision_ref,
                            "key": decision.decision.key,
                            "strength": decision.strength,
                            "strong_int": 1
                            if decision.strength == PolicyStrength.STRONG
                            else 0,
                            "alignment": decision.alignment,
                            "notes": decision.notes,
                        }
                    )

        return data


@duck.as_table
class policy_votes_with_id:
    """
    Update the policy votes table with the division_id
    stored in the division's table.
    This is an internal PW id, so can't use it anywhere externally.
    As our eventual data migration might change all the division_ids.
    """

    query = """
    select
        policy_votes.*,
        pw_division.division_id as division_id
    from
        policy_votes
    left join
        pw_division
    on(
        policy_votes.division_date = pw_division.division_date
        and policy_votes.chamber = pw_division.house
        and policy_votes.division_number = pw_division.division_number
    )
    
"""


@duck.as_cached_table
class pw_vote_with_absences:
    """
    Creates an 'absent' vote record for each mp for each division they were absent for.
    the distinct here is to catch some duplication in the record where someone gets 'tellno' and 'no'.
    We are reducing the tellno votes here anyway.
    """

    query = """
    select distinct
        pw_division.division_id,
        pw_mp.mp_id,
        case when vote is null then 'absent' else get_effective_vote(vote) end as vote
    from
        pw_division
    join
        pw_mp on (pw_division.division_date between pw_mp.entered_house and pw_mp.left_house and pw_division.house = pw_mp.house)
    left join
        pw_vote on (pw_vote.division_id = pw_division.division_id and pw_vote.mp_id = pw_mp.mp_id)
    """


@duck.as_table_macro
class target_memberships:
    """
    Table macro to get the memberships for a person in a chamber
    """

    args = ["_person_id", "_chamber_slug"]
    macro = """
    select
        person as person_id,
        mp_id as membership_id,
        get_effective_party(party) as membership_party,
        * exclude (mp_id, party)
    from
        pw_mp
    where
    person = {{ _person_id }} and
    house = {{ _chamber_slug }}
    """


@duck.as_table_macro
class policy_alignment:
    """
    Table macro - For each vote/absence, calculate the policy alignment per person.
    """

    args = ["_person_id", "_chamber_slug", "_party_id"]
    macro = """
    SELECT
        policy_id,
        pw_mp.person as person_id,
        case pw_mp.person when {{ _person_id }} then 1 else 0 end as is_target,
        strong_int,
        -- alignment,
        policy_votes.division_id as division_id,
        policy_votes.division_date as division_date,
        policy_votes.division_number as division_number,
        date_part('year',policy_votes.division_date) as division_year,
        pw_vote.vote as mp_vote,
        case (pw_vote.vote, alignment) when ('aye', 'agree') then 1 when ('no', 'against') then 1 else 0 end as answer_agreed,
        case (pw_vote.vote, alignment) when ('aye', 'against') then 1 when ('no', 'agree') then 1 else 0 end as answer_disagreed,
        case when mp_vote = 'abstention' then 1 else 0 end as abstained,
        case when mp_vote = 'absent' then 1 else 0 end as absent,
        -- unique_rows((policy_id, policy_votes.division_id, pw_mp.person)) AS dupe_count
    FROM
        policy_votes_with_id as policy_votes
    join
        policies on (policy_votes.policy_id = policies.id)
    join
        pw_division on (policy_votes.division_id = pw_division.division_id)
    join
        target_memberships({{ _person_id}}, {{ _chamber_slug }}) as target_memberships
            on policy_votes.division_date between target_memberships.entered_house and target_memberships.left_house
    join
        pw_vote_with_absences as pw_vote on (policy_votes.division_id = pw_vote.division_id)
    join
        pw_mp using (mp_id)
    where
        policy_votes.alignment != 'neutral'
        and policy_votes.chamber = {{ _chamber_slug }}
        and policies.chamber = {{ _chamber_slug }}
        and ( -- here we want either the persons own divisions, or the divisions of the party they are in.
            pw_mp.person = {{ _person_id }}
            or
            party = {{ _party_id }}
            )
    """


@duck.as_table_macro
class comparisons_by_policy_vote:
    """
    Table Macro.
    For each policy/vote, group up both the target and the comparison mps, and create an equiv score for the comparison
    This will be floats - but will sum to the same total of votes as the number of divisions the target could vote in.
    """

    args = ["_person_id", "_chamber_slug", "_party_id"]
    macro = """
    select
        is_target,
        policy_id,
        division_id,
        ANY_VALUE(strong_int) as strong_int,
        count(*) as total,
        any_value(division_year) as division_year,
        sum(answer_agreed) / total as num_divisions_agreed,
        sum(answer_disagreed) / total as num_divisions_disagreed,
        sum(abstained) / total as num_divisions_abstained,
        sum(absent) / total as num_divisions_absent,
        sum(answer_agreed) + sum(answer_disagreed) + sum(abstained) + sum(absent) as num_comparators,
    from
        policy_alignment({{ _person_id }}, {{ _chamber_slug }}, {{ _party_id }})
    group by
        is_target, policy_id, division_id
    """


@duck.as_source
class policy_distributions:
    """ "
    This is the main table for the policy analysis.
    This is generated by the 'create-voting-records' command.
    """

    source = Path("data", "processed", "person_policies.parquet")


@duck.as_view
class pw_comparison_party:
    """
    Get a list of a single comparison party
    """

    query = """
        select
        house as chamber,
        person as person_id,
        CASE
        WHEN person_id IN (10172, 14031, 25873) THEN
            get_effective_party(last(party order by entered_house))
        ELSE
        get_effective_party(first(party order by entered_house))
        END 
        as comparison_party
        from pw_mp
        group by person_id, house    
    """
