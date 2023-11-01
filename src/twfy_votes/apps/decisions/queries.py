"""
Holds templates used to generate queries
"""

import datetime

from twfy_votes.helpers.duck import BaseQuery


class DivisionQuery(BaseQuery):
    query_template = """
        SELECT
            * exclude (house, clock_time),
        CASE 
            WHEN clock_time IS NULL THEN 'No recorded time'
            ELSE CAST(clock_time AS VARCHAR)
        END AS clock_time,
        house as house__slug
        FROM
            pw_division
        WHERE
            division_date = {{ division_date }} and
            division_number = {{ division_number }} and
            house = {{ chamber_slug }}
        """
    division_date: datetime.date
    division_number: int
    chamber_slug: str


class DivisionQueryKeys(BaseQuery):
    query_template = """
        SELECT
            * exclude (house, clock_time),
        CASE 
            WHEN clock_time IS NULL THEN 'No recorded time'
            ELSE CAST(clock_time AS VARCHAR)
        END AS clock_time,
        house as chamber__slug
        FROM
            pw_division
        WHERE
            division_key in {{ keys | inclause }}
        """
    keys: list[str]


class DivisionVotesQuery(BaseQuery):
    query_template = """
    SELECT
        given_name as person__first_name,
        last_name as person__last_name,
        nice_name as person__nice_name,
        party_name as person__party,
        pw_votes_with_party_difference.mp_id as membership_id,
        person_id as person__person_id,
        pw_votes_with_party_difference.* exclude (division_id, mp_id, __index_level_0__)
    FROM
        pw_division
    JOIN pw_votes_with_party_difference using (division_id)
    WHERE
        pw_division.division_date = {{ division_date }} and
        pw_division.division_number = {{ division_number }} and
        pw_division.house = {{ chamber_slug }}
    """
    division_date: datetime.date
    division_number: int
    chamber_slug: str


class PersonVotesQuery(BaseQuery):
    query_template = """
    SELECT
        given_name as person__first_name,
        last_name as person__last_name,
        nice_name as person__nice_name,
        party_name as person__party,
        pw_votes_with_party_difference.mp_id as membership_id,
        person_id as person__person_id,
        pw_votes_with_party_difference.* exclude (division_id, mp_id, __index_level_0__),
        division_key as division__division_key,
        house as division__chamber__slug,
        division_id as division__division_id,
        division_date as division__division_date,
        division_number as division__division_number,
        division_name as division__division_name,
        source_url as division__source_url,
        motion as division__motion,
        debate_url as division__debate_url,
        source_gid as division__source_gid,
        debate_gid as division__debate_gid,
    FROM
        pw_division
    JOIN pw_votes_with_party_difference using (division_id)
    WHERE
        person_id = {{ person_id }}
    """
    person_id: int


class DivisionBreakDownQuery(BaseQuery):
    query_template = """
    SELECT
        *
    FROM
        pw_divisions_with_counts
    WHERE
        division_id = {{ division_id }}
    """
    division_id: int


class ChamberDivisionsQuery(BaseQuery):
    query_template = """
    SELECT
        * exclude (house, clock_time),
        house as chamber__slug,
        CASE 
            WHEN clock_time IS NULL THEN 'No recorded time'
            ELSE CAST(clock_time AS VARCHAR)
        END AS clock_time
    FROM
        pw_division
    WHERE
        house = {{ chamber_slug }} and
        division_date between {{ start_date }} and {{ end_date }}
    """
    chamber_slug: str
    start_date: datetime.date
    end_date: datetime.date


class PartyDivisionBreakDownQuery(BaseQuery):
    query_template = """
    SELECT
        *
    FROM
        pw_divisions_party_with_counts
    WHERE
        division_id = {{ division_id }}
    """
    division_id: int


class GovDivisionBreakDownQuery(BaseQuery):
    query_template = """
    SELECT
        *
    FROM
        pw_divisions_gov_with_counts
    WHERE
        division_id = {{ division_id }}
    """
    division_id: int


class GetPersonQuery(BaseQuery):
    query_template = """
    SELECT
        *
    FROM
        pd_people
    WHERE
        person_id = {{ person_id }}
    """
    person_id: int


class GetAllPersonsQuery(BaseQuery):
    query_template = """
    SELECT
        *
    FROM
        pd_people
    ORDER BY
        person_id

    """
