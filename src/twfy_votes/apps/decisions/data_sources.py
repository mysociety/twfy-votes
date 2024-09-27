"""
This module contains the information needed to load 
the duckdb database from various sources.
"""

from pathlib import Path
from typing import Any

from ...helpers.duck import DuckQuery, DuckUrl, YamlData
from .models import (
    GovernmentParties,
    ManualMotion,
    PartialAgreement,
    VoteMotionAnalysis,
)

processed_data = Path("data", "cached")
raw_data = Path("data", "raw")

duck = DuckQuery(cached_dir=processed_data)


public_whip = DuckUrl(
    "https://pages.mysociety.org/publicwhip-data/data/public_whip_data/latest"
)

twfy = DuckUrl("https://www.theyworkforyou.com/pwdata/votes")

# this is indirectly sources from parlparse's people.json
politician_data = DuckUrl(
    "https://pages.mysociety.org/politician_data/data/uk_politician_data/latest"
)


@duck.as_python_source
class government_parties_nested(YamlData[GovernmentParties]):
    yaml_source = Path("data", "raw", "government_parties.yaml")
    validation_model = GovernmentParties


@duck.as_python_source
class pw_agreements(YamlData[PartialAgreement]):
    yaml_source = Path("data", "raw", "agreements.yaml")
    validation_model = PartialAgreement


@duck.as_python_source
class vote_motions(YamlData[VoteMotionAnalysis]):
    yaml_source = Path("data", "cached", "motions.yaml")
    alt_yaml_source = Path("data", "processed", "motions.yaml")
    validation_model = VoteMotionAnalysis

    @classmethod
    def _get_yaml_source(cls) -> Path:
        if cls.yaml_source.exists():
            return cls.yaml_source
        else:
            return cls.alt_yaml_source

    @classmethod
    def get_data_solo(cls) -> list[dict[str, Any]]:
        data: dict[str, list[dict[str, Any]]] = super().get_data_solo()  # type: ignore
        return data.get("items", [])


@duck.as_view
class government_parties_unnest_parties:
    query = """
        select
            * exclude (party),
            unnest(party) as party,
            True as is_gov
        from
        government_parties_nested
    """


@duck.as_view
class government_parties:
    query = """
        select
            * exclude (chamber),
            unnest(chamber) as chamber,
            True as is_gov
        from
        (
        select
            * exclude (party),
            unnest(party) as party,
            True as is_gov
        from
            government_parties_nested
        )
    """


@duck.as_source
class party_lookup:
    source = raw_data / "party_lookup.csv"


@duck.as_table
class pd_people_source:
    source = politician_data / "person_alternative_names.parquet"


@duck.as_view
class pd_people_non_unique:
    """
    Reconstruct the people table to tidy up where
    names are stored in different columns for lords.
    """

    query = """
    SELECT
        * exclude (person_id),
        case
            when family_name is null then lordname
            else family_name
        end as last_name,
        split(person_id, '/')[-1] as person_id,
        case
        when honorific_prefix is null then
            concat(given_name, ' ', last_name)
        else
            concat(given_name, ' ', last_name, ' (', honorific_prefix, ')')
        end as nice_name,
        ROW_NUMBER() OVER (PARTITION BY person_id) as row_number
    FROM
        pd_people_source
    WHERE
        note = 'Main'
    """


@duck.as_view
class pd_people:
    query = """
    SELECT
    *
    FROM
        pd_people_non_unique
    WHERE
        row_number = 1
    """


@duck.as_macro
class get_effective_party:
    """
    Reduce variant parties to a single canonical entry
    mostly taking out the co-operative part of labour/co-operative
    """

    args = ["party"]
    macro = """
        case
            when party = 'labourco-operative' then 'labour'
            else party
        end
    """


@duck.as_source
class pd_orgs:
    source = politician_data / "organizations.parquet"


@duck.as_source
class pd_posts:
    source = politician_data / "posts.parquet"


@duck.as_source
class source_pd_memberships:
    source = politician_data / "simple_memberships.parquet"


@duck.as_view
class memberships_adjusted:
    """
    Intermediate view that adjusts the memberships table
    """

    query = """
    select
    CAST(split(source.membership_id, '/')[-1] as BIGINT) as membership_id,
    constituency as constituency,
    CAST(split(source.person_id, '/')[-1] as BIGINT) as person_id,
    source.start_date as start_date,
    case when source.end_date is null then '9999-12-31' else source.end_date end as end_date,
    source.chamber as chamber,
    source.party as party,
    get_effective_party(source.party) as party_reduced,
    source.first_name as first_name,
    source.last_name as last_name,
    source.nice_name as nice_name
    from 
        source_pd_memberships as source
"""


@duck.as_table
class pd_memberships:
    """
    This uses a simplified version of the memberships table
    Which brings in the party and chamber information
    """

    query = """
    SELECT
        source.*,
        pd_orgs.name AS party_name,
        po2.name AS party_reduced_name
    FROM 
        memberships_adjusted AS source
    LEFT JOIN pd_orgs ON source.party = pd_orgs.id
    LEFT JOIN pd_orgs AS po2 ON source.party_reduced = po2.id;
"""


@duck.as_source
class source_pw_division:
    source = twfy / "divisions.parquet"


if (processed_data / "voting_clusters.parquet").exists():
    # need a quick back up here because cluster table is used in the division table
    # which is loaded when trying to run the update script that will create it in the first place
    @duck.as_source
    class pw_division_cluster:  # type: ignore
        source = processed_data / "voting_clusters.parquet"
else:

    @duck.as_view
    class pw_division_cluster:
        query = """
            SELECT
            NULL AS division_id,
            NULL AS cluster
            WHERE
            1 = 0;
            """


@duck.as_table
class pd_member_counts:
    source = politician_data / "membership_counts.parquet"


@duck.as_python_source
class pw_manual_motions(YamlData[ManualMotion]):
    yaml_source = Path("data", "divisions", "manual_motions.yaml")
    validation_model = ManualMotion


@duck.as_table
class pw_division:
    query = """
    SELECT
        source_pw_division.* EXCLUDE (house, gid, division_title),
        house as chamber,
        division_title as division_name,
        split(gid, '/')[-1] as source_gid,
        split(gid, '/')[-1] as debate_gid,
        null as clock_time,
        '' as source_url,
        '' as motion,
        '' as debate_url,
        CASE WHEN manual_motion is NULL THEN '' ELSE manual_motion END AS manual_motion,
        CASE WHEN cluster is NULL THEN '' ELSE cluster END AS voting_cluster,
        concat(house, '-', source_pw_division.division_date, '-', source_pw_division.division_number) as division_key,
        pd_member_counts.members_count as total_possible_members
    FROM
        source_pw_division
    LEFT JOIN pw_manual_motions on
        (source_pw_division.house = pw_manual_motions.chamber
         and source_pw_division.division_date = pw_manual_motions.division_date
         and source_pw_division.division_number = pw_manual_motions.division_number
         )
    LEFT JOIN pw_division_cluster on (source_pw_division.division_id = cast(pw_division_cluster.division_id as string))
    LEFT JOIN pd_member_counts on
        (source_pw_division.division_date between pd_member_counts.start_date and
        pd_member_counts.end_date and
        source_pw_division.house = pd_member_counts.chamber)
    WHERE
        house != 'pbc'
        AND source_pw_division.division_id NOT LIKE '%cy-senedd%'
    """


@duck.as_source
class source_pw_vote:
    source = twfy / "votes.parquet"


@duck.as_macro
class unique_rows:
    args = ["a"]
    macro = "ROW_NUMBER() OVER (PARTITION BY a)"


@duck.as_macro
class get_clean_vote:
    """
    Remove 'both' entries from the vote column.
    Conform on 'abstention' as the value for 'both'
    """

    args = ["vote"]
    macro = """
        case
            when vote = 'both' then 'abstention'
            else vote
        end
    """


@duck.as_macro
class null_to_zero:
    """
    Simplify that null should be zero
    """

    args = ["val"]
    macro = """
    case 
        when val is NULL then 0
        else val
    end       
    """


@duck.as_view
class pw_vote:
    query = """

    SELECT
        division_id as division_id,
        get_clean_vote(vote) as vote,
        pd_memberships.membership_id
    from source_pw_vote
        join pw_division
            using (division_id)
        join pd_memberships
            on
                source_pw_vote.person_id = pd_memberships.person_id
            and
                pw_division.division_date between pd_memberships.start_date and pd_memberships.end_date
    """


@duck.as_macro
class get_effective_vote:
    """
    Reduce values so tellers are counted as aye or no
    """

    args = ["vote"]
    macro = """
    case vote
        when 'tellaye' then 'aye'
        when 'tellno' then 'no'
        else vote
    end       
    """


@duck.as_view
class cm_agreement_present:
    query = """
        SELECT
            key,
            membership_id,
            chamber_slug,
            'collective' as vote,
            pd_people.given_name as person__first_name,
            pd_people.last_name as person__last_name,
            pd_people.nice_name as person__nice_name,
            pd_memberships.party_name as person__party,
            pd_memberships.person_id as person__person_id,
        FROM
            pw_agreements
        JOIN
            pd_memberships on pw_agreements.date between pd_memberships.start_date and pd_memberships.end_date
            and pw_agreements.chamber_slug = pd_memberships.chamber
        JOIN
            pd_people on (pd_people.person_id = pd_memberships.person_id)
    """


@duck.as_table
class cm_votes_with_people:
    """
    Use political data to get more information into the votes table
    """

    query = """
    SELECT
        pw_vote.*,
        get_effective_vote(vote) as effective_vote,
        person_id,
        pd_memberships.party as party,
        pd_memberships.party_name as party_name,
        pd_memberships.party_reduced as party_reduced,
        pd_memberships.party_reduced_name as party_reduced_name,
        given_name,
        pd_memberships.last_name as last_name,
        pd_memberships.nice_name as nice_name,
        CASE WHEN government_parties.is_gov is NULL THEN 'Other' ELSE 'Government' END AS is_gov,
        total_possible_members
    FROM
        pw_vote
    JOIN
        pd_memberships on (pw_vote.membership_id = pd_memberships.membership_id)
    JOIN
        pd_people using (person_id)
    LEFT JOIN
        pw_division using (division_id)
    LEFT JOIN government_parties 
        government_parties on
            (division_date between government_parties.start_date and 
            government_parties.end_date and 
            government_parties.party = pd_memberships.party_reduced and
            pw_division.chamber = government_parties.chamber)
    """


@duck.as_cached_table
class pw_divisions_with_counts:
    """
    Get the counts for and against in a division
    """

    query = """
    select
        division_id,
        count(*) as vote_participant_count,
        any_value(total_possible_members) as total_possible_members,
        sum(case when effective_vote = 'aye' then 1 else 0 end) as for_motion,
        sum(case when effective_vote = 'no' then 1 else 0 end) as against_motion,
        sum(case when effective_vote = 'both' then 1 else 0 end) as neutral_motion,
        for_motion + against_motion as signed_votes,
        for_motion - against_motion as motion_majority,
        for_motion / signed_votes as for_motion_percentage,
        case 
            when motion_majority = 0 then 0
            when motion_majority > 0 then 1
            when motion_majority < 0 then -1
        end as motion_result_int

        from
        cm_votes_with_people
        group by
            all
    """


@duck.as_cached_table
class pw_divisions_party_with_counts:
    """
    Get the counts for and against in a division (within a party)
    """

    query = """
    SELECT
        division_id,
        party_reduced_name as grouping,
        count(*) as vote_participant_count,
        any_value(total_possible_members) as total_possible_members,
        sum(case when effective_vote = 'aye' then 1 else 0 end) as for_motion,
        sum(case when effective_vote = 'no' then 1 else 0 end) as against_motion,
        sum(case when effective_vote = 'abstention' then 1 else 0 end) as neutral_motion,
        for_motion + against_motion as signed_votes,
        for_motion - against_motion as motion_majority,
        for_motion / signed_votes as for_motion_percentage,
        case 
            when motion_majority = 0 then 0
            when motion_majority > 0 then 1
            when motion_majority < 0 then -1
        end as motion_result_int
    FROM
        cm_votes_with_people
    GROUP BY 
        all
    """


@duck.as_cached_table
class pw_divisions_gov_with_counts:
    """
    Get the counts for and against in a division (by government and 'other' reps)
    """

    query = """
    SELECT
        division_id,
        is_gov as grouping,
        count(*) as vote_participant_count,
        any_value(total_possible_members) as total_possible_members,
        sum(case when effective_vote = 'aye' then 1 else 0 end) as for_motion,
        sum(case when effective_vote = 'no' then 1 else 0 end) as against_motion,
        sum(case when effective_vote = 'both' then 1 else 0 end) as neutral_motion,
        for_motion + against_motion as signed_votes,
        for_motion - against_motion as motion_majority,
        for_motion / signed_votes as for_motion_percentage,
        case 
            when motion_majority = 0 then 0
            when motion_majority > 0 then 1
            when motion_majority < 0 then -1
        end as motion_result_int
    FROM
        cm_votes_with_people
    GROUP BY
        all
    """


@duck.as_cached_table
class pw_votes_with_party_difference:
    """
    Update the votes table to include difference from the party average for each vote.
    """

    query = """
    SELECT
        cm_votes_with_people.*,
        for_motion_percentage,
        case effective_vote
            when 'aye' then 1
            when 'no' then 0
            when 'abstention' then 0.5
        end as effective_vote_int,
        abs(effective_vote_int - for_motion_percentage) as diff_from_party_average
    FROM
        cm_votes_with_people
    JOIN
        pw_divisions_party_with_counts
            on
                (cast(cm_votes_with_people.division_id as string) = cast(pw_divisions_party_with_counts.division_id as string) and
                 cm_votes_with_people.party_reduced_name = pw_divisions_party_with_counts.grouping)
    """


@duck.as_view
class pw_chamber_division_span:
    """
    Get the earliest and latest division dates for a chamber
    """

    query = """
    select chamber as chamber_slug,
        SUBSTRING(max(division_date),1,4) as latest_year,
        SUBSTRING(min(division_date),1,4) as earliest_year,
    from pw_division
    group by all
    """
