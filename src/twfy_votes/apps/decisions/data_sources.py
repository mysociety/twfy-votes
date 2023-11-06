"""
This module contains the information needed to load 
the duckdb database from various sources.
"""

from pathlib import Path

from ...helpers.duck import DuckQuery, DuckUrl, YamlData
from .models import GovernmentParties, ManualMotion

processed_data = Path("data", "cached")
raw_data = Path("data", "raw")

duck = DuckQuery(cached_dir=processed_data)


public_whip = DuckUrl(
    "https://pages.mysociety.org/publicwhip-data/data/public_whip_data/latest"
)

# this is indirectly sources from parlparse's people.json
politician_data = DuckUrl(
    "https://pages.mysociety.org/politician_data/data/uk_politician_data/latest"
)


@duck.as_python_source
class government_parties_nested(YamlData[GovernmentParties]):
    yaml_source = Path("data", "raw", "government_parties.yaml")
    validation_model = GovernmentParties


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


@duck.as_source
class source_pw_mp:
    source = public_whip / "pw_mp.parquet"


@duck.as_table
class pw_mp:
    query = """
    select
    source_pw_mp.* exclude(party),
    case when twfy_party_slug is null then party else twfy_party_slug end as party,
     from 
        source_pw_mp
    left join party_lookup on (
        source_pw_mp.party = party_lookup.pw_party_slug
    )
"""


@duck.as_source
class source_pw_vote:
    source = public_whip / "pw_vote.parquet"


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


@duck.as_table
class pw_vote:
    query = """
    select * exclude (vote), get_clean_vote(vote) as vote from source_pw_vote
    """


@duck.as_python_source
class pw_manual_motions(YamlData[ManualMotion]):
    yaml_source = Path("data", "divisions", "manual_motions.yaml")
    validation_model = ManualMotion


@duck.as_source
class source_pw_division:
    source = public_whip / "pw_division.parquet"


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
class pw_division:
    query = """
    SELECT
        source_pw_division.*,
        CASE WHEN manual_motion is NULL THEN '' ELSE manual_motion END AS manual_motion,
        CASE WHEN cluster is NULL THEN '' ELSE cluster END AS voting_cluster,
        concat(house, '-', source_pw_division.division_date, '-', source_pw_division.division_number) as division_key
    FROM
        source_pw_division
    LEFT JOIN pw_manual_motions on
        (source_pw_division.house = pw_manual_motions.chamber
         and source_pw_division.division_date = pw_manual_motions.division_date
         and source_pw_division.division_number = pw_manual_motions.division_number
         )
    LEFT JOIN pw_division_cluster on (source_pw_division.division_id = pw_division_cluster.division_id)

    """


@duck.as_table
class pd_people_source:
    source = politician_data / "person_alternative_names.parquet"


@duck.as_view
class pd_people:
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
    FROM
        pd_people_source
    WHERE
        note = 'Main'
    """


@duck.as_table
class pd_memberships:
    source = politician_data / "memberships.parquet"


@duck.as_table
class pd_orgs:
    source = politician_data / "organizations.parquet"


@duck.as_table
class pd_posts:
    source = politician_data / "posts.parquet"


@duck.as_macro
class get_effective_party:
    """
    Reduce variant parties to a single canonical entry
    mostly taking out the co-operative part of labour/co-operative
    """

    args = ["party"]
    macro = """
        case
            when party = 'Labour/Co-operative' then 'Labour'
            when party = 'Social Democratic and Labour Party' then 'SDLP'
            else party
        end
    """


@duck.as_view
class pd_members_and_orgs:
    """
    Update the memberships table to include the party name
    and map better to the IDs used by the public whip trailer.
    """

    query = """
    SELECT
        pd_memberships.* exclude (id, person_id),
        split(person_id, '/')[-1] as person_id,
        split(pd_memberships.id, '/')[-1] as membership_id,
        pd_orgs.name as party_name,
        get_effective_party(pd_orgs.name) as party_name_reduced
    FROM
        pd_memberships
    JOIN
        pd_orgs on (pd_memberships.on_behalf_of_id = pd_orgs.id)
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
        party_name,
        party_name_reduced,
        given_name,
        last_name,
        nice_name,
        CASE WHEN government_parties.is_gov is NULL THEN 'Other' ELSE 'Government' END AS is_gov  
    
    FROM
        pw_vote
    JOIN
        pd_members_and_orgs on (pw_vote.mp_id = pd_members_and_orgs.membership_id)
    JOIN
        pd_people using (person_id)
    LEFT JOIN
        pw_division using (division_id)
    LEFT JOIN government_parties 
        government_parties on
            (division_date between government_parties.start_date and 
            government_parties.end_date and 
            government_parties.party = party_name_reduced and
            pw_division.house = government_parties.chamber)
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
        sum(case when effective_vote = 'aye' then 1 else 0 end) as for_motion,
        sum(case when effective_vote = 'no' then 1 else 0 end) as against_motion,
        sum(case when effective_vote = 'both' then 1 else 0 end) as neutral_motion,
        for_motion + against_motion as signed_votes,
        for_motion - against_motion as motion_majority,
        for_motion / signed_votes as motion_majority_ratio,
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
        party_name_reduced as grouping,
        count(*) as vote_participant_count,
        sum(case when effective_vote = 'aye' then 1 else 0 end) as for_motion,
        sum(case when effective_vote = 'no' then 1 else 0 end) as against_motion,
        sum(case when effective_vote = 'abstention' then 1 else 0 end) as neutral_motion,
        for_motion + against_motion as signed_votes,
        for_motion - against_motion as motion_majority,
        for_motion / signed_votes as motion_majority_ratio,
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
        sum(case when effective_vote = 'aye' then 1 else 0 end) as for_motion,
        sum(case when effective_vote = 'no' then 1 else 0 end) as against_motion,
        sum(case when effective_vote = 'both' then 1 else 0 end) as neutral_motion,
        for_motion + against_motion as signed_votes,
        for_motion - against_motion as motion_majority,
        for_motion / signed_votes as motion_majority_ratio,
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
        motion_majority_ratio,
        case effective_vote
            when 'aye' then 1
            when 'no' then 0
            when 'abstention' then 0.5
        end as effective_vote_int,
        abs(effective_vote_int - motion_majority_ratio) as diff_from_party_average
    FROM
        cm_votes_with_people
    JOIN
        pw_divisions_party_with_counts
            on
                (cm_votes_with_people.division_id = pw_divisions_party_with_counts.division_id and
                 cm_votes_with_people.party_name_reduced = pw_divisions_party_with_counts.grouping)
    """


@duck.as_view
class pw_chamber_division_span:
    """
    Get the earliest and latest division dates for a chamber
    """

    query = """
    select house as chamber_slug,
        SUBSTRING(max(division_date),1,4) as latest_year,
        SUBSTRING(min(division_date),1,4) as earliest_year,
    from pw_division
    group by all
    """
