from twfy_votes.helpers.duck import BaseQuery


class AllPolicyQuery(BaseQuery):
    query_template = """
        SELECT * from policies
        order by id
    """


class GroupPolicyQuery(BaseQuery):
    query_template = """
        select * from policies where list_has(group_ids, {{ group }})
        order by id
    """
    group: str


class GroupStatusPolicyQuery(BaseQuery):
    # the group field is a list of groups, we want to get any item that where the group is in the list
    query_template = """
        select * from policies where
            {% if group %}
                list_has(group_ids, {{ group }})
                {% if status or chamber %} and {% endif %}
            {% endif %}
            {% if status %}
                status = {{ status }} 
                {% if chamber %} and {% endif %}
            {% endif %}
            {% if chamber %}
                chamber = {{ chamber }}
            {% endif %}
        order by id
    """
    group: str | None
    status: str | None
    chamber: str | None


class StatusPolicyQuery(BaseQuery):
    # the group field is a list of groups, we want to get any item that where the group is in the list
    query_template = """
        select * from policies where status = {{ status }}
        order by id
    """
    status: str


class PolicyIdQuery(BaseQuery):
    query_template = """
        SELECT * from policies where id = {{ id }}
    """
    id: int


class PolicyPivotTable(BaseQuery):
    """
    Retrieve all policy breakdowns and comparison breakdowns
    for a single person, given a chamber and a party.
    """

    query_template = """
        select
        is_target,
        policy_id,
        sum(num_divisions_agreed) filter (where strong_int = 0) as num_votes_same,
        sum(num_divisions_agreed) filter (where strong_int = 1) as num_strong_votes_same,
        sum(num_divisions_disagreed) filter (where strong_int = 0) as num_votes_different,
        sum(num_divisions_disagreed) filter (where strong_int = 1) as num_strong_votes_different,
        sum(num_divisions_absent) filter (where strong_int = 0) as num_votes_absent,
        sum(num_divisions_absent) filter (where strong_int = 1) as num_strong_votes_absent,
        sum(num_divisions_abstained) filter (where strong_int = 0) as num_votes_abstained,
        sum(num_divisions_abstained) filter (where strong_int = 1) as num_strong_votes_abstained,
        list(num_comparators) as num_comparators,
        min(division_year) as start_year,
        max(division_year) as end_year
        from comparisons_by_policy_vote({{ person_id }}, {{ chamber_slug }}, {{ party_slug }})
        group by is_target, policy_id
    """
    person_id: int
    chamber_slug: str
    party_slug: str


class PolicyAffectedPeople(BaseQuery):
    """
    Can be given a specific list of policies,
    or will return all people who have been present for a policy
    """

    query_template = """
    SELECT distinct
        person as person_id
    FROM
        policy_votes
    join
        policies on (policy_votes.policy_id = policies.id)
    join
        pw_division on
            (
            pw_division.division_date = policy_votes.division_date and 
            pw_division.division_number = policy_votes.division_number and 
            pw_division.house = policy_votes.chamber
            )
    join
        pw_vote using (division_id)
    join
        pw_mp using (mp_id)
    where policies.chamber = {{ chamber_slug }}
    {% if policy_ids %}
    and policy_id in {{ policy_ids| inclause }}
    {% endif %}
    """
    chamber_slug: str
    policy_ids: list[int] | None = None


class GetPersonParties(BaseQuery):
    """
    Get all 'real' parties that a person has been a member of in a chamber.
    Excludes independents, etc.
    """

    query_template = """
    select * from (
    select distinct(party) as party from pw_mp
    where house == {{ chamber_slug }}
    and person == {{ person_id }}
    )
    {% if banned_parties %}
    where party not in {{ banned_parties | inclause }}
    {% endif %}
    """
    chamber_slug: str
    person_id: int
    banned_parties: list[str] = [
        "independent",
        "speaker",
        "deputy-speaker",
        "independent-conservative",
        "independent-labour",
        "independent-ulster-unionist",
    ]
