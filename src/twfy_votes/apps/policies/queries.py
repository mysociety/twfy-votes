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
                list_has(groups, {{ group }})
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
        from comparisons_by_policy_vote({{ person_id }},
                                        {{ chamber_slug }},
                                        {{ party_slug }},
                                        {{ start_date }},
                                        {{ end_date }}
                                        )
        group by is_target, policy_id
    """
    person_id: int
    chamber_slug: str
    party_slug: str
    start_date: str = "1900-01-01"
    end_date: str = "2100-01-01"


class PolicyAffectedPeople(BaseQuery):
    """
    Can be given a specific list of policies,
    or will return all people who have been present for a policy
    """

    query_template = """
    SELECT distinct
        person_id
    FROM
        policy_votes
    join
        policies on (policy_votes.policy_id = policies.id)
    join
        pw_division on
            (
            pw_division.division_date = policy_votes.division_date and 
            pw_division.division_number = policy_votes.division_number and 
            pw_division.chamber = policy_votes.chamber
            )
    join
        pw_vote using (division_id)
    join
        pd_memberships on (pw_vote.membership_id = pd_memberships.membership_id)
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
    select distinct(party) as party from pd_memberships
    where chamber == {{ chamber_slug }}
    and person_id == {{ person_id }}
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


class PolicyDistributionQuery(BaseQuery):
    """
    Here we're joining with the comparison party table to limit to just the
    'official' single comparisons used in TWFY.

    Here in this app, we can store and display multiple comparisons.
    """

    query_template = """
    select
        policy_distributions.*,
        policies.strength_meaning as strength_meaning
    from 
        policy_distributions
    join
        policies on (policy_distributions.policy_id = policies.id)
    {% if single_comparisons %}
    join
        pw_comparison_party using (person_id, chamber, comparison_party)
    {% endif %}
    where
        policy_id = {{ policy_id }}
        and period_slug = {{ period_slug }}
    """
    policy_id: int
    period_slug: str
    single_comparisons: bool = False


class PolicyAgreementPersonQuery(BaseQuery):
    query_template = """
        select * from policy_agreement_count({{start_date}}, {{end_date}})
        where person_id = {{ person_id }}
    """
    person_id: int
    start_date: str
    end_date: str


class PolicyAgreementPolicyQuery(BaseQuery):
    query_template = """
        select * from policy_agreement_count({{start_date}}, {{end_date}})
        where policy_id = {{ policy_id }}
    """
    policy_id: int
    start_date: str
    end_date: str


class PolicyDistributionPersonQuery(BaseQuery):
    """
    Get the policy distribution calculation associated with a person.
    """

    query_template = """
    select
        policy_distributions.*,
        policies.strength_meaning as strength_meaning
    from
        policy_distributions
    join
        policies on (policy_distributions.policy_id = policies.id)
    {% if single_comparisons %}
    join
        pw_comparison_party using (person_id, chamber, comparison_party)
    {% endif %}
    where
        person_id = {{ person_id }}
        and period_slug = {{ period_slug }}
    """
    person_id: int
    period_slug: str
    single_comparisons: bool = False
