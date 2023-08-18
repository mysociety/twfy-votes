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
                chamber_id = {{ chamber }}
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
