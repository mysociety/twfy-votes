import datetime
from pathlib import Path

from ruamel.yaml import YAML

from ...helpers.data.models import data_to_yaml
from .models import (
    AllowedChambers,
    LinkStatus,
    PartialDivision,
    PartialPolicy,
    PartialPolicyDecisionLink,
    PolicyDirection,
    PolicyGroupSlug,
    PolicyStatus,
    PolicyStrength,
)

vote_folder = Path("data", "policies")

PartialDivisionLink = PartialPolicyDecisionLink[PartialDivision]


def create_new_policy(
    name: str,
    context_description: str = "",
    policy_description: str = "",
    status: PolicyStatus = PolicyStatus.DRAFT,
    chamber: AllowedChambers = AllowedChambers.COMMONS,
    groups: list[PolicyGroupSlug] = [],
):
    """ """
    all_current_ids = [int(x.stem) for x in vote_folder.glob("*.yml")]

    # Giving a healthy range to existing PW policies (generally less than 10000)
    starting_value = {
        AllowedChambers.COMMONS: 20000,
        AllowedChambers.LORDS: 30000,
        AllowedChambers.WALES: 40000,
        AllowedChambers.SCOTLAND: 50000,
        AllowedChambers.NI: 60000,
    }

    policy_id = starting_value[chamber]
    while policy_id in all_current_ids:
        policy_id += 1

    policy = PartialPolicy(
        id=policy_id,
        name=name,
        context_description=context_description,
        policy_description=policy_description,
        status=status,
        chamber=chamber,
        groups=groups,
        highlightable=False,
        division_links=[],
        agreement_links=[],
    ).model_dump()

    policy_path = vote_folder / f"{policy_id}.yml"

    data_to_yaml(policy, policy_path)

    print(f"Created policy {policy_id} at {policy_path}")


def add_vote_to_policy_from_url(
    votes_url: str,
    policy_id: int,
    vote_alignment: PolicyDirection,
    strength: PolicyStrength = PolicyStrength.STRONG,
):
    parts = votes_url.split("/")
    chamber_slug = AllowedChambers(parts[-3])
    date = datetime.datetime.strptime(parts[-2], "%Y-%m-%d").date()
    division_number = int(parts[-1])

    policy_path = vote_folder / f"{policy_id}.yml"

    if not policy_path.exists():
        raise ValueError("Policy does not exist")

    yaml = YAML()
    yaml.default_flow_style = False

    data = yaml.load(policy_path)

    partial = PartialDivision(
        chamber_slug=chamber_slug, date=date, division_number=division_number
    )

    policy_link = PartialDivisionLink(
        decision=partial,
        alignment=vote_alignment,
        strength=strength,
        status=LinkStatus.ACTIVE,
    ).model_dump()
    del policy_link["decision_key"]

    data["division_links"].append(policy_link)

    # quick double check haven't done this before
    keys = []
    for division in data["division_links"]:
        decision = division["decision"]
        key = "-".join(
            [
                decision["chamber_slug"],
                str(decision["date"]),
                str(decision["division_number"]),
            ]
        )
        if key in keys:
            raise ValueError(f"Division {key} already exists in policy.")
        keys.append(key)

    data_to_yaml(data, policy_path)
