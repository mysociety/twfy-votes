from pathlib import Path

from ruamel.yaml import YAML

data_path = Path("data", "policies")


def fix_policy(policy_id: int, report_only: bool = False):
    yaml_path = data_path / f"{policy_id}.yml"
    yaml = YAML()
    yaml.default_flow_style = False

    data = yaml.load(yaml_path)

    policy_chamber = data["chamber"]

    division_count = len(data["division_links"])

    data["division_links"] = [
        x
        for x in data["division_links"]
        if x["decision"]["chamber_slug"] == policy_chamber
    ]
    data["division_links"] = [
        x for x in data["division_links"] if x["alignment"] != "neutral"
    ]

    new_division_count = len(data["division_links"])

    diff = division_count - new_division_count

    print(f"Policy {policy_id} had {diff} divisions removed.")

    yaml.dump(data, yaml_path)

    return diff


def test_basic_policy_rules():
    # Test there are no neutral
    # or out of chamber policies
    total_change = 0
    for policy in data_path.glob("*.yml"):
        print(f"Fixing {policy.stem}")
        total_change += fix_policy(int(policy.stem))

    assert total_change == 0, "neutral or out of chamber policies present"
