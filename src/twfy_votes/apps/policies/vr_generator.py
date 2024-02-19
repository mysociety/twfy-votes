import json
from hashlib import md5
from pathlib import Path

import pandas as pd
from ruamel.yaml import YAML
from tqdm import tqdm
from twfy_votes.apps.core.db import duck_core
from twfy_votes.apps.policies.queries import PolicyPivotTable

from ..decisions.models import AllowedChambers
from .queries import GetPersonParties, PolicyAffectedPeople

policy_dir = Path("data", "policies")


async def get_parties(person_id: int, chamber_slug: AllowedChambers) -> list[str]:
    """
    Get a list of parties for a person in a chamber.
    """
    df = (
        await GetPersonParties(chamber_slug=chamber_slug, person_id=person_id)
        .compile(await duck_core.child_query())
        .df()
    )

    parties: list[str] = df["party"].tolist()
    return parties


async def get_pivot_df(
    *, person_id: int, party_id: str, chamber_slug: str
) -> pd.DataFrame:
    """
    Function to generate the policy vote breakdowns for a person and party.
    """
    duck = await duck_core.child_query()
    df = (
        await PolicyPivotTable(
            person_id=person_id, party_slug=party_id, chamber_slug=chamber_slug
        )
        .compile(duck)
        .df()
    )

    df["person_id"] = person_id
    df["comparison_party"] = party_id
    df["chamber"] = chamber_slug
    return df.fillna(0.0)


async def get_relevant_people(
    chamber_slug: AllowedChambers, policy_id: int | None = None
):
    """
    Get all the people who are logically affected by a policy.
    """
    duck = await duck_core.child_query()
    list_ids = [policy_id] if policy_id else None
    df = (
        await PolicyAffectedPeople(chamber_slug=chamber_slug, policy_ids=list_ids)
        .compile(duck)
        .df()
    )
    person_ids: list[int] = df["person_id"].to_list()
    return person_ids


def get_policy_hash(policy_id: int) -> str:
    yaml = YAML()
    yaml.default_flow_style = False
    policy_file = policy_dir / f"{policy_id}.yml"
    with policy_file.open() as f:
        policy = yaml.load(f)

    # in division_links and agreement_links
    # delete the notes key
    # conver the date to iso format

    for link_type in ["division_links", "agreement_links"]:
        for link in policy[link_type]:
            del link["notes"]
            link["decision"]["date"] = link["decision"]["date"].isoformat()

    hashable_items = [
        policy["strength_meaning"],
        policy["division_links"],
        policy["agreement_links"],
    ]

    hashable = json.dumps(hashable_items, sort_keys=True).encode("utf-8")
    return md5(hashable).hexdigest()


def get_policies_hash(specific_id_only: int | None = None):
    items = []

    for policy_file in policy_dir.glob("*.yml"):
        policy_id = int(policy_file.stem)
        policy_hash = get_policy_hash(policy_id)
        items.append({"policy_id": policy_id, "hash": policy_hash})

    df = pd.DataFrame(items)

    if specific_id_only:
        df = df[df["policy_id"] == specific_id_only]

    return df


def update_policies_hash(specific_id_only: int | None = None):
    """
    Update the policy hash file.
    Tested for misalignment as a test
    """
    new_df = get_policies_hash(specific_id_only)

    data_path = Path("data", "processed", "policy_update_hash.csv")

    if not data_path.exists():
        new_df.to_csv(data_path, index=False)
        return
    df = pd.read_csv(data_path)
    # update df with new values - concat and remove first duplicates of policy_id
    df = pd.concat([df, new_df]).drop_duplicates(subset=["policy_id"], keep="last")

    df.to_csv(data_path, index=False)


def check_policy_hash() -> bool:
    old_df = pd.read_csv(Path("data", "processed", "policy_update_hash.csv"))

    new_df = get_policies_hash()

    # want to check if the hash value is the same for all policy_ids in new_df in old_df

    old_df = old_df.sort_values(by="policy_id")
    new_df = new_df.sort_values(by="policy_id")

    old_hash = old_df.set_index("policy_id")["hash"].to_dict()
    new_hash = new_df.set_index("policy_id")["hash"].to_dict()

    # check if two dictionaries are the same
    return old_hash == new_hash


async def generate_voting_records_for_chamber(
    *,
    chamber: AllowedChambers,
    policy_id: int | None = None,
    person_id: int | None = None,
):
    """Generate voting records for a given chamber."""
    dfs: list[pd.DataFrame] = []

    if person_id:
        person_ids = [person_id]
    else:
        person_ids = await get_relevant_people(
            chamber_slug=chamber, policy_id=policy_id
        )

    for p_id in tqdm(person_ids):
        parties = await get_parties(person_id=p_id, chamber_slug=chamber)
        for party_id in parties:
            df = await get_pivot_df(
                person_id=p_id, party_id=party_id, chamber_slug=chamber
            )
            dfs.append(df)

    new_df: pd.DataFrame = pd.concat(dfs).drop(columns=["num_comparators"])

    if person_id or policy_id:
        # just regenerate some data, so need to get the old data and add the new data in
        old_df = pd.read_parquet(Path("data", "processed", "person_policies.parquet"))

        # join the old and new data
        df = pd.concat([old_df, new_df])

        # drop duplicates, preferring later entries
        df = df.drop_duplicates(
            subset=["is_target", "person_id", "policy_id", "comparison_party"],
            keep="last",
        )
    else:
        df = new_df

    df.to_parquet(Path("data", "processed", "person_policies.parquet"))
    update_policies_hash(policy_id)
