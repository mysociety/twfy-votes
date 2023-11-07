from pathlib import Path

import pandas as pd
from tqdm import tqdm
from twfy_votes.apps.core.db import duck_core
from twfy_votes.apps.policies.queries import PolicyPivotTable

from ..decisions.models import AllowedChambers
from .queries import GetPersonParties, PolicyAffectedPeople


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
    return df.fillna(0.0)  # type: ignore


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

    new_df: pd.DataFrame = pd.concat(dfs).drop(columns=["num_comparators"])  # type: ignore

    if person_id or policy_id:
        # just regenerate some data, so need to get the old data and add the new data in
        old_df = pd.read_parquet(Path("data", "processed", "person_policies.parquet"))

        # join the old and new data
        df = pd.concat([old_df, new_df])  # type: ignore

        # drop duplicates, preferring later entries
        df = df.drop_duplicates(
            subset=["is_target", "person_id", "policy_id", "comparison_party"],
            keep="last",
        )
    else:
        df = new_df

    df.to_parquet(Path("data", "processed", "person_policies.parquet"))
