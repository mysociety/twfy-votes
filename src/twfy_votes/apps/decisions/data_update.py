"""
Module to store functions that create data sources that are saved as files rather than recalcualted at start up
"""

from pathlib import Path

import rich

from ..core.db import duck_core
from .analysis import get_commons_clusters  # type: ignore


async def process_cached_tables():
    await duck_core.create_cached_queries()


async def create_commons_cluster():
    rich.print("[green]Creating Commons Votes clusters[/green]")

    duck = await duck_core.child_query()

    query = """
    select
        pw_divisions_gov_with_counts.division_id as division_id,
        sum(case when grouping = 'Other' then for_motion else 0 end) as opp_aye,
        sum(case when grouping = 'Other' then against_motion else 0 end) as opp_no,
        sum(case when grouping = 'Government' then for_motion else 0 end) as gov_aye,
        sum(case when grouping = 'Government' then against_motion else 0 end) as gov_no,
        650 - (
            sum(case when grouping = 'Other' then for_motion else 0 end) +
            sum(case when grouping = 'Other' then against_motion else 0 end) +
            sum(case when grouping = 'Government' then for_motion else 0 end) +
            sum(case when grouping = 'Government' then against_motion else 0 end)
        ) as other
    from pw_divisions_gov_with_counts
    join pw_division on (pw_division.division_id = pw_divisions_gov_with_counts.division_id)
    where pw_division.house = 'commons'
    group by pw_divisions_gov_with_counts.division_id
    order by pw_divisions_gov_with_counts.division_id
    """

    df = await duck.compile(query).df()
    df["cluster"] = get_commons_clusters(df, quiet=False)
    df = df[["division_id", "cluster"]]
    df.to_parquet(Path("data", "processed", "voting_clusters.parquet"))
