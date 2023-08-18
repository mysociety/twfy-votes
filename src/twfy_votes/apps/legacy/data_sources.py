"""
This module loads legacy data from public whip to populate data here
"""


from ...helpers.duck import DuckQuery, DuckUrl

duck = DuckQuery()

public_whip = DuckUrl(
    "https://pages.mysociety.org/publicwhip-data/data/public_whip_data/latest"
)


@duck.as_table
class pw_dyn_dreamvote:
    source = public_whip / "pw_dyn_dreamvote.parquet"


@duck.as_table
class pw_dyn_dreammp:
    source = public_whip / "pw_dyn_dreammp.parquet"
