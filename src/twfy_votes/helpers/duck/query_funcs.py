from pathlib import Path
from typing import Any

import pandas as pd
from jinjasql import JinjaSql  # type: ignore

from .types import (
    CompiledJinjaSQL,
    FileSourceType,
    MacroQuery,
    SQLQuery,
    TableQuery,
    ViewQuery,
)
from .url import DuckUrl


def source_to_query(source: FileSourceType | str) -> SQLQuery:
    if isinstance(source, DuckUrl):
        return SQLQuery(f"SELECT * FROM '{str(source)}'")
    elif isinstance(source, Path):
        # if csv
        if source.suffix == ".csv":
            return SQLQuery(
                "SELECT * FROM read_csv('{str(source)}', HEADER=True, AUTO_DETECT=True)"
            )
        else:
            return SQLQuery(f"SELECT * FROM '{str(source)}'")
    elif isinstance(source, pd.DataFrame):
        raise ValueError(
            "Can't convert a dataframe into a query in an abstract way, use 'register' instead"
        )
    else:
        return SQLQuery(f"SELECT * FROM '{str(source)}'")


def query_to_macro(
    name: str, args: list[str], macro: str, table: bool = False
) -> MacroQuery:
    if table:
        macro = f"table {macro}"
    return MacroQuery(
        f"""
    CREATE OR REPLACE MACRO {name}({", ".join(args)}) AS
    {macro}
    """
    )


def query_to_view(query: SQLQuery, name: str, namespace: str = "") -> ViewQuery:
    view_name = f"{namespace}.{name}" if namespace else name
    return ViewQuery(f"CREATE OR REPLACE VIEW {view_name} AS {query}")


def query_to_table(query: SQLQuery, name: str, namespace: str = "") -> TableQuery:
    view_name = f"{namespace}.{name}" if namespace else name
    return TableQuery(f"CREATE OR REPLACE TABLE {view_name} AS {query}")


class TypedJinjaSql(JinjaSql):
    def get_compiled_query(self, source: str, data: dict[str, Any]) -> CompiledJinjaSQL:
        query, bind_params = self.prepare_query(source, data)  # type: ignore
        return CompiledJinjaSQL(query=query, bind_params=bind_params)  # type: ignore


jsql = TypedJinjaSql(param_style="asyncpg")


def get_compiled_query(source: str, data: dict[str, Any]) -> CompiledJinjaSQL:
    return jsql.get_compiled_query(source, data)
