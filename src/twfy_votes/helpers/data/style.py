import pandas as pd
from pydantic import BaseModel
from starlette.datastructures import URL


def style_df(df: pd.DataFrame, percentage_columns: list[str] | None = None) -> str:
    if percentage_columns is None:
        percentage_columns = []

    def format_percentage(value: float):
        return "{:.2%}".format(value)

    df = df.rename(columns=nice_headers)

    styled_df = df.style.hide(axis="index").format(  # type: ignore
        formatter={x: format_percentage for x in percentage_columns}  # type: ignore
    )

    return styled_df.to_html()  # type: ignore


class UrlColumn(BaseModel, arbitrary_types_allowed=True):
    url: URL
    text: str

    def __str__(self) -> str:
        return f'<a href="{self.url}">{self.text}</a>'


def nice_headers(s: str) -> str:
    s = s.replace("_", " ")
    return s
