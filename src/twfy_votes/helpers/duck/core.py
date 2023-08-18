"""
Helper functions for interacting with sync and async duckdb connections.

You can use a basic DuckQuery to construct and store queries.

For things that actually talk to duckdb, call factory methods instead.

DuckQuery.connect()
and DuckQuery.async_create() will create a new async connection.

"""

from __future__ import annotations

import random
from pathlib import Path
from typing import (
    Any,
    Generic,
    Type,
    TypeVar,
)

import aioduckdb
import duckdb
from typing_extensions import Self

from .funcs import get_name
from .query_funcs import (
    get_compiled_query,
    query_to_macro,
    query_to_table,
    query_to_view,
    source_to_query,
)
from .response import AsyncDuckResponse, DuckResponse, ResponseType
from .types import (
    CompiledJinjaSQL,
    ConnectionType,
    DataSourceValue,
    DuckMacro,
    DuckorSourceViewType,
    DuckView,
    DuckViewInstance,
    DuckViewType,
    PythonDataSource,
    PythonDataSourceCallableProtocol,
    SourceViewType,
)
from .url import DuckUrl

T = TypeVar("T")


class DuckQuery:
    def __init__(
        self,
        namespace: str | None = "",
    ):
        self.queries: list[str] = []
        if namespace is None:
            self.namespace = "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=10))
        else:
            self.namespace = namespace
        self.source_lookup: list[str] = []
        self.data_sources: list[DataSourceValue] = []

    @classmethod
    async def _async_create_connection(cls):
        connection = await aioduckdb.connect(":memory:")
        await connection.execute("INSTALL httpfs")
        return connection

    @classmethod
    async def async_create(
        cls, connection: Any = None, namespace: str = ""
    ) -> ConnectedDuckQuery[AsyncDuckResponse]:
        if not connection:
            connection = await cls._async_create_connection()
        return ConnectedDuckQuery[AsyncDuckResponse](
            namespace=namespace, connection=connection, response_type=AsyncDuckResponse
        )

    @classmethod
    def connect(
        cls, namespace: str = "", connection: ConnectionType | None = NotImplemented
    ) -> ConnectedDuckQuery[DuckResponse]:
        if not connection:
            connection = duckdb.connect(":memory:")  # type: ignore

        return ConnectedDuckQuery[DuckResponse](
            namespace=namespace, connection=connection, response_type=DuckResponse
        )

    def child_query(self, namespace: str | None = None) -> Self:
        """
        Return a new DuckQuery with the same connection but a different namespace
        """
        return self.__class__(
            namespace=namespace,
        )

    def construct_query(self, variables: dict[str, Any] = {}) -> CompiledJinjaSQL:
        complex_query = ";".join(self.queries)

        if self.namespace:
            for source in self.source_lookup:
                complex_query = complex_query.replace(
                    source, f"{self.namespace}.{source}"
                )

        if variables:
            complex_query = get_compiled_query(complex_query, variables)
        else:
            complex_query = CompiledJinjaSQL(query=complex_query, bind_params=tuple())

        return complex_query

    def as_macro(self, item: DuckMacro):
        name = get_name(item)

        args = getattr(item, "args", None)

        if args is None:
            raise ValueError("Macro must have an args attribute")

        macro = getattr(item, "macro", None)

        if macro is None:
            raise ValueError("Macro must have a macro method")

        query = query_to_macro(name, args, macro)
        self.queries.append(query)

        return item

    def as_python_source(self, item: PythonDataSource) -> PythonDataSource:
        if isinstance(item, PythonDataSourceCallableProtocol):
            source = item.get_source()
        else:
            source = item.source
        table_name = get_name(item)
        self.data_sources.append(
            DataSourceValue(name="_source_" + table_name, item=source)
        )

        table_query = (
            f"CREATE TABLE {table_name} AS (SELECT * FROM _source_{table_name})"
        )

        self.queries.append(table_query)

        return item

    def as_source(self, item: SourceViewType, to_table: bool = False) -> SourceViewType:
        """
        Decorator to store a source as part of a longer running query
        """

        name = get_name(item)
        source = getattr(item, "source", None)

        if isinstance(source, str):
            # if starts with http, treat as a url
            if source.startswith("http"):
                source = DuckUrl(source)
            else:
                source = Path(source)

        if source is None:
            raise ValueError("Class must have a source attribute")

        if to_table:
            query_func = query_to_table
        else:
            query_func = query_to_view

        query = query_func(source_to_query(source), name=name, namespace=self.namespace)
        self.queries.append(query)
        return item

    def as_view(self, item: DuckViewType, as_table: bool = False) -> DuckViewType:
        """
        Decorator to stash a view as part of a longer running query
        """

        name = get_name(item)
        query = getattr(item, "query", None)

        if query is None:
            raise ValueError("Class must have a query method")

        self.source_lookup.append(name)
        if as_table:
            store_as_view = query_to_table(query, name=name, namespace=self.namespace)
        else:
            store_as_view = query_to_view(query, name=name, namespace=self.namespace)
        self.queries.append(store_as_view)

        return item

    def as_query(self, item: DuckView) -> DuckView:
        """
        Decorator to convert something implementing DuckView to a DuckResponse
        """

        query = getattr(item, "query", None)

        if query is None:
            raise ValueError("Class must have a query method")

        self.queries.append(query)

        return item

    def as_table(self, item: DuckorSourceViewType) -> DuckorSourceViewType:
        """
        Decorator to convert something implementing SourceView to a DuckResponse
        """
        if isinstance(item, DuckView):
            return self.as_view(item, as_table=True)
        return self.as_source(item, to_table=True)


class ConnectedDuckQuery(DuckQuery, Generic[ResponseType]):
    def __init__(
        self,
        namespace: str | None,
        connection: ConnectionType,
        response_type: Type[ResponseType],
    ):
        super().__init__(namespace=namespace)

        self.response_type = response_type
        self.connection = connection

    def child_query(self, namespace: str | None = None) -> Self:
        """
        Return a new DuckQuery with the same connection but a different namespace
        """
        return self.__class__(
            namespace=namespace,
            connection=self.connection,
            response_type=self.response_type,
        )

    def compile(
        self,
        query: str | DuckQuery | Type[DuckView] | DuckViewInstance,
        variables: dict[str, Any] = {},
    ) -> ResponseType:
        if hasattr(query, "query"):
            _query = getattr(query, "query")
            if hasattr(query, "params"):
                _params = getattr(query, "params")
                _query = get_compiled_query(_query, _params)
        elif isinstance(query, DuckQuery):
            _query = query.construct_query(variables)
            self.data_sources += query.data_sources
        elif isinstance(query, str):
            _query = query
            if variables:
                _query = get_compiled_query(_query, variables)
        else:
            raise ValueError(
                "Can only compile a string, DuckQuery or object with a 'query' property"
            )

        if len(self.queries) > 0:
            raise ValueError("Can only use 'compile' on a fresh or empty query.")

        return self.response_type(self.connection, _query, self.data_sources)  # type: ignore

    def compile_queue(self, variables: dict[str, Any] = {}) -> ResponseType:
        _query = self.construct_query(variables)
        self.queries = []
        return self.response_type(self.connection, _query, self.data_sources)  # type: ignore


class AsyncDuckDBManager:

    """
    Singleton class to manage the duckdb connection.
    Should be initalised only once in a project.
    """

    def __init__(self):
        self._core: ConnectedDuckQuery[AsyncDuckResponse] | None = None

    async def close(self):
        if isinstance(self._core, DuckQuery):
            if isinstance(self._core.connection, aioduckdb.Connection):
                return await self._core.connection.close()
        raise ValueError("Trying to close database connection that doesn't exist")

    async def get_core(self):
        # create core duck

        if self._core:
            return self._core

        self._core = await DuckQuery.async_create()
        return self._core

    async def child_query(self, namespace: str | None = None):
        core = await self.get_core()
        return core.child_query(namespace)
