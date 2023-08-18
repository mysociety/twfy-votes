from contextlib import asynccontextmanager

from fastapi import FastAPI

from ..helpers.duck.core import AsyncDuckDBManager, DuckQuery

duck_core = AsyncDuckDBManager()


def get_db_lifespan(queries: list[DuckQuery]):
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Load the ML model
        core = await duck_core.get_core()
        for query in queries:
            await core.compile(query).run_on_self()
        yield
        # Clean up the ML models and release the resources
        await duck_core.close()

    return lifespan
