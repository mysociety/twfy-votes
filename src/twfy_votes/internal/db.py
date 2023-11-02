from contextlib import asynccontextmanager

from fastapi import FastAPI

from ..helpers.duck.core import AsyncDuckDBManager, DuckQuery

duck_core = AsyncDuckDBManager()


def get_db_lifespan(queries: list[DuckQuery]):
    @asynccontextmanager
    async def lifespan(app: FastAPI | None = None):
        # Load the ML model
        await duck_core.get_loaded_core(queries)
        yield
        # Clean up the ML models and release the resources
        await duck_core.close()

    return lifespan
