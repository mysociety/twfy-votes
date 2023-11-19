from contextlib import asynccontextmanager

from fastapi import FastAPI

from ..helpers.duck.core import AsyncDuckDBManager, DuckQuery
from .settings import Settings


def get_core():
    settings = Settings()
    if settings.server_production:
        option = AsyncDuckDBManager.ConnectionOptions.FILE_DISPOSABLE
    else:
        option = AsyncDuckDBManager.ConnectionOptions.MEMORY

    return AsyncDuckDBManager(
        connection_option=option,
    )


duck_core = get_core()


def get_db_lifespan(queries: list[DuckQuery]):
    @asynccontextmanager
    async def lifespan(app: FastAPI | None = None):
        # Load the ML model
        await duck_core.get_loaded_core(queries)
        yield
        duck_core.delete_database()
        await duck_core.close()

    return lifespan
