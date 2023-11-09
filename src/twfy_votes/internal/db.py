import os
import random
import string
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI

from ..helpers.duck.core import AsyncDuckDBManager, DuckQuery

# make random database name

SERVER_PRODUCTION = bool(os.environ.get("SERVER_PRODUCTION", False))

# create random five character string


if SERVER_PRODUCTION:
    # Random string because this app is not *meant* to share a database
    # Using a file is to reduce memory requirements in production
    random_string = "".join(random.choices(string.ascii_lowercase + string.digits, k=5))
    duck_core = AsyncDuckDBManager(
        database=Path("databases", f"duck_{random_string}.duckdb"),
        destroy_existing=True,
    )
else:
    # pure memory approach
    duck_core = AsyncDuckDBManager()


def get_db_lifespan(queries: list[DuckQuery]):
    @asynccontextmanager
    async def lifespan(app: FastAPI | None = None):
        # Load the ML model
        await duck_core.get_loaded_core(queries)
        yield
        duck_core.delete_database()
        await duck_core.close()

    return lifespan
