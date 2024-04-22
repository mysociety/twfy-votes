import asyncio
from types import TracebackType
from typing import Type

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


class LifeSpanManager:
    def __init__(self, queries: list[DuckQuery]):
        self.queries = queries
        self.tasks = []

    def __call__(self, app: FastAPI | None = None):
        """
        We don't actualy need the app, but fastapi will pass it in.
        """
        self.app = app
        return self

    async def __aenter__(self):
        db_task = asyncio.create_task(duck_core.get_loaded_core(self.queries))
        self.tasks.append(db_task)
        await db_task

    async def __aexit__(
        self, exc_t: Type[BaseException], exc_v: BaseException, exc_tb: TracebackType
    ) -> None:
        for task in self.tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass  # Handle the cancellation appropriately
        self.tasks = []
        duck_core.delete_database()
        await duck_core.close()
