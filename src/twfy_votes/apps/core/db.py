# This notebook imports legacy publicwhip policy information into yaml files that power new database

from ...helpers.duck.core import AsyncDuckDBManager
from ...internal.db import duck_core, get_db_lifespan
from ..decisions.data_sources import duck as decisions_duck
from ..policies.data_sources import duck as policies_duck

data_sources = [decisions_duck, policies_duck]


class CommonCore:
    def __init__(self):
        self.loaded = False

    async def load_common_core(self, force_reload: bool = False):
        if not self.loaded or force_reload:
            self.loaded = True
            await duck_core.get_loaded_core(data_sources)
        return duck_core


async def reload_database():
    """
    Function to reload the database from scatch while it is still running.
    """
    from ..decisions.data_update import (
        create_commons_cluster,
        process_cached_tables,
    )

    duck_core.loading_status = duck_core.LoadingStatus.CREATING_CACHE

    # Create a seperate running process to recreate cached files
    reload_core = AsyncDuckDBManager(
        connection_option=AsyncDuckDBManager.ConnectionOptions.FILE_DISPOSABLE
    )

    await reload_core.get_loaded_core(data_sources)

    # recreate cached tables
    await process_cached_tables(reload_core)
    await create_commons_cluster(reload_core)
    await reload_core.close()

    # reload source files into current core - as cached tables will have updated
    await duck_core.get_loaded_core(data_sources)


load_common_core = CommonCore().load_common_core

db_lifespan = get_db_lifespan(data_sources)
