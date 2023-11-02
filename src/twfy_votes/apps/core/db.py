# This notebook imports legacy publicwhip policy information into yaml files that power new database

from ...internal.db import duck_core, get_db_lifespan
from ..decisions.data_sources import duck as decisions_duck
from ..policies.data_sources import duck as policies_duck

data_sources = [decisions_duck, policies_duck]


class CommonCore:
    def __init__(self):
        self.loaded = False

    async def load_common_core(self):
        if not self.loaded:
            self.loaded = True
            await duck_core.get_loaded_core(data_sources)
        return duck_core


load_common_core = CommonCore().load_common_core

db_lifespan = get_db_lifespan(data_sources)
