# Views

from fastapi import BackgroundTasks

from ...helpers.static_fastapi.static import StaticAPIRouter
from ...internal.settings import settings
from .db import duck_core, reload_database

router = StaticAPIRouter(template_directory=settings.template_dir)


@router.get("/functions/database_load_time")
async def database_load_time_view():
    return {
        "database_load_time": duck_core.loaded_time,
        "database_loading_status": duck_core.loading_status,
    }


@router.post("/functions/reload_database", include_in_schema=False)
async def reload_database_view(background_tasks: BackgroundTasks):
    if duck_core.loading_status in [
        duck_core.LoadingStatus.EMPTY,
        duck_core.LoadingStatus.LOADED,
    ]:
        background_tasks.add_task(reload_database)
        return {"message": "Database reload started"}
    return {"message": "Database reload already in progress."}
