from typing import Any

from ...helpers.static_fastapi.static import StaticAPIRouter
from ...internal.settings import settings

router = StaticAPIRouter(template_directory=settings.template_dir)


@router.render_parameters_for_path("/policies")
@router.render_parameters_for_path("/policies.json")
async def no_args():
    no_args: dict[str, Any] = {}
    yield no_args
