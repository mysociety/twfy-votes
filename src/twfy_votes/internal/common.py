from typing import Any

from fastapi import APIRouter, Request
from starlette.datastructures import URL

from .settings import settings


def absolute_url_for(request: Request, __name: str, **path_params: Any) -> URL:
    if settings.server_production:
        return request.url_for(__name, **path_params)
    else:
        router: APIRouter = request.scope["router"]
        url_path = router.url_path_for(__name, **path_params)
        return url_path.make_absolute_url(base_url=settings.base_url)
