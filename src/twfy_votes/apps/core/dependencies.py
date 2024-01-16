"""
This module contains the dependency functions for the FastAPI app.

This acts as tiered and shared logic between views, that lets the views be
simple and declarative.

"""
from __future__ import annotations

from typing import Any

from fastapi import Request

from ...helpers.static_fastapi.dependencies import dependency
from ...internal.common import absolute_url_for
from ...internal.settings import settings

universal_context: dict[str, Any] = {"settings": settings}


def _url_for(request: Request):
    # work around for debugging inside codespaces
    # can't set base_url directly, so just change how the url_for function works
    def inner(view_name: str, **kwargs: Any):
        return absolute_url_for(request, view_name, **kwargs)

    return inner


@dependency
async def GetContext(request: Request) -> dict[str, Any]:
    """
    Copy and return the universal context with the current request added
    """
    # copy and return the universal context
    context = universal_context.copy()
    context["request"] = request
    context["url_for"] = _url_for(request)
    return context
