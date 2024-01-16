"""
Helper functions for managing dependencies.
"""
from __future__ import annotations

from typing import (
    Annotated,
    Any,
    AsyncGenerator,
    Callable,
    Coroutine,
    Generator,
    Type,
    TypeVar,
    get_type_hints,
)

from fastapi import Depends

ReturnType = TypeVar("ReturnType")
AsyncAgnosticDependencyFunction = Callable[
    ...,
    Coroutine[Any, Any, ReturnType]
    | Generator[ReturnType, Any, Any]
    | AsyncGenerator[ReturnType, Any]
    | ReturnType,
]


def dependency(
    item: AsyncAgnosticDependencyFunction[ReturnType],
) -> Type[ReturnType]:
    """
    This streamlines declarations for dependency injection.

    Rather than:

    item = Annotated[str, Depends(get_item)]

    You can write:

    item = dependency(get_item)

    If the return type is specified for `get_item`, item will appear as that type.
    Otherwise, it will appear as Any.

    If there is a function that *only* returns a dependency, you can use the
    as_decorator method to use it as a decorator:

    @dependency
    async def GetItem() -> str:
        return "item"

    item: GetItem

    """
    depends_type = get_type_hints(item).get("return", Any)
    return Annotated[depends_type, Depends(item)]
