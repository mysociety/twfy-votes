"""
This module contains the dependency functions for the FastAPI app.

This acts as tiered and shared logic between views, that lets the views be
simple and declarative.

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
)

from fastapi import Depends

DecoratorType = TypeVar("DecoratorType")
AsyncAgnosticDependencyFunction = Callable[
    ...,
    Coroutine[Any, Any, DecoratorType]
    | Generator[DecoratorType, Any, Any]
    | AsyncGenerator[DecoratorType, Any]
    | DecoratorType,
]


def dependency_alias_for(
    dependency_class: Type[DecoratorType],
) -> Callable[[AsyncAgnosticDependencyFunction[DecoratorType]], Type[DecoratorType]]:
    """
    Tidies up FastAPI dependency injection.

    Rather than specify the function as metadata in typing.Annotated,
    instead use this as a decorator on the function.

    e.g. rather than:

    ```
    def context_dependency():
        return {"item": "value"}

    GetContext = Annotated[dict, Depends(context_dependency)]
    ```

    use:

    ```
    @dependency_alias_for(dict[str, str])
    def GetContext():
        return {"item": "value"}
    ```

    This has the same result - GetText is an Annotated object.

    The function is then not accessible directly - but the advantage is
    type checkers can check if the function matches the type annotation.

    """

    def decorator(
        func: AsyncAgnosticDependencyFunction[DecoratorType],
    ) -> Type[DecoratorType]:
        return Annotated[dependency_class, Depends(func)]

    return decorator
