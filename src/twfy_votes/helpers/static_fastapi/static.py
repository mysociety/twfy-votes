import asyncio
import shutil
from functools import wraps
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Coroutine,
    Iterator,
    Protocol,
    Type,
    TypeVar,
)

import aiofiles
import httpx
from fastapi import APIRouter, FastAPI, Response
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.types import DecoratedCallable
from starlette.templating import _TemplateResponse  # type: ignore
from tqdm import tqdm as tqdm_sync

AsyncDictIterator = AsyncIterator[dict[str, Any]]
AsyncTemplateResponse = Coroutine[Any, Any, _TemplateResponse]
AsyncParameterFunction = Callable[[], AsyncDictIterator]
AsyncTemplateFunction = Callable[..., AsyncTemplateResponse]
PathLike = Path | str


async def no_params():
    result: dict[str, str] = {}
    yield result
    await asyncio.sleep(0.0)


class HasGetEndpoint(Protocol):
    def get(
        self,
        path: str,
        *,
        response_class: Type[Response] = JSONResponse,
        include_in_schema: bool = True,
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        ...


class StaticRenderMixin:
    def __init__(
        self,
        *args: Any,
        template_directory: PathLike,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self.template_directory = (
            template_directory
            if isinstance(template_directory, Path)
            else Path(template_directory)
        )

        self._render_parameters: dict[str, AsyncParameterFunction] = {}

        self.templates = Jinja2Templates(directory=self.template_directory)

    def get_html(self: HasGetEndpoint, path: str):
        t = self.get(path, response_class=HTMLResponse, include_in_schema=False)
        return t

    def get_json(self: HasGetEndpoint, path: str):
        """
        Decorator to add a function that returns a dictionary of parameters
        to be passed to be passed to the view template.
        """

        def inner(func: Callable[..., Awaitable[Any]]):
            async def wrapper(*args: Any, **kwargs: Any):
                context = await func(*args, **kwargs)
                json_compatible_item_data = jsonable_encoder(context)
                return JSONResponse(content=json_compatible_item_data)

            return wrapper

        return self.get(path)(inner)

    def use_template(self, template: str):
        """
        Decorator to add a function that returns a dictionary of parameters
        to be passed to be passed to the view template.
        """

        def inner(func: Callable[..., Awaitable[dict[str, Any]]]):
            @wraps(func)
            async def wrapper(*args: Any, **kwargs: Any):
                context = await func(*args, **kwargs)
                return self.template_response(template, context)

            return wrapper

        return inner

    def template_response(
        self, name: str, context: dict[str, Any]
    ) -> _TemplateResponse:
        return self.templates.TemplateResponse(name, context)  # type: ignore

    def render_parameters_for(self, func: AsyncTemplateFunction):
        """
        Decorator to add a function that returns a dictionary of parameters to be passed to be passed to the view template.
        function should be an async function.

        """
        api_paths: dict[Callable[..., Any], str] = {
            x.endpoint: x.path  # type: ignore
            for x in self.router.routes  # type: ignore
            if hasattr(x, "endpoint")  # type: ignore
        }
        path = api_paths[func]
        return self.render_parameters_for_path(path)

    def render_parameters_for_path(self, path: str):
        """
        Decorator to add a function that returns a dictionary of parameters to be passed to be passed to the view template.
        Expects the url path of the view.
        """

        def inner(func: AsyncParameterFunction):
            self._render_parameters[path] = func
            return func

        return inner

    render_for_path = render_parameters_for_path


class StaticAPIRouter(StaticRenderMixin, APIRouter):
    ...


T = TypeVar("T")


def chunked(iterable: list[T], size: int) -> Iterator[list[T]]:
    """
    Yield successive n-sized chunks from l.
    """
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


class StaticFastApi(StaticRenderMixin, FastAPI):
    """
    Very basic static site generator over the top of FastAPI.
    """

    def __init__(
        self,
        *args: Any,
        render_directory: PathLike,
        static_directory: PathLike,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)
        self.render_directory = (
            render_directory
            if isinstance(render_directory, Path)
            else Path(render_directory)
        )
        self.static_directory = (
            static_directory
            if isinstance(static_directory, Path)
            else Path(static_directory)
        )

        self.render_directory.mkdir(parents=True, exist_ok=True)
        self._render_parameters: dict[str, AsyncParameterFunction] = {}

        self.mount(
            "/static", StaticFiles(directory=self.static_directory), name="static"
        )

    def include_router(self, router: APIRouter, *args: Any, **kwargs: Any):
        super().include_router(router, *args, **kwargs)
        if isinstance(router, StaticAPIRouter):
            # if there is a prefix, add that to the paths
            for k, v in router._render_parameters.items():
                if router.prefix:
                    k = router.prefix + k
                self._render_parameters[k] = v

    def render(self):
        print(f"Publishing to {self.render_directory}")
        print("Running render")
        asyncio.run(self.async_render())
        print("Moving static files")
        shutil.copytree(
            self.static_directory, self.render_directory / "static", dirs_exist_ok=True
        )

    async def async_render(self):
        """
        Activate the context to load databases for rendering
        """
        async with self.router.lifespan_context(self):  # type: ignore
            return await self._async_render()

    async def _async_render(self) -> None:
        """
        async function to render the site.
        Any functions that have been decorated with `static_parameters_for` will be called and the results passed to the view.
        """

        client = httpx.AsyncClient(app=self, base_url="http://testserver")

        api_path_formats: dict[str, str] = {
            x.path: x.path_format  # type: ignore
            for x in self.router.routes
            if hasattr(x, "endpoint")  # type: ignore
        }

        endpoint_tasks: list[asyncio.Task[Any]] = []

        async def endpoint_with_parameters(
            path_format: str, parameters: dict[str, Any]
        ):
            path_with_parms = path_format.format(**parameters)
            result: httpx.Response = await client.get(path_with_parms)  # type: ignore
            return result, parameters

        # chunk_size manages the number of currently queued tasks
        # higher is more efficent, but needs more memory, and gives less feedback.
        chunk_size = 100
        for path, parameter_generator in self._render_parameters.items():
            path_format = api_path_formats[path]

            raw_parameters = [x async for x in parameter_generator()]

            path_counter = tqdm_sync(
                total=len(raw_parameters), desc=f"Rendering `{path}`"
            )

            for parameters_chunk in chunked(raw_parameters, chunk_size):
                endpoint_tasks = []
                for parameters in parameters_chunk:
                    async_task = asyncio.create_task(
                        endpoint_with_parameters(path_format, parameters)
                    )
                    endpoint_tasks.append(async_task)

                for async_task in asyncio.as_completed(endpoint_tasks):
                    response, parameters = await async_task
                    body = response.content.decode()

                    destination_path = path.format(**parameters)
                    if destination_path.startswith("/"):
                        destination_path = destination_path[1:]
                    file_path = self.render_directory / destination_path
                    # if a directory, add 'index.html'
                    if file_path.is_dir():
                        file_path = file_path / "index.html"
                    # if no file extension, add '.html'
                    if not file_path.suffix:
                        file_path = file_path.with_suffix(".html")
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    async with aiofiles.open(file_path, mode="w") as f:
                        await f.write(body)

                path_counter.update(len(parameters_chunk))
