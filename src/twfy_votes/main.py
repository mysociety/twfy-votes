from .apps.core.db import db_lifespan
from .apps.decisions import render as decisions_render
from .apps.decisions import router as decisions_router
from .apps.policies import render as policies_render
from .apps.policies import router as policies_router
from .apps.twfy_compatible import router as twfy_compatible_router
from .helpers.static_fastapi import StaticFastApi
from .internal.settings import settings

# Create the basic FastAPI app
# We've made a subclass of FastAPI that steamlines a few features:
# - Static file serving
# - Jinja2 template rendering
# - Static site rendering

app = StaticFastApi(
    render_directory=settings.render_dir,
    template_directory=settings.template_dir,
    static_directory=settings.static_dir,
    lifespan=db_lifespan,
)

app.include_router(decisions_render.router)
app.include_router(decisions_router.router)
app.include_router(policies_router.router)
app.include_router(policies_render.router)
app.include_router(twfy_compatible_router.router)
