from .apps.decisions import render as decisions_render
from .apps.decisions import router as decisions_router
from .apps.decisions.data_sources import duck as decisions_duck
from .apps.policies import render as policies_render
from .apps.policies import router as policies_router
from .apps.policies.data_sources import duck as policies_duck
from .helpers.static_fastapi import StaticFastApi
from .internal.db import get_db_lifespan
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
    lifespan=get_db_lifespan(
        [decisions_duck, policies_duck]
    ),  # add any database ducks to be loaded on startup here
)

app.include_router(decisions_render.router)
app.include_router(decisions_router.router)
app.include_router(policies_router.router)
app.include_router(policies_render.router)
