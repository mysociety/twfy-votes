import asyncio
import os
from unittest import mock

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_database_reload():
    with mock.patch.dict(os.environ, {"SERVER_PRODUCTION": "True"}):
        from twfy_votes.main import app

        async with (
            app.router.lifespan_context(app) as _,
            AsyncClient(app=app, base_url="http://test") as client,
        ):
            # get current loaded time
            response = await client.get("/functions/database_load_time")
            assert response.status_code == 200
            load_time = response.json()["database_load_time"]

            response = await client.post("/functions/reload_database")
            assert response.status_code == 200
            assert response.json() == {"message": "Database reload started"}

            new_load_time = load_time

            # wait for reload to complete
            allowed_loops = 10
            while new_load_time == load_time:
                await asyncio.sleep(5)
                response = await client.get("/functions/database_load_time")
                assert response.status_code == 200
                new_load_time = response.json()["database_load_time"]
                allowed_loops -= 1
                assert allowed_loops > 0, "Reload failed to complete"
