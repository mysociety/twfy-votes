import pytest
from fastapi.testclient import TestClient


@pytest.fixture(scope="session")
def client():
    from twfy_votes.main import app

    with TestClient(app) as client:
        yield client
