import pytest
from fastapi.testclient import TestClient
from twfy_votes.main import app


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as client:
        yield client
