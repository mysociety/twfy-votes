import pytest
from fastapi.testclient import TestClient
from twfy_votes.apps.policies.vr_validator import test_policy_sample


@pytest.mark.asyncio
async def test_voting_records(client: TestClient):
    await test_policy_sample(5)
