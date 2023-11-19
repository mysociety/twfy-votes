import pytest
from fastapi.testclient import TestClient


@pytest.mark.asyncio
async def test_voting_records(client: TestClient):
    from twfy_votes.apps.policies.vr_validator import test_policy_sample

    await test_policy_sample(5)
