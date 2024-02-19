import pytest
from fastapi.testclient import TestClient


@pytest.mark.asyncio
async def test_voting_records(client: TestClient):
    from twfy_votes.apps.policies.vr_validator import test_policy_sample

    await test_policy_sample(5)


@pytest.mark.skip(reason="Wait for intital issues to be fixed, then turn this on")
@pytest.mark.asyncio
async def test_policy_issues(client: TestClient):
    """
    Fetch currently active polices and assert there are no issues are defined
    in the PolicyReport process
    """
    from twfy_votes.apps.policies.models import PolicyReport, PolicyStatus

    reports = await PolicyReport.fetch_multiple(statuses=[PolicyStatus.ACTIVE])

    assert (
        any([x.has_issues() for x in reports]) is False
    ), "A policy has issues! Use report view for more info"


def test_voting_records_up_to_date():
    from twfy_votes.apps.policies.vr_generator import check_policy_hash

    assert (
        check_policy_hash() is True
    ), "Voting records have not been recalcuated after an update"
