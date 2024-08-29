"""
This module contains the dependency functions for the FastAPI app.

This acts as tiered and shared logic between views, that lets the views be
simple and declarative.

"""
from __future__ import annotations

from typing import Literal

from ...helpers.static_fastapi.dependencies import dependency
from .models import (
    AllowedChambers,
    PersonPolicyDisplay,
    Policy,
    PolicyCollection,
    PolicyGroupSlug,
    PolicyReport,
    PolicyStatus,
    PolicyTimePeriodSlug,
)


@dependency
async def GetPolicyCollection(
    group_slug: PolicyGroupSlug | None | Literal["all"] = None,
    chamber_slug: AllowedChambers | None | Literal["all"] = None,
    status: PolicyStatus | None | Literal["all"] = None,
) -> PolicyCollection:
    if group_slug == "all":
        group_slug = None
    if chamber_slug == "all":
        chamber_slug = None
    if status == "all":
        status = None

    return await PolicyCollection.fetch_from_slug(
        chamber_slug=chamber_slug, group_slug=group_slug, status=status
    )


@dependency
async def GetGroupsAndPolicies() -> list[PolicyCollection]:
    """
    Get a list of groups and policies for the sidebar
    """
    return await PolicyCollection.fetch_all()


@dependency
async def GetPolicy(policy_id: int) -> Policy:
    return await Policy.from_id(id=policy_id)


@dependency
async def GetPersonPolicy(
    person_id: int,
    chamber_slug: AllowedChambers,
    party_id: str,
    comparison_period_slug: PolicyTimePeriodSlug = PolicyTimePeriodSlug.ALL_TIME,
) -> PersonPolicyDisplay:
    return await PersonPolicyDisplay.from_person_and_party(
        person_id=person_id,
        comparison_party=party_id,
        chamber_slug=chamber_slug,
        comparison_period_slug=comparison_period_slug,
    )


@dependency
async def GetAllPolicyReports() -> list[PolicyReport]:
    """
    Get reports for active and candidate polices for interface.
    The pytest only enforces tests on active policies.
    """
    return await PolicyReport.fetch_multiple(
        statuses=[PolicyStatus.ACTIVE, PolicyStatus.CANDIDATE]
    )


@dependency
async def GetPolicyReport(policy: GetPolicy) -> PolicyReport:
    return PolicyReport.from_policy(policy=policy)
