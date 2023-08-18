"""
This module contains the dependency functions for the FastAPI app.

This acts as tiered and shared logic between views, that lets the views be
simple and declarative.

"""
from __future__ import annotations

from typing import Literal

from ...helpers.static_fastapi.dependencies import dependency_alias_for
from .models import (
    AllowedChambers,
    Policy,
    PolicyCollection,
    PolicyGroupSlug,
    PolicyStatus,
)


@dependency_alias_for(PolicyCollection)
async def GetPolicyCollection(
    group_slug: PolicyGroupSlug | None | Literal["all"] = None,
    chamber_slug: AllowedChambers | None | Literal["all"] = None,
    status: PolicyStatus | None | Literal["all"] = None,
):
    if group_slug == "all":
        group_slug = None
    if chamber_slug == "all":
        chamber_slug = None
    if status == "all":
        status = None

    return await PolicyCollection.fetch_from_slug(
        chamber_slug=chamber_slug, group_slug=group_slug, status=status
    )


@dependency_alias_for(list[PolicyCollection])
async def GetGroupsAndPolicies():
    """
    Get a list of groups and policies for the sidebar
    """
    return await PolicyCollection.fetch_all()


@dependency_alias_for(Policy)
async def GetPolicy(policy_id: int):
    return await Policy.from_id(id=policy_id)
