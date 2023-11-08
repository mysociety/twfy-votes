# Views

from ...helpers.static_fastapi.static import StaticAPIRouter
from ...internal.settings import settings
from ..core.dependencies import GetContext
from ..decisions.dependencies import AllChambers
from .dependencies import (
    GetGroupsAndPolicies,
    GetPolicy,
    GetPolicyCollection,
)
from .models import PolicyStatus

router = StaticAPIRouter(template_directory=settings.template_dir)


@router.get_html("/policies")
@router.use_template("policies.html")
async def policies(context: GetContext, all_chambers: AllChambers):
    context["all_chambers"] = all_chambers
    context["all_statuses"] = [x for x in PolicyStatus if x != PolicyStatus.REJECTED]
    return context


@router.get("/policies.json")
async def api_policies(groups_and_policies: GetGroupsAndPolicies):
    return groups_and_policies


@router.get_html("/policies/{chamber_slug}/{status}/{group_slug}/")
@router.use_template("policy_collection.html")
async def policy_collection(
    context: GetContext, policy_collection: GetPolicyCollection
):
    context["policy_collection"] = policy_collection
    return context


@router.get("/policies/{chamber_slug}/{status}/{group_slug}.json")
async def api_policy_collection(policies: GetPolicyCollection):
    return policies


@router.get_html("/policy/{policy_id}")
@router.use_template("policy.html")
async def policy(context: GetContext, policy: GetPolicy):
    context["policy"] = policy
    context["decision_df"] = await policy.division_df(context["request"])
    return context


@router.get("/policy/{policy_id}.json")
async def api_policy(policy: GetPolicy):
    return policy
