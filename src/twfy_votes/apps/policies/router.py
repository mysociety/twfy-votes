# Views

from ...helpers.static_fastapi.static import StaticAPIRouter
from ...internal.settings import settings
from ..core.dependencies import GetContext
from ..decisions.dependencies import AllChambers
from .dependencies import (
    GetAllPolicyReports,
    GetGroupsAndPolicies,
    GetPersonPolicy,
    GetPolicy,
    GetPolicyCollection,
    GetPolicyReport,
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
async def api_policies(
    groups_and_policies: GetGroupsAndPolicies
) -> GetGroupsAndPolicies:
    return groups_and_policies


@router.get("/policies/{chamber_slug}/{status}/{group_slug}.json")
async def api_policy_collection(policies: GetPolicyCollection) -> GetPolicyCollection:
    return policies


@router.get_html("/policies/{chamber_slug}/{status}/{group_slug}")
@router.use_template("policy_collection.html")
async def policy_collection(
    context: GetContext, policy_collection: GetPolicyCollection
):
    context["policy_collection"] = policy_collection
    return context


@router.get("/policy/{policy_id}.json")
async def api_policy(policy: GetPolicy) -> GetPolicy:
    return policy


@router.get_html("/policy/{policy_id}")
@router.use_template("policy.html")
async def policy(context: GetContext, policy: GetPolicy):
    context["policy"] = policy
    context["decision_df"] = await policy.division_df(context["request"])
    return context


@router.get("/policies/reports.json")
async def api_all_reports(policy: GetAllPolicyReports) -> GetAllPolicyReports:
    return policy


@router.get_html("/policies/reports")
@router.use_template("policy_reports.html")
async def app_reports(context: GetContext, reports: GetAllPolicyReports):
    context["item"] = reports
    context["policy_level_errors"] = sum([len(x.policy_issues) for x in reports])
    context["division_level_errors"] = sum([x.len_division_issues() for x in reports])
    return context


@router.get("/policy/{policy_id}/report.json")
async def api_issue_report(policy: GetPolicyReport) -> GetPolicyReport:
    return policy


@router.get_html("/policy/{policy_id}/report")
@router.use_template("policy_report.html")
async def policy_report(context: GetContext, policy: GetPolicyReport):
    context["item"] = policy
    return context


@router.get("/person/{person_id}/records/{chamber_slug}/{party_id}.json")
async def api_person_policy(person_policy: GetPersonPolicy) -> GetPersonPolicy:
    return person_policy


@router.get_html("/person/{person_id}/records/{chamber_slug}/{party_id}")
@router.use_template("person_policies.html")
async def person_policy(context: GetContext, person_policy: GetPersonPolicy):
    context["item"] = person_policy
    return context
