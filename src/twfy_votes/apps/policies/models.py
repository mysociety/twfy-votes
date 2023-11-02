from typing import Any, ClassVar

import pandas as pd
from fastapi import Request
from pydantic import Field, computed_field, model_validator
from typing_extensions import Self

from ...helpers.data.models import ProjectBaseModel as BaseModel
from ...helpers.data.models import StrEnum
from ...helpers.data.style import UrlColumn, style_df
from ...internal.common import absolute_url_for
from ...internal.db import duck_core
from ..decisions.models import (
    AgreementInfo,
    AllowedChambers,
    Chamber,
    DivisionBreakdown,
    DivisionInfo,
    PartialAgreement,
    PartialDivision,
)
from ..decisions.queries import DivisionBreakDownQuery
from .queries import (
    AllPolicyQuery,
    GroupStatusPolicyQuery,
    PolicyIdQuery,
)


def nice_headers(s: str) -> str:
    s = s.replace("_", " ")
    return s


class PolicyStrength(StrEnum):
    """
    This is the strength of the relationship between the motion and the policy.
    Labelled strong and weak for historical purposes - but the precise meaning of that in a policy
    is defined by strength meaning at the policy level.
    """

    WEAK = "weak"
    STRONG = "strong"


class StrengthMeaning(StrEnum):
    """
    We have changed what strong and weak means overtime.
    This is for keeping track of a policy's current conversion status.
    """

    CLASSIC = "classic"  # complex calculation of strong and weak votes
    V2 = "v2"  # Only strong votes count for big stats, weak votes are informative


class PolicyDirection(StrEnum):
    """
    This is the relatonship between the motion and the policy.
    Agree means that if the motion passes it's good for the policy.
    """

    AGREE = "agree"
    AGAINST = "against"
    NEUTRAL = "neutral"


class PolicyGroupSlug(StrEnum):
    HEALTH = "health"
    MISC = "misc"
    SOCIAL = "social"
    REFORM = "reform"
    FOREIGNPOLICY = "foreignpolicy"
    ENVIRONMENT = "environment"
    EDUCATION = "education"
    TAXATION = "taxation"
    BUSINESS = "business"
    TRANSPORT = "transport"
    HOUSING = "housing"
    HOME = "home"
    JUSTICE = "justice"
    WELFARE = "welfare"


class PolicyGroup(BaseModel):
    slug: PolicyGroupSlug

    policy_descs: ClassVar[dict[str, str]] = {
        PolicyGroupSlug.BUSINESS: "Business and the Economy",
        PolicyGroupSlug.REFORM: "Constitutional Reform",
        PolicyGroupSlug.EDUCATION: "Education",
        PolicyGroupSlug.ENVIRONMENT: "Environmental Issues",
        PolicyGroupSlug.TAXATION: "Taxation and Employment",
        PolicyGroupSlug.FOREIGNPOLICY: "Foreign Policy and Defence",
        PolicyGroupSlug.HEALTH: "Health",
        PolicyGroupSlug.HOME: "Home Affairs",
        PolicyGroupSlug.HOUSING: "Housing",
        PolicyGroupSlug.JUSTICE: "Justice",
        PolicyGroupSlug.MISC: "Miscellaneous Topics",
        PolicyGroupSlug.SOCIAL: "Social Issues",
        PolicyGroupSlug.WELFARE: "Welfare, Benefits and Pensions",
        PolicyGroupSlug.TRANSPORT: "Transport",
    }

    @computed_field
    @property
    def name(self) -> str:
        return self.policy_descs[self.slug]


class PolicyStatus(StrEnum):
    ACTIVE = "active"
    CANDIDATE = "candidate"
    DRAFT = "draft"
    REJECTED = "rejected"


class LinkStatus(StrEnum):
    ACTIVE = "active"
    DRAFT = "draft"


class DivisionType(StrEnum):
    DIVISION = "division"
    AGREEMENT = "agreement"


class PartialPolicyDecisionLink(BaseModel):
    division: PartialDivision | None = None
    agreement: PartialAgreement | None = None
    alignment: PolicyDirection
    strength: PolicyStrength = PolicyStrength.WEAK
    status: LinkStatus = LinkStatus.ACTIVE
    notes: str = ""

    @computed_field
    @property
    def decision_type(self) -> str:
        if self.division:
            return DivisionType.DIVISION
        elif self.agreement:
            return DivisionType.AGREEMENT
        else:
            raise ValueError("Must have agreement or division")

    @computed_field
    @property
    def decision_key(self) -> str:
        if self.division:
            return self.division.key
        elif self.agreement:
            return self.agreement.key
        else:
            raise ValueError("Must have agreement or division")

    @model_validator(mode="after")
    def check_decision_type(self):
        """
        A decision link might go to either a division or agreement.
        But must be either or, not none or both.
        """
        if self.agreement is None and self.division is None:
            raise ValueError("Must have agreement or division")
        elif self.agreement and self.division:
            raise ValueError("Must have agreement or division, not both")
        else:
            return self


class PolicyDecisionLink(BaseModel):
    decision: DivisionInfo | AgreementInfo
    alignment: PolicyDirection
    strength: PolicyStrength = PolicyStrength.WEAK
    status: LinkStatus = LinkStatus.ACTIVE
    notes: str = ""

    @computed_field
    @property
    def decision_type(self) -> str:
        match self.decision:
            case DivisionInfo():
                return DivisionType.DIVISION
            case AgreementInfo():
                return DivisionType.AGREEMENT
            case _:  # type: ignore
                raise ValueError("Must have agreement or division")

    @classmethod
    async def from_partials(
        cls, partials: list[PartialPolicyDecisionLink]
    ) -> list[Self]:
        divisions = [x.division for x in partials if x.division]
        divisions = await DivisionInfo.from_partials(partials=divisions)

        agreements = [x.agreement for x in partials if x.agreement]
        agreements = await AgreementInfo.from_partials(partials=agreements)

        all_options = divisions + agreements
        lookup: dict[str, AgreementInfo | DivisionInfo] = {
            x.key: x for x in all_options
        }
        all_decisions: list[AgreementInfo | DivisionInfo] = []
        for decision in partials:
            all_decisions.append(lookup[decision.decision_key])

        full_links: list[PolicyDecisionLink] = []
        for decision, partial in zip(all_decisions, partials):
            full_links.append(
                PolicyDecisionLink(
                    decision=decision,
                    alignment=partial.alignment,
                    strength=partial.strength,
                    status=partial.status,
                    notes=partial.notes,
                )
            )
        return full_links


class PartialPolicy(BaseModel):
    """
    Version of policy object for reading and writing from basic storage.
    Doesn't store full details of related decisions etc.
    """

    id: int = Field(
        description="Preverse existing public whip ids as URLs reflect these in TWFY. New ID should start at 10000"
    )

    name: str
    chamber_id: AllowedChambers
    context_description: str
    policy_description: str
    notes: str = ""
    status: PolicyStatus
    group_ids: list[PolicyGroupSlug]
    strength_meaning: StrengthMeaning = StrengthMeaning.V2
    highlightable: bool = Field(
        description="Policy can be drawn out as a highlight on page if no calculcated 'interesting' votes"
    )
    decision_links_refs: list[PartialPolicyDecisionLink]

    def model_dump_reduced(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """
        Tidy up YAML representation a bit
        """
        di = self.model_dump(*args, **kwargs)

        for ref in di["decision_links_refs"]:
            if ref["agreement"] is None:
                del ref["agreement"]
            if ref["division"] is None:
                del ref["division"]
            if ref["division"]:
                del ref["division"]["key"]
            del ref["decision_key"]
        return di


class Policy(PartialPolicy):
    chamber: Chamber
    groups: list[PolicyGroup]
    decision_links: list[PolicyDecisionLink]

    @classmethod
    async def from_partials(cls, partials: list[PartialPolicy]) -> list[Self]:
        # get all links in a single list

        decisions = [x.decision_links_refs for x in partials]
        decisions = [x for y in decisions for x in y]
        full_decisions = await PolicyDecisionLink.from_partials(partials=decisions)

        # create a lookup from a PartialPolicyDecisionLink to a PolicyDecisionLink
        lookup: dict[str, PolicyDecisionLink] = {
            x.decision_key: y for x, y in zip(decisions, full_decisions)
        }

        return [
            cls(
                name=x.name,
                id=x.id,
                chamber_id=x.chamber_id,
                chamber=Chamber(slug=x.chamber_id),
                context_description=x.context_description,
                policy_description=x.policy_description,
                notes=x.notes,
                status=x.status,
                groups=[PolicyGroup(slug=y) for y in x.group_ids],
                group_ids=x.group_ids,
                strength_meaning=x.strength_meaning,
                highlightable=x.highlightable,
                decision_links=[lookup[y.decision_key] for y in x.decision_links_refs],
                decision_links_refs=x.decision_links_refs,
            )
            for x in partials
        ]

    @classmethod
    async def for_collection(
        cls,
        group: PolicyGroupSlug | None = None,
        chamber: AllowedChambers | None = None,
        status: PolicyStatus | None = None,
    ) -> list[Self]:
        duck = await duck_core.child_query()

        if group or status or chamber:
            query = GroupStatusPolicyQuery(group=group, chamber=chamber, status=status)
        else:
            query = AllPolicyQuery()

        policies = await query.to_model_list(
            duck=duck,
            model=PartialPolicy,
            validate=AllPolicyQuery.validate.NO_VALIDATION,
        )

        return await cls.from_partials(partials=policies)

    @classmethod
    async def from_id(cls, id: int) -> Self:
        duck = await duck_core.child_query()
        partial = await PolicyIdQuery(id=id).to_model_single(
            duck=duck, model=PartialPolicy
        )
        full = await cls.from_partials(partials=[partial])
        return full[0]

    async def decision_df(self, request: Request):
        duck = await duck_core.child_query()
        all_decisions = [x.model_dump() for x in self.decision_links]
        decision_infos = [
            x.decision.division_id
            for x in self.decision_links
            if isinstance(x.decision, DivisionInfo)
        ]
        decision_breakdowns = await DivisionBreakDownQuery(
            division_ids=decision_infos
        ).to_model_list(
            duck=duck,
            model=DivisionBreakdown,
        )

        # need to rearrange to match the right order in DivisionBreakdown
        breakdown_lookup: dict[int, DivisionBreakdown] = {
            x.division_id: x for x in decision_breakdowns
        }

        breakdown_in_order = [
            breakdown_lookup[x.decision.division_id]
            if isinstance(x.decision, DivisionInfo)
            else None
            for x in self.decision_links
        ]

        # need to make participant count line up

        df = pd.DataFrame(data=all_decisions)
        df["decision"] = [
            UrlColumn(url=x.decision.url(request), text=x.decision.division_name)
            for x in self.decision_links
        ]
        df["uses_powers"] = [
            x.decision.motion_uses_powers() for x in self.decision_links
        ]
        df["participant_count"] = [
            x.vote_participant_count if x else 0 for x in breakdown_in_order
        ]
        df["voting_cluster"] = [x.decision.voting_cluster for x in self.decision_links]

        banned_columns = ["decision_type", "notes"]
        df = df.drop(columns=banned_columns)  # type: ignore
        return style_df(df=df)

    def url(self, request: Request):
        return absolute_url_for(
            request,
            "policy",
            policy_id=self.id,
        )


class PolicyCollection(BaseModel):
    group: PolicyGroup | None = None
    chamber: Chamber | None = None
    status: PolicyStatus | None = None
    policies: list[Policy]

    def grouped_policies(self) -> dict[str, list[Policy]]:
        grouped: dict[str, list[Policy]] = {}
        for policy in self.policies:
            for group in policy.groups:
                if group.name not in grouped:
                    grouped[group.name] = []
                grouped[group.name].append(policy)

        # resort dictionary alphabetically by keys
        grouped = dict(sorted(grouped.items()))

        return grouped

    @computed_field
    @property
    def name(self) -> str:
        if self.group:
            group_name = self.group.name
        else:
            group_name = "All"
        if self.status:
            status_name = self.status
        else:
            status_name = "All"
        if self.chamber:
            chamber_name = self.chamber.name
        else:
            chamber_name = "All"

        return f"{chamber_name} - {group_name}  - {status_name}"

    @classmethod
    async def fetch_from_slug(
        cls,
        group_slug: PolicyGroupSlug | None = None,
        chamber_slug: AllowedChambers | None = None,
        status: PolicyStatus | None = None,
    ) -> Self:
        policies = await Policy.for_collection(
            group=group_slug, chamber=chamber_slug, status=status
        )
        return cls(
            group=PolicyGroup(slug=group_slug) if group_slug else None,
            chamber=Chamber(slug=chamber_slug) if chamber_slug else None,
            status=status,
            policies=policies,
        )

    @classmethod
    async def fetch_all(cls) -> list[Self]:
        return [await cls.fetch_from_slug(slug) for slug in PolicyGroupSlug]
