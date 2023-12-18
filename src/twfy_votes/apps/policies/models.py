from __future__ import annotations

from typing import Any, ClassVar, Generic, TypeVar, overload

import pandas as pd
from fastapi import Request
from pydantic import Field, computed_field
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
    Person,
)
from ..decisions.queries import DivisionBreakDownQuery
from .queries import (
    AllPolicyQuery,
    GroupStatusPolicyQuery,
    PolicyDistributionPersonQuery,
    PolicyDistributionQuery,
    PolicyIdQuery,
)

PartialDecisionType = TypeVar("PartialDecisionType", PartialAgreement, PartialDivision)
InfoType = TypeVar("InfoType", AgreementInfo, DivisionInfo)


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
    SIMPLIFIED = "simplified"  # Only strong votes count for big stats, weak votes are informative


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


class DecisionType(StrEnum):
    DIVISION = "division"
    AGREEMENT = "agreement"


class PartialPolicyDecisionLink(BaseModel, Generic[PartialDecisionType]):
    decision: PartialDecisionType
    alignment: PolicyDirection
    strength: PolicyStrength = PolicyStrength.WEAK
    status: LinkStatus = LinkStatus.ACTIVE
    notes: str = ""

    @computed_field
    @property
    def decision_type(self) -> str:
        match self.decision:
            case PartialDivision():
                return DecisionType.DIVISION
            case PartialAgreement():
                return DecisionType.AGREEMENT
            case _:  # type: ignore
                raise ValueError("Must have agreement or division")

    @computed_field
    @property
    def decision_key(self) -> str:
        return self.decision.key


class PolicyDecisionLink(BaseModel, Generic[InfoType]):
    decision: InfoType
    alignment: PolicyDirection
    strength: PolicyStrength = PolicyStrength.WEAK
    status: LinkStatus = LinkStatus.ACTIVE
    notes: str = ""

    @computed_field
    @property
    def decision_type(self) -> str:
        match self.decision:
            case DivisionInfo():
                return DecisionType.DIVISION
            case AgreementInfo():
                return DecisionType.AGREEMENT
            case _:  # type: ignore
                raise ValueError("Must have agreement or division")

    @overload
    @classmethod
    async def from_partials(
        cls, partials: list[PartialPolicyDecisionLink[PartialAgreement]]
    ) -> list[PolicyDecisionLink[AgreementInfo]]:
        ...

    @overload
    @classmethod
    async def from_partials(
        cls, partials: list[PartialPolicyDecisionLink[PartialDivision]]
    ) -> list[PolicyDecisionLink[DivisionInfo]]:
        ...

    @classmethod
    async def from_partials(
        cls, partials: list[PartialPolicyDecisionLink[Any]]
    ) -> list[PolicyDecisionLink[Any]]:
        if len(partials) == 0:
            return []

        # get first type of decision
        decisions = [x.decision for x in partials]

        decision_types = [x.decision_type for x in partials]
        decision_type = DecisionType(decision_types[0])

        if len(set(decision_types)) != 1:
            raise ValueError("All decisions must be the same type to use partials")

        full_links: list[PolicyDecisionLink[Any]] = []
        match decision_type:
            case DecisionType.DIVISION:
                decisions = await DivisionInfo.from_partials(partials=decisions)

                # need to rearrange decisions so it's in the correct order as partials
                decision_lookup = {x.key: x for x in decisions}
                decisions = [decision_lookup[x.decision_key] for x in partials]

                for decision, partial in zip(decisions, partials):
                    full_links.append(
                        PolicyDecisionLink[DivisionInfo](
                            decision=decision,
                            alignment=partial.alignment,
                            strength=partial.strength,
                            status=partial.status,
                            notes=partial.notes,
                        )
                    )
            case DecisionType.AGREEMENT:
                decisions = await AgreementInfo.from_partials(partials=decisions)
                for decision, partial in zip(decisions, partials):
                    full_links.append(
                        PolicyDecisionLink[AgreementInfo](
                            decision=decision,
                            alignment=partial.alignment,
                            strength=partial.strength,
                            status=partial.status,
                            notes=partial.notes,
                        )
                    )

        return full_links


class PolicyBase(BaseModel):
    """
    Version of policy object for reading and writing from basic storage.
    Doesn't store full details of related decisions etc.
    """

    id: int = Field(
        description="Preverse existing public whip ids as URLs reflect these in TWFY. New ID should start at 10000"
    )

    name: str
    context_description: str
    policy_description: str
    notes: str = ""
    status: PolicyStatus
    strength_meaning: StrengthMeaning = StrengthMeaning.SIMPLIFIED
    highlightable: bool = Field(
        description="Policy can be drawn out as a highlight on page if no calculcated 'interesting' votes"
    )


class PartialPolicy(PolicyBase):
    """
    Version of policy object for reading and writing from basic storage.
    Doesn't store full details of related decisions etc.
    """

    chamber: AllowedChambers
    groups: list[PolicyGroupSlug]
    division_links: list[PartialPolicyDecisionLink[PartialDivision]]
    agreement_links: list[PartialPolicyDecisionLink[PartialAgreement]]

    def model_dump_reduced(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """
        Tidy up YAML representation a bit
        """
        di = self.model_dump(*args, **kwargs)

        for ref in di["division_links"]:
            if ref["decision"]:
                del ref["decision"]["key"]
            del ref["decision_key"]
        return di


class Policy(PolicyBase):
    chamber_id: AllowedChambers
    chamber: Chamber
    groups: list[PolicyGroup]
    division_links: list[PolicyDecisionLink[DivisionInfo]]
    agreement_links: list[PolicyDecisionLink[AgreementInfo]]

    @classmethod
    async def from_partials(cls, partials: list[PartialPolicy]) -> list[Self]:
        # get all links in a single list

        # get divisions
        divisions = [x.division_links for x in partials]
        divisions = [x for y in divisions for x in y]
        full_decisions = await PolicyDecisionLink.from_partials(partials=divisions)
        # create a lookup from a PartialPolicyDecisionLink to a PolicyDecisionLink
        division_lookup = {x.decision.key: x for x in full_decisions}
        # get agreements
        agreements = [x.agreement_links for x in partials]
        agreements = [x for y in agreements for x in y]
        full_agreements = await PolicyDecisionLink.from_partials(partials=agreements)
        # create a lookup from a PartialPolicyDecisionLink to a PolicyDecisionLink
        agreement_lookup = {x.decision.key: x for x in full_agreements}
        return [
            cls(
                name=x.name,
                id=x.id,
                chamber_id=x.chamber,
                chamber=Chamber(slug=x.chamber),
                context_description=x.context_description,
                policy_description=x.policy_description,
                notes=x.notes,
                status=x.status,
                groups=[PolicyGroup(slug=y) for y in x.groups],
                strength_meaning=x.strength_meaning,
                highlightable=x.highlightable,
                division_links=[
                    division_lookup[y.decision_key] for y in x.division_links
                ],
                agreement_links=[
                    agreement_lookup[y.decision_key] for y in x.agreement_links
                ],
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

    async def division_df(self, request: Request):
        duck = await duck_core.child_query()
        all_decisions = [x.model_dump() for x in self.division_links]
        decision_infos = [x.decision.division_id for x in self.division_links]
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
            breakdown_lookup[x.decision.division_id] for x in self.division_links
        ]

        # need to make participant count line up

        df = pd.DataFrame(data=all_decisions)
        df["decision"] = [
            UrlColumn(url=x.decision.url(request), text=x.decision.division_name)
            for x in self.division_links
        ]
        df["uses_powers"] = [
            x.decision.motion_uses_powers() for x in self.division_links
        ]
        df["participant_count"] = [
            x.vote_participant_count if x else 0 for x in breakdown_in_order
        ]
        df["voting_cluster"] = [x.decision.voting_cluster for x in self.division_links]

        banned_columns = ["decision_type", "notes"]
        df = df.drop(columns=banned_columns)
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


class VoteDistribution(BaseModel):
    """
    Store the breakdown of votes associated with a policy
    and either a person or a comparison.
    """

    num_votes_same: float = 0.0
    num_strong_votes_same: float = 0.0
    num_votes_different: float = 0.0
    num_strong_votes_different: float = 0.0
    num_votes_absent: float = 0.0
    num_strong_votes_absent: float = 0.0
    num_votes_abstain: float = 0.0
    num_strong_votes_abstain: float = 0.0
    start_year: int
    end_year: int
    distance_score: float = 0.0
    similarity_score: float = 0.0

    @computed_field
    @property
    def total_votes(self) -> float:
        return (
            self.num_votes_same
            + self.num_votes_different
            + self.num_votes_absent
            + self.num_strong_votes_same
            + self.num_strong_votes_different
            + self.num_strong_votes_absent
            + self.num_votes_abstain
            + self.num_strong_votes_abstain
        )

    @computed_field
    @property
    def verbose_score(self) -> str:
        match self.distance_score:
            case s if 0 <= s <= 0.05:
                return "Consistently voted for"
            case s if 0.05 < s <= 0.15:
                return "Almost always voted for"
            case s if 0.15 < s <= 0.4:
                return "Generally voted for"
            case s if 0.4 < s <= 0.6:
                return "Voted a mixture of for and against"
            case s if 0.6 < s <= 0.85:
                return "Generally voted against"
            case s if 0.85 < s <= 0.95:
                return "Almost always voted against"
            case s if 0.95 < s <= 1:
                return "Consistently voted against"
            case _:
                raise ValueError("Score must be between 0 and 1")

    def score_classic(self):
        """
        Just run the classic public whip score calculation
        """
        from .analysis import public_whip_score_difference

        score = public_whip_score_difference(
            num_votes_same=self.num_votes_same,
            num_strong_votes_same=self.num_strong_votes_same,
            num_votes_different=self.num_votes_different,
            num_strong_votes_different=self.num_strong_votes_different,
            num_votes_absent=self.num_votes_absent,
            num_strong_votes_absent=self.num_strong_votes_absent,
            num_strong_votes_abstain=self.num_strong_votes_abstain,
            num_votes_abstain=self.num_votes_abstain,
        )

        self.distance_score = score
        if self.distance_score == -1:
            self.similarity_score = -1
        else:
            self.similarity_score = 1.0 - score

    def score_simplifed(self):
        from .analysis import simplified_score_difference

        score = simplified_score_difference(
            num_votes_same=self.num_votes_same,
            num_strong_votes_same=self.num_strong_votes_same,
            num_votes_different=self.num_votes_different,
            num_strong_votes_different=self.num_strong_votes_different,
            num_votes_absent=self.num_votes_absent,
            num_strong_votes_absent=self.num_strong_votes_absent,
            num_strong_votes_abstain=self.num_strong_votes_abstain,
            num_votes_abstain=self.num_votes_abstain,
        )

        self.distance_score = score
        self.similarity_score = 1.0 - score

    def score(self, strength_meaning: StrengthMeaning):
        match StrengthMeaning(strength_meaning):
            case StrengthMeaning.CLASSIC:
                self.score_classic()
            case StrengthMeaning.SIMPLIFIED:
                self.score_simplifed()

        return self


class ReducedPersonPolicyLink(BaseModel):
    person_id: str
    policy_id: int
    comparison_party: str
    chamber: AllowedChambers
    person_distance_from_policy: float
    comparison_distance_from_policy: float
    comparison_score_diff: float
    count_present: int
    count_absent: int
    start_year: int
    end_year: int
    no_party_comparison: bool

    @classmethod
    def from_person_policy_link(cls, link: PersonPolicyLink) -> Self:
        person_id = f"uk.org.publicwhip/person/{link.person_id}"

        absent = (
            link.own_distribution.num_votes_absent
            + link.own_distribution.num_strong_votes_absent
        )

        both_voted = (
            link.own_distribution.total_votes
            - link.own_distribution.num_votes_abstain
            - link.own_distribution.num_strong_votes_abstain
            - absent
        )

        return cls(
            person_id=person_id,
            policy_id=link.policy_id,
            comparison_party=link.comparison_party,
            chamber=link.chamber,
            count_present=int(both_voted),
            count_absent=int(absent),
            start_year=int(link.own_distribution.start_year),
            end_year=int(link.own_distribution.end_year),
            no_party_comparison=link.no_party_comparison,
            person_distance_from_policy=link.own_distribution.distance_score,
            comparison_distance_from_policy=link.other_distribution.distance_score,
            comparison_score_diff=link.comparison_score_difference,
        )


class PersonPolicyLink(BaseModel):
    """
    Storage object to connect person_id and policy_id
    """

    person_id: int
    policy_id: int
    comparison_party: str
    chamber: AllowedChambers
    own_distribution: VoteDistribution
    other_distribution: VoteDistribution
    no_party_comparison: bool = False
    comparison_score_difference: float = 0.0

    def xml_dict(self) -> dict[str, str]:
        """
        Calculate the information to be added to the XML for this person.

        For unclear historical reasons 'both_voted' means any 'non-abstain' vote.
        """

        person_id = f"uk.org.publicwhip/person/{self.person_id}"
        dbase = f"public_whip_dreammp{self.policy_id}_"

        absent = (
            self.own_distribution.num_votes_absent
            + self.own_distribution.num_strong_votes_absent
        )

        both_voted = (
            self.own_distribution.total_votes
            - self.own_distribution.num_votes_abstain
            - self.own_distribution.num_strong_votes_abstain
            - absent
        )

        def str_4dp(f: float) -> str:
            return f"{f:.4f}"

        di: dict[str, str] = {}
        di["id"] = person_id
        di[f"{dbase}distance"] = str_4dp(self.own_distribution.distance_score)
        di[f"{dbase}both_voted"] = str(int(both_voted))
        di[f"{dbase}absent"] = str(int(absent))

        di[f"{dbase}comparison_distance"] = str_4dp(
            self.other_distribution.distance_score
        )
        di[f"{dbase}comparison_score_diff"] = str_4dp(self.comparison_score_difference)
        di[f"{dbase}comparison_significant"] = str(int(self.significant_difference))
        di[f"{dbase}comparison_party"] = self.comparison_party
        di[f"{dbase}no_party_comparison"] = str(int(self.no_party_comparison))

        di[f"{dbase}start_year"] = str(int(self.own_distribution.start_year))
        di[f"{dbase}end_year"] = str(int(self.own_distribution.end_year))

        return di

    def score(self):
        self.comparison_score_difference = abs(
            self.own_distribution.distance_score
            - self.other_distribution.distance_score
        )
        return self

    @computed_field
    @property
    def significant_difference(self) -> bool:
        """
        The rules here:
        If own score is below 0.4 or above 0.6
        and the other score is below 0.4 or above 0.6 the other way
        then it's significant
        """
        own_score = self.own_distribution.distance_score
        other_score = self.other_distribution.distance_score
        if own_score < 0.4 and other_score > 0.6:
            return True
        if own_score > 0.6 and other_score < 0.4:
            return True
        return False

    @classmethod
    async def from_person_id(cls, person_id: int) -> list[Self]:
        duck = await duck_core.child_query()
        df = (
            await PolicyDistributionPersonQuery(person_id=person_id)
            .compile(duck=duck)
            .df()
        )
        return cls.from_df(df=df)

    @classmethod
    async def from_policy_id(
        cls, policy_id: int, single_comparisons: bool = False
    ) -> list[Self]:
        duck = await duck_core.child_query()
        df = (
            await PolicyDistributionQuery(
                policy_id=policy_id, single_comparisons=single_comparisons
            )
            .compile(duck=duck)
            .df()
        )

        return cls.from_df(df=df)

    @classmethod
    def from_df(cls, df: pd.DataFrame) -> list[Self]:
        """
        Create a list of PersonPolicyLinks from a dataframe
        """
        items: list[Self] = []
        for grouper, gdf in df.groupby(  # type: ignore
            ["policy_id", "person_id", "chamber", "comparison_party"]
        ):
            grouper: tuple[int, int, AllowedChambers, str]
            strength_meaning = StrengthMeaning(gdf["strength_meaning"].iloc[0])
            policy_id, person_id, chamber, comparison_party = grouper

            gdf = gdf.set_index("is_target").to_dict("index")
            target_values = VoteDistribution.model_validate(gdf[1]).score(
                strength_meaning
            )
            # if no comparison, then there are no compariable mps (rare) of the same party, so just use self.
            comparison_values = VoteDistribution.model_validate(
                gdf.get(0, gdf[1])
            ).score(strength_meaning)

            items.append(
                cls(
                    person_id=person_id,
                    policy_id=policy_id,
                    comparison_party=comparison_party,
                    chamber=chamber,
                    own_distribution=target_values,
                    other_distribution=comparison_values,
                    no_party_comparison=0 not in gdf,
                ).score()
            )
        return items


class ConnectedPolicyLink(BaseModel):
    policy: Policy
    link: PersonPolicyLink


class PolicyLinkDisplayGroup(BaseModel):
    name: str
    links: list[ConnectedPolicyLink]

    def as_df(self, request: Request) -> str:
        class GroupTableItem(BaseModel):
            policy_name: str
            policy_status: str
            person_score: float
            person_score_verbose: str
            comparison_score: float
            diff: float
            sig_diff: bool

        items: list[GroupTableItem] = []
        for link in self.links:
            item = GroupTableItem(
                policy_name=str(
                    UrlColumn(
                        url=link.policy.url(request=request),
                        text=link.policy.context_description or link.policy.name,
                    )
                ),
                policy_status=link.policy.status,
                person_score=link.link.own_distribution.distance_score,
                person_score_verbose=link.link.own_distribution.verbose_score,
                comparison_score=link.link.other_distribution.distance_score,
                diff=link.link.comparison_score_difference,
                sig_diff=link.link.significant_difference,
            )
            items.append(item)

        df = pd.DataFrame(data=[x.model_dump() for x in items])

        return style_df(df, percentage_columns=["person_score", "comparison_score"])


class PersonPolicyDisplay(BaseModel):
    person: Person
    comparison_party: str
    chamber: Chamber
    links: list[ConnectedPolicyLink]

    @classmethod
    async def from_person_and_party(
        cls, person_id: int, chamber_slug: AllowedChambers, comparison_party: str
    ):
        all_policies = await Policy.for_collection(chamber=chamber_slug)

        allowed_status = [PolicyStatus.ACTIVE, PolicyStatus.CANDIDATE]

        policy_lookup = {x.id: x for x in all_policies if x.status in allowed_status}

        links = await PersonPolicyLink.from_person_id(person_id=person_id)
        # narrow down to just ones for right party
        links = [x for x in links if x.comparison_party == comparison_party]

        connected_links: list[ConnectedPolicyLink] = []
        for link in links:
            policy = policy_lookup.get(link.policy_id, None)
            if not policy:
                # filter out drafts
                continue
            connected_links.append(ConnectedPolicyLink(policy=policy, link=link))

        person = await Person.from_id(person_id=person_id)
        chamber = Chamber(slug=chamber_slug)

        return cls(
            person=person,
            comparison_party=comparison_party,
            chamber=chamber,
            links=connected_links,
        )

    def display_groups(self) -> list[PolicyLinkDisplayGroup]:
        return [self.significant_policy_group()] + self.policies_by_group()

    def significant_policy_group(self) -> PolicyLinkDisplayGroup:
        links = [x for x in self.links if x.link.significant_difference]
        return PolicyLinkDisplayGroup(name="Significant policies", links=links)

    def policies_by_group(self) -> list[PolicyLinkDisplayGroup]:
        """
        Return a list of groups to display
        """
        groups = []
        for group_slug in PolicyGroupSlug:
            group = PolicyGroup(slug=group_slug)
            links = [x for x in self.links if group in x.policy.groups]
            groups.append(PolicyLinkDisplayGroup(name=group.name, links=links))

        return groups
