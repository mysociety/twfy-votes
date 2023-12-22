"""
This module contains the dependency functions for the FastAPI app.

This acts as tiered and shared logic between views, that lets the views be
simple and declarative.

"""
from __future__ import annotations

import datetime
from typing import Literal

from ...helpers.static_fastapi.dependencies import dependency_alias_for
from .models import (
    AgreementAndVotes,
    AgreementInfo,
    AllowedChambers,
    Chamber,
    ChamberWithYearRange,
    DecisionListing,
    DivisionAndVotes,
    DivisionInfo,
    PartialAgreement,
    PartialDivision,
    Person,
    PersonAndRecords,
    PersonAndVotes,
)


@dependency_alias_for(Chamber)
async def GetChamber(chamber_slug: AllowedChambers) -> Chamber:
    """
    Get a chamber object from a slug
    """
    return Chamber(slug=chamber_slug)


@dependency_alias_for(list[Chamber])
async def AllChambers() -> list[Chamber]:
    return [Chamber(slug=chamber_slug) for chamber_slug in AllowedChambers]


@dependency_alias_for(DivisionInfo)
async def GetDivision(
    chamber_slug: AllowedChambers,
    date: datetime.date,
    division_number: int,
) -> DivisionInfo:
    """
    Get a partial division and elevate it to a full division
    """
    partial = PartialDivision(
        chamber_slug=chamber_slug, date=date, division_number=division_number
    )
    return await DivisionInfo.from_partial(partial)


@dependency_alias_for(DivisionAndVotes)
async def GetDivisionAndVotes(division: GetDivision) -> DivisionAndVotes:
    """
    Fetch the full votes from a division object
    """
    return await DivisionAndVotes.from_division(division)


@dependency_alias_for(AgreementInfo)
async def GetAgreement(
    chamber_slug: AllowedChambers, date: datetime.date, decision_ref: str
) -> AgreementInfo:
    """
    Get a partial agreement and elevate it to a full agreement
    """
    partial = PartialAgreement(
        chamber_slug=chamber_slug, date=date, decision_ref=decision_ref
    )
    return await AgreementInfo.from_partial(partial)


@dependency_alias_for(AgreementAndVotes)
async def GetAgreementAndVotes(agreement: GetAgreement) -> AgreementAndVotes:
    """
    Fetch the full votes from a division object
    """
    return await AgreementAndVotes.from_agreement(agreement)


@dependency_alias_for(DecisionListing)
async def GetDecisionListing(
    chamber: GetChamber, year: int, month: int | None = None
) -> DecisionListing:
    """
    Get a list of divisions for a chamber and year
    """
    if month:
        return await DecisionListing.from_chamber_year_month(chamber, year, month)
    else:
        return await DecisionListing.from_chamber_year(chamber, year)


@dependency_alias_for(list[ChamberWithYearRange])
async def GetChambersWithYearRange():
    """
    Get a list of chambers with the years they have divisions for
    """
    return await ChamberWithYearRange.fetch_all()


@dependency_alias_for(Person)
async def GetPerson(person_id: int) -> Person:
    """
    Get a person object from a person_id
    """
    return await Person.from_id(person_id)


@dependency_alias_for(PersonAndVotes)
async def GetPersonAndVotes(person: GetPerson) -> PersonAndVotes:
    """
    Fetch the full votes from a division object
    """
    return await PersonAndVotes.from_person(person)


@dependency_alias_for(list[Person])
async def GetPeopleList(people_option: Literal["current", "all"]) -> list[Person]:
    """
    Get a list of all people
    """
    return await Person.fetch_group(people_option)


@dependency_alias_for(PersonAndRecords)
async def GetPersonAndRecords(person: GetPerson) -> PersonAndRecords:
    """
    Fetch the full votes from a division object
    """
    return await PersonAndRecords.from_person(person)
