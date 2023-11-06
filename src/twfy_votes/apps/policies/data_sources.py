"""
This module contains the information needed to load 
the duckdb database from various sources.
"""

from pathlib import Path
from typing import Any

from ...helpers.duck import DuckQuery, YamlData
from .models import PartialPolicy

duck = DuckQuery(cached_dir=Path("data", "cached"))


@duck.as_python_source
class policies(YamlData[PartialPolicy]):
    yaml_source = Path("data", "policies", "*.yml")
    validation_model = PartialPolicy


@duck.as_python_source
class policy_votes(YamlData[PartialPolicy]):
    """
    Also add this as a seperate table to make query maths easier
    """

    yaml_source = Path("data", "policies", "*.yml")
    validation_model = PartialPolicy

    @classmethod
    def post_validation(cls, models: list[PartialPolicy]) -> list[dict[str, Any]]:
        data: list[dict[str, Any]] = []

        for policy in models:
            for decision in policy.decision_links_refs:
                if decision.division:
                    data.append(
                        {
                            "policy_id": policy.id,
                            "division_date": decision.division.date,
                            "division_chamber_slug": decision.division.chamber_slug,
                            "division_number": decision.division.division_number,
                            "strength": decision.strength,
                            "alignment": decision.alignment,
                            "notes": decision.notes,
                        }
                    )

        return data
