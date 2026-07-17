"""Tests for the BATS-2023 enrich step (industry_empsix + is_home_based_worker).

`enrich_2023_bats` lives in the bats_2023 project scripts, so the project
directory is added to sys.path to import it.
"""

import sys
from pathlib import Path

import polars as pl
import pytest

_PROJECT_DIR = Path(__file__).resolve().parents[1] / "projects" / "bats_2023"
if str(_PROJECT_DIR) not in sys.path:
    sys.path.insert(0, str(_PROJECT_DIR))

from clean_bats_2023 import enrich_2023_bats  # noqa: E402

from data_canon.codebook.ctramp import CTRAMPIndustry  # noqa: E402
from data_canon.codebook.persons import Industry  # noqa: E402


def _run(persons: pl.DataFrame, households: pl.DataFrame) -> pl.DataFrame:
    """Call the step and return the enriched persons frame."""
    return enrich_2023_bats(households=households, persons=persons)["persons"]


@pytest.fixture
def households() -> pl.DataFrame:
    """A single household whose home is at (37.8, -122.4)."""
    return pl.DataFrame({"hh_id": [10], "home_lat": [37.8], "home_lon": [-122.4]})


def test_industry_recoded_to_empsix(households):
    """The raw industry code maps to the six-category empsix sector."""
    persons = pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "hh_id": [10, 10, 10],
            "industry": [
                Industry.RETAIL_TRADE.value,
                Industry.HEALTH_AND_SOCIAL.value,
                Industry.FINANCE_AND_INSURANCE.value,
            ],
            "industry_other": pl.Series([None, None, None], dtype=pl.String),
            "work_lat": [None, None, None],
            "work_lon": [None, None, None],
        }
    )

    out = _run(persons, households)

    assert out.sort("person_id")["industry_empsix"].to_list() == [
        CTRAMPIndustry.RETEMPN.value,
        CTRAMPIndustry.HEREMPN.value,
        CTRAMPIndustry.FPSEMPN.value,
    ]


def test_industry_other_keyword_fallback(households):
    """When industry is unresolved, a keyword in industry_other fills empsix."""
    persons = pl.DataFrame(
        {
            "person_id": [1],
            "hh_id": [10],
            # 995 (Missing) does not map, so the free-text fallback applies
            "industry": [995],
            "industry_other": pl.Series(["Software engineering"], dtype=pl.String),
            "work_lat": [None],
            "work_lon": [None],
        }
    )

    out = _run(persons, households)

    assert out["industry_empsix"][0] == CTRAMPIndustry.FPSEMPN.value


def test_is_home_based_worker_when_work_equals_home(households):
    """A worker whose work location equals home is flagged home-based."""
    persons = pl.DataFrame(
        {
            "person_id": [1, 2],
            "hh_id": [10, 10],
            "industry": [Industry.RETAIL_TRADE.value, Industry.RETAIL_TRADE.value],
            "industry_other": pl.Series([None, None], dtype=pl.String),
            "work_lat": [37.8, 37.85],  # person 1 works at home, person 2 elsewhere
            "work_lon": [-122.4, -122.45],
        }
    )

    out = _run(persons, households).sort("person_id")

    assert out["is_home_based_worker"].to_list() == [True, False]
