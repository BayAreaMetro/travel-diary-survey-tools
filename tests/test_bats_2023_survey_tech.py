"""The bats_2023 cleaner's survey technology, and the persons it excludes by it."""

import importlib.util
from pathlib import Path

import polars as pl
import pytest

from data_canon.codebook.persons import SurveyTech

_PATH = Path(__file__).parents[1] / "projects" / "bats_2023" / "clean_bats_2023.py"
_spec = importlib.util.spec_from_file_location("clean_bats_2023", _PATH)
cleaner = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(cleaner)


def _persons(platforms: list[str | None]) -> pl.DataFrame:
    return pl.DataFrame(
        {"person_id": list(range(1, len(platforms) + 1)), "diary_platform": platforms},
        schema_overrides={"diary_platform": pl.String},
    )


def test_each_platform_maps_to_its_technology():
    """The phone app is GPS, the browser a form, and the call center itself."""
    out = cleaner.survey_tech(_persons(["rmove", "browser", "call"]))
    assert out["survey_tech"].to_list() == [
        SurveyTech.GPS.value,
        SurveyTech.FORM.value,
        SurveyTech.CALL_CENTER.value,
    ]


def test_a_null_platform_is_missing():
    """No platform recorded means no technology is known, not a guess at one."""
    out = cleaner.survey_tech(_persons(["rmove", None]))
    assert out["survey_tech"].to_list() == [SurveyTech.GPS.value, SurveyTech.MISSING.value]


def test_an_unknown_platform_stops_the_run():
    """A new vendor platform must be mapped on purpose, not guessed."""
    with pytest.raises(ValueError, match="no survey_tech"):
        cleaner.survey_tech(_persons(["rmove", "kiosk"]))


def test_listed_technologies_get_a_reason_and_the_rest_none():
    """The reason names the technology, so a dropped person can be traced."""
    persons = cleaner.survey_tech(_persons(["rmove", "browser", "call"]))
    out = cleaner.exclude_by_survey_tech(persons, ["call_center", "form"])
    assert out["exclusion_reason"].to_list() == [
        None,
        "survey_tech excluded: form",
        "survey_tech excluded: call_center",
    ]


def test_an_empty_list_excludes_no_one():
    """An empty list is a stated choice to keep everyone."""
    persons = cleaner.survey_tech(_persons(["rmove", "browser"]))
    out = cleaner.exclude_by_survey_tech(persons, [])
    assert out["exclusion_reason"].null_count() == out.height


def test_an_unknown_technology_name_stops_the_run():
    """A vendor word in the config (browser) is not one of ours (form)."""
    persons = cleaner.survey_tech(_persons(["rmove"]))
    with pytest.raises(ValueError, match="not one of"):
        cleaner.exclude_by_survey_tech(persons, ["browser"])
