"""A profile is the strict gate plus what it names, and nothing else.

``usability_profiles`` stamps one verdict per profile so consumers can hold
different standards in the same run -- a joint-tour model needs whole
households, a trip-level estimation does not. Each profile is the strict gate
relaxed by an explicit list of admission rules.

Two things have to hold for that to be safe. A profile must admit exactly what
it names, so reading the config tells you what the column means. And a profile
must never *remove* a record the strict gate kept, or "relaxed" would be a lie
and consumers could not reason about which column is the widest.

The rule that needs the most care is ``incomplete_household_days``, which
changes three separate places in the cascade. Miss the third and the profile
looks like it works -- tours and days flip -- while households stay strict and
the weighting re-zeroes everything downstream.
"""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.tours import TourCategory, TourDataQuality
from processing.completeness import (
    ADMISSION_RULES,
    INCOMPLETE_HOUSEHOLD_DAYS,
    MULTI_HOME_TOURS,
    UsabilityProfile,
    cascade_complete,
    parse_usability_profiles,
    stamp_usable,
)

STRICT = UsabilityProfile("strict")
MULTI_HOME = UsabilityProfile("multi_home", frozenset({MULTI_HOME_TOURS}))
LOOSE_DAYS = UsabilityProfile("loose_days", frozenset({INCOMPLETE_HOUSEHOLD_DAYS}))
BOTH = UsabilityProfile("both", frozenset(ADMISSION_RULES))


def _one_household(quality: TourDataQuality, category: TourCategory) -> dict:
    """Two people sharing a date. Person 1's tour is clean, person 2's is not.

    One member failing is what makes the household-day reduction bite, so this
    shape exercises both admission rules from a single fixture.
    """
    return {
        "households": pl.DataFrame({"hh_id": [1], "complete": [True]}),
        "persons": pl.DataFrame(
            {
                "person_id": [1, 2],
                "hh_id": [1, 1],
                "complete": [True, True],
                "surveyable": [True, True],
            }
        ),
        "days": pl.DataFrame(
            {
                "day_id": [1, 2],
                "person_id": [1, 2],
                "hh_id": [1, 1],
                "travel_date": [datetime(2023, 5, 1)] * 2,
                "complete": [True, True],
            }
        ),
        "tours": pl.DataFrame(
            {
                "tour_id": [10, 20],
                "day_id": [1, 2],
                "person_id": [1, 2],
                "complete": [True, True],
                "parent_tour_id": [10, 20],
                "tour_data_quality": [TourDataQuality.VALID.value, quality.value],
                "tour_category": [TourCategory.COMPLETE.value, category.value],
            }
        ),
    }


def _stamp(profiles: list[UsabilityProfile], tables: dict) -> dict[str, pl.DataFrame]:
    """Run the complete cascade once, then every profile over it."""
    working: dict[str, pl.DataFrame | None] = dict(tables)
    cascade_complete(working)
    for profile in profiles:
        stamp_usable(working, profile)
    return {name: df for name, df in working.items() if df is not None}


class TestTheConfigVocabularyIsClosed:
    """A profile that does not mean what it says is worse than no profile."""

    def test_an_unknown_rule_is_rejected(self):
        """A typo must fail loudly rather than silently admit nothing."""
        with pytest.raises(ValueError, match="unknown admission rule"):
            parse_usability_profiles({"p": ["multi_home_toors"]})

    def test_the_error_lists_what_is_available(self):
        """Someone who mistyped needs the vocabulary, not just a rejection."""
        with pytest.raises(ValueError, match=MULTI_HOME_TOURS):
            parse_usability_profiles({"p": ["nonsense"]})

    def test_an_empty_block_is_rejected(self):
        """Declaring the block but naming nothing stamps no verdict at all."""
        with pytest.raises(ValueError, match="at least one profile"):
            parse_usability_profiles({})

    @pytest.mark.parametrize("reserved", ["complete", "hh_day_complete"])
    def test_a_profile_cannot_take_a_reporting_column_name(self, reserved):
        """Reporting flags are facts; a profile must not overwrite one."""
        with pytest.raises(ValueError, match="collides"):
            parse_usability_profiles({reserved: []})

    def test_an_empty_list_is_the_strict_gate(self):
        """``model_usable: []`` is how a project asks for strict."""
        (profile,) = parse_usability_profiles({"model_usable": []})
        assert profile.admits == frozenset()

    def test_declaration_order_is_preserved(self):
        """Profiles are stamped in the order the config names them."""
        profiles = parse_usability_profiles({"a": [], "b": [MULTI_HOME_TOURS], "c": []})
        assert [p.name for p in profiles] == ["a", "b", "c"]


class TestMultiHomeTours:
    """Admits a tour that reached another home of this person's."""

    def test_a_second_home_tour_is_admitted(self):
        """Its trips are whole; only the anchor differs."""
        tables = _one_household(TourDataQuality.PARTIAL_OTHER_HOME, TourCategory.PARTIAL_END)
        out = _stamp([STRICT, MULTI_HOME], tables)
        assert out["tours"][STRICT.flag].to_list() == [True, False]
        assert out["tours"][MULTI_HOME.flag].to_list() == [True, True]

    @pytest.mark.parametrize(
        "quality",
        [
            TourDataQuality.PARTIAL_DAY_SPLIT,
            TourDataQuality.PARTIAL_DIARY_EDGE,
            TourDataQuality.NO_DESTINATION,
            TourDataQuality.SPATIAL_GAP,
        ],
        ids=lambda q: q.name,
    )
    def test_no_other_defect_is_admitted(self, quality):
        """Every other code marks something genuinely missing."""
        tables = _one_household(quality, TourCategory.PARTIAL_END)
        out = _stamp([MULTI_HOME], tables)
        assert out["tours"][MULTI_HOME.flag].to_list() == [True, False]


class TestIncompleteHouseholdDays:
    """Stops requiring that every member reported the same date."""

    def test_the_household_is_admitted(self):
        """The third place. Relaxing tours and days alone leaves this False.

        Person 2's day is unusable, so no date has all members usable. Strict
        drops the household; the relaxed profile counts usable *days* instead.
        """
        tables = _one_household(TourDataQuality.SPATIAL_GAP, TourCategory.PARTIAL_END)
        out = _stamp([STRICT, LOOSE_DAYS], tables)
        assert out["households"][STRICT.flag].to_list() == [False]
        assert out["households"][LOOSE_DAYS.flag].to_list() == [True]

    def test_it_does_not_admit_the_broken_tour(self):
        """Relaxing coherence says nothing about tour structure."""
        tables = _one_household(TourDataQuality.SPATIAL_GAP, TourCategory.PARTIAL_END)
        out = _stamp([LOOSE_DAYS], tables)
        assert out["tours"][LOOSE_DAYS.flag].to_list() == [True, False]

    def test_the_household_day_column_is_still_recorded(self):
        """Computed for every profile, so the column means one thing everywhere.

        This profile does not gate on it, but it is the only place that says
        which dates were fully usable, which is how a dropped household is
        traced back to a cause.
        """
        tables = _one_household(TourDataQuality.SPATIAL_GAP, TourCategory.PARTIAL_END)
        out = _stamp([LOOSE_DAYS], tables)
        assert LOOSE_DAYS.household_day in out["days"].columns
        assert out["days"][LOOSE_DAYS.household_day].to_list() == [False, False]


class TestProfilesAreIndependent:
    """Stamping one must not disturb another."""

    def test_a_later_pass_does_not_change_an_earlier_one(self):
        """Order of declaration cannot change any profile's verdict."""
        tables = _one_household(TourDataQuality.PARTIAL_OTHER_HOME, TourCategory.PARTIAL_END)
        forwards = _stamp([STRICT, MULTI_HOME, BOTH], tables)
        backwards = _stamp([BOTH, MULTI_HOME, STRICT], tables)
        for name, df in forwards.items():
            for profile in (STRICT, MULTI_HOME, BOTH):
                assert df[profile.flag].to_list() == backwards[name][profile.flag].to_list()

    def test_relaxing_never_removes_a_record(self):
        """A profile admitting more is a superset of one admitting less.

        Checked at every level, not just tours: the cascade could widen a tour
        and still lose its day or household to a rule applied inconsistently.
        """
        for quality in TourDataQuality:
            tables = _one_household(quality, TourCategory.PARTIAL_END)
            out = _stamp([STRICT, MULTI_HOME, LOOSE_DAYS, BOTH], tables)
            for name, df in out.items():
                strict = df[STRICT.flag].to_list()
                for wider in (MULTI_HOME, LOOSE_DAYS, BOTH):
                    lost = [
                        i
                        for i, (was, now) in enumerate(zip(strict, df[wider.flag], strict=True))
                        if was and not now
                    ]
                    assert not lost, (
                        f"{wider.name} dropped {name} row(s) {lost} that {STRICT.name} kept, "
                        f"with tour quality {quality.name}"
                    )
