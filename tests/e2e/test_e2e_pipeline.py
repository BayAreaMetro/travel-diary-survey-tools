"""End-to-end integration tests for the survey-processing pipeline.

Two kinds of tests:

* ``TestStepToggling`` is parametrized over the toggle *profiles* (see
  ``conftest.PROFILES``) — a leave-one-out matrix over the optional steps. It
  verifies that turning an optional step on/off does not break the downstream
  steps (the pipeline still completes and produces valid, referentially-consistent
  output). The enabled formatters/joint tables must appear; the disabled ones must
  not be required.

* The remaining classes use the ``full_result`` fixture (all optional steps on)
  to assert what only a whole run can show: at-work subtours surviving the gate
  and both formatters, DaySim tours and trips referencing each other, imputation
  writing through its stash, and the categories the formatters branch on still
  being present in the toy population.

What is deliberately *not* here: anything a unit test already pins on a
hand-built frame, and anything the committed baseline already fails on. The
baseline reports that a number moved; these say which rule broke.
"""

from pathlib import Path
from typing import ClassVar

import polars as pl
import pytest

from data_canon.codebook.ctramp import AtWorkFreq, CTRAMPTourCategory
from data_canon.codebook.tours import TourCategory, TourDataQuality, TourDirection, TourType
from tests.e2e.assertions import assert_referential_integrity, assert_tables_non_empty

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

# Core canonical tables produced regardless of the optional-step toggles.
CORE_TABLES = ["households", "persons", "days", "unlinked_trips", "linked_trips", "tours"]

_DAYSIM_TABLES = [
    "households_daysim",
    "persons_daysim",
    "days_daysim",
    "linked_trips_daysim",
    "tours_daysim",
]
_CTRAMP_TABLES = [
    "households_ctramp",
    "persons_ctramp",
    "individual_tours_ctramp",
    "individual_trips_ctramp",
]

_INCOME_MISSING = 995  # IncomeBroad.MISSING


# ── Step toggling (parametrized over profiles) ────────────────────────
# Runs the whole matrix: for each profile, prove toggling a step off/on does
# not break the downstream steps.


class TestStepToggling:
    def test_core_tables_populated(self, profile_run):
        _name, _enabled, result, _out = profile_run
        assert_tables_non_empty(result, CORE_TABLES)

    def test_referential_integrity(self, profile_run):
        _name, _enabled, result, _out = profile_run
        assert_referential_integrity(result)

    def test_enabled_formatters_produced(self, profile_run):
        _name, enabled, result, _out = profile_run
        if "format_daysim" in enabled:
            for name in _DAYSIM_TABLES:
                df = getattr(result, name, None)
                assert df is not None and df.height > 0, f"DaySim table '{name}' missing/empty"
        if "format_ctramp" in enabled:
            for name in _CTRAMP_TABLES:
                df = getattr(result, name, None)
                assert df is not None and df.height > 0, f"CT-RAMP table '{name}' missing/empty"

    def test_core_output_files_written(self, profile_run):
        _name, _enabled, _result, output_dir = profile_run
        survey = Path(output_dir) / "survey"
        for fname in ["households", "persons", "days", "unlinked_trips", "linked_trips", "tours"]:
            path = survey / f"{fname}.csv"
            assert path.exists(), f"Missing output file: {path}"
            assert path.stat().st_size > 0, f"Empty output file: {path}"


# ── Tour extraction (full profile) ────────────────────────────────────


class TestTourExtraction:
    def test_at_work_subtour_extracted(self, full_result):
        # HH 6 is home -> work -> lunch -> work -> home. The lunch leg is an
        # at-work subtour: anchored at the workplace, so it never touches home
        # and is only COMPLETE against its own anchor (issue #85).
        hh6 = full_result.tours.filter(pl.col("hh_id") == 6)
        subtours = hh6.filter(pl.col("tour_type") == TourType.WORK_BASED.value)
        assert subtours.height == 1, f"expected one at-work subtour for HH 6, got {hh6.height}"
        assert subtours["tour_category"].to_list() == [TourCategory.COMPLETE.value]
        assert subtours["tour_data_quality"].to_list() == [TourDataQuality.VALID.value]

    def test_at_work_subtour_reaches_ctramp(self, full_result):
        # The whole point of #85: the subtour must survive the usable gate
        # and the CT-RAMP drop, and be emitted as an AT_WORK tour whose parent
        # reports it in atWork_freq.
        tours = full_result.individual_tours_ctramp
        # CT-RAMP ids are re-encoded per person-day: hh_id = survey_hh_id * 100 + day_num.
        hh6 = tours.filter(pl.col("hh_id") // 100 == 6)
        at_work = hh6.filter(pl.col("tour_category") == CTRAMPTourCategory.AT_WORK.value)
        assert at_work.height == 1, "HH 6's at-work subtour is missing from CT-RAMP output"
        assert hh6["tour_id"].n_unique() == hh6.height, (
            "a work tour and its at-work subtour must not share a CT-RAMP tour_id"
        )
        # atWork_freq is a CT-RAMP category, not a raw count: one eating subtour -> ONE_EAT.
        assert hh6["atWork_freq"].max() == AtWorkFreq.ONE_EAT.value, (
            "the parent work tour should report one eating subtour"
        )

    def test_subtour_trips_have_a_real_direction(self, full_result):
        # A subtour is split into halves against its own anchor (the workplace),
        # like any other tour. Stamping both legs with a "subtour" sentinel threw
        # the direction away, which DaySim (half) and CT-RAMP (inbound) require.
        subtour_trips = full_result.linked_trips.filter(pl.col("subtour_num") > 0).sort(
            "depart_time"
        )
        assert subtour_trips.height == 2
        assert subtour_trips["tour_direction"].to_list() == [
            TourDirection.OUTBOUND.value,
            TourDirection.INBOUND.value,
        ]


class TestDaysimTourLinkage:
    """DaySim trips must reference the tour records they belong to.

    Schema validation alone does not cover this: a subtour trip filed under its
    parent's tour number is individually well-formed, it just points at the
    wrong tour. These assertions are what catch that.
    """

    _KEY: ClassVar[list[str]] = ["hhno", "pno", "day", "tour"]

    def test_no_trip_references_a_missing_tour(self, full_result):
        tour_keys = full_result.tours_daysim.select(self._KEY).unique()
        trip_keys = full_result.linked_trips_daysim.select(self._KEY).unique()
        orphans = trip_keys.join(tour_keys, on=self._KEY, how="anti")
        assert orphans.height == 0, f"trips reference non-existent tours:\n{orphans}"

    def test_no_tour_is_left_without_trips(self, full_result):
        # The subtour's own tour record went trip-less when its trips were
        # numbered by tour_num (shared with the parent) instead of tour_id.
        tour_keys = full_result.tours_daysim.select(self._KEY).unique()
        trip_keys = full_result.linked_trips_daysim.select(self._KEY).unique()
        childless = tour_keys.join(trip_keys, on=self._KEY, how="anti")
        assert childless.height == 0, f"tour records with no trips:\n{childless}"


# ── Imputation (full profile) ─────────────────────────────────────────
# The full profile runs imputation (RF on households.income_bin); these exercise
# the pre-imputation stash + RF feature importance.


class TestImputation:
    def test_missing_income_bins_were_imputed(self, full_result):
        hh = full_result.households
        stashed_missing = hh.filter(pl.col("income_bin_preimputed") == _INCOME_MISSING)
        assert stashed_missing.height > 0, "expected some stashed MISSING income bins"
        assert (stashed_missing["income_bin"] != _INCOME_MISSING).all(), (
            "MISSING income bins should have been imputed to valid values"
        )

    def test_unmissing_values_unchanged(self, full_result):
        hh = full_result.households
        kept = hh.filter(pl.col("income_bin_preimputed") != _INCOME_MISSING)
        assert (kept["income_bin"] == kept["income_bin_preimputed"]).all()


# ── Edge-case coverage (full profile; enforces the classification matrix) ──
# Assert that the synthetic data exercises each distinct classification bucket the
# formatters branch on. If a scenario household is removed, the relevant assertion
# fails. See tests/e2e/COVERAGE.md for the scenario -> edge-case map.


class TestEdgeCaseCoverage:
    """The toy population still spans the categories the formatters branch on.

    The committed baseline pins these counts exactly, so it fails first and
    harder if a bucket empties. These stay because the baseline says only that a
    number moved; this says which category went missing, which is the thing a
    reader needs.
    """

    @pytest.mark.parametrize(
        ("table", "column", "expected"),
        [
            (
                "persons_ctramp",
                "type",
                {
                    "Full-time worker",
                    "Part-time worker",
                    "University student",
                    "Non-worker",
                    "Retired",
                    "Student of driving age",
                    "Student of non-driving age",
                    "Child too young for school",
                },
            ),
            (
                "mandatory_locations_ctramp",
                "StudentCategory",
                {"College or higher", "Grade or high school", "Not student"},
            ),
            ("persons_ctramp", "activity_pattern", {"M", "N", "H"}),
            # VALID(0), PARTIAL_DIARY_EDGE(3), NO_DESTINATION(4), SPATIAL_GAP(5).
            # Absent by construction: a person with a second home (1) and a chain
            # resuming across diary days (2); both are covered by unit tests.
            ("tours", "tour_data_quality", {0, 3, 4, 5}),
            # COMPLETE(1), PARTIAL_END(2), PARTIAL_START(3), PARTIAL_BOTH(4).
            ("tours", "tour_category", {1, 2, 3, 4}),
            # ADULTS_ONLY(1), CHILDREN_ONLY(2), ADULTS_AND_CHILDREN(3).
            ("joint_tours_ctramp", "tour_composition", {1, 2, 3}),
        ],
        ids=lambda v: v if isinstance(v, str) else "",
    )
    def test_every_category_the_formatters_branch_on_is_exercised(
        self, full_result, table, column, expected
    ):
        present = set(getattr(full_result, table)[column].to_list())
        assert expected <= present, f"{table}.{column} is missing {expected - present}"

    def test_no_destination_means_a_closed_tour_without_a_purpose(self, full_result):
        """The code holds exactly when that is what the tour is.

        A partial tour may also lack a purpose -- half of it went unobserved --
        and is graded on its open end, so a null purpose alone is not enough.
        """
        tours = full_result.tours
        nothing_to_anchor_on = pl.col("tour_purpose").is_null() & (pl.col("tour_category") == 1)
        assert tours.filter((pl.col("tour_data_quality") == 4) & ~nothing_to_anchor_on).is_empty()
        assert tours.filter((pl.col("tour_data_quality") != 4) & nothing_to_anchor_on).is_empty()
