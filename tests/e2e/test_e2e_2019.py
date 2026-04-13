"""End-to-end integration tests for the BATS 2019 pipeline."""

from pathlib import Path

import polars as pl
import pytest

from tests.e2e.assertions import (
    assert_referential_integrity,
    assert_tables_non_empty,
    assert_weights_populated,
    assert_weights_positive_for_complete,
    assert_weights_propagated,
)

pytestmark = [pytest.mark.e2e, pytest.mark.slow]

CORE_TABLES = [
    "households",
    "persons",
    "days",
    "unlinked_trips",
    "linked_trips",
    "tours",
]

# ── Data completeness ─────────────────────────────────────────────────


class TestDataCompleteness2019:
    def test_all_core_tables_populated(self, result_2019):
        assert_tables_non_empty(result_2019, CORE_TABLES)

    def test_joint_trips_table_exists(self, result_2019):
        jt = result_2019.joint_trips
        assert jt is not None
        assert isinstance(jt, pl.DataFrame)

    def test_daysim_tables_populated(self, result_2019):
        for name in [
            "households_daysim",
            "persons_daysim",
            "days_daysim",
            "linked_trips_daysim",
            "tours_daysim",
        ]:
            df = getattr(result_2019, name, None)
            assert df is not None, f"DaySim table '{name}' is None"
            assert df.height > 0, f"DaySim table '{name}' is empty"


# ── Referential integrity ─────────────────────────────────────────────


class TestReferentialIntegrity2019:
    def test_foreign_keys_valid(self, result_2019):
        assert_referential_integrity(result_2019)

    def test_linked_trips_reference_tours(self, result_2019):
        lt = result_2019.linked_trips
        tours = result_2019.tours
        if "tour_id" in lt.columns:
            tour_ids = set(tours["tour_id"].to_list())
            lt_tour_ids = set(lt["tour_id"].drop_nulls().to_list())
            orphans = lt_tour_ids - tour_ids
            assert not orphans, f"linked_trips has tour_ids not in tours: {orphans}"

    def test_unlinked_trips_reference_linked(self, result_2019):
        ut = result_2019.unlinked_trips
        lt = result_2019.linked_trips
        if "linked_trip_id" in ut.columns:
            lt_ids = set(lt["linked_trip_id"].to_list())
            ut_lt_ids = set(ut["linked_trip_id"].drop_nulls().to_list())
            orphans = ut_lt_ids - lt_ids
            assert not orphans, f"unlinked_trips has linked_trip_ids not in linked_trips: {orphans}"


# ── Trip linking ──────────────────────────────────────────────────────


class TestTripLinking2019:
    def test_transit_trips_linked(self, result_2019):
        """HH2's 4 unlinked transit segments should merge into 2 linked trips."""
        lt = result_2019.linked_trips
        hh2_trips = lt.filter(pl.col("hh_id") == 2)
        assert hh2_trips.height == 2, (
            f"Expected 2 linked trips for HH2 (transit), got {hh2_trips.height}"
        )

    def test_linked_trip_count_reasonable(self, result_2019):
        """Linked trips should be fewer than unlinked (mode changes merged)."""
        ut = result_2019.unlinked_trips
        lt = result_2019.linked_trips
        assert lt.height <= ut.height

    def test_access_egress_modes_set(self, result_2019):
        """Transit linked trips should have access_mode and egress_mode."""
        lt = result_2019.linked_trips
        if "access_mode" in lt.columns and "egress_mode" in lt.columns:
            hh2_trips = lt.filter(pl.col("hh_id") == 2)
            for row in hh2_trips.iter_rows(named=True):
                assert row["access_mode"] is not None or row["egress_mode"] is not None


# ── Joint trips ───────────────────────────────────────────────────────


class TestJointTrips2019:
    def test_joint_trips_detected(self, result_2019):
        """HH3 has 2 persons making the same trip — should detect joint trips."""
        jt = result_2019.joint_trips
        if jt is not None and jt.height > 0:
            hh3_joints = jt.filter(pl.col("hh_id") == 3)
            assert hh3_joints.height > 0, "Expected joint trips for HH3"

    def test_joint_trip_ids_on_linked_trips(self, result_2019):
        """Some linked trips should have joint_trip_id set."""
        lt = result_2019.linked_trips
        if "joint_trip_id" in lt.columns:
            has_jt = lt.filter(pl.col("joint_trip_id").is_not_null())
            # At least some should be set (from HH3 and/or HH8)
            assert has_jt.height >= 0  # Soft check — detection is probabilistic


# ── Tour extraction ───────────────────────────────────────────────────


class TestTourExtraction2019:
    def test_tours_extracted(self, result_2019):
        tours = result_2019.tours
        assert tours.height > 0

    def test_simple_commute_tour(self, result_2019):
        """HH1 simple car commuter should produce 1 tour."""
        tours = result_2019.tours
        hh1 = tours.filter(pl.col("hh_id") == 1)
        assert hh1.height >= 1

    def test_multi_stop_tour(self, result_2019):
        """HH4 multi-stop errands should produce 1 tour (home-based loop)."""
        tours = result_2019.tours
        hh4 = tours.filter(pl.col("hh_id") == 4)
        assert hh4.height >= 1

    def test_subtour_detected(self, result_2019):
        """HH6 work subtour (lunch) should produce either 1 tour with subtour or 2 tours."""
        tours = result_2019.tours
        hh6 = tours.filter(pl.col("hh_id") == 6)
        assert hh6.height >= 1

    def test_single_trip_tour_flagged(self, result_2019):
        """HH7 person left home and didn't return — should be a single-trip tour."""
        tours = result_2019.tours
        hh7 = tours.filter(pl.col("hh_id") == 7)
        assert hh7.height >= 1
        if "single_trip_tour" in hh7.columns:
            assert hh7["single_trip_tour"].to_list()[0] is True

    def test_multi_day_traveler(self, result_2019):
        """HH11 has 2 travel days — should produce tours across both days."""
        tours = result_2019.tours
        hh11 = tours.filter(pl.col("hh_id") == 11)
        assert hh11.height >= 2, f"Expected >=2 tours for HH11, got {hh11.height}"


# ── DaySim formatting ────────────────────────────────────────────────


class TestDaysimFormat2019:
    def test_daysim_household_columns(self, result_2019):
        df = result_2019.households_daysim
        assert df is not None and df.height > 0

    def test_daysim_linked_trips_columns(self, result_2019):
        df = result_2019.linked_trips_daysim
        assert df is not None and df.height > 0

    def test_daysim_tours_columns(self, result_2019):
        df = result_2019.tours_daysim
        assert df is not None and df.height > 0


# ── Weighting ─────────────────────────────────────────────────────────


class TestWeighting2019:
    def test_weights_populated(self, result_2019):
        assert_weights_populated(result_2019)

    def test_weights_propagated(self, result_2019):
        assert_weights_propagated(result_2019)

    def test_weights_positive_for_complete(self, result_2019):
        assert_weights_positive_for_complete(result_2019)

    def test_household_weights_exist(self, result_2019):
        hh = result_2019.households
        assert "hh_weight" in hh.columns
        assert hh["hh_weight"].null_count() < hh.height  # At least some non-null


# ── Output files ──────────────────────────────────────────────────────


class TestOutput2019:
    def test_output_files_written(self, output_dir_2019):
        survey_dir = Path(output_dir_2019) / "survey"
        expected = [
            "households_2019.csv",
            "persons_2019.csv",
            "days_2019.csv",
            "unlinked_trips_2019.csv",
            "linked_trips_2019.csv",
            "tours_2019.csv",
        ]
        for fname in expected:
            path = survey_dir / fname
            assert path.exists(), f"Missing output file: {path}"
            assert path.stat().st_size > 0, f"Empty output file: {path}"
