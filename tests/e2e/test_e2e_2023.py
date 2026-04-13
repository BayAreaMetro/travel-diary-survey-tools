"""End-to-end integration tests for the BATS 2023 pipeline."""

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


class TestDataCompleteness2023:
    def test_all_core_tables_populated(self, result_2023):
        assert_tables_non_empty(result_2023, CORE_TABLES)

    def test_joint_trips_table_exists(self, result_2023):
        jt = result_2023.joint_trips
        assert jt is not None
        assert isinstance(jt, pl.DataFrame)

    def test_daysim_tables_populated(self, result_2023):
        for name in [
            "households_daysim",
            "persons_daysim",
            "days_daysim",
            "linked_trips_daysim",
            "tours_daysim",
        ]:
            df = getattr(result_2023, name, None)
            assert df is not None, f"DaySim table '{name}' is None"
            assert df.height > 0, f"DaySim table '{name}' is empty"

    def test_ctramp_tables_populated(self, result_2023):
        for name in [
            "households_ctramp",
            "persons_ctramp",
            "individual_tours_ctramp",
            "individual_trips_ctramp",
        ]:
            df = getattr(result_2023, name, None)
            assert df is not None, f"CT-RAMP table '{name}' is None"
            assert df.height > 0, f"CT-RAMP table '{name}' is empty"


# ── Referential integrity ─────────────────────────────────────────────


class TestReferentialIntegrity2023:
    def test_foreign_keys_valid(self, result_2023):
        assert_referential_integrity(result_2023)

    def test_linked_trips_reference_tours(self, result_2023):
        lt = result_2023.linked_trips
        tours = result_2023.tours
        if "tour_id" in lt.columns:
            tour_ids = set(tours["tour_id"].to_list())
            lt_tour_ids = set(lt["tour_id"].drop_nulls().to_list())
            orphans = lt_tour_ids - tour_ids
            assert not orphans, f"linked_trips has tour_ids not in tours: {orphans}"


# ── Trip linking ──────────────────────────────────────────────────────


class TestTripLinking2023:
    def test_transit_trips_linked(self, result_2023):
        """HH2's transit segments should merge into 2 linked trips."""
        lt = result_2023.linked_trips
        hh2_trips = lt.filter(pl.col("hh_id") == 2)
        assert hh2_trips.height == 2, f"Expected 2 linked trips for HH2, got {hh2_trips.height}"

    def test_linked_trip_count_reasonable(self, result_2023):
        ut = result_2023.unlinked_trips
        lt = result_2023.linked_trips
        assert lt.height <= ut.height


# ── Joint trips ───────────────────────────────────────────────────────


class TestJointTrips2023:
    def test_joint_trips_detected(self, result_2023):
        jt = result_2023.joint_trips
        if jt is not None and jt.height > 0:
            hh3_joints = jt.filter(pl.col("hh_id") == 3)
            assert hh3_joints.height > 0, "Expected joint trips for HH3"


# ── Tour extraction ───────────────────────────────────────────────────


class TestTourExtraction2023:
    def test_tours_extracted(self, result_2023):
        tours = result_2023.tours
        assert tours.height > 0

    def test_simple_commute_tour(self, result_2023):
        tours = result_2023.tours
        hh1 = tours.filter(pl.col("hh_id") == 1)
        assert hh1.height >= 1

    def test_multi_stop_tour(self, result_2023):
        tours = result_2023.tours
        hh4 = tours.filter(pl.col("hh_id") == 4)
        assert hh4.height >= 1

    def test_single_trip_tour_flagged(self, result_2023):
        tours = result_2023.tours
        hh7 = tours.filter(pl.col("hh_id") == 7)
        assert hh7.height >= 1
        if "single_trip_tour" in hh7.columns:
            assert hh7["single_trip_tour"].to_list()[0] is True


# ── CT-RAMP formatting ───────────────────────────────────────────────


class TestCtrampFormat2023:
    def test_ctramp_households(self, result_2023):
        df = result_2023.households_ctramp
        assert df is not None and df.height > 0

    def test_ctramp_persons(self, result_2023):
        df = result_2023.persons_ctramp
        assert df is not None and df.height > 0

    def test_ctramp_individual_tours(self, result_2023):
        df = result_2023.individual_tours_ctramp
        assert df is not None and df.height > 0

    def test_ctramp_individual_trips(self, result_2023):
        df = result_2023.individual_trips_ctramp
        assert df is not None and df.height > 0


# ── DaySim formatting ────────────────────────────────────────────────


class TestDaysimFormat2023:
    def test_daysim_household_columns(self, result_2023):
        df = result_2023.households_daysim
        assert df is not None and df.height > 0

    def test_daysim_tours_columns(self, result_2023):
        df = result_2023.tours_daysim
        assert df is not None and df.height > 0


# ── Weighting ─────────────────────────────────────────────────────────


class TestWeighting2023:
    def test_weights_populated(self, result_2023):
        assert_weights_populated(result_2023)

    def test_weights_propagated(self, result_2023):
        assert_weights_propagated(result_2023)

    def test_weights_positive_for_complete(self, result_2023):
        assert_weights_positive_for_complete(result_2023)

    def test_household_weights_exist(self, result_2023):
        hh = result_2023.households
        assert "hh_weight" in hh.columns
        assert hh["hh_weight"].null_count() < hh.height


# ── Output files ──────────────────────────────────────────────────────


class TestOutput2023:
    def test_output_files_written(self, output_dir_2023):
        survey_dir = Path(output_dir_2023) / "survey"
        expected = [
            "households_2023.csv",
            "persons_2023.csv",
            "days_2023.csv",
            "unlinked_trips_2023.csv",
            "linked_trips_2023.csv",
            "tours_2023.csv",
        ]
        for fname in expected:
            path = survey_dir / fname
            assert path.exists(), f"Missing output file: {path}"
            assert path.stat().st_size > 0, f"Empty output file: {path}"
