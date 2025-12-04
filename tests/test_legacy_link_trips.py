"""Tests for legacy trip linking functionality from daysim_old pipeline."""

import importlib.util
from datetime import datetime
from pathlib import Path

import numpy as np  # noqa: F401
import pandas as pd
import polars as pl

from processing.link import link_trips

# Import the legacy function dynamically due to non-standard module name
spec = importlib.util.spec_from_file_location(
    "link_trips_week_module",
    Path(__file__).parent.parent
    / "scripts"
    / "daysim_old"
    / "survey_processing"
    / "02b-link_trips_week.py",
)
link_trips_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(link_trips_module)
link_trip_legacy = link_trips_module._link_trip_week


# Purpose code mappings for format conversion
PURPOSE_MAP_LEGACY = {
    "home": 0,
    "work": 1,
    "school": 3,
    "shop": 4,
    "change_mode": 10,
}

PURPOSE_MAP_NEW = {
    "home": 1,
    "work": 2,
    "school": 4,
    "shop": 7,
    "change_mode": 11,
}

# Mode code mappings (same for both implementations)
MODE_MAP = {
    "walk": 1,
    "bike": 2,
    "drive": 3,
    "transit": 6,
    "school_bus": 8,
}


# Test data: trips in new format (polars DataFrame)
# This is the canonical format - legacy format is derived from this
SIMPLE_TRANSIT_JOURNEY = pl.DataFrame(
    {
        "trip_id": [1, 2, 3, 4],
        "day_id": [1, 1, 1, 1],
        "person_id": [1, 1, 1, 1],
        "hh_id": [1, 1, 1, 1],
        "depart_time": [
            datetime(2024, 1, 1, 8, 0),  # Walk to transit
            datetime(2024, 1, 1, 8, 15),  # Transit
            datetime(2024, 1, 1, 8, 20),  # Walk to work
            datetime(2024, 1, 1, 17, 0),  # Drive home
        ],
        "arrive_time": [
            datetime(2024, 1, 1, 8, 10),
            datetime(2024, 1, 1, 8, 18),
            datetime(2024, 1, 1, 9, 0),
            datetime(2024, 1, 1, 17, 30),
        ],
        "o_purpose_category": [
            PURPOSE_MAP_NEW["home"],
            PURPOSE_MAP_NEW["change_mode"],
            PURPOSE_MAP_NEW["change_mode"],
            PURPOSE_MAP_NEW["work"],
        ],
        "d_purpose_category": [
            PURPOSE_MAP_NEW["change_mode"],
            PURPOSE_MAP_NEW["change_mode"],
            PURPOSE_MAP_NEW["work"],
            PURPOSE_MAP_NEW["home"],
        ],
        "o_purpose": [
            PURPOSE_MAP_NEW["home"],
            PURPOSE_MAP_NEW["change_mode"],
            PURPOSE_MAP_NEW["change_mode"],
            PURPOSE_MAP_NEW["work"],
        ],
        "d_purpose": [
            PURPOSE_MAP_NEW["change_mode"],
            PURPOSE_MAP_NEW["change_mode"],
            PURPOSE_MAP_NEW["work"],
            PURPOSE_MAP_NEW["home"],
        ],
        "mode_type": [
            MODE_MAP["walk"],
            MODE_MAP["transit"],
            MODE_MAP["walk"],
            MODE_MAP["drive"],
        ],
        "o_lat": [37.70, 37.71, 37.72, 37.75],
        "o_lon": [-122.40, -122.41, -122.42, -122.45],
        "d_lat": [37.71, 37.72, 37.75, 37.70],
        "d_lon": [-122.41, -122.42, -122.45, -122.40],
        "depart_date": [datetime(2024, 1, 1).date()] * 4,
        "arrive_date": [datetime(2024, 1, 1).date()] * 4,
        "depart_hour": [8, 8, 8, 17],
        "depart_minute": [0, 15, 20, 0],
        "depart_seconds": [0, 0, 0, 0],
        "arrive_hour": [8, 8, 9, 17],
        "arrive_minute": [10, 18, 0, 30],
        "arrive_seconds": [0, 0, 0, 0],
        "distance_miles": [0.5, 0.5, 5.0, 5.0],
        "duration_minutes": [10.0, 3.0, 40.0, 30.0],
        "trip_weight": [1.0, 1.0, 1.0, 1.0],
    }
)

# Expected outcome: 2 linked trips
# Trip 1: home -> work (segments 1-3 linked via change_mode)
# Trip 2: work -> home (segment 4 standalone)
SIMPLE_TRANSIT_JOURNEY_EXPECTED = {
    "num_linked_trips": 2,
    "trip1_mode": MODE_MAP["transit"],  # Highest hierarchy mode
    "trip2_mode": MODE_MAP["drive"],
}


def to_legacy_format(new_df: pl.DataFrame) -> pd.DataFrame:
    """Convert new format DataFrame to legacy pandas format for comparison.

    Args:
        new_df: Trip data in new polars format

    Returns:
        DataFrame in legacy format for link_trip_legacy function
    """
    # Create mapping from new purpose codes to legacy purpose codes
    # Find the string key for each new code, then get legacy code
    new_to_legacy_purpose = {}
    for key, new_code in PURPOSE_MAP_NEW.items():
        legacy_code = PURPOSE_MAP_LEGACY[key]
        new_to_legacy_purpose[new_code] = legacy_code

    # Extract time components from datetime
    depart_times = new_df.select("depart_time").to_series()
    arrive_times = new_df.select("arrive_time").to_series()

    return pd.DataFrame(
        {
            "hhno": new_df["hh_id"].to_list(),
            "pno": new_df["person_id"].to_list(),
            "dow": new_df["day_id"].to_list(),
            "tripno": new_df["trip_id"].to_list(),
            "opurp": [
                new_to_legacy_purpose[p]
                for p in new_df["o_purpose_category"].to_list()
            ],
            "dpurp": [
                new_to_legacy_purpose[p]
                for p in new_df["d_purpose_category"].to_list()
            ],
            "mode": new_df["mode_type"].to_list(),
            "mode_type": new_df["mode_type"].to_list(),
            "path": [1, 2, 1, 1],  # path hierarchy for mode selection
            "deptm": [dt.hour * 100 + dt.minute for dt in depart_times],
            "arrtm": [dt.hour * 100 + dt.minute for dt in arrive_times],
            "otaz": list(range(100, 100 + len(new_df))),
            "dtaz": list(range(101, 101 + len(new_df))),
            "dpcl": [
                new_to_legacy_purpose[p]
                for p in new_df["d_purpose_category"].to_list()
            ],
            "dxcord": new_df["d_lon"].to_list(),
            "dycord": new_df["d_lat"].to_list(),
        }
    )


def test_linking_row_count():
    """Test new implementation produces same row count as legacy."""
    new_data = SIMPLE_TRANSIT_JOURNEY
    expected = SIMPLE_TRANSIT_JOURNEY_EXPECTED

    # Convert to legacy format for comparison
    legacy_data = to_legacy_format(new_data)

    # Run both implementations
    legacy_result, _ = link_trip_legacy(
        legacy_data, act_dur_limit=35, act_dur_limit2=15
    )
    new_result = link_trips(
        new_data,
        change_mode_code=PURPOSE_MAP_NEW["change_mode"],
        transit_mode_codes=[MODE_MAP["transit"]],
        max_dwell_time=120,
        dwell_buffer_distance=100,
    )

    # Compare row counts
    legacy_count = len(legacy_result)
    new_count = len(new_result["linked_trips"])
    expected_count = expected["num_linked_trips"]

    assert new_count == expected_count, (
        f"New: expected {expected_count} trips, got {new_count}"
    )
    assert legacy_count == expected_count, (
        f"Legacy: expected {expected_count} trips, got {legacy_count}"
    )
    assert new_count == legacy_count, (
        f"Row count mismatch: legacy={legacy_count}, new={new_count}"
    )


def test_linked_trip_purposes():
    """Test that linked trip purposes are correctly preserved."""
    new_data = SIMPLE_TRANSIT_JOURNEY

    # Run new implementation
    new_result = link_trips(
        new_data,
        change_mode_code=PURPOSE_MAP_NEW["change_mode"],
        transit_mode_codes=[MODE_MAP["transit"]],
        max_dwell_time=120,
        dwell_buffer_distance=100,
    )

    linked_df = new_result["linked_trips"].sort("linked_trip_id")

    # Expected: 2 linked trips
    expected_num = SIMPLE_TRANSIT_JOURNEY_EXPECTED["num_linked_trips"]
    assert len(linked_df) == expected_num

    # First linked trip: home -> work (trips 1-3 merged)
    assert linked_df[0, "o_purpose_category"] == PURPOSE_MAP_NEW["home"]
    assert linked_df[0, "d_purpose_category"] == PURPOSE_MAP_NEW["work"]

    # Second linked trip: work -> home (trip 4 standalone)
    assert linked_df[1, "o_purpose_category"] == PURPOSE_MAP_NEW["work"]
    assert linked_df[1, "d_purpose_category"] == PURPOSE_MAP_NEW["home"]


def test_linked_trip_modes():
    """Test that linked trip modes follow hierarchy rules."""
    new_data = SIMPLE_TRANSIT_JOURNEY
    expected = SIMPLE_TRANSIT_JOURNEY_EXPECTED

    # Convert to legacy format
    legacy_data = to_legacy_format(new_data)

    # Run both implementations
    legacy_result, _ = link_trip_legacy(
        legacy_data, act_dur_limit=35, act_dur_limit2=15
    )
    new_result = link_trips(
        new_data,
        change_mode_code=PURPOSE_MAP_NEW["change_mode"],
        transit_mode_codes=[MODE_MAP["transit"]],
        max_dwell_time=120,
        dwell_buffer_distance=100,
    )

    linked_df = new_result["linked_trips"].sort("linked_trip_id")
    legacy_sorted = legacy_result.sort_values("lintripno")

    # First linked trip: walk + transit + walk
    # Should use highest hierarchy mode = transit
    assert linked_df[0, "mode_type"] == expected["trip1_mode"]

    # Second linked trip: drive only
    assert linked_df[1, "mode_type"] == expected["trip2_mode"]

    # Compare with legacy - modes should match
    assert linked_df[0, "mode_type"] == legacy_sorted.iloc[0]["mode"], (
        "First trip mode mismatch with legacy"
    )
    assert linked_df[1, "mode_type"] == legacy_sorted.iloc[1]["mode"], (
        "Second trip mode mismatch with legacy"
    )
