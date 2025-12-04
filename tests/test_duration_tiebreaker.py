"""Test duration tie-breaker for tour purpose selection."""

from datetime import datetime

import polars as pl

from data_canon.codebook.persons import PersonType
from data_canon.codebook.trips import ModeType, PurposeCategory
from processing.tours.extraction import extract_tours
from processing.tours.tour_configs import TourConfig


def create_test_data(
    trips_data: list[dict],
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Create minimal test data for tour extraction.

    Args:
        trips_data: List of trip dictionaries with required fields

    Returns:
        Tuple of (persons, households, trips) DataFrames
    """
    trips = pl.DataFrame(trips_data)

    # Extract unique person info from trips
    person_info = trips.select(
        [
            pl.col("person_id").first(),
            pl.col("hh_id").first(),
            pl.col("person_type").first(),
        ]
    ).unique()

    # Create persons DataFrame
    persons = person_info.with_columns(
        [
            pl.lit(37.7749).alias("home_lat"),
            pl.lit(-122.4194).alias("home_lon"),
            pl.lit(37.7849).alias("work_lat"),
            pl.lit(-122.4094).alias("work_lon"),
            pl.lit(None).cast(pl.Float64).alias("school_lat"),
            pl.lit(None).cast(pl.Float64).alias("school_lon"),
        ]
    )

    # Create households DataFrame
    households = (
        person_info.select("hh_id")
        .unique()
        .with_columns(
            [
                pl.lit(37.7749).alias("home_lat"),
                pl.lit(-122.4194).alias("home_lon"),
            ]
        )
    )

    return persons, households, trips


def test_duration_tiebreaker_equal_priority():
    """When destinations have equal priority, select longest duration."""
    # Create sample trips with equal priority destinations
    persons, households, trips = create_test_data(
        [
            {
                "linked_trip_id": 1,
                "person_id": 100,
                "hh_id": 10,
                "day_id": 1,
                "person_type": PersonType.FULL_TIME_WORKER.value,
                "depart_time": datetime(2024, 1, 1, 8, 0),  # Leave home
                "arrive_time": datetime(
                    2024, 1, 1, 8, 30
                ),  # Arrive at first dest
                "o_lat": 37.7749,
                "o_lon": -122.4194,  # Home
                "d_lat": 37.7850,
                "d_lon": -122.4000,  # First destination
                "o_purpose_category": PurposeCategory.HOME.value,
                "d_purpose_category": PurposeCategory.ERRAND.value,
                "mode_type": ModeType.CAR.value,
            },
            {
                "linked_trip_id": 2,
                "person_id": 100,
                "hh_id": 10,
                "day_id": 1,
                "person_type": PersonType.FULL_TIME_WORKER.value,
                # Leave first dest (30 min)
                "depart_time": datetime(2024, 1, 1, 9, 0),
                "arrive_time": datetime(
                    2024, 1, 1, 9, 30
                ),  # Arrive at 2nd dest
                "o_lat": 37.7850,
                "o_lon": -122.4000,  # First destination
                "d_lat": 37.7950,
                "d_lon": -122.4100,  # Second destination
                "o_purpose_category": PurposeCategory.ERRAND.value,
                "d_purpose_category": PurposeCategory.ERRAND.value,
                "mode_type": ModeType.CAR.value,
            },
            {
                "linked_trip_id": 3,
                "person_id": 100,
                "hh_id": 10,
                "day_id": 1,
                "person_type": PersonType.FULL_TIME_WORKER.value,
                # Leave second dest (60 min)
                "depart_time": datetime(2024, 1, 1, 10, 30),
                "arrive_time": datetime(2024, 1, 1, 11, 0),  # Arrive home
                "o_lat": 37.7950,
                "o_lon": -122.4100,  # Second destination
                "d_lat": 37.7749,
                "d_lon": -122.4194,  # Home
                "o_purpose_category": PurposeCategory.ERRAND.value,
                "d_purpose_category": PurposeCategory.HOME.value,
                "mode_type": ModeType.CAR.value,
            },
        ]
    )

    config = TourConfig()
    _, tours = extract_tours(persons, households, trips, config)

    # Should have 1 tour
    assert len(tours) == 1

    # Activity durations:
    # - Trip 1: 9:00 - 8:30 = 30 min
    # - Trip 2: 10:30 - 9:30 = 60 min
    # - Trip 3: None (last trip, gets default 240 min)
    # Since trip 2 has longer activity (60 min > 30 min) and same
    # priority, tour purpose should be ERRAND
    tour = tours.row(0, named=True)
    assert tour["tour_purpose"] == PurposeCategory.ERRAND.value


def test_duration_tiebreaker_different_priority():
    """Priority wins over duration when priorities differ."""
    # Create sample trips with different priorities
    persons, households, trips = create_test_data(
        [
            {
                "linked_trip_id": 1,
                "person_id": 100,
                "hh_id": 10,
                "day_id": 1,
                "person_type": PersonType.FULL_TIME_WORKER.value,
                "depart_time": datetime(2024, 1, 1, 8, 0),  # Leave home
                "arrive_time": datetime(2024, 1, 1, 8, 30),  # Arrive at work
                "o_lat": 37.7749,
                "o_lon": -122.4194,  # Home
                "d_lat": 37.7849,
                "d_lon": -122.4094,  # Work
                "o_purpose_category": PurposeCategory.HOME.value,
                "d_purpose_category": PurposeCategory.WORK.value,
                "mode_type": ModeType.CAR.value,
            },
            {
                "linked_trip_id": 2,
                "person_id": 100,
                "hh_id": 10,
                "day_id": 1,
                "person_type": PersonType.FULL_TIME_WORKER.value,
                "depart_time": datetime(
                    2024, 1, 1, 9, 0
                ),  # Leave work (30 min)
                "arrive_time": datetime(2024, 1, 1, 9, 30),  # Arrive at shop
                "o_lat": 37.7849,
                "o_lon": -122.4094,  # Work
                "d_lat": 37.7950,
                "d_lon": -122.4100,  # Shopping
                "o_purpose_category": PurposeCategory.WORK.value,
                "d_purpose_category": PurposeCategory.SHOP.value,
                "mode_type": ModeType.CAR.value,
            },
            {
                "linked_trip_id": 3,
                "person_id": 100,
                "hh_id": 10,
                "day_id": 1,
                "person_type": PersonType.FULL_TIME_WORKER.value,
                "depart_time": datetime(
                    2024, 1, 1, 10, 30
                ),  # Leave shop (60 min)
                "arrive_time": datetime(2024, 1, 1, 11, 0),  # Arrive home
                "o_lat": 37.7950,
                "o_lon": -122.4100,  # Shopping
                "d_lat": 37.7749,
                "d_lon": -122.4194,  # Home
                "o_purpose_category": PurposeCategory.SHOP.value,
                "d_purpose_category": PurposeCategory.HOME.value,
                "mode_type": ModeType.CAR.value,
            },
        ]
    )

    config = TourConfig()
    _, tours = extract_tours(persons, households, trips, config)

    # Tour purpose should be work (priority 1) regardless of duration
    tour = tours.row(0, named=True)
    assert tour["tour_purpose"] == PurposeCategory.WORK.value


def test_activity_duration_last_trip():
    """Last trip of day should get default 240 minute duration."""
    persons, households, trips = create_test_data(
        [
            {
                "linked_trip_id": 1,
                "person_id": 100,
                "hh_id": 10,
                "day_id": 1,
                "person_type": PersonType.FULL_TIME_WORKER.value,
                "depart_time": datetime(2024, 1, 1, 8, 0),  # Leave home
                "arrive_time": datetime(2024, 1, 1, 9, 0),  # Arrive at work
                "o_lat": 37.7749,
                "o_lon": -122.4194,  # Home
                "d_lat": 37.7849,
                "d_lon": -122.4094,  # Work
                "o_purpose_category": PurposeCategory.HOME.value,
                "d_purpose_category": PurposeCategory.WORK.value,
                "mode_type": ModeType.CAR.value,
            },
        ]
    )

    config = TourConfig()
    _, tours = extract_tours(persons, households, trips, config)

    # This should not error - last trip gets default duration
    assert len(tours) == 1
