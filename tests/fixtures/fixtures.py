"""Pytest fixtures and pipeline processing helpers for canonical test data.

This module provides pytest fixtures that run scenarios through the
link_trips → extract_tours pipeline, and helper functions for processing
test data.
"""

import polars as pl
import pytest

from data_canon.codebook.trips import PurposeCategory
from processing.link_trips import link_trips
from processing.tours import extract_tours

from .base_records import create_day
from .scenario_builders import (
    DEFAULT_TRANSIT_MODE_CODES,
    multi_person_household,
    multi_stop_tour,
    multi_tour_day,
    simple_work_tour,
    transit_commute,
)
from .trip_records import create_unlinked_trip

# ==============================================================================
# Processed Scenario Functions
# ==============================================================================


def create_simple_work_tour_processed(
    hh_id: int = 1,
    person_id: int = 101,
) -> dict[str, pl.DataFrame]:
    """Create simple work tour processed through pipeline.

    Returns processed data ready for formatter testing.

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                       linked_trips, tours
    """
    households, persons, days, unlinked_trips = simple_work_tour(
        hh_id, person_id
    )
    return process_scenario_through_pipeline(
        households, persons, days, unlinked_trips
    )


def create_transit_commute_processed() -> dict[str, pl.DataFrame]:
    """Create transit commute scenario with all processing applied.

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                       linked_trips, tours
    """
    households, persons, days, unlinked_trips = transit_commute()
    return process_scenario_through_pipeline(
        households, persons, days, unlinked_trips
    )


def create_multi_person_household_processed(
    hh_id: int = 1,
) -> dict[str, pl.DataFrame]:
    """Create multi-person household processed through pipeline.

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                       linked_trips, tours
    """
    households, persons = multi_person_household(hh_id=hh_id)

    # Create simple days for all persons
    days_list = []
    unlinked_trips_list = []
    trip_id = 1

    for person in persons.iter_rows(named=True):
        person_id = person["person_id"]
        person_num = person["person_num"]

        days_list.append(
            create_day(
                day_id=person_id,
                person_id=person_id,
                hh_id=hh_id,
                person_num=person_num,
                day_num=1,
            )
        )

        # Create simple home->work->home trips for workers
        if person.get("work_taz"):
            unlinked_trips_list.extend(
                [
                    create_unlinked_trip(
                        trip_id=trip_id,
                        person_id=person_id,
                        hh_id=hh_id,
                        day_id=person_id,
                        person_num=person_num,
                        o_purpose_category=PurposeCategory.HOME,
                        d_purpose_category=PurposeCategory.WORK,
                        o_taz=households["home_taz"][0],
                        d_taz=person["work_taz"],
                    ),
                    create_unlinked_trip(
                        trip_id=trip_id + 1,
                        person_id=person_id,
                        hh_id=hh_id,
                        day_id=person_id,
                        person_num=person_num,
                        o_purpose_category=PurposeCategory.WORK,
                        d_purpose_category=PurposeCategory.HOME,
                        o_taz=person["work_taz"],
                        d_taz=households["home_taz"][0],
                    ),
                ]
            )
            trip_id += 2

    days = pl.DataFrame(days_list)
    unlinked_trips = (
        pl.DataFrame(unlinked_trips_list)
        if unlinked_trips_list
        else pl.DataFrame()
    )

    if not unlinked_trips.is_empty():
        return process_scenario_through_pipeline(
            households, persons, days, unlinked_trips
        )
    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": pl.DataFrame(),
        "tours": pl.DataFrame(),
    }


# ==============================================================================
# Pipeline Processing Helper
# ==============================================================================


def process_scenario_through_pipeline(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    days: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """Process a scenario through link_trips → extract_tours pipeline.

    Utility function for tests that need to process custom scenarios through
    the full pipeline with production defaults.

    Args:
        households: Households DataFrame
        persons: Persons DataFrame
        days: Days DataFrame
        unlinked_trips: Unlinked trips DataFrame

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                        linked_trips, tours
    """
    # Link trips (using config.yaml defaults)
    link_result = link_trips(
        unlinked_trips,
        change_mode_code=PurposeCategory.CHANGE_MODE.value,
        transit_mode_codes=DEFAULT_TRANSIT_MODE_CODES,
        max_dwell_time=180,  # in minutes
        dwell_buffer_distance=100,  # in meters
    )
    linked_trips = link_result["linked_trips"]
    unlinked_trips = link_result[
        "unlinked_trips"
    ]  # Use updated unlinked trips with linked_trip_id

    # Extract tours (using config.yaml defaults)
    tour_result = extract_tours(
        persons=persons,
        households=households,
        unlinked_trips=unlinked_trips,
        linked_trips=linked_trips,
    )

    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": tour_result["unlinked_trips"],
        "linked_trips": tour_result["linked_trips"],
        "tours": tour_result["tours"],
    }


# ==============================================================================
# Pytest Fixtures
# ==============================================================================


@pytest.fixture(scope="module")
def simple_work_tour_processed():
    """Simple work tour processed through link_trips and extract_tours.

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                        linked_trips, tours
    """
    return create_simple_work_tour_processed()


@pytest.fixture(scope="module")
def multi_stop_tour_processed():
    """Multi-stop work tour processed through link_trips and extract_tours.

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                        linked_trips, tours
    """
    households, persons, days, unlinked_trips = multi_stop_tour()
    return process_scenario_through_pipeline(
        households, persons, days, unlinked_trips
    )


@pytest.fixture(scope="module")
def multi_tour_day_processed():
    """Multi-tour day processed through link_trips and extract_tours.

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                        linked_trips, tours
    """
    households, persons, days, unlinked_trips = multi_tour_day()
    return process_scenario_through_pipeline(
        households, persons, days, unlinked_trips
    )
