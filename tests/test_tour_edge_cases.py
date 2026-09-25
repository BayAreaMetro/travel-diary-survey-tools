"""Tests tour extraction edge cases that previously caused validation errors.

This test module ensures that tour extraction handles edge cases correctly:
- Single-trip tours (tour has only one trip before returning home)
- Partial tours (tour starts away from home, no initial home departure)
- Tours starting with tour_num=1 (not 0)
- Distance threshold fallback for destination timing
- A tour destination that is the place visited, not the home returned to
"""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.days import TravelDow
from data_canon.codebook.generic import LocationType
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    Student,
)
from data_canon.codebook.tours import TourCategory, TourDataQuality
from data_canon.codebook.trips import (
    Driver,
    ModeType,
    Purpose,
    PurposeCategory,
    PurposeToCategoryMap,
)
from processing import link_trips
from tests.fixtures.tour_pipeline import locate_and_extract_tours

HOME = (37.8, -122.4)
SHOP = (37.82, -122.42)
CORNER_SHOP = (37.81, -122.41)
WORK = (37.85, -122.45)
LUNCH = (37.86, -122.46)
DOWNTOWN = (37.79, -122.39)
ACROSS_THE_BAY = (37.6, -122.2)

HOUSEHOLDS = pl.DataFrame({"hh_id": [1], "home_lat": [HOME[0]], "home_lon": [HOME[1]]})


def _worker(work: tuple[float, float] | None = WORK) -> pl.DataFrame:
    """One employed adult, optionally with a reported workplace."""
    return pl.DataFrame(
        {
            "person_id": [1],
            "hh_id": [1],
            "age": [AgeCategory.AGE_35_TO_44.value],
            "employment": [Employment.EMPLOYED_FULLTIME.value],
            "student": [Student.NONSTUDENT.value],
            "school_type": [None],
            "work_lat": [work[0] if work else None],
            "work_lon": [work[1] if work else None],
            "school_lat": [None],
            "school_lon": [None],
        },
        schema_overrides={
            "school_type": pl.Int64,
            "work_lat": pl.Float64,
            "work_lon": pl.Float64,
            "school_lat": pl.Float64,
            "school_lon": pl.Float64,
        },
    )


def _at(hour: int, minute: int = 0) -> datetime:
    return datetime(2024, 1, 15, hour, minute)


def _leg(
    o: tuple[float, float],
    d: tuple[float, float],
    o_purpose: Purpose,
    d_purpose: Purpose,
    depart: datetime,
    arrive: datetime,
    *,
    distance_m: float = 2000.0,
    mode: int = ModeType.CAR.value,
) -> dict:
    """One unlinked trip. Its reported duration is read off the timestamps.

    Purpose categories are derived rather than stated, so a leg cannot claim a
    purpose and a category that disagree.
    """
    return {
        "day_id": 1,
        "person_id": 1,
        "hh_id": 1,
        "travel_dow": TravelDow.WEDNESDAY.value,
        "depart_time": depart,
        "arrive_time": arrive,
        "o_purpose": o_purpose.value,
        "d_purpose": d_purpose.value,
        "o_purpose_category": PurposeToCategoryMap.PURPOSE_TO_CATEGORY[o_purpose].value,
        "d_purpose_category": PurposeToCategoryMap.PURPOSE_TO_CATEGORY[d_purpose].value,
        "mode_type": mode,
        "o_lat": o[0],
        "o_lon": o[1],
        "d_lat": d[0],
        "d_lon": d[1],
        "unlinked_trip_weight": 1.0,
        "distance_meters": distance_m,
        "duration_minutes": (arrive - depart).total_seconds() / 60,
        "num_travelers": 1,
        "driver": Driver.DRIVER.value,
    }


def _prepare(legs: list[dict], persons: pl.DataFrame | None = None) -> tuple:
    """Link the legs and return everything ``locate_and_extract_tours`` needs."""
    unlinked_trips = pl.DataFrame(
        [{"unlinked_trip_id": i, **leg} for i, leg in enumerate(legs, start=1)]
    )

    link_result = link_trips(
        unlinked_trips=unlinked_trips,
        change_mode_enum=PurposeCategory.CHANGE_MODE.value,
        transit_mode_enums=[ModeType.TRANSIT.value],
        split_on_occupancy=False,
    )
    linked_trips = link_result["linked_trips"].with_columns(
        pl.lit(None).cast(pl.Int64).alias("joint_trip_id")
    )

    return (
        persons if persons is not None else _worker(),
        HOUSEHOLDS,
        link_result["unlinked_trips"],
        linked_trips,
    )


@pytest.fixture
def single_trip_tour_data():
    """Person makes a single trip to the grocery store and stops there.

    This tests the edge case where a tour consists of only one trip.
    Previously, this caused tour_purpose=None because the logic filtered
    out "last trips" which left no trips to determine purpose from.
    """
    return _prepare([_leg(HOME, SHOP, Purpose.HOME, Purpose.SHOPPING_ERRANDS, _at(9), _at(9, 15))])


@pytest.fixture
def partial_tour_data():
    """Person starts the day away from home, goes to lunch and back, then home.

    This tests the edge case where the first trip doesn't originate from home.
    Previously, this could cause tour_num=0 for trips before the first
    "leaving home" flag was set.
    """
    return _prepare(
        [
            _leg(
                WORK,
                LUNCH,
                Purpose.PRIMARY_WORKPLACE,
                Purpose.DINING,
                _at(12),
                _at(12, 15),
                distance_m=1000.0,
                mode=ModeType.WALK.value,
            ),
            _leg(
                LUNCH,
                WORK,
                Purpose.DINING,
                Purpose.PRIMARY_WORKPLACE,
                _at(12, 30),
                _at(12, 45),
                distance_m=1000.0,
                mode=ModeType.WALK.value,
            ),
            _leg(
                WORK,
                HOME,
                Purpose.PRIMARY_WORKPLACE,
                Purpose.HOME,
                _at(13, 30),
                _at(14),
                distance_m=5000.0,
            ),
        ]
    )


@pytest.fixture
def distant_destinations_data():
    """Person makes a tour whose stops are far from each other.

    This tests the edge case where destination distance thresholds might
    exclude all trips, causing dest_arrive_time and dest_depart_time to be None.
    """
    return _prepare(
        [
            _leg(
                HOME,
                DOWNTOWN,
                Purpose.HOME,
                Purpose.PRIMARY_WORKPLACE,
                _at(8),
                _at(9),
                distance_m=10000.0,
            ),
            _leg(
                DOWNTOWN,
                ACROSS_THE_BAY,
                Purpose.PRIMARY_WORKPLACE,
                Purpose.OTHER_SOCIAL,
                _at(10),
                _at(11),
                distance_m=20000.0,
            ),
            _leg(
                ACROSS_THE_BAY,
                HOME,
                Purpose.OTHER_SOCIAL,
                Purpose.HOME,
                _at(12),
                _at(14),
                distance_m=30000.0,
            ),
        ]
    )


@pytest.fixture
def round_trip_tour_data():
    """Person goes home -> shop -> home, so the tour's destination is the shop.

    The last trip of the tour ends at home, so anything reading the tour
    destination off the final trip reports home rather than where the person
    actually went.
    """
    return _prepare(
        [
            _leg(HOME, SHOP, Purpose.HOME, Purpose.SHOPPING_ERRANDS, _at(9), _at(9, 15)),
            _leg(SHOP, HOME, Purpose.SHOPPING_ERRANDS, Purpose.HOME, _at(11), _at(11, 15)),
        ]
    )


def test_tour_destination_type_matches_destination_coords(round_trip_tour_data):
    """d_location_type must describe the same place as d_lat/d_lon.

    For home -> shop -> home the destination is the shop (OTHER), not the home
    the tour returns to. d_location_type is taken from the primary destination,
    like d_lat/d_lon, so the two agree. Previously d_location_type came from the
    last trip (home) while d_lat came from the primary destination, so a tour
    zoned to the shop was labelled HOME.
    """
    persons, households, unlinked_trips, linked_trips = round_trip_tour_data

    tours = locate_and_extract_tours(persons, households, unlinked_trips, linked_trips)["tours"]

    assert len(tours) == 1
    tour = tours.row(0, named=True)
    assert (tour["d_lat"], tour["d_lon"]) == SHOP, (
        "Tour destination should be the primary destination (the shop), not the "
        "last trip's destination (home)"
    )
    assert tour["d_location_type"] == LocationType.OTHER.value, (
        "d_location_type should describe the primary destination (shop=OTHER), "
        "consistent with d_lat/d_lon, not the last trip's home"
    )


def test_single_trip_tour(single_trip_tour_data):
    """Test that single-trip tours are flagged appropriately."""
    persons, households, unlinked_trips, linked_trips = single_trip_tour_data

    result = locate_and_extract_tours(persons, households, unlinked_trips, linked_trips)
    tours_df = result["tours"]

    # Single-trip tours should be kept but flagged
    assert len(tours_df) == 1

    # Kept, with the count on the tour and a reason for being open-ended. Here
    # nothing precedes or follows it in the diary, so that is where it stops.
    assert tours_df["trip_count"][0] == 1
    assert tours_df["tour_data_quality"][0] == TourDataQuality.PARTIAL_DIARY_EDGE.value

    # Tour number should be 1, not 0
    assert tours_df["tour_num"][0] == 1

    # The one trip leaves home and stops at the shop without returning, so the
    # shop is a genuine primary destination -- not a return leg to be skipped.
    assert tours_df["tour_purpose"][0] == PurposeCategory.SHOP.value

    # With no non-last trip to read, the destination falls back to this trip's
    # end. Without that fallback the tour's coordinates would come out null.
    assert (tours_df["d_lat"][0], tours_df["d_lon"][0]) == SHOP


def test_partial_tour(partial_tour_data):
    """A day that starts away from home is one tour, numbered from 1.

    The person is already at work when the diary opens, goes to lunch and back,
    then home. That is a single tour anchored on the workplace it never left
    from, so it is partial at the start only.
    """
    persons, households, unlinked_trips, linked_trips = partial_tour_data

    tours_df = locate_and_extract_tours(persons, households, unlinked_trips, linked_trips)["tours"]

    assert len(tours_df) == 1
    tour = tours_df.row(0, named=True)

    assert tour["tour_num"] == 1, "Tour numbering starts at 1, never 0"
    assert tour["trip_count"] == 3
    assert (tour["o_lat"], tour["o_lon"]) == WORK, "The day opens away from home"
    assert tour["tour_category"] == TourCategory.PARTIAL_START.value
    assert tour["tour_data_quality"] == TourDataQuality.PARTIAL_DIARY_EDGE.value
    assert tour["tour_purpose"] == PurposeCategory.WORK.value


def test_distant_destinations(distant_destinations_data):
    """Test that tours with distant destinations still get valid times."""
    persons, households, unlinked_trips, linked_trips = distant_destinations_data

    # Pin the hierarchy method: this fixture's work and social stops are both
    # 60 min, and under the scoring method a 60-min stay reads as a normal social
    # visit but an atypically brief work visit, so social would win. This test is
    # about destination-time fallback, not purpose selection.
    result = locate_and_extract_tours(
        persons, households, unlinked_trips, linked_trips, tour_purpose_method="hierarchy"
    )
    tours_df = result["tours"]

    assert len(tours_df) == 1

    # Even with distant destinations, should have destination times
    # (fallback logic should apply)
    assert tours_df["dest_arrive_time"][0] is not None
    assert tours_df["dest_depart_time"][0] is not None

    # Tour purpose should be WORK (highest priority non-home purpose)
    assert tours_df["tour_purpose"][0] == PurposeCategory.WORK.value


def test_tour_num_sequential():
    """Test that tour numbers are sequential (1, 2, 3...) for multiple tours."""
    # home->work->home, home->shop->home, then out again to a social visit
    persons, households, unlinked_trips, linked_trips = _prepare(
        [
            _leg(
                HOME,
                WORK,
                Purpose.HOME,
                Purpose.PRIMARY_WORKPLACE,
                _at(8),
                _at(8, 30),
                distance_m=5000.0,
            ),
            _leg(
                WORK,
                HOME,
                Purpose.PRIMARY_WORKPLACE,
                Purpose.HOME,
                _at(9),
                _at(9, 30),
                distance_m=5000.0,
            ),
            _leg(
                HOME,
                CORNER_SHOP,
                Purpose.HOME,
                Purpose.GROCERY,
                _at(10),
                _at(10, 15),
                distance_m=1000.0,
            ),
            _leg(
                CORNER_SHOP,
                HOME,
                Purpose.GROCERY,
                Purpose.HOME,
                _at(11),
                _at(11, 15),
                distance_m=1000.0,
            ),
            _leg(HOME, SHOP, Purpose.HOME, Purpose.SOCIAL, _at(13), _at(13, 15)),
        ]
    )

    tours_df = locate_and_extract_tours(persons, households, unlinked_trips, linked_trips)["tours"]

    # Tour numbers should start at 1 and be sequential
    tour_nums = sorted(tours_df["tour_num"].unique().to_list())
    assert tour_nums == list(range(1, len(tour_nums) + 1))
    assert tours_df["origin_depart_time"].null_count() == 0
    assert tours_df["origin_arrive_time"].null_count() == 0


def test_two_trips_out_and_back_are_two_tours_whatever_the_purpose_says():
    """Out and back twice is two tours, even when an arrival home is coded as an activity.

    The second arrival home carries the purpose of what the person did there --
    a walk that ends at their own door -- rather than "home". Read strictly,
    that end is not home, and the two tours weld into one long chain. Within the
    home's own address the purpose describes the activity, not a different
    place.
    """
    # home -> shop -> home (arrival coded exercise) -> shop -> home
    persons, households, unlinked_trips, linked_trips = _prepare(
        [
            _leg(HOME, SHOP, Purpose.HOME, Purpose.SHOPPING_ERRANDS, _at(9), _at(9, 15)),
            _leg(SHOP, HOME, Purpose.SHOPPING_ERRANDS, Purpose.EXERCISE, _at(11), _at(11, 15)),
            _leg(HOME, SHOP, Purpose.EXERCISE, Purpose.SHOPPING_ERRANDS, _at(14), _at(14, 15)),
            _leg(SHOP, HOME, Purpose.SHOPPING_ERRANDS, Purpose.HOME, _at(16), _at(16, 15)),
        ],
        persons=_worker(work=None),
    )

    tours = locate_and_extract_tours(persons, households, unlinked_trips, linked_trips)["tours"]

    assert tours.height == 2
    assert tours["tour_data_quality"].to_list() == [TourDataQuality.VALID.value] * 2
