"""Scenario builders for creating common test patterns.

This module provides pre-built test scenarios with households, persons,
days, and trips. Uses data-driven patterns to reduce code duplication.
"""

from datetime import UTC, datetime, time

import polars as pl

from data_canon.codebook.households import IncomeDetailed
from data_canon.codebook.persons import (
    AgeCategory,
    CommuteSubsidy,
    Employment,
    Gender,
    SchoolType,
    Student,
)
from data_canon.codebook.trips import Mode, ModeType, Purpose, PurposeCategory

from .base_records import create_day, create_household, create_person
from .trip_records import create_unlinked_trip

# Default coordinates for common locations
DEFAULT_COORDS = {
    "home": (37.70, -122.40),
    "work": (37.75, -122.45),
    "bart_home": (37.71, -122.41),
    "bart_work": (37.74, -122.44),
}

# Default transit mode codes
DEFAULT_TRANSIT_MODE_CODES = [
    ModeType.TRANSIT.value,
    ModeType.FERRY.value,
    ModeType.LONG_DISTANCE.value,
]


def _create_trips_from_spec(
    trip_specs: list[dict], person_id: int, hh_id: int, day_id: int = 1
) -> list[dict]:
    """Create trips from a specification array.

    Args:
        trip_specs: List of trip specification dicts with keys:
            - trip_id: Trip ID
            - o_coords: Origin coordinate key in DEFAULT_COORDS
            - d_coords: Destination coordinate key in DEFAULT_COORDS
            - o_taz, d_taz: Origin/destination TAZ
            - o_maz, d_maz: Origin/destination MAZ (optional)
            - o_purpose_category, d_purpose_category: Purpose categories
            - mode, mode_type: Mode enums
            - depart_hour, depart_minute: Departure time
            - arrive_hour, arrive_minute: Arrival time
            - travel_time: Travel time in minutes
            - purpose: Legacy purpose field (optional)
        person_id: Person ID
        hh_id: Household ID
        day_id: Day ID (defaults to 1)

    Returns:
        List of trip dictionaries
    """
    trips = []
    for spec in trip_specs:
        # Resolve coordinates
        o_lat, o_lon = DEFAULT_COORDS[spec["o_coords"]]
        d_lat, d_lon = DEFAULT_COORDS[spec["d_coords"]]

        # Create time objects
        depart_time = datetime.combine(
            datetime.now(tz=UTC).date(),
            time(spec["depart_hour"], spec.get("depart_minute", 0)),
        )
        arrive_time = datetime.combine(
            datetime.now(tz=UTC).date(),
            time(spec["arrive_hour"], spec.get("arrive_minute", 0)),
        )

        trip = create_unlinked_trip(
            trip_id=spec["trip_id"],
            person_id=person_id,
            hh_id=hh_id,
            day_id=day_id,
            o_taz=spec["o_taz"],
            d_taz=spec["d_taz"],
            o_maz=spec.get("o_maz"),
            d_maz=spec.get("d_maz"),
            o_lat=o_lat,
            o_lon=o_lon,
            d_lat=d_lat,
            d_lon=d_lon,
            o_purpose_category=spec["o_purpose_category"],
            d_purpose_category=spec["d_purpose_category"],
            mode_1=spec["mode"],
            mode_type=spec["mode_type"],
            depart_time=depart_time,
            arrive_time=arrive_time,
            travel_time=spec["travel_time"],
            purpose=spec.get("purpose"),
        )
        trips.append(trip)

    return trips


def simple_work_tour(
    hh_id: int = 1,
    person_id: int = 101,
    home_taz: int = 100,
    work_taz: int = 200,
    home_maz: int | None = None,
    work_maz: int | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Create a simple home → work → home tour scenario (2 trips).

    Args:
        hh_id: Household ID
        person_id: Person ID
        home_taz: Home TAZ
        work_taz: Work TAZ
        home_maz: Home MAZ (optional, for DaySim)
        work_maz: Work MAZ (optional, for DaySim)

    Returns:
        Tuple of (households, persons, days, unlinked_trips) DataFrames
    """
    home_lat, home_lon = DEFAULT_COORDS["home"]
    work_lat, work_lon = DEFAULT_COORDS["work"]

    # Create household
    household = create_household(
        hh_id=hh_id,
        home_taz=home_taz,
        home_maz=home_maz,
        home_lat=home_lat,
        home_lon=home_lon,
        num_people=1,
        num_workers=1,
    )

    # Create person
    person = create_person(
        person_id=person_id,
        hh_id=hh_id,
        person_num=1,
        age=AgeCategory.AGE_35_TO_44,
        employment=Employment.EMPLOYED_FULLTIME,
        home_lat=home_lat,
        home_lon=home_lon,
        work_taz=work_taz,
        work_maz=work_maz,
        work_lat=work_lat,
        work_lon=work_lon,
    )

    # Create day
    day = create_day(
        day_id=1,
        person_id=person_id,
        hh_id=hh_id,
        num_trips=2,
        is_complete=True,
    )

    # Create trips using specification
    trip_specs = [
        {
            "trip_id": 1,
            "o_coords": "home",
            "d_coords": "work",
            "o_taz": home_taz,
            "d_taz": work_taz,
            "o_maz": home_maz,
            "d_maz": work_maz,
            "o_purpose_category": PurposeCategory.HOME,
            "d_purpose_category": PurposeCategory.WORK,
            "purpose": Purpose.PRIMARY_WORKPLACE,
            "mode": Mode.HOUSEHOLD_VEHICLE,
            "mode_type": ModeType.CAR,
            "depart_hour": 8,
            "arrive_hour": 9,
            "travel_time": 60,
        },
        {
            "trip_id": 2,
            "o_coords": "work",
            "d_coords": "home",
            "o_taz": work_taz,
            "d_taz": home_taz,
            "o_maz": work_maz,
            "d_maz": home_maz,
            "o_purpose_category": PurposeCategory.WORK,
            "d_purpose_category": PurposeCategory.HOME,
            "purpose": Purpose.HOME,
            "mode": Mode.HOUSEHOLD_VEHICLE,
            "mode_type": ModeType.CAR,
            "depart_hour": 17,
            "arrive_hour": 18,
            "travel_time": 60,
        },
    ]

    trips = _create_trips_from_spec(trip_specs, person_id, hh_id, day_id=1)
    unlinked_trips = pl.DataFrame(trips)

    households = pl.DataFrame([household])
    persons = pl.DataFrame([person])
    days = pl.DataFrame([day])

    return households, persons, days, unlinked_trips


def transit_commute(
    hh_id: int = 1,
    person_id: int = 101,
    home_taz: int = 100,
    work_taz: int = 200,
    home_maz: int | None = None,
    work_maz: int | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Create transit commute scenario with walk-BART-walk (6 trips).

    Pattern: Home → (walk) → BART → (BART) → BART → (walk) → Work → (return)

    Args:
        hh_id: Household ID
        person_id: Person ID
        home_taz: Home TAZ
        work_taz: Work TAZ
        home_maz: Home MAZ (optional)
        work_maz: Work MAZ (optional)

    Returns:
        Tuple of (households, persons, days, unlinked_trips) DataFrames
    """
    home_lat, home_lon = DEFAULT_COORDS["home"]
    work_lat, work_lon = DEFAULT_COORDS["work"]

    household = create_household(
        hh_id=hh_id,
        home_taz=home_taz,
        home_maz=home_maz,
        home_lat=home_lat,
        home_lon=home_lon,
        num_people=1,
        num_workers=1,
    )

    person = create_person(
        person_id=person_id,
        hh_id=hh_id,
        person_num=1,
        age=AgeCategory.AGE_35_TO_44,
        employment=Employment.EMPLOYED_FULLTIME,
        home_lat=home_lat,
        home_lon=home_lon,
        work_taz=work_taz,
        work_maz=work_maz,
        work_lat=work_lat,
        work_lon=work_lon,
    )

    day = create_day(
        day_id=1,
        person_id=person_id,
        hh_id=hh_id,
        num_trips=6,
        is_complete=True,
    )

    # Trip specification: Morning commute (3 trips) + Evening return (3 trips)
    trip_specs = [
        # Morning: Home → walk to BART
        {
            "trip_id": 1,
            "o_coords": "home",
            "d_coords": "bart_home",
            "o_taz": home_taz,
            "d_taz": home_taz,
            "o_maz": home_maz,
            "d_maz": home_maz,
            "o_purpose_category": PurposeCategory.HOME,
            "d_purpose_category": PurposeCategory.CHANGE_MODE,
            "mode": Mode.WALK,
            "mode_type": ModeType.WALK,
            "depart_hour": 7,
            "depart_minute": 50,
            "arrive_hour": 8,
            "arrive_minute": 0,
            "travel_time": 10,
        },
        # Morning: BART ride
        {
            "trip_id": 2,
            "o_coords": "bart_home",
            "d_coords": "bart_work",
            "o_taz": home_taz,
            "d_taz": work_taz,
            "o_maz": home_maz,
            "d_maz": work_maz,
            "o_purpose_category": PurposeCategory.CHANGE_MODE,
            "d_purpose_category": PurposeCategory.CHANGE_MODE,
            "mode": Mode.BART,
            "mode_type": ModeType.TRANSIT,
            "depart_hour": 8,
            "depart_minute": 5,
            "arrive_hour": 8,
            "arrive_minute": 35,
            "travel_time": 30,
        },
        # Morning: Walk to work
        {
            "trip_id": 3,
            "o_coords": "bart_work",
            "d_coords": "work",
            "o_taz": work_taz,
            "d_taz": work_taz,
            "o_maz": work_maz,
            "d_maz": work_maz,
            "o_purpose_category": PurposeCategory.CHANGE_MODE,
            "d_purpose_category": PurposeCategory.WORK,
            "mode": Mode.WALK,
            "mode_type": ModeType.WALK,
            "depart_hour": 8,
            "depart_minute": 35,
            "arrive_hour": 8,
            "arrive_minute": 45,
            "travel_time": 10,
        },
        # Evening: Walk to BART
        {
            "trip_id": 4,
            "o_coords": "work",
            "d_coords": "bart_work",
            "o_taz": work_taz,
            "d_taz": work_taz,
            "o_maz": work_maz,
            "d_maz": work_maz,
            "o_purpose_category": PurposeCategory.WORK,
            "d_purpose_category": PurposeCategory.CHANGE_MODE,
            "mode": Mode.WALK,
            "mode_type": ModeType.WALK,
            "depart_hour": 17,
            "depart_minute": 0,
            "arrive_hour": 17,
            "arrive_minute": 10,
            "travel_time": 10,
        },
        # Evening: BART ride
        {
            "trip_id": 5,
            "o_coords": "bart_work",
            "d_coords": "bart_home",
            "o_taz": work_taz,
            "d_taz": home_taz,
            "o_maz": work_maz,
            "d_maz": home_maz,
            "o_purpose_category": PurposeCategory.CHANGE_MODE,
            "d_purpose_category": PurposeCategory.CHANGE_MODE,
            "mode": Mode.BART,
            "mode_type": ModeType.TRANSIT,
            "depart_hour": 17,
            "depart_minute": 15,
            "arrive_hour": 17,
            "arrive_minute": 45,
            "travel_time": 30,
        },
        # Evening: Walk home
        {
            "trip_id": 6,
            "o_coords": "bart_home",
            "d_coords": "home",
            "o_taz": home_taz,
            "d_taz": home_taz,
            "o_maz": home_maz,
            "d_maz": home_maz,
            "o_purpose_category": PurposeCategory.CHANGE_MODE,
            "d_purpose_category": PurposeCategory.HOME,
            "mode": Mode.WALK,
            "mode_type": ModeType.WALK,
            "depart_hour": 17,
            "depart_minute": 45,
            "arrive_hour": 17,
            "arrive_minute": 55,
            "travel_time": 10,
        },
    ]

    trips = _create_trips_from_spec(trip_specs, person_id, hh_id, day_id=1)
    unlinked_trips = pl.DataFrame(trips)

    households = pl.DataFrame([household])
    persons = pl.DataFrame([person])
    days = pl.DataFrame([day])

    return households, persons, days, unlinked_trips


def multi_stop_tour(
    hh_id: int = 1,
    person_id: int = 101,
    home_taz: int = 100,
    work_taz: int = 200,
    stop_taz: int = 150,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Create a work tour with intermediate stop (4 trips).

    Pattern: Home → Work → Stop (lunch/errand) → Work → Home

    Args:
        hh_id: Household ID
        person_id: Person ID
        home_taz: Home TAZ
        work_taz: Work TAZ
        stop_taz: Intermediate stop TAZ

    Returns:
        Tuple of (households, persons, days, unlinked_trips) DataFrames
    """
    # Coordinates
    home_lat, home_lon = DEFAULT_COORDS["home"]
    work_lat, work_lon = DEFAULT_COORDS["work"]
    stop_lat, stop_lon = (37.72, -122.42)  # Intermediate stop location

    household = create_household(
        hh_id=hh_id,
        home_taz=home_taz,
        home_lat=home_lat,
        home_lon=home_lon,
        num_people=1,
        num_workers=1,
    )

    person = create_person(
        person_id=person_id,
        hh_id=hh_id,
        person_num=1,
        age=AgeCategory.AGE_35_TO_44,
        employment=Employment.EMPLOYED_FULLTIME,
        home_lat=home_lat,
        home_lon=home_lon,
        work_taz=work_taz,
        work_lat=work_lat,
        work_lon=work_lon,
    )

    day = create_day(day_id=1, person_id=person_id, hh_id=hh_id, num_trips=4)

    # Home → Work → Stop → Work → Home
    trips = [
        create_unlinked_trip(
            trip_id=1,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=home_taz,
            d_taz=work_taz,
            o_lat=home_lat,
            o_lon=home_lon,
            d_lat=work_lat,
            d_lon=work_lon,
            purpose=Purpose.PRIMARY_WORKPLACE,
            o_purpose_category=PurposeCategory.HOME,
            d_purpose_category=PurposeCategory.WORK,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(8, 0)
            ),
        ),
        create_unlinked_trip(
            trip_id=2,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=work_taz,
            d_taz=stop_taz,
            o_lat=work_lat,
            o_lon=work_lon,
            d_lat=stop_lat,
            d_lon=stop_lon,
            purpose=Purpose.DINING,
            o_purpose_category=PurposeCategory.WORK,
            d_purpose_category=PurposeCategory.MEAL,
            mode_type=ModeType.WALK,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(12, 0)
            ),
        ),
        create_unlinked_trip(
            trip_id=3,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=stop_taz,
            d_taz=work_taz,
            o_lat=stop_lat,
            o_lon=stop_lon,
            d_lat=work_lat,
            d_lon=work_lon,
            purpose=Purpose.PRIMARY_WORKPLACE,
            o_purpose_category=PurposeCategory.MEAL,
            d_purpose_category=PurposeCategory.WORK,
            mode_type=ModeType.WALK,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(13, 0)
            ),
        ),
        create_unlinked_trip(
            trip_id=4,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=work_taz,
            d_taz=home_taz,
            o_lat=work_lat,
            o_lon=work_lon,
            d_lat=home_lat,
            d_lon=home_lon,
            purpose=Purpose.HOME,
            o_purpose_category=PurposeCategory.WORK,
            d_purpose_category=PurposeCategory.HOME,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(17, 0)
            ),
        ),
    ]

    households = pl.DataFrame([household])
    persons = pl.DataFrame([person])
    days = pl.DataFrame([day])
    unlinked_trips = pl.DataFrame(trips)

    return households, persons, days, unlinked_trips


def multi_tour_day(
    hh_id: int = 1,
    person_id: int = 101,
    home_taz: int = 100,
    work_taz: int = 200,
    shop_taz: int = 150,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Create a person with multiple tours in one day.

    Pattern: Home → Work → Home → Shopping → Home

    Args:
        hh_id: Household ID
        person_id: Person ID
        home_taz: Home TAZ
        work_taz: Work TAZ
        shop_taz: Shopping TAZ

    Returns:
        Tuple of (households, persons, days, unlinked_trips) DataFrames
    """
    # Coordinates
    home_lat, home_lon = DEFAULT_COORDS["home"]
    work_lat, work_lon = DEFAULT_COORDS["work"]
    shop_lat, shop_lon = (37.73, -122.43)  # Shopping location

    household = create_household(
        hh_id=hh_id,
        home_taz=home_taz,
        home_lat=home_lat,
        home_lon=home_lon,
        num_people=1,
        num_workers=1,
    )

    person = create_person(
        person_id=person_id,
        hh_id=hh_id,
        person_num=1,
        age=AgeCategory.AGE_35_TO_44,
        employment=Employment.EMPLOYED_FULLTIME,
        work_taz=work_taz,
        home_lat=home_lat,
        home_lon=home_lon,
    )

    day = create_day(day_id=1, person_id=person_id, hh_id=hh_id, num_trips=4)

    # Home → Work → Home → Shop → Home
    trips = [
        create_unlinked_trip(
            trip_id=1,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=home_taz,
            d_taz=work_taz,
            o_lat=home_lat,
            o_lon=home_lon,
            d_lat=work_lat,
            d_lon=work_lon,
            purpose=Purpose.PRIMARY_WORKPLACE,
            o_purpose_category=PurposeCategory.HOME,
            d_purpose_category=PurposeCategory.WORK,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(8, 0)
            ),
        ),
        create_unlinked_trip(
            trip_id=2,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=work_taz,
            d_taz=home_taz,
            o_lat=work_lat,
            o_lon=work_lon,
            d_lat=home_lat,
            d_lon=home_lon,
            purpose=Purpose.HOME,
            o_purpose_category=PurposeCategory.WORK,
            d_purpose_category=PurposeCategory.HOME,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(17, 0)
            ),
        ),
        create_unlinked_trip(
            trip_id=3,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=home_taz,
            d_taz=shop_taz,
            o_lat=home_lat,
            o_lon=home_lon,
            d_lat=shop_lat,
            d_lon=shop_lon,
            purpose=Purpose.GROCERY,
            o_purpose_category=PurposeCategory.HOME,
            d_purpose_category=PurposeCategory.SHOP,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(19, 0)
            ),
        ),
        create_unlinked_trip(
            trip_id=4,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=shop_taz,
            d_taz=home_taz,
            o_lat=shop_lat,
            o_lon=shop_lon,
            d_lat=home_lat,
            d_lon=home_lon,
            purpose=Purpose.HOME,
            o_purpose_category=PurposeCategory.SHOP,
            d_purpose_category=PurposeCategory.HOME,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(20, 0)
            ),
        ),
    ]

    households = pl.DataFrame([household])
    persons = pl.DataFrame([person])
    days = pl.DataFrame([day])
    unlinked_trips = pl.DataFrame(trips)

    return households, persons, days, unlinked_trips


def work_tour_no_usual_location(
    hh_id: int = 1,
    person_id: int = 101,
    home_taz: int = 100,
    work_taz: int = 200,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Create a work tour for person without usual work location defined.

    Tests edge case where worker has no work_lat/work_lon set in person record.
    The tour still goes to a work destination, but person doesn't have a
    defined usual workplace.

    Pattern: Home → Work → Home

    Args:
        hh_id: Household ID
        person_id: Person ID
        home_taz: Home TAZ
        work_taz: Work destination TAZ (not usual workplace)

    Returns:
        Tuple of (households, persons, days, unlinked_trips) DataFrames
    """
    # Coordinates
    home_lat, home_lon = DEFAULT_COORDS["home"]
    work_lat, work_lon = DEFAULT_COORDS["work"]

    household = create_household(
        hh_id=hh_id, home_taz=home_taz, num_people=1, num_workers=1
    )

    # Worker WITHOUT work location defined (work_lat/lon = None)
    person = create_person(
        person_id=person_id,
        hh_id=hh_id,
        person_num=1,
        age=AgeCategory.AGE_35_TO_44,
        employment=Employment.EMPLOYED_FULLTIME,
        work_lat=None,  # No usual work location
        work_lon=None,
        work_taz=None,
    )

    day = create_day(day_id=1, person_id=person_id, hh_id=hh_id, num_trips=2)

    # Home → Work → Home (work destination is ad-hoc, not usual location)
    trips = [
        create_unlinked_trip(
            trip_id=1,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=home_taz,
            d_taz=work_taz,
            o_lat=home_lat,
            o_lon=home_lon,
            d_lat=work_lat,
            d_lon=work_lon,
            purpose=Purpose.PRIMARY_WORKPLACE,
            o_purpose_category=PurposeCategory.HOME,
            d_purpose_category=PurposeCategory.WORK,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(8, 0)
            ),
        ),
        create_unlinked_trip(
            trip_id=2,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=work_taz,
            d_taz=home_taz,
            o_lat=work_lat,
            o_lon=work_lon,
            d_lat=home_lat,
            d_lon=home_lon,
            purpose=Purpose.HOME,
            o_purpose_category=PurposeCategory.WORK,
            d_purpose_category=PurposeCategory.HOME,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(17, 0)
            ),
        ),
    ]

    households = pl.DataFrame([household])
    persons = pl.DataFrame([person])
    days = pl.DataFrame([day])
    unlinked_trips = pl.DataFrame(trips)

    return households, persons, days, unlinked_trips


def multi_person_household(
    hh_id: int = 1,
    num_workers: int = 2,
    num_students: int = 1,
    num_children: int = 1,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Create a multi-person household with various person types.

    Uses data-driven approach to create persons based on type specifications.

    Args:
        hh_id: Household ID
        num_workers: Number of workers (full-time)
        num_students: Number of students (university)
        num_children: Number of children (age 5-15)

    Returns:
        Tuple of (households, persons) DataFrames (no trips/days)
    """
    total_people = num_workers + num_students + num_children

    household = create_household(
        hh_id=hh_id,
        num_people=total_people,
        num_workers=num_workers,
        num_vehicles=num_workers,
    )

    home_lat = household["home_lat"]
    home_lon = household["home_lon"]

    # Person type specifications
    person_specs = []
    person_num = 1
    base_person_id = hh_id * 100

    # Add workers
    for i in range(num_workers):
        person_specs.append(
            {
                "person_id": base_person_id + person_num,
                "person_num": person_num,
                "age": AgeCategory.AGE_35_TO_44,
                "employment": Employment.EMPLOYED_FULLTIME,
                "student": Student.NONSTUDENT,
                "work_taz": 200 + i,
            }
        )
        person_num += 1

    # Add students
    for _ in range(num_students):
        person_specs.append(
            {
                "person_id": base_person_id + person_num,
                "person_num": person_num,
                "age": AgeCategory.AGE_18_TO_24,
                "employment": Employment.UNEMPLOYED_NOT_LOOKING,
                "student": Student.FULLTIME_INPERSON,
                "school_taz": 300,
            }
        )
        person_num += 1

    # Add children
    for _ in range(num_children):
        person_specs.append(
            {
                "person_id": base_person_id + person_num,
                "person_num": person_num,
                "age": AgeCategory.AGE_5_TO_15,
                "employment": Employment.UNEMPLOYED_NOT_LOOKING,
                "student": Student.FULLTIME_INPERSON,
                "school_taz": 310,
            }
        )
        person_num += 1

    # Create persons from specifications
    persons = []
    for spec in person_specs:
        person = create_person(
            hh_id=hh_id, home_lat=home_lat, home_lon=home_lon, **spec
        )
        persons.append(person)

    households = pl.DataFrame([household])
    persons_df = pl.DataFrame(persons)

    return households, persons_df


# ==============================================================================
# Pre-built Household Scenarios for Tests
# ==============================================================================


def create_single_adult_household():
    """Create a single full-time worker household for tests."""
    household = create_household(
        hh_id=1,
        home_taz=100,
        num_people=1,
        num_vehicles=1,
        num_workers=1,
        income_detailed=IncomeDetailed.INCOME_75TO100,
    )

    person = create_person(
        person_id=101,
        hh_id=1,
        person_num=1,
        age=AgeCategory.AGE_35_TO_44,
        gender=Gender.MALE,
        employment=Employment.EMPLOYED_FULLTIME,
        student=Student.NONSTUDENT,
        commute_subsidy=CommuteSubsidy.FREE_PARK,
    )

    return pl.DataFrame([household]), pl.DataFrame([person])


def create_family_household():
    """Create household with working adults and school-age children."""
    household = create_household(
        hh_id=2,
        home_taz=200,
        num_people=4,
        num_vehicles=2,
        num_workers=2,
        income_detailed=IncomeDetailed.INCOME_100TO150,
    )

    persons = [
        # Full-time worker (parent 1)
        create_person(
            person_id=201,
            hh_id=2,
            person_num=1,
            age=AgeCategory.AGE_35_TO_44,
            gender=Gender.FEMALE,
            employment=Employment.EMPLOYED_FULLTIME,
            student=Student.NONSTUDENT,
        ),
        # Part-time worker (parent 2)
        create_person(
            person_id=202,
            hh_id=2,
            person_num=2,
            age=AgeCategory.AGE_35_TO_44,
            gender=Gender.MALE,
            employment=Employment.EMPLOYED_PARTTIME,
            student=Student.NONSTUDENT,
        ),
        # High school student
        create_person(
            person_id=203,
            hh_id=2,
            person_num=3,
            age=AgeCategory.AGE_16_TO_17,
            gender=Gender.FEMALE,
            employment=Employment.UNEMPLOYED_NOT_LOOKING,
            student=Student.FULLTIME_INPERSON,
            school_type=SchoolType.HIGH_SCHOOL,
        ),
        # Child under 16
        create_person(
            person_id=204,
            hh_id=2,
            person_num=4,
            age=AgeCategory.AGE_5_TO_15,
            gender=Gender.MALE,
            employment=Employment.UNEMPLOYED_NOT_LOOKING,
            student=Student.FULLTIME_INPERSON,
            school_type=SchoolType.ELEMENTARY,
        ),
    ]

    return pl.DataFrame([household]), pl.DataFrame(persons)


def create_retired_household():
    """Create a household with retired persons for tests."""
    household = create_household(
        hh_id=3,
        home_taz=300,
        num_people=2,
        num_vehicles=1,
        num_workers=0,
        income_detailed=IncomeDetailed.INCOME_50TO75,
    )

    persons = [
        create_person(
            person_id=301,
            hh_id=3,
            person_num=1,
            age=AgeCategory.AGE_65_TO_74,
            gender=Gender.MALE,
            employment=Employment.UNEMPLOYED_NOT_LOOKING,
            student=Student.NONSTUDENT,
        ),
        create_person(
            person_id=302,
            hh_id=3,
            person_num=2,
            age=AgeCategory.AGE_65_TO_74,
            gender=Gender.FEMALE,
            employment=Employment.UNEMPLOYED_NOT_LOOKING,
            student=Student.NONSTUDENT,
        ),
    ]

    return pl.DataFrame([household]), pl.DataFrame(persons)


def create_university_student_household():
    """Create a household with university students for tests."""
    household = create_household(
        hh_id=4,
        home_taz=400,
        num_people=1,
        num_vehicles=1,
        num_workers=0,
        income_detailed=IncomeDetailed.INCOME_35TO50,
    )

    person = create_person(
        person_id=401,
        hh_id=4,
        person_num=1,
        age=AgeCategory.AGE_18_TO_24,
        gender=Gender.FEMALE,
        employment=Employment.UNEMPLOYED_NOT_LOOKING,
        student=Student.FULLTIME_INPERSON,
        school_type=SchoolType.COLLEGE_4YEAR,
    )

    return pl.DataFrame([household]), pl.DataFrame([person])
