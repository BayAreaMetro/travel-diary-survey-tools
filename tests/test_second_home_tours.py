"""Second homes bound tours, and tours touching them are graded OTHER_HOME.

A person's other home is known only from the respondent: a vendor's
second-home address, or a day they said began or ended at home somewhere other
than their reported home. Travel alone never makes one.

Covers, through the pipeline's three stages (delivery, detection, tours):
- A round trip from the second home is a closed tour, graded OTHER_HOME
- The drive straight from the primary home to the second is OTHER_HOME, not
  NO_DESTINATION
- A tour from the primary home is untouched
- A stated day end locates the second home just as the vendor address does
- Without either, the second home is just a place, and the tour to it stays open
"""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.days import BeginEndDay, TravelDow
from data_canon.codebook.generic import LocationType
from data_canon.codebook.persons import AgeCategory, Employment, Student
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

HOME = (37.80, -122.40)
SECOND_HOME = (38.30, -122.90)
SHOP = (37.82, -122.42)
SHOP_NEAR_SECOND = (38.31, -122.91)


@pytest.fixture
def person_and_household():
    """One retiree with only a home."""
    persons = pl.DataFrame(
        {
            "person_id": [1],
            "hh_id": [1],
            "age": [AgeCategory.AGE_65_TO_74.value],
            "employment": [Employment.UNEMPLOYED_NOT_LOOKING.value],
            "student": [Student.NONSTUDENT.value],
            "school_type": [None],
            "work_lat": [None],
            "work_lon": [None],
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
    households = pl.DataFrame({"hh_id": [1], "home_lat": [HOME[0]], "home_lon": [HOME[1]]})
    return persons, households


# (day, depart hour, arrive hour, origin, destination, origin purpose, destination purpose)
WEEKEND = [
    (1, 9, 9.5, HOME, SHOP, Purpose.HOME, Purpose.GROCERY),
    (1, 10.5, 11, SHOP, HOME, Purpose.GROCERY, Purpose.HOME),
    (1, 17, 18, HOME, SECOND_HOME, Purpose.HOME, Purpose.OTHER_RESIDENCE),
    (2, 9, 9.5, SECOND_HOME, SHOP_NEAR_SECOND, Purpose.OTHER_RESIDENCE, Purpose.GROCERY),
    (2, 11, 11.5, SHOP_NEAR_SECOND, SECOND_HOME, Purpose.GROCERY, Purpose.OTHER_RESIDENCE),
]


def _at(day: int, hour: float) -> datetime:
    return datetime(2024, 1, 16 + day, int(hour), int((hour % 1) * 60))


def _unlinked(legs) -> pl.DataFrame:
    """Unlinked trips for the legs."""
    n = len(legs)

    def category(purpose: Purpose) -> int:
        return PurposeToCategoryMap.PURPOSE_TO_CATEGORY[purpose].value

    return pl.DataFrame(
        {
            "unlinked_trip_id": list(range(1, n + 1)),
            "day_id": [leg[0] for leg in legs],
            "person_id": [1] * n,
            "hh_id": [1] * n,
            "travel_dow": [TravelDow.SATURDAY.value] * n,
            "depart_time": [_at(leg[0], leg[1]) for leg in legs],
            "arrive_time": [_at(leg[0], leg[2]) for leg in legs],
            "o_purpose": [leg[5].value for leg in legs],
            "d_purpose": [leg[6].value for leg in legs],
            "o_purpose_category": [category(leg[5]) for leg in legs],
            "d_purpose_category": [category(leg[6]) for leg in legs],
            "mode_type": [ModeType.CAR.value] * n,
            "o_lat": [leg[3][0] for leg in legs],
            "o_lon": [leg[3][1] for leg in legs],
            "d_lat": [leg[4][0] for leg in legs],
            "d_lon": [leg[4][1] for leg in legs],
            "unlinked_trip_weight": [1.0] * n,
            "distance_meters": [5000.0] * n,
            "duration_minutes": [30.0] * n,
            "num_travelers": [1] * n,
            "driver": [Driver.DRIVER.value] * n,
        }
    )


def _days(end_day_1: BeginEndDay | None = None) -> pl.DataFrame:
    """The two diary days, with day 1's stated end."""
    return pl.DataFrame(
        {
            "day_id": [1, 2],
            "begin_day": [None, None],
            "end_day": [end_day_1.value if end_day_1 else None, None],
        },
        schema={"day_id": pl.Int64, "begin_day": pl.Int64, "end_day": pl.Int64},
    )


def _vendor_second_home() -> pl.DataFrame:
    """The vendor's second-home address, as cleaning hands it to the helper."""
    return pl.DataFrame(
        {
            "person_id": [1],
            "location_type": [LocationType.HOME.value],
            "lat": [SECOND_HOME[0]],
            "lon": [SECOND_HOME[1]],
        }
    )


def _extract(persons, households, *, days, second_home=None) -> dict:
    link_result = link_trips(
        unlinked_trips=_unlinked(WEEKEND),
        change_mode_enum=PurposeCategory.CHANGE_MODE.value,
        transit_mode_enums=[ModeType.TRANSIT.value],
        split_on_occupancy=False,
    )
    linked = link_result["linked_trips"].with_columns(
        pl.lit(None).cast(pl.Int64).alias("joint_trip_id")
    )
    return locate_and_extract_tours(
        persons,
        households,
        link_result["unlinked_trips"],
        linked,
        days=days,
        extra=second_home,
    )


def _quality(tours: pl.DataFrame) -> list[int]:
    return tours.sort("origin_depart_time")["tour_data_quality"].to_list()


@pytest.mark.parametrize(
    ("days", "supplied"),
    [
        (_days(), _vendor_second_home()),  # the vendor's second-home address
        (_days(BeginEndDay.OTHER_HOME), None),  # "the day ended at my other home"
    ],
)
def test_second_home_bounds_tours(person_and_household, days, supplied):
    """Shop from home; drive to the second home; shop from there."""
    result = _extract(*person_and_household, days=days, second_home=supplied)
    tours = result["tours"]

    assert tours["tour_category"].to_list() == [TourCategory.COMPLETE.value] * 3
    assert _quality(tours) == [
        TourDataQuality.VALID.value,  # from the primary home: untouched
        TourDataQuality.OTHER_HOME.value,  # the drive between homes
        TourDataQuality.OTHER_HOME.value,  # a round trip from the second home
    ]
    homes = result["habitual_locations"].filter(pl.col("location_type") == LocationType.HOME.value)
    assert homes.height == 2


def test_without_a_report_the_second_home_is_just_a_place(person_and_household):
    """Nights at "another residence" do not make a home on their own."""
    result = _extract(*person_and_household, days=_days())
    tours = result["tours"].sort("origin_depart_time")

    homes = result["habitual_locations"].filter(pl.col("location_type") == LocationType.HOME.value)
    assert homes.height == 1
    assert tours["tour_category"].to_list() == [
        TourCategory.COMPLETE.value,
        TourCategory.PARTIAL_END.value,
        TourCategory.PARTIAL_BOTH.value,
    ]
    assert TourDataQuality.OTHER_HOME.value not in tours["tour_data_quality"].to_list()
