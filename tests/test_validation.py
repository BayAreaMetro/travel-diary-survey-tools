"""Tests for data validation framework."""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.days import TravelDow
from data_canon.codebook.persons import AgeCategory, Gender
from data_canon.codebook.tours import TourDataQuality
from data_canon.codebook.trips import ModeType, PurposeCategory
from data_canon.core.dataclass import CanonicalData
from data_canon.core.exceptions import DataValidationError
from data_canon.validation.custom import (
    check_trip_spatial_continuity,
    check_valid_tours_are_complete,
)
from tests.fixtures import create_household, create_person, create_unlinked_trip

# The same two-household, two-person block is what every structural rule below
# needs; only the defect changes, so each test states just its own defect.
HOUSEHOLD_KWARGS = [
    {"hh_id": 1, "home_taz": 100, "income": 50000, "num_vehicles": 1},
    {
        "hh_id": 2,
        "home_taz": 200,
        "home_lat": 37.8,
        "home_lon": -122.5,
        "income": 75000,
        "num_vehicles": 2,
    },
    {
        "hh_id": 3,
        "home_taz": 300,
        "home_lat": 37.9,
        "home_lon": -122.6,
        "income": 100000,
        "num_vehicles": 2,
    },
]


def _households(hh_ids: list[int], *, num_people: int = 1) -> pl.DataFrame:
    """Households by id, drawn from a fixed block so the ids are the only variable."""
    by_id = {kwargs["hh_id"]: kwargs for kwargs in HOUSEHOLD_KWARGS}
    return pl.DataFrame(
        [create_household(**by_id[hh_id], num_people=num_people) for hh_id in hh_ids]
    )


def _persons(hh_ids: list[int]) -> pl.DataFrame:
    """One person per entry, numbered 101 upwards, in the household named."""
    return pl.DataFrame(
        [
            create_person(
                person_id=101 + i,
                hh_id=hh_id,
                age=AgeCategory.AGE_5_TO_15,
                gender=Gender.MALE if i % 2 == 0 else Gender.FEMALE,
            )
            for i, hh_id in enumerate(hh_ids)
        ]
    )


def _linked(households: list[int], persons: list[int]) -> CanonicalData:
    data = CanonicalData()
    data.households = _households(households)
    data.persons = _persons(persons)
    return data


class TestUniqueConstraints:
    """Tests for uniqueness validation."""

    def test_unique_passes(self):
        """Should pass with unique IDs."""
        data = CanonicalData()
        data.households = _households([1, 2, 3], num_people=2)
        data.validate("households", step="link_trips")

    def test_unique_fails_with_duplicates(self):
        """Should fail with duplicate IDs."""
        data = CanonicalData()
        data.households = pl.concat([_households([1, 2], num_people=2), _households([2])])
        with pytest.raises(DataValidationError) as exc:
            data.validate("households", step="link_trips")
        assert exc.value.rule == "unique_constraint"


class TestForeignKeys:
    """Tests for FK validation, in both directions."""

    def test_fk_and_required_children_pass(self):
        """Every person has a household, and every household has a person."""
        data = _linked(households=[1, 2], persons=[1, 2])
        data.validate("persons", step="link_trips")
        data.validate("households", step="link_trips")

    def test_fk_fails_with_orphans(self):
        """Should fail with orphaned FKs."""
        data = _linked(households=[1, 2], persons=[1, 999])
        with pytest.raises(DataValidationError) as exc:
            data.validate("persons", step="link_trips")
        assert exc.value.rule == "foreign_key"

    def test_required_children_fails(self):
        """Should fail when parent missing children."""
        data = _linked(households=[1, 2, 3], persons=[1, 2])
        with pytest.raises(DataValidationError) as exc:
            data.validate("households", step="link_trips")
        assert exc.value.rule == "required_children"


class TestRequiredChildrenWhen:
    """Only surveyable persons need a day (``required_child_when``).

    Unrelated household members are enumerated for household composition and
    weighting, but the survey never asks for their travel and the vendor gives
    them no day rows. Requiring a day of them could only be satisfied by
    fabricating one, which reads downstream as a genuine no-travel day.
    """

    def _data(self, *, surveyable_values: list[bool | None], with_days: list[int]):
        data = CanonicalData()
        data.households = pl.DataFrame(
            [create_household(hh_id=1, home_taz=100, income=50000, num_people=2, num_vehicles=1)]
        )
        data.persons = pl.DataFrame(
            [
                create_person(
                    person_id=101 + i,
                    hh_id=1,
                    age=AgeCategory.AGE_35_TO_44,
                    gender=Gender.FEMALE,
                )
                for i in range(len(surveyable_values))
            ]
        ).with_columns(pl.Series("surveyable", surveyable_values, dtype=pl.Boolean))
        data.days = pl.DataFrame(
            {
                "day_id": [1000 + p for p in with_days],
                "person_id": with_days,
                "hh_id": [1] * len(with_days),
                "travel_date": [datetime(2024, 1, 17)] * len(with_days),
                "travel_dow": [TravelDow.WEDNESDAY.value] * len(with_days),
            }
        )
        return data

    def test_unsurveyable_person_may_have_no_days(self):
        """A person marked unsurveyable is legitimately childless."""
        data = self._data(surveyable_values=[True, False], with_days=[101])
        data.validate("persons", step="write_data")

    def test_surveyable_person_without_days_still_fails(self):
        """The constraint keeps its teeth where it is meaningful."""
        data = self._data(surveyable_values=[True, True], with_days=[101])
        with pytest.raises(DataValidationError) as exc:
            data.validate("persons", step="write_data")
        assert exc.value.rule == "required_children"
        assert "surveyable" in str(exc.value)

    def test_null_surveyable_counts_as_surveyable(self):
        """Null must not silently exempt a row from the constraint."""
        data = self._data(surveyable_values=[True, None], with_days=[101])
        with pytest.raises(DataValidationError) as exc:
            data.validate("persons", step="write_data")
        assert exc.value.rule == "required_children"

    def test_missing_when_column_requires_children_for_all(self):
        """Without the column the constraint applies to every row, not none."""
        data = self._data(surveyable_values=[True, True], with_days=[101])
        data.persons = data.persons.drop("surveyable")
        with pytest.raises(DataValidationError) as exc:
            data.validate("persons", step="write_data")
        assert exc.value.rule == "required_children"


class TestCustomValidators:
    """Tests for custom validator registration."""

    def test_single_table_validator(self):
        """Should run custom validator on single table."""
        data_obj = CanonicalData()

        @data_obj.register_validator("unlinked_trips")
        def check_trip_duration(unlinked_trips: pl.DataFrame) -> list[str]:
            """Check that trips are not unreasonably long (>4 hours)."""
            hours = (pl.col("arrive_time") - pl.col("depart_time")).dt.total_seconds() / 3600
            long_trips = unlinked_trips.filter(hours > 4)
            if len(long_trips) > 0:
                trip_ids = long_trips["unlinked_trip_id"].to_list()[:5]
                return [f"Found {len(long_trips)} trips longer than 4 hours: {trip_ids}"]
            return []

        def trip(trip_id: int, depart: datetime, arrive: datetime) -> dict:
            return create_unlinked_trip(
                unlinked_trip_id=trip_id,
                day_id=10101,
                depart_time=depart,
                arrive_time=arrive,
                duration_minutes=(arrive - depart).total_seconds() / 60,
                o_lat=37.7749,
                o_lon=-122.4194,
                d_lat=37.7849,
                d_lon=-122.4094,
                o_purpose_category=PurposeCategory.HOME,
                d_purpose_category=PurposeCategory.WORK,
                mode_type=ModeType.WALK,
            )

        day = datetime(2024, 1, 15)
        data_obj.unlinked_trips = pl.DataFrame(
            [
                trip(1, day.replace(hour=10), day.replace(hour=10, minute=30)),
                trip(2, day.replace(hour=11), day.replace(hour=11, minute=30)),
                # Ten hours long: the one the validator is meant to catch.
                trip(3, day.replace(hour=8), day.replace(hour=18)),
            ]
        )
        with pytest.raises(DataValidationError) as exc:
            data_obj.validate("unlinked_trips", step="link_trips")
        assert exc.value.rule == "check_trip_duration"

    def test_multi_table_validator(self):
        """Should run custom validator with multiple tables."""
        data = CanonicalData()

        @data.register_validator("persons")
        def check_size(
            persons: pl.DataFrame,
            households: pl.DataFrame,
        ) -> list[str]:
            actual = persons.group_by("hh_id").agg(pl.len().alias("n"))
            merged = households.join(actual, on="hh_id", how="left")
            bad = merged.filter(pl.col("num_people") != pl.col("n"))
            if len(bad) > 0:
                return ["Size mismatch"]
            return []

        data.households = _households([1, 2])
        data.persons = _persons([1, 2])
        data.validate("persons", step="link_trips")


class TestValidToursAreComplete:
    """Tests for the check_valid_tours_are_complete custom validator."""

    def _tours(self, quality, *, single_trip, purpose):
        """Build a one-row tours frame with the given quality/trip count/purpose."""
        return pl.DataFrame(
            {
                "tour_id": [1],
                "tour_data_quality": [quality],
                "trip_count": [1 if single_trip else 3],
                "tour_purpose": [purpose],
            },
            schema={
                "tour_id": pl.Int64,
                "tour_data_quality": pl.Int64,
                "trip_count": pl.Int64,
                "tour_purpose": pl.Int64,
            },
        )

    @pytest.mark.parametrize(
        ("quality", "single_trip", "purpose", "n_errors", "says"),
        [
            pytest.param(
                TourDataQuality.VALID.value,
                False,
                PurposeCategory.WORK.value,
                0,
                "",
                id="valid-and-complete",
            ),
            # A single-trip tour flagged non-VALID is allowed to lack a purpose.
            pytest.param(
                TourDataQuality.NO_DESTINATION.value, True, None, 0, "", id="invalid-single-trip"
            ),
            pytest.param(
                TourDataQuality.VALID.value,
                True,
                PurposeCategory.WORK.value,
                1,
                "VALID",
                id="valid-but-single-trip",
            ),
            pytest.param(
                TourDataQuality.VALID.value, False, None, 1, "VALID", id="valid-but-null-purpose"
            ),
        ],
    )
    def test_valid_tours_must_be_complete(
        self, quality: int, single_trip: bool, purpose: int | None, n_errors: int, says: str
    ):
        """A tour labelled VALID has to hold a real, multi-trip, purposeful tour."""
        errors = check_valid_tours_are_complete(
            self._tours(quality, single_trip=single_trip, purpose=purpose)
        )

        assert len(errors) == n_errors
        if says:
            assert says in errors[0]

    def test_missing_quality_column_is_noop(self):
        """Frames without tour_data_quality produce no errors."""
        tours = pl.DataFrame({"tour_id": [1], "trip_count": [1], "tour_purpose": [None]})
        assert check_valid_tours_are_complete(tours) == []


class TestTripSpatialContinuity:
    """Tests for the check_trip_spatial_continuity custom validator."""

    def _trips(self, points):
        """Build a linked_trips frame from (person, day, depart, o, d) points.

        Each origin/destination is a (lat, lon) tuple.
        """
        return pl.DataFrame(
            {
                "linked_trip_id": list(range(1, len(points) + 1)),
                "person_id": [p[0] for p in points],
                "day_id": [p[1] for p in points],
                "depart_time": [p[2] for p in points],
                "o_lat": [p[3][0] for p in points],
                "o_lon": [p[3][1] for p in points],
                "d_lat": [p[4][0] for p in points],
                "d_lon": [p[4][1] for p in points],
            }
        )

    def test_continuous_trips_pass(self):
        """A day where each trip resumes where the last ended has no gaps."""
        home, work = (37.70, -122.40), (37.80, -122.45)
        trips = self._trips(
            [
                (1, 1, 8.0, home, work),
                (1, 1, 17.0, work, home),  # resumes at work -> continuous
            ]
        )
        assert check_trip_spatial_continuity(trips) == []

    def test_small_sample_with_gap_never_fails(self):
        """A high gap rate over only a few junctions is noise, not a failure."""
        home, a = (37.70, -122.40), (37.75, -122.42)
        far = (38.50, -123.20)
        # One person-day, one junction that jumps: 100% rate but tiny sample.
        trips = self._trips(
            [
                (1, 1, 8.0, home, a),
                (1, 1, 17.0, far, home),
            ]
        )
        assert check_trip_spatial_continuity(trips) == []

    def test_low_rate_at_scale_passes(self):
        """A small fraction of gaps across many junctions is normal survey noise."""
        home, a = (37.70, -122.40), (37.75, -122.42)
        far = (38.20, -122.90)
        points = []
        # 1,200 continuous person-days (1 junction each, gap 0)
        for person in range(1, 1201):
            points.append((person, 1, 8.0, home, a))
            points.append((person, 1, 17.0, a, home))
        # 20 person-days with a genuine gap -> ~1.6% << 15% ceiling
        for person in range(1201, 1221):
            points.append((person, 1, 8.0, home, a))
            points.append((person, 1, 17.0, far, home))
        assert check_trip_spatial_continuity(self._trips(points)) == []

    def test_high_rate_at_scale_fails(self):
        """A high gap rate over a meaningful sample flags a systemic problem."""
        home, a = (37.70, -122.40), (37.75, -122.42)
        far = (38.50, -123.20)
        points = []
        # 1,200 person-days where every junction jumps -> 100% rate at scale.
        for person in range(1, 1201):
            points.append((person, 1, 8.0, home, a))
            points.append((person, 1, 17.0, far, home))
        errors = check_trip_spatial_continuity(self._trips(points))
        assert len(errors) == 1
        assert "systemic" in errors[0].lower()

    def test_missing_columns_is_noop(self):
        """Frames without coordinate columns produce no errors."""
        trips = pl.DataFrame({"linked_trip_id": [1], "person_id": [1], "day_id": [1]})
        assert check_trip_spatial_continuity(trips) == []
