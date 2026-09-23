"""Tests for joint tour identification functionality.

This test module ensures joint tour identification correctly handles:
- Tours where all trips are joint with same stable group (2+ people)
- Partial dropoffs (3 people start, 1 drops off, remaining 2 form joint tour)
- Tours with a solo leg, which are not joint
- The same group on two occasions, which are two joint tours
- Tours without any joint trips
"""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.days import TravelDow
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    SchoolType,
    Student,
)
from data_canon.codebook.trips import Driver, ModeType, Purpose, PurposeCategory
from processing import link_trips
from processing.joint_trips import detect_joint_trips
from processing.tours.joint_tour_helpers import (
    _validate_joint_tours_have_joint_trips,
    identify_joint_tours,
)
from tests.fixtures.tour_pipeline import locate_and_extract_tours

HH_ID = 23000075
P1 = 23000075001
P2 = 23000075002
P3 = 23000075003
PERSON_IDS = [P1, P2, P3]
DAY_IDS = [2300007500101, 2300007500201, 2300007500301]

HOME = (37.8, -122.4)
SHOP = (37.82, -122.42)


@pytest.fixture
def basic_household_data():
    """Create basic household and person data for 3-person household."""
    persons = pl.DataFrame(
        {
            "person_id": PERSON_IDS,
            "hh_id": [HH_ID] * 3,
            "age": [
                AgeCategory.AGE_35_TO_44.value,
                AgeCategory.AGE_35_TO_44.value,
                AgeCategory.AGE_5_TO_15.value,
            ],
            "employment": [
                Employment.EMPLOYED_FULLTIME.value,
                Employment.EMPLOYED_PARTTIME.value,
                Employment.UNEMPLOYED_NOT_LOOKING.value,
            ],
            "student": [
                Student.NONSTUDENT.value,
                Student.NONSTUDENT.value,
                Student.FULLTIME_INPERSON.value,
            ],
            "school_type": [None, None, SchoolType.ELEMENTARY.value],
            "work_lat": [37.85, 37.82, None],
            "work_lon": [-122.45, -122.48, None],
            "school_lat": [None, None, 37.81],
            "school_lon": [None, None, -122.43],
        }
    )

    households = pl.DataFrame({"hh_id": [HH_ID], "home_lat": [HOME[0]], "home_lon": [HOME[1]]})

    return persons, households


def link_and_detect_joint_trips(
    unlinked_trips: pl.DataFrame, households: pl.DataFrame
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Helper to link trips and detect joint trips.

    Args:
        unlinked_trips: Unlinked trip data
        households: Household data

    Returns:
        Tuple of (unlinked_trips_with_ids, linked_trips, joint_trips)
    """
    # Link trips first
    link_result = link_trips(
        unlinked_trips=unlinked_trips,
        change_mode_enum=PurposeCategory.CHANGE_MODE.value,
        transit_mode_enums=[ModeType.TRANSIT.value],
        split_on_occupancy=False,
    )
    unlinked_trips_with_ids = link_result["unlinked_trips"]
    linked_trips = link_result["linked_trips"]

    # Detect joint trips
    joint_result = detect_joint_trips(
        linked_trips=linked_trips,
        households=households,
        method="buffer",
        time_threshold_minutes=5.0,
        space_threshold_meters=50.0,
    )
    linked_trips_with_joints = joint_result["linked_trips"]
    joint_trips = joint_result["joint_trips"]

    return unlinked_trips_with_ids, linked_trips_with_joints, joint_trips


def _tours_from(persons, households, unlinked_trips) -> pl.DataFrame:
    """Run the whole link -> joint trips -> tours path and return the tours."""
    (
        unlinked_trips_with_ids,
        linked_trips,
        joint_trips,
    ) = link_and_detect_joint_trips(unlinked_trips, households)

    result = locate_and_extract_tours(
        persons=persons,
        households=households,
        unlinked_trips=unlinked_trips_with_ids,
        linked_trips=linked_trips,
        joint_trips=joint_trips,
    )
    return result["tours"]


def _out_and_back(n_people: int, stagger_hours: int = 0) -> pl.DataFrame:
    """``n_people`` household members each go home -> shop -> home.

    With ``stagger_hours`` at zero every member's two trips share their
    departure time, arrival time and both endpoints, so the buffer detector
    sees one group on each leg. Staggering pushes each later member clear of
    the one before, which leaves nobody travelling together.
    """
    n = n_people
    departures = [datetime(2024, 1, 15, 10 + stagger_hours * person, 0, 0) for person in range(n)]
    return pl.DataFrame(
        {
            "unlinked_trip_id": list(range(1, 2 * n + 1)),
            "day_id": [day for day in DAY_IDS[:n] for _ in range(2)],
            "person_id": [person for person in PERSON_IDS[:n] for _ in range(2)],
            "hh_id": [HH_ID] * (2 * n),
            "travel_dow": [TravelDow.SATURDAY.value] * (2 * n),
            "depart_time": [
                time
                for depart in departures
                for time in (depart, depart.replace(hour=depart.hour + 1))
            ],
            "arrive_time": [
                time
                for depart in departures
                for time in (
                    depart.replace(minute=15),
                    depart.replace(hour=depart.hour + 1, minute=15),
                )
            ],
            "o_purpose_category": [
                PurposeCategory.HOME.value,
                PurposeCategory.SHOP.value,
            ]
            * n,
            "d_purpose_category": [
                PurposeCategory.SHOP.value,
                PurposeCategory.HOME.value,
            ]
            * n,
            "o_purpose": [Purpose.HOME.value, Purpose.GROCERY.value] * n,
            "d_purpose": [Purpose.GROCERY.value, Purpose.HOME.value] * n,
            "mode_type": [ModeType.CAR.value] * (2 * n),
            "o_lat": [HOME[0], SHOP[0]] * n,
            "o_lon": [HOME[1], SHOP[1]] * n,
            "d_lat": [SHOP[0], HOME[0]] * n,
            "d_lon": [SHOP[1], HOME[1]] * n,
            "unlinked_trip_weight": [1.0] * (2 * n),
            "distance_meters": [2000.0] * (2 * n),
            "duration_minutes": [15.0] * (2 * n),
            "num_travelers": [n] * (2 * n),
            "driver": [Driver.DRIVER.value] * (2 * n),
        }
    )


class TestFullyJointTour:
    """Test tours where all trips involve the same group of people."""

    @pytest.mark.parametrize(
        "n_people", [pytest.param(2, id="two_people"), pytest.param(3, id="three_people")]
    )
    def test_a_whole_tour_taken_together_is_one_joint_tour(self, basic_household_data, n_people):
        """Each traveller keeps their own tour; the group shares one joint id."""
        persons, households = basic_household_data

        tours = _tours_from(persons, households, _out_and_back(n_people))

        assert len(tours) == n_people, "One tour per person"

        joint_tours = tours.filter(pl.col("joint_tour_id").is_not_null())
        assert len(joint_tours) == n_people, "Every tour should be marked joint"
        assert joint_tours["joint_tour_id"].n_unique() == 1, "All of them share one id"

        # joint_tour_id is the household id plus a two-digit sequence
        assert int(joint_tours["joint_tour_id"][0]) // 100 == HH_ID


class TestPartialDropoff:
    """Test scenarios where some participants drop off mid-tour."""

    def test_three_start_one_drops_off(self, basic_household_data):
        """Three people start, one drops off.

        The stable group is the people on *every* joint trip of the tour, so
        the two parents who carry on to the shop and home are joint and the
        child who stays at school is not.
        """
        persons, households = basic_household_data

        # All 3: home -> school (drop off child)
        # Parents only: school -> shop -> home
        unlinked_trips = pl.DataFrame(
            {
                "unlinked_trip_id": list(range(1, 10)),
                "day_id": [DAY_IDS[0]] * 3 + [DAY_IDS[1]] * 3 + [DAY_IDS[2]] * 3,
                "person_id": [P1] * 3 + [P2] * 3 + [P3] * 3,
                "hh_id": [HH_ID] * 9,
                "travel_dow": [TravelDow.MONDAY.value] * 9,
                "depart_time": [
                    # Person 1: home -> school -> shop -> home
                    datetime(2024, 1, 15, 8, 0, 0),
                    datetime(2024, 1, 15, 8, 30, 0),
                    datetime(2024, 1, 15, 9, 30, 0),
                    # Person 2: home -> school -> shop -> home
                    datetime(2024, 1, 15, 8, 0, 0),
                    datetime(2024, 1, 15, 8, 30, 0),
                    datetime(2024, 1, 15, 9, 30, 0),
                    # Person 3 (child): home -> school (stays)
                    datetime(2024, 1, 15, 8, 0, 0),
                    datetime(2024, 1, 15, 16, 0, 0),  # School day
                    datetime(2024, 1, 15, 16, 15, 0),
                ],
                "arrive_time": [
                    datetime(2024, 1, 15, 8, 15, 0),
                    datetime(2024, 1, 15, 9, 0, 0),
                    datetime(2024, 1, 15, 10, 0, 0),
                    datetime(2024, 1, 15, 8, 15, 0),
                    datetime(2024, 1, 15, 9, 0, 0),
                    datetime(2024, 1, 15, 10, 0, 0),
                    datetime(2024, 1, 15, 8, 15, 0),
                    datetime(2024, 1, 15, 16, 15, 0),
                    datetime(2024, 1, 15, 16, 30, 0),
                ],
                "o_purpose_category": [
                    PurposeCategory.HOME.value,
                    PurposeCategory.SCHOOL.value,
                    PurposeCategory.SHOP.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.SCHOOL.value,
                    PurposeCategory.SHOP.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.SCHOOL.value,
                    PurposeCategory.SCHOOL.value,
                ],
                "d_purpose_category": [
                    PurposeCategory.SCHOOL.value,
                    PurposeCategory.SHOP.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.SCHOOL.value,
                    PurposeCategory.SHOP.value,
                    PurposeCategory.HOME.value,
                    PurposeCategory.SCHOOL.value,
                    PurposeCategory.SCHOOL.value,
                    PurposeCategory.HOME.value,
                ],
                "o_purpose": [
                    Purpose.HOME.value,
                    Purpose.K12_SCHOOL.value,
                    Purpose.GROCERY.value,
                    Purpose.HOME.value,
                    Purpose.K12_SCHOOL.value,
                    Purpose.GROCERY.value,
                    Purpose.HOME.value,
                    Purpose.K12_SCHOOL.value,
                    Purpose.K12_SCHOOL.value,
                ],
                "d_purpose": [
                    Purpose.K12_SCHOOL.value,
                    Purpose.GROCERY.value,
                    Purpose.HOME.value,
                    Purpose.K12_SCHOOL.value,
                    Purpose.GROCERY.value,
                    Purpose.HOME.value,
                    Purpose.K12_SCHOOL.value,
                    Purpose.K12_SCHOOL.value,
                    Purpose.HOME.value,
                ],
                "mode_type": [ModeType.CAR.value] * 9,
                "o_lat": [37.8, 37.81, 37.82] * 3,
                "o_lon": [-122.4, -122.43, -122.42] * 3,
                "d_lat": [37.81, 37.82, 37.8] * 3,
                "d_lon": [-122.43, -122.42, -122.4] * 3,
                "unlinked_trip_weight": [1.0] * 9,
                "distance_meters": [2000.0] * 9,
                "duration_minutes": [15.0] * 9,
                "num_travelers": [
                    3,
                    2,
                    2,
                    3,
                    2,
                    2,
                    3,
                    1,
                    1,
                ],  # Child drops off after first trip
                "driver": [Driver.DRIVER.value] * 9,
            }
        )

        tours = _tours_from(persons, households, unlinked_trips)

        parent_tours = tours.filter(pl.col("person_id").is_in([P1, P2]))
        child_tours = tours.filter(pl.col("person_id") == P3)

        parent_joint = parent_tours.filter(pl.col("joint_tour_id").is_not_null())
        assert len(parent_joint) == 2, "Both parents should have joint tours"
        assert parent_joint["joint_tour_id"].n_unique() == 1, "Parents share one joint_tour_id"

        # The child left the group at school, so no tour of theirs is joint
        assert not child_tours.is_empty(), "The child still has a tour of their own"
        assert child_tours["joint_tour_id"].null_count() == len(child_tours)


def _build_joint_tour_frames(
    trips: list[tuple[int, int, int, int | None]],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Build minimal linked_trips/tours frames for identify_joint_tours.

    Args:
        trips: (linked_trip_id, person_id, tour_id, joint_trip_id) rows.

    Returns:
        Tuple of (linked_trips, tours).
    """
    linked_trips = pl.DataFrame(
        {
            "linked_trip_id": [t[0] for t in trips],
            "person_id": [t[1] for t in trips],
            "tour_id": [t[2] for t in trips],
            "joint_trip_id": [t[3] for t in trips],
            "hh_id": [HH_ID] * len(trips),
        },
        schema_overrides={"joint_trip_id": pl.Int64},
    )
    tours = pl.DataFrame({"tour_id": sorted({t[2] for t in trips})})
    return linked_trips, tours


class TestJointTourOccasions:
    """Test that joint_tour_id distinguishes occasions, not participant groups."""

    def test_same_pair_two_occasions_get_different_ids(self):
        """The same pair travelling twice should get one joint_tour_id per occasion.

        joint_tour_id is keyed on the set of joint_trip_ids (the occasion), so the
        same stable group on a second outing is a distinct joint tour rather than a
        continuation of the first.
        """
        # Occasion 1 = joint trips 1001/1002; occasion 2 = joint trips 1003/1004.
        linked_trips, tours = _build_joint_tour_frames(
            [
                (1, P1, 101, 1001),
                (2, P1, 101, 1002),
                (3, P2, 201, 1001),
                (4, P2, 201, 1002),
                (5, P1, 102, 1003),
                (6, P1, 102, 1004),
                (7, P2, 202, 1003),
                (8, P2, 202, 1004),
            ]
        )

        _, tours_out = identify_joint_tours(linked_trips, tours)

        ids = dict(zip(tours_out["tour_id"], tours_out["joint_tour_id"], strict=True))
        assert all(v is not None for v in ids.values()), "All four tours should be joint"

        # Partners on the same occasion share an id...
        assert ids[101] == ids[201], "Occasion 1 partners should share a joint_tour_id"
        assert ids[102] == ids[202], "Occasion 2 partners should share a joint_tour_id"

        # ...but the two occasions are distinct joint tours.
        assert ids[101] != ids[102], (
            "The same pair on a second occasion should get a different joint_tour_id"
        )

    def test_singleton_joint_tour_id_is_nulled(self):
        """A joint_tour_id surviving for only one person is reset to null.

        P2's tour has a non-joint trip, so it fails the all-trips-joint filter. That
        leaves P1's tour as the only member of the occasion; a joint tour needs 2+
        participants, so the id is dropped rather than left as a singleton.
        """
        linked_trips, tours = _build_joint_tour_frames(
            [
                (1, P1, 101, 1001),
                (2, P1, 101, 1002),
                (3, P2, 201, 1001),
                (4, P2, 201, 1002),
                (5, P2, 201, None),  # non-joint trip disqualifies P2's tour
            ]
        )

        _, tours_out = identify_joint_tours(linked_trips, tours)

        assert tours_out["joint_tour_id"].null_count() == len(tours_out), (
            "A joint_tour_id held by a single person should be nulled"
        )


class TestNoJointTours:
    """Test cases where no joint tours should be identified."""

    @pytest.mark.parametrize(
        "trips",
        [
            pytest.param(
                [
                    (1, P1, 101, None),
                    (2, P1, 101, None),
                    (3, P2, 201, None),
                    (4, P2, 201, None),
                ],
                id="nobody_shared_a_trip_all_day",
            ),
            pytest.param(
                [
                    (1, P1, 101, 1001),
                    (2, P1, 101, None),
                    (3, P2, 201, 1002),
                    (4, P2, 201, None),
                ],
                id="every_tour_has_a_solo_leg",
            ),
        ],
    )
    def test_no_joint_trips(self, trips):
        """Two people out on their own: nothing for the tours to intersect on."""
        linked_trips, tours = _build_joint_tour_frames(trips)

        _, tours_out = identify_joint_tours(linked_trips, tours)

        assert tours_out["joint_tour_id"].null_count() == len(tours_out)

    def test_the_whole_pipeline_survives_a_day_with_no_joint_trips(self, basic_household_data):
        """Two members shopping five hours apart never travel together.

        Tour extraction has its own branch for the case where joint trip
        detection came back with nothing, so it is worth reaching from the top
        rather than only through ``identify_joint_tours``.
        """
        persons, households = basic_household_data

        tours = _tours_from(persons, households, _out_and_back(2, stagger_hours=5))

        assert len(tours) == 2
        assert tours["joint_tour_id"].null_count() == 2


class TestAJointTourHoldsAJointTrip:
    """``joint_tour_id`` claims shared travel, so some trip must be shared.

    Deliberately weaker than the rule ids are currently assigned under: a tour
    is admitted today only when *every* trip is joint, but a survey can record a
    half-shared tour -- a parent dropping a child, then driving on to work -- and
    widening the id to cover those should not have to revisit this check.
    """

    @staticmethod
    def _frame(joint_trip_ids: list[int | None], joint_tour_ids: list[int | None]) -> pl.DataFrame:
        n = len(joint_trip_ids)
        return pl.DataFrame(
            {
                "linked_trip_id": list(range(1, n + 1)),
                "person_id": [P1] * n,
                "tour_id": [101] * n,
                "joint_trip_id": joint_trip_ids,
                "joint_tour_id": joint_tour_ids,
            },
            schema_overrides={"joint_trip_id": pl.Int64, "joint_tour_id": pl.Int64},
        )

    def test_a_solo_leg_stops_the_tour_being_joint(self):
        """Today's stricter rule: a tour with one non-joint leg is left individual."""
        linked_trips, tours = _build_joint_tour_frames(
            [
                (1, P1, 101, 1001),
                (2, P1, 101, None),  # solo leg
                (3, P2, 201, 1001),
                (4, P2, 201, 1002),
            ]
        )

        _, tours_out = identify_joint_tours(linked_trips, tours)

        assert tours_out.filter(pl.col("joint_tour_id").is_not_null()).is_empty()

    def test_the_guard_rejects_a_joint_tour_with_no_joint_trip(self):
        """The tripwire, on a frame the public path cannot currently produce."""
        broken = self._frame([None, None], [9001, 9001])

        with pytest.raises(ValueError, match="hold no joint trip"):
            _validate_joint_tours_have_joint_trips(broken)

    @pytest.mark.parametrize(
        ("joint_trip_ids", "joint_tour_ids"),
        [
            pytest.param(
                [1001, None],
                [9001, 9001],
                id="a_partly_shared_tour_is_allowed_through",
            ),
            pytest.param(
                [1001, 1002],
                [9001, 9001],
                id="the_guard_passes_when_every_leg_is_joint",
            ),
            pytest.param([None], [None], id="a_trip_with_no_joint_tour_is_not_policed"),
        ],
    )
    def test_the_guard_admits_the_rest(self, joint_trip_ids, joint_tour_ids):
        """One shared leg is enough for canonical, and a solo trip is not policed.

        CT-RAMP cannot represent a half-shared tour and rejects it in its own
        formatter, but the survey can record one and canonical should not
        forbid it.
        """
        _validate_joint_tours_have_joint_trips(self._frame(joint_trip_ids, joint_tour_ids))
