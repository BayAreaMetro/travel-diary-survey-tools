"""Which tours are on a joint tour, per profile.

``joint_tour_id`` records the group the survey detected. Whether a tour is *on*
it is a profile's call: a model draws one set of trips for everyone on a joint
tour, so a person who leaves for a leg is not on the same tour and travels
alone. ``joint_tour_id_<profile>`` records that answer beside the bare id: the
same id where the tour is on it, null where it is not.
"""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.tours import TourCategory, TourDataQuality
from data_canon.codebook.trips import PurposeCategory
from processing.completeness import (
    ALL_MEMBERS,
    FULLY_JOINT,
    PARTIAL,
    PRIMARY_HOME,
    UsabilityProfile,
    cascade_complete,
    stamp_usable,
)

STRICT = UsabilityProfile("strict", PRIMARY_HOME, ALL_MEMBERS, joint_tours=FULLY_JOINT)
LOOSE = UsabilityProfile("loose", PRIMARY_HOME, ALL_MEMBERS, joint_tours=PARTIAL)

SHOP = PurposeCategory.SHOP.value


def _household(
    legs: dict[int, list[int | None]],
    purposes: dict[int, int] | None = None,
) -> dict[str, pl.DataFrame]:
    """One joint tour (id 100) with one member tour per person.

    *legs* gives each person's trips as the joint trip each leg is part of, or
    None for a leg travelled alone. Every record is complete and every tour
    valid, so only the joint rule can move anything.
    """
    people = sorted(legs)
    purposes = purposes or dict.fromkeys(people, SHOP)
    tours = pl.DataFrame(
        {
            "tour_id": [p * 10 for p in people],
            "day_id": people,
            "person_id": people,
            "hh_id": [1] * len(people),
            "joint_tour_id": [100] * len(people),
            "tour_purpose": [purposes[p] for p in people],
            "survey_complete": [True] * len(people),
            "parent_tour_id": [p * 10 for p in people],
            "tour_data_quality": [TourDataQuality.VALID.value] * len(people),
            "tour_category": [TourCategory.COMPLETE.value] * len(people),
        }
    )
    trip_rows = [(p * 100 + i, p * 10, p, joint) for p in people for i, joint in enumerate(legs[p])]
    linked_trips = pl.DataFrame(
        {
            "linked_trip_id": [r[0] for r in trip_rows],
            "tour_id": [r[1] for r in trip_rows],
            "day_id": [r[2] for r in trip_rows],
            "joint_trip_id": [r[3] for r in trip_rows],
            "survey_complete": [True] * len(trip_rows),
        },
        schema_overrides={"joint_trip_id": pl.Int64},
    )
    joint_trip_ids = sorted({r[3] for r in trip_rows if r[3] is not None})
    return {
        "households": pl.DataFrame({"hh_id": [1], "survey_complete": [True]}),
        "persons": pl.DataFrame(
            {
                "person_id": people,
                "hh_id": [1] * len(people),
                "survey_complete": [True] * len(people),
                "surveyable": [True] * len(people),
            }
        ),
        "days": pl.DataFrame(
            {
                "day_id": people,
                "person_id": people,
                "hh_id": [1] * len(people),
                "travel_date": [datetime(2023, 5, 1)] * len(people),
                "survey_complete": [True] * len(people),
            }
        ),
        "tours": tours,
        "linked_trips": linked_trips,
        "joint_trips": pl.DataFrame(
            {
                "joint_trip_id": joint_trip_ids,
                "day_id": [1] * len(joint_trip_ids),
                "survey_complete": [True] * len(joint_trip_ids),
            }
        ),
        "joint_tours": pl.DataFrame(
            {"joint_tour_id": [100], "day_id": [1], "survey_complete": [True]}
        ),
    }


def _stamp(profile: UsabilityProfile, tables: dict) -> dict[str, pl.DataFrame]:
    working: dict[str, pl.DataFrame | None] = dict(tables)
    cascade_complete(working)
    stamp_usable(working, profile)
    return {name: df for name, df in working.items() if df is not None}


def _on_tour(out: dict[str, pl.DataFrame], profile: UsabilityProfile) -> dict[int, bool]:
    """Per person, whether their tour is on the joint tour under *profile*."""
    tours = out["tours"]
    on = tours[profile.joint_tour_id].is_not_null().to_list()
    return dict(zip(tours["person_id"].to_list(), on, strict=True))


class TestFullyJoint:
    """Everyone on the tour shares every leg; anyone who does not travels alone."""

    def test_one_member_who_leaves_for_a_leg_travels_alone(self):
        """Three people out together; person 3 goes their own way for the last leg.

        Persons 1 and 2 are on the same tour. Person 3 is not, and the joint
        tour is the two of them.
        """
        tables = _household({1: [500, 501], 2: [500, 501], 3: [500, None]})
        out = _stamp(STRICT, tables)
        assert _on_tour(out, STRICT) == {1: True, 2: True, 3: False}
        assert out["joint_tours"][STRICT.flag].to_list() == [True]
        # Person 3's tour is still a usable tour, just an individual one.
        assert out["tours"][STRICT.flag].to_list() == [True, True, True]

    def test_a_leg_shared_with_someone_who_left_is_not_shared(self):
        """Person 1's second leg is joint only with person 3, who left.

        Person 1 then has a leg nobody remaining shares, so leaves too, and the
        group of one dissolves: everyone travels alone.
        """
        tables = _household({1: [500, 502], 2: [500, None], 3: [500, 502, None]})
        out = _stamp(STRICT, tables)
        assert _on_tour(out, STRICT) == {1: False, 2: False, 3: False}
        assert out["joint_tours"][STRICT.flag].to_list() == [False]

    @pytest.mark.parametrize(
        "purpose",
        [PurposeCategory.WORK, PurposeCategory.SCHOOL, PurposeCategory.ESCORT],
        ids=lambda p: p.name,
    )
    def test_work_school_and_escort_are_never_joint(self, purpose):
        """Person 2's tour is one a model never draws jointly, so they are not on it."""
        tables = _household({1: [500], 2: [500], 3: [500]}, {1: SHOP, 2: purpose.value, 3: SHOP})
        out = _stamp(STRICT, tables)
        assert _on_tour(out, STRICT) == {1: True, 2: False, 3: True}

    def test_mixed_purposes_dissolve_the_tour(self):
        """One model purpose per joint tour; two purposes is two tours."""
        tables = _household({1: [500], 2: [500]}, {1: SHOP, 2: PurposeCategory.MEAL.value})
        out = _stamp(STRICT, tables)
        assert _on_tour(out, STRICT) == {1: False, 2: False}
        assert out["joint_tours"][STRICT.flag].to_list() == [False]

    def test_linked_trips_follow_their_tour(self):
        """A trip is in its joint trip only while its tour is in its joint tour."""
        tables = _household({1: [500, 501], 2: [500, 501], 3: [500, None]})
        out = _stamp(STRICT, tables)
        trips = out["linked_trips"].sort("linked_trip_id")
        assert trips[STRICT.joint_trip_id].to_list() == [500, 501, 500, 501, None, None]
        assert trips[STRICT.joint_tour_id].to_list() == [100, 100, 100, 100, None, None]
        # Joint trip 500 keeps two members; its third left with their tour.
        assert out["joint_trips"].sort("joint_trip_id")[STRICT.flag].to_list() == [True, True]


class TestPartial:
    """The survey's groups, as detected."""

    def test_a_person_who_leaves_for_a_leg_stays_on_the_tour(self):
        """The survey said they were together, and this profile takes its word."""
        tables = _household({1: [500, 501], 2: [500, 501], 3: [500, None]})
        out = _stamp(LOOSE, tables)
        assert _on_tour(out, LOOSE) == {1: True, 2: True, 3: True}

    def test_an_unusable_tour_is_still_not_on_it(self):
        """Partial relaxes the leg rule, not usability."""
        tables = _household({1: [500], 2: [500], 3: [500]})
        tables["tours"] = tables["tours"].with_columns(
            pl.when(pl.col("person_id") == 3)
            .then(pl.lit(TourDataQuality.SPATIAL_GAP.value))
            .otherwise(pl.col("tour_data_quality"))
            .alias("tour_data_quality")
        )
        out = _stamp(UsabilityProfile("loose_days", PRIMARY_HOME, "nothing"), tables)
        on = _on_tour(out, UsabilityProfile("loose_days", PRIMARY_HOME, "nothing"))
        assert on == {1: True, 2: True, 3: False}


class TestTheColumnIsPerProfile:
    """One survey group, two answers, side by side."""

    def test_two_profiles_disagree_on_the_same_table(self):
        """Strict drops person 3; partial keeps them; the id itself never moves."""
        tables = _household({1: [500, 501], 2: [500, 501], 3: [500, None]})
        working: dict[str, pl.DataFrame | None] = dict(tables)
        cascade_complete(working)
        stamp_usable(working, STRICT)
        stamp_usable(working, LOOSE)
        tours = working["tours"]
        assert tours is not None
        assert tours[STRICT.joint_tour_id].to_list() == [100, 100, None]
        assert tours[LOOSE.joint_tour_id].to_list() == [100, 100, 100]
        # The survey's own record of the group is untouched.
        assert tours["joint_tour_id"].to_list() == [100, 100, 100]
