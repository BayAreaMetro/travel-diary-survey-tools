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
    HouseholdDayNeeds,
    JointTours,
    TourClosesAt,
    UsabilityProfile,
    cascade_complete,
    stamp_usable,
)
from processing.completeness.kept_records import keep_records, select_profile_columns
from processing.joint_trips.aggregation import build_joint_trips_table
from processing.tours.joint_tour_helpers import build_joint_tours_table
from tests.fixtures.schema_utils import add_household_days

STRICT = UsabilityProfile(
    "strict",
    TourClosesAt.PRIMARY_HOME,
    HouseholdDayNeeds.ALL_MEMBERS,
    joint_tours=JointTours.FULLY_JOINT,
)
LOOSE = UsabilityProfile(
    "loose",
    TourClosesAt.PRIMARY_HOME,
    HouseholdDayNeeds.ALL_MEMBERS,
    joint_tours=JointTours.PARTIAL,
)

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
            "person_day_id": people,
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
            "person_day_id": [r[2] for r in trip_rows],
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
        "person_days": pl.DataFrame(
            {
                "person_day_id": people,
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
                "person_day_id": [1] * len(joint_trip_ids),
                "survey_complete": [True] * len(joint_trip_ids),
            }
        ),
        "joint_tours": pl.DataFrame(
            {"joint_tour_id": [100], "person_day_id": [1], "survey_complete": [True]}
        ),
    }


def _stamp(profile: UsabilityProfile, tables: dict) -> dict[str, pl.DataFrame]:
    working: dict[str, pl.DataFrame | None] = dict(tables)
    cascade_complete(add_household_days(working))
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
        [
            PurposeCategory.WORK,
            PurposeCategory.WORK_RELATED,
            PurposeCategory.SCHOOL,
            PurposeCategory.ESCORT,
        ],
        ids=lambda p: p.name,
    )
    def test_work_school_and_escort_are_never_joint(self, purpose):
        """Person 2's tour is one a model never draws jointly, so they are not on it.

        Work-related counts as work: a model treats a meeting or jobsite as work.
        """
        tables = _household({1: [500], 2: [500], 3: [500]}, {1: SHOP, 2: purpose.value, 3: SHOP})
        out = _stamp(STRICT, tables)
        assert _on_tour(out, STRICT) == {1: True, 2: False, 3: True}

    def test_mixed_purposes_dissolve_the_tour(self):
        """One model purpose per joint tour; two purposes is two tours."""
        tables = _household({1: [500], 2: [500]}, {1: SHOP, 2: PurposeCategory.MEAL.value})
        out = _stamp(STRICT, tables)
        assert _on_tour(out, STRICT) == {1: False, 2: False}
        assert out["joint_tours"][STRICT.flag].to_list() == [False]

    def test_an_unreported_traveller_is_pruned_not_the_tour(self):
        """Persons 1 and 2 share both legs; person 3 joins them for the second.

        Person 3's trip on that leg went unreported, so joint trip 501 is
        incomplete as a whole. Person 3 is pruned from it; persons 1 and 2 keep
        their joint tour and both its joint trips, so no leg of it is left
        without one.
        """
        tables = _household({1: [500, 501], 2: [500, 501], 3: [501]})
        tables["linked_trips"] = tables["linked_trips"].with_columns(
            (pl.col("tour_id") != 30).alias("survey_complete")
        )
        tables["joint_trips"] = tables["joint_trips"].with_columns(
            (pl.col("joint_trip_id") != 501).alias("survey_complete")
        )
        out = _stamp(STRICT, tables)
        assert _on_tour(out, STRICT) == {1: True, 2: True, 3: False}
        assert out["joint_tours"][STRICT.flag].to_list() == [True]
        assert out["joint_trips"].sort("joint_trip_id")[STRICT.flag].to_list() == [True, True]
        trips = out["linked_trips"]
        stray = trips.filter(
            pl.col(STRICT.joint_tour_id).is_not_null() & pl.col(STRICT.joint_trip_id).is_null()
        )
        assert stray.is_empty()

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
        out = _stamp(UsabilityProfile("loose_days", TourClosesAt.PRIMARY_HOME, "nothing"), tables)
        on = _on_tour(out, UsabilityProfile("loose_days", TourClosesAt.PRIMARY_HOME, "nothing"))
        assert on == {1: True, 2: True, 3: False}


class TestTheColumnIsPerProfile:
    """One survey group, two answers, side by side."""

    def test_two_profiles_disagree_on_the_same_table(self):
        """Strict drops person 3; partial keeps them; the id itself never moves."""
        tables = _household({1: [500, 501], 2: [500, 501], 3: [500, None]})
        working: dict[str, pl.DataFrame | None] = dict(tables)
        cascade_complete(add_household_days(working))
        stamp_usable(working, STRICT)
        stamp_usable(working, LOOSE)
        tours = working["tours"]
        assert tours is not None
        assert tours[STRICT.joint_tour_id].to_list() == [100, 100, None]
        assert tours[LOOSE.joint_tour_id].to_list() == [100, 100, 100]
        # The survey's own record of the group is untouched.
        assert tours["joint_tour_id"].to_list() == [100, 100, 100]


class TestAGroupOfOneDissolves:
    """Whatever the profile, an id with one record left on it is nulled."""

    def test_partial_nulls_the_id_when_the_other_tour_is_unusable(self):
        """Two tours in the survey's group; one fails on its own. The other is alone."""
        tables = _household({1: [500], 2: [500]})
        tables["tours"] = tables["tours"].with_columns(
            pl.when(pl.col("person_id") == 2)
            .then(pl.lit(TourDataQuality.SPATIAL_GAP.value))
            .otherwise(pl.col("tour_data_quality"))
            .alias("tour_data_quality")
        )
        loose = UsabilityProfile("loose_days", TourClosesAt.PRIMARY_HOME, "nothing")
        out = _stamp(loose, tables)
        assert out["tours"][loose.joint_tour_id].to_list() == [None, None]
        assert out["linked_trips"][loose.joint_trip_id].to_list() == [None, None]
        assert out["joint_tours"][loose.flag].to_list() == [False]
        assert out["joint_trips"][loose.flag].to_list() == [False]


class TestAJointRecordBelongsToItsHousehold:
    """A grouping inherits its household's verdict, not one traveller's day."""

    def test_a_third_person_with_an_unusable_day_does_not_break_the_other_two(self):
        """Persons 2 and 3 share all three legs; person 1 joins legs 2 and 3 only.

        Person 1's tour fails quality, so their day (complete, but with no
        usable tour) is unusable, and the ``joint_trips`` rows for legs 2 and 3
        happen to name that day. The household qualifies on another date, so it
        stays usable. Persons 2 and 3 must keep every leg: the joint trip is
        still two usable travellers.
        """
        tables = _household({1: [None, 500, 501], 2: [600, 500, 501], 3: [600, 500, 501]})
        tables["tours"] = tables["tours"].with_columns(
            pl.when(pl.col("person_id") == 1)
            .then(pl.lit(TourDataQuality.SPATIAL_GAP.value))
            .otherwise(pl.col("tour_data_quality"))
            .alias("tour_data_quality")
        )
        tables["person_days"] = pl.concat(
            [
                tables["person_days"],
                pl.DataFrame(
                    {
                        "person_day_id": [11, 12, 13],
                        "person_id": [1, 2, 3],
                        "hh_id": [1, 1, 1],
                        "travel_date": [datetime(2023, 5, 2)] * 3,
                        "survey_complete": [True] * 3,
                    }
                ),
            ]
        )
        assert tables["joint_trips"]["person_day_id"].to_list() == [1, 1, 1]

        out = _stamp(STRICT, tables)

        assert out["households"][STRICT.flag].to_list() == [True]
        assert _on_tour(out, STRICT) == {1: False, 2: True, 3: True}
        trips = out["linked_trips"].sort("linked_trip_id")
        by_tour = trips.group_by("tour_id", maintain_order=True).agg(pl.col(STRICT.joint_trip_id))
        legs = dict(
            zip(by_tour["tour_id"].to_list(), by_tour[STRICT.joint_trip_id].to_list(), strict=True)
        )
        assert legs == {10: [None, None, None], 20: [600, 500, 501], 30: [600, 500, 501]}
        assert out["joint_trips"].sort("joint_trip_id")[STRICT.flag].to_list() == [True, True, True]
        assert out["joint_tours"][STRICT.flag].to_list() == [True]


def _three_person_household(travel_date: object, next_date: object) -> dict[str, pl.DataFrame]:
    """The real case, household 23314479, with joint tables built by the pipeline.

    Persons 2 and 3 share all three legs of a shop tour; person 1 joins legs 2
    and 3 only, on a tour that fails quality, so person 1's day is complete but
    unusable. All three members report a second date with no travel, so the
    household stays usable. Person 1's rows come first, so a rule that files a
    joint record under its first member's day files it under the unusable one.
    """
    people = [1, 2, 3]
    legs = {1: [None, 500, 501], 2: [600, 500, 501], 3: [600, 500, 501]}
    tours = pl.DataFrame(
        {
            "tour_id": [p * 10 for p in people],
            "person_day_id": people,
            "person_id": people,
            "hh_id": [1] * 3,
            "joint_tour_id": [None, 100, 100],
            "tour_purpose": [SHOP] * 3,
            "survey_complete": [True] * 3,
            "parent_tour_id": [p * 10 for p in people],
            "tour_data_quality": [
                TourDataQuality.SPATIAL_GAP.value,
                TourDataQuality.VALID.value,
                TourDataQuality.VALID.value,
            ],
            "tour_category": [TourCategory.COMPLETE.value] * 3,
        },
        schema_overrides={"joint_tour_id": pl.Int64},
    )
    rows = [(p * 100 + i, p * 10, p, joint) for p in people for i, joint in enumerate(legs[p])]
    linked_trips = pl.DataFrame(
        {
            "linked_trip_id": [r[0] for r in rows],
            "tour_id": [r[1] for r in rows],
            "person_id": [r[2] for r in rows],
            "person_day_id": [r[2] for r in rows],
            "hh_id": [1] * len(rows),
            "joint_trip_id": [r[3] for r in rows],
            "joint_tour_id": [None if r[2] == 1 else 100 for r in rows],
            "survey_complete": [True] * len(rows),
        },
        schema_overrides={"joint_trip_id": pl.Int64, "joint_tour_id": pl.Int64},
    )
    person_days = pl.DataFrame(
        {
            "person_day_id": [1, 2, 3, 11, 12, 13],
            "person_id": [1, 2, 3, 1, 2, 3],
            "hh_id": [1] * 6,
            "hh_day_id": [101] * 3 + [102] * 3,
            "travel_date": [travel_date] * 3 + [next_date] * 3,
            "survey_complete": [True] * 6,
        }
    )
    assignments = linked_trips.select("linked_trip_id", "joint_trip_id")
    return {
        "households": pl.DataFrame({"hh_id": [1], "survey_complete": [True]}),
        "persons": pl.DataFrame(
            {
                "person_id": people,
                "hh_id": [1] * 3,
                "survey_complete": [True] * 3,
                "surveyable": [True] * 3,
            }
        ),
        "person_days": person_days,
        "tours": tours,
        "linked_trips": linked_trips,
        "joint_trips": build_joint_trips_table(linked_trips, assignments, person_days),
        "joint_tours": build_joint_tours_table(tours, person_days),
    }


@pytest.mark.parametrize(
    ("travel_date", "next_date"),
    [
        pytest.param(datetime(2023, 12, 10), datetime(2023, 12, 11), id="date"),
        # The real days table carries travel_date as a string.
        pytest.param("2023-12-10", "2023-12-11", id="string"),
    ],
)
class TestTheGateKeepsWhatTheCascadeAdmits:
    """Cascade and gate must agree: no joint record is kept or dropped by a different rule.

    This is the shape that failed on real data: the gate refused joint trips the
    cascade had admitted, because they pointed at one member's unusable day.
    """

    @pytest.mark.parametrize("profile", [STRICT, LOOSE], ids=lambda p: p.name)
    def test_the_gate_passes(self, travel_date, next_date, profile):
        """keep_records raises on any disagreement, so not raising is the assertion."""
        out = _stamp(profile, _three_person_household(travel_date, next_date))
        resolved = select_profile_columns(
            out, profile.name, only=("joint_tour_id", "joint_trip_id")
        )
        keep_records(resolved, profile.flag)

    def test_the_two_who_shared_every_leg_keep_their_joint_travel(self, travel_date, next_date):
        """Persons 2 and 3 keep all three joint trips and their joint tour; person 1 is out."""
        out = _stamp(STRICT, _three_person_household(travel_date, next_date))
        resolved = select_profile_columns(out, STRICT.name, only=("joint_tour_id", "joint_trip_id"))
        kept, _ = keep_records(resolved, STRICT.flag)

        assert sorted(kept["joint_trips"]["joint_trip_id"].to_list()) == [500, 501, 600]
        assert kept["joint_tours"]["joint_tour_id"].to_list() == [100]
        assert sorted(kept["tours"].filter(pl.col("joint_tour_id") == 100)["person_id"]) == [2, 3]
        assert kept["tours"].filter(pl.col("person_id") == 1).is_empty()
