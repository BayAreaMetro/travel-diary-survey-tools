"""The weight hierarchy: the walk, the gate, and the post-run checks.

The walk carries and aggregates weights down the levels, the gate decides who
enters it, and the checks assert the identities the walk was supposed to
maintain. All three read the same records and the same usability flag, so they are tested
against the same hand-built frames.
"""

import ast
import pathlib

import polars as pl
import pytest

from processing.weighting.core.hierarchy import HIERARCHY
from processing.weighting.core.propagation import (
    collect_tables,
    is_usable,
    non_null_tables,
    propagate_weights,
    safe_join_weight,
    seed_admits,
)
from processing.weighting.core.specs import ControlTotals
from processing.weighting.validation.weight_checks import (
    _check_hierarchy,
    _check_joint_sums,
    weight_sanity_checks,
)

# ---------------------------------------------------------------------------
# collect_tables / non_null_tables
# ---------------------------------------------------------------------------


def test_collect_tables_fills_the_rest_with_none():
    """Every level gets a key; the ones not supplied are None, and filterable."""
    assert all(v is None for v in collect_tables().values())

    hh = pl.DataFrame({"hh_id": [1]})
    tables = collect_tables(households=hh)
    assert len(tables) == len(HIERARCHY)
    assert tables["households"] is hh
    assert tables["persons"] is None
    assert list(non_null_tables(tables).keys()) == ["households"]


# ---------------------------------------------------------------------------
# safe_join_weight
# ---------------------------------------------------------------------------


def test_safe_join_replaces_any_existing_weight():
    """The target's own weight column is dropped first, and rows w lacks go null."""
    df = pl.DataFrame({"hh_id": [1, 2, 3], "hh_weight": [0.0, 0.0, 0.0]})
    w = pl.DataFrame({"hh_id": [1, 2], "hh_weight": [1.5, 2.0]})
    result = safe_join_weight(df, w, "hh_id")
    assert result["hh_weight"].to_list() == [1.5, 2.0, None]


# ---------------------------------------------------------------------------
# propagate_weights -- carry-forward
# ---------------------------------------------------------------------------


def _make_tables():
    """Build a minimal set of canonical tables for propagation tests."""
    households = pl.DataFrame({"hh_id": [1, 2], "hh_weight": [10.0, 20.0]})
    persons = pl.DataFrame({"person_id": [1, 2, 3], "hh_id": [1, 1, 2]})
    days = pl.DataFrame({"day_id": [10, 20, 30], "person_id": [1, 2, 3]})
    unlinked_trips = pl.DataFrame(
        {
            "unlinked_trip_id": [100, 200, 300, 400],
            "day_id": [10, 10, 20, 30],
            "linked_trip_id": [1, 1, 2, 2],
        }
    )
    linked_trips = pl.DataFrame(
        {
            "linked_trip_id": [1, 2],
            "tour_id": [1, 1],
        }
    )
    tours = pl.DataFrame({"tour_id": [1]})
    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "joint_trips": None,
        "tours": tours,
    }


class TestPropagateCarryForward:
    """Tests for the carry-forward (parent -> child) branch."""

    def test_full_hierarchy(self):
        """Weights propagate from households all the way to unlinked_trips."""
        tables = _make_tables()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="usable")

        # persons get hh_weight via hh_id
        assert "person_weight" in tables["persons"].columns
        assert tables["persons"].sort("person_id")["person_weight"].to_list() == [
            10.0,
            10.0,
            20.0,
        ]

        # days get person_weight via person_id
        assert "day_weight" in tables["days"].columns
        assert tables["days"].sort("day_id")["day_weight"].to_list() == [
            10.0,
            10.0,
            20.0,
        ]

        # unlinked_trips get day_weight via day_id
        assert "unlinked_trip_weight" in tables["unlinked_trips"].columns
        assert tables["unlinked_trips"].sort("unlinked_trip_id")[
            "unlinked_trip_weight"
        ].to_list() == [10.0, 10.0, 10.0, 20.0]

        # has_weight tracks all (carry-forward + aggregation)
        assert has_weight == {
            "households": "hh_weight",
            "persons": "person_weight",
            "days": "day_weight",
            "unlinked_trips": "unlinked_trip_weight",
            "linked_trips": "linked_trip_weight",
            "tours": "tour_weight",
        }

    @pytest.mark.parametrize(
        ("mutate", "has_weight", "match"),
        [
            pytest.param(
                lambda t: t,
                {},
                "has no weight column",
                id="parent_has_no_weight",
            ),
            pytest.param(
                lambda t: t | {"households": None},
                {"households": "hh_weight"},
                "parent table households is None",
                id="parent_frame_is_none",
            ),
            pytest.param(
                lambda t: t | {"persons": pl.DataFrame({"person_id": [1], "age": [25]})},
                {"households": "hh_weight"},
                "missing join key hh_id",
                id="child_missing_join_key",
            ),
        ],
    )
    def test_carry_forward_gaps_raise(self, mutate, has_weight, match):
        """Each way the carry-forward edge can be broken names itself."""
        tables = mutate(_make_tables())
        with pytest.raises(ValueError, match=match):
            propagate_weights(tables, dict(has_weight), usability_flag_col="usable")

    def test_skip_prevents_carry_forward(self):
        """Tables in the skip set are not overwritten."""
        tables = _make_tables()
        # Give persons their own weight already
        tables["persons"] = tables["persons"].with_columns(pl.lit(99.0).alias("person_weight"))
        has_weight: dict[str, str] = {"households": "hh_weight"}

        # Skip persons, days, and unlinked_trips so we only test skip on persons
        propagate_weights(
            tables,
            has_weight,
            skip={"persons", "days", "unlinked_trips", "linked_trips", "tours"},
            usability_flag_col="usable",
        )

        # persons weight should stay at 99, not be overwritten
        assert tables["persons"]["person_weight"].to_list() == [99.0, 99.0, 99.0]
        # persons not in has_weight because it was skipped
        assert "persons" not in has_weight

    def test_child_none_is_skipped(self):
        """If a child table is None, propagation skips it."""
        tables = _make_tables()
        tables["persons"] = None
        tables["days"] = None
        tables["unlinked_trips"] = None
        tables["linked_trips"] = None
        tables["tours"] = None
        has_weight: dict[str, str] = {"households": "hh_weight"}

        # Should not raise -- all downstream tables are None
        propagate_weights(tables, has_weight, usability_flag_col="usable")

        assert has_weight == {"households": "hh_weight"}


# ---------------------------------------------------------------------------
# propagate_weights -- aggregation
# ---------------------------------------------------------------------------


class TestPropagateAggregate:
    """Tests for the aggregate (mean weight) branch."""

    def test_aggregate_linked_trips(self):
        """Each aggregate level is the mean of its members, one level at a time."""
        tables = _make_tables()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="usable")

        lt = tables["linked_trips"].sort("linked_trip_id")
        # linked_trip 1 has unlinked_trips 100 (wt=10) and 200 (wt=10) -> mean=10
        # linked_trip 2 has unlinked_trips 300 (wt=10) and 400 (wt=20) -> mean=15
        assert lt["linked_trip_weight"].to_list() == [10.0, 15.0]
        # tour 1 then holds linked_trips 1 (wt=10) and 2 (wt=15) -> mean=12.5
        assert tables["tours"]["tour_weight"].to_list() == [12.5]

    def test_aggregate_excludes_zeros_and_nulls(self):
        """Zeros and nulls are excluded from the mean aggregation."""
        tables = _make_tables()
        # Manually set unlinked trip weights (bypass carry-forward)
        tables["unlinked_trips"] = pl.DataFrame(
            {
                "unlinked_trip_id": [100, 200, 300, 400],
                "day_id": [10, 10, 20, 30],
                "linked_trip_id": [1, 1, 2, 2],
                "unlinked_trip_weight": [5.0, 0.0, None, 3.0],
            }
        )
        has_weight: dict[str, str] = {
            "households": "hh_weight",
            "persons": "person_weight",
            "days": "day_weight",
            "unlinked_trips": "unlinked_trip_weight",
        }

        propagate_weights(
            tables,
            has_weight,
            skip={"persons", "days", "unlinked_trips"},
            usability_flag_col="usable",
        )

        lt = tables["linked_trips"].sort("linked_trip_id")
        # linked_trip 1: mean(5.0) = 5.0  (zero excluded)
        # linked_trip 2: mean(3.0) = 3.0  (null excluded)
        assert lt["linked_trip_weight"].to_list() == [5.0, 3.0]

    @pytest.mark.parametrize(
        ("mutate", "drop_source_weight", "match"),
        [
            pytest.param(
                lambda t: (
                    t
                    | {
                        "unlinked_trips": pl.DataFrame(
                            {
                                "unlinked_trip_id": [100, 200],
                                "day_id": [10, 10],
                                "unlinked_trip_weight": [5.0, 3.0],
                            }
                        )
                    }
                ),
                False,
                "missing linked_trip_id",
                id="source_missing_group_key",
            ),
            pytest.param(
                lambda t: t | {"unlinked_trips": None},
                False,
                "source table unlinked_trips is None",
                id="source_frame_is_none",
            ),
            pytest.param(
                lambda t: t,
                True,
                "source table unlinked_trips has no weight",
                id="source_has_no_weight",
            ),
        ],
    )
    def test_aggregate_gaps_raise(self, mutate, drop_source_weight, match):
        """Each way the aggregate edge can be broken names itself."""
        tables = mutate(_make_tables())
        has_weight: dict[str, str] = {
            "households": "hh_weight",
            "persons": "person_weight",
            "days": "day_weight",
            "unlinked_trips": "unlinked_trip_weight",
        }
        if drop_source_weight:
            del has_weight["unlinked_trips"]

        with pytest.raises(ValueError, match=match):
            propagate_weights(
                tables,
                has_weight,
                skip={"persons", "days", "unlinked_trips"},
                usability_flag_col="usable",
            )


# ---------------------------------------------------------------------------
# propagate_weights -- completion flag
# ---------------------------------------------------------------------------


def _make_tables_with_complete():
    """Build tables where some records are marked incomplete."""
    households = pl.DataFrame(
        {
            "hh_id": [1, 2],
            "hh_weight": [10.0, 20.0],
            "survey_complete": [True, False],
        }
    )
    persons = pl.DataFrame(
        {
            "person_id": [1, 2, 3],
            "hh_id": [1, 1, 2],
            "survey_complete": [True, True, False],
        }
    )
    days = pl.DataFrame(
        {
            "day_id": [10, 20, 30],
            "person_id": [1, 2, 3],
            "hh_id": [1, 1, 2],
            "survey_complete": [True, False, False],
        }
    )
    unlinked_trips = pl.DataFrame(
        {
            "unlinked_trip_id": [100, 200, 300, 400],
            "day_id": [10, 10, 20, 30],
            "linked_trip_id": [1, 1, 2, 2],
            "survey_complete": [True, True, False, False],
        }
    )
    linked_trips = pl.DataFrame(
        {
            "linked_trip_id": [1, 2],
            "tour_id": [1, 1],
        }
    )
    tours = pl.DataFrame({"tour_id": [1]})
    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "joint_trips": None,
        "tours": tours,
    }


def _make_tables_partial_usability():
    """One household whose parents each keep only some of their children.

    Person 1 reports 4 days of which 2 are usable; day 10 holds 4 trips of which
    1 is usable. Every parent keeps at least one child, so the carry-forward
    checksum must hold exactly at every level.
    """
    households = pl.DataFrame({"hh_id": [1], "hh_weight": [10.0], "survey_complete": [True]})
    persons = pl.DataFrame({"person_id": [1], "hh_id": [1], "survey_complete": [True]})
    days = pl.DataFrame(
        {
            "day_id": [10, 20, 30, 40],
            "person_id": [1, 1, 1, 1],
            "hh_id": [1, 1, 1, 1],
            "survey_complete": [True, True, False, False],
        }
    )
    unlinked_trips = pl.DataFrame(
        {
            "unlinked_trip_id": [100, 200, 300, 400],
            "day_id": [10, 10, 10, 10],
            "linked_trip_id": [1, 1, 2, 2],
            "survey_complete": [True, False, False, False],
        }
    )
    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": pl.DataFrame({"linked_trip_id": [1, 2], "tour_id": [1, 1]}),
        "joint_trips": None,
        "tours": pl.DataFrame({"tour_id": [1]}),
    }


class TestPropagateUsableColumn:
    """Tests for spreading each parent's weight across its usable children.

    ``usability_flag_col`` is required of every caller; these pass
    ``survey_complete``, one of the two columns the pipeline uses.
    """

    def test_unusable_records_get_zero_weight(self):
        """A record the flag excludes gets 0 at every level of the walk.

        Person 2's only day is unusable, so their weight goes unrepresented at
        the day level -- it is **not** pooled onto person 1's day.  Person 1's
        single usable day carries exactly person 1's weight (the average-day
        split, ``person_weight / n_usable_days``).
        """
        tables = _make_tables_with_complete()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="survey_complete")

        assert tables["persons"].sort("person_id")["person_weight"].to_list() == [10.0, 10.0, 0.0]

        days = tables["days"].sort("day_id")
        assert days["day_weight"].to_list() == [10.0, 0.0, 0.0]
        # person 1's days sum to person 1's weight; person 2's 10 is a shortfall
        assert days["day_weight"].sum() == pytest.approx(10.0)

        # day 10 carries 10.0 and both its trips are usable, so each keeps 10.0
        ut = tables["unlinked_trips"].sort("unlinked_trip_id")
        assert ut["unlinked_trip_weight"].to_list() == [10.0, 10.0, 0.0, 0.0]

        # and the aggregation upward leaves the all-zero grouping at zero
        lt = tables["linked_trips"].sort("linked_trip_id")
        assert lt["linked_trip_weight"].to_list() == [10.0, 0.0]

    def test_no_usable_column_keeps_carried_weights(self):
        """With usability_flag_col=None, unusable children keep the parent weight."""
        tables = _make_tables_with_complete()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col=None)

        # Person 3 (survey_complete=False, in HH 2 with weight 20) keeps 20 instead of 0
        persons = tables["persons"].sort("person_id")
        assert persons["person_weight"].to_list() == [10.0, 10.0, 20.0]
        days = tables["days"].sort("day_id")
        assert days["day_weight"].to_list() == [10.0, 10.0, 20.0]

    def test_missing_usable_column_propagates_normally(self):
        """Without the usability column present, weights propagate as before."""
        tables = _make_tables()  # no complete/usable column
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="survey_complete")

        persons = tables["persons"].sort("person_id")
        assert persons["person_weight"].to_list() == [10.0, 10.0, 20.0]

    def test_unusable_aggregate_target_carries_no_weight(self):
        """A tour flagged unusable gets 0, even if a member trip has weight."""
        tables = _make_tables_with_complete()
        tables["linked_trips"] = tables["linked_trips"].with_columns(
            pl.lit(value=True).alias("survey_complete")
        )
        tables["tours"] = tables["tours"].with_columns(pl.lit(value=False).alias("survey_complete"))
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="survey_complete")

        assert tables["linked_trips"]["linked_trip_weight"].to_list() == [10.0, 0.0]
        assert tables["tours"]["tour_weight"].to_list() == [0.0]


class TestPropagateRedistribution:
    """Days split their person's weight; trips conserve their day's claim."""

    def test_usable_days_split_the_person_weight(self):
        """A person keeping 2 of 4 days puts half their weight on each survivor.

        The average-day rule: day_weight = person_weight / n_usable_days, so the
        person's usable days sum to exactly their person weight.
        """
        tables = _make_tables_partial_usability()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="survey_complete")

        days = tables["days"].sort("day_id")
        assert days["day_weight"].to_list() == [5.0, 5.0, 0.0, 0.0]

    def test_usable_trips_absorb_the_unusable_ones(self):
        """A day keeping 1 of 4 trips gives that trip four times the day weight."""
        tables = _make_tables_partial_usability()
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="survey_complete")

        ut = tables["unlinked_trips"].sort("unlinked_trip_id")
        # day 10 carries 5.0 and kept 1 of its 4 trips -> 5 * 4/1
        assert ut["unlinked_trip_weight"].to_list() == [20.0, 0.0, 0.0, 0.0]

    @pytest.mark.parametrize(
        ("hh_weight", "usable_days", "expected_day_weights", "expected_per_person"),
        [
            pytest.param(
                10.0,
                [True, True, False, False],
                [5.0, 5.0, 0.0, 0.0],
                [10.0, 0.0],
                id="person_2_kept_no_day",
            ),
            pytest.param(
                100.0,
                [True, True, True, False],
                [50.0, 50.0, 100.0, 0.0],
                [100.0, 100.0],
                id="persons_kept_different_day_counts",
            ),
            pytest.param(
                10.0,
                [False, False, False, False],
                [0.0, 0.0, 0.0, 0.0],
                [0.0, 0.0],
                id="nobody_kept_a_day",
            ),
        ],
    )
    def test_person_with_no_usable_day_is_a_shortfall_not_pooled(
        self, hh_weight, usable_days, expected_day_weights, expected_per_person
    ):
        """Each person's usable days split that person's weight, and nobody else's.

        Two persons in one household, four days between them.  A person who
        reported no usable travel day keeps their person weight (person weights
        stay calibrated to the person controls), but that weight is simply
        unrepresented at the day level -- the other person's days must NOT
        inflate to cover it, or person-day totals would be silently distorted.
        The shortfall is deliberate, and is not rescaled away.
        """
        tables = {
            "households": pl.DataFrame(
                {"hh_id": [1], "hh_weight": [hh_weight], "survey_complete": [True]}
            ),
            "persons": pl.DataFrame(
                {"person_id": [1, 2], "hh_id": [1, 1], "survey_complete": [True, True]}
            ),
            "days": pl.DataFrame(
                {
                    "day_id": [10, 20, 30, 40],
                    "person_id": [1, 1, 2, 2],
                    "hh_id": [1, 1, 1, 1],
                    "survey_complete": usable_days,
                }
            ),
            "unlinked_trips": None,
            "linked_trips": None,
            "joint_trips": None,
            "tours": None,
        }
        has_weight: dict[str, str] = {"households": "hh_weight"}

        propagate_weights(tables, has_weight, usability_flag_col="survey_complete")

        # both persons keep their weight -- each is still a real person
        assert tables["persons"]["person_weight"].to_list() == [hh_weight, hh_weight]

        days = tables["days"].sort("day_id")
        assert days["day_weight"].to_list() == expected_day_weights
        per_person = (
            tables["days"].group_by("person_id").agg(pl.col("day_weight").sum()).sort("person_id")
        )
        assert per_person["day_weight"].to_list() == pytest.approx(expected_per_person)


# ---------------------------------------------------------------------------
# Joint levels: SUM, not mean
# ---------------------------------------------------------------------------


def _make_tables_with_joints():
    """One household, two members sharing a trip and the tour it sits on.

    Both members' days are usable, but member 2's trip is not, so the joint
    grouping has a party of two and only one *represented* member. That gap is
    the whole point: it is what separates the party size from the divisor.
    """
    households = pl.DataFrame({"hh_id": [1], "hh_weight": [10.0], "usable": [True]})
    persons = pl.DataFrame({"person_id": [1, 2], "hh_id": [1, 1], "usable": [True, True]})
    days = pl.DataFrame(
        {
            "day_id": [10, 20],
            "person_id": [1, 2],
            "hh_id": [1, 1],
            "usable": [True, True],
        }
    )
    unlinked_trips = pl.DataFrame(
        {
            "unlinked_trip_id": [100, 200],
            "day_id": [10, 20],
            "linked_trip_id": [1, 2],
            "usable": [True, False],
        }
    )
    linked_trips = pl.DataFrame(
        {
            "linked_trip_id": [1, 2],
            "day_id": [10, 20],
            "tour_id": [1, 2],
            "joint_trip_id": [500, 500],
            "usable": [True, False],
        }
    )
    tours = pl.DataFrame(
        {
            "tour_id": [1, 2],
            "day_id": [10, 20],
            "joint_tour_id": [900, 900],
            "usable": [True, False],
        }
    )
    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "joint_trips": pl.DataFrame({"joint_trip_id": [500], "usable": [True]}),
        "tours": tours,
        "joint_tours": pl.DataFrame({"joint_tour_id": [900], "usable": [True]}),
    }


def _make_tables_with_unequal_joint_members():
    """Two members of one joint grouping carrying *different* weights.

    Person 1 reports two usable days and person 2 only one, so the average-day
    split leaves them on 15 and 30. Equal member weights would let the mean, the
    first and the max all coincide with the sum or half of it; unequal ones
    separate every candidate: sum 45, mean 22.5, first 15, max 30.
    """
    households = pl.DataFrame({"hh_id": [1], "hh_weight": [30.0], "usable": [True]})
    persons = pl.DataFrame({"person_id": [1, 2], "hh_id": [1, 1], "usable": [True, True]})
    days = pl.DataFrame(
        {
            "day_id": [10, 11, 20],
            "person_id": [1, 1, 2],
            "hh_id": [1, 1, 1],
            "usable": [True, True, True],
        }
    )
    unlinked_trips = pl.DataFrame(
        {
            "unlinked_trip_id": [100, 200],
            "day_id": [10, 20],
            "linked_trip_id": [1, 2],
            "usable": [True, True],
        }
    )
    linked_trips = pl.DataFrame(
        {
            "linked_trip_id": [1, 2],
            "day_id": [10, 20],
            "tour_id": [1, 2],
            "joint_trip_id": [500, 500],
            "usable": [True, True],
        }
    )
    tours = pl.DataFrame(
        {
            "tour_id": [1, 2],
            "day_id": [10, 20],
            "joint_tour_id": [900, 900],
            "usable": [True, True],
        }
    )
    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "joint_trips": pl.DataFrame({"joint_trip_id": [500], "usable": [True]}),
        "tours": tours,
        "joint_tours": pl.DataFrame({"joint_tour_id": [900], "usable": [True]}),
    }


class TestUnequalJointMembers:
    """Members on different weights, so no other combination can imitate the sum."""

    @pytest.mark.parametrize(
        ("joint_table", "joint_weight", "member_table", "member_id", "member_weight"),
        [
            pytest.param(
                "joint_trips",
                "joint_trip_weight",
                "linked_trips",
                "linked_trip_id",
                "linked_trip_weight",
                id="trips",
            ),
            pytest.param(
                "joint_tours",
                "joint_tour_weight",
                "tours",
                "tour_id",
                "tour_weight",
                id="tours",
            ),
        ],
    )
    def test_joint_trip_weight_is_the_sum_and_nothing_else(
        self, joint_table, joint_weight, member_table, member_id, member_weight
    ):
        """45 is the sum; the mean, the first and the max are 22.5, 15 and 30."""
        tables = _make_tables_with_unequal_joint_members()
        propagate_weights(tables, {"households": "hh_weight"}, usability_flag_col="usable")

        # the premise: the split really does leave the two members apart
        members = tables[member_table].sort(member_id)[member_weight].to_list()
        assert members == pytest.approx([15.0, 30.0])

        weight = tables[joint_table][joint_weight][0]
        assert weight == pytest.approx(45.0)
        for imitation in (22.5, 15.0, 30.0):
            assert weight != pytest.approx(imitation)


class TestJointEdgeCases:
    """Awkward joint groupings that the sum convention has to survive."""

    def test_members_on_different_days_still_sum(self):
        """The sum crosses the day scope: a grouping is not confined to one day.

        ``scope`` bounds where an unusable record's claim is *re-homed*, not what a
        grouping may span. Two members reporting on different days is ordinary --
        household members are surveyed on their own day rows.
        """
        tables = _make_tables_with_joints()
        # Member 2 becomes usable, so both members carry weight on their own days.
        tables["unlinked_trips"] = tables["unlinked_trips"].with_columns(
            pl.lit(value=True).alias("usable")
        )
        tables["linked_trips"] = tables["linked_trips"].with_columns(
            pl.lit(value=True).alias("usable")
        )
        propagate_weights(tables, {"households": "hh_weight"}, usability_flag_col="usable")

        members = tables["linked_trips"]
        assert members["day_id"].n_unique() == 2
        assert tables["joint_trips"]["joint_trip_weight"][0] == pytest.approx(
            members["linked_trip_weight"].sum()
        )

    def test_a_grouping_with_no_weighted_member_is_zero_not_null(self):
        """Zero is a weight; null would silently drop out of downstream sums."""
        tables = _make_tables_with_joints()
        tables["unlinked_trips"] = tables["unlinked_trips"].with_columns(
            pl.lit(value=False).alias("usable")
        )
        tables["linked_trips"] = tables["linked_trips"].with_columns(
            pl.lit(value=False).alias("usable")
        )
        propagate_weights(tables, {"households": "hh_weight"}, usability_flag_col="usable")

        weight = tables["joint_trips"]["joint_trip_weight"][0]
        assert weight is not None
        assert weight == 0.0


# ---------------------------------------------------------------------------
# The seed gate: who the balancer fits
#
# ``seed_admits`` is the expression the whole per-profile change turns on.
# Filtering the seed by the profile is what makes each fit spread its zone's
# population over the households that will keep a weight; gating afterwards
# instead deletes fitted mass that nothing re-spreads, because households are
# the hierarchy anchor.
#
# It is unit-tested here because the only callers are inside ``compute_weights``,
# which fetches PUMS and so cannot run in the e2e. A mutation replacing this with
# ``survey_complete`` alone survived the entire suite until these existed.
# ---------------------------------------------------------------------------

# Every combination of the two inputs, nulls included.
GATE_FRAME = pl.DataFrame(
    {
        "case": ["both", "flag only", "complete only", "neither", "null flag", "null complete"],
        "profile": [True, True, False, False, None, True],
        "survey_complete": [True, False, True, False, True, None],
    }
)


def _admitted(flag: str = "profile") -> list[str]:
    """The cases *flag* admits into the seed."""
    return GATE_FRAME.filter(seed_admits(flag))["case"].to_list()


class TestTheGate:
    """Both conditions must hold, and a null is not a yes."""

    def test_only_the_fully_qualified_case_is_admitted(self):
        """Stated as the whole set, so a new admission cannot slip in unnoticed.

        The profile alone is not enough (a hand-written flag is not bound by the
        cascade's subset rule), completeness alone is not enough (the seed is the
        profile's universe, not the survey's), and a null means the cascade never
        reached the row rather than that it passed.
        """
        assert _admitted() == ["both"]


class TestGatingOnCompletenessItself:
    """``survey_complete`` is a legitimate thing to weight, and is not floored by itself."""

    def test_every_complete_household_is_seeded(self):
        """Including ones no profile admits -- that is what asking for it means."""
        assert _admitted("survey_complete") == ["both", "complete only", "null flag"]

    def test_it_does_not_require_a_profile_column(self):
        """A survey-analysis run may have no profile columns at all."""
        frame = pl.DataFrame({"survey_complete": [True, False]})
        assert frame.filter(seed_admits("survey_complete")).height == 1


class TestAgainstTheZeroingPredicate:
    """The seed and the zeroing must agree, or a null appears where a zero belongs."""

    def test_everything_it_admits_is_also_usable(self):
        """So a seeded household is never zeroed by the propagation that follows."""
        seeded = GATE_FRAME.filter(seed_admits("profile"))["case"].to_list()
        usable = GATE_FRAME.filter(is_usable("profile"))["case"].to_list()
        assert set(seeded) <= set(usable)


# ---------------------------------------------------------------------------
# The weighting's own post-run checks, and the flag they have to read
#
# ``weight_sanity_checks`` is the last thing ``compute_weights`` calls: it
# compares the balanced totals against their controls and then asserts the
# hierarchy identities the propagation above is supposed to maintain. It had no
# test at all, and that is how it came to call ``_check_hierarchy(tables)`` after
# that function grew a required ``usability_flag_col`` -- a TypeError on the last
# line of every weighting run, in a module the suite never entered.
#
# Two things are pinned. The entry point runs and threads the flag to both
# checks, and the checks read the *same* universe the propagation weighted: the
# identities only hold over records that carried weight, so a check gated on a
# different column would fail on correct output. ``TestNoCallerOmitsTheFlag``
# then generalises the miss -- it walks the tree for any call that drops the
# argument, so the next required parameter cannot quietly diverge from its
# callers.
# ---------------------------------------------------------------------------

FLAG = "ctramp"


def _empty_totals() -> ControlTotals:
    """Controls with nothing to compare, so only the hierarchy checks run."""
    return ControlTotals(
        totals=pl.DataFrame(
            schema={
                "ctrl_geoid": pl.String,
                "control_name": pl.String,
                "category": pl.String,
                "target_total": pl.Float64,
            }
        ),
        pums_hh_count=0,
        pums_person_count=0,
        geo_ids=[],
    )


def _nothing_usable() -> dict[str, pl.DataFrame]:
    """A person carrying weight whose every day was dropped.

    This is the one shape where the flag changes the verdict. The split level
    expects a person's days to sum to their weight; here they sum to zero. Read
    through the flag the scope kept nothing, which is a reported shortfall --
    the weight is deliberately unrepresented below, never pooled onto another
    person. Read without it, every day looks usable and the same data is a
    hierarchy failure.
    """
    return {
        "households": pl.DataFrame(
            {"hh_id": [1], "hh_weight": [100.0], "ctrl_geoid": ["a"], "base_weight": [100.0]}
        ),
        "persons": pl.DataFrame(
            {"person_id": [1], "hh_id": [1], "person_weight": [100.0], FLAG: [True]}
        ),
        "days": pl.DataFrame(
            {
                "day_id": [1, 2],
                "person_id": [1, 1],
                "day_weight": [0.0, 0.0],
                FLAG: [False, False],
            }
        ),
    }


def _coherent_tables(*, usable: list[bool] | None = None) -> dict[str, pl.DataFrame]:
    """One household, two persons, one day each -- weights that reconcile.

    persons is a copy level (each person carries the household weight), days is
    a split level (a person's usable days sum back to the person weight).
    """
    usable = [True, True] if usable is None else usable
    n_usable = sum(usable) or 1
    return {
        "households": pl.DataFrame(
            {"hh_id": [1], "hh_weight": [100.0], "ctrl_geoid": ["a"], "base_weight": [100.0]}
        ),
        "persons": pl.DataFrame(
            {
                "person_id": [1, 2],
                "hh_id": [1, 1],
                "person_weight": [100.0, 100.0],
                FLAG: [True, True],
            }
        ),
        "days": pl.DataFrame(
            {
                "day_id": [1, 2],
                "person_id": [1, 1],
                "day_weight": [100.0 / n_usable if u else 0.0 for u in usable],
                FLAG: usable,
            }
        ),
    }


class TestTheEntryPointRuns:
    """It is the last line of every weighting run, and nothing covered it."""

    def test_coherent_weights_pass(self):
        """The baseline: correct output must not raise."""
        weight_sanity_checks(_coherent_tables(), _empty_totals(), [], FLAG)

    def test_the_flag_reaches_the_hierarchy_check(self):
        """The regression: this call raised TypeError before the flag was threaded.

        Uses the one shape whose verdict depends on the flag, so the argument is
        shown to arrive rather than merely to be accepted.
        """
        weight_sanity_checks(_nothing_usable(), _empty_totals(), [], FLAG)

    def test_a_broken_hierarchy_still_raises(self):
        """The check has teeth -- threading the flag did not defang it."""
        tables = _coherent_tables()
        tables["persons"] = tables["persons"].with_columns(pl.Series("person_weight", [100.0, 7.0]))

        with pytest.raises(ValueError, match="Weight cascade broken"):
            weight_sanity_checks(tables, _empty_totals(), [], FLAG)

    def test_missing_tables_are_skipped_not_crashed(self):
        """A partial run is legitimate; the checks log and return."""
        weight_sanity_checks(
            {"households": pl.DataFrame({"hh_id": [1]})}, _empty_totals(), [], FLAG
        )


class TestTheChecksReadTheWeightedUniverse:
    """Gating on a different column than the propagation used fails correct output."""

    def test_the_same_data_fails_when_the_flag_is_not_found(self):
        """Every child then looks usable, so the shortfall reads as a broken sum.

        Naming a column no table carries is not itself an error -- a project may
        weight tables that were never gated -- which is exactly why the caller
        has to pass the column the propagation actually used.
        """
        with pytest.raises(ValueError, match="Weight cascade broken"):
            _check_hierarchy(_nothing_usable(), "a_profile_nobody_stamped")

    def test_joint_sums_skip_unusable_groupings(self):
        """_aggregate_up zeroes them regardless of members, so they never reconcile."""
        tables = {
            "linked_trips": pl.DataFrame(
                {
                    "linked_trip_id": [1, 2],
                    "joint_trip_id": [10, 10],
                    "linked_trip_weight": [5.0, 5.0],
                }
            ),
            "joint_trips": pl.DataFrame(
                {"joint_trip_id": [10], "joint_trip_weight": [0.0], FLAG: [False]}
            ),
        }

        _check_joint_sums(tables, FLAG)

    def test_a_usable_grouping_that_does_not_reconcile_raises(self):
        """A joint entity that survived must equal the members it kept."""
        tables = {
            "linked_trips": pl.DataFrame(
                {
                    "linked_trip_id": [1, 2],
                    "joint_trip_id": [10, 10],
                    "linked_trip_weight": [5.0, 5.0],
                }
            ),
            "joint_trips": pl.DataFrame(
                {"joint_trip_id": [10], "joint_trip_weight": [3.0], FLAG: [True]}
            ),
        }

        with pytest.raises(ValueError, match="Joint weight is not its members"):
            _check_joint_sums(tables, FLAG)


class TestNoCallerOmitsTheFlag:
    """No call anywhere may drop a required ``usability_flag_col``.

    The specific bug above was one call site; the shape of it is general. When a
    parameter becomes required, the callers are what has to change, and a caller
    inside a module the suite never enters will not say so until a real run
    dies on it. This walks the source instead of relying on coverage.
    """

    ROOTS = ("src", "tests", "projects", "scripts")
    PARAM = "usability_flag_col"

    def _functions_requiring_the_flag(self) -> dict[str, list[str]]:
        """Map function name -> positional parameter names, for those requiring it."""
        required: dict[str, list[str]] = {}
        for path in pathlib.Path("src").rglob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                    continue
                args = node.args
                positional = [a.arg for a in args.posonlyargs + args.args]
                n_defaults = len(args.defaults)
                required_positional = positional[: len(positional) - n_defaults or None]
                required_kwonly = [
                    a.arg
                    for a, d in zip(args.kwonlyargs, args.kw_defaults, strict=True)
                    if d is None
                ]
                if self.PARAM in required_positional + required_kwonly:
                    required[node.name] = positional
        return required

    def _calls_missing_the_flag(self, required: dict[str, list[str]]) -> list[str]:
        misses = []
        for root in self.ROOTS:
            for path in pathlib.Path(root).rglob("*.py"):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                for node in ast.walk(tree):
                    if not isinstance(node, ast.Call):
                        continue
                    func = node.func
                    name = (
                        func.id
                        if isinstance(func, ast.Name)
                        else func.attr
                        if isinstance(func, ast.Attribute)
                        else None
                    )
                    if name not in required:
                        continue
                    by_keyword = {kw.arg for kw in node.keywords if kw.arg}
                    forwards_kwargs = any(kw.arg is None for kw in node.keywords)
                    positional = required[name]
                    index = positional.index(self.PARAM) if self.PARAM in positional else None
                    by_position = index is not None and len(node.args) > index
                    if self.PARAM in by_keyword or by_position or forwards_kwargs:
                        continue
                    misses.append(f"{path}:{node.lineno} {name}()")
        return misses

    def test_the_audit_finds_the_functions(self):
        """A guard that finds nothing to guard would pass forever."""
        required = self._functions_requiring_the_flag()

        assert "_check_hierarchy" in required
        assert "propagate_weights" in required

    def test_every_call_passes_it(self):
        """The guard proper: one entry per call site that dropped the argument."""
        misses = self._calls_missing_the_flag(self._functions_requiring_the_flag())

        assert misses == [], "call sites dropping a required usability_flag_col:\n" + "\n".join(
            misses
        )
