"""What a run reports about the weights it produced.

Three things, read together because the document is where they all land. The
comparer reads two weight sets against each other -- the cases that matter are
the ones where a plausible-looking implementation still reports the wrong thing:
a regression run in the wrong direction, an overlap that quietly includes
records one side never weighted, and a pair with too little in common to say
anything at all. The cascade counts what carries weight at each level. The
report then assembles both, once per run and once per fitted profile.
"""

import json
import re

import numpy as np
import polars as pl
import pytest

from processing.weighting.core.hierarchy import LEVELS
from processing.weighting.core.specs import (
    ControlTotals,
    GeographyCoverage,
    ImputationSummary,
    ProfileFit,
    ZoneStatus,
)
from processing.weighting.diagnostics.comparison import (
    MAX_POINTS,
    MIN_OVERLAP,
    N_DECILES,
    Comparison,
    WeightSet,
    all_pairs,
    compare_pair,
    fitted_weight_sets,
    inheritance,
    reference_weight_sets,
)
from processing.weighting.diagnostics.data import (
    redistribution,
    split_identity,
    weight_cascade,
)
from processing.weighting.diagnostics.report import generate_report

HOUSEHOLDS = LEVELS["households"]
PERSONS = LEVELS["persons"]


def _set(name: str, column: str, *, table: str = "households", fitted: bool = True) -> WeightSet:
    """A weight set reaching one table under one column."""
    return WeightSet(name=name, label=name, columns={table: column}, fitted=fitted)


def _frame(**columns: list[float]) -> pl.DataFrame:
    """A table of weight columns."""
    return pl.DataFrame(columns)


class TestFit:
    """The reduced-major-axis line through the scatter."""

    def test_slope_recovers_a_known_relationship(self):
        """An exact relationship of ln A = 0.5 ln B reads as a slope of 0.5."""
        log_b = np.linspace(np.log(10), np.log(10_000), 500)
        df = _frame(a=list(np.exp(0.5 * log_b)), b=list(np.exp(log_b)))
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        assert stats.slope == pytest.approx(0.5, abs=1e-9)

    def test_weak_correlation_does_not_flatten_the_slope(self):
        """Regression: a least-squares slope shrinks toward zero as correlation weakens.

        Two sets with the same spread and r of about 0.5 have a least-squares
        slope near 0.5, which reads as one set compressing the other when neither
        does. Measured on bats_2023: least squares gave 0.42 for ours on the
        vendor's, where the spreads differ by only 0.86.
        """
        rng = np.random.default_rng(0)
        common = rng.normal(0, 1, 5000)
        log_a = 5 + common + rng.normal(0, 1, 5000)
        log_b = 5 + common + rng.normal(0, 1, 5000)
        df = _frame(a=list(np.exp(log_a)), b=list(np.exp(log_b)))
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        least_squares = np.cov(log_a, log_b, bias=True)[0, 1] / log_b.var()
        assert stats is not None
        assert least_squares == pytest.approx(0.5, abs=0.05)
        assert stats.slope == pytest.approx(1.0, abs=0.05)

    def test_swapping_the_sets_gives_the_reciprocal(self):
        """The fit is symmetric, so the reverse needs no second regression."""
        rng = np.random.default_rng(7)
        log_b = rng.uniform(np.log(20), np.log(2000), 300)
        log_a = 0.4 * log_b + rng.normal(0, 0.5, 300)
        forward = compare_pair(
            _frame(a=list(np.exp(log_a)), b=list(np.exp(log_b))),
            HOUSEHOLDS,
            _set("a", "a"),
            _set("b", "b"),
        )
        reverse = compare_pair(
            _frame(a=list(np.exp(log_b)), b=list(np.exp(log_a))),
            HOUSEHOLDS,
            _set("a", "a"),
            _set("b", "b"),
        )

        assert forward is not None
        assert reverse is not None
        assert forward.slope * reverse.slope == pytest.approx(1.0)

    def test_the_line_passes_through_the_centre(self):
        """The intercept places the line through the mean of both logs."""
        rng = np.random.default_rng(3)
        log_b = rng.uniform(np.log(20), np.log(2000), 400)
        log_a = 1.3 + 0.7 * log_b + rng.normal(0, 0.3, 400)
        df = _frame(a=list(np.exp(log_a)), b=list(np.exp(log_b)))
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        assert stats.intercept + stats.slope * log_b.mean() == pytest.approx(log_a.mean())


class TestSummaries:
    """The five tile values: fold difference at three points, and two correlations."""

    def test_a_set_against_itself(self):
        """The anchor: no difference anywhere, perfect agreement."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(1000), 200)))
        stats = compare_pair(
            _frame(a=weights, b=list(weights)), HOUSEHOLDS, _set("a", "a"), _set("b", "b")
        )

        assert stats is not None
        assert stats.median_fold == pytest.approx(1.0)
        assert stats.p90_fold == pytest.approx(1.0)
        assert stats.max_fold == pytest.approx(1.0)
        assert stats.rank_corr == pytest.approx(1.0)
        assert stats.r2 == pytest.approx(1.0)
        assert stats.slope == pytest.approx(1.0)

    def test_fold_difference_is_symmetric(self):
        """A record twice as heavy in A and one twice as heavy in B differ equally."""
        base = np.full(100, 100.0)
        a = base.copy()
        b = base.copy()
        a[:50] *= 2
        b[50:] *= 2
        stats = compare_pair(
            _frame(a=list(a), b=list(b)), HOUSEHOLDS, _set("a", "a"), _set("b", "b")
        )

        assert stats is not None
        assert stats.median_fold == pytest.approx(2.0)
        assert stats.max_fold == pytest.approx(2.0)

    def test_a_single_extreme_record_moves_only_the_max(self):
        """The tail number and the typical numbers answer different questions."""
        weights = np.exp(np.linspace(np.log(10), np.log(1000), 200))
        a = weights.copy()
        a[0] *= 500
        stats = compare_pair(
            _frame(a=list(a), b=list(weights)), HOUSEHOLDS, _set("a", "a"), _set("b", "b")
        )

        assert stats is not None
        assert stats.median_fold == pytest.approx(1.0)
        assert stats.p90_fold == pytest.approx(1.0)
        assert stats.max_fold == pytest.approx(500.0)

    def test_rank_correlation_ignores_how_the_scale_is_stretched(self):
        """Same ordering, any monotone rescaling: the ranks agree exactly."""
        weights = np.exp(np.linspace(np.log(10), np.log(1000), 200))
        rng = np.random.default_rng(5)
        bent = weights**3 + rng.uniform(0, 1e-6, 200)
        stats = compare_pair(
            _frame(a=list(bent), b=list(weights)), HOUSEHOLDS, _set("a", "a"), _set("b", "b")
        )

        assert stats is not None
        assert stats.rank_corr == pytest.approx(1.0)
        assert stats.median_fold > 1.0

    def test_float_noise_is_no_difference(self):
        """Two fits that agree to 1e-14 read as the same, not as 2% identical."""
        base = np.exp(np.linspace(np.log(10), np.log(1000), 500))
        nudged = base * (1 + np.linspace(-2e-14, 2e-14, 500))
        stats = compare_pair(
            _frame(a=list(base), b=list(nudged)), HOUSEHOLDS, _set("a", "a"), _set("b", "b")
        )

        assert stats is not None
        assert stats.max_fold == pytest.approx(1.0)


class TestOverlap:
    """Which records a comparison is entitled to read."""

    @pytest.mark.parametrize(
        ("blank", "expected_n"),
        [
            # a zero weight is an exclusion from that fit, not a small number
            pytest.param(0.0, 60, id="zero_weights"),
            # a record one side never weighted says nothing about the difference
            pytest.param(None, 60, id="null_weights"),
        ],
    )
    def test_records_one_side_did_not_weight_are_excluded(self, blank, expected_n):
        """Only records both sets actually weighted are compared."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(1000), 100)))
        df = _frame(a=[blank] * 40 + weights, b=weights + [blank] * 40)
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        assert stats.n == expected_n

    def test_too_little_overlap_is_not_reported(self):
        """Below the floor a slope describes the sample, so report nothing."""
        n = MIN_OVERLAP - 1
        weights = list(np.exp(np.linspace(np.log(10), np.log(100), n)))
        df = _frame(a=weights, b=list(weights))

        assert compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b")) is None

    def test_missing_column_is_not_an_error(self):
        """A set that does not reach this level is skipped, not a failure."""
        df = _frame(a=[1.0] * 100)

        assert compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b")) is None
        assert compare_pair(df, PERSONS, _set("a", "a"), _set("b", "b")) is None


class TestDeciles:
    """The stability strip: how the ratio behaves across the range of B."""

    def test_heavy_ties_collapse_bins_rather_than_erroring(self):
        """A set where most weights are equal has fewer than ten distinct edges."""
        df = _frame(a=[100.0] * 90 + [500.0] * 10, b=[100.0] * 90 + [250.0] * 10)
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        assert 0 < len(stats.deciles_ab) < N_DECILES

    def test_a_stable_pair_has_a_flat_band(self):
        """Constant offset between the sets means every decile reads the same."""
        weights = np.exp(np.linspace(np.log(10), np.log(10_000), 500))
        df = _frame(a=list(weights * 3), b=list(weights))
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        # the strip never carries more bands than it has room to label
        assert len(stats.deciles_ab) == N_DECILES
        medians = [d.median for d in stats.deciles_ab]
        assert max(medians) - min(medians) == pytest.approx(0.0, abs=1e-9)
        assert all(d.iqr == pytest.approx(0.0, abs=1e-9) for d in stats.deciles_ab)


class TestSampling:
    """The points drawn for the scatter."""

    @pytest.mark.parametrize(
        ("n", "expected_points"),
        [
            # a level with a quarter-million records still ships a readable scatter
            pytest.param(MAX_POINTS * 3, MAX_POINTS, id="capped"),
            # under the cap nothing is thrown away
            pytest.param(50, 50, id="every_point_kept"),
        ],
    )
    def test_sample_is_capped(self, n, expected_points):
        """The scatter carries every overlapping record, up to the cap."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(10_000), n)))
        df = _frame(a=weights, b=list(weights))
        stats = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert stats is not None
        assert len(stats.points) == expected_points

    def test_sampling_is_reproducible(self):
        """Rerunning on unchanged data must rewrite an identical report."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(10_000), MAX_POINTS * 2)))
        df = _frame(a=weights, b=list(weights))
        first = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))
        second = compare_pair(df, HOUSEHOLDS, _set("a", "a"), _set("b", "b"))

        assert first is not None
        assert second is not None
        assert first.points == second.points


class TestWeightSets:
    """Resolving which sets exist before anything is compared."""

    def test_fitted_sets_take_columns_from_the_hierarchy(self):
        """The profile suffix is spelled in one place, and it is not here."""
        (ctramp,) = fitted_weight_sets(["ctramp"])

        assert ctramp.name == "ctramp"
        assert ctramp.fitted
        assert ctramp.columns["households"] == "hh_weight_ctramp"
        assert ctramp.columns["unlinked_trips"] == "unlinked_trip_weight_ctramp"

    def test_unprofiled_fit_is_named_for_the_survey(self):
        """A run weighting the whole survey has no suffix to carry."""
        (survey,) = fitted_weight_sets([None])

        assert survey.name == "survey"
        assert survey.columns["households"] == "hh_weight"

    def test_reference_sets_are_named_not_discovered(self):
        """A supplied column need not share the level's base name.

        The vendor's trip weights arrive as ``trip_weight`` while the hierarchy
        calls that level ``unlinked_trip_weight``, so no suffix rule can reach
        them and the config states the column outright.
        """
        (vendor,) = reference_weight_sets(
            {
                "vendor": {
                    "label": "vendor",
                    "columns": {"households": "hh_weight", "unlinked_trips": "trip_weight"},
                }
            }
        )

        assert not vendor.fitted
        assert vendor.columns["unlinked_trips"] == "trip_weight"

    @pytest.mark.parametrize(
        "config",
        [
            # a set reaching no level cannot be compared to anything
            pytest.param({"vendor": {"label": "vendor"}}, id="no_columns"),
            # with nothing configured the run compares its own fits and stops
            pytest.param(None, id="nothing_configured"),
        ],
    )
    def test_a_set_with_nothing_to_compare_is_dropped(self, config):
        """Only reference sets that reach at least one level survive."""
        assert reference_weight_sets(config) == []


class TestAllPairs:
    """Every comparable pair, at every level both sets reach."""

    def test_one_entry_per_pair_per_level(self):
        """Pairs are unordered, so three sets make three comparisons, not six."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(1000), 100)))
        sets = [
            WeightSet("x", "x", {"households": "x", "persons": "x"}, fitted=True),
            WeightSet("y", "y", {"households": "y", "persons": "y"}, fitted=True),
            WeightSet("z", "z", {"households": "z", "persons": "z"}, fitted=True),
        ]
        table = _frame(x=weights, y=list(weights), z=list(weights))
        pairs = all_pairs({"households": table, "persons": table}, sets)

        # Three sets make three pairs, at each of the two levels.
        assert len(pairs) == 6
        assert {p.level for p in pairs} == {"households", "persons"}

    def test_levels_a_set_does_not_reach_are_skipped(self):
        """A set that stops at households says nothing about persons."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(1000), 100)))
        sets = [
            WeightSet("x", "x", {"households": "x", "persons": "x"}, fitted=True),
            WeightSet("y", "y", {"households": "y"}, fitted=True),
        ]
        table = _frame(x=weights, y=list(weights))
        pairs = all_pairs({"households": table, "persons": table}, sets)

        assert [p.level for p in pairs] == ["households"]

    def test_nothing_to_pair_is_not_an_error(self):
        """A table the run never built, and a lone set, both yield no pairs."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(1000), 100)))

        assert all_pairs({"households": None}, [_set("x", "x"), _set("y", "y")]) == []
        assert all_pairs({"households": _frame(x=weights)}, [_set("x", "x")]) == []


class TestInheritance:
    """How a level's weight descends from its parent, and by what mechanism."""

    def _tables(self, *, trips, days, day_of_trip=None):
        """A days table and a trips table, trips mapped to days by *day_of_trip*."""
        ids = day_of_trip if day_of_trip is not None else list(range(len(trips)))
        return {
            "days": pl.DataFrame({"day_id": list(range(len(days))), "dw": days}),
            "unlinked_trips": pl.DataFrame({"day_id": ids, "tw": trips}),
        }

    def _set(self):
        return WeightSet(
            name="v", label="v", columns={"days": "dw", "unlinked_trips": "tw"}, fitted=False
        )

    def test_a_pure_copy_adds_nothing(self):
        """Every child carries its parent's weight, so the level restates its parent."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(100), 100)))
        (row,) = inheritance(self._tables(trips=weights, days=list(weights)), [self._set()])

        assert row.level == "unlinked_trips"
        assert row.parent == "days"
        assert row.share == pytest.approx(100.0)
        assert row.mode == "copied"

    def test_one_factor_per_scope_is_redistribution(self):
        """Siblings absorbing a dropped sibling all move by the same factor.

        Two trips share each day; both are scaled together, so the level is still
        a faithful expansion of the level above.
        """
        days = list(np.exp(np.linspace(np.log(10), np.log(100), 60)))
        day_of_trip, trips = [], []
        for i, dw in enumerate(days):
            factor = 1.5 if i % 3 == 0 else 1.0
            day_of_trip += [i, i]
            trips += [dw * factor, dw * factor]
        (row,) = inheritance(
            self._tables(trips=trips, days=days, day_of_trip=day_of_trip), [self._set()]
        )

        assert row.n_scopes_varying == 0
        assert row.max_scope_spread == pytest.approx(1.0)
        assert row.share < 100.0
        assert row.mode == "redistributed"

    def test_siblings_on_different_factors_are_an_adjustment(self):
        """The case worth flagging: records rescaled individually after descending.

        Measured on bats_2023, the vendor's trip weights vary within 1,102 of
        25,312 days by up to exactly 2x, while every fitted profile varies within
        none. A summary that only counts departures from the parent cannot tell
        the two apart.
        """
        days = list(np.exp(np.linspace(np.log(10), np.log(100), 60)))
        day_of_trip, trips = [], []
        for i, dw in enumerate(days):
            day_of_trip += [i, i]
            trips += [dw, dw * 2 if i % 5 == 0 else dw]
        (row,) = inheritance(
            self._tables(trips=trips, days=days, day_of_trip=day_of_trip), [self._set()]
        )

        assert row.n_scopes_varying == 12
        assert row.max_scope_spread == pytest.approx(2.0)
        assert row.mode == "adjusted"

    def test_float_noise_is_not_an_adjustment(self):
        """Division noise must not be read as siblings on different factors."""
        days = list(np.exp(np.linspace(np.log(10), np.log(100), 60)))
        day_of_trip, trips = [], []
        for i, dw in enumerate(days):
            day_of_trip += [i, i]
            trips += [dw, dw * (1 + 1e-15)]
        (row,) = inheritance(
            self._tables(trips=trips, days=days, day_of_trip=day_of_trip), [self._set()]
        )

        assert row.n_scopes_varying == 0
        assert row.mode != "adjusted"

    def test_split_levels_are_excluded(self):
        """Days divide their person's weight, so sibling ratios differ by design."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(100), 100)))
        tables = {
            "persons": pl.DataFrame({"person_id": list(range(100)), "pw": weights}),
            "days": pl.DataFrame({"person_id": list(range(100)), "dw": list(weights)}),
        }
        weight_set = WeightSet("v", "v", {"persons": "pw", "days": "dw"}, fitted=False)

        assert inheritance(tables, [weight_set]) == []

    def test_a_set_not_reaching_both_levels_is_skipped(self):
        """Inheritance needs a parent weight to inherit from."""
        weights = list(np.exp(np.linspace(np.log(10), np.log(100), 100)))
        weight_set = WeightSet("v", "v", {"unlinked_trips": "tw"}, fitted=False)

        assert inheritance(self._tables(trips=weights, days=list(weights)), [weight_set]) == []


# ===========================================================================
# The weight cascade: what carries weight below the household
#
# The cases that matter are the ones where a level disagrees with its parent:
# a record the profile admits whose household was dropped, and a parent
# carrying weight with no usable child at all. Both are correct behaviour
# that the report exists to surface.
# ===========================================================================

PROFILE = "ctramp"
FLAG = "usable_ctramp"


def _tables(
    *,
    hh_usable: list[bool],
    hh_weight: list[float],
    per_usable: list[bool],
    per_weight: list[float],
    per_hh: list[int],
) -> dict[str, pl.DataFrame]:
    """Minimal households/persons pair carrying one profile's columns."""
    return {
        "households": pl.DataFrame(
            {
                "hh_id": list(range(1, len(hh_usable) + 1)),
                FLAG: hh_usable,
                f"hh_weight_{PROFILE}": hh_weight,
            }
        ),
        "persons": pl.DataFrame(
            {
                "person_id": list(range(1, len(per_usable) + 1)),
                "hh_id": per_hh,
                FLAG: per_usable,
                f"person_weight_{PROFILE}": per_weight,
            }
        ),
    }


class TestWeightCascade:
    """Counts per level, split by why a record carries no weight."""

    def test_counts_split_exhaustively(self):
        """Gated + weighted + unweighted must account for every row."""
        tables = _tables(
            hh_usable=[True, False],
            hh_weight=[10.0, 0.0],
            per_usable=[True, True, False],
            per_weight=[10.0, 0.0, 0.0],
            per_hh=[1, 2, 1],
        )
        cascade = weight_cascade(tables, profile=PROFILE, usability_flag_col=FLAG)
        rows = {r.table: r for r in cascade}

        households = rows["households"]
        assert (households.rows, households.usable, households.weighted) == (2, 1, 1)
        assert households.gated == 1
        assert households.unweighted == 0

        persons = rows["persons"]
        assert (persons.rows, persons.usable, persons.weighted) == (3, 2, 1)
        assert persons.gated == 1
        assert persons.unweighted == 1

    def test_usable_person_in_dropped_household_is_unweighted(self):
        """A person the profile admits still carries nothing if their household fell.

        Person and household reduce differently -- a person needs one usable day,
        a household needs a date on which every member was usable -- so the two
        verdicts legitimately disagree. The count has to show it rather than
        assume the flag implies a weight.
        """
        tables = _tables(
            hh_usable=[False],
            hh_weight=[0.0],
            per_usable=[True],
            per_weight=[0.0],
            per_hh=[1],
        )
        persons = next(
            r
            for r in weight_cascade(tables, profile=PROFILE, usability_flag_col=FLAG)
            if r.table == "persons"
        )
        assert persons.usable == 1
        assert persons.weighted == 0
        assert persons.unweighted == 1
        assert persons.gated == 0

    @pytest.mark.parametrize(
        "drop",
        [
            pytest.param("rename", id="another_profiles_column"),
            pytest.param("delete", id="table_not_supplied"),
        ],
    )
    def test_a_level_this_profile_does_not_reach_is_skipped(self, drop):
        """Whether the column belongs to another profile or the table is absent."""
        tables = _tables(
            hh_usable=[True],
            hh_weight=[1.0],
            per_usable=[True],
            per_weight=[1.0],
            per_hh=[1],
        )
        if drop == "rename":
            tables["persons"] = tables["persons"].rename(
                {f"person_weight_{PROFILE}": "person_weight_x"}
            )
        else:
            del tables["persons"]

        reported = [
            r.table for r in weight_cascade(tables, profile=PROFILE, usability_flag_col=FLAG)
        ]
        assert reported == ["households"]


class TestRedistribution:
    """The factor a survivor absorbs for the siblings the gate removed."""

    def test_ratio_is_one_when_every_sibling_is_usable(self):
        """Nothing was removed, so nothing is being carried."""
        tables = _tables(
            hh_usable=[True],
            hh_weight=[10.0],
            per_usable=[True, True],
            per_weight=[10.0, 10.0],
            per_hh=[1, 1],
        )
        row = next(iter(redistribution(tables, profile=PROFILE, usability_flag_col=FLAG)))
        assert row.p50 == 1.0
        assert row.maximum == 1.0
        assert row.share_above == 0.0

    def test_survivor_carries_the_whole_household(self):
        """One usable member of four stands in for all of them."""
        tables = _tables(
            hh_usable=[True],
            hh_weight=[10.0],
            per_usable=[True, False, False, False],
            per_weight=[40.0, 0.0, 0.0, 0.0],
            per_hh=[1, 1, 1, 1],
        )
        row = next(iter(redistribution(tables, profile=PROFILE, usability_flag_col=FLAG)))
        assert row.maximum == 4.0
        assert row.share_above == 100.0

    def test_dropped_households_are_excluded(self):
        """A rejected household's members say nothing about redistribution."""
        tables = _tables(
            hh_usable=[True, False],
            hh_weight=[10.0, 0.0],
            per_usable=[True, True],
            per_weight=[10.0, 0.0],
            per_hh=[1, 2],
        )
        row = next(iter(redistribution(tables, profile=PROFILE, usability_flag_col=FLAG)))
        assert row.maximum == 1.0


class TestSplitIdentity:
    """Days divide their person's weight; they must sum back to it."""

    @staticmethod
    def _with_days(day_weight: list[float], day_person: list[int]) -> dict[str, pl.DataFrame]:
        tables = _tables(
            hh_usable=[True],
            hh_weight=[10.0],
            per_usable=[True, True],
            per_weight=[10.0, 10.0],
            per_hh=[1, 1],
        )
        tables["days"] = pl.DataFrame(
            {
                "day_id": list(range(1, len(day_weight) + 1)),
                "person_id": day_person,
                FLAG: [True] * len(day_weight),
                f"day_weight_{PROFILE}": day_weight,
            }
        )
        return tables

    def test_days_summing_to_the_person_leave_no_residual(self):
        """Two usable days at half the person weight each."""
        tables = self._with_days([5.0, 5.0, 10.0], [1, 1, 2])
        row = next(iter(split_identity(tables, profile=PROFILE)))
        assert row.max_residual == 0.0
        assert row.stranded_parents == 0
        assert row.parents_checked == 2

    def test_residual_is_reported_not_swallowed(self):
        """A split that does not reconcile is arithmetic we control -- surface it."""
        tables = self._with_days([5.0, 3.0, 10.0], [1, 1, 2])
        row = next(iter(split_identity(tables, profile=PROFILE)))
        assert row.max_residual == 2.0

    def test_person_with_no_usable_day_is_stranded(self):
        """Their weight is unrepresented below, never pooled onto other people."""
        tables = self._with_days([10.0], [2])
        row = next(iter(split_identity(tables, profile=PROFILE)))
        assert row.stranded_parents == 1
        assert row.stranded_weight == 10.0


# ===========================================================================
# Assembling the run's diagnostics document
#
# One report covers the whole run, so the cases that matter are the ones about
# *how many fits* it describes: a single un-profiled fit must still render, and
# several fits must each get their own pane without the run-level sections
# being repeated.
# ===========================================================================

TARGETS = ["h_total", "p_total"]
ZONE = "01"


def _control_totals() -> ControlTotals:
    """Targets for one zone at both structural controls."""
    return ControlTotals(
        totals=pl.DataFrame(
            {
                "geo_id": [ZONE, ZONE],
                "control_name": ["h_total", "p_total"],
                "category": ["total", "total"],
                "target_total": [1000.0, 2500.0],
            }
        ),
        pums_hh_count=500,
        pums_person_count=1200,
        geo_ids=[ZONE],
    )


def _fit(profile: str | None, *, n_hh: int = 4) -> ProfileFit:
    """A completed fit carrying the minimum every report section reads."""
    hh_ids = list(range(1, n_hh + 1))
    seed = pl.DataFrame(
        {
            "hh_id": hh_ids,
            "ctrl_geoid": [ZONE] * n_hh,
            "study_geoid": [ZONE] * n_hh,
            "h_total": [1.0] * n_hh,
            "p_total": [2.0] * n_hh,
            "base_weight": [200.0] * n_hh,
        }
    )
    return ProfileFit(
        profile=profile,
        usability_flag_col=f"usable_{profile}" if profile else "survey_complete",
        seed_incidence=seed,
        pre_imputation_incidence=seed,
        imputation_summary=[ImputationSummary("h_size", "household", n_hh, 1, 0.4, 0.8)],
        coverage=GeographyCoverage(profile=profile, n_universe=n_hh, n_placed=n_hh),
        weights=pl.DataFrame({"hh_id": hh_ids, "hh_weight": [250.0] * n_hh}),
        statuses=[
            ZoneStatus(geo_id=ZONE, converged=True, iterations=7, delta=1e-9, max_gamma_diff=1e-9)
        ],
    )


def _propagated_tables(profiles: list[str | None]) -> dict[str, pl.DataFrame]:
    """Propagated canonical tables carrying one weight column per profile."""
    households = pl.DataFrame({"hh_id": [1, 2, 3, 4]})
    persons = pl.DataFrame({"person_id": [1, 2, 3, 4], "hh_id": [1, 1, 2, 3]})
    for profile in profiles:
        suffix = f"_{profile}" if profile else ""
        flag = f"usable_{profile}" if profile else "survey_complete"
        households = households.with_columns(
            pl.Series(flag, [True, True, True, False]),
            pl.Series(f"hh_weight{suffix}", [250.0, 250.0, 250.0, 0.0]),
        )
        persons = persons.with_columns(
            pl.Series(flag, [True, True, True, True]),
            pl.Series(f"person_weight{suffix}", [250.0, 250.0, 250.0, 250.0]),
        )
    return {"households": households, "persons": persons}


def _headings(html: str) -> list[tuple[int, str]]:
    """Every numbered section heading in document order, as (number, title)."""
    return [
        (int(number), title.strip())
        for number, title in re.findall(r"<h2>(\d+) &mdash; ([^<]*)", html)
    ]


def _numbered_headings(html: str) -> list[int]:
    """Distinct section numbers, in the order they first appear.

    The per-profile sections repeat once per pane, so a run's numbering is the
    de-duplicated sequence rather than every heading in the file.
    """
    seen: list[int] = []
    for number, _ in _headings(html):
        if number not in seen:
            seen.append(number)
    return seen


def _section(html: str, section_id: str) -> str:
    """One section's markup, from its opening tag to its close."""
    start = html.index(f'<section id="{section_id}"')
    return html[start : html.index("</section>", start)]


def _panes(html: str) -> list[str]:
    """The per-profile panes, as raw HTML fragments."""
    return html.split('class="profile-pane"')[1:]


class TestGenerateReport:
    """The document a run writes, however many profiles it fitted."""

    def test_single_unprofiled_fit_renders(self, tmp_path):
        """A run weighting the whole survey collapses the toggle to one pane."""
        out = generate_report(
            fits={None: _fit(None)},
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_propagated_tables([None]),
            output_path=tmp_path / "diagnostics.html",
        )
        html = out.read_text(encoding="utf-8")
        assert html.count('class="profile-pane"') == 1
        assert 'data-profile="survey"' in html

    def test_one_pane_per_fit(self, tmp_path):
        """Each profile gets a pane and a button; run-level sections appear once."""
        fits = {p: _fit(p) for p in ("ctramp", "daysim", "analysis")}
        out = generate_report(
            fits=fits,
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_propagated_tables(list(fits)),
            output_path=tmp_path / "diagnostics.html",
            run_meta={"PUMS": "2023 &middot; FIPS 06"},
        )
        html = out.read_text(encoding="utf-8")

        assert html.count('class="profile-pane"') == 3
        for profile in fits:
            assert f'data-profile="{profile}"' in html
        # a report shared detached from its run still identifies the run
        assert "2023 &middot; FIPS 06" in html

        # The run-level sections are written once, not once per fit.
        titles = [title for _, title in _headings(html)]
        assert titles.count("Profile Comparison") == 1
        assert titles.count("Weight Cascade") == 1
        cascade = _section(html, "sec-cascade")
        for profile in fits:
            assert f'colspan="3">{profile}' in cascade
        # No geometry was passed, so the crosswalk is left out entirely rather
        # than rendered as a heading, a sidebar entry and an empty map.
        assert "Crosswalk Map" not in html

    def test_omitted_imputation_leaves_no_gap(self, tmp_path):
        """A run that imputed nothing skips the section without skipping a number.

        Regression: the heading numbers were literals, so a run with nothing to
        impute rendered 1, 2, 3, 5 -- the section vanished and took its number
        with it.
        """
        fit = _fit("ctramp")
        fit.imputation_summary = []
        out = generate_report(
            fits={"ctramp": fit},
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_propagated_tables(["ctramp"]),
            output_path=tmp_path / "diagnostics.html",
        )
        html = out.read_text(encoding="utf-8")
        assert "Fractional Seed Imputation" not in html
        numbers = _numbered_headings(html)
        assert numbers == list(range(1, len(numbers) + 1))

    def test_no_fits_raises(self, tmp_path):
        """An empty run has nothing to describe; say so rather than write a shell."""
        with pytest.raises(ValueError, match="at least one completed fit"):
            generate_report(
                fits={},
                control_totals=_control_totals(),
                target_names=TARGETS,
                tables={},
                output_path=tmp_path / "diagnostics.html",
            )


class TestSectionNumbering:
    """Heading numbers as addresses: consecutive, and fixed under the toggle."""

    def test_panes_share_one_numbering(self, tmp_path):
        """A section number is an address, so it cannot move under the toggle."""
        fits = {p: _fit(p) for p in ("ctramp", "daysim", "analysis")}
        out = generate_report(
            fits=fits,
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_propagated_tables(list(fits)),
            output_path=tmp_path / "diagnostics.html",
        )
        panes = _panes(out.read_text(encoding="utf-8"))
        assert len(panes) == 3
        assert len({tuple(_headings(pane)) for pane in panes}) == 1

    def test_pane_missing_a_section_keeps_the_others_in_place(self, tmp_path):
        """One profile with nothing to impute must not renumber the other's pane.

        The number is assigned once for the document, so the profile that skips
        the section leaves a gap in its own pane rather than shifting every
        heading below it.
        """
        fits = {p: _fit(p) for p in ("ctramp", "analysis")}
        fits["analysis"].imputation_summary = []
        out = generate_report(
            fits=fits,
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_propagated_tables(list(fits)),
            output_path=tmp_path / "diagnostics.html",
        )
        ctramp, analysis = (
            {title: number for number, title in _headings(pane)}
            for pane in _panes(out.read_text(encoding="utf-8"))
        )

        # Only ctramp renders the imputation section, and it keeps its number.
        imputed = next(t for t in ctramp if t.startswith("Fractional"))
        assert imputed not in analysis
        assert ctramp[imputed] not in analysis.values()

        # Every section both panes do render carries the same number in each.
        for title in ctramp.keys() & analysis.keys():
            assert ctramp[title] == analysis[title]


class TestComparerSection:
    """The pairwise weight-set comparer, which is run-level or absent."""

    def _comparison(self, names):
        """A comparison over the household weights the fixture tables carry."""
        sets = [
            WeightSet(name=n, label=n, columns={"households": f"hh_weight_{n}"}, fitted=True)
            for n in names
        ]
        weights = np.exp(np.linspace(np.log(10), np.log(1000), 200))
        table = pl.DataFrame({f"hh_weight_{n}": weights * (i + 1) for i, n in enumerate(names)})
        return Comparison(sets=sets, pairs=all_pairs({"households": table}, sets))

    def _render(self, tmp_path, comparison):
        """Render a one-profile report carrying *comparison*."""
        out = generate_report(
            fits={"ctramp": _fit("ctramp")},
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_propagated_tables(["ctramp"]),
            output_path=tmp_path / "diagnostics.html",
            comparison=comparison,
        )
        return out.read_text(encoding="utf-8")

    def test_absent_when_nothing_to_compare(self, tmp_path):
        """One weight set has no pair, so the section is left out entirely."""
        html = self._render(tmp_path, None)

        assert "Weight Set Comparer" not in html
        titles = [title for _, title in _headings(html)]
        assert not any(t.startswith("Weight Set Comparer") for t in titles)

    def test_pairs_are_embedded_as_data(self, tmp_path):
        """One payload drawn in the browser, not one baked figure per pair."""
        html = self._render(tmp_path, self._comparison(["ctramp", "vendor"]))
        comparer = _section(html, "sec-comparer")
        blob = html.split("var DATA = ")[1].split(";\n")[0]
        data = json.loads(blob)

        # the section is run-level, so it sits above the per-profile panes,
        # appears once, and carries the five agreed summary tiles
        assert html.index('id="sec-comparer"') < html.index('class="profile-pane"')
        assert [t for _, t in _headings(html)].count("Weight Set Comparer") == 1
        assert comparer.count('class="tile"') == 5
        # the caveat is above the plot it applies to
        assert html.index("not a validation") < html.index('id="cmp-scatter"')
        # adding a run-level section must not leave a gap in the numbering
        numbers = _numbered_headings(html)
        assert numbers == list(range(1, len(numbers) + 1))

        assert [s["name"] for s in data["sets"]] == ["ctramp", "vendor"]
        assert data["levels"] == [{"name": "households", "label": "Households"}]
        assert len(data["pairs"]) == 1
        pair = data["pairs"][0]
        assert pair["n"] == 200
        assert {"med", "p90", "max", "rho", "r2", "slope", "icept", "decAB", "decBA"} <= set(pair)


class TestNavigationAndDefinitions:
    """The sidebar outline and the Definitions section every header links into."""

    def _render(self, tmp_path, profiles=("ctramp", "analysis")):
        out = generate_report(
            fits={p: _fit(p) for p in profiles},
            control_totals=_control_totals(),
            target_names=TARGETS,
            tables=_propagated_tables(list(profiles)),
            output_path=tmp_path / "diagnostics.html",
        )
        return out.read_text(encoding="utf-8")

    def test_every_definition_link_has_a_target(self, tmp_path):
        """A header that links to a definition the page does not carry is a dead end."""
        html = self._render(tmp_path)
        links = set(re.findall(r'href="#(g-[\w]+)"', html))
        targets = set(re.findall(r'id="(g-[\w]+)"', html))

        assert links
        assert links <= targets

    def test_definitions_list_only_what_the_page_uses(self, tmp_path):
        """With nothing to compare, the comparer's statistics are not listed."""
        html = self._render(tmp_path)

        assert 'id="g-ess"' in html
        assert 'id="g-rho"' not in html

    def test_sidebar_lists_every_numbered_section(self, tmp_path):
        """One entry per rendered section, carrying the same number as the heading."""
        html = self._render(tmp_path)
        nav = html[html.index('<nav class="side"') : html.index("</nav>")]
        entries = re.findall(r'data-sec="(\w+)"><span class="n">(\d+)</span>', nav)

        assert [int(n) for _, n in entries] == _numbered_headings(html)

    def test_section_ids_are_unique(self, tmp_path):
        """Each pane repeats its sections, so their ids carry the profile."""
        html = self._render(tmp_path)
        ids = re.findall(r'<section id="([^"]+)"', html)

        assert len(ids) == len(set(ids))
        assert "sec-balancer-ctramp" in ids
        assert "sec-balancer-analysis" in ids
