"""The controls a weighting run fits to, from definition to checksum.

Four things, read together because each consumes the last. Cross-tab targets are
registered from YAML config and recoded into a composite key, then pivoted into
an incidence table and collapsed by N-D merges. Control totals are aggregated
from the recoded PUMS frames and may be pooled across zone groups. Zone-targeted
1-D merges layer on top of the global ones, and the fit report reads its labels
off them. The checksums then assert the incidence table against its own
structural totals.
"""

import logging
import re

import numpy as np
import polars as pl
import pytest

from data_canon.codebook.households import IncomeBroad
from processing.weighting.balancing.merges import apply_category_merges
from processing.weighting.controls.base import ControlLevel, CrosstabControlTarget
from processing.weighting.controls.enums import HHSizeCategory
from processing.weighting.controls.registry import CONTROLS, register_crosstab
from processing.weighting.core.specs import (
    ControlRegistryConfig,
    ControlSpec,
    ControlTotals,
    MergeSpec,
)
from processing.weighting.data_prep.control_data import (
    apply_zone_groups,
    build_control_totals,
    recode_pums_households,
    recode_pums_persons,
)
from processing.weighting.data_prep.incidence import (
    aggregate_control_totals,
    build_incidence_table,
)
from processing.weighting.data_prep.merges import apply_1d_merges, apply_crosstab_merges
from processing.weighting.data_prep.seed_data import recode_survey_households
from processing.weighting.diagnostics.data import apply_fit_merges
from processing.weighting.validation.checksums import check_incidence_sums, check_recode_nulls
from processing.weighting.validation.control_validation import warn_crosstab_sparsity

# The merge every test in this file uses: income collapsed to three bins
# (under 100k, 100-200k, 200k+) and size to three (1, 2, 3 or more), so the
# 10 x 6 cartesian product becomes 3 x 3 = 9 cells.
PREMERGE_GROUPS = {
    "h_income": {
        "income_under_100": [
            "income_under25",
            "income_25to50",
            "income_50to75",
            "income_75to100",
        ],
    },
    "h_size": {
        "size_3_plus": [
            "size_3",
            "size_4",
            "size_5",
            "size_6",
            "size_7",
            "size_8",
            "size_9",
            "size_10_plus",
        ],
    },
}


@pytest.fixture(autouse=True)
def _isolate_registry(_clean_registry):
    """Every test here registers cross-tabs, so every test cleans up after itself.

    Autouse, not a ``usefixtures`` mark: autouse fixtures are set up before the
    ones a test asks for, so the cleanup's snapshot is taken before a fixture
    like ``size_income_xtab`` registers anything. Under a mark the order flips,
    the new cross-tab is mistaken for a pre-existing one, and the next test
    fails with "already exists in registry".
    """


@pytest.fixture
def size_income_xtab():
    """Register and return a h_size x h_income cross-tab control."""
    return register_crosstab("h_size_x_income", ["h_size", "h_income"])


@pytest.fixture
def households() -> pl.DataFrame:
    """Synthetic canonical households (3 HHs, 2 zones)."""
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 3, 4],
            "income_bin": [1, 5, 3, 2],  # UNDER25, 100TO200, 50TO75, 25TO50
            "ctrl_geoid": ["Z1", "Z1", "Z2", "Z2"],
        }
    )


@pytest.fixture
def persons() -> pl.DataFrame:
    """Synthetic persons — defines household size."""
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 2, 3, 3, 3, 4],
            "person_id": [101, 201, 202, 301, 302, 303, 401],
            "age": [5, 5, 5, 5, 5, 5, 5],  # adult ages (AgeCategory values)
            "gender": [2, 1, 2, 1, 2, 1, 2],  # canonical Gender values
            "employment": [5, 5, 5, 5, 5, 5, 5],  # not employed
            "student": [2, 2, 2, 2, 2, 2, 2],  # non-student
            "school_type": [None, None, None, None, None, None, None],
            "work_mode": [None, None, None, None, None, None, None],
        }
    )


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabRegistration:
    """Tests for dynamic cross-tab control registration."""

    def test_register_creates_control(self):
        """Cross-tab is registered in the global CONTROLS dict, with its dimensions."""
        ctrl = CONTROLS["h_size_x_income"]
        assert isinstance(ctrl, CrosstabControlTarget)
        assert [d.name for d in ctrl.dim_controls] == ["h_size", "h_income"]

    def test_register_creates_composite_enum(self):
        """Composite enum has full cartesian product of dimension members."""
        ctrl = CONTROLS["h_size_x_income"]
        n_size = len(list(HHSizeCategory))
        n_income = len([m for m in IncomeBroad if m.name not in ("MISSING", "PNTA")])
        assert len(ctrl.valid_members) == n_size * n_income

    def test_register_duplicate_raises(self):
        """Re-registering the same name raises ValueError."""
        with pytest.raises(ValueError, match="already exists"):
            register_crosstab("h_size_x_income", ["h_size", "h_income"])

    def test_register_with_merges_reduces_cell_count(self):
        """Pre-merge at registration reduces the enum to effective cell count."""
        xtab = register_crosstab(
            "h_size_income_merged",
            ["h_size", "h_income"],
            merges=PREMERGE_GROUPS,
        )
        # 3 size bins x 3 income bins = 9 cells
        assert len(xtab.valid_members) == 9
        # and the merged groups name themselves in the composite members
        member_names = [name for _, name in xtab.valid_members]
        assert "SIZE_3_PLUS_INCOME_UNDER_100" in member_names

    def test_from_yaml_parses_dimensions(self):
        """Cross-tab specs parse dimensions from YAML config."""
        config = [
            {"name": "h_size"},
            {"name": "h_income"},
            {"name": "h_size_by_income", "dimensions": ["h_size", "h_income"]},
        ]
        parsed = ControlRegistryConfig.from_yaml(config)
        xtab_spec = next(s for s in parsed.specs if s.name == "h_size_by_income")
        assert xtab_spec.dimensions == ["h_size", "h_income"]
        # Non-crosstab specs have None
        size_spec = next(s for s in parsed.specs if s.name == "h_size")
        assert size_spec.dimensions is None

    def test_from_yaml_stores_merges_on_spec(self):
        """Cross-tab merges are stored on ControlSpec, not in crosstab_merges list.

        They are applied at registration time, not post-pivot.
        """
        config = [
            {"name": "h_size"},
            {"name": "h_income"},
            {
                "name": "h_size_by_income",
                "dimensions": ["h_size", "h_income"],
                "merges": {
                    "h_income": {
                        "income_under_100": [
                            "income_under25",
                            "income_25to50",
                            "income_50to75",
                            "income_75to100",
                        ]
                    }
                },
            },
        ]
        parsed = ControlRegistryConfig.from_yaml(config)
        # Cross-tab merges are NOT added to crosstab_merges (pre-merge at registration)
        assert len(parsed.crosstab_merges) == 0
        # Instead, merges are stored on the ControlSpec itself
        xtab_spec = next(s for s in parsed.specs if s.name == "h_size_by_income")
        assert xtab_spec.merges is not None
        assert "h_income" in xtab_spec.merges


# ---------------------------------------------------------------------------
# Composite expression
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabExpression:
    """Tests for composite key expression building."""

    def test_expression_null_for_sentinel(self):
        """Sentinel (MISSING/PNTA) values in dimensions produce null composite."""
        ctrl = CONTROLS["h_size_x_income"]
        df = pl.DataFrame(
            {
                "h_size": [1, 1],
                "h_income": [995, 999],  # MISSING, PNTA
            }
        ).cast({"h_size": pl.Int16, "h_income": pl.Int16})

        result = df.with_columns(ctrl.survey_expr().alias("xtab"))
        assert result["xtab"].null_count() == 2

    def test_distinct_combos_get_distinct_indices(self):
        """Every unique (size, income) pair maps to a unique index."""
        ctrl = CONTROLS["h_size_x_income"]
        combos = [
            (v1, v2)
            for v1, _ in ctrl.dim_controls[0].valid_members
            for v2, _ in ctrl.dim_controls[1].valid_members
        ]
        df = pl.DataFrame(
            {"h_size": [c[0] for c in combos], "h_income": [c[1] for c in combos]}
        ).cast({"h_size": pl.Int16, "h_income": pl.Int16})

        result = df.with_columns(ctrl.survey_expr().alias("xtab"))
        assert result["xtab"].null_count() == 0
        assert result["xtab"].n_unique() == len(combos)
        # the indices are the dense range over the valid members
        assert sorted(result["xtab"].to_list()) == list(range(len(ctrl.valid_members)))

    def test_merged_expression_maps_multiple_values(self):
        """Merged groups correctly map multiple original values to one index."""
        xtab = register_crosstab(
            "size_income_expr_test",
            ["h_size", "h_income"],
            merges=PREMERGE_GROUPS,
        )
        # All income values 1-4 should map to the same cell when paired
        # with the same size value
        df = pl.DataFrame(
            {
                "h_size": [1, 1, 1, 1],
                "h_income": [1, 2, 3, 4],  # All under 100
            }
        ).cast({"h_size": pl.Int16, "h_income": pl.Int16})

        result = df.with_columns(xtab.survey_expr().alias("xtab"))
        assert result["xtab"].null_count() == 0
        assert result["xtab"].n_unique() == 1  # All map to same cell

        # Total should be 9 valid indices
        assert len(xtab.valid_members) == 9


# ---------------------------------------------------------------------------
# Incidence pivot
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabIncidence:
    """Tests for cross-tab incidence table construction."""

    def test_incidence_crosstab_is_binary(self, households, persons):
        """Each household belongs to exactly one cross-tab cell: 0/1 indicators."""
        ctrl = CONTROLS["h_size_x_income"]
        targets = ["h_size", "h_income", "h_size_x_income"]
        hh_recoded = recode_survey_households(households, persons, targets)
        incidence = build_incidence_table(hh_recoded, persons, targets).incidence
        xtab_cols = [c for c in incidence.columns if c.startswith("h_size_x_income__")]

        # one column per composite member
        assert len(xtab_cols) == len(ctrl.valid_members)
        # Row sums should be 0 (null recode) or 1 (exactly one cell)
        row_sums = incidence.select(pl.sum_horizontal(*xtab_cols).alias("row_sum"))["row_sum"]
        assert (row_sums <= 1).all()

    def test_premerge_incidence_has_reduced_columns(self, households, persons):
        """Pre-merged cross-tab produces fewer incidence columns than full cartesian."""
        register_crosstab(
            "h_size_income_pre",
            ["h_size", "h_income"],
            merges=PREMERGE_GROUPS,
        )
        targets = ["h_size", "h_income", "h_size_income_pre"]
        hh_recoded = recode_survey_households(households, persons, targets)
        incidence = build_incidence_table(hh_recoded, persons, targets).incidence
        xtab_cols = [c for c in incidence.columns if c.startswith("h_size_income_pre__")]
        # 3 size bins x 3 income bins = 9 incidence columns
        assert len(xtab_cols) == 9


# ---------------------------------------------------------------------------
# Cross-tab merges
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabMerges:
    """Tests for N-D merge operations on cross-tab incidence columns."""

    def test_merge_preserves_row_sums(self, households, persons):
        """Merging doesn't change the total incidence per household."""
        targets = ["h_size", "h_income", "h_size_x_income"]
        hh_recoded = recode_survey_households(households, persons, targets)
        incidence = build_incidence_table(hh_recoded, persons, targets).incidence

        before_cols = [c for c in incidence.columns if c.startswith("h_size_x_income__")]
        before_sums = incidence.select(pl.sum_horizontal(*before_cols).alias("s"))["s"]

        merge_spec = MergeSpec(
            control="h_size_x_income",
            groups={"h_income": PREMERGE_GROUPS["h_income"]},
        )
        merged = apply_crosstab_merges(incidence, [merge_spec])
        after_cols = [c for c in merged.columns if c.startswith("h_size_x_income__")]
        after_sums = merged.select(pl.sum_horizontal(*after_cols).alias("s"))["s"]

        assert before_sums.to_list() == after_sums.to_list()

    def test_multi_dim_merge(self, households, persons):
        """Merging both dimensions simultaneously produces expected cell count."""
        targets = ["h_size", "h_income", "h_size_x_income"]
        hh_recoded = recode_survey_households(households, persons, targets)
        incidence = build_incidence_table(hh_recoded, persons, targets).incidence

        # Merge: 4 income -> 1 merged + 2 kept = 3 income bins
        # Merge: 8 size -> 1 merged + 2 kept = 3 size bins
        # Result: 3 x 3 = 9 cells
        merge_spec = MergeSpec(
            control="h_size_x_income",
            groups=PREMERGE_GROUPS,
        )
        merged = apply_crosstab_merges(incidence, [merge_spec])
        after_cols = [c for c in merged.columns if c.startswith("h_size_x_income__")]

        # 3 size bins (size_1, size_2, size_3_plus) x 3 income bins
        # (income_under_100, income_100to200, income_200_or_more) = 9
        assert len(after_cols) == 9

    def test_1d_merges_independent_of_crosstab(self, households, persons):
        """Global 1-D merges on h_size don't affect cross-tab columns."""
        targets = ["h_size", "h_income", "h_size_x_income"]
        hh_recoded = recode_survey_households(households, persons, targets)
        incidence = build_incidence_table(hh_recoded, persons, targets).incidence

        xtab_before = sorted(c for c in incidence.columns if c.startswith("h_size_x_income__"))

        # Apply a 1-D merge on h_size (should not touch cross-tab columns)
        merge_1d = MergeSpec(
            control="h_size",
            groups={
                "size_4_plus": [
                    "size_4",
                    "size_5",
                    "size_6",
                    "size_7",
                    "size_8",
                    "size_9",
                    "size_10_plus",
                ]
            },
        )
        merged = apply_1d_merges(incidence, [merge_1d])
        xtab_after = sorted(c for c in merged.columns if c.startswith("h_size_x_income__"))

        assert xtab_before == xtab_after


# ---------------------------------------------------------------------------
# PUMS-style aggregation
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabAggregation:
    """Tests for cross-tab control total aggregation."""

    def test_aggregate_includes_crosstab_totals(self, households, persons):
        """Aggregated control totals include cross-tab categories."""
        targets = ["h_size", "h_income", "h_size_x_income"]
        hh_recoded = recode_survey_households(households, persons, targets)
        incidence = build_incidence_table(hh_recoded, persons, targets).incidence

        # Add a weight column and geo column (simulate PUMS incidence)
        pums_inc = incidence.with_columns(
            pl.lit(1.0).alias("WGTP"),
            pl.col("hh_id").cast(pl.Utf8).alias("SERIALNO"),
        ).join(
            households.select("hh_id", "ctrl_geoid"),
            on="hh_id",
            how="left",
        )

        totals = aggregate_control_totals(
            pums_inc, targets, weight_col="WGTP", geo_col="ctrl_geoid"
        )
        xtab_rows = totals.totals.filter(pl.col("control_name") == "h_size_x_income")
        assert len(xtab_rows) > 0


# ---------------------------------------------------------------------------
# Sparsity warnings
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("size_income_xtab")
class TestCrosstabSparsity:
    """Tests for cross-tab cell sparsity warnings."""

    def test_no_warning_when_all_cells_above_threshold(self, caplog):
        """No warning when every cross-tab cell has enough records."""
        ctrl = CONTROLS["h_size_x_income"]
        prefix = f"{ctrl.name}__"
        cols = [
            f"{prefix}{m.name.lower()}"
            for m in ctrl.categories
            if m.name not in ("MISSING", "PNTA")
        ]
        data: dict[str, list] = {"ctrl_geoid": ["Z1"] * 50}
        for col in cols:
            data[col] = [1] * 50  # 50 records per cell
        seed = pl.DataFrame(data)

        with caplog.at_level("WARNING"):
            warn_crosstab_sparsity(seed, [ctrl], threshold=30)
        assert "sparse" not in caplog.text.lower()

    def test_warning_when_cells_below_threshold(self, caplog):
        """Warning logged when cross-tab cells have fewer than threshold records."""
        ctrl = CONTROLS["h_size_x_income"]
        prefix = f"{ctrl.name}__"
        members = [m for m in ctrl.categories if m.name not in ("MISSING", "PNTA")]
        cols = [f"{prefix}{m.name.lower()}" for m in members]

        data: dict[str, list] = {"ctrl_geoid": ["Z1"] * 5}
        for i, col in enumerate(cols):
            # First cell has only 5 records (sparse), rest have 0
            data[col] = [1 if i == 0 else 0] * 5
        seed = pl.DataFrame(data)

        with caplog.at_level("WARNING"):
            warn_crosstab_sparsity(seed, [ctrl], threshold=30)
        assert "sparse" in caplog.text.lower()
        assert ctrl.name in caplog.text

    def test_no_warning_for_1d_controls(self, caplog):
        """1-D controls are not checked by warn_crosstab_sparsity."""
        ctrl = CONTROLS["h_size"]
        data: dict[str, list] = {
            "ctrl_geoid": ["Z1"],
            "h_size__size_1": [1],
        }
        seed = pl.DataFrame(data)

        with caplog.at_level("WARNING"):
            warn_crosstab_sparsity(seed, [ctrl], threshold=30)
        assert "sparse" not in caplog.text.lower()


# ===========================================================================
# Control totals, aggregated from the recoded PUMS frames
# ===========================================================================


# ---------------------------------------------------------------------------
# build_control_totals
# ---------------------------------------------------------------------------
class TestBuildControlTotals:
    """Tests for building control totals from recoded PUMS data."""

    def test_basic_aggregation(self, pums_households, pums_persons):
        """Basic aggregation should produce totals DataFrame, over the PUMS geographies."""
        hh_recoded = recode_pums_households(pums_households, pums_persons)
        per_recoded = recode_pums_persons(pums_persons)

        controls = [ControlSpec(name="h_size")]
        result = build_control_totals(hh_recoded, per_recoded, controls)

        assert isinstance(result, ControlTotals)
        assert result.pums_hh_count == 3
        assert result.pums_person_count == 7
        assert "target_total" in result.totals.columns
        assert "control_name" in result.totals.columns
        assert set(result.geo_ids) == {"00100", "00200"}
        # without merges the categories stay the granular enum values
        assert all(isinstance(c, int) for c in result.totals["category"].to_list())

    def test_totals_sum_to_weight(self, pums_households, pums_persons):
        """Control totals should sum to total WGTP within each PUMA."""
        hh_recoded = recode_pums_households(pums_households, pums_persons)
        per_recoded = recode_pums_persons(pums_persons)

        controls = [ControlSpec(name="h_size")]
        result = build_control_totals(hh_recoded, per_recoded, controls)

        # Sum of totals per geo should equal sum of WGTP for that geo
        total_by_geo = result.totals.group_by("geo_id").agg(pl.col("target_total").sum())
        # PUMA 00100: WGTP 100 + 200 = 300
        # PUMA 00200: WGTP 150
        puma_100 = total_by_geo.filter(pl.col("geo_id") == "00100")["target_total"][0]
        puma_200 = total_by_geo.filter(pl.col("geo_id") == "00200")["target_total"][0]
        assert puma_100 == pytest.approx(300.0)
        assert puma_200 == pytest.approx(150.0)

    def test_multiple_controls(self, pums_households, pums_persons):
        """Household and person controls land side by side, each on its own weights."""
        hh_recoded = recode_pums_households(pums_households, pums_persons)
        per_recoded = recode_pums_persons(pums_persons)

        controls = [ControlSpec(name="h_size"), ControlSpec(name="p_gender")]
        result = build_control_totals(hh_recoded, per_recoded, controls)

        control_names = result.totals["control_name"].unique().sort().to_list()
        assert control_names == ["h_size", "p_gender"]
        # person controls are expanded on PWGTP: 100+100 + 200*4 + 150 = 1150
        person_total = result.totals.filter(pl.col("control_name") == "p_gender")[
            "target_total"
        ].sum()
        assert person_total == pytest.approx(1150.0)

    @pytest.mark.parametrize(
        ("controls", "match"),
        [
            pytest.param([ControlSpec(name="bogus")], "Unknown control name", id="unknown_name"),
            pytest.param([], "No controls specified", id="no_controls"),
        ],
    )
    def test_bad_control_list_raises(self, pums_households, pums_persons, controls, match):
        """A name the registry does not know, and an empty list, are both refused."""
        hh_recoded = recode_pums_households(pums_households, pums_persons)
        per_recoded = recode_pums_persons(pums_persons)

        with pytest.raises(ValueError, match=match):
            build_control_totals(hh_recoded, per_recoded, controls)


# ---------------------------------------------------------------------------
# Zone grouping
# ---------------------------------------------------------------------------
class TestApplyZoneGroups:
    """Tests for apply_zone_groups."""

    @pytest.fixture
    def control_totals(self) -> ControlTotals:
        """Minimal ControlTotals fixture with 3 zones and 2 categories each."""
        totals = pl.DataFrame(
            {
                "geo_id": ["A", "A", "B", "B", "C", "C"],
                "control_name": ["ctrl"] * 6,
                "category": [1, 2, 1, 2, 1, 2],
                "target_total": [100.0, 200.0, 30.0, 40.0, 50.0, 60.0],
            }
        )
        return ControlTotals(
            totals=totals, pums_hh_count=10, pums_person_count=20, geo_ids=["A", "B", "C"]
        )

    @pytest.fixture
    def seed(self) -> pl.DataFrame:
        """Minimal seed DataFrame with hh_id and ctrl_geoid matching control_totals."""
        return pl.DataFrame(
            {
                "hh_id": [1, 2, 3],
                "ctrl_geoid": ["A", "B", "C"],
            }
        )

    def test_merges_zones(self, control_totals, seed):
        """Grouped zones should merge in both totals and seed."""
        groups = {"AB": ["A", "B"]}
        new_ct, new_seed = apply_zone_groups(control_totals, seed, groups)

        new_ids = sorted(new_ct.geo_ids)
        assert new_ids == ["AB", "C"]

        # Targets should sum
        ab_cat1 = new_ct.totals.filter((pl.col("geo_id") == "AB") & (pl.col("category") == 1))[
            "target_total"
        ].item()
        assert ab_cat1 == pytest.approx(130.0)  # 100 + 30

        # Seed geo_id remapped
        assert new_seed.filter(pl.col("hh_id") == 1)["ctrl_geoid"].item() == "AB"
        assert new_seed.filter(pl.col("hh_id") == 2)["ctrl_geoid"].item() == "AB"
        assert new_seed.filter(pl.col("hh_id") == 3)["ctrl_geoid"].item() == "C"

        # Original zone IDs preserved
        assert "_orig_ctrl_geoid" in new_seed.columns
        assert new_seed.filter(pl.col("hh_id") == 1)["_orig_ctrl_geoid"].item() == "A"
        assert new_seed.filter(pl.col("hh_id") == 2)["_orig_ctrl_geoid"].item() == "B"
        assert new_seed.filter(pl.col("hh_id") == 3)["_orig_ctrl_geoid"].item() == "C"

        # Zone group labels
        assert "zone_group" in new_seed.columns
        assert new_seed.filter(pl.col("hh_id") == 1)["zone_group"].item() == "AB"
        assert new_seed.filter(pl.col("hh_id") == 2)["zone_group"].item() == "AB"
        assert new_seed.filter(pl.col("hh_id") == 3)["zone_group"].item() is None

        # Zone C is in no group, so its target is untouched
        c_total = new_ct.totals.filter((pl.col("geo_id") == "C") & (pl.col("category") == 2))[
            "target_total"
        ].item()
        assert c_total == pytest.approx(60.0)

        # and the PUMS counts are carried forward unchanged
        assert new_ct.pums_hh_count == 10
        assert new_ct.pums_person_count == 20

    def test_duplicate_zone_raises(self, control_totals, seed):
        """A zone in two groups should raise ValueError."""
        groups = {"AB": ["A", "B"], "AC": ["A", "C"]}
        with pytest.raises(ValueError, match="multiple groups"):
            apply_zone_groups(control_totals, seed, groups)


# ===========================================================================
# Zone-targeted 1-D merges, and the labels the fit report reads off them
# ===========================================================================


# ---------------------------------------------------------------------------
# ControlRegistryConfig.from_yaml — zone_merges config key
# ---------------------------------------------------------------------------
class TestParseControlsZoneMerges:
    """ControlRegistryConfig.from_yaml should emit MergeSpecs for zone_merges entries."""

    def test_no_zone_merges(self):
        """Controls without zone_merges produce only global MergeSpecs."""
        controls = [
            {
                "name": "h_size",
                "merge": {"size_5_plus": ["size_5", "size_6"]},
            },
        ]
        cfg = ControlRegistryConfig.from_yaml(controls)
        assert len(cfg.merges_1d) == 1
        assert cfg.merges_1d[0].zones is None

    def test_zone_merges_appended(self):
        """zone_merges produce zone-specific MergeSpecs after the global one."""
        controls = [
            {
                "name": "h_size",
                "merge": {"size_5_plus": ["size_5", "size_6"]},
                "zone_merges": {
                    "Z1": {"size_4_plus": ["size_4", "size_5_plus"]},
                    "Z2": {"size_3_plus": ["size_3", "size_5_plus"]},
                },
            },
        ]
        cfg = ControlRegistryConfig.from_yaml(controls)
        merges = cfg.merges_1d
        assert len(merges) == 3
        # Global first
        assert merges[0].zones is None
        assert "size_5_plus" in merges[0].groups
        # Then one MergeSpec per zone key, in config order
        assert [m.zones for m in merges[1:]] == [["Z1"], ["Z2"]]
        assert "size_4_plus" in merges[1].groups
        assert "size_3_plus" in merges[2].groups

    def test_zone_merges_without_global_merge(self):
        """A control with only zone_merges (no global merge) still works."""
        controls = [
            {
                "name": "h_size",
                "zone_merges": {
                    "Z1": {"size_5_plus": ["size_5", "size_6"]},
                },
            },
        ]
        cfg = ControlRegistryConfig.from_yaml(controls)
        merges = cfg.merges_1d
        assert len(merges) == 1
        assert merges[0].zones == ["Z1"]
        assert merges[0].control == "h_size"


# ---------------------------------------------------------------------------
# apply_category_merges — targeted merge layered on global merge
# ---------------------------------------------------------------------------
def _make_arrays(labels, targets_list):
    """Helper: build incidence, targets, importance arrays from row labels."""
    n_hh = 3
    incidence = np.ones((len(labels), n_hh), dtype=np.float64)
    # Give each row distinct values so we can verify summation
    for i in range(len(labels)):
        incidence[i] = np.array([i + 1, i + 2, i + 3], dtype=np.float64)
    targets = np.array(targets_list, dtype=np.float64)
    importance = np.ones(len(labels), dtype=np.float64)
    master_idx = next(i for i, (c, _) in enumerate(labels) if c == "h_total")
    return incidence, targets, master_idx, importance


class TestApplyMergesTargeted:
    """Targeted merges should be able to reference labels produced by global merges."""

    def test_global_then_targeted_merge(self):
        """A targeted merge can reference a label created by a preceding global merge."""
        # Row labels: h_total + h_size with 4 categories
        labels = [
            ("h_total", "total"),
            ("h_size", "size_3"),
            ("h_size", "size_4"),
            ("h_size", "size_5"),
            ("h_size", "size_6"),
        ]
        targets = [100.0, 30.0, 25.0, 25.0, 20.0]
        incidence, tgt, master_idx, imp = _make_arrays(labels, targets)

        # Global: merge 5+6 → size_5_plus
        global_merge = MergeSpec(
            control="h_size",
            groups={"size_5_plus": ["size_5", "size_6"]},
            zones=None,
        )
        # Targeted: further merge 4 + size_5_plus → size_4_plus (zone Z1 only)
        targeted_merge = MergeSpec(
            control="h_size",
            groups={"size_4_plus": ["size_4", "size_5_plus"]},
            zones=["Z1"],
        )

        # Apply both in order (global first)
        incidence, tgt, master_idx, imp = apply_category_merges(
            incidence, tgt, labels, master_idx, [global_merge, targeted_merge], imp
        )

        # After global: size_5,size_6→size_5_plus  (target=25+20=45)
        # After targeted: size_4+size_5_plus→size_4_plus  (target=25+45=70)
        remaining_members = {member for _, member in labels}
        assert "size_5" not in remaining_members
        assert "size_6" not in remaining_members
        assert "size_4" not in remaining_members
        assert "size_5_plus" not in remaining_members
        assert "size_4_plus" in remaining_members
        assert "size_3" in remaining_members

        # Find the merged target
        idx_4_plus = next(i for i, (c, m) in enumerate(labels) if m == "size_4_plus")
        assert tgt[idx_4_plus] == pytest.approx(70.0)

    def test_targeted_merge_unknown_label_raises(self):
        """Referencing a label that doesn't exist should raise ValueError."""
        labels = [
            ("h_total", "total"),
            ("h_size", "size_3"),
            ("h_size", "size_4"),
        ]
        targets = [100.0, 50.0, 50.0]
        incidence, tgt, master_idx, imp = _make_arrays(labels, targets)

        bad_merge = MergeSpec(
            control="h_size",
            groups={"merged": ["size_4", "nonexistent"]},
            zones=["Z1"],
        )

        with pytest.raises(ValueError, match="unknown categories"):
            apply_category_merges(incidence, tgt, labels, master_idx, [bad_merge], imp)


# ---------------------------------------------------------------------------
# apply_fit_merges — diagnostics table labelling
# ---------------------------------------------------------------------------


def _make_fit(zones: list[str], categories: list[str], control: str = "h_size") -> pl.DataFrame:
    """Build a minimal fit DataFrame with string categories for testing."""
    rows = []
    for zi, z in enumerate(zones):
        for ci, cat in enumerate(categories):
            # deterministic, and distinct per (zone, category)
            t = float(10 + 10 * ci + zi)
            w = t + 1.0  # small diff
            rows.append(
                {
                    "geo_id": z,
                    "control_name": control,
                    "category": cat,
                    "target_total": t,
                    "weighted_total": w,
                    "diff": w - t,
                    "diff_pct": (w - t) / t * 100 if t else 0.0,
                }
            )
    return pl.DataFrame(rows)


class TestApplyFitMergesLabel:
    """apply_fit_merges should add a label column from enum definitions."""

    def test_adds_label_column(self):
        """A label column should be added based on the control and category."""
        fit = _make_fit(["Z1"], ["size_1", "size_2", "size_3"])
        result = apply_fit_merges(fit, None, ["h_size"])
        assert "label" in result.columns
        labels = result["label"].to_list()
        assert "Size 1" in labels
        assert "Size 2" in labels

    def test_merged_category_gets_label(self):
        """A merged category string gets a title-cased label from the merge spec."""
        fit = _make_fit(["Z1"], ["size_3", "size_5_plus"])
        merge = MergeSpec(
            control="h_size",
            groups={"size_5_plus": ["size_5", "size_6"]},
            zones=None,
        )
        result = apply_fit_merges(fit, [merge], ["h_size"])
        labels = result["label"].to_list()
        assert "Size 5 Plus" in labels
        assert "Size 3" in labels


class TestApplyFitMergesZoneAware:
    """Zone merge labels should appear correctly in the fit table."""

    def test_zone_merge_labels_present(self):
        """Zone-specific merged categories get proper labels."""
        # Simulate a fit table where zone Z1 has the merged category
        # and zone Z2 has the originals (this is how the data arrives
        # after apply_zone_merges + merge_control_totals upstream).
        z1_data = _make_fit(["Z1"], ["size_3", "size_4_plus"])
        z2_data = _make_fit(["Z2"], ["size_3", "size_4", "size_5_plus"])
        fit = pl.concat([z1_data, z2_data])

        global_merge = MergeSpec(
            control="h_size",
            groups={"size_5_plus": ["size_5", "size_6"]},
            zones=None,
        )
        zone_merge = MergeSpec(
            control="h_size",
            groups={"size_4_plus": ["size_4", "size_5_plus"]},
            zones=["Z1"],
        )
        result = apply_fit_merges(fit, [global_merge, zone_merge], ["h_size"])

        # Z1 should have the merged label
        z1 = result.filter((pl.col("geo_id") == "Z1") & pl.col("target_total").is_not_null())
        z1_labels = z1["label"].to_list()
        assert "Size 4 Plus" in z1_labels
        assert "Size 3" in z1_labels

        # Z2 should have the original labels
        z2 = result.filter((pl.col("geo_id") == "Z2") & pl.col("target_total").is_not_null())
        z2_labels = z2["label"].to_list()
        assert "Size 4" in z2_labels
        assert "Size 5 Plus" in z2_labels

        # and the merged row keeps the target total the upstream merge gave it
        z1_merged = result.filter((pl.col("geo_id") == "Z1") & (pl.col("label") == "Size 4 Plus"))
        assert z1_merged.height == 1
        assert z1_merged["target_total"].item() is not None

    def test_null_placeholders_for_consistency(self):
        """Every zone should have every label (real or null placeholder)."""
        # Simulate post-merge data: Z1 has merged, Z2 has originals
        z1_data = _make_fit(["Z1"], ["size_3", "size_4_plus"])
        z2_data = _make_fit(["Z2"], ["size_3", "size_4", "size_5_plus"])
        fit = pl.concat([z1_data, z2_data])

        global_merge = MergeSpec(
            control="h_size",
            groups={"size_5_plus": ["size_5", "size_6"]},
            zones=None,
        )
        zone_merge = MergeSpec(
            control="h_size",
            groups={"size_4_plus": ["size_4", "size_5_plus"]},
            zones=["Z1"],
        )
        result = apply_fit_merges(fit, [global_merge, zone_merge], ["h_size"])

        all_labels = result["label"].unique().sort().to_list()
        for z in ["Z1", "Z2"]:
            zone_labels = result.filter(pl.col("geo_id") == z)["label"].unique().sort().to_list()
            assert zone_labels == all_labels, f"Zone {z} missing some labels"


# ===========================================================================
# Checksums: the incidence table against its own structural totals
# ===========================================================================


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def good_seed() -> pl.DataFrame:
    """Seed table where all controls are properly accounted for.

    3 households:
      HH1: 1 person (male, employed_full)
      HH2: 3 persons (1 male + 2 female, 1 employed_full + 1 employed_part + 1 not_employed)
      HH3: 2 persons (1 male + 1 female, 2 not_employed)
    """
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 3],
            "p_total": [1, 3, 2],
            "h_income__low": [1, 0, 0],
            "h_income__mid": [0, 1, 0],
            "h_income__high": [0, 0, 1],
            "p_gender__male": [1, 1, 1],
            "p_gender__female": [0, 2, 1],
            "p_employment__employed_full": [1, 1, 0],
            "p_employment__employed_part": [0, 1, 0],
            "p_employment__not_employed": [0, 1, 2],
        },
    )


@pytest.fixture
def overcount_seed() -> pl.DataFrame:
    """Seed where p_gender incidence sums > p_total for HH3 (double-classified)."""
    return pl.DataFrame(
        {
            "hh_id": [1, 2, 3],
            "p_total": [1, 3, 2],
            "h_income__low": [1, 0, 0],
            "h_income__mid": [0, 1, 0],
            "h_income__high": [0, 0, 1],
            # HH3: 3 gender incidence but only 2 persons (double-classified)
            "p_gender__male": [1, 1, 2],
            "p_gender__female": [0, 2, 1],
            "p_employment__employed_full": [1, 1, 0],
            "p_employment__employed_part": [0, 1, 0],
            "p_employment__not_employed": [0, 1, 2],
        },
    )


# ---------------------------------------------------------------------------
# check_incidence_sums
# ---------------------------------------------------------------------------
class TestCheckIncidenceSums:
    """Tests for incidence-sum validation on the seed table."""

    @pytest.mark.parametrize(
        ("seed_name", "targets"),
        [
            pytest.param("good", ["h_income", "p_gender", "p_employment"], id="sums_all_match"),
            pytest.param("good", ["bogus_control"], id="unknown_target_skipped"),
            pytest.param("no_incidence", ["p_gender"], id="no_incidence_columns_skipped"),
        ],
    )
    def test_passes_without_a_failure(self, good_seed, seed_name, targets, caplog):
        """Matching sums, unknown names and absent columns all pass quietly."""
        seed = good_seed if seed_name == "good" else pl.DataFrame({"hh_id": [1], "p_total": [1]})
        with caplog.at_level(logging.INFO):
            check_incidence_sums(seed, targets, source_label="test")
        assert "all controls pass" in caplog.text
        assert "failure" not in caplog.text

    def test_undercount_raises(self, good_seed):
        """Undercount (< p_total) now raises — nulls should be filled."""
        seed = good_seed.with_columns(
            # HH2 employment sums to 2 instead of 3 (undercount)
            pl.when(pl.col("hh_id") == 2)
            .then(0)
            .otherwise(pl.col("p_employment__employed_part"))
            .alias("p_employment__employed_part")
        )
        with pytest.raises(ValueError, match="p_employment"):
            check_incidence_sums(seed, ["p_employment"], source_label="test")

    def test_fails_on_person_overcount(self, overcount_seed):
        """Overcount in person control should raise ValueError."""
        with pytest.raises(ValueError, match="p_gender"):
            check_incidence_sums(overcount_seed, ["p_gender"], source_label="test")

    def test_fails_on_hh_control_mismatch(self):
        """HH-level control with wrong sum raises ValueError (no h_total needed)."""
        seed = pl.DataFrame(
            {
                "hh_id": [1, 2],
                "h_income__low": [1, 0],
                "h_income__mid": [0, 0],  # HH2 sums to 0 instead of 1
                "h_income__high": [0, 0],
            },
        )
        with pytest.raises(ValueError, match="h_income"):
            check_incidence_sums(seed, ["h_income"], source_label="test")

    def test_tolerance_allows_fractional_drift(self, good_seed):
        """Small floating-point deviation passes with tolerance."""
        seed = good_seed.with_columns(
            (pl.col("p_gender__male").cast(pl.Float64) + 0.005).alias("p_gender__male")
        )
        # Fails with zero tolerance
        with pytest.raises(ValueError, match="p_gender"):
            check_incidence_sums(seed, ["p_gender"], source_label="test")
        # Passes with tolerance
        check_incidence_sums(seed, ["p_gender"], source_label="test", tolerance=0.01)

    def test_error_shows_hh_id_and_counts(self, overcount_seed):
        """The error names the source, the one bad household, and both counts.

        Only HH3 is double-classified: its two gender columns sum to 3
        against a ``p_total`` of 2.  HH1 and HH2 must not appear.
        """
        with pytest.raises(ValueError, match="p_gender") as exc_info:
            check_incidence_sums(overcount_seed, ["p_gender"], source_label="survey")
        msg = str(exc_info.value)

        assert "Incidence sum failures (survey):" in msg
        # one row: hh_id 3, control p_gender, actual 3.00, expected 2.00
        assert re.search(r"^\s+3\s+p_gender\s+3\.00\s+2\.00\s*$", msg, re.MULTILINE)
        assert "1 total failure(s)." in msg
        reported_hh_ids = re.findall(r"^\s+(\d+)\s+p_gender\s", msg, re.MULTILINE)
        assert reported_hh_ids == ["3"]

    def test_warns_when_p_total_missing(self, caplog):
        """Person-control check warns and skips when p_total is absent."""
        seed = pl.DataFrame(
            {
                "hh_id": [1],
                "p_gender__male": [1],
                "p_gender__female": [0],
            },
        )
        with caplog.at_level(logging.WARNING):
            check_incidence_sums(seed, ["p_gender"], source_label="test")
        assert "p_total column missing" in caplog.text


# ---------------------------------------------------------------------------
# check_recode_nulls
# ---------------------------------------------------------------------------
class TestCheckRecodeNulls:
    """Tests for null-detection on recoded DataFrames (pre-aggregation)."""

    @pytest.mark.parametrize(
        ("df", "targets"),
        [
            pytest.param(
                pl.DataFrame({"person_id": [1, 2], "p_gender": [1, 2]}),
                ["p_gender"],
                id="every_record_classified",
            ),
            pytest.param(
                pl.DataFrame({"person_id": [1], "p_gender": [1]}),
                ["bogus_control"],
                id="unknown_target_skipped",
            ),
            pytest.param(
                pl.DataFrame({"person_id": [1]}),
                ["p_gender"],
                id="absent_column_skipped",
            ),
        ],
    )
    def test_passes_when_all_classified(self, df, targets, caplog):
        """Full classification, unknown names and absent columns all pass quietly."""
        with caplog.at_level(logging.INFO):
            check_recode_nulls(
                df,
                targets,
                level=ControlLevel.PERSON,
                id_col="person_id",
                source_label="test",
            )
        assert "all records classified" in caplog.text
        assert "Null recode values" not in caplog.text

    @pytest.mark.parametrize(
        ("df", "targets", "level", "id_col", "null_id"),
        [
            pytest.param(
                pl.DataFrame({"person_id": [1, 2, 3], "p_gender": [1, None, 2]}),
                ["p_gender"],
                ControlLevel.PERSON,
                "person_id",
                "2",
                id="person_control",
            ),
            pytest.param(
                pl.DataFrame({"hh_id": [10, 20], "h_workers": [1, None]}),
                ["h_workers"],
                ControlLevel.HOUSEHOLD,
                "hh_id",
                "20",
                id="household_control",
            ),
        ],
    )
    def test_warns_on_null_control(self, df, targets, level, id_col, null_id, caplog):
        """A null recode warns and names the record; it never raises."""
        with caplog.at_level(logging.WARNING):
            check_recode_nulls(
                df,
                targets,
                level=level,
                id_col=id_col,
                source_label="survey",
            )
        assert "Null recode values" in caplog.text
        assert re.search(rf"^\s+{null_id}\s+{targets[0]}\s*$", caplog.text, re.MULTILINE)

    def test_filters_by_level(self, caplog):
        """Only controls matching the requested level are checked."""
        df = pl.DataFrame({"hh_id": [1], "h_workers": [None], "p_gender": [None]})
        with caplog.at_level(logging.INFO):
            check_recode_nulls(
                df,
                ["h_workers", "p_gender"],
                level=ControlLevel.HOUSEHOLD,
                id_col="hh_id",
                source_label="test",
            )
        # h_workers is HH → should be flagged
        assert "h_workers" in caplog.text
        # p_gender is person → should NOT appear (wrong level)
        assert "p_gender" not in caplog.text
