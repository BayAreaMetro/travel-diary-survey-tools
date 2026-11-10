"""What the balancer is given, and how hard it is told to pull.

Three inputs, read together because each shapes the same fit. Base weights are
the starting expansion factors, from a zone's response rate or from an explicit
sample plan's strata. Importance says how hard to chase each control, derived
from the MOE its PUMS replicate weights imply. The expansion-factor grid search
then trades fit error against weight spread, and reports what each choice costs.
"""

from enum import IntEnum

import numpy as np
import plotly.graph_objects as go
import polars as pl
import pytest

from processing.weighting.balancing.balancer import (
    _prepare_zone,
    balance_weights,
    grid_search_expansion_factor,
)
from processing.weighting.balancing.base_weights import (
    SamplePlan,
    compute_base_weights,
    grow_block_households,
    load_sample_plan,
)
from processing.weighting.balancing.importance import (
    DEFAULT_IMPORTANCE,
    _control_cell_moe,
    _control_cv,
    _normalize_cvs,
    compute_control_moe,
    compute_moe_importance,
)
from processing.weighting.controls.base import ControlLevel, ControlTarget
from processing.weighting.controls.registry import CONTROLS, register_crosstab
from processing.weighting.core.specs import (
    BalancingConfig,
    ControlTotals,
    GridPoint,
)
from processing.weighting.diagnostics.charts import ef_tradeoff_figure

# The balancer settings these tests run under: the values the code used to default to.
_BALANCING = BalancingConfig(
    max_expansion_factor=10.0,
    min_expansion_factor=0.1,
    max_base_weight_gap=0.005,
    min_weight=None,
    max_iterations=10_000,
    n_workers=1,
    moe_based_importance=False,
    default_importance=100.0,
    min_survey_share_ratio=0.25,
)


def _cv(ctrl: ControlTarget, hh: pl.DataFrame, person: pl.DataFrame) -> float | None:
    """One control's CV, from its cells as ``compute_control_moe`` builds them."""
    cells = _control_cell_moe(ctrl, hh, person)
    return None if cells is None else _control_cv(cells)


def _moe_importance(hh: pl.DataFrame, person: pl.DataFrame, names: list[str]) -> dict[str, float]:
    """Importance per control, through the per-cell MOE as the pipeline runs it."""
    return compute_moe_importance(compute_control_moe(hh, person, names), names)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_control_totals(
    zone_data: dict[str, list[tuple[str, int, float]]],
) -> ControlTotals:
    """Build a minimal ControlTotals from {zone_group_id: [(ctrl_name, cat, total)]}.

    ``zone_data`` maps zone_group_id strings to lists of (control_name, category,
    target_total) tuples, which become rows of the tidy totals frame.
    """
    rows: list[dict] = []
    for zone_group_id, entries in zone_data.items():
        for ctrl_name, cat, total in entries:
            rows.append(
                {
                    "zone_group_id": zone_group_id,
                    "control_name": ctrl_name,
                    "category": cat,
                    "target_total": total,
                }
            )
    totals = pl.DataFrame(rows).cast(
        {"zone_group_id": pl.Utf8, "category": pl.Int64, "target_total": pl.Float64}
    )
    return ControlTotals(
        totals=totals,
        pums_hh_count=0,
        pums_person_count=0,
        zone_group_ids=sorted(zone_data),
    )


def _make_seed(geo_col: str, zone_hh: dict[str, int]) -> pl.DataFrame:
    """Build a minimal seed table with hh_id and geo_col."""
    rows: list[dict] = []
    hh_id = 1
    for zone_group_id, n in zone_hh.items():
        for _ in range(n):
            rows.append({"hh_id": hh_id, geo_col: zone_group_id})
            hh_id += 1
    return pl.DataFrame(rows).cast({"hh_id": pl.Int64, geo_col: pl.Utf8})


def _make_seed_with_bg(geo_col: str, bg_hh: dict[str, tuple[str, int]]) -> pl.DataFrame:
    """Build a seed table with hh_id, geo_col, and bg_geo_id.

    *bg_hh* maps ``bg_geo_id → (zone_group_id, n_households)``.
    """
    rows: list[dict] = []
    hh_id = 1
    for bg_id, (zone_id, n) in bg_hh.items():
        for _ in range(n):
            rows.append({"hh_id": hh_id, geo_col: zone_id, "bg_geo_id": bg_id})
            hh_id += 1
    return pl.DataFrame(rows).cast({"hh_id": pl.Int64, geo_col: pl.Utf8, "bg_geo_id": pl.Utf8})


# ---------------------------------------------------------------------------
# Response inversion tests
# ---------------------------------------------------------------------------
class TestResponseInversion:
    """Default strategy: target_hh_pop / n_responses per zone."""

    def test_multiple_zones(self):
        """Each zone gets its own ratio, and the seed's own columns survive."""
        ct = _make_control_totals(
            {
                "A": [("h_size", 1, 5_000.0), ("h_size", 2, 5_000.0)],
                "B": [("h_size", 1, 80_000.0), ("h_size", 2, 20_000.0)],
            }
        )
        seed = _make_seed("zone_group_id", {"A": 50, "B": 200}).with_columns(
            pl.lit(42).alias("extra_col")
        )
        result = compute_base_weights(seed, ct, ["h_size"])

        zone_a = result.filter(pl.col("zone_group_id") == "A")
        zone_b = result.filter(pl.col("zone_group_id") == "B")

        # A: 10_000/50 = 200, B: 100_000/200 = 500
        assert zone_a["base_weight"].to_list() == [pytest.approx(200.0)] * 50
        assert zone_b["base_weight"].to_list() == [pytest.approx(500.0)] * 200
        assert result["base_weight"].dtype == pl.Float64
        assert result["extra_col"].to_list() == [42] * 250
        assert sorted(result["hh_id"].to_list()) == list(range(1, 251))

    def test_uses_first_hh_control(self):
        """When multiple controls present, uses first HH-level for total."""
        ct = _make_control_totals(
            {
                "Z1": [
                    ("h_size", 1, 60_000.0),
                    ("h_size", 2, 40_000.0),
                    ("h_income", 1, 50_000.0),
                    ("h_income", 2, 50_000.0),
                ],
            }
        )
        seed = _make_seed("zone_group_id", {"Z1": 100})
        # h_size is first HH control → total 100_000, base_weight = 1000
        result = compute_base_weights(seed, ct, ["h_size", "h_income"])
        assert result["base_weight"][0] == pytest.approx(1_000.0)

    def test_no_hh_controls_raises(self):
        """Fail if no household-level control in targets."""
        ct = _make_control_totals({"Z1": [("p_age", 1, 1_000.0)]})
        seed = _make_seed("zone_group_id", {"Z1": 10})
        with pytest.raises(ValueError, match="no household-level control"):
            compute_base_weights(seed, ct, ["p_age"])


# ---------------------------------------------------------------------------
# Sample plan tests (block-group level)
# ---------------------------------------------------------------------------
class TestSamplePlan:
    """Explicit sample plan: segments of block groups, split by zone group."""

    @staticmethod
    def _plan(segments: dict[str, str]) -> SamplePlan:
        """A plan from ``{bg_geo_id: sample_segment}``."""
        return SamplePlan(
            strata=pl.DataFrame(
                {"bg_geo_id": list(segments), "sample_segment": list(segments.values())}
            )
        )

    @staticmethod
    def _blocks(blocks: dict[str, tuple[str, float]]) -> pl.DataFrame:
        """Block households from ``{block_id: (zone_group_id, households)}``."""
        return pl.DataFrame(
            {
                "block_id": list(blocks),
                "zone_group_id": [z for z, _ in blocks.values()],
                "households": [float(h) for _, h in blocks.values()],
            }
        )

    @staticmethod
    def _weights(seed, plan, blocks, zones=("Z1",)) -> pl.DataFrame:
        ct = _make_control_totals({z: [("h_size", 1, 999.0)] for z in zones})
        return compute_base_weights(seed, ct, ["h_size"], sample_plan=plan, block_households=blocks)

    def test_single_segment_all_bgs(self):
        """All BGs in one segment and zone → weight = households / responses."""
        plan = self._plan({"060010001001": "seg_a", "060010001002": "seg_a"})
        blocks = self._blocks(
            {"060010001001000": ("Z1", 60_000), "060010001002000": ("Z1", 40_000)}
        )
        # 10 + 20 = 30 responses in one cell, households = 100k → weight = 100k/30
        seed = _make_seed_with_bg(
            "zone_group_id",
            {"060010001001": ("Z1", 10), "060010001002": ("Z1", 20)},
        )
        result = self._weights(seed, plan, blocks)
        assert result["base_weight"].to_list() == [pytest.approx(100_000.0 / 30)] * 30

    def test_blocks_add_up_to_their_block_group(self):
        """Every block in a planned block group counts toward its cell."""
        plan = self._plan({"060010001001": "seg_a"})
        blocks = self._blocks(
            {"060010001001000": ("Z1", 30_000), "060010001001001": ("Z1", 70_000)}
        )
        seed = _make_seed_with_bg("zone_group_id", {"060010001001": ("Z1", 100)})
        result = self._weights(seed, plan, blocks)
        assert result["base_weight"][0] == pytest.approx(1_000.0)

    def test_multiple_segments(self):
        """A segment spanning two block groups pools them, and only itself.

        The two segments are deliberately unequal, so pooling every response
        together (180 000 / 140 = 1285.7) fails both assertions.
        """
        plan = self._plan(
            {"060010001001": "urban", "060130001001": "rural", "060130001002": "rural"}
        )
        blocks = self._blocks(
            {
                "060010001001000": ("Z1", 100_000),
                "060130001001000": ("Z2", 50_000),
                "060130001002000": ("Z2", 30_000),
            }
        )
        # urban: 100 HHs, households = 100k → 1000
        # rural: 25 + 15 = 40 HHs, households = 50k + 30k = 80k → 2000
        seed = _make_seed_with_bg(
            "zone_group_id",
            {
                "060010001001": ("Z1", 100),
                "060130001001": ("Z2", 25),
                "060130001002": ("Z2", 15),
            },
        )
        result = self._weights(seed, plan, blocks, zones=("Z1", "Z2"))
        z1 = result.filter(pl.col("zone_group_id") == "Z1")
        z2 = result.filter(pl.col("zone_group_id") == "Z2")
        assert z1["base_weight"].to_list() == [pytest.approx(1_000.0)] * 100
        # both rural block groups share the segment's single ratio
        assert z2["base_weight"].to_list() == [pytest.approx(2_000.0)] * 40

    def test_a_segment_crossing_zones_is_split_by_zone(self):
        """Each zone's base weights add up to its own households, not its share of responses.

        Pooled, the segment's 100k over 100 responses gives every home 1,000, so the
        zone with 95 responses would carry 95k against its 80k households.
        """
        plan = self._plan({"060010001001": "seg", "060130001001": "seg"})
        blocks = self._blocks(
            {"060010001001000": ("Z1", 80_000), "060130001001000": ("Z2", 20_000)}
        )
        seed = _make_seed_with_bg(
            "zone_group_id", {"060010001001": ("Z1", 95), "060130001001": ("Z2", 5)}
        )
        result = self._weights(seed, plan, blocks, zones=("Z1", "Z2"))
        sums = dict(result.group_by("zone_group_id").agg(pl.col("base_weight").sum()).iter_rows())
        assert sums == {"Z1": pytest.approx(80_000.0), "Z2": pytest.approx(20_000.0)}

    def test_cell_with_zero_responses_raises(self):
        """If a segment has households in a zone but no responses there, fail loud."""
        plan = self._plan({"060010001001": "has_data", "060750001001": "empty"})
        blocks = self._blocks(
            {"060010001001000": ("Z1", 100_000), "060750001001000": ("Z1", 50_000)}
        )
        # Only BG1 has responses; BG2 (segment "empty") has none
        seed = _make_seed_with_bg("zone_group_id", {"060010001001": ("Z1", 10)})
        with pytest.raises(ValueError, match="zero survey responses"):
            self._weights(seed, plan, blocks)

    def test_responses_without_households_raise(self):
        """Responses in a cell the Census gives no households cannot be weighted."""
        plan = self._plan({"060010001001": "seg"})
        blocks = self._blocks({"060010001001000": ("Z1", 0)})
        seed = _make_seed_with_bg("zone_group_id", {"060010001001": ("Z1", 10)})
        with pytest.raises(ValueError, match="responses but no Census households"):
            self._weights(seed, plan, blocks)

    def test_sample_plan_missing_column_raises(self):
        """SamplePlan validates required columns on construction."""
        with pytest.raises(ValueError, match="missing required columns"):
            SamplePlan(strata=pl.DataFrame({"zone_group_id": ["Z1"], "target_population": [100]}))

    def test_missing_block_households_raises(self):
        """sample_plan without block_households should raise."""
        plan = self._plan({"060010001001": "seg_a"})
        ct = _make_control_totals({"Z1": [("h_size", 1, 999.0)]})
        seed = _make_seed_with_bg("zone_group_id", {"060010001001": ("Z1", 10)})
        with pytest.raises(ValueError, match="block_households is required"):
            compute_base_weights(
                seed,
                ct,
                ["h_size"],
                sample_plan=plan,
            )

    def test_missing_bg_geo_id_raises(self):
        """Seed without bg_geo_id should raise when sample plan is used."""
        plan = self._plan({"060010001001": "seg_a"})
        blocks = self._blocks({"060010001001000": ("Z1", 100_000)})
        seed = _make_seed("zone_group_id", {"Z1": 10})  # no bg_geo_id
        with pytest.raises(ValueError, match="missing 'bg_geo_id'"):
            self._weights(seed, plan, blocks)


class TestGrowBlockHouseholds:
    """Census block households are grown to the PUMS year by their PUMA."""

    BLOCKS = pl.DataFrame(
        {
            "block_id": ["060010001001000", "060010001001001", "060130001001000"],
            "puma_id": ["00101", "00101", "01301"],
            # the third block's PUMA sits outside the zones; it gets no growth
            "zone_group_id": ["Z1", None, None],
            "households": [60, 40, 500],
        }
    )
    PUMS = pl.DataFrame({"PUMA": ["00101", "00101", "01301"], "WGTP": [70.0, 40.0, 600.0]})

    def test_each_puma_scales_its_blocks(self):
        """Growth is the PUMA's PUMS households over all its Census households."""
        grown, growth = grow_block_households(self.BLOCKS, self.PUMS)
        # PUMA 00101: 110 PUMS households over 100 Census households, outside block included
        assert growth.select("puma_id", "growth").rows() == [("00101", pytest.approx(1.1))]
        assert grown.rows() == [("060010001001000", "Z1", pytest.approx(66.0))]

    def test_a_puma_without_pums_households_stops(self):
        """No PUMS count means no growth, and the run should not go on."""
        with pytest.raises(ValueError, match="no PUMS households"):
            grow_block_households(self.BLOCKS, self.PUMS.filter(pl.col("PUMA") != "00101"))


# ---------------------------------------------------------------------------
# Balancer integration: base_weight required
# ---------------------------------------------------------------------------
class TestBalancerRequiresBaseWeight:
    """_prepare_zone must fail if base_weight is missing."""

    def test_balance_weights_missing_base_weight_raises(self):
        """balance_weights → _prepare_zone should raise on missing col."""
        ct = _make_control_totals(
            {"Z1": [("h_total", 1, 3.0), ("h_size", 1, 500.0), ("h_size", 2, 500.0)]}
        )
        seed = pl.DataFrame(
            {
                "hh_id": [1, 2, 3],
                "zone_group_id": ["Z1", "Z1", "Z1"],
                "h_total": [1, 1, 1],
                "h_size": [1, 2, 1],
            }
        )
        with pytest.raises(ValueError, match="missing 'base_weight'"):
            balance_weights(
                seed,
                ct,
                ["h_total", "h_size"],
                balancing=_BALANCING,
                importance={"h_total": 100.0, "h_size": 100.0},
            )


class TestBaseWeightsMatchTheTarget:
    """The expansion limits are multiples of the base weight, so nothing rescales them."""

    CT = _make_control_totals({"Z1": [("h_total", 1, 300.0)]})

    @staticmethod
    def _seed(base: list[float]) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "hh_id": list(range(1, len(base) + 1)),
                "zone_group_id": ["Z1"] * len(base),
                "h_total": [1] * len(base),
                "base_weight": base,
            }
        )

    def test_base_weights_off_the_target_stop(self):
        """Base weights adding to half the target mean the base weights are wrong."""
        with pytest.raises(ValueError, match="max_base_weight_gap"):
            balance_weights(
                self._seed([50.0, 50.0, 50.0]),
                self.CT,
                ["h_total"],
                balancing=_BALANCING,
                importance={"h_total": 100.0},
            )

    def test_limits_are_the_base_weight_times_the_factors(self):
        """A home's cap is its base weight x max_expansion_factor, unscaled."""
        # 301 against 300 is within 0.5%, so the zone balances
        zone = _prepare_zone(
            self._seed([100.0, 100.0, 101.0]),
            self.CT.totals.filter(pl.col("zone_group_id") == "Z1"),
            ["h_total"],
            "Z1",
            importance={"h_total": 100.0},
            min_expansion_factor=0.1,
            max_expansion_factor=10.0,
            max_base_weight_gap=0.005,
            min_weight=None,
            max_weight=None,
            max_iterations=10,
            verbose=False,
        )
        assert zone.ub.tolist() == [1_000.0, 1_000.0, 1_010.0]
        assert zone.lb.tolist() == pytest.approx([10.0, 10.0, 10.1])


# ---------------------------------------------------------------------------
# CSV loading tests
# ---------------------------------------------------------------------------
class TestLoadSamplePlan:
    """load_sample_plan reads a CSV into a SamplePlan."""

    def test_load_valid_csv(self, tmp_path):
        """A well-formed CSV loads, and columns beyond the required two survive."""
        csv = tmp_path / "plan.csv"
        csv.write_text(
            "bg_geo_id,sample_segment,county\n"
            "060010001001,seg_a,Alameda\n"
            "060010001002,seg_a,Alameda\n"
        )
        plan = load_sample_plan(csv)
        assert isinstance(plan, SamplePlan)
        assert plan.strata.height == 2
        assert plan.strata["bg_geo_id"].to_list() == ["060010001001", "060010001002"]
        assert plan.strata["county"].to_list() == ["Alameda", "Alameda"]

    def test_load_file_not_found(self, tmp_path):
        """Loading a nonexistent file should raise FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="not found"):
            load_sample_plan(tmp_path / "nonexistent.csv")

    def test_end_to_end_csv_to_base_weights(self, tmp_path):
        """Load CSV → SamplePlan → compute_base_weights, with unequal segments.

        Pooling the two segments would give 160 000 / 250 = 640 everywhere, so
        neither assertion can pass unless each segment is weighted on its own.
        """
        csv = tmp_path / "plan.csv"
        csv.write_text("bg_geo_id,sample_segment\n060010001001,urban\n060750001001,rural\n")
        plan = load_sample_plan(csv)
        blocks = pl.DataFrame(
            {
                "block_id": ["060010001001000", "060750001001000"],
                "zone_group_id": ["Z1", "Z2"],
                "households": [100_000.0, 60_000.0],
            }
        )
        ct = _make_control_totals({"Z1": [("h_size", 1, 999.0)], "Z2": [("h_size", 1, 999.0)]})
        seed = _make_seed_with_bg(
            "zone_group_id",
            {"060010001001": ("Z1", 200), "060750001001": ("Z2", 50)},
        )
        result = compute_base_weights(
            seed,
            ct,
            ["h_size"],
            sample_plan=plan,
            block_households=blocks,
        )
        z1 = result.filter(pl.col("zone_group_id") == "Z1")
        z2 = result.filter(pl.col("zone_group_id") == "Z2")
        # urban: 100k/200 = 500, rural: 60k/50 = 1200
        assert z1["base_weight"].to_list() == [pytest.approx(500.0)] * 200
        assert z2["base_weight"].to_list() == [pytest.approx(1_200.0)] * 50


# ===========================================================================
# Per-control importance, from the PUMS replicate weights
# ===========================================================================


# ---------------------------------------------------------------------------
# Helpers — minimal stub control for testing without the full registry
# ---------------------------------------------------------------------------
class _StubCategory(IntEnum):
    CAT_A = 1
    CAT_B = 2


class _HHStubControl(ControlTarget):
    name = "h_stub"
    level = ControlLevel.HOUSEHOLD
    description = "stub"
    categories = _StubCategory
    survey_fields = ("stub_col",)
    pums_fields = ("STUB",)

    def survey_expr(self) -> pl.Expr:
        return pl.col("stub_col")

    def pums_expr(self) -> pl.Expr:
        return pl.col("STUB")


class _PersonStubControl(ControlTarget):
    name = "p_stub"
    level = ControlLevel.PERSON
    description = "person stub"
    categories = _StubCategory
    survey_fields = ("stub_col",)
    pums_fields = ("STUB",)

    def survey_expr(self) -> pl.Expr:
        return pl.col("stub_col")

    def pums_expr(self) -> pl.Expr:
        return pl.col("STUB")


def _hh_frame_with_replicates(
    n_rows: int = 10,
    *,
    zone_group_ids: list[str] | None = None,
    ctrl_col: str = "h_stub",
    ctrl_values: list[int] | None = None,
    base_weight: float = 100.0,
    noise_scale: float = 5.0,
    seed: int = 42,
) -> pl.DataFrame:
    """Build a household DataFrame with replicate weight columns."""
    rng = np.random.default_rng(seed)
    geos = zone_group_ids or ["Z1"]
    vals = ctrl_values or [1, 2]

    data: dict[str, list] = {
        "zone_group_id": [geos[i % len(geos)] for i in range(n_rows)],
        ctrl_col: [vals[i % len(vals)] for i in range(n_rows)],
        "_xw_WGTP": [base_weight] * n_rows,
    }
    for r in range(1, 81):
        data[f"_xw_WGTP{r}"] = (base_weight + rng.normal(0, noise_scale, n_rows)).tolist()

    return pl.DataFrame(data)


# ---------------------------------------------------------------------------
# _normalize_cvs
# ---------------------------------------------------------------------------
class TestNormalizeCvs:
    """Tests for the CV → importance transformation."""

    def test_empty_returns_empty(self):
        """Empty input returns empty dict."""
        assert _normalize_cvs({}) == {}

    def test_median_equals_default_importance(self):
        """Median importance across controls equals DEFAULT_IMPORTANCE."""
        cvs = {"a": 0.01, "b": 0.05, "c": 0.10}
        result = _normalize_cvs(cvs)
        median = float(np.median(list(result.values())))
        assert median == pytest.approx(DEFAULT_IMPORTANCE)

    def test_sqrt_dampening(self):
        """Ratio of importances should follow 1/sqrt(CV) relationship."""
        cvs = {"a": 0.04, "b": 0.16}
        result = _normalize_cvs(cvs)
        # 1/sqrt(0.04) = 5, 1/sqrt(0.16) = 2.5  → ratio = 2, median scaled to 100
        assert result["a"] / result["b"] == pytest.approx(2.0)
        assert result["a"] > result["b"]
        assert result["a"] == pytest.approx(4.0 / 3.0 * DEFAULT_IMPORTANCE)
        assert result["b"] == pytest.approx(2.0 / 3.0 * DEFAULT_IMPORTANCE)

    def test_tiny_cv_clipped(self):
        """Near-zero CVs should not produce infinity."""
        cvs = {"zero": 1e-15, "normal": 0.05}
        result = _normalize_cvs(cvs)
        assert np.isfinite(result["zero"])
        assert result["zero"] > result["normal"]


# ---------------------------------------------------------------------------
# _control_cv
# ---------------------------------------------------------------------------
class TestControlCv:
    """Tests for per-control CV estimation from replicate weights."""

    def test_low_noise_gives_lower_cv(self):
        """Lower replicate noise produces a lower, still positive, CV."""
        hh_low = _hh_frame_with_replicates(50, noise_scale=1.0, seed=1)
        hh_high = _hh_frame_with_replicates(50, noise_scale=50.0, seed=2)
        ctrl = _HHStubControl()
        cv_low = _cv(ctrl, hh_low, pl.DataFrame())
        cv_high = _cv(ctrl, hh_high, pl.DataFrame())
        assert cv_low is not None
        assert cv_high is not None
        assert 0 < cv_low < cv_high

    def test_returns_none_when_no_matching_records(self):
        """If no rows match valid categories, return None."""
        hh = _hh_frame_with_replicates(10, ctrl_values=[99, 98])  # not in _StubCategory
        ctrl = _HHStubControl()
        cv = _cv(ctrl, hh, pl.DataFrame())
        assert cv is None

    @pytest.mark.parametrize(
        ("with_replicates", "with_control", "match"),
        [
            pytest.param(False, True, "Replicate weight columns missing", id="no_replicates"),
            pytest.param(True, False, r"Control column.*not found", id="no_control_column"),
        ],
    )
    def test_raises_on_missing_columns(self, with_replicates, with_control, match):
        """Both the replicate block and the control column are required."""
        data: dict[str, list] = {"zone_group_id": ["Z1"], "_xw_WGTP": [100.0]}
        if with_control:
            data["h_stub"] = [1]
        if with_replicates:
            for r in range(1, 81):
                data[f"_xw_WGTP{r}"] = [100.0]
        hh = pl.DataFrame(data)
        ctrl = _HHStubControl()
        with pytest.raises(ValueError, match=match):
            _cv(ctrl, hh, pl.DataFrame())

    def test_person_level_uses_person_frame(self):
        """Person-level controls should read from person_df, not hh_df."""
        person = _hh_frame_with_replicates(20, ctrl_col="p_stub")
        # Rename weight columns to person variants
        renames = {"_xw_WGTP": "_xw_PWGTP"}
        for r in range(1, 81):
            renames[f"_xw_WGTP{r}"] = f"_xw_PWGTP{r}"
        person = person.rename(renames)

        ctrl = _PersonStubControl()
        cv = _cv(ctrl, pl.DataFrame(), person)
        assert cv is not None
        assert cv > 0

    def test_multiple_zones_returns_median(self):
        """CV is the median across every zone x category cell, not one zone's.

        Four cells, one row each, all with an estimate of 100.  Every
        replicate of a row sits a constant distance from that estimate, so
        the cell's standard error is ``sqrt((4/80) * 80 * d**2) == 2*d`` and
        its CV is ``2*d / 100``.  Offsets of 1, 2, 3, 4 give CVs of 0.02,
        0.04, 0.06, 0.08, whose median is 0.05 — a number no single zone or
        single cell produces.
        """
        offsets = [1.0, 2.0, 3.0, 4.0]
        data: dict[str, list] = {
            "zone_group_id": ["Z1", "Z1", "Z2", "Z2"],
            "h_stub": [1, 2, 1, 2],
            "_xw_WGTP": [100.0] * 4,
        }
        for r in range(1, 81):
            data[f"_xw_WGTP{r}"] = [100.0 + d for d in offsets]
        hh = pl.DataFrame(data)

        cv = _cv(_HHStubControl(), hh, pl.DataFrame())
        assert cv == pytest.approx(0.05)

    def test_zero_estimate_cells_excluded(self):
        """Cells where the estimate is zero should be excluded from the median."""
        data: dict[str, list] = {
            "zone_group_id": ["Z1", "Z1"],
            "h_stub": [1, 2],
            "_xw_WGTP": [100.0, 0.0],  # second cell has zero weight
        }
        for r in range(1, 81):
            data[f"_xw_WGTP{r}"] = [100.0, 0.0]
        hh = pl.DataFrame(data)
        ctrl = _HHStubControl()
        cv = _cv(ctrl, hh, pl.DataFrame())
        # Only one valid cell (estimate=100), replicates all equal → CV=0
        assert cv is not None
        assert cv == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# compute_moe_importance (integration)
# ---------------------------------------------------------------------------
class TestComputeMoeImportance:
    """Integration tests for the full MOE importance pipeline."""

    def test_raises_on_unknown_control(self):
        """If a target name is not in the registry, raise ValueError."""
        hh = pl.DataFrame()
        person = pl.DataFrame()
        with pytest.raises(ValueError, match="Unknown control"):
            compute_control_moe(hh, person, ["not_a_control"])

    def test_end_to_end_with_real_registry(self):
        """Use h_size from the real registry with synthetic data."""
        ctrl = CONTROLS["h_size"]
        n = 100
        rng = np.random.default_rng(42)
        sizes = rng.choice([m[0] for m in ctrl.valid_members], size=n)

        data: dict[str, list] = {
            "zone_group_id": ["Z1"] * n,
            "h_size": sizes.tolist(),
            "_xw_WGTP": [50.0] * n,
        }
        for r in range(1, 81):
            data[f"_xw_WGTP{r}"] = (50.0 + rng.normal(0, 3, n)).tolist()

        hh = pl.DataFrame(data)
        person = pl.DataFrame()
        result = _moe_importance(hh, person, ["h_size"])

        assert "h_size" in result
        assert result["h_size"] > 0
        assert np.isfinite(result["h_size"])

    def test_structural_controls_omitted_when_sparse(self):
        """Structural controls with no matching data return empty dict."""
        # h_total has category TOTAL=1, but we provide no records matching it
        data: dict[str, list] = {
            "zone_group_id": ["Z1"],
            "h_total": [99],  # not a valid member value
            "_xw_WGTP": [100.0],
        }
        for r in range(1, 81):
            data[f"_xw_WGTP{r}"] = [100.0]
        hh = pl.DataFrame(data)
        result = _moe_importance(hh, pl.DataFrame(), ["h_total"])
        # h_total should be omitted (no valid cells)
        assert "h_total" not in result

    def test_ordering_preserved(self):
        """The control whose cells are tighter comes out of the pipeline heavier.

        Four households in one zone, all weighted 100, whose replicates sit a
        constant 1, 1, 1 and 9 above that weight.  ``h_size`` splits them into
        two cells of two — offsets (1, 1) and (1, 9) — for cell CVs of 0.02
        and 0.10 and a median of 0.06.  ``h_workers`` puts every household in
        a cell of its own, for CVs of 0.02, 0.02, 0.02, 0.18 and a median of
        0.02.  So ``h_workers`` is the tighter control and must come back with
        the higher importance, by exactly ``sqrt(0.06 / 0.02)``.
        """
        offsets = [1.0, 1.0, 1.0, 9.0]
        data: dict[str, list] = {
            "zone_group_id": ["Z1"] * 4,
            "h_size": [1, 1, 2, 2],
            "h_workers": [0, 1, 2, 3],
            "_xw_WGTP": [100.0] * 4,
        }
        for r in range(1, 81):
            data[f"_xw_WGTP{r}"] = [100.0 + d for d in offsets]
        hh = pl.DataFrame(data)

        # the CVs the two groupings produce, asserted before the normalisation
        assert _cv(CONTROLS["h_size"], hh, pl.DataFrame()) == pytest.approx(0.06)
        assert _cv(CONTROLS["h_workers"], hh, pl.DataFrame()) == pytest.approx(0.02)

        result = _moe_importance(hh, pl.DataFrame(), ["h_size", "h_workers"])
        assert result["h_workers"] > result["h_size"]
        assert result["h_workers"] / result["h_size"] == pytest.approx(np.sqrt(3.0))
        # the two importances straddle the default, since their median is it
        assert np.median([result["h_size"], result["h_workers"]]) == pytest.approx(
            DEFAULT_IMPORTANCE
        )


# ---------------------------------------------------------------------------
# Cross-tab MOE support
# ---------------------------------------------------------------------------


@pytest.mark.usefixtures("_clean_registry")
class TestCrosstabMoe:
    """Tests for MOE computation on cross-tab controls."""

    @staticmethod
    def _xtab_hh_frame(n: int = 80, noise: float = 3.0) -> pl.DataFrame:
        """Build PUMS HH data with dimension columns and replicate weights."""
        rng = np.random.default_rng(42)
        size_ctrl = CONTROLS["h_size"]
        income_ctrl = CONTROLS["h_income"]
        size_vals = [m[0] for m in size_ctrl.valid_members]
        income_vals = [m[0] for m in income_ctrl.valid_members]
        data: dict[str, list] = {
            "zone_group_id": ["Z1"] * n,
            "h_size": rng.choice(size_vals, size=n).tolist(),
            "h_income": rng.choice(income_vals, size=n).tolist(),
            "_xw_WGTP": [100.0] * n,
        }
        for r in range(1, 81):
            data[f"_xw_WGTP{r}"] = (100.0 + rng.normal(0, noise, n)).tolist()
        return pl.DataFrame(data)

    def test_cell_moe_returns_dataframe_for_crosstab(self):
        """_control_cell_moe computes MOE for cross-tab by adding composite column."""
        xtab = register_crosstab("h_size_x_income", ["h_size", "h_income"])
        hh = self._xtab_hh_frame()
        result = _control_cell_moe(xtab, hh, pl.DataFrame())
        assert result is not None
        assert "control_name" in result.columns
        assert result["control_name"][0] == "h_size_x_income"
        assert len(result) > 0

    def test_compute_moe_importance_includes_crosstab(self):
        """compute_moe_importance returns importance for cross-tab controls."""
        register_crosstab("h_size_x_income", ["h_size", "h_income"])
        hh = self._xtab_hh_frame(n=200)
        result = _moe_importance(hh, pl.DataFrame(), ["h_size", "h_size_x_income"])
        assert "h_size_x_income" in result
        assert result["h_size_x_income"] > 0


# ===========================================================================
# The expansion-factor grid search, and the chart that reads it
# ===========================================================================


def _grid_control_totals(
    zone_data: dict[str, list[tuple[str, int, float]]],
) -> ControlTotals:
    rows: list[dict] = []
    for zone_group_id, entries in zone_data.items():
        for ctrl_name, cat, total in entries:
            rows.append(
                {
                    "zone_group_id": zone_group_id,
                    "control_name": ctrl_name,
                    "category": cat,
                    "target_total": total,
                }
            )
    totals = pl.DataFrame(rows).cast(
        {"zone_group_id": pl.Utf8, "category": pl.Utf8, "target_total": pl.Float64}
    )
    return ControlTotals(
        totals=totals,
        pums_hh_count=0,
        pums_person_count=0,
        zone_group_ids=sorted(zone_data),
    )


def _grid_seed(zone_hh: dict[str, int], base_weight: float) -> pl.DataFrame:
    """Build a seed table with base_weight and h_total/h_size category columns."""
    rows: list[dict] = []
    hh_id = 1
    for zone_group_id, n in zone_hh.items():
        for i in range(n):
            rows.append(
                {
                    "hh_id": hh_id,
                    "zone_group_id": zone_group_id,
                    "base_weight": base_weight,
                    "h_total": 1,
                    "h_size": 1 if i % 2 == 0 else 2,
                }
            )
            hh_id += 1
    return pl.DataFrame(rows).cast(
        {
            "hh_id": pl.Int64,
            "zone_group_id": pl.Utf8,
            "base_weight": pl.Float64,
            "h_total": pl.Int64,
            "h_size": pl.Int64,
        }
    )


# ---------------------------------------------------------------------------


class TestGridSearch:
    """Integration-style test with a tiny single-zone setup."""

    @pytest.fixture
    def tiny_setup(self):
        """Single zone with two controls (h_total and h_size) and 20 seed households."""
        targets = ["h_total", "h_size"]
        ct = _grid_control_totals(
            {
                "Z1": [
                    ("h_total", 1, 100.0),
                    ("h_size", 1, 50.0),
                    ("h_size", 2, 50.0),
                ]
            }
        )
        # 20 homes at 5 add up to the 100-household target
        seed = _grid_seed({"Z1": 20}, base_weight=5.0)
        return seed, ct, targets

    def test_returns_one_point_per_ef(self, tiny_setup):
        """Returns one GridPoint per max_expansion_factor in the grid."""
        seed, ct, targets = tiny_setup
        grid = [2.0, 5.0, 10.0]
        results = grid_search_expansion_factor(
            seed,
            ct,
            targets,
            ef_grid=grid,
            selected_ef=5.0,
            balancing=_BALANCING,
            importance=dict.fromkeys(targets, 100.0),
        )
        assert len(results) == 3
        assert [r.max_expansion_factor for r in results] == [2.0, 5.0, 10.0]

    @pytest.mark.parametrize(
        ("ef_grid", "expected"),
        [
            pytest.param([2.0, 10.0], [2.0, 5.0, 10.0], id="inserted_in_order"),
            pytest.param([], [5.0], id="empty_grid_is_just_the_selection"),
        ],
    )
    def test_selected_ef_injected_into_grid(self, tiny_setup, ef_grid, expected):
        """selected_ef is always searched, whether or not the grid names it."""
        seed, ct, targets = tiny_setup
        results = grid_search_expansion_factor(
            seed,
            ct,
            targets,
            ef_grid=ef_grid,
            selected_ef=5.0,
            balancing=_BALANCING,
            importance=dict.fromkeys(targets, 100.0),
        )
        assert [r.max_expansion_factor for r in results] == expected

    def test_metrics_are_finite(self, tiny_setup):
        """Metrics should be finite and within expected ranges."""
        seed, ct, targets = tiny_setup
        results = grid_search_expansion_factor(
            seed,
            ct,
            targets,
            ef_grid=[5.0],
            selected_ef=5.0,
            balancing=_BALANCING,
            importance=dict.fromkeys(targets, 100.0),
        )
        gp = results[0]
        assert gp.mape >= 0
        assert gp.p90 >= 0
        assert gp.cv >= 0
        assert 0 <= gp.ess_pct <= 100
        assert gp.converged_zones <= gp.total_zones


# ---------------------------------------------------------------------------
# ef_tradeoff_figure
# ---------------------------------------------------------------------------


class TestEFTradeoffFigure:
    """Tests for the EF tradeoff chart generation."""

    def _sample_grid(self) -> list[GridPoint]:
        return [
            GridPoint(2.0, 10, 10, 5.0, 12.0, 20.0, 0.3, 90.0),
            GridPoint(5.0, 10, 10, 3.0, 8.0, 15.0, 0.5, 75.0),
            GridPoint(10.0, 10, 10, 2.0, 5.0, 10.0, 0.8, 60.0),
        ]

    def test_returns_figure(self):
        """A Figure carrying the three fit-error series plus CV and ESS%."""
        fig = ef_tradeoff_figure(self._sample_grid(), selected_ef=5.0)
        assert isinstance(fig, go.Figure)
        names = {t.name for t in fig.data}  # pyright: ignore[reportAttributeAccessIssue]
        assert names == {"MAPE (%)", "P90 (%)", "Max Error (%)", "CV", "ESS (%)"}
