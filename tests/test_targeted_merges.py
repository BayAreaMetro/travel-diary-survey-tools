"""Tests for zone-specific merges layered on top of global merges."""

import numpy as np
import polars as pl
import pytest

from processing.weighting.balancing.merges import apply_category_merges
from processing.weighting.diagnostics.data import apply_fit_merges
from processing.weighting.specs import MergeSpec
from processing.weighting.weighting import _parse_controls


# ---------------------------------------------------------------------------
# _parse_controls — zone_merges config key
# ---------------------------------------------------------------------------
class TestParseControlsZoneMerges:
    """_parse_controls should emit MergeSpecs for zone_merges entries."""

    def test_no_zone_merges(self):
        """Controls without zone_merges produce only global MergeSpecs."""
        controls = [
            {
                "name": "h_size",
                "merge": {"size_5_plus": ["size_5", "size_6"]},
            },
        ]
        _, _, merges, _ = _parse_controls(controls)
        assert len(merges) == 1
        assert merges[0].zones is None

    def test_zone_merges_appended(self):
        """zone_merges produce zone-specific MergeSpecs after the global one."""
        controls = [
            {
                "name": "h_size",
                "merge": {"size_5_plus": ["size_5", "size_6"]},
                "zone_merges": {
                    "Z1": {"size_4_plus": ["size_4", "size_5_plus"]},
                },
            },
        ]
        _, _, merges, _ = _parse_controls(controls)
        assert len(merges) == 2
        # Global first
        assert merges[0].zones is None
        assert "size_5_plus" in merges[0].groups
        # Zone-specific second
        assert merges[1].zones == ["Z1"]
        assert "size_4_plus" in merges[1].groups

    def test_multiple_zone_merges(self):
        """Multiple zone keys each produce their own MergeSpec."""
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
        _, _, merges, _ = _parse_controls(controls)
        assert len(merges) == 3
        zone_map = {m.zones[0]: m for m in merges if m.zones is not None}
        assert "size_4_plus" in zone_map["Z1"].groups
        assert "size_3_plus" in zone_map["Z2"].groups

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
        _, _, merges, _ = _parse_controls(controls)
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
# apply_fit_merges — diagnostics table zone-aware collapsing
# ---------------------------------------------------------------------------


def _make_fit(zones: list[str], categories: list[int], control: str = "h_size") -> pl.DataFrame:
    """Build a minimal fit DataFrame for testing."""
    rows = []
    for z in zones:
        for cat in categories:
            t = float(cat * 10 + hash(z) % 7)
            w = t + (cat - 3)  # small diff for realistic data
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
    return pl.DataFrame(rows).with_columns(pl.col("category").cast(pl.Int16))


class TestApplyFitMergesLabel:
    """apply_fit_merges should add a label column from enum definitions."""

    def test_adds_label_column(self):
        """A label column should be added based on the control and category."""
        fit = _make_fit(["Z1"], [1, 2, 3])
        result = apply_fit_merges(fit, None, ["h_size"])
        assert "label" in result.columns
        labels = result["label"].to_list()
        assert "Size 1" in labels
        assert "Size 2" in labels

    def test_global_merge_collapses_all_zones(self):
        """A global merge should collapse categories for all zones."""
        fit = _make_fit(["Z1", "Z2"], [4, 5, 6])
        merge = MergeSpec(
            control="h_size",
            groups={"size_5_plus": ["size_5", "size_6"]},
            zones=None,
        )
        result = apply_fit_merges(fit, [merge], ["h_size"])
        # 5 and 6 should be collapsed into "Size 5 Plus" for both zones
        for z in ["Z1", "Z2"]:
            zone_labels = result.filter(
                (pl.col("geo_id") == z) & pl.col("target_total").is_not_null()
            )["label"].to_list()
            assert "Size 5 Plus" in zone_labels
            assert "Size 5" not in zone_labels
            assert "Size 6" not in zone_labels
            assert "Size 4" in zone_labels


class TestApplyFitMergesZoneAware:
    """Zone merges should only collapse rows for the listed zones."""

    def test_zone_merge_collapses_only_target_zone(self):
        """A zone merge should only collapse categories for the specified zones."""
        fit = _make_fit(["Z1", "Z2"], [3, 4, 5, 6])
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

        # Z1 should have "Size 4 Plus" with real data
        z1 = result.filter(pl.col("geo_id") == "Z1")
        z1_real = z1.filter(pl.col("target_total").is_not_null())
        z1_labels = z1_real["label"].to_list()
        assert "Size 4 Plus" in z1_labels
        assert "Size 4" not in z1_labels
        assert "Size 5 Plus" not in z1_labels
        assert "Size 3" in z1_labels

        # Z2 should keep original categories with real data
        z2 = result.filter(pl.col("geo_id") == "Z2")
        z2_real = z2.filter(pl.col("target_total").is_not_null())
        z2_labels = z2_real["label"].to_list()
        assert "Size 4" in z2_labels
        assert "Size 5 Plus" in z2_labels
        assert "Size 4 Plus" not in z2_labels

    def test_null_placeholders_for_consistency(self):
        """Every zone should have every label (real or null placeholder)."""
        fit = _make_fit(["Z1", "Z2"], [3, 4, 5, 6])
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

    def test_zone_merge_target_sums_correct(self):
        """Merged row totals should equal sum of constituents."""
        fit = _make_fit(["Z1", "Z2"], [4, 5, 6])
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
        # Get original Z1 targets for cats 4, 5, 6 before any merge
        z1_orig = fit.filter(pl.col("geo_id") == "Z1")
        expected_target = z1_orig["target_total"].sum()

        result = apply_fit_merges(fit, [global_merge, zone_merge], ["h_size"])
        z1_merged = result.filter((pl.col("geo_id") == "Z1") & (pl.col("label") == "Size 4 Plus"))
        assert z1_merged["target_total"].item() == pytest.approx(expected_target)
