"""Data transformations for the diagnostics report."""

import polars as pl

from processing.weighting.balancing.specs import MergeSpec
from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import CONTROLS, resolve_targets
from processing.weighting.data_prep.control_data import ControlTotals


def _member_value_map(control_name: str) -> dict[str, int]:
    """Lowercase member name → category int for one control."""
    ctrl = CONTROLS.get(control_name)
    if ctrl is None:
        return {}
    return {name.lower(): value for value, name in ctrl.valid_members}


def category_label_map(
    target_names: list[str],
    merges: list[MergeSpec] | None = None,
) -> dict[tuple[str, int], str]:
    """Map ``(control_name, category_int)`` to a human-readable label.

    When *merges* are provided the constituent categories are removed and
    replaced by a single entry keyed on the **minimum** category int in
    each group.
    """
    merged_cats: set[tuple[str, int]] = set()
    merged_labels: dict[tuple[str, int], str] = {}

    for spec in merges or []:
        vmap = _member_value_map(spec.control)
        for merged_label, base_members in spec.groups.items():
            ints = sorted(vmap[m] for m in base_members if m in vmap)
            if not ints:
                continue
            merged_cats.update((spec.control, v) for v in ints)
            merged_labels[(spec.control, ints[0])] = merged_label.replace("_", " ").title()

    labels = {}
    for name in target_names:
        ctrl = CONTROLS.get(name)
        if ctrl is None:
            continue
        for value, member in ctrl.valid_members:
            if (name, value) in merged_cats:
                continue
            lbl = member.replace("_", " ").title()
            # Disambiguate single-category controls (e.g. h_total vs p_total)
            if len(ctrl.valid_members) == 1:
                lbl = ctrl.description
            labels[(name, value)] = lbl
    labels.update(merged_labels)
    return labels


def _process_single_merge(
    result: pl.DataFrame,
    spec: MergeSpec,
    base_vmap: dict[str, int],
    ext_vmap: dict[tuple[str, str], int],
) -> tuple[pl.DataFrame, dict[tuple[str, str], int]]:
    """Process a single merge specification and update the result dataframe."""
    combined = dict(base_vmap)
    for (ctrl, name), val in ext_vmap.items():
        if ctrl == spec.control:
            combined[name] = val

    for merged_label_raw, base_members in spec.groups.items():
        ints = sorted(combined[m] for m in base_members if m in combined)
        if len(ints) < 2:  # noqa: PLR2004
            continue

        merged_cat = ints[0]
        label_str = merged_label_raw.replace("_", " ").title()
        ext_vmap[(spec.control, merged_label_raw.lower())] = merged_cat

        cat_match = (pl.col("control_name") == spec.control) & pl.col("category").is_in(ints)
        if spec.zones is not None:
            cat_match = cat_match & pl.col("geo_id").is_in(spec.zones)

        keep = result.filter(~cat_match)
        to_merge = result.filter(cat_match)
        if to_merge.is_empty():
            continue

        agg = (
            to_merge.group_by("geo_id")
            .agg(
                pl.col("target_total").sum(),
                pl.col("weighted_total").sum(),
            )
            .with_columns(
                pl.lit(spec.control).alias("control_name"),
                pl.lit(merged_cat, dtype=pl.Int16).alias("category"),
                pl.lit(label_str).alias("label"),
            )
            .with_columns(
                (pl.col("weighted_total") - pl.col("target_total")).alias("diff"),
            )
            .with_columns(
                (pl.col("diff") / pl.col("target_total") * 100)
                .fill_nan(0)
                .fill_null(0)
                .alias("diff_pct"),
            )
        )
        result = pl.concat([keep, agg.select(result.columns)])

    return result, ext_vmap


def _pad_missing_rows(result: pl.DataFrame) -> pl.DataFrame:
    """Pad missing (control_name, label) pairs for every zone."""
    all_zones = result["geo_id"].unique()
    all_cl = result.select("control_name", "category", "label").unique()
    full_grid = all_cl.join(all_zones.to_frame("geo_id"), how="cross")
    existing = result.select("geo_id", "control_name", "label").unique()
    missing = full_grid.join(existing, on=["geo_id", "control_name", "label"], how="anti")

    if missing.is_empty():
        return result

    schema = result.schema
    pad = missing
    for col_name in ["target_total", "weighted_total", "diff", "diff_pct"]:
        pad = pad.with_columns(pl.lit(None).cast(schema[col_name]).alias(col_name))
    return pl.concat([result, pad.select(result.columns)])


def apply_fit_merges(
    fit: pl.DataFrame,
    merges: list | None,
    target_names: list[str],
) -> pl.DataFrame:
    """Collapse merged categories in the fit table.

    Adds a ``label`` column.  Global merges collapse rows for every zone.
    Zone-specific merges only collapse for the listed zones.  After all
    merges, missing ``(control_name, label)`` pairs are padded with null
    rows so that every zone panel has consistent y-axis entries.
    """
    # ---- initial label column from enum definitions ----
    label_rows = []
    for name in target_names:
        ctrl = CONTROLS.get(name)
        if ctrl is None:
            continue
        for value, member in ctrl.valid_members:
            lbl = member.replace("_", " ").title()
            if len(ctrl.valid_members) == 1:
                lbl = ctrl.description
            label_rows.append({"control_name": name, "category": value, "label": lbl})

    label_df = pl.DataFrame(label_rows)
    result = fit.join(label_df, on=["control_name", "category"], how="left").with_columns(
        pl.col("label").fill_null(pl.col("control_name") + ":" + pl.col("category").cast(pl.Utf8))
    )

    if not merges:
        return result

    # ---- process merges sequentially ----
    ext_vmap: dict[tuple[str, str], int] = {}
    for spec in merges:
        base_vmap = _member_value_map(spec.control)
        result, ext_vmap = _process_single_merge(result, spec, base_vmap, ext_vmap)

    # ---- pad: every (control_name, label) in every geo_id ----
    result = _pad_missing_rows(result)
    return result.sort("control_name", "category", "label", "geo_id")


def _first_control_name(target_names: list[str], level: ControlLevel) -> str | None:
    """Return the first control name at *level*, or None."""
    ctrls = resolve_targets(target_names, level)
    return ctrls[0].name if ctrls else None


def zone_fit_summary(
    fit: pl.DataFrame,
    target_names: list[str],
) -> pl.DataFrame:
    """Per-zone summary: HH/Person pop target & weighted, %Err, MAPE.

    Population totals are derived by summing categories of one representative
    control at each level (any control's categories partition the population).

    Returns columns: geo_id, hh_target, hh_weighted, hh_pct_err,
    per_target, per_weighted, per_pct_err, mape.
    """
    hh_ctrl = _first_control_name(target_names, ControlLevel.HOUSEHOLD)
    per_ctrl = _first_control_name(target_names, ControlLevel.PERSON)

    def _pop(zf: pl.DataFrame, ctrl_name: str | None) -> tuple[float, float, float]:
        if ctrl_name is None:
            return 0.0, 0.0, 0.0
        cf = zf.filter(pl.col("control_name") == ctrl_name)
        target = cf["target_total"].sum() or 0.0
        weighted = cf["weighted_total"].sum() or 0.0
        pct_err = (weighted - target) / target * 100 if target else 0.0  # pyright: ignore[reportOperatorIssue]
        return target, weighted, pct_err  # pyright: ignore[reportReturnType]

    zones = sorted(fit["geo_id"].unique().to_list())
    rows: list[dict] = []

    for z in zones:
        zf = fit.filter(pl.col("geo_id") == z)
        mape = zf["diff_pct"].abs().mean() or 0.0
        abs_errs = zf["diff_pct"].abs()
        p90 = abs_errs.quantile(0.9, interpolation="higher") or 0.0
        max_err = abs_errs.max() or 0.0

        ht, hw, he = _pop(zf, hh_ctrl)
        pt, pw, pe = _pop(zf, per_ctrl)
        rows.append(
            {
                "geo_id": z,
                "hh_target": ht,
                "hh_weighted": hw,
                "hh_pct_err": he,
                "per_target": pt,
                "per_weighted": pw,
                "per_pct_err": pe,
                "mape": mape,
                "p90_err": p90,
                "max_err": max_err,
            }
        )

    return pl.DataFrame(rows)


def compute_weighted_totals(
    seed: pl.DataFrame,
    weights: pl.DataFrame,
    target_names: list[str],
) -> pl.DataFrame:
    """Weighted totals per (geo_id, control_name, category)."""
    sw = seed.join(weights.select("hh_id", "hh_weight"), on="hh_id", how="left")
    rows: list[dict] = []

    for ctrl in resolve_targets(target_names, ControlLevel.HOUSEHOLD):
        for value, _ in ctrl.valid_members:
            agg = (
                sw.filter(pl.col(ctrl.name) == value)
                .group_by("ctrl_geoid")
                .agg(pl.col("hh_weight").sum().alias("weighted_total"))
            )
            rows.extend(
                [
                    {
                        "geo_id": r["ctrl_geoid"],
                        "control_name": ctrl.name,
                        "category": value,
                        "weighted_total": r["weighted_total"],
                    }
                    for r in agg.iter_rows(named=True)
                ]
            )

    for ctrl in resolve_targets(target_names, ControlLevel.PERSON):
        for value, member in ctrl.valid_members:
            col = f"{ctrl.name}__{member.lower()}"
            if col not in sw.columns:
                continue
            agg = sw.group_by("ctrl_geoid").agg(
                (pl.col(col) * pl.col("hh_weight")).sum().alias("weighted_total")
            )
            rows.extend(
                [
                    {
                        "geo_id": r["ctrl_geoid"],
                        "control_name": ctrl.name,
                        "category": value,
                        "weighted_total": r["weighted_total"],
                    }
                    for r in agg.iter_rows(named=True)
                ]
            )

    return pl.DataFrame(rows)


def fit_table(
    control_totals: ControlTotals,
    weighted_totals: pl.DataFrame,
) -> pl.DataFrame:
    """Join targets to weighted totals; add ``diff`` and ``diff_pct`` columns."""
    return (
        control_totals.totals.join(
            weighted_totals, on=["geo_id", "control_name", "category"], how="left"
        )
        .with_columns(pl.col("weighted_total").fill_null(0))
        .with_columns((pl.col("weighted_total") - pl.col("target_total")).alias("diff"))
        .with_columns(
            (pl.col("diff") / pl.col("target_total") * 100)
            .fill_nan(0)
            .fill_null(0)
            .alias("diff_pct")
        )
    )
