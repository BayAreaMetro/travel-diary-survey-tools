"""Data transformations for the diagnostics report."""

import polars as pl

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
    merges: list | None = None,
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

    labels = {
        (name, value): member.replace("_", " ").title()
        for name in target_names
        if (ctrl := CONTROLS.get(name)) is not None
        for value, member in ctrl.valid_members
        if (name, value) not in merged_cats
    }
    labels.update(merged_labels)
    return labels


def apply_fit_merges(
    fit: pl.DataFrame,
    merges: list | None,
) -> pl.DataFrame:
    """Collapse merged categories in the fit table.

    For each merge group the constituent rows are replaced by a single
    row whose ``target_total`` and ``weighted_total`` are the sums of the
    originals.  The representative ``category`` is the minimum int in the
    group.  ``diff`` and ``diff_pct`` are recomputed.
    """
    if not merges:
        return fit

    drop_keys: set[tuple[str, int]] = set()
    new_rows: list[pl.DataFrame] = []

    for spec in merges:
        vmap = _member_value_map(spec.control)
        for base_members in spec.groups.values():
            ints = sorted(vmap[m] for m in base_members if m in vmap)
            if len(ints) < 2:  # noqa: PLR2004
                continue
            drop_keys.update((spec.control, v) for v in ints)
            merged_cat = ints[0]
            rows = fit.filter(
                (pl.col("control_name") == spec.control) & pl.col("category").is_in(ints)
            )
            if rows.is_empty():
                continue
            agg = (
                rows.group_by("geo_id")
                .agg(
                    pl.col("target_total").sum(),
                    pl.col("weighted_total").sum(),
                )
                .with_columns(
                    pl.lit(spec.control).alias("control_name"),
                    pl.lit(merged_cat, dtype=pl.Int16).alias("category"),
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
            new_rows.append(agg.select(fit.columns))

    if not drop_keys:
        return fit

    # Remove constituent rows
    mask = pl.struct("control_name", "category").map_elements(
        lambda s: (s["control_name"], s["category"]) not in drop_keys,
        return_dtype=pl.Boolean,
    )
    result = fit.filter(mask)

    # Append merged rows
    if new_rows:
        result = pl.concat([result, *new_rows])
    return result.sort("control_name", "category", "geo_id")


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
        pct_err = (weighted - target) / target * 100 if target else 0.0
        return target, weighted, pct_err

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
