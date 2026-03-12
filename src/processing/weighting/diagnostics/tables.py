"""HTML table builders for the diagnostics report."""

import re

import polars as pl

from processing.weighting.balancing.specs import ZoneStatus
from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import resolve_targets
from processing.weighting.data_prep.control_data import ControlTotals

from .data import category_label_map

# ---------------------------------------------------------------------------
# HTML primitives
# ---------------------------------------------------------------------------


def _tag(name: str, text: str, **attrs: str) -> str:
    """Wrap *text* in an HTML element with optional attributes."""
    attr_str = "".join(f' {k.rstrip("_")}="{v}"' for k, v in attrs.items()) if attrs else ""
    return f"<{name}{attr_str}>{text}</{name}>"


def _wbr(text: str) -> str:
    """Insert ``<wbr>`` word-break opportunities after ``/``, ``-``, ``_``, and spaces."""
    return re.sub(r"([/_\- ])", r"\1<wbr>", text)


def _html_table(headers: list[str], rows: list[list[str]]) -> str:
    """Build a ``<table>`` from *headers* and pre-formatted cell strings.

    Cells starting with ``<td`` are inserted as-is (for pre-styled cells).
    """
    head = "<tr>" + "".join(_tag("th", h) for h in headers) + "</tr>"
    body = "\n".join(
        "<tr>" + "".join(c if c.startswith("<td") else _tag("td", c) for c in cells) + "</tr>"
        for cells in rows
    )
    return f"<table>\n{head}\n{body}\n</table>"


# ---------------------------------------------------------------------------
# Section 2 — Balancer Performance (convergence + target fit)
# ---------------------------------------------------------------------------


def balancer_performance_table(
    statuses: list[ZoneStatus],
    weighted: pl.DataFrame,
    zone_fit: pl.DataFrame,
) -> str:
    """Combined per-zone convergence status and target-fit metrics."""
    status_map = {s.geo_id: s for s in statuses}
    fit_map = {r["geo_id"]: r for r in zone_fit.iter_rows(named=True)}
    zones = sorted(status_map)

    group_row = (
        "<tr>"
        '<th rowspan="2">Zone</th>'
        '<th rowspan="2">N</th>'
        '<th rowspan="2">Conv?</th>'
        '<th rowspan="2">Iter</th>'
        '<th colspan="2">Household</th>'
        '<th colspan="2">Person</th>'
        '<th rowspan="2">MAPE</th>'
        '<th rowspan="2">P90</th>'
        '<th rowspan="2">Max</th>'
        "</tr>"
    )
    sub_row = "<tr><th>Target</th><th>% Error</th><th>Target</th><th>% Error</th></tr>"

    data_rows: list[str] = []
    for z in zones:
        s = status_map[z]
        f = fit_map.get(z, {})
        zone_df = weighted.filter(pl.col("ctrl_geoid") == z)
        n = zone_df.height

        css = "converged" if s.converged else "failed"
        cells = [
            _tag("td", z),
            _tag("td", f"{n:,}"),
            f'<td class="{css}">{"Y" if s.converged else "N"}</td>',
            _tag("td", str(s.iterations)),
            _tag("td", f"{f.get('hh_target', 0):,.0f}"),
            _tag("td", f"{f.get('hh_pct_err', 0):+.1f}%"),
            _tag("td", f"{f.get('per_target', 0):,.0f}"),
            _tag("td", f"{f.get('per_pct_err', 0):+.1f}%"),
            _tag("td", f"{f.get('mape', 0):.2f}%"),
            _tag("td", f"{f.get('p90_err', 0):.2f}%"),
            _tag("td", f"{f.get('max_err', 0):.2f}%"),
        ]
        data_rows.append("<tr>" + "".join(cells) + "</tr>")

    return "<table>\n" + group_row + "\n" + sub_row + "\n" + "\n".join(data_rows) + "\n</table>"


# ---------------------------------------------------------------------------
# Section 3 — Weight Quality (distribution + ESS + CV + expansion factors)
# ---------------------------------------------------------------------------


def _weight_stats(w: pl.Series, bw: pl.Series) -> dict[str, str]:
    """Summary statistics for a single weight Series."""
    mean = w.mean() or 0
    n = len(w)
    sum_w = w.sum()
    sum_w2 = (w * w).sum()
    ess_pct = ((sum_w**2 / sum_w2) / n * 100) if sum_w2 > 0 and n > 0 else 0.0
    cv = f"{w.std() / mean:.3f}" if mean else "N/A"  # pyright: ignore[reportOperatorIssue]
    ratio = w / bw
    return {
        "mean": f"{mean:,.2f}",
        "median": f"{w.median():,.2f}",
        "std": f"{w.std():,.2f}",
        "min": f"{w.min():,.2f}",
        "max": f"{w.max():,.2f}",
        "min_ef": f"{ratio.min():.3f}",
        "max_ef": f"{ratio.max():.3f}",
        "mean_ef": f"{ratio.mean():.3f}",
        "median_ef": f"{ratio.median():.3f}",
        "cv": cv,
        "ess_pct": f"{ess_pct:.1f}%",
    }


def weight_quality_table(weighted: pl.DataFrame) -> str:
    """Per-zone + total weight quality table (distribution, CV, ESS, EF)."""
    headers = [
        "Zone",
        "N",
        "Mean",
        "Median",
        "Std",
        "Min",
        "Max",
        "Min&nbsp;EF",
        "Max&nbsp;EF",
        "Mean&nbsp;EF",
        "Median&nbsp;EF",
        "CV",
        "ESS %",
    ]

    def _row(label: str, df: pl.DataFrame) -> list[str]:
        s = _weight_stats(df["hh_weight"], df["base_weight"])
        return [
            label,
            f"{df.height:,}",
            s["mean"],
            s["median"],
            s["std"],
            s["min"],
            s["max"],
            s["min_ef"],
            s["max_ef"],
            s["mean_ef"],
            s["median_ef"],
            s["cv"],
            s["ess_pct"],
        ]

    zones = sorted(weighted["ctrl_geoid"].unique().to_list())
    rows = [_row(str(z), weighted.filter(pl.col("ctrl_geoid") == z)) for z in zones]
    rows.append(_row("TOTAL", weighted))
    return _html_table(headers, rows)


# ---------------------------------------------------------------------------
# Section 5 — Unweighted cell counts (data sparsity matrix)
# ---------------------------------------------------------------------------

_LOW_COUNT_THRESHOLD = 30


def _count_cell(count: int, pums_pct: float | None = None) -> str:
    """Format a count, highlighting values < 30 in red.

    When *pums_pct* is provided the PUMS-weighted share is appended
    in italic parentheses, e.g. ``23 (41.2%)``.
    """
    pct_html = ""
    if pums_pct is not None:
        pct_html = f' <em style="color:#888;font-weight:normal">({pums_pct:.1f}%)</em>'
    if count < _LOW_COUNT_THRESHOLD:
        return f'<td style="color:#c33;font-weight:bold">{count}{pct_html}</td>'
    return f"<td>{count}{pct_html}</td>"


def unweighted_cell_counts(  # noqa: C901
    seed: pl.DataFrame,
    target_names: list[str],
    control_totals: ControlTotals | None = None,
) -> str:
    """Single matrix table: categories (rows) x zones (columns).

    Row headers are grouped by control name using ``<th rowspan>``.
    A level separator row (Household / Person) divides the two groups.

    When *control_totals* is provided, each cell also shows the
    PUMS-weighted percentage in italic parentheses so the reader can
    compare survey representation against the PUMS universe.
    """
    labels = category_label_map(target_names)
    zones = sorted(seed["ctrl_geoid"].unique().to_list())
    zone_dfs = {z: seed.filter(pl.col("ctrl_geoid") == z) for z in zones}
    n_zone_cols = len(zones)

    # Pre-compute PUMS share per (zone, control, category) ----------------
    pums_pct: dict[tuple[str, str, int], float] = {}
    # Also compute an all-zone total PUMS share per (control, category)
    pums_pct_total: dict[tuple[str, int], float] = {}
    if control_totals is not None:
        ct = control_totals.totals
        zone_ctrl_totals = ct.group_by(["geo_id", "control_name"]).agg(
            pl.col("target_total").sum().alias("zone_ctrl_total")
        )
        ct_with_pct = ct.join(zone_ctrl_totals, on=["geo_id", "control_name"], how="left")
        ct_with_pct = ct_with_pct.with_columns(
            (pl.col("target_total") / pl.col("zone_ctrl_total") * 100).alias("pct")
        )
        for row in ct_with_pct.iter_rows(named=True):
            pums_pct[(row["geo_id"], row["control_name"], row["category"])] = row["pct"]

        # Total across all zones
        all_ctrl_totals = ct.group_by("control_name").agg(
            pl.col("target_total").sum().alias("all_ctrl_total")
        )
        ct_total = (
            ct.group_by(["control_name", "category"])
            .agg(pl.col("target_total").sum().alias("cat_total"))
            .join(all_ctrl_totals, on="control_name", how="left")
            .with_columns((pl.col("cat_total") / pl.col("all_ctrl_total") * 100).alias("pct"))
        )
        for row in ct_total.iter_rows(named=True):
            pums_pct_total[(row["control_name"], row["category"])] = row["pct"]

    header = (
        "<tr>"
        + _tag("th", "Control")
        + _tag("th", "Category")
        + "".join(_tag("th", _wbr(str(z))) for z in zones)
        + _tag("th", "Total")
        + "</tr>"
    )

    data_rows: list[str] = []

    for level in (ControlLevel.HOUSEHOLD, ControlLevel.PERSON):
        ctrls = resolve_targets(target_names, level)
        if not ctrls:
            continue
        # Level separator row
        level_label = "Household" if level == ControlLevel.HOUSEHOLD else "Person"
        data_rows.append(
            f'<tr><td colspan="{n_zone_cols + 3}" '
            f'style="background:#e8e8e8;font-weight:bold;text-align:left">{level_label}</td></tr>'
        )
        for ctrl in ctrls:
            members = list(ctrl.valid_members)
            for i, (value, member) in enumerate(members):
                cells = ""
                if i == 0:
                    cells += f'<th rowspan="{len(members)}">{ctrl.name}</th>'
                cells += (
                    f'<td style="text-align:left">{labels.get((ctrl.name, value), member)}</td>'
                )
                row_total = 0
                for z in zones:
                    zdf = zone_dfs[z]
                    if level == ControlLevel.HOUSEHOLD:
                        count = zdf.filter(pl.col(ctrl.name) == value).height
                    else:
                        col = f"{ctrl.name}__{member.lower()}"
                        count = int(zdf[col].sum()) if col in seed.columns else 0
                    row_total += count
                    pct = pums_pct.get((z, ctrl.name, value))
                    cells += _count_cell(count, pct)
                total_pct = pums_pct_total.get((ctrl.name, value))
                cells += _count_cell(row_total, total_pct)
                data_rows.append(f"<tr>{cells}</tr>")

    return '<table class="sparsity">\n' + header + "\n" + "\n".join(data_rows) + "\n</table>"


# ---------------------------------------------------------------------------
# Section 1b — Crosswalk summary table
# ---------------------------------------------------------------------------


def crosswalk_summary_table(crosswalk_df: pl.DataFrame, seed: pl.DataFrame) -> str:  # noqa: C901, PLR0912
    """Compact Zone -> HH Samples table with optional Zone Group column."""
    # Use original zone IDs for sample counts (zone groups remap ctrl_geoid)
    count_col = "_orig_ctrl_geoid" if "_orig_ctrl_geoid" in seed.columns else "ctrl_geoid"
    sample_counts = dict(
        seed.filter(pl.col(count_col).is_not_null()).group_by(count_col).len().iter_rows()
    )

    # Zone group mapping (original zone → group name)
    zone_to_group: dict[str, str] = {}
    if "zone_group" in seed.columns:
        zone_to_group = dict(
            seed.filter(pl.col("zone_group").is_not_null())
            .select(count_col, "zone_group")
            .unique()
            .iter_rows()
        )
    has_groups = bool(zone_to_group)

    raw_zones = (
        crosswalk_df.select("ctrl_geoid")
        .unique()
        .filter(pl.col("ctrl_geoid").is_not_null())
        .to_series()
        .to_list()
    )
    # Sort by zone group then descending HH samples so group members stay
    # contiguous (needed for correct rowspan) and larger zones appear first.
    zones = sorted(
        raw_zones,
        key=lambda g: (zone_to_group.get(g, ""), -(sample_counts.get(g, 0))),
    )

    headers = []
    if has_groups:
        headers += ["Zone Group", "Grouped HH Samples"]
    headers += ["Zone", "HH Samples"]
    header = "<tr>" + "".join(_tag("th", h) for h in headers) + "</tr>"

    # Build runs of consecutive zones sharing the same group for rowspan
    group_runs: list[tuple[str, int]] = []  # (group_name, span)
    if has_groups:
        for geo in zones:
            grp = zone_to_group.get(geo, "")
            if group_runs and group_runs[-1][0] == grp and grp:
                group_runs[-1] = (grp, group_runs[-1][1] + 1)
            else:
                group_runs.append((grp, 1))

    # Grouped sample totals
    group_sample_totals: dict[str, int] = {}
    if has_groups:
        for geo, grp in zone_to_group.items():
            group_sample_totals[grp] = group_sample_totals.get(grp, 0) + sample_counts.get(geo, 0)

    body_rows: list[str] = []
    run_idx = 0
    pos_in_run = 0
    for geo in zones:
        cells = ""
        if has_groups:
            grp = zone_to_group.get(geo, "")
            if grp and group_runs:
                _, span = group_runs[run_idx]
                if pos_in_run == 0:
                    rs = f' rowspan="{span}"' if span > 1 else ""
                    cells += f"<td{rs}>{grp}</td>"
                    total = group_sample_totals.get(grp, 0)
                    cells += f"<td{rs}>{total:,}</td>"
                pos_in_run += 1
                if pos_in_run >= span:
                    run_idx += 1
                    pos_in_run = 0
            else:
                cells += _tag("td", "")
                cells += _tag("td", "")
        cells += f'<td style="text-align:left">{geo}</td>'
        cells += _tag("td", f"{sample_counts.get(geo, 0):,}")
        body_rows.append(f"<tr>{cells}</tr>")

    return f"<table>\n{header}\n" + "\n".join(body_rows) + "\n</table>"
