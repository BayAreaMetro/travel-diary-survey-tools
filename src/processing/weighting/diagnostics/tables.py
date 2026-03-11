"""HTML table builders for the diagnostics report."""

import polars as pl

from processing.weighting.balancing.balancer import ZoneStatus
from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import resolve_targets

from .data import category_label_map

# ---------------------------------------------------------------------------
# HTML primitives
# ---------------------------------------------------------------------------


def _tag(name: str, text: str, **attrs: str) -> str:
    """Wrap *text* in an HTML element with optional attributes."""
    attr_str = "".join(f' {k.rstrip("_")}="{v}"' for k, v in attrs.items()) if attrs else ""
    return f"<{name}{attr_str}>{text}</{name}>"


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
# Section 2 — Convergence & Weight Summary
# ---------------------------------------------------------------------------


def convergence_table(
    statuses: list[ZoneStatus],
    weighted: pl.DataFrame,
) -> str:
    """Per-zone convergence status, weight sums, and ESS%."""
    has_bw = "base_weight" in weighted.columns
    status_map = {s.geo_id: s for s in statuses}
    zones = sorted(status_map)

    headers = ["Zone", "Conv?", "Iter", "N", "&sum; base_wt", "&sum; hh_wt", "ESS %", "CV"]
    header_row = "<tr>" + "".join(_tag("th", h) for h in headers) + "</tr>"

    data_rows: list[str] = []
    for z in zones:
        s = status_map[z]
        zone_df = weighted.filter(pl.col("ctrl_geoid") == z)
        zone_w = zone_df["hh_weight"]
        n = len(zone_w)
        mean_w = zone_w.mean() or 0
        sum_w = zone_w.sum()
        sum_w2 = (zone_w * zone_w).sum()
        ess_pct = ((sum_w**2 / sum_w2) / n * 100) if sum_w2 > 0 and n > 0 else 0.0
        cv = f"{zone_w.std() / mean_w:.3f}" if mean_w else "N/A"
        sum_bw = zone_df["base_weight"].sum() if has_bw else sum_w

        css = "converged" if s.converged else "failed"
        cells = [
            _tag("td", z),
            f'<td class="{css}">{"Y" if s.converged else "N"}</td>',
            _tag("td", str(s.iterations)),
            _tag("td", f"{n:,}"),
            _tag("td", f"{sum_bw:,.0f}"),
            _tag("td", f"{sum_w:,.0f}"),
            _tag("td", f"{ess_pct:.1f}%"),
            _tag("td", cv),
        ]
        data_rows.append("<tr>" + "".join(cells) + "</tr>")

    return "<table>\n" + header_row + "\n" + "\n".join(data_rows) + "\n</table>"


# ---------------------------------------------------------------------------
# Section 3 — Target Fit
# ---------------------------------------------------------------------------


def target_fit_table(
    statuses: list[ZoneStatus],
    zone_fit: pl.DataFrame,
) -> str:
    """Per-zone fit metrics: household/person targets and error statistics."""
    status_map = {s.geo_id: s for s in statuses}
    fit_map = {r["geo_id"]: r for r in zone_fit.iter_rows(named=True)}
    zones = sorted(status_map)

    # Two-row grouped header
    group_row = (
        "<tr>"
        '<th rowspan="2">Zone</th>'
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
        f = fit_map.get(z, {})
        cells = [
            _tag("td", z),
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
# Section 3 — Weight distribution
# ---------------------------------------------------------------------------


def _weight_stats(w: pl.Series, bw: pl.Series | None) -> dict[str, str]:
    """Summary statistics for a single weight Series."""
    mean = w.mean() or 0
    stats = {
        "n": f"{len(w):,}",
        "mean": f"{mean:,.2f}",
        "median": f"{w.median():,.2f}",
        "std": f"{w.std():,.2f}",
        "min": f"{w.min():,.2f}",
        "max": f"{w.max():,.2f}",
        "cv": f"{w.std() / mean:.3f}" if mean else "N/A",  # pyright: ignore[reportOperatorIssue]
        "min_ef": "",
        "max_ef": "",
        "mean_ef": "",
        "median_ef": "",
    }
    if bw is not None and len(bw) > 0:
        ratio = w / bw
        stats["min_ef"] = f"{ratio.min():.3f}"
        stats["max_ef"] = f"{ratio.max():.3f}"
        stats["mean_ef"] = f"{ratio.mean():.3f}"
        stats["median_ef"] = f"{ratio.median():.3f}"
    return stats


def weight_distribution_table(weighted: pl.DataFrame) -> str:
    """Per-zone + total weight distribution table."""
    has_bw = "base_weight" in weighted.columns
    headers = ["Zone", "N", "Mean", "Median", "Std", "Min", "Max", "CV"]
    if has_bw:
        headers += ["Min&nbsp;EF", "Max&nbsp;EF", "Mean&nbsp;EF", "Median&nbsp;EF"]

    def _row(label: str, df: pl.DataFrame) -> list[str]:
        bw = df["base_weight"] if has_bw else None
        s = _weight_stats(df["hh_weight"], bw)
        row = [label, s["n"], s["mean"], s["median"], s["std"], s["min"], s["max"], s["cv"]]
        if has_bw:
            row += [s["min_ef"], s["max_ef"], s["mean_ef"], s["median_ef"]]
        return row

    zones = sorted(weighted["ctrl_geoid"].unique().to_list())
    rows = [_row(str(z), weighted.filter(pl.col("ctrl_geoid") == z)) for z in zones]
    rows.append(_row("TOTAL", weighted))
    return _html_table(headers, rows)


# ---------------------------------------------------------------------------
# Section 5 — Unweighted cell counts (data sparsity matrix)
# ---------------------------------------------------------------------------

_LOW_COUNT_THRESHOLD = 30


def _count_cell(count: int) -> str:
    """Format a count, highlighting values < 30 in red."""
    if count < _LOW_COUNT_THRESHOLD:
        return f'<td style="color:#c33;font-weight:bold">{count}</td>'
    return _tag("td", str(count))


def unweighted_cell_counts(seed: pl.DataFrame, target_names: list[str]) -> str:
    """Single matrix table: categories (rows) x zones (columns).

    Row headers are grouped by control name using ``<th rowspan>``.
    A level separator row (Household / Person) divides the two groups.
    """
    labels = category_label_map(target_names)
    zones = sorted(seed["ctrl_geoid"].unique().to_list())
    zone_dfs = {z: seed.filter(pl.col("ctrl_geoid") == z) for z in zones}
    n_zone_cols = len(zones)

    header = (
        "<tr>"
        + _tag("th", "Control")
        + _tag("th", "Category")
        + "".join(_tag("th", str(z)) for z in zones)
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
            f'<tr><td colspan="{n_zone_cols + 2}" '
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
                for z in zones:
                    zdf = zone_dfs[z]
                    if level == ControlLevel.HOUSEHOLD:
                        count = zdf.filter(pl.col(ctrl.name) == value).height
                    else:
                        col = f"{ctrl.name}__{member.lower()}"
                        count = int(zdf[col].sum()) if col in seed.columns else 0
                    cells += _count_cell(count)
                data_rows.append(f"<tr>{cells}</tr>")

    return f"<table>\n{header}\n" + "\n".join(data_rows) + "\n</table>"


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

    zones = sorted(
        crosswalk_df.select("ctrl_geoid")
        .unique()
        .filter(pl.col("ctrl_geoid").is_not_null())
        .to_series()
        .to_list()
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
