"""Plotly chart builders for the diagnostics report."""

import math

import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

from .data import category_label_map

_WARN_PCT = 25  # bright-red threshold


def fit_diverging_figure(fit: pl.DataFrame, target_names: list[str]) -> go.Figure:
    """Grid of horizontal diverging bar charts (% error, one panel per zone + overall)."""
    labels = category_label_map(target_names)
    zones = sorted(fit["geo_id"].unique().to_list())

    overall = (
        fit.group_by("control_name", "category")
        .agg(pl.col("target_total").sum(), pl.col("weighted_total").sum())
        .with_columns(
            ((pl.col("weighted_total") - pl.col("target_total")) / pl.col("target_total") * 100)
            .fill_nan(0)
            .fill_null(0)
            .alias("diff_pct"),
            (pl.col("weighted_total") - pl.col("target_total")).alias("diff"),
        )
        .sort("control_name", "category")
    )

    panels = [*zones, "Overall"]
    n_cols = min(4, len(panels))
    n_rows = math.ceil(len(panels) / n_cols)

    fig = make_subplots(
        rows=n_rows,
        cols=n_cols,
        subplot_titles=[str(p) for p in panels],
        shared_xaxes=True,
        shared_yaxes=True,
        horizontal_spacing=0.03,
        vertical_spacing=max(0.03, 0.15 / max(n_rows, 1)),
    )

    # Consistent y-label ordering from the overall panel
    cat_labels = [
        labels.get((r["control_name"], r["category"]), f"{r['control_name']}:{r['category']}")
        for r in overall.iter_rows(named=True)
    ]

    for idx, panel in enumerate(panels):
        r_idx, c_idx = divmod(idx, n_cols)
        pdf = (
            overall
            if panel == "Overall"
            else fit.filter(pl.col("geo_id") == panel).sort("control_name", "category")
        )

        y, x, colors, hovers = [], [], [], []
        for r in pdf.iter_rows(named=True):
            lbl = labels.get(
                (r["control_name"], r["category"]),
                f"{r['control_name']}:{r['category']}",
            )
            pct = r["diff_pct"]
            y.append(lbl)
            x.append(pct)
            if abs(pct) > _WARN_PCT:
                colors.append("#c33")
            elif pct > 0:
                colors.append("#d8b365")
            else:
                colors.append("#5ab4ac")
            hovers.append(
                f"<b>{lbl}</b><br>"
                f"Target: {r['target_total']:,.1f}<br>"
                f"Weighted: {r['weighted_total']:,.1f}<br>"
                f"Diff: {r['diff']:+,.1f}<br>"
                f"Diff %: {pct:+.1f}%"
            )

        fig.add_trace(
            go.Bar(
                y=y,
                x=x,
                orientation="h",
                marker_color=colors,
                hovertext=hovers,
                hoverinfo="text",
                showlegend=False,
            ),
            row=r_idx + 1,
            col=c_idx + 1,
        )

    fig.update_xaxes(zeroline=True, zerolinewidth=1, zerolinecolor="black")
    fig.update_yaxes(tickfont_size=9)
    fig.update_layout(
        height=max(350, 18 * len(cat_labels) * n_rows + 40 * n_rows),
        margin={"l": 160, "r": 20, "t": 30, "b": 20},
    )
    return fig


def violins_figure(weighted: pl.DataFrame) -> go.Figure:
    """Violin plot of ``hh_weight`` by zone (log scale)."""
    zones = sorted(weighted["ctrl_geoid"].unique().to_list())
    fig = go.Figure()
    for z in zones:
        w = weighted.filter(pl.col("ctrl_geoid") == z)["hh_weight"].to_list()
        fig.add_trace(
            go.Violin(
                y=w,
                name=str(z),
                box_visible=True,
                meanline_visible=True,
                bandwidth=max(0.05, (max(w) - min(w)) / 40) if w else 0.1,
                spanmode="manual",
                span=[max(1e-6, min(w)), max(w)],
            )
        )
    fig.update_layout(
        showlegend=False,
        title="Household Weight Distribution by Zone",
        yaxis_title="hh_weight",
        yaxis_type="log",
        height=max(400, 60 * len(zones)),
        margin={"l": 60, "r": 20, "t": 40, "b": 40},
    )
    return fig
