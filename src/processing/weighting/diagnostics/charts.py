"""Plotly chart builders for the diagnostics report."""

import math
from collections import defaultdict

import geopandas as gpd
import plotly.graph_objects as go
import polars as pl
from plotly.subplots import make_subplots

_WARN_PCT = 10  # bright-red threshold


def fit_diverging_figure(
    fit: pl.DataFrame,
) -> go.Figure:
    """Grid of horizontal diverging bar charts (% error, one panel per zone + overall).

    Expects *fit* to contain a ``label`` column (added by
    :func:`~.data.apply_fit_merges`).  Null placeholder rows are rendered
    as invisible bars so the y-axis remains consistent across panels.
    """
    zones = sorted(fit["geo_id"].unique().to_list())

    overall = (
        fit.group_by("control_name", "category", "label")
        .agg(pl.col("target_total").sum(), pl.col("weighted_total").sum())
        .with_columns(
            ((pl.col("weighted_total") - pl.col("target_total")) / pl.col("target_total") * 100)
            .fill_nan(0)
            .fill_null(0)
            .alias("diff_pct"),
            (pl.col("weighted_total") - pl.col("target_total")).alias("diff"),
        )
        .sort("control_name", "category", "label")
    )

    panels = [*zones, "Overall"]
    n_cols = min(4, len(panels))
    n_rows = math.ceil(len(panels) / n_cols)

    fig = make_subplots(
        rows=n_rows,
        cols=n_cols,
        subplot_titles=[str(p) for p in panels],
        shared_xaxes=False,
        shared_yaxes=True,
        horizontal_spacing=0.03,
        vertical_spacing=max(0.03, 0.15 / max(n_rows, 1)),
    )

    # Consistent y-label ordering from the overall panel
    cat_labels = [r["label"] for r in overall.iter_rows(named=True)]

    for idx, panel in enumerate(panels):
        r_idx, c_idx = divmod(idx, n_cols)
        pdf = (
            overall
            if panel == "Overall"
            else fit.filter(pl.col("geo_id") == panel).sort("control_name", "category", "label")
        )

        y, x, colors, hovers = [], [], [], []
        for r in pdf.iter_rows(named=True):
            lbl = r["label"]
            target = r["target_total"]

            # Null placeholder -> invisible bar for consistent y-axis
            if target is None:
                y.append(lbl)
                x.append(None)
                colors.append("rgba(0,0,0,0)")
                hovers.append("")
                continue

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
                f"Target: {target:,.1f}<br>"
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

    fig.update_xaxes(
        zeroline=True,
        zerolinewidth=1,
        zerolinecolor="black",
        title_text="% Error",
        matches="x",  # shared x-range across all panels
    )
    fig.update_yaxes(tickfont_size=9)
    fig.update_layout(
        autosize=False,
        width=960,
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
        autosize=False,
        width=960,
        height=max(400, 60 * len(zones)),
        margin={"l": 60, "r": 20, "t": 40, "b": 40},
        updatemenus=[
            {
                "type": "buttons",
                "direction": "left",
                "x": 1.0,
                "y": 1.02,
                "xanchor": "right",
                "yanchor": "bottom",
                "buttons": [
                    {"label": "Log", "method": "relayout", "args": [{"yaxis.type": "log"}]},
                    {"label": "Linear", "method": "relayout", "args": [{"yaxis.type": "linear"}]},
                ],
            }
        ],
    )
    return fig


# ---------------------------------------------------------------------------
# Crosswalk map
# ---------------------------------------------------------------------------

# Target zones — per-zone traces for group-aware colouring
_GROUP_FILLS = [
    "rgba(70,130,180,0.15)",  # steel blue (default / ungrouped)
    "rgba(180,100,50,0.15)",  # burnt orange
    "rgba(100,170,80,0.15)",  # olive green
    "rgba(150,80,160,0.15)",  # plum
    "rgba(200,170,50,0.15)",  # gold
    "rgba(80,170,170,0.15)",  # teal
]
_GROUP_BORDERS = [
    "steelblue",
    "rgb(180,100,50)",
    "rgb(100,170,80)",
    "rgb(150,80,160)",
    "rgb(200,170,50)",
    "rgb(80,170,170)",
]


def _build_tooltips(
    target_4326: gpd.GeoDataFrame,
    xw: pl.DataFrame,
    sample_counts: dict[str, int],
) -> dict[str, str]:
    """Build per-zone tooltips showing PUMA allocation weights."""
    tooltips: dict[str, str] = {}
    for geo_id in target_4326["ctrl_geoid"]:
        rows = xw.filter(pl.col("ctrl_geoid") == geo_id).sort(
            "allocation_weight",
            descending=True,
        )
        if rows.is_empty():
            tooltips[geo_id] = f"Zone {geo_id}: no PUMA overlap"
            continue
        zone_pop = rows["population"].sum()
        header = f"Zone {geo_id} (BG 2020 person pop {zone_pop:,.0f})"
        if geo_id in sample_counts:
            header += f" — {sample_counts[geo_id]:,} sample HH"
        lines = [header]
        for r in rows.iter_rows(named=True):
            aw = r["allocation_weight"] * 100
            # Drop slivers below 0.01% allocation to reduce tooltip noise (outer joins)
            if aw < 0.01:  # noqa: PLR2004
                continue
            lines.append(f"  PUMA {r['puma_id']}: {aw:.2f}%")
        tooltips[geo_id] = "<br>".join(lines)
    return tooltips


def _build_zone_labels_and_colors(
    target_4326: gpd.GeoDataFrame,
    zone_to_group_idx: dict[str, int],
    zone_to_group_name: dict[str, str],
) -> tuple[list[str], list[str]]:
    """Build zone labels and colors for centroid labels."""
    zone_labels: list[str] = []
    zone_colors: list[str] = []
    for gid in target_4326["ctrl_geoid"]:
        gname = zone_to_group_name.get(gid)
        label = f"Zone {gid} ({gname})" if gname else f"Zone {gid}"
        zone_labels.append(label)
        gi = zone_to_group_idx.get(gid, 0)
        zone_colors.append(_GROUP_BORDERS[gi])
    return zone_labels, zone_colors


def _build_zone_group_index(
    target_4326: gpd.GeoDataFrame,
    zone_groups: dict[str, list[str]] | None = None,
) -> dict[str, int]:
    """Build mapping from zone ID to group index."""
    zone_to_group_idx: dict[str, int] = {}
    next_idx = 1  # 0 is reserved; explicit groups start at 1
    if zone_groups:
        for zones in zone_groups.values():
            idx = next_idx % len(_GROUP_FILLS)
            for z in zones:
                zone_to_group_idx[z] = idx
            next_idx += 1

    # Assign each ungrouped zone its own cycling colour
    for _, row in target_4326.iterrows():
        gid = row["ctrl_geoid"]
        if gid not in zone_to_group_idx:
            zone_to_group_idx[gid] = next_idx % len(_GROUP_FILLS)
            next_idx += 1

    return zone_to_group_idx


def _add_zone_traces(
    fig: go.Figure,
    target_4326: gpd.GeoDataFrame,
    zone_to_group_idx: dict[str, int],
) -> None:
    """Add target zone traces grouped by color."""
    group_rows: dict[int, list] = defaultdict(list)
    group_tooltips: dict[int, list[str]] = defaultdict(list)
    for _, row in target_4326.iterrows():
        gi = zone_to_group_idx.get(row["ctrl_geoid"], 0)
        group_rows[gi].append(row)
        group_tooltips[gi].append(row["tooltip"])

    for gi, rows in group_rows.items():
        fill = _GROUP_FILLS[gi]
        border = _GROUP_BORDERS[gi]
        batch = gpd.GeoDataFrame(rows, crs=target_4326.crs)
        fig.add_trace(
            go.Choroplethmap(
                geojson=batch.__geo_interface__,
                locations=batch.index,
                z=[0] * len(batch),
                colorscale=[[0, fill], [1, fill]],
                marker_line_color=border,
                marker_line_width=2,
                showscale=False,
                text=group_tooltips[gi],
                hoverinfo="text",
            )
        )


def _add_zone_label_traces(
    fig: go.Figure,
    zone_centroids: gpd.GeoSeries,
    zone_labels: list[str],
    zone_colors: list[str],
) -> None:
    """Add zone label traces grouped by color."""
    label_groups: dict[str, tuple[list[float], list[float], list[str]]] = defaultdict(
        lambda: ([], [], [])
    )
    for i, pt in enumerate(zone_centroids):
        c = zone_colors[i]
        lons, lats, texts = label_groups[c]
        lons.append(pt.x)  # pyright: ignore[reportAttributeAccessIssue]
        lats.append(pt.y)  # pyright: ignore[reportAttributeAccessIssue]
        texts.append(zone_labels[i])

    for color, (lons, lats, texts) in label_groups.items():
        fig.add_trace(
            go.Scattermap(
                lon=lons,
                lat=lats,
                mode="text",
                text=texts,
                textfont={"size": 12, "color": color},
                hoverinfo="skip",
                showlegend=False,
            )
        )


def crosswalk_figure(
    puma_gdf: gpd.GeoDataFrame,
    target_gdf: gpd.GeoDataFrame,
    crosswalk_df: pl.DataFrame,
    households: pl.DataFrame | None = None,
    zone_groups: dict[str, list[str]] | None = None,
) -> go.Figure:
    """Build an interactive Plotly map of the crosswalk.

    Layers:
    - PUMA boundaries (dashed grey) — full extent
    - Study area outline (bold black)
    - Target zones (solid border, transparent fill) with tooltip
      showing PUMA allocation weights from the crosswalk.

    Parameters
    ----------
    puma_gdf : gpd.GeoDataFrame
        PUMA boundary polygons (must have ``puma_id`` column).
    target_gdf : gpd.GeoDataFrame
        Target zone polygons (must have ``ctrl_geoid`` column).
    crosswalk_df : pl.DataFrame
        Crosswalk table with ``puma_id``, ``ctrl_geoid``,
        ``population``, ``allocation_weight``.
    households : pl.DataFrame | None
        Assigned households (must contain ``ctrl_geoid``).  When
        provided, per-zone sample counts appear in the tooltip.
    zone_groups : dict[str, list[str]] | None
        Optional zone group mapping.  When provided, grouped zones
        share a fill colour and labels include the group name.

    Returns:
    -------
    go.Figure
    """
    puma_4326 = puma_gdf.to_crs("EPSG:4326")
    target_4326 = target_gdf.to_crs("EPSG:4326")
    study_boundary = target_4326.dissolve()

    # Build tooltip per target zone showing allocation weights
    xw = crosswalk_df.select(
        "puma_id",
        "ctrl_geoid",
        "population",
        "allocation_weight",
    )

    # Per-zone sample counts from assigned households (Polars only)
    sample_counts: dict[str, int] = {}
    if households is not None and "ctrl_geoid" in households.columns:
        sample_counts = dict(
            households.filter(pl.col("ctrl_geoid").is_not_null())
            .group_by("ctrl_geoid")
            .len()
            .iter_rows()
        )

    tooltips = _build_tooltips(target_4326, xw, sample_counts)
    target_4326 = target_4326.copy()
    target_4326["tooltip"] = target_4326["ctrl_geoid"].map(tooltips)

    fig = go.Figure()

    # PUMAs -- full boundaries, outline only
    fig.add_trace(
        go.Choroplethmap(
            geojson=puma_4326.__geo_interface__,
            locations=puma_4326.index,
            z=[0] * len(puma_4326),
            colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(0,0,0,0)"]],
            marker_line_color="rgba(100,100,100,0.7)",
            marker_line_width=1,
            showscale=False,
            hoverinfo="skip",
        )
    )

    # Study area -- bold black outline
    fig.add_trace(
        go.Choroplethmap(
            geojson=study_boundary.__geo_interface__,
            locations=study_boundary.index,
            z=[0] * len(study_boundary),
            colorscale=[[0, "rgba(0,0,0,0)"], [1, "rgba(0,0,0,0)"]],
            marker_line_color="black",
            marker_line_width=3,
            showscale=False,
            hoverinfo="skip",
        )
    )

    # Build zone grouping
    zone_to_group_idx = _build_zone_group_index(target_4326, zone_groups)

    # Add target zone traces
    _add_zone_traces(fig, target_4326, zone_to_group_idx)

    # PUMA labels at centroids (grey)
    puma_centroids = puma_4326.to_crs("EPSG:5070").centroid.to_crs("EPSG:4326")
    fig.add_trace(
        go.Scattermap(
            lon=[p.x for p in puma_centroids],  # pyright: ignore[reportAttributeAccessIssue]
            lat=[p.y for p in puma_centroids],  # pyright: ignore[reportAttributeAccessIssue]
            mode="text",
            text=[f"PUMA {pid}" for pid in puma_4326["puma_id"]],
            textfont={"size": 10, "color": "gray"},
            hoverinfo="skip",
            showlegend=False,
        )
    )

    # Zone labels at centroids — colour-matched to group
    zone_centroids = target_4326.to_crs("EPSG:5070").centroid.to_crs("EPSG:4326")
    zone_to_group_name: dict[str, str] = {}
    if zone_groups:
        for gname, zones in zone_groups.items():
            for z in zones:
                zone_to_group_name[z] = gname

    zone_labels, zone_colors = _build_zone_labels_and_colors(
        target_4326, zone_to_group_idx, zone_to_group_name
    )

    # Add zone label traces
    _add_zone_label_traces(fig, zone_centroids, zone_labels, zone_colors)

    # Layout -- use PUMA bounds so full boundaries are visible
    bounds = puma_4326.total_bounds
    fig.update_layout(
        map={
            "style": "carto-positron",
            "center": {"lat": (bounds[1] + bounds[3]) / 2, "lon": (bounds[0] + bounds[2]) / 2},
            "zoom": 8,
        },
        autosize=False,
        width=960,
        height=700,
        margin={"l": 0, "r": 0, "t": 30, "b": 0},
        title="Crosswalk: PUMA to Target Zones",
    )

    return fig
