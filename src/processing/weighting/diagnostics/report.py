"""Report orchestration: assemble sections and render the Jinja2 template.

Entry point is :func:`generate_report`, which collects data from the
balancer run, builds Plotly figures and HTML tables via the sibling
modules (``charts``, ``data``, ``tables``), then renders everything
into a single ``.html`` file using a bundled Jinja2 template.
"""

import logging
from pathlib import Path

import jinja2
import polars as pl
from geopandas import GeoDataFrame

from processing.weighting.specs import ControlTotals, GridPoint, ZoneStatus

from .charts import crosswalk_figure, ef_tradeoff_figure, fit_diverging_figure, violins_figure
from .data import apply_fit_merges, compute_weighted_totals, fit_table, zone_fit_summary
from .tables import (
    balancer_performance_table,
    crosswalk_summary_table,
    unweighted_cell_counts,
    weight_quality_table,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Template setup
# ---------------------------------------------------------------------------

_TEMPLATE_DIR = Path(__file__).parent
_ENV = jinja2.Environment(
    loader=jinja2.FileSystemLoader(_TEMPLATE_DIR),
    autoescape=False,  # noqa: S701
    undefined=jinja2.StrictUndefined,
)
_TEMPLATE = _ENV.get_template("diagnostics_template.html")


def generate_report(  # noqa: PLR0913
    seed: pl.DataFrame,
    weights: pl.DataFrame,
    control_totals: ControlTotals,
    target_names: list[str],
    statuses: list[ZoneStatus],
    output_path: Path,
    *,
    puma_gdf: GeoDataFrame | None = None,
    target_gdf: GeoDataFrame | None = None,
    crosswalk_df: pl.DataFrame | None = None,
    zone_groups: dict[str, list[str]] | None = None,
    merge_specs: list | None = None,
    grid_results: list[GridPoint] | None = None,
    selected_ef: float | None = None,
    control_moe: pl.DataFrame | None = None,
) -> Path:
    """Write the self-contained HTML diagnostics report to *output_path*."""
    weighted = seed.join(weights.select("hh_id", "hh_weight"), on="hh_id", how="left")
    weighted_totals = compute_weighted_totals(seed, weights, target_names)
    fit = apply_fit_merges(fit_table(control_totals, weighted_totals), merge_specs, target_names)

    # Join per-cell MOE from PUMS replicate weights (when available)
    if control_moe is not None:
        moe_cols = control_moe.select("geo_id", "control_name", "category", "moe_pct")
        fit = fit.join(moe_cols, on=["geo_id", "control_name", "category"], how="left")

    zf = zone_fit_summary(fit, target_names)

    # Section 1 — crosswalk map
    if puma_gdf is not None and target_gdf is not None and crosswalk_df is not None:
        fig = crosswalk_figure(
            puma_gdf=puma_gdf,
            target_gdf=target_gdf,
            crosswalk_df=crosswalk_df,
            households=seed,
            zone_groups=zone_groups,
        )
        xw_div = fig.to_html(
            full_html=False,
            include_plotlyjs=False,
            config={"responsive": False},
        )
        crosswalk_section = (
            f'<h2>1 &mdash; Crosswalk Map</h2>\n<div class="chart-map">{xw_div}</div>'
        )
    else:
        crosswalk_section = ""

    # Section 1b — crosswalk summary table
    if crosswalk_df is not None:
        crosswalk_table = crosswalk_summary_table(crosswalk_df, seed)
    else:
        crosswalk_table = ""

    # Section — EF tradeoff chart (optional)
    if grid_results and selected_ef is not None:
        ef_fig = ef_tradeoff_figure(grid_results, selected_ef)
        ef_div = ef_fig.to_html(
            full_html=False,
            include_plotlyjs=False,
            config={"responsive": False},
        )
        ef_tradeoff_section = (
            "<h2>4 &mdash; Expansion Factor Calibration</h2>\n"
            '<p class="note">\n'
            "  Tradeoff between target fit (left axis) and weight quality "
            "(right axis) across a grid of <code>max_expansion_factor</code> "
            "values.  Tighter bounds (lower EF) yield more stable weights "
            "(lower CV, higher ESS%) but may degrade fit (higher MAPE/P90). "
            "The dashed vertical line marks the selected production EF.\n"
            "</p>\n"
            f'<div class="chart">{ef_div}</div>'
        )
    else:
        ef_tradeoff_section = ""

    ctx = {
        "title": "Weighting Diagnostics Report",
        "crosswalk_section": crosswalk_section,
        "crosswalk_table": crosswalk_table,
        "balancer_performance_table": balancer_performance_table(statuses, weighted, zf),
        "weight_quality_table": weight_quality_table(weighted),
        "fit_bars_html": fit_diverging_figure(fit).to_html(
            full_html=False,
            include_plotlyjs=False,
            config={"responsive": False},
        ),
        "violins_html": violins_figure(weighted).to_html(
            full_html=False,
            include_plotlyjs=False,
            config={"responsive": False},
        ),
        "sparsity_html": unweighted_cell_counts(seed, target_names, control_totals),
        "ef_tradeoff_section": ef_tradeoff_section,
    }

    html = _TEMPLATE.render(**ctx)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    logger.info("Diagnostics report written to %s", output_path)
    return output_path
