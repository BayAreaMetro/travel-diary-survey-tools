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

from processing.weighting.balancing.balancer import ZoneStatus
from processing.weighting.data_prep.control_data import ControlTotals

from .charts import crosswalk_figure, fit_diverging_figure, violins_figure
from .data import apply_fit_merges, compute_weighted_totals, fit_table, zone_fit_summary
from .tables import (
    crosswalk_summary_table,
    unweighted_cell_counts,
    weight_distribution_table,
    zone_overview_table,
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


def generate_report(
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
) -> Path:
    """Write the self-contained HTML diagnostics report to *output_path*."""
    weighted = seed.join(weights.select("hh_id", "hh_weight"), on="hh_id", how="left")
    weighted_totals = compute_weighted_totals(seed, weights, target_names)
    fit = apply_fit_merges(fit_table(control_totals, weighted_totals), merge_specs)
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

    ctx = {
        "title": "Weighting Diagnostics Report",
        "crosswalk_section": crosswalk_section,
        "crosswalk_table": crosswalk_table,
        "zone_overview_table": zone_overview_table(statuses, weighted, zf),
        "weight_distribution_table": weight_distribution_table(weighted),
        "fit_bars_html": fit_diverging_figure(fit, target_names, merges=merge_specs).to_html(
            full_html=False,
            include_plotlyjs=False,
            config={"responsive": False},
        ),
        "violins_html": violins_figure(weighted).to_html(
            full_html=False,
            include_plotlyjs=False,
            config={"responsive": False},
        ),
        "sparsity_html": unweighted_cell_counts(seed, target_names),
    }

    html = _TEMPLATE.render(**ctx)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    logger.info("Diagnostics report written to %s", output_path)
    return output_path
