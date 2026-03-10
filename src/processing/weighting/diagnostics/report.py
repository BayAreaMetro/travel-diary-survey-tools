"""Report orchestration: assemble sections and render the Jinja2 template."""

import logging
from pathlib import Path

import jinja2
import plotly.graph_objects as go
import polars as pl

from processing.weighting.balancing.balancer import ZoneStatus
from processing.weighting.data_prep.control_data import ControlTotals

from .charts import fit_diverging_figure, violins_figure
from .data import compute_weighted_totals, fit_table, zone_fit_summary
from .tables import unweighted_cell_counts, weight_distribution_table, zone_overview_table

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

_ESS_NOTE = (
    "Population totals are derived from the first household- and person-level control. "
    "<br><b>MAPE</b> is the mean |%&nbsp;error| across all control categories for the zone. "
    "<br><b>ESS</b> = (&sum;w)<sup>2</sup> / &sum;w<sup>2</sup> (Kish); "
    "above 80% is good, below 50% suggests a few records dominate."
    "<br><b>MAPE</b> is mean absolute percentage error across control categories, "
    "averaged over household- and person-level controls. "
    "<br><b>CV</b> Coefficient of variation of weights: standard deviation / mean. "
    "High CV (e.g. above 0.5) indicates extreme weight variability, "
    "which can lead to unstable estimates."
)
_FIT_NOTE = "Bars show (weighted &minus; target) / target &times; 100. Hover for absolute values."
_SPARSITY_NOTE = (
    "Unweighted household or person counts per control category per zone. "
    "Low counts (&lt;&nbsp;30) flag thin cells where the balancer has little "
    "data to work with."
)


def generate_report(
    seed: pl.DataFrame,
    weights: pl.DataFrame,
    control_totals: ControlTotals,
    target_names: list[str],
    statuses: list[ZoneStatus],
    output_path: Path,
    *,
    crosswalk_fig: go.Figure | None = None,
) -> Path:
    """Write the self-contained HTML diagnostics report to *output_path*."""
    weighted = seed.join(weights.select("hh_id", "hh_weight"), on="hh_id", how="left")
    weighted_totals = compute_weighted_totals(seed, weights, target_names)
    fit = fit_table(control_totals, weighted_totals)
    zf = zone_fit_summary(fit, target_names)

    # Section 1 — crosswalk map
    if crosswalk_fig is not None:
        xw_div = crosswalk_fig.to_html(full_html=False, include_plotlyjs=False)
        crosswalk_section = f'<h2>1 &mdash; Crosswalk Map</h2>\n<div class="chart">{xw_div}</div>'
    else:
        crosswalk_section = ""

    ctx = {
        "title": "Weighting Diagnostics Report",
        "ess_note": _ESS_NOTE,
        "fit_note": _FIT_NOTE,
        "sparsity_note": _SPARSITY_NOTE,
        "crosswalk_section": crosswalk_section,
        "zone_overview_table": zone_overview_table(statuses, weighted, zf),
        "weight_distribution_table": weight_distribution_table(weighted),
        "fit_bars_html": fit_diverging_figure(fit, target_names).to_html(
            full_html=False,
            include_plotlyjs=False,
        ),
        "violins_html": violins_figure(weighted).to_html(
            full_html=False,
            include_plotlyjs=False,
        ),
        "sparsity_html": unweighted_cell_counts(seed, target_names),
    }

    html = _TEMPLATE.render(**ctx)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    logger.info("Diagnostics report written to %s", output_path)
    return output_path
