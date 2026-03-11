"""Diagnostics sub-package — HTML report generation for weighting results.

Produces a self-contained interactive HTML report (Plotly + Jinja2, no
external dependencies) with the following sections:

1. **Crosswalk Map** — geographic crosswalk visualization.
2. **Convergence & Weight Summary** — per-zone convergence status,
   weight sums, ESS%, and CV.
3. **Target Fit** — per-zone fit metrics (HH/person targets, MAPE,
   P90, Max error).
4. **Weight Distribution** — violin / jitter plots of
   ``final_weight / base_weight`` per zone, with summary statistics.
5. **Target Fit (% Error)** — diverging bar charts per zone.
6. **Unweighted Cell Counts (Data Sparsity)** — seed counts per
   control category per zone.

Future:
7. **Expansion Factor Calibration** — MAPE vs CV across a grid of
   ``max_expansion_factor`` values (not yet implemented).

Configuration (YAML)::

    diagnostics:
      enabled: true
      output_path: "weighting_diagnostics.html"
      fit_error_thresholds: [2, 5]
      min_seed_count_warning: 10
      expansion_factor_grid: [2, 4, 6, 8, 10, 15, 20, 30, 50]
      plotly_cdn: true
"""

from .charts import crosswalk_figure
from .report import generate_report

__all__ = ["crosswalk_figure", "generate_report"]
