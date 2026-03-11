"""Diagnostics sub-package — HTML report generation for weighting results.

Produces a self-contained interactive HTML report (Plotly + Jinja2, no
external dependencies) with the following sections:

1. **Recode Coverage** — null-leak summary per control.
2. **Weight Summary** — initial, final, and target weight sums.
3. **Target Fit** — % error bar charts per zone (green < 2%, yellow 2-5%,
   red > 5%).
4. **Expansion Factor Calibration** — MAPE vs CV across a grid of
   ``max_expansion_factor`` values.
5. **Weight Distribution** — violin / jitter plots of
   ``final_weight / base_weight`` per zone.
6. **Seed vs Targets** — detailed table of every control cell in every zone.
7. **Convergence & ESS** — per-zone convergence metadata, effective sample
   size, design effect.

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
