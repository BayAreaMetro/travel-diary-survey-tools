"""Diagnostics sub-package — HTML report generation for weighting results.

Produces one self-contained interactive HTML report per *run* (Plotly + Jinja2,
no external dependencies), covering every profile the run fitted. The crosswalk
geometry, control totals and PUMS incidence are built once however many profiles
are fitted -- and on a regional run that geometry is the great majority of the
file -- so one document per run embeds them once rather than once per profile.

# Run-level sections, always visible

1. **Profile Comparison** — one row per fit: seed size, weight sums by level,
   ESS%, CV, max/median, MAPE and convergence. Two profiles matching exactly
   means two balancing runs produced one answer.
2. **Crosswalk Map** — geographic crosswalk visualization, with every profile's
   seed count in each zone's tooltip. Boundaries are simplified to a few metres.
3. **Weight Cascade** — per level, what each profile admitted and what carries
   weight, all profiles side by side; plus the redistribution each survivor
   absorbs, the day-split identity, and control-geography coverage.

# Per-profile sections, behind the profile toggle

4. **Fractional Seed Imputation** — per-control null rate in the seed incidence
   matrix and the quality of the PUMS-trained model that filled it. Shown only
   when something was filled. This describes the seed handed to the balancer,
   not the survey ``imputation`` pipeline step.
5. **Balancer Performance** — per-zone convergence status, target fit (MAPE,
   P90, Max), CV and ESS%.
6. **Weight Quality** — per-zone weight and expansion-factor statistics, with
   violin plots of ``final_weight / base_weight``.
7. **Expansion Factor Calibration** — MAPE vs CV across a grid of
   ``max_expansion_factor`` values. Enabled by setting ``expansion_factor_grid``
   in the weighting config.
8. **Target Fit (% Error)** — diverging bar charts per control category per
   zone, with PUMS replicate-weight whiskers.
9. **Unweighted Cell Counts (Data Sparsity)** — seed counts per control category
   per zone.

Each per-profile heading names its profile, so a screenshot cropped out of the
document still says which fit it describes.

# Configuration (YAML)

```yaml
diagnostics:
  output_path: "{{ output_dir }}/weighting_diagnostics.html"
```

When `output_path` is omitted the report is written to
``<cache_dir>/diagnostics.html``. One file per run, with no profile suffix.
"""

from .charts import crosswalk_figure
from .report import generate_report

__all__ = ["crosswalk_figure", "generate_report"]
