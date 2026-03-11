[← Back to Main README](../../../README.md)

# Weighting Pipeline — Development Plan

Remaining features for the weighting module. For documentation of the implemented pipeline, see [README.md](README.md).

---

## Remaining Work

The core pipeline (geography crosswalk → PUMS controls → seed prep → base weights → max-entropy balancing → weight propagation → HTML diagnostics) is fully implemented and operational. The items below extend the pipeline to handle more complex weighting scenarios.

### 1. Cross-Tab Targets 🔲

**Status:** Not implemented. The current control system supports only marginal (single-variable) targets. Each `ControlTarget` maps one PUMS variable to one set of bins.

**Goal:** Allow two or more control variables to be crossed, producing joint targets (e.g., age × sex, income × workers). The control totals become the full Cartesian product of the component categories.

**Design considerations:**
- Cross-tabs must be defined in YAML (see control data spec in [README.md](README.md)).
- The `ControlTarget` base class needs a variant (or a new `CrossTabTarget` subclass) that holds multiple PUMS fields and produces joint category labels (e.g., `"18-34 × female"`).
- The incidence matrix builder (`_build_incidence` in `balancer.py`) must generate a row for each cell in the cross-tab, with incidence = 1 only when a record matches all component categories simultaneously.
- Control totals aggregation in `control_data.py` must group by the joint category.
- Diagnostics already handle arbitrary category labels — no changes needed there.
- Cross-tabs increase the control count multiplicatively, so sparsity warnings (cells with few seed records) become more important.

**Files affected:** `controls/base.py`, `controls/registry.py`, `data_prep/control_data.py`, `data_prep/seed_data.py`, `balancing/balancer.py` (incidence builder).

---

### 2. Custom Trip-Level Targets 🔲

**Status:** Not implemented. The current system controls only household and person-level variables.

**Goal:** Support trip-level aggregate targets such as transit mode share, VMT, average trip distance, or total linked trips by mode. These are not marginal cell counts — they are weighted sums or ratios computed from the trip table.

**Design considerations:**
- Trip targets operate at a different level than household/person controls: the "population" being matched is trips (or trip-miles), not households.
- Each trip target requires: (a) a filter expression identifying which trips contribute (e.g., `mode == "transit"`), (b) a metric (count, sum of distance, etc.), and (c) an external target value (from NTDL, FHWA, or regional counts).
- These cannot be expressed as incidence matrix rows in the standard balancer — they are non-linear constraints (weighted sum of trip attributes, where trip count per household varies).
- **Approach options:**
  - **Post-balancing raking adjustment:** Run the marginal balancer first, then apply iterative proportional fitting (raking) to nudge weights toward trip targets. Simple but may degrade marginal fit.
  - **Augmented incidence matrix:** Pre-compute per-household trip aggregates (e.g., "number of transit trips made by this household") and treat them as continuous incidence values. This linearizes the constraint but changes the interpretation of the incidence row from 0/1 to arbitrary counts.
  - **Two-stage weighting:** Household-level balancing first, then a trip-level calibration layer. More complex but cleanly separates the two constraint types.
- External targets must be supplied in the YAML config (not derived from PUMS).

```yaml
trip_targets:
  - name: transit_trips
    filter: "mode == 'transit'"
    metric: count             # count | sum(field_name)
    target: 425000            # external target value
  - name: total_vmt
    metric: "sum(trip_distance)"
    filter: "mode in ['drive_alone', 'shared_ride_2', 'shared_ride_3plus']"
    target: 89000000          # total VMT from HPMS or regional model
```

**Files affected:** New module (e.g., `balancing/trip_targets.py`), `weighting.py`, config schema, diagnostics.

---

### 3. Day-of-Week Structuring 🔲

**Status:** Not implemented. The development plan spec above describes the design. The `dow.py` module is planned but not yet created.

**Goal:** Expand each household into household-day records so that the balancer assigns separate weights per travel day, correcting for day-of-week sampling imbalance (e.g., surveys that over-sample weekdays relative to weekends).

**Design considerations:**
- Each household-day record inherits the household's stratum variables and is tagged with a `dow_group` label (e.g., "weekday", "weekend").
- The balancer seeds become household-day rows instead of household rows. All existing incidence, importance, and bounding logic applies unchanged — the row count simply increases.
- `dow_groups` are fully configurable in YAML (see spec above).
- Base weight scaling: `base_weight *= (target_days_in_group / observed_days_in_group_for_hh)`.
- Weight propagation must route day-specific weights correctly: `day_weight` comes from the household-day seed; trip weights inherit from the day, not the household.
- This interacts with the trip-targets feature: trip-level targets would need to be evaluated per day-group (e.g., separate weekday vs weekend VMT targets).

```yaml
dow_weighting: true
dow_groups:
  weekday: [1, 2, 3, 4, 5]   # day_of_week: 1=Mon
  weekend: [6, 7]
```

**Files affected:** New `balancing/dow.py`, `data_prep/seed_data.py` (expansion), `balancing/base_weights.py` (scaling), `balancing/weight_propagation.py` (day-specific routing), `weighting.py`.

---

### 4. Platform / Mode Bias Adjustment 🔲

**Status:** Not implemented. Not currently in the development plan.

**Goal:** Correct for systematic biases introduced by the survey collection method — respondents who complete the survey via different platforms (web browser, mobile app, phone interview, paper mail-back) may exhibit different travel behaviors, response rates, and demographic profiles. If the platform mix in the sample doesn't match the population, weighted estimates will be biased.

**Design considerations:**
- This is typically handled as an **additional control variable** if external benchmarks for platform share exist, or as a **propensity-score stratification** if they don't.
- **Option A — Platform as a control:** Add a `survey_platform` control with categories (browser, mobile, phone, paper) and external targets for the share of each. Requires an external source for population-level platform adoption rates (may not exist). Simple to implement — it's just another marginal control.
- **Option B — Propensity weighting:** Model P(platform = X | demographics) using a logistic regression on the survey data. Weight inversely by the propensity to correct for differential response. This is a pre-balancing adjustment to the base weights, not a control target.
- **Option C — Post-stratification raking:** After marginal balancing, add a raking step that adjusts weights to match assumed platform shares. Similar to trip-targets raking.
- For travel diary surveys, the main concern is that phone/paper respondents tend to under-report trips (especially short ones) relative to app-tracked respondents. This is a **measurement bias**, not a sampling bias — weighting alone cannot fix under-reporting, only adjust for differential inclusion.
- Platform information must be present in the survey data (a `survey_mode` or `platform` field).

```yaml
platform_adjustment:
  enabled: true
  method: control          # control | propensity | raking
  field: survey_platform   # canonical field name
  # Only for method: control — external target shares
  targets:
    browser: 0.35
    mobile: 0.25
    phone: 0.30
    paper: 0.10
```

**Files affected:** Depends on method. `control` → just add a new `ControlTarget` subclass + YAML entry. `propensity` → new module (e.g., `balancing/propensity.py`) adjusting base weights. `raking` → post-balancing adjustment in `weighting.py`.

---

### 5. Expansion Factor Grid Search 🔲

**Status:** Not implemented. The `expansion_factor_grid` config key is documented in the diagnostics YAML example but never consumed.

**Goal:** Automatically re-run the balancer across a grid of `max_expansion_factor` values and produce a tradeoff chart so the user can pick the tightest bounds that still achieve acceptable fit. Tighter bounds → more stable weights (lower CV) but potentially worse fit (higher MAPE). The grid search reveals the Pareto frontier.

**Metrics (per grid point, aggregated across all zones):**

| Metric | Axis | Role |
|--------|------|------|
| **MAPE** | Left y (primary, solid) | Average fit — does the balancer hit the targets? |
| **P90** | Left y (secondary, dashed) | Tail fit — are there badly-missed cells hiding behind a good average? |
| **CV** | Right y (primary, solid) | Weight dispersion — are a few records carrying all the weight? |
| **ESS %** | Right y (secondary, dashed) | Effective sample utilisation — interpretable transform of CV: ESS% ≈ 1/(1+CV²) |

MAPE and P90 decrease (improve) as EF increases; CV worsens and ESS% drops. The chart should highlight the user's chosen EF value with a vertical marker.

**Implementation plan:**

1. **`balancing/balancer.py` — `grid_search_expansion_factor()`**

   New public function. Accepts the same `seed`, `control_totals`, `targets`, etc. as `balance_weights()`, plus an `ef_grid: list[float]`. For each EF value, calls `balance_weights()` with that `max_expansion_factor`, collects per-zone `ZoneStatus` and weights, then computes aggregate metrics.

   ```python
   @dataclass
   class GridPoint:
       max_expansion_factor: float
       converged_zones: int
       total_zones: int
       mape: float           # overall (all zones, all controls)
       p90: float            # 90th percentile |% error|
       cv: float             # weight CV (pooled across zones)
       ess_pct: float        # Kish ESS % (pooled)

   def grid_search_expansion_factor(
       seed: pl.DataFrame,
       control_totals: ControlTotals,
       targets: list[str],
       ef_grid: list[float],
       *,
       # same kwargs as balance_weights() minus max_expansion_factor
       ...
   ) -> list[GridPoint]:
   ```

   The outer loop is sequential (each EF run already uses `n_workers` threads for per-zone parallelism internally). Each run reuses the same pre-built seed/totals/incidence — only the upper bounds change.

   The fit metrics (MAPE, P90) require comparing weighted totals against control targets. Reuse `compute_weighted_totals()` and `fit_table()` from `diagnostics/data.py`.

2. **`diagnostics/charts.py` — `ef_tradeoff_figure()`**

   New Plotly figure: dual y-axis scatter+line chart.
   - X-axis: `max_expansion_factor` (log scale, since the grid often spans 2–50).
   - Left y-axis: MAPE (solid line + markers), P90 (dashed line + markers). Label: "Fit error (%)".
   - Right y-axis: CV (solid), ESS% (dashed). Label: "Weight quality".
   - Vertical dashed line at the user's selected EF value.
   - Hover: shows all four metrics + convergence count.

3. **`diagnostics/report.py` — wire into `generate_report()`**

   Add optional `grid_results: list[GridPoint] | None` parameter. When present, render the tradeoff chart in a new section (between convergence and weight distribution). Template gets a `{{ ef_tradeoff_section }}` block.

4. **`weighting.py` — wire into the pipeline step**

   Parse `diagnostics.expansion_factor_grid` from the YAML config. After the main `balance_weights()` call, if the grid is present, call `grid_search_expansion_factor()` with the same inputs and pass results to `generate_report()`.

   ```yaml
   diagnostics:
     enabled: true
     expansion_factor_grid: [2, 4, 6, 8, 10, 15, 20, 30, 50]
   ```

**Design notes:**
- The grid search is **diagnostics-only** — it does not change the actual weights used downstream. The user's configured `max_expansion_factor` determines the production weights; the grid search just shows what would happen with other values.
- Grid runs can be expensive for large zone counts. Consider logging progress (`"Grid search: EF=4 (2/9)..."`) and a config flag to skip it (`expansion_factor_grid: []` or omitted).
- The main balance run's EF value should always be included in the grid (inserted if missing) so the tradeoff chart has a point at the production setting.

**Files affected:** `balancing/balancer.py` (new function + dataclass), `diagnostics/charts.py` (new figure), `diagnostics/report.py` (new section), `diagnostics/diagnostics_template.html` (new template block), `weighting.py` (config parsing + wiring).

---

### Priority and Dependencies

| # | Feature | Depends On | Complexity | Impact |
|---|---------|-----------|------------|--------|
| 1 | Cross-tab targets | — | Medium | High — enables joint demographic controls (age×sex, income×workers) that significantly improve weight quality |
| 2 | Custom trip targets | — | High | Medium — useful for matching to NTD/FHWA but adds architectural complexity |
| 3 | DOW structuring | — | Medium | High — critical for surveys with day-of-week sampling imbalance |
| 4 | Platform bias | — | Low–High (method-dependent) | Medium — important for mixed-mode surveys but many surveys are single-platform |
| 5 | EF grid search | — | Low | Medium — calibration aid; no algorithmic changes, purely diagnostic |

Features 1 and 3 are independent and can be developed in parallel. Feature 2 is architecturally distinct (non-linear constraints). Feature 4 is orthogonal — it modifies inputs (base weights or controls) rather than the balancer itself. Feature 5 is self-contained — it only adds a diagnostic loop and chart with no impact on the core balancer.
