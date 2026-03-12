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

### 5. Expansion Factor Grid Search ✅

**Status:** Implemented (March 2026).

**Goal:** Automatically re-run the balancer across a grid of `max_expansion_factor` values and produce a tradeoff chart so the user can pick the tightest bounds that still achieve acceptable fit. Tighter bounds → more stable weights (lower CV) but potentially worse fit (higher MAPE). The grid search reveals the Pareto frontier.

**Metrics (per grid point, aggregated across all zones):**

| Metric | Chart | Role |
|--------|-------|------|
| **MAPE** | Subplot 1 | Average fit — does the balancer hit the targets? |
| **P90** | Subplot 2 | Tail fit — are there badly-missed cells hiding behind a good average? |
| **CV** | Subplot 3 | Weight dispersion — are a few records carrying all the weight? |
| **ESS %** | Subplot 4 | Effective sample utilisation — interpretable transform of CV |

All four metrics are shown as separate stacked subplots with shared x-axis (EF value) and `hovermode="x unified"` for synchronized hover labels. A vertical dashed line marks the selected production EF value.

**Implemented components:**

1. **`balancing/specs.py`** — Added `GridPoint` dataclass to break circular imports:
   ```python
   @dataclass
   class GridPoint:
       max_expansion_factor: float
       converged_zones: int
       total_zones: int
       mape: float
       p90: float
       cv: float
       ess_pct: float
   ```

2. **`balancing/balancer.py`** — `grid_search_expansion_factor()` function runs the grid search loop, computing aggregate metrics from per-zone results.

3. **`diagnostics/charts.py`** — `ef_tradeoff_figure()` renders a 4-subplot stacked chart using `make_subplots(rows=4, cols=1, shared_xaxes=True)`.

4. **`diagnostics/report.py`** — Wired into `generate_report()` with optional `grid_results` and `selected_ef` parameters.

5. **`weighting.py`** — Parses `diagnostics.expansion_factor_grid` from YAML config and calls grid search when present.

6. **`diagnostics/diagnostics_template.html`** — Added conditional section 4 "Expansion Factor Calibration" with explanatory notes.

7. **Tests** — `tests/test_grid_search.py` covers grid search execution, chart generation, and report integration.

**Configuration:**

```yaml
diagnostics:
  enabled: true
  expansion_factor_grid: [2, 4, 6, 8, 10, 15, 20, 30, 50]
```

The grid search is diagnostics-only — it does not change the production weights. The user's configured `max_expansion_factor` determines the actual weights used downstream.

---

### 6. Diagnostics Table Refactoring ✅

**Status:** Implemented (March 2026).

**Goal:** Consolidate weight quality metrics (CV and ESS%) into the main balancer performance table to provide a unified view of convergence, fit, and weight quality per zone. Standardize HTML table generation to use the shared `_html_table()` helper with support for grouped/spanned headers.

**Changes:**

1. **Extended `_html_table()` helper** — Added optional `group_row` parameter for two-tier headers with rowspan/colspan support, and `css_class` parameter for custom styling.

2. **Enhanced `balancer_performance_table()`** — Added CV and ESS% columns (computed inline from weights). Now shows 13 columns: Zone, N, Conv?, Iter, Household (Target, % Error), Person (Target, % Error), MAPE, P90, Max, CV, ESS%. Refactored to use `_html_table()` with `group_row` instead of manual HTML string concatenation.

3. **Simplified `weight_quality_table()`** — Removed CV and ESS% columns (now in balancer performance table). Retained weight distribution stats (Mean, Median, Std, Min, Max) and expansion factor stats (Min EF, Max EF, Mean EF, Median EF). Simplified `_weight_stats()` helper to remove CV/ESS computation.

4. **Updated template notes** — Moved CV and ESS% descriptions from Section 3 (Weight Quality) to Section 2 (Balancer Performance) to match the new column locations.

**Consistency improvements:**

- `balancer_performance_table()` now uses `_html_table()` with grouped headers (was manual HTML)
- `weight_quality_table()` continues to use `_html_table()` (no change in pattern)
- `unweighted_cell_counts()` and `crosswalk_summary_table()` still use manual HTML due to complex rowspan logic that exceeds `_html_table()` capabilities

**Files affected:** `diagnostics/tables.py`, `diagnostics/diagnostics_template.html`.

---

### Priority and Dependencies

| # | Feature | Status | Depends On | Complexity | Impact |
|---|---------|--------|-----------|------------|--------|
| 1 | Cross-tab targets | 🔲 | — | Medium | High — enables joint demographic controls (age×sex, income×workers) that significantly improve weight quality |
| 2 | Custom trip targets | 🔲 | — | Medium | High — useful for matching to NTD/FHWA but adds architectural complexity |
| 3 | DOW structuring | 🔲 | — | High | High — provides day of week weight granularity |
| 4 | Platform bias | 🔲 | — | Low–High (method-dependent) | Medium — important for mixed-mode surveys |
| 5 | EF grid search | ✅ | — | Low | Low — calibration aid; no algorithmic changes, purely diagnostic |
| 6 | Table refactoring | ✅ | — | Low | Low — improved diagnostics UI and code maintainability |

Features 1 and 3 are independent and can be developed in parallel. Feature 2 is architecturally distinct (non-linear constraints). Feature 4 is orthogonal — it modifies inputs (base weights or controls) rather than the balancer itself. Features 5 and 6 are complete — they enhanced the diagnostics reporting without impacting the core balancer algorithm.
