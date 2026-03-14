"""Top-level weighting pipeline step.

Orchestrates the full weighting pipeline as a single ``@step`` entry point:

1.  **Setup** — parse YAML config → specs, target names, merges, importance.
2.  **Data fetching** — load PUMS (API or files); receive survey tables.
3.  **Conformance** — recode both PUMS and survey through identical control
    expressions → same control-column schema.
4.  **Incidence pivot** — unified pivoter produces identical
    ``{ctrl}__{member}`` column layout for both datasets.
5.  **Zone assignment** — crosswalk assigns ``study_geoid`` and
    ``ctrl_geoid`` to survey HHs (point-in-polygon) and allocates PUMS
    weights to target zones.  Zone groups (if configured) are applied
    inside the crosswalk so ``ctrl_geoid`` is ready for balancing.
6.  **Crosstab merges** — N-D merge specs collapse composite incidence
    columns on both tables before any 1-D merges are applied.
7.  **1-D merges** — global merges collapse incidence columns symmetrically
    on both tables (originals dropped).  Zone-specific merges add merged
    columns (originals kept) and modify control totals for the specified
    zones after aggregation.
8.  **Control totals** — aggregate PUMS incidence into target totals per zone.
9.  **Balancer** — base weights → max-entropy balancing → weight propagation.

Design decisions:

* **PopulationSim dependency** — uses PopulationSim's core numba balancer
  (``np_balancer_numba``) directly — a pure ``@njit`` function (~120 lines)
  taking numpy arrays.  No PopulationSim pipeline infrastructure involved.
* **Geography columns** — three distinct levels: ``PUMA`` (raw Census
  PUMA), ``study_geoid`` (crosswalk target zones from the user's polygon
  file), and ``ctrl_geoid`` (balancing geography, equal to ``study_geoid``
  unless zone groups are configured).  Downstream balancing always uses
  ``ctrl_geoid``; diagnostics/maps use ``study_geoid`` for spatial detail.
* **Symmetric incidence** — both the survey sample and the PUMS universe are
  first recoded and pivoted into incidence tables with identical column
  layouts.  Geography, merges, and crosstabs are applied *after* incidence
  construction, keeping the recode/pivot logic independent of geography.

Algorithm:

    Find weight vector **w** closest to seed weights **w₀** (KL-divergence)
    subject to marginal constraints:

    min Σᵢ wᵢ ln(wᵢ / w₀ᵢ)   s.t.  A w = t,  wᵢ ≥ 0

    where **A** is the incidence matrix and **t** is the target totals vector.
    Runs independently per control geography zone (zones are parallelisable).
"""

import logging

import polars as pl
from numpy.compat import Path

from pipeline.cache import PipelineCache
from pipeline.decoration import step
from processing.weighting.balancing.balancer import (
    balance_weights,
    grid_search_expansion_factor,
)
from processing.weighting.balancing.base_weights import compute_base_weights
from processing.weighting.balancing.importance import compute_moe_importance
from processing.weighting.balancing.weight_propagation import (
    collect_tables,
    non_null_tables,
    propagate_weights,
    safe_join_weight,
)
from processing.weighting.controls.registry import register_crosstabs_from_config, resolve_targets
from processing.weighting.data_prep.control_data import (
    allocate_pums_zones,
    recode_pums_households,
    recode_pums_persons,
)
from processing.weighting.data_prep.crosswalk import GeographyConfig, PumaCrosswalk
from processing.weighting.data_prep.incidence import (
    aggregate_control_totals,
    build_incidence_table,
)
from processing.weighting.data_prep.merges import (
    apply_1d_merges,
    apply_crosstab_merges,
    merge_control_totals,
)
from processing.weighting.data_prep.pums_data import (
    PUMSSource,
    fetch_pums_data,
    load_pums_from_files,
)
from processing.weighting.data_prep.seed_data import (
    recode_survey_households,
    recode_survey_persons,
)
from processing.weighting.diagnostics import generate_report
from processing.weighting.specs import ControlSpec, GridPoint, MergeSpec
from processing.weighting.validation.checksums import check_incidence_sums
from processing.weighting.validation.control_validation import validate_total_control_categories
from processing.weighting.validation.weight_checks import weight_sanity_checks

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers — keep the orchestrator thin
# ---------------------------------------------------------------------------


def _parse_controls(
    controls: list[dict],
) -> tuple[list[ControlSpec], list[str], list[MergeSpec], list[MergeSpec], dict[str, float]]:
    """Extract specs, target names, merge specs, and explicit importance overrides.

    Returns:
    -------
    specs, target_names, crosstab_merges, merges_1d, importance
    """
    _spec_keys = ("name", "importance")
    specs = [ControlSpec(**{k: v for k, v in c.items() if k in _spec_keys}) for c in controls]
    target_names = [s.name for s in specs]

    crosstab_merges: list[MergeSpec] = []
    merges_1d: list[MergeSpec] = []

    for c in controls:
        # N-D merges for cross-tabs (YAML 'merges' key)
        if c.get("merges"):
            crosstab_merges.append(MergeSpec(control=c["name"], groups=c["merges"]))

        # 1-D merges for regular controls (YAML 'merge' key)
        if c.get("merge"):
            merges_1d.append(MergeSpec(control=c["name"], groups=c["merge"]))

        # Zone-specific 1-D merges
        for zone_id, groups in c.get("zone_merges", {}).items():
            merges_1d.append(MergeSpec(control=c["name"], groups=groups, zones=[zone_id]))

    importance = {s.name: s.importance for s in specs if s.importance is not None}
    return specs, target_names, crosstab_merges, merges_1d, importance


def _load_pums(
    *,
    state_fips: str,
    pums_year: int,
    pums_households: str | None,
    pums_persons: str | None,
    puma_ids: list[str] | None = None,
    load_replicate_weights: bool = False,
    cache_dir: Path | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Load PUMS household and person microdata from local files or Census API."""
    if pums_households is not None and pums_persons is not None:
        logger.info("Loading PUMS from local files")
        return load_pums_from_files(
            pums_households, pums_persons, load_replicate_weights=load_replicate_weights
        )

    source = PUMSSource(state_fips=state_fips, pums_year=pums_year, puma_ids=puma_ids)
    logger.info("Fetching PUMS via Census API: state=%s year=%d", state_fips, pums_year)
    return fetch_pums_data(
        source, load_replicate_weights=load_replicate_weights, cache_dir=cache_dir
    )


def _resolve_importance(
    target_names: list[str],
    explicit: dict[str, float],
    *,
    moe_based: bool,
    default: float,
    crosswalk: PumaCrosswalk | None = None,
    pums_hh: pl.DataFrame | None = None,
    pums_per: pl.DataFrame | None = None,
) -> dict[str, float]:
    """Build the final importance dict and log it.

    When *moe_based* is True, builds crosswalk-allocated PUMS frames
    (with replicate-weight columns) internally so that per-control CVs
    can be computed.  YAML explicit overrides always take precedence.
    """
    importance = dict(explicit)
    if moe_based:
        if crosswalk is None or pums_hh is None or pums_per is None:
            msg = (
                "MOE-based importance requires crosswalk and PUMS data "
                "loaded with replicate weights."
            )
            raise ValueError(msg)
        pums_hh_xw, pums_per_xw = crosswalk.allocate_pums_weights(pums_hh, pums_per)
        moe_importance = compute_moe_importance(pums_hh_xw, pums_per_xw, target_names)
        # YAML explicit overrides take precedence over MOE-derived values
        moe_importance.update(importance)
        importance = moe_importance

    full = {name: importance.get(name, default) for name in target_names}
    imp_lines = "\n".join(f"  {k}: {v:.1f}" for k, v in full.items())
    logger.info("Importance weights:\n%s", imp_lines)
    return importance


# ---------------------------------------------------------------------------
# Pipeline step
# ---------------------------------------------------------------------------
@step()
def weighting(  # noqa: PLR0913, PLR0915
    # -- Config params (from YAML) --------------------------------------
    state_fips: str,
    pums_year: int,
    controls: list[dict],
    geography: dict,
    *,
    # -- Existing PUMS files (optional) --------------------------------------
    pums_households: str | None = None,
    pums_persons: str | None = None,
    # -- Sample plan (optional) -----------------------------------------
    sample_plan: str | None = None,
    # -- Pipeline plumbing (auto-injected by @step decorator) -----------
    pipeline_cache: PipelineCache | None = None,
    # -- Importance / MOE -----------------------------------------
    moe_based_importance: bool = False,
    default_importance: float = 100.0,
    # -- Balancing params --------------------------------------
    max_expansion_factor: float = 10.0,
    min_expansion_factor: float = 0.1,
    min_weight: float | None = 1,
    max_weight: float | None = None,
    max_iterations: int = 10_000,
    n_workers: int = 1,
    # -- Diagnostics ----------------------------------------------------
    expansion_factor_grid: list[float] | None = None,
    # -- Canonical tables (auto-injected by pipeline) -------------------
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    days: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    joint_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
) -> dict[str, pl.DataFrame]:
    """Compute expansion weights from PUMS controls and propagate to all tables.

    The step produces **expansion weights** that scale the survey sample to
    represent the full population.  Internally it orchestrates: geography
    crosswalk → PUMS control totals → survey seed prep → maximum-entropy
    balancing → weight propagation.

    Control Variable Configuration:
        Each entry in ``controls`` defines one marginal target.  Names must
        match keys in the ``CONTROLS`` registry (see
        :mod:`processing.weighting.controls.registry`).  Example::

            controls:
              - name: h_size
              - name: h_income
              - name: gender
                importance: 200      # explicit override
              - name: commute_mode
                merge:               # collapse categories
                  active: [bike, walk]

    Importance Tiers:
        Three-tier system controlling how hard the balancer tries to match
        each control:

        1. **Default** — ``default_importance`` (100) for all controls.
        2. **MOE-based** — when ``moe_based_importance=True``, PUMS replicate
           weights (``WGTP1``-``WGTP80`` / ``PWGTP1``-``PWGTP80``) are used
           to estimate per-control CV, then normalised so
           median importance = 100.  Transfer function: ``1 / sqrt(CV)``.
        3. **Explicit override** — per-control ``importance:`` in YAML takes
           highest precedence.

        Structural controls (``h_total``, ``p_total``) always receive fixed
        importance of 1000 regardless of MOE.

    Field Mapping:
        The ``field_mapping`` key inside the YAML maps PUMS variable names
        to canonical survey field names so the same bin/group definitions
        can be applied to both datasets::

            field_mapping:
              households:
                NP: num_people
                HINCP: income
              persons:
                AGEP: age
                SEX: sex
                JWTRNS: commute_mode_code

    Diagnostics:
        When enabled, produces a self-contained interactive HTML report
        (Plotly + Jinja2) with: recode coverage, weight summary, target-fit
        bar charts, expansion-factor calibration, weight-distribution
        violins, seed-vs-targets detail, and convergence / ESS metrics::

            diagnostics:
              enabled: true
              output_path: "weighting_diagnostics.html"
              fit_error_thresholds: [2, 5]
              min_seed_count_warning: 10
              expansion_factor_grid: [2, 4, 6, 8, 10, 15, 20, 30, 50]
              plotly_cdn: true

    Parameters
    ----------
    state_fips : str
        Two-digit FIPS code (e.g. ``"06"`` for California).
    pums_year : int
        ACS 1-year PUMS vintage (e.g. ``2023``).
    controls : list[dict]
        Control specifications, each with ``name``.
        Names must match keys in the ``CONTROLS`` registry.
    geography : dict
        Geography crosswalk configuration.  Builds a PUMA → target-zone
        crosswalk from a user-supplied polygon file using Census block
        population.  See :class:`GeographyConfig`.
    pums_households, pums_persons : str | None
        Optional local file paths.  When both are provided the Census
        API is skipped and data is loaded from disk instead.
    sample_plan : str | None
        Path to a sample-plan CSV (columns: ``geo_id``,
        ``target_population``, ``expected_responses``).  When provided,
        base weights are derived from stratified response inversion
        instead of the default PUMS-target fallback.  If the file does
        not exist a ``FileNotFoundError`` is raised.
    moe_based_importance : bool
        Compute per-control importance from PUMS replicate-weight MOEs
        (default: False).
    default_importance : float
        Fallback importance when neither MOE nor explicit override is
        set (default: 100.0).
    max_expansion_factor, min_expansion_factor : float
        Bounds on balanced / initial weight ratio.
    min_weight, max_weight : float | None
        Optional absolute floor / ceiling on final weights.
    max_iterations : int
        Newton-Raphson cap per zone.
    n_workers : int
        Threads for parallel zone balancing.
    households, persons, days, ... : pl.DataFrame | None
        Canonical tables auto-injected by the pipeline.

    Returns:
    -------
    dict[str, pl.DataFrame]
        All canonical tables with weight columns attached.

    Example config:
        .. code-block:: yaml

            - name: weighting
              params:
                state_fips: "06"
                pums_year: 2023
                pums_households: "pums/psam_h06.csv"
                pums_persons: "pums/psam_p06.csv"

                geography:
                  target_zones:
                    path: "geo/weighting_zones.shp"
                    id_field: zone_id
                  state_fips: "06"
                  pums_year: 2023

                field_mapping:
                  households:
                    NP: num_people
                  persons:
                    AGEP: age
                    SEX: sex

                controls:
                  - name: h_size
                  - name: h_income
                  - name: gender
                  - name: age

                max_expansion_factor: 10
                min_expansion_factor: 0.1
                max_iterations: 1000
    """
    if households is None or persons is None:
        msg = "Weighting requires at least households and persons tables."
        raise ValueError(msg)

    # ===================================================================
    # Step 1 — Setup
    # ===================================================================
    register_crosstabs_from_config(controls)

    specs, target_names, crosstab_merges, merges_1d, importance_overrides = _parse_controls(
        controls
    )
    logger.info("Controls: %s", target_names)

    ctrl_instances = resolve_targets(target_names)
    validate_total_control_categories(ctrl_instances)

    cache_dir = pipeline_cache.cache_dir if pipeline_cache else None
    zone_groups: dict[str, list[str]] | None = geography.get("zone_groups")

    # ===================================================================
    # Step 2 — Data fetching
    # ===================================================================
    pums_hh, pums_per = _load_pums(
        state_fips=state_fips,
        pums_year=pums_year,
        pums_households=pums_households,
        pums_persons=pums_persons,
        load_replicate_weights=moe_based_importance,
        cache_dir=cache_dir,
    )

    # ===================================================================
    # Step 3 — Conformance (recoding)
    # ===================================================================
    # Both datasets are recoded through the same control expressions,
    # producing identical control-column schemas.
    hh_recoded = recode_survey_households(households, persons, target_names)
    per_recoded = recode_survey_persons(persons, target_names)

    pums_hh = recode_pums_households(pums_hh, pums_per, target_names)
    pums_per = recode_pums_persons(pums_per, target_names)

    # ===================================================================
    # Step 4 — Incidence pivot
    # ===================================================================
    # Unified pivoter applied to both — identical column layout.
    seed_incidence = build_incidence_table(hh_recoded, per_recoded, target_names)

    pums_incidence = build_incidence_table(
        pums_hh,
        pums_per,
        target_names,
        hh_id_col="SERIALNO",
        extra_cols=["WGTP", "PUMA"],
    )

    # Checksums to verify correct recode + pivot logic before zone assignment.
    check_incidence_sums(seed_incidence, target_names, source_label="survey")
    check_incidence_sums(pums_incidence, target_names, source_label="pums")

    # ===================================================================
    # Step 5 — Zone assignment (+ zone groups applied in crosswalk)
    # ===================================================================
    xw = PumaCrosswalk(
        GeographyConfig(**geography),
        state_fips=state_fips,
        pums_year=pums_year,
        cache_dir=cache_dir,
        zone_groups=zone_groups,
    )

    # Survey: point-in-polygon → join study_geoid + ctrl_geoid.
    households = xw.assign_households(households)
    n_assigned = households.filter(pl.col("study_geoid").is_not_null()).height
    logger.info("Assigned %d / %d HHs to target zones", n_assigned, len(households))

    seed_incidence = seed_incidence.join(
        households.select("hh_id", "study_geoid", "ctrl_geoid"), on="hh_id", how="left"
    )

    # PUMS: crosswalk weight allocation → adds study_geoid + ctrl_geoid, scales WGTP.
    pums_incidence = allocate_pums_zones(pums_incidence, xw.crosswalk_df)

    # ===================================================================
    # Step 6 — Crosstab dimension merges
    # ===================================================================
    # Crosstabs are independent targets with their own N-D merge specs.
    # Applied first — they don't interact with 1-D merges.
    if crosstab_merges:
        seed_incidence = apply_crosstab_merges(seed_incidence, crosstab_merges)
        pums_incidence = apply_crosstab_merges(pums_incidence, crosstab_merges)
        logger.info("Applied %d crosstab merge specs", len(crosstab_merges))

    # ===================================================================
    # Step 7 — 1-D merges (global + zone-specific)
    # ===================================================================
    # Global (zones=None): collapse incidence columns, drop originals.
    # Zone-specific: add merged columns, keep originals.
    if merges_1d:
        seed_incidence = apply_1d_merges(seed_incidence, merges_1d)
        pums_incidence = apply_1d_merges(pums_incidence, merges_1d)
        logger.info(
            "Applied %d 1-D merge specs; incidence now %d columns",
            len(merges_1d),
            len(seed_incidence.columns),
        )

    # ===================================================================
    # Step 8 — Control totals (aggregate PUMS incidence)
    # ===================================================================
    control_totals = aggregate_control_totals(
        pums_incidence, target_names, weight_col="WGTP", geo_col="ctrl_geoid"
    )

    # Zone-specific merges: collapse constituent targets for merged zones.
    if merges_1d:
        control_totals = merge_control_totals(control_totals, merges_1d)

    logger.info(
        "Control totals: %d zones, %d PUMS HHs, %d PUMS persons",
        len(control_totals.geo_ids),
        control_totals.pums_hh_count,
        control_totals.pums_person_count,
    )

    # Importance
    importance = _resolve_importance(
        target_names,
        importance_overrides,
        moe_based=moe_based_importance,
        default=default_importance,
        crosswalk=xw,
        pums_hh=pums_hh,
        pums_per=pums_per,
    )

    # ===================================================================
    # Step 9 — Balancer
    # ===================================================================
    seed_incidence = compute_base_weights(
        seed_incidence,
        control_totals,
        target_names,
        geo_col="ctrl_geoid",
        sample_plan=sample_plan,
    )

    weights_df, statuses = balance_weights(
        seed_incidence,
        control_totals,
        target_names,
        importance=importance or None,
        default_importance=default_importance,
        max_expansion_factor=max_expansion_factor,
        min_expansion_factor=min_expansion_factor,
        min_weight=min_weight,
        max_weight=max_weight,
        max_iterations=max_iterations,
        n_workers=n_workers,
    )

    n_failed = sum(not s.converged for s in statuses)
    if n_failed:
        msg = f"Balancing failed to converge for {n_failed} zones.  See logs for details."
        raise RuntimeError(msg)

    # EF grid search (optional diagnostics)
    grid_results: list[GridPoint] | None = None
    if expansion_factor_grid:
        grid_results = grid_search_expansion_factor(
            seed_incidence,
            control_totals,
            target_names,
            ef_grid=expansion_factor_grid,
            selected_ef=max_expansion_factor,
            importance=importance or None,
            default_importance=default_importance,
            min_expansion_factor=min_expansion_factor,
            min_weight=min_weight,
            max_weight=max_weight,
            max_iterations=max_iterations,
            n_workers=n_workers,
        )

    # -- Diagnostics report -----------------------------------------
    _report_dir = cache_dir / "weighting" if cache_dir else Path.cwd() / "weighting"
    generate_report(
        seed=seed_incidence,
        weights=weights_df,
        control_totals=control_totals,
        target_names=target_names,
        statuses=statuses,
        output_path=_report_dir / "diagnostics.html",
        puma_gdf=xw.puma_gdf,
        target_gdf=xw.target_gdf,
        crosswalk_df=xw.crosswalk_df,
        zone_groups=zone_groups,
        merge_specs=crosstab_merges + merges_1d,
        grid_results=grid_results,
        selected_ef=max_expansion_factor if grid_results else None,
    )

    # -- Attach & propagate weights ---------------------------------
    households = safe_join_weight(
        households,
        weights_df.select("hh_id", "hh_weight"),
        "hh_id",
    )
    households = households.join(
        seed_incidence.select("hh_id", "base_weight"), on="hh_id", how="left"
    )
    tables = collect_tables(
        households=households,
        persons=persons,
        days=days,
        unlinked_trips=unlinked_trips,
        linked_trips=linked_trips,
        joint_trips=joint_trips,
        tours=tours,
    )
    has_weight: dict[str, str] = {"households": "hh_weight"}
    propagate_weights(tables, has_weight)

    # -- Sanity checks ----------------------------------------------
    weight_sanity_checks(non_null_tables(tables), control_totals, specs)

    return non_null_tables(tables)
