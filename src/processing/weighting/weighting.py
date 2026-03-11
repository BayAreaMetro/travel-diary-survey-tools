"""Top-level weighting pipeline step.

Orchestrates the full weighting pipeline as a single ``@step`` entry point:

1. Build PUMA -> target-zone crosswalk from geography config
2. Build PUMS-derived control totals (load, recode, allocate, aggregate)
3. Assign survey households to zones; build seed table
4. Run maximum-entropy balancer per zone
5. Diagnostics report
6. Join ``hh_weight`` to households; propagate to all downstream tables
"""

import logging

import polars as pl
from numpy.compat import Path

from pipeline.cache import PipelineCache
from pipeline.decoration import step
from processing.weighting.balancing.balancer import MergeSpec, balance_weights
from processing.weighting.balancing.base_weights import compute_base_weights, load_sample_plan
from processing.weighting.balancing.importance import compute_moe_importance
from processing.weighting.balancing.weight_propagation import (
    collect_tables,
    non_null_tables,
    propagate_weights,
    safe_join_weight,
)
from processing.weighting.data_prep.control_data import (
    ControlSpec,
    ControlTotals,
    apply_zone_groups,
    build_control_totals,
    recode_pums_households,
    recode_pums_persons,
)
from processing.weighting.data_prep.crosswalk import GeographyConfig, PumaCrosswalk
from processing.weighting.data_prep.pums_data import (
    PUMSSource,
    fetch_pums_data,
    load_pums_from_files,
)
from processing.weighting.data_prep.seed_data import (
    build_seed_table,
    recode_survey_households,
    recode_survey_persons,
)
from processing.weighting.diagnostics import generate_report
from processing.weighting.validation.checksums import check_incidence_sums
from processing.weighting.validation.weight_checks import weight_sanity_checks

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers — keep the orchestrator thin
# ---------------------------------------------------------------------------
def _parse_controls(
    controls: list[dict],
) -> tuple[list[ControlSpec], list[str], list[MergeSpec], dict[str, float]]:
    """Extract specs, target names, merge specs, and explicit importance overrides."""
    _spec_keys = ("name", "importance")
    specs = [ControlSpec(**{k: v for k, v in c.items() if k in _spec_keys}) for c in controls]
    target_names = [s.name for s in specs]
    merge_specs = [
        MergeSpec(control=c["name"], groups=c["merge"], zones=c.get("merge_zones"))
        for c in controls
        if c.get("merge")
    ]
    importance = {s.name: s.importance for s in specs if s.importance is not None}
    return specs, target_names, merge_specs, importance


def _build_control_totals(
    xw: PumaCrosswalk,
    specs: list[ControlSpec],
    target_names: list[str],
    *,
    state_fips: str,
    pums_year: int,
    pums_households: str | None,
    pums_persons: str | None,
    load_replicate_weights: bool = False,
    cache_dir: Path | None = None,
) -> tuple[ControlTotals, pl.DataFrame | None, pl.DataFrame | None]:
    """Load PUMS, recode, allocate via crosswalk, and aggregate control totals.

    When *load_replicate_weights* is True the crosswalk-allocated PUMS
    frames are returned alongside the totals so downstream code can
    compute replicate-weight MOEs.  Otherwise ``(totals, None, None)``.
    """
    if pums_households is not None and pums_persons is not None:
        logger.info("Loading PUMS from local files")
        pums_hh, pums_per = load_pums_from_files(
            pums_households, pums_persons, load_replicate_weights=load_replicate_weights
        )
    else:
        source = PUMSSource(state_fips=state_fips, pums_year=pums_year, puma_ids=xw.puma_ids)
        logger.info("Fetching PUMS via Census API: state=%s year=%d", state_fips, pums_year)
        pums_hh, pums_per = fetch_pums_data(
            source, load_replicate_weights=load_replicate_weights, cache_dir=cache_dir
        )

    pums_hh = recode_pums_households(pums_hh, pums_per, target_names)
    pums_per = recode_pums_persons(pums_per, target_names)

    pums_hh_xw, pums_per_xw = xw.allocate_pums_weights(pums_hh, pums_per)
    totals = build_control_totals(pums_hh_xw, pums_per_xw, specs, geo_col="ctrl_geoid")

    if load_replicate_weights:
        return totals, pums_hh_xw, pums_per_xw
    return totals, None, None


def _resolve_importance(
    target_names: list[str],
    explicit: dict[str, float],
    *,
    moe_based: bool,
    default: float,
    xw: PumaCrosswalk,
    specs: list[ControlSpec],
    state_fips: str,
    pums_year: int,
    pums_households: str | None,
    pums_persons: str | None,
    cache_dir: Path | None,
) -> dict[str, float]:
    """Build the final importance dict and log it.

    When *moe_based* is True, loads PUMS replicate weights (or re-fetches
    them), computes per-control CVs, and merges with any explicit YAML
    overrides.  The large replicate-weight frames stay local to this
    function scope.
    """
    importance = dict(explicit)
    if moe_based:
        _, pums_hh_xw, pums_per_xw = _build_control_totals(
            xw,
            specs,
            target_names,
            state_fips=state_fips,
            pums_year=pums_year,
            pums_households=pums_households,
            pums_persons=pums_persons,
            load_replicate_weights=True,
            cache_dir=cache_dir,
        )
        assert pums_hh_xw is not None  # noqa: S101
        assert pums_per_xw is not None  # noqa: S101
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
def weighting(  # noqa: PLR0913
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
        Geography crosswalk configuration.  Builds a PUMA -> target-zone
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
    """
    if households is None or persons is None:
        msg = "Weighting requires at least households and persons tables."
        raise ValueError(msg)

    # -- 1. Parse config --------------------------------------------
    specs, target_names, merge_specs, importance = _parse_controls(controls)
    logger.info("Controls: %s", target_names)

    cache_dir = pipeline_cache.cache_dir if pipeline_cache else None
    zone_groups: dict[str, list[str]] | None = geography.get("zone_groups")

    # -- 2. Geography crosswalk -------------------------------------
    xw = PumaCrosswalk(
        GeographyConfig(**geography),
        state_fips=state_fips,
        pums_year=pums_year,
        cache_dir=cache_dir,
    )

    # -- 3. PUMS control totals -------------------------------------
    control_totals, _, _ = _build_control_totals(
        xw,
        specs,
        target_names,
        state_fips=state_fips,
        pums_year=pums_year,
        pums_households=pums_households,
        pums_persons=pums_persons,
        cache_dir=cache_dir,
    )

    # -- 3b. Importance (MOE + explicit overrides) -------------------
    importance = _resolve_importance(
        target_names,
        importance,
        moe_based=moe_based_importance,
        default=default_importance,
        xw=xw,
        specs=specs,
        state_fips=state_fips,
        pums_year=pums_year,
        pums_households=pums_households,
        pums_persons=pums_persons,
        cache_dir=cache_dir,
    )

    logger.info(
        "Control totals: %d zones, %d PUMS HHs, %d PUMS persons",
        len(control_totals.geo_ids),
        control_totals.pums_hh_count,
        control_totals.pums_person_count,
    )

    # -- 4. Survey seed (assign HHs to zones, recode, build seed) ---
    households = xw.assign_households(households)
    n_assigned = households.filter(pl.col("ctrl_geoid").is_not_null()).height
    logger.info("Assigned %d / %d HHs to target zones", n_assigned, len(households))

    hh_recoded = recode_survey_households(households, persons, target_names)
    per_recoded = recode_survey_persons(persons, target_names)
    seed = build_seed_table(hh_recoded, per_recoded, target_names, geo_col="ctrl_geoid")
    check_incidence_sums(seed, target_names, source_label="survey")

    # -- 4b. Zone groups (optional) ----------------------------------
    if zone_groups:
        control_totals, seed = apply_zone_groups(
            control_totals, seed, zone_groups, geo_col="ctrl_geoid"
        )

    # -- 4c. Base weights --------------------------------------------
    plan = None
    if sample_plan is not None:
        plan = load_sample_plan(sample_plan)
    else:
        logger.warning(
            "No sample_plan provided; using PUMS-target response inversion "
            "for initial weights.  Provide a sample_plan CSV for more precisely "
            "stratified base weights."
        )
    seed = compute_base_weights(
        seed, control_totals, target_names, geo_col="ctrl_geoid", sample_plan=plan
    )

    # -- 5. Balance -------------------------------------------------
    weights_df, statuses = balance_weights(
        seed,
        control_totals,
        target_names,
        merges=merge_specs,
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

    # -- 6. Diagnostics report --------------------------------------
    _report_dir = cache_dir / "weighting" if cache_dir else Path.cwd() / "weighting"
    generate_report(
        seed=seed,
        weights=weights_df,
        control_totals=control_totals,
        target_names=target_names,
        statuses=statuses,
        output_path=_report_dir / "diagnostics.html",
        puma_gdf=xw.puma_gdf,
        target_gdf=xw.target_gdf,
        crosswalk_df=xw.crosswalk_df,
        zone_groups=zone_groups,
        merge_specs=merge_specs,
    )

    # -- 7. Attach & propagate weights ------------------------------
    households = safe_join_weight(
        households,
        weights_df.select("hh_id", "hh_weight"),
        "hh_id",
    )
    households = households.join(seed.select("hh_id", "base_weight"), on="hh_id", how="left")
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

    # -- 8. Sanity checks -------------------------------------------
    weight_sanity_checks(non_null_tables(tables), control_totals, specs)

    return non_null_tables(tables)
