"""Top-level weighting pipeline step.

Orchestrates the full weighting pipeline via :class:`WeightingPipeline`:

1.  **Setup** — parse YAML config → specs, target names, merges, importance.
    Cross-tab controls are registered with pre-merged dimensions so the
    enum reflects the effective cell count.
2.  **Data fetching** — load PUMS (API or files); receive survey tables.
3.  **Conformance** — recode both PUMS and survey through identical control
    expressions → same control-column schema.
4.  **Incidence pivot** — unified pivoter produces identical
    ``{ctrl}__{member}`` column layout for both datasets.
5.  **Zone assignment** — crosswalk assigns ``study_geoid`` and
    ``ctrl_geoid`` to survey HHs (point-in-polygon) and allocates PUMS
    weights to target zones.  Zone groups (if configured) are applied
    inside the crosswalk so ``ctrl_geoid`` is ready for balancing.
6.  **1-D merges** — global merges collapse incidence columns symmetrically
    on both tables (originals dropped).  Zone-specific merges add merged
    columns (originals kept) and modify control totals for the specified
    zones after aggregation.
7.  **Control totals** — aggregate PUMS incidence into target totals per zone.
8.  **Balancer** — base weights → max-entropy balancing → weight propagation.

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
from pathlib import Path

import polars as pl

from data_canon.core.dataclass import CanonicalData
from pipeline.cache import PipelineCache
from pipeline.decoration import step
from processing.weighting.balancing.balancer import (
    balance_weights,
    grid_search_expansion_factor,
)
from processing.weighting.balancing.base_weights import compute_base_weights
from processing.weighting.balancing.importance import compute_control_moe, compute_moe_importance
from processing.weighting.balancing.weight_propagation import (
    collect_tables,
    non_null_tables,
    propagate_weights,
    safe_join_weight,
)
from processing.weighting.controls.registry import register_crosstabs_from_config, resolve_targets
from processing.weighting.data_prep.control_data import (
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
from processing.weighting.specs import (
    BalancingConfig,
    ControlRegistryConfig,
    ControlTotals,
    GridPoint,
    ImportanceConfig,
    WeightingConfig,
    ZoneStatus,
)
from processing.weighting.validation.checksums import check_incidence_sums
from processing.weighting.validation.control_validation import (
    validate_total_control_categories,
    warn_crosstab_sparsity,
)
from processing.weighting.validation.weight_checks import weight_sanity_checks

logger = logging.getLogger(__name__)


# ===========================================================================
# Pipeline class
# ===========================================================================


class WeightingPipeline:
    """Stateful weighting pipeline.

    Separates *configuration* (frozen at ``__init__``) from *intermediate
    state* (built up phase-by-phase).  Phase methods are designed to be
    called in sequence from the ``@step() weighting()`` entry-point; each
    stores results as instance attributes.

    Usage::

        pipeline = WeightingPipeline(controls=..., config=..., data=...)

        pipeline.setup()
        pipeline.fetch_pums()

        pipeline.recode_and_pivot()
        pipeline.assign_zones()
        pipeline.apply_merges()

        pipeline.aggregate_totals()
        pipeline.resolve_importance()

        pipeline.balance()
        pipeline.propagate()

        pipeline.generate_diagnostics()

        weights = pipeline.household_weights
    """

    # -- Intermediate state (populated by phase methods) ----------------
    crosswalk: PumaCrosswalk
    pums_hh: pl.DataFrame
    pums_per: pl.DataFrame
    seed_incidence: pl.DataFrame
    pums_incidence: pl.DataFrame
    control_totals: ControlTotals
    resolved_importance: dict[str, float]
    control_moe: pl.DataFrame | None
    weights: pl.DataFrame
    statuses: list[ZoneStatus]
    grid_results: list[GridPoint] | None

    def __init__(
        self,
        *,
        controls: ControlRegistryConfig,
        config: WeightingConfig,
        data: CanonicalData,
        balancing: BalancingConfig | None = None,
        importance: ImportanceConfig | None = None,
    ) -> None:
        """Initialise with configuration and survey data."""
        if data.households is None or data.persons is None:
            msg = "WeightingPipeline requires at least households and persons tables."
            raise ValueError(msg)

        self.controls = controls
        self.config = config
        self.data = data
        self.balancing = balancing or BalancingConfig()
        self.importance_cfg = importance or ImportanceConfig()

        # Convenience aliases from config
        self.geography = config.geography
        self.state_fips = config.state_fips
        self.pums_year = config.pums_year
        self.pums_households = config.pums_households
        self.pums_persons = config.pums_persons
        self.sample_plan = config.sample_plan
        self.cache_dir = config.cache_dir / "weighting" if config.cache_dir else None
        self.expansion_factor_grid = config.expansion_factor_grid
        self.strict_survey_nulls = config.strict_survey_nulls

        # Mutable state initialised to None; populated by phases
        self.control_moe = None
        self.grid_results = None

    # ------------------------------------------------------------------
    # Phase methods
    # ------------------------------------------------------------------

    def setup(self) -> None:
        """Register crosstabs, resolve control instances, build crosswalk."""
        register_crosstabs_from_config(
            [
                {
                    "name": s.name,
                    **({"importance": s.importance} if s.importance is not None else {}),
                    **({"dimensions": s.dimensions} if s.dimensions is not None else {}),
                    **({"merges": s.merges} if s.merges is not None else {}),
                }
                for s in self.controls.specs
            ]
        )
        ctrl_instances = resolve_targets(self.controls.target_names)
        validate_total_control_categories(ctrl_instances)
        logger.info("Controls: %s", self.controls.target_names)

        zone_groups: dict[str, list[str]] | None = self.geography.get("zone_groups")
        # Prepare the crosswalk with the full set of zones (including zone groups)
        # Store as an instance attribute for use in zone assignment and diagnostics
        self.crosswalk = PumaCrosswalk(
            GeographyConfig(**self.geography),
            state_fips=self.state_fips,
            pums_year=self.pums_year,
            cache_dir=self.cache_dir,
            zone_groups=zone_groups,
        )

    def fetch_pums(self) -> None:
        """Load PUMS microdata from local files or the Census API."""
        load_reps = self.importance_cfg.moe_based
        if self.pums_households is not None and self.pums_persons is not None:
            logger.info("Loading PUMS from local files")
            self.pums_hh, self.pums_per = load_pums_from_files(
                self.pums_households,
                self.pums_persons,
                load_replicate_weights=load_reps,
            )
        else:
            source = PUMSSource(state_fips=self.state_fips, pums_year=self.pums_year)
            logger.info(
                "Fetching PUMS via Census API: state=%s year=%d", self.state_fips, self.pums_year
            )
            self.pums_hh, self.pums_per = fetch_pums_data(
                source,
                load_replicate_weights=load_reps,
                cache_dir=self.cache_dir,
            )

        # Ensure that PUMA is 0 padded to 5 digits for consistency with crosswalk
        self.pums_hh = self.pums_hh.with_columns(pl.col("PUMA").cast(pl.Utf8).str.zfill(5))
        self.pums_per = self.pums_per.with_columns(pl.col("PUMA").cast(pl.Utf8).str.zfill(5))

    def recode_survey(self) -> None:
        """Recode survey HH/persons through control expressions and pivot to incidence."""
        names = self.controls.target_names
        strict_nulls = self.strict_survey_nulls
        hh_recoded = recode_survey_households(
            self.data.households,  # pyright: ignore[reportArgumentType]
            self.data.persons,  # pyright: ignore[reportArgumentType]
            names,
            strict_nulls=strict_nulls,
        )
        per_recoded = recode_survey_persons(self.data.persons, names, strict_nulls=strict_nulls)  # pyright: ignore[reportArgumentType]
        self.seed_incidence = build_incidence_table(hh_recoded, per_recoded, names)
        check_incidence_sums(self.seed_incidence, names, source_label="survey")

    def recode_pums(self) -> None:
        """Recode PUMS HH/persons through control expressions and pivot to incidence."""
        names = self.controls.target_names
        self.pums_hh = recode_pums_households(self.pums_hh, self.pums_per, names)
        self.pums_per = recode_pums_persons(self.pums_per, names)
        self.pums_incidence = build_incidence_table(
            self.pums_hh,
            self.pums_per,
            names,
            hh_id_col="SERIALNO",
            extra_cols=["WGTP", "PUMA"],
        )
        check_incidence_sums(self.pums_incidence, names, source_label="pums")

    def assign_zones(self) -> None:
        """Point-in-polygon zone assignment + optional block-group assignment.

        Mutates ``self.data.households`` in place (adds geo columns) and
        updates ``self.seed_incidence`` and ``self.pums_incidence`` with
        zone info.
        """
        hh = self.crosswalk.assign_households(self.data.households)  # pyright: ignore[reportArgumentType]
        n_assigned = hh.filter(pl.col("study_geoid").is_not_null()).height
        logger.info("Assigned %d / %d HHs to target zones", n_assigned, len(hh))

        if self.sample_plan is not None:
            hh = self.crosswalk.assign_block_groups(hh)
            n_bg = hh.filter(pl.col("bg_geo_id").is_not_null()).height
            logger.info("Assigned %d / %d HHs to block groups", n_bg, len(hh))

        self.data.households = hh

        hh_join_cols = ["hh_id", "study_geoid", "ctrl_geoid"]
        if "bg_geo_id" in hh.columns:
            hh_join_cols.append("bg_geo_id")

        self.seed_incidence = self.seed_incidence.join(
            hh.select(hh_join_cols),
            on="hh_id",
            how="left",
        )
        self.pums_incidence = self.crosswalk.allocate_pums_incidence(
            self.pums_incidence,
        )

    def apply_merges(self) -> None:
        """Apply 1-D merges symmetrically to both incidence tables.

        Cross-tab merges are applied at registration time (pre-merge into
        the enum), so only 1-D merges need post-pivot application here.
        """
        if self.controls.merges_1d:
            self.seed_incidence = apply_1d_merges(
                self.seed_incidence,
                self.controls.merges_1d,
            )
            self.pums_incidence = apply_1d_merges(
                self.pums_incidence,
                self.controls.merges_1d,
            )
            logger.info(
                "Applied %d 1-D merge specs; incidence now %d columns",
                len(self.controls.merges_1d),
                len(self.seed_incidence.columns),
            )

        # Check for sparse cross-tab cells that may cause balancing issues
        ctrl_instances = resolve_targets(self.controls.target_names)
        warn_crosstab_sparsity(self.seed_incidence, ctrl_instances)

    def aggregate_totals(self) -> None:
        """Aggregate PUMS incidence into per-zone control totals."""
        names = self.controls.target_names
        self.control_totals = aggregate_control_totals(
            self.pums_incidence,
            names,
            weight_col="WGTP",
            geo_col="ctrl_geoid",
        )
        if self.controls.merges_1d:
            self.control_totals = merge_control_totals(
                self.control_totals,
                self.controls.merges_1d,
            )
        logger.info(
            "Control totals: %d zones, %d PUMS HHs, %d PUMS persons",
            len(self.control_totals.geo_ids),
            self.control_totals.pums_hh_count,
            self.control_totals.pums_person_count,
        )

    def resolve_importance(self) -> None:
        """Build the final importance dict (MOE-based, explicit overrides, or default)."""
        overrides = dict(self.controls.importance_overrides)

        if self.importance_cfg.moe_based:
            pums_hh_xw, pums_per_xw = self.crosswalk.allocate_pums_weights(
                self.pums_hh,
                self.pums_per,
            )
            moe_importance = compute_moe_importance(
                pums_hh_xw,
                pums_per_xw,
                self.controls.target_names,
            )
            # YAML explicit overrides take precedence over MOE-derived
            moe_importance.update(overrides)
            overrides = moe_importance

            # Per-cell MOE for diagnostics
            self.control_moe = compute_control_moe(
                pums_hh_xw,
                pums_per_xw,
                self.controls.target_names,
            )

        default = self.importance_cfg.default
        full = {name: overrides.get(name, default) for name in self.controls.target_names}
        imp_lines = "\n".join(f"  {k}: {v:.1f}" for k, v in full.items())
        logger.info("Importance weights:\n%s", imp_lines)
        self.resolved_importance = overrides

    def balance(self) -> None:
        """Compute base weights, run max-entropy balancing, optional grid search."""
        names = self.controls.target_names
        self.seed_incidence = compute_base_weights(
            self.seed_incidence,
            self.control_totals,
            names,
            geo_col="ctrl_geoid",
            sample_plan=self.sample_plan,
            bg_populations=(self.crosswalk.block_group_populations if self.sample_plan else None),
        )

        imp_cfg = ImportanceConfig(
            explicit=self.resolved_importance,
            moe_based=False,  # already resolved
            default=self.importance_cfg.default,
        )
        self.weights, self.statuses = balance_weights(
            self.seed_incidence,
            self.control_totals,
            names,
            balancing=self.balancing,
            importance=imp_cfg,
        )

        n_failed = sum(not s.converged for s in self.statuses)
        if n_failed:
            msg = f"Balancing failed to converge for {n_failed} zones.  See logs for details."
            raise RuntimeError(msg)

        if self.expansion_factor_grid:
            self.grid_results = grid_search_expansion_factor(
                self.seed_incidence,
                self.control_totals,
                names,
                ef_grid=self.expansion_factor_grid,
                selected_ef=self.balancing.max_expansion_factor,
                balancing=self.balancing,
                importance=imp_cfg,
            )

    def generate_diagnostics(self) -> None:
        """Write the self-contained HTML diagnostics report."""
        report_dir = self.cache_dir or Path.cwd() / "weighting"
        zone_groups: dict[str, list[str]] | None = self.geography.get("zone_groups")
        generate_report(
            seed=self.seed_incidence,
            weights=self.weights,
            control_totals=self.control_totals,
            target_names=self.controls.target_names,
            statuses=self.statuses,
            output_path=report_dir / "diagnostics.html",
            puma_gdf=self.crosswalk.puma_gdf,
            target_gdf=self.crosswalk.target_gdf,
            crosswalk_df=self.crosswalk.crosswalk_df,
            zone_groups=zone_groups,
            merge_specs=self.controls.all_merges,
            grid_results=self.grid_results,
            selected_ef=(self.balancing.max_expansion_factor if self.grid_results else None),
            control_moe=self.control_moe,
        )

    @property
    def household_weights(self) -> pl.DataFrame:
        """The core pipeline output: ``hh_id → hh_weight``."""
        return self.weights.select("hh_id", "hh_weight")

    def propagate(self) -> None:
        """Attach weights to households and propagate to all tables on ``self.data``."""
        self.data.households = safe_join_weight(
            self.data.households,  # pyright: ignore[reportArgumentType]
            self.weights.select("hh_id", "hh_weight"),
            "hh_id",
        )
        self.data.households = self.data.households.join(
            self.seed_incidence.select("hh_id", "base_weight"),
            on="hh_id",
            how="left",
        )
        tables = collect_tables(
            households=self.data.households,
            persons=self.data.persons,
            days=self.data.days,
            unlinked_trips=self.data.unlinked_trips,
            linked_trips=self.data.linked_trips,
            joint_trips=self.data.joint_trips,
            tours=self.data.tours,
        )
        has_weight: dict[str, str] = {"households": "hh_weight"}
        propagate_weights(tables, has_weight)
        weight_sanity_checks(
            non_null_tables(tables),
            self.control_totals,
            self.controls.specs,
        )
        # Write propagated tables back to self.data
        for name, df in tables.items():
            if df is not None:
                setattr(self.data, name, df)


# ===========================================================================
# Pipeline step entry point
# ===========================================================================


@step()
def weighting(  # noqa: PLR0913
    # -- Config params (from YAML) --------------------------------------
    state_fips: str,
    pums_year: int,
    controls: list[dict],
    geography: dict,
    *,
    # -- Existing PUMS files (optional) ---------------------------------
    pums_households: str | None = None,
    pums_persons: str | None = None,
    # -- Sample plan (optional) -----------------------------------------
    sample_plan: str | None = None,
    # -- Pipeline plumbing (auto-injected by @step decorator) -----------
    pipeline_cache: PipelineCache | None = None,
    # -- Importance / MOE -----------------------------------------------
    moe_based_importance: bool = False,
    default_importance: float = 100.0,
    # -- Balancing params -----------------------------------------------
    max_expansion_factor: float = 10.0,
    min_expansion_factor: float = 0.1,
    min_weight: float | None = 1,
    max_weight: float | None = None,
    max_iterations: int = 10_000,
    n_workers: int = 1,
    # -- Diagnostics ----------------------------------------------------
    expansion_factor_grid: list[float] | None = None,
    # -- Validation ------------------------------------------------------
    strict_survey_nulls: bool = False,
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

    Flat-parameter entry point required by the ``@step()`` decorator
    (YAML → keyword args).  Constructs a :class:`WeightingPipeline` and
    delegates to :meth:`WeightingPipeline.run`.

    See :class:`WeightingPipeline` for full documentation of the algorithm,
    configuration, and diagnostics.
    """
    if households is None or persons is None:
        msg = "Weighting requires at least households and persons tables."
        raise ValueError(msg)

    # Prepare our data for the weighting pipeline
    # We reused the canonical data handler :)
    data = CanonicalData(
        households=households,
        persons=persons,
        days=days,
        unlinked_trips=unlinked_trips,
        linked_trips=linked_trips,
        tours=tours,
        joint_trips=joint_trips,
    )
    # Prepare the pipeline configuration objects
    wt_config = WeightingConfig(
        geography=geography,
        state_fips=state_fips,
        pums_year=pums_year,
        pums_households=pums_households,  # Optional local files take precedence over API fetching
        pums_persons=pums_persons,  # Optional local files take precedence over API fetching
        sample_plan=sample_plan,
        cache_dir=pipeline_cache.cache_dir if pipeline_cache else None,
        expansion_factor_grid=expansion_factor_grid,
        strict_survey_nulls=strict_survey_nulls,
    )
    # Prepare the balancing configs (max expansion factor, weight bounds, max iterations, etc.)
    balance_cfg = BalancingConfig(
        max_expansion_factor=max_expansion_factor,
        min_expansion_factor=min_expansion_factor,
        min_weight=min_weight,
        max_weight=max_weight,
        max_iterations=max_iterations,
        n_workers=n_workers,
    )
    # Prepare the importance config (MOE-based, explicit overrides, or default).
    importance_cfg = ImportanceConfig(
        moe_based=moe_based_importance,
        default=default_importance,
    )
    # Initialize and run the pipeline with the provided configs and data
    pipeline = WeightingPipeline(
        controls=ControlRegistryConfig.from_yaml(controls),
        config=wt_config,
        data=data,
        balancing=balance_cfg,
        importance=importance_cfg,
    )

    # 1. Setup — register controls, build crosswalk, fetch PUMS
    pipeline.setup()
    pipeline.fetch_pums()

    # 2. Incidence prep
    # Recode survey and PUMS into identical control columns
    # then pivot to incidence tables with the same column layout for both datasets.
    # Enables identical downstream processing (zone assignment, merges, balancing, etc.)
    pipeline.recode_survey()
    pipeline.recode_pums()

    # 2a. Zone assignment and merges are intertwined — merges may depend on zone groups, and

    pipeline.assign_zones()
    # 2b. Merge controls, must be applied after zone assignment and before total aggregation
    pipeline.apply_merges()

    # 3. Control aggregation
    pipeline.aggregate_totals()
    pipeline.resolve_importance()

    # 4. Balance + propagate weights
    pipeline.balance()
    pipeline.propagate()

    # 5. Diagnostics
    pipeline.generate_diagnostics()

    return non_null_tables(
        collect_tables(
            households=pipeline.data.households,
            persons=pipeline.data.persons,
            days=pipeline.data.days,
            unlinked_trips=pipeline.data.unlinked_trips,
            linked_trips=pipeline.data.linked_trips,
            joint_trips=pipeline.data.joint_trips,
            tours=pipeline.data.tours,
        )
    )
