"""Entry point for the weighting pipeline step.

Orchestrates the full weighting pipeline, which consists of the following stages:
1. **Setup** -- register controls, build crosswalk, prepare sample plan, etc.
2. **PUMS recoding** -- recode PUMS microdata into the same control categories as the survey seed.
3. **Survey recoding** -- recode canonical survey data into control categories.
4. **Null imputation** -- fill null-induced zeros in the survey incidence with RF-predicted fractional probabilities.
5. **Zone assignment** -- assign survey records to weighting zones via crosswalk.
6. **Merges** -- apply any user-specified merges to control categories.
7. **Control aggregation** -- aggregate recoded PUMS microdata into control totals per zone.
8. **Importance resolution** -- compute control importance values based on MOE or explicit config.
9. **Balancing** -- fit weights using PopulationSim's numba core balancer per zone.
10. **Weight propagation** -- propagate final household weights to all canonical tables (persons, days, trips, tours).
11. **Diagnostics** -- generate an interactive HTML report with diagnostics and validation results.
12. **Validation** -- run sanity checks on the final weights and control totals.
Done! Easy as 1-2-3...12. :)
"""  # noqa: E501

import logging

import polars as pl

from data_canon.core.dataclass import CanonicalData
from pipeline.cache import PipelineCache
from pipeline.decoration import step
from processing.weighting.specs import (
    BalancingConfig,
    ControlRegistryConfig,
    ImportanceConfig,
    WeightingConfig,
)
from processing.weighting.validation.weight_checks import weight_sanity_checks
from processing.weighting.weighting_pipeline import WeightingPipeline

logger = logging.getLogger(__name__)


# ===========================================================================
# Pipeline step entry point
# ===========================================================================


@step()
def compute_weights(  # noqa: PLR0913
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
    wt_pipeline = WeightingPipeline(
        controls=ControlRegistryConfig.from_yaml(controls),
        config=wt_config,
        data=data,
        balancing=balance_cfg,
        importance=importance_cfg,
    )

    # 1. Setup — register controls, build crosswalk, fetch PUMS
    wt_pipeline.setup()
    wt_pipeline.fetch_pums()

    # 2. Incidence prep — recode both PUMS and survey, pivot, fill nulls
    wt_pipeline.recode_and_pivot()

    # 3. Zone assignment and merges are intertwined — merges may depend on zone groups
    wt_pipeline.assign_zones()

    # 4. Merge controls, must be applied after zone assignment and before total aggregation
    wt_pipeline.apply_merges()

    # 5. Control aggregation
    wt_pipeline.aggregate_totals()
    wt_pipeline.resolve_importance()

    # 6. Balance + propagate weights
    wt_pipeline.balance()
    wt_pipeline.propagate()

    # 7. Diagnostics
    wt_pipeline.generate_diagnostics()

    # Basic sanity check to ensure weights were propagated to all tables before returning
    result_tables = wt_pipeline.data.as_dict_non_null()
    weight_sanity_checks(
        result_tables,
        wt_pipeline.control_totals,
        wt_pipeline.controls.specs,
    )

    return result_tables
