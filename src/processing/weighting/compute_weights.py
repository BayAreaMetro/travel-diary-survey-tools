"""Entry point for the weighting pipeline step.

Orchestrates the full weighting pipeline in the following stages:

**A. [Geography Crosswalk][weighting-crosswalk]**

1. **Setup** -- register controls from YAML config, build the geographic
   crosswalk (translating between Census PUMAs and the project's custom
   weighting geography using block-group population as the intermediary),
   and prepare the sample plan.

**B. [Control Data Preparation][weighting-data-preparation]**

2. **PUMS recoding** -- load PUMS 1-year microdata and recode into the
   YAML-configured variable bins used by the controls.
3. **Merges** -- apply any user-specified category merges (global or
   zone-specific) to both controls and the survey incidence table.
4. **Control aggregation** -- apply the crosswalk to PUMS and aggregate
   into marginal control totals per zone.

**C. [Survey Seed Preparation][weighting-data-preparation]**

5. **Survey recoding** -- recode canonical survey variables into the same
   bin / group categories as the PUMS controls.
6. **Null imputation** -- fill null-induced zeros in the survey incidence
   table with RF-predicted fractional class probabilities.
7. **Zone assignment** -- assign survey households to weighting zones via
   the geographic crosswalk.

**D. [Maximum-Entropy Balancing][weighting-balancing]**

8. **Importance resolution** -- compute per-control importance weights
   from replicate-weight MOE or explicit YAML overrides.
9. **Balancing** -- build per-household incidence vectors and fit weights
   using a numba-accelerated max-entropy balancer (vendored from
   PopulationSim), independently per zone, once per profile.
10. **[Weight propagation][weighting-hierarchy]** -- propagate each profile's
    ``hh_weight_<profile>`` to all canonical tables (persons, days, trips, tours).

**E. [Diagnostics][weighting-diagnostics] & [Validation][weighting-validation]**

11. **Diagnostics** -- generate one self-contained interactive HTML report
    for the run, comparing every profile fitted and carrying each one's
    convergence, fit, weight-quality and weight-cascade diagnostics.
12. **Validation** -- run sanity checks on the final weights and control
    totals. The fit against the controls is logged; the hierarchy identities,
    the joint-table sums and weight placement raise when they fail. None of it
    is in the HTML report -- check the pipeline log.

"""

import logging

import polars as pl

from data_canon.core.dataclass import CanonicalData
from pipeline.cache import PipelineCache
from pipeline.decoration import step
from processing.weighting.core.hierarchy import (
    describe_weight_columns,
    seed_col_for,
    weight_col_for,
)
from processing.weighting.core.pipeline import WeightingPipeline
from processing.weighting.core.propagation import drop_unsuffixed_weights
from processing.weighting.core.specs import (
    BalancingConfig,
    CensusConfig,
    ControlRegistryConfig,
    DiagnosticsConfig,
    ImportanceConfig,
    WeightingConfig,
)
from processing.weighting.data_prep.crosswalk import GeographyConfig
from processing.weighting.validation.weight_checks import weight_sanity_checks

logger = logging.getLogger(__name__)

# Geography the balancer works from. It rides on ``households`` through the run
# because zone assignment, balancing and diagnostics all read it, then moves to
# its own table before the tables are handed back. Profile-independent: where a
# household is does not depend on which universe was fitted.
_GEOGRAPHY_COLUMNS = ("study_geoid", "ctrl_geoid", "home_puma", "bg_geo_id")


def _split_weighting_columns(
    tables: dict[str, pl.DataFrame],
    profiles: tuple[str | None, ...],
) -> None:
    """Move the weighting's working columns off ``households`` in place.

    Each fit's ``base_weight`` moves and its ``hh_weight`` is copied: the weight
    is the deliverable and stays on ``households``, but repeating it beside the
    seed weight lets an expansion factor be read off one table. The table stays
    one row per household, so several profiles widen it rather than lengthen it.
    """
    hh = tables.get("households")
    if hh is None:
        return

    geography = [c for c in _GEOGRAPHY_COLUMNS if c in hh.columns]
    seed_weights = [col for p in profiles if (col := seed_col_for("base_weight", p)) in hh.columns]
    if not geography and not seed_weights:
        return

    carried = [col for p in profiles if (col := weight_col_for("hh_weight", p)) in hh.columns]
    moved = geography + seed_weights
    tables["household_weights"] = hh.select("hh_id", *moved, *carried)
    tables["households"] = hh.drop(moved)


# ===========================================================================
# Pipeline step entry point
# ===========================================================================


@step(
    # Every field any control in weighting/controls/ reads, whether or not a project
    # turns that control on. Adding a control means adding its fields here.
    requires={
        "households": {
            "hh_id",
            "home_lon",
            "home_lat",
            "survey_complete",
            "usable_<profile>",
            "income_bin",  # h_income
            "num_vehicles",  # h_vehicles, h_vehicles_per_driver
        },
        "persons": {
            "person_id",
            "hh_id",  # also h_size
            "employment",  # h_workers, p_employment
            "age",  # h_children, h_vehicles_per_driver, p_age, p_student, p_education
            "student",  # p_student
            "school_type",  # p_student
            "gender",  # p_gender
            "race",  # p_race
            "ethnicity",  # p_ethnicity
            "education",  # p_education
            "work_mode",  # p_commute_mode
            "job_type",  # p_commute_mode
            "telework_freq",  # p_commute_mode
            "commute_freq",  # p_commute_mode
        },
        "person_days": {"person_day_id", "person_id"},
        "unlinked_trips": {"unlinked_trip_id", "person_day_id", "linked_trip_id"},
        "linked_trips": {"linked_trip_id", "tour_id", "joint_trip_id"},
        "tours": {"tour_id", "joint_tour_id"},
        "joint_trips": {"joint_trip_id"},
        "joint_tours": {"joint_tour_id"},
    },
    produces={
        "households": {"hh_weight_<profile>"},
        "persons": {"person_weight_<profile>"},
        "person_days": {"person_day_weight_<profile>"},
        "unlinked_trips": {"unlinked_trip_weight_<profile>"},
        "linked_trips": {"linked_trip_weight_<profile>"},
        "tours": {"tour_weight_<profile>"},
        "joint_trips": {"joint_trip_weight_<profile>"},
        "joint_tours": {"joint_tour_weight_<profile>"},
        "household_weights": {
            "hh_id",
            "study_geoid",
            "ctrl_geoid",
            "home_puma",
            "bg_geo_id",
            "base_weight_<profile>",
            "hh_weight_<profile>",
        },
    },
)
def compute_weights(  # noqa: PLR0913
    # -- Settings (from YAML) -------------------------------------------
    census: CensusConfig,
    geography: GeographyConfig,
    balancing: BalancingConfig,
    controls: list[dict],
    diagnostics: DiagnosticsConfig,
    weight_profiles: list[str],
    max_unplaceable_share: float,
    sample_plan: str | None,
    strict_survey_nulls: bool = False,
    *,
    # -- Pipeline plumbing (auto-injected by @step decorator) -----------
    pipeline_cache: PipelineCache | None = None,
    # -- Canonical tables (auto-injected by pipeline) -------------------
    canonical_data: CanonicalData | None = None,
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    person_days: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    joint_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
    joint_tours: pl.DataFrame | None = None,
) -> dict[str, pl.DataFrame]:
    """Compute expansion weights from PUMS controls and propagate to all tables.

    Constructs a
    [`WeightingPipeline`][processing.weighting.core.pipeline.WeightingPipeline];
    full documentation of the algorithm, configuration, and diagnostics is
    included in that class.

    Args:
        census: Where the Census controls come from. See ``CensusConfig``.
        geography: The zones weights are balanced over. See ``GeographyConfig``.
        balancing: Solver bounds and control importance. See ``BalancingConfig``.
        controls: The control list, each by registry name with optional
            ``importance`` and ``merge``.
        diagnostics: The report. See ``DiagnosticsConfig``.
        weight_profiles: Usability profiles to weight, one fit each; a single
            profile is a list of one.
        max_unplaceable_share: The most of the survey whose homes may fall
            outside the control geography before the step raises.
        sample_plan: Path to the sample plan for base weights; null for none.
        strict_survey_nulls: Raise on survey nulls in control variables rather
            than imputing them.
        pipeline_cache: Supplied by the pipeline.
        canonical_data: Supplied by the pipeline.
        households: Households.
        persons: Persons.
        person_days: Days.
        unlinked_trips: Unlinked trips.
        linked_trips: Linked trips.
        joint_trips: Joint trips.
        tours: Tours.
        joint_tours: Joint tours.
    """
    if households is None or persons is None:
        msg = "Weighting requires at least households and persons tables."
        raise ValueError(msg)

    # Prepare our data for the weighting pipeline
    # We reused the canonical data handler :)
    data = CanonicalData(
        households=households,
        persons=persons,
        person_days=person_days,
        unlinked_trips=unlinked_trips,
        linked_trips=linked_trips,
        tours=tours,
        joint_trips=joint_trips,
        joint_tours=joint_tours,
    )
    wt_config = WeightingConfig(
        geography=geography,
        state_fips=census.state_fips,
        pums_year=census.pums_year,
        pums_weights=census.pums_weights,
        pums_households=census.pums_households,  # local files take precedence over the API
        pums_persons=census.pums_persons,
        sample_plan=sample_plan,
        cache_dir=pipeline_cache.cache_dir if pipeline_cache else None,
        expansion_factor_grid=diagnostics.expansion_factor_grid,
        strict_survey_nulls=strict_survey_nulls,
        weight_profiles=tuple(weight_profiles),
        max_unplaceable_share=max_unplaceable_share,
    )
    # Importance for controls without their own; per-control overrides come from controls.
    importance_cfg = ImportanceConfig(
        moe_based=balancing.moe_based_importance,
        default=balancing.default_importance,
    )
    wt_pipeline = WeightingPipeline(
        controls=ControlRegistryConfig.from_yaml(controls),
        config=wt_config,
        data=data,
        balancing=balancing,
        importance=importance_cfg,
    )

    # 1. Setup — register controls, build crosswalk, fetch PUMS
    wt_pipeline.setup()
    wt_pipeline.fetch_pums()

    # 2. Place households first: h_puma's recode reads their home PUMA
    wt_pipeline.place_households()

    # 3. Incidence prep — recode both PUMS and survey, pivot, fill nulls
    wt_pipeline.recode_and_pivot()

    # 4. Zone allocation and merges are intertwined — merges may depend on zone groups
    wt_pipeline.allocate_pums()

    # 5. Merge controls, must be applied after zone allocation and before total aggregation
    wt_pipeline.apply_merges()

    # 6. Control aggregation
    wt_pipeline.aggregate_totals()
    wt_pipeline.resolve_importance()

    # 7. One fit per profile: balance, propagate, and report each in turn
    profiles = wt_config.fitted_profiles
    logger.info(
        "Weighting: %d balancing run(s), one per profile (%s). Each is fitted to the "
        "controls over its own universe and writes its own columns; nothing is derived "
        "from another fit.",
        len(profiles),
        ", ".join(profiles),
    )
    # The whole block, not one key: the comparer also reads it, and it must run
    # before drop_unsuffixed_weights below takes the supplied weights away.
    fits = wt_pipeline.fit_all(diagnostics=diagnostics)

    # Basic sanity check to ensure weights were propagated to all tables before returning
    result_tables = wt_pipeline.data.as_dict_non_null()
    for fit in fits.values():
        weight_sanity_checks(
            result_tables,
            wt_pipeline.control_totals,
            wt_pipeline.controls.specs,
            fit.usability_flag_col,
            profile=fit.profile,
        )

    drop_unsuffixed_weights(result_tables)

    # The suffixed names come from config, so they cannot be model fields; without
    # registering them the delivered output would drop them.
    if canonical_data is not None:
        for table, described in describe_weight_columns(profiles).items():
            canonical_data.register_generated_columns(table, described)

    # After the checks, which still read the seed off households.
    _split_weighting_columns(result_tables, profiles)

    return result_tables
