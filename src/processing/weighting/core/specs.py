"""Data structures for the weighting pipeline.

Single source of truth for all dataclasses and named tuples used across
data preparation, balancing, validation, and diagnostics.

Kept as a leaf module with minimal imports to avoid circular dependencies.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple

import numpy as np
import polars as pl
from pydantic import BaseModel, ConfigDict, Field

from processing.completeness import usable_col_for
from processing.weighting.data_prep.crosswalk import GeographyConfig

logger = logging.getLogger(__name__)

# ==============================================================================
# Data Preparation
# ==============================================================================


@dataclass
class ControlSpec:
    """Specification for a single weighting control.

    Attributes:
        name: Registry name (must exist in ``CONTROLS``).
        importance: Explicit importance weight for the balancer.  ``None`` means use
            the default (100 for normal controls, 1000 for structural) or
            the MOE-derived value when ``moe_based_importance`` is enabled.
        dimensions: For cross-tab controls only: list of dimension control names
            (e.g. ``["h_size", "h_income"]``).  ``None`` for standard controls.
        merges: For cross-tab controls only: per-dimension merge specs applied
            at registration time.  ``None`` for standard controls.
    """

    name: str
    importance: float | None = None
    dimensions: list[str] | None = None
    merges: dict | None = None


@dataclass
class ControlTotals:
    """Result of PUMS control-total aggregation.

    Attributes:
        totals: Tidy frame with columns: [geo_id, control_name, category, target_total]
        pums_hh_count: Total PUMS housing unit records (before weighting).
        pums_person_count: Total PUMS person records.
        geo_ids: Unique geography IDs in the totals.
    """

    totals: pl.DataFrame
    pums_hh_count: int
    pums_person_count: int
    geo_ids: list[str]


# ---------------------------------------------------------------------------
# Incidence Handler
# ---------------------------------------------------------------------------


@dataclass
class IncidenceBundle:
    """All intermediate and final products of incidence-table construction.

    Built by
    [`build_incidence_table`][processing.weighting.data_prep.incidence.build_incidence_table]
    and available for downstream consumers (balancer, fractional imputation, diagnostics).

    Attributes:
        incidence: Combined household-level incidence table — one row per HH with
            ``{ctrl}__{member}`` columns for *all* controls (HH + person
            aggregated).  This is what the balancer operates on.
        person_pivot: Per-person 0/1 indicator table — one row per person with
            ``{p_ctrl}__{member}`` columns.  Useful for person-level
            classification (fractional imputation of null persons).
        household_pivot: HH-only 0/1 indicator table — one row per HH with
            ``{h_ctrl}__{member}`` columns.  Useful for HH-level
            classification without person-count noise.
    """

    incidence: pl.DataFrame
    person_pivot: pl.DataFrame
    household_pivot: pl.DataFrame

    def filter_households(
        self, hh_ids: list | pl.Series, hh_id_col: str = "hh_id"
    ) -> "IncidenceBundle":
        """Return a new bundle keeping only the given household IDs."""
        return IncidenceBundle(
            incidence=self.incidence.filter(pl.col(hh_id_col).is_in(hh_ids)),
            person_pivot=self.person_pivot.filter(pl.col(hh_id_col).is_in(hh_ids)),
            household_pivot=self.household_pivot.filter(pl.col(hh_id_col).is_in(hh_ids)),
        )


# ---------------------------------------------------------------------------
# Imputation Metadata
# ---------------------------------------------------------------------------


@dataclass
class ImputationSummary:
    """Per-control fractional seed imputation metadata for diagnostics.

    Describes what [`fill_null_incidence`]
    [processing.weighting.data_prep.fractional_impute.fill_null_incidence]
    filled in the seed incidence matrix -- never the survey ``imputation``
    step, which writes discrete values to canonical data and is reported
    separately.

    One entry per control target, regardless of whether any filling was
    needed.  Controls with no nulls have ``n_null == 0`` and ``None``
    for the RF metrics.
    """

    control: str
    level: str  # "person" | "household"
    n_total: int
    n_null: int
    log_loss: float | None = None
    f1_macro: float | None = None


# ---------------------------------------------------------------------------
# PUMS Configuration
# ---------------------------------------------------------------------------
@dataclass
class PUMSSource:
    """Configuration for PUMS data source.

    Attributes:
        state_fips: Two-digit FIPS code for the state (e.g. "06" for California).
        pums_year: ACS 1-year PUMS vintage (e.g. 2022).
        puma_ids: Optional list of PUMA codes to fetch. If None, fetches all PUMAs in
            the state (can be large).
    """

    state_fips: str
    pums_year: int
    puma_ids: list[str] | None = None


# ==============================================================================
# Balancing Configuration
# ==============================================================================


@dataclass
class MergeSpec:
    """Category-merge specification for one control.

    Supports both 1D (standard) and N-D (cross-tab) merges.

    Attributes:
        control: Registry name (e.g. ``"p_employment"`` or ``"h_income_x_size"``).
        groups: Merge specifications. Each key is a merged label, each value is either:

            - **1D merge** (list): Base member names to combine.
              E.g. ``{"employed": ["employed_full", "employed_part"]}``

            - **N-D merge** (dict): Dimension name -> member names for cross-tabs.
              E.g. ``{"low_income_large": {"h_income": ["inc_under_25k", "inc_25k_to_50k"],
              "h_size": ["size_4", "size_5"]}}``

              The N-D merge creates a single merged cell from the cartesian product
              of all specified dimension members.
        zones: If set, apply this merge only to these geo IDs.
            ``None`` means apply globally (all zones).

    Example:
        1D merge:

    >>> MergeSpec(
    ...     control="p_employment",
    ...     groups={"employed": ["employed_full", "employed_part"]},
    ... )

    N-D merge for cross-tab:

    >>> MergeSpec(
    ...     control="h_income_x_size",
    ...     groups={
    ...         "low_income_large": {
    ...             "h_income": ["inc_under_25k", "inc_25k_to_50k"],
    ...             "h_size": ["size_4", "size_5", "size_6"],
    ...         }
    ...     },
    ... )
    """

    control: str
    groups: dict[str, list[str] | dict[str, list[str]]]
    zones: list[str] | None = None


class CensusConfig(BaseModel):
    """Where the Census controls come from: the state, the PUMS year, and optional local files."""

    model_config = ConfigDict(extra="forbid")

    state_fips: str = Field(description="Two-digit state FIPS code, e.g. '06'.")
    pums_year: int = Field(description="ACS PUMS 5-year vintage the controls are built from.")
    pums_households: str | None = Field(
        default=None,
        description="Local PUMS household file; fetched from the Census API when not given.",
    )
    pums_persons: str | None = Field(
        default=None,
        description="Local PUMS person file; fetched from the Census API when not given.",
    )


class BalancingConfig(BaseModel):
    """Maximum-entropy balancer configuration, and how hard each control pulls.

    Controls Newton-Raphson iteration bounds, weight expansion limits, parallel
    execution and the importance given to controls without their own. Consumed by
    [`balance_weights`][processing.weighting.balancing.balancer.balance_weights],
    [`grid_search_expansion_factor`][processing.weighting.balancing.balancer.grid_search_expansion_factor],
    and the pipeline orchestrator.
    """

    model_config = ConfigDict(extra="forbid")

    max_expansion_factor: float = Field(
        description="Upper bound on a weight, as a multiple of its seed weight."
    )
    min_expansion_factor: float = Field(
        description="Lower bound on a weight, as a multiple of its seed weight."
    )
    min_weight: float | None = Field(description="Absolute weight floor; null for none.")
    max_weight: float | None = Field(
        default=None, description="Absolute weight ceiling; none unless given."
    )
    max_iterations: int = Field(description="Newton-Raphson iteration limit per zone.")
    n_workers: int = Field(description="Zones (or grid points) balanced in parallel.")
    moe_based_importance: bool = Field(
        description="Derive each control's importance from its PUMS replicate-weight MOE."
    )
    default_importance: float = Field(
        description="Importance for a control without its own, when not MOE-derived."
    )


@dataclass
class ImportanceConfig:
    """Importance weighting configuration.

    Determines how per-control importance weights are computed:

    - ``explicit`` — YAML-declared overrides (always highest precedence).
    - ``moe_based`` — derive importance from PUMS replicate-weight CVs.
    - ``default`` — fallback value when neither of the above applies.
    """

    explicit: dict[str, float] = field(default_factory=dict)
    moe_based: bool = False
    default: float = 100.0


def resolve_fitted_profiles(
    weight_profiles: tuple[str, ...] | list[str] | None,
) -> tuple[str, ...]:
    """The profiles to weight: one entry per weight column set to be written.

    Shared by both weighting entry points so they cannot disagree about what a
    profile list is supposed to mean.

    Args:
        weight_profiles: Profiles to weight, each writing its own suffixed
            column set. A single profile is a list of one.

    Returns:
        One entry per profile, in the order given.

    Raises:
        ValueError: If no profile is given, or a profile is named twice.
    """
    profiles = tuple(weight_profiles or ())
    if not profiles:
        msg = (
            "The weighting needs a universe: name weight_profiles, one weight set "
            "per profile (a single profile is a list of one)."
        )
        raise ValueError(msg)

    duplicates = sorted({p for p in profiles if profiles.count(p) > 1})
    if duplicates:
        msg = f"weight_profiles names the same profile twice: {duplicates}"
        raise ValueError(msg)

    return profiles


@dataclass
class WeightingConfig:
    """Top-level configuration for the weighting pipeline.

    Groups the parameters that are *not* already covered by
    [`ControlRegistryConfig`][processing.weighting.core.specs.ControlRegistryConfig],
    [`BalancingConfig`][processing.weighting.core.specs.BalancingConfig], or
    [`ImportanceConfig`][processing.weighting.core.specs.ImportanceConfig]
    — primarily Census/geography settings and pipeline plumbing.
    """

    geography: GeographyConfig
    state_fips: str
    pums_year: int
    # Which usability profiles decide who carries weight, one fit each. Required:
    # with several profiles stamped there is no defensible default, and picking
    # silently would weight a different universe than the formatters emit.
    weight_profiles: tuple[str, ...] = ()
    max_unplaceable_share: float = 0.01
    pums_households: str | None = None
    pums_persons: str | None = None
    sample_plan: str | None = None
    cache_dir: Path | None = None
    expansion_factor_grid: list[float] | None = None
    strict_survey_nulls: bool = False
    _fitted_profiles: tuple[str, ...] = field(default=(), init=False, repr=False)

    def __post_init__(self) -> None:
        """Reject a weighting universe that is unstated or names a profile twice."""
        self._fitted_profiles = resolve_fitted_profiles(self.weight_profiles)

    @property
    def fitted_profiles(self) -> tuple[str, ...]:
        """One entry per balancing run."""
        return self._fitted_profiles

    def flag_for(self, profile: str) -> str:
        """The verdict column the fit for *profile* is gated on.

        A profile names both its gate and its weights -- ``usable_<profile>`` and
        ``hh_weight_<profile>`` -- so there is no second setting to disagree with
        the first.
        """
        return usable_col_for(profile)


@dataclass
class ControlRegistryConfig:
    """Parsed control definitions, merge specs, and derived target names.

    Built via [`from_yaml`][processing.weighting.core.specs.ControlRegistryConfig.from_yaml]
    from the YAML ``controls`` block.
    """

    specs: list[ControlSpec]
    target_names: list[str]
    crosstab_merges: list[MergeSpec]
    merges_1d: list[MergeSpec]

    @classmethod
    def from_yaml(cls, controls: list[dict]) -> "ControlRegistryConfig":
        """Parse the YAML controls block into a config object."""
        _spec_keys = ("name", "importance", "dimensions", "merges")
        specs = [ControlSpec(**{k: v for k, v in c.items() if k in _spec_keys}) for c in controls]
        target_names = [s.name for s in specs]

        crosstab_merges: list[MergeSpec] = []
        merges_1d: list[MergeSpec] = []

        for c in controls:
            # Cross-tab merges are applied at registration time, not here.
            # Only parse 1-D merges and zone-specific merges.
            global_labels: set[str] = set()
            if c.get("merge"):
                merges_1d.append(MergeSpec(control=c["name"], groups=c["merge"]))
                global_labels = set(c["merge"].keys())
            for zone_id, groups in c.get("zone_merges", {}).items():
                overlap = set(groups.keys()) & global_labels
                if overlap:
                    msg = (
                        "Control '%s' zone_merges for '%s' redefines global "
                        "merge label(s) %s — the zone merge is redundant "
                        "because the global merge already applies everywhere.",
                        c["name"],
                        zone_id,
                        sorted(overlap),
                    )
                    raise ValueError(msg)
                merges_1d.append(MergeSpec(control=c["name"], groups=groups, zones=[zone_id]))

        return cls(
            specs=specs,
            target_names=target_names,
            crosstab_merges=crosstab_merges,
            merges_1d=merges_1d,
        )

    @property
    def importance_overrides(self) -> dict[str, float]:
        """Explicit importance overrides from YAML."""
        return {s.name: s.importance for s in self.specs if s.importance is not None}

    @property
    def all_merges(self) -> list[MergeSpec]:
        """All merge specs (crosstab + 1-D) for diagnostics."""
        return self.crosstab_merges + self.merges_1d


@dataclass
class GridPoint:
    """Aggregate metrics for one expansion-factor grid point."""

    max_expansion_factor: float
    converged_zones: int
    total_zones: int
    mape: float
    p90: float
    max_error: float
    cv: float
    ess_pct: float


@dataclass
class SamplePlan:
    """Stratified sampling plan mapping Census block groups to segments.

    Each row represents a Census block group.  Block groups that share
    the same ``sample_segment`` are treated as a single stratum for
    initial-weight computation:
    ``base_weight = segment_bg_pop / segment_n_responses``.

    Block-group population totals are sourced from the crosswalk
    ([`PumaCrosswalk.block_group_populations`][processing.weighting.data_prep.crosswalk.PumaCrosswalk.block_group_populations]), not from this table.

    Attributes:
        strata: Required columns:

            * ``bg_geo_id``  (str) — 12-character Census block-group FIPS code.
            * ``sample_segment`` (str) — sampling-stratum label.  All block
              groups sharing a segment get the same base weight.
    """  # noqa: E501

    strata: pl.DataFrame

    # -- validation --
    _REQUIRED_COLS: tuple[str, ...] = field(
        default=("bg_geo_id", "sample_segment"),
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        """Validate that *strata* has the required columns."""
        missing = [c for c in self._REQUIRED_COLS if c not in self.strata.columns]
        if missing:
            msg = f"SamplePlan.strata missing required columns: {missing}"
            raise ValueError(msg)


# ==============================================================================
# Balancing Internals
# ==============================================================================


class ZoneStatus(NamedTuple):
    """Per-zone convergence diagnostics."""

    geo_id: str
    converged: bool
    iterations: int
    delta: float
    max_gamma_diff: float


class ZoneInput(NamedTuple):
    """Pre-built numpy arrays for a single geography zone."""

    hh_ids: pl.Series
    incidence: np.ndarray
    initial: np.ndarray
    lb: np.ndarray
    ub: np.ndarray
    targets: np.ndarray
    importance: np.ndarray
    master_idx: int
    max_iterations: int
    geo_id: str
    verbose: bool = True


class ZoneResult(NamedTuple):
    """Balancer output for a single geography zone."""

    weights: pl.DataFrame
    status: ZoneStatus


# ==============================================================================
# Per-profile fits
# ==============================================================================


class GeographyCoverage(NamedTuple):
    """How much of one profile's universe the control geography can place.

    A household the control polygons do not contain belongs to no balancing zone,
    so no fit can give it a weight. That is a bound on what the weighting can
    answer, not a defect in the household, and it is counted so a reader sees the
    bound rather than inferring it from nulls.
    """

    profile: str | None
    n_universe: int
    n_placed: int

    @property
    def n_unplaceable(self) -> int:
        """Households in the profile that the control geography cannot place."""
        return self.n_universe - self.n_placed

    @property
    def unplaceable_share(self) -> float:
        """Unplaceable households as a share of the profile's universe."""
        return self.n_unplaceable / self.n_universe if self.n_universe else 0.0


@dataclass
class ProfileFit:
    """Everything one balancing run produced.

    A run may fit several profiles over the same survey. Holding each fit's
    products here rather than on the pipeline keeps them from overwriting one
    another -- otherwise the last profile's seed silently becomes the one the
    diagnostics describe and the checks read.

    Attributes:
        profile: Usability profile fitted, or None for a single un-suffixed set.
            Suffixes every weight column this fit writes.
        usability_flag_col: The column that gated the seed and will gate the
            propagation. Equal to *profile* whenever a profile was named.
        seed_incidence: The seed this fit was solved from, carrying its own
            ``base_weight`` once balancing has run.
        pre_imputation_incidence: The same seed before null imputation, for the
            diagnostics report.
        imputation_summary: Per-control imputation metadata for this seed.
        coverage: How much of the profile's universe had a control geography.
        weights: Balancer output, keyed by ``hh_id``, carrying the *base* weight
            column name -- the suffix is applied where it is joined on.
        statuses: Per-zone convergence diagnostics.
        grid_results: Expansion-factor grid points, when a grid was configured.
    """

    profile: str | None
    usability_flag_col: str
    seed_incidence: pl.DataFrame
    pre_imputation_incidence: pl.DataFrame
    imputation_summary: list[ImputationSummary]
    coverage: GeographyCoverage
    weights: pl.DataFrame | None = None
    statuses: list[ZoneStatus] = field(default_factory=list)
    grid_results: list[GridPoint] | None = None

    @property
    def unconverged_zones(self) -> int:
        """Zones this fit failed to converge."""
        return sum(not s.converged for s in self.statuses)
