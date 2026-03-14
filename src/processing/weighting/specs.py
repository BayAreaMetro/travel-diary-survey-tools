"""Data structures for the weighting pipeline.

Single source of truth for all dataclasses and named tuples used across
data preparation, balancing, validation, and diagnostics.

Kept as a leaf module with minimal imports to avoid circular dependencies.
"""

from dataclasses import dataclass, field
from typing import NamedTuple

import numpy as np
import polars as pl

# ==============================================================================
# Data Preparation
# ==============================================================================


@dataclass
class ControlSpec:
    """Specification for a single weighting control.

    Parameters
    ----------
    name : str
        Registry name (must exist in ``CONTROLS``).
    importance : float | None
        Explicit importance weight for the balancer.  ``None`` means use
        the default (100 for normal controls, 1000 for structural) or
        the MOE-derived value when ``moe_based_importance`` is enabled.
    """

    name: str
    importance: float | None = None


@dataclass
class ControlTotals:
    """Result of PUMS control-total aggregation.

    Attributes:
    ----------
    totals : pl.DataFrame
        Tidy frame with columns:
        [geo_id, control_name, category, target_total]
    pums_hh_count : int
        Total PUMS housing unit records (before weighting).
    pums_person_count : int
        Total PUMS person records.
    geo_ids : list[str]
        Unique geography IDs in the totals.
    """

    totals: pl.DataFrame
    pums_hh_count: int
    pums_person_count: int
    geo_ids: list[str]


# ==============================================================================
# Balancing Configuration
# ==============================================================================


@dataclass
class MergeSpec:
    """Category-merge specification for one control.

    Supports both 1D (standard) and N-D (cross-tab) merges.

    Attributes:
    ----------
    control : str
        Registry name (e.g. ``"p_employment"`` or ``"h_income_x_size"``).
    groups : dict[str, list[str] | dict[str, list[str]]]
        Merge specifications. Each key is a merged label, each value is either:

        - **1D merge** (list): Base member names to combine.
          E.g. ``{"employed": ["employed_full", "employed_part"]}``

        - **N-D merge** (dict): Dimension name -> member names for cross-tabs.
          E.g. ``{"low_income_large": {"h_income": ["inc_under_25k", "inc_25k_to_50k"],
          "h_size": ["size_4", "size_5"]}}``

          The N-D merge creates a single merged cell from the cartesian product
          of all specified dimension members.
    zones : list[str] | None
        If set, apply this merge only to these geo IDs.
        ``None`` means apply globally (all zones).

    Examples:
    --------
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


@dataclass
class GridPoint:
    """Aggregate metrics for one expansion-factor grid point."""

    max_expansion_factor: float
    converged_zones: int
    total_zones: int
    mape: float
    p90: float
    cv: float
    ess_pct: float


@dataclass
class SamplePlan:
    """Stratified sampling plan mapping zones to segments.

    Each row represents a target zone.  Zones that share the same
    ``sample_segment`` are treated as a single stratum for initial-weight
    computation: ``base_weight = segment_target_pop / segment_n_responses``.

    Population totals per zone are sourced from the crosswalk
    (:attr:`PumaCrosswalk.zone_populations`), not from this table.

    Attributes:
    ----------
    strata : pl.DataFrame
        Required columns:

        * ``geo_id``  (str) — target-zone identifier (matches ``ctrl_geoid``).
        * ``sample_segment`` (str) — sampling-stratum label.  All zones
          sharing a segment get the same base weight.
    """

    strata: pl.DataFrame

    # -- validation --
    _REQUIRED_COLS: tuple[str, ...] = field(
        default=("geo_id", "sample_segment"),
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
