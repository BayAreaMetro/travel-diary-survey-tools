"""Data structures shared between balancing and diagnostics packages.

Kept in a separate leaf module to avoid circular imports between
``balancer`` (which uses diagnostics helpers) and ``diagnostics``
(which uses these types).
"""

from dataclasses import dataclass
from typing import NamedTuple

import numpy as np
import polars as pl


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


class ZoneResult(NamedTuple):
    """Balancer output for a single geography zone."""

    weights: pl.DataFrame
    status: ZoneStatus


@dataclass
class MergeSpec:
    """Category-merge specification for one control.

    Attributes:
    ----------
    control : str
        Registry name (e.g. ``"p_employment"``).
    groups : dict[str, list[str]]
        Merged label -> list of base member names to combine.
        E.g. ``{"employed": ["employed_full", "employed_part"]}``.
    zones : list[str] | None
        If set, apply this merge only to these geo IDs.
        ``None`` means apply globally (all zones).
    """

    control: str
    groups: dict[str, list[str]]
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
