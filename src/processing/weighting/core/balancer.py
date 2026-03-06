"""Maximum-entropy list balancer.

Thin Polars->numpy bridge around PopulationSim's ``np_balancer_numba``.
Runs independently per geography zone.
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import NamedTuple

import numpy as np
import polars as pl
from populationsim.balancing.balancers_numba import np_balancer_numba

from processing.weighting.core.control_data import ControlTotals
from processing.weighting.core.controls import CONTROLS, ControlLevel, ControlTarget

logger = logging.getLogger(__name__)


class ZoneStatus(NamedTuple):
    """Per-zone convergence diagnostics."""

    geo_id: str
    converged: bool
    iterations: int
    delta: float
    max_gamma_diff: float


# -- Public API ------------------------------------------------------------


def balance_weights(
    seed: pl.DataFrame,
    control_totals: ControlTotals,
    targets: list[str],
    *,
    geo_col: str = "puma_id",
    max_expansion_factor: float = 10.0,
    min_expansion_factor: float = 0.1,
    min_weight: float | None = None,
    max_weight: float | None = None,
    max_iterations: int = 10_000,
    n_workers: int = 1,
) -> tuple[pl.DataFrame, list[ZoneStatus]]:
    """Balance household weights to match control totals per zone.

    Parameters
    ----------
    seed : pl.DataFrame
        From ``build_seed_table``.  Must have ``hh_id``, *geo_col*,
        ``ctrl_*`` and ``inc_*`` columns.
    control_totals : ControlTotals
        PUMS-derived targets from ``build_control_totals``.
    targets : list[str]
        Control registry names.
    geo_col : str
        Geography column on *seed*.
    max_expansion_factor, min_expansion_factor : float
        Bounds on final / initial weight ratio.
    min_weight, max_weight : float | None
        Optional absolute floor / ceiling applied after expansion-factor
        scaling.  ``None`` means no absolute bound.
    max_iterations : int
        Newton-Raphson cap per zone.
    n_workers : int
        Threads for parallel zone balancing (``1`` = sequential).
        Numba releases the GIL, so real parallelism is achieved.

    Returns:
    -------
    weights : pl.DataFrame
        Columns: ``hh_id``, ``hh_weight``, ``geo_id``.
    statuses : list[ZoneStatus]
        One entry per zone with convergence info.
    """
    # Pre-partition seed and build numpy inputs per zone
    zone_args: list[
        tuple[pl.Series, np.ndarray, np.ndarray, np.ndarray, np.ndarray, np.ndarray, int, int, str]
    ] = []

    for geo_id in control_totals.geo_ids:
        zone_seed = seed.filter(pl.col(geo_col).cast(pl.Utf8) == geo_id)
        if len(zone_seed) == 0:
            logger.warning("Zone %s: no seed records, skipping", geo_id)
            continue

        zone_totals = control_totals.totals.filter(pl.col("geo_id") == geo_id)
        incidence, ctrl_targets, master_idx = _build_incidence(
            zone_seed,
            zone_totals,
            targets,
        )

        n = len(zone_seed)
        initial = np.ones(n, dtype=np.float64)
        lb, ub = _bounds(
            initial,
            ctrl_targets,
            master_idx,
            min_expansion_factor,
            max_expansion_factor,
            min_weight,
            max_weight,
        )

        zone_args.append(
            (
                zone_seed["hh_id"],
                incidence,
                initial,
                lb,
                ub,
                ctrl_targets,
                master_idx,
                max_iterations,
                geo_id,
            )
        )

    # Run balancer - numba @njit releases the GIL so threads parallelize
    if n_workers > 1 and len(zone_args) > 1:
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            results = list(pool.map(lambda a: _balance_zone(*a), zone_args))
    else:
        results = [_balance_zone(*a) for a in zone_args]

    # Assemble output
    weight_frames = [r[0] for r in results]
    statuses = [r[1] for r in results]

    weights = (
        pl.concat(weight_frames)
        if weight_frames
        else pl.DataFrame(schema={"hh_id": pl.Int64, "hh_weight": pl.Float64, "geo_id": pl.Utf8})
    )

    n_fail = sum(not s.converged for s in statuses)
    logger.info("Balancing: %d zones, %d failed", len(statuses), n_fail)
    return weights, statuses


# -- Internals -------------------------------------------------------------


def _balance_zone(
    hh_ids: pl.Series,
    incidence: np.ndarray,
    initial: np.ndarray,
    lb: np.ndarray,
    ub: np.ndarray,
    ctrl_targets: np.ndarray,
    master_idx: int,
    max_iterations: int,
    geo_id: str,
) -> tuple[pl.DataFrame, ZoneStatus]:
    """Run ``np_balancer_numba`` for a single zone and return results."""
    n = len(initial)

    w_final, _relax, status = np_balancer_numba(
        sample_count=n,
        control_count=incidence.shape[0],
        master_control_index=master_idx,
        incidence=incidence,
        weights_initial=initial,
        weights_lower_bound=lb,
        weights_upper_bound=ub,
        controls_constraint=ctrl_targets,
        controls_importance=np.ones(incidence.shape[0], dtype=np.float64),
        max_iterations=max_iterations,
    )

    converged, iters, delta, gamma = status
    zs = ZoneStatus(geo_id, bool(converged), int(iters), float(delta), float(gamma))

    level = logging.INFO if converged else logging.WARNING
    logger.log(
        level,
        "Zone %s: %s in %d iters (delta=%.2e)",
        geo_id,
        "converged" if converged else "NOT CONVERGED",
        iters,
        delta,
    )

    frame = pl.DataFrame(
        {
            "hh_id": hh_ids,
            "hh_weight": w_final,
            "geo_id": [geo_id] * n,
        }
    )
    return frame, zs


def _build_incidence(
    zone_seed: pl.DataFrame,
    zone_totals: pl.DataFrame,
    targets: list[str],
) -> tuple[np.ndarray, np.ndarray, int]:
    """Build incidence matrix + target vector for one zone.

    Returns ``(incidence, targets, master_control_index)``.
    ``incidence`` has shape ``(n_controls, n_households)``.
    """
    n = len(zone_seed)
    rows: list[np.ndarray] = []
    tgt: list[float] = []

    # Row 0: total-HH control (incidence = 1 for every household)
    master_idx = 0
    rows.append(np.ones(n, dtype=np.float64))
    hh_names = [t for t in targets if CONTROLS[t].level == ControlLevel.HOUSEHOLD]
    if hh_names:
        total_hh = float(
            zone_totals.filter(pl.col("control_name") == hh_names[0])["target_total"].sum()
        )
    else:
        total_hh = float(n)
    tgt.append(total_hh)

    # Remaining rows: one per control x category
    for name in targets:
        ctrl = CONTROLS[name]
        ctrl_rows = zone_totals.filter(pl.col("control_name") == name)
        if len(ctrl_rows) == 0:
            continue

        for cat_val, target_val in zip(
            ctrl_rows["category"].to_list(),
            ctrl_rows["target_total"].to_list(),
            strict=True,
        ):
            if ctrl.level == ControlLevel.HOUSEHOLD:
                col = zone_seed[f"ctrl_{name}"].to_numpy()
                rows.append((col == cat_val).astype(np.float64))
            else:
                inc_col = f"inc_{name}_{_member_name(ctrl, cat_val)}"
                if inc_col in zone_seed.columns:
                    rows.append(zone_seed[inc_col].to_numpy().astype(np.float64))
                else:
                    rows.append(np.zeros(n, dtype=np.float64))
            tgt.append(float(target_val))

    return np.vstack(rows), np.array(tgt, dtype=np.float64), master_idx


def _member_name(ctrl: ControlTarget, cat_val: int) -> str:
    """Map a category int to its lowercase enum member name."""
    for value, name in ctrl.valid_members:
        if value == cat_val:
            return name.lower()
    msg = f"Category {cat_val} not in {ctrl.name}.valid_members"
    raise ValueError(msg)


def _bounds(
    initial: np.ndarray,
    targets: np.ndarray,
    master_idx: int,
    min_factor: float,
    max_factor: float,
    min_weight: float | None = None,
    max_weight: float | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    """Weight bounds scaled by target/sample ratio, then clipped to absolutes."""
    total = initial.sum()
    ratio = float(targets[master_idx]) / total if master_idx >= 0 and total > 0 else 1.0
    lb = np.maximum(initial * min_factor * ratio, 0.0)
    ub = np.maximum(initial * max_factor * ratio, 1.0)
    if min_weight is not None:
        lb = np.maximum(lb, min_weight)
    if max_weight is not None:
        ub = np.minimum(ub, max_weight)
    return lb, ub
