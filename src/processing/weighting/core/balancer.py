"""Maximum-entropy list balancer.

Thin Polars->numpy bridge around PopulationSim's ``np_balancer_numba``.
Runs independently per geography zone.
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import NamedTuple

import numpy as np
import polars as pl
from populationsim.balancing.balancers_numba import np_balancer_numba

from processing.weighting.core.control_data import ControlTotals
from processing.weighting.core.controls import CONTROLS, ControlLevel, ControlTarget

logger = logging.getLogger(__name__)


# -- Data structures --------------------------------------------------------


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


# -- Public API ------------------------------------------------------------


def balance_weights(
    seed: pl.DataFrame,
    control_totals: ControlTotals,
    targets: list[str],
    *,
    merges: list[MergeSpec] | None = None,
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
        HH category columns (e.g. ``h_size``) and person incidence
        columns (e.g. ``p_gender_male``).
    control_totals : ControlTotals
        PUMS-derived targets from ``build_control_totals``.
    targets : list[str]
        Control registry names.
    merges : list[MergeSpec] | None
        Optional category merges applied at the matrix level before
        balancing.  Each spec collapses incidence rows + target entries
        for the specified categories.  Zone-specific merges are supported
        via ``MergeSpec.zones``.
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
    merges = merges or []

    # Build per-zone inputs
    zone_inputs: list[ZoneInput] = []
    for geo_id in control_totals.geo_ids:
        zone_seed = seed.filter(pl.col(geo_col).cast(pl.Utf8) == geo_id)
        if len(zone_seed) == 0:
            logger.warning("Zone %s: no seed records, skipping", geo_id)
            continue

        zone_totals = control_totals.totals.filter(pl.col("geo_id") == geo_id)
        zone_inputs.append(
            _prepare_zone(
                zone_seed,
                zone_totals,
                targets,
                merges,
                geo_id,
                min_expansion_factor=min_expansion_factor,
                max_expansion_factor=max_expansion_factor,
                min_weight=min_weight,
                max_weight=max_weight,
                max_iterations=max_iterations,
            )
        )

    # Run balancer — numba @njit releases the GIL so threads parallelize
    if n_workers > 1 and len(zone_inputs) > 1:
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            results = list(pool.map(_balance_zone, zone_inputs))
    else:
        results = [_balance_zone(z) for z in zone_inputs]

    # Assemble output
    weight_frames = [r.weights for r in results]
    statuses = [r.status for r in results]

    weights = (
        pl.concat(weight_frames)
        if weight_frames
        else pl.DataFrame(schema={"hh_id": pl.Int64, "hh_weight": pl.Float64, "geo_id": pl.Utf8})
    )

    n_fail = sum(not s.converged for s in statuses)
    logger.info("Balancing: %d zones, %d failed", len(statuses), n_fail)
    return weights, statuses


# -- Zone preparation -------------------------------------------------------


def _prepare_zone(
    zone_seed: pl.DataFrame,
    zone_totals: pl.DataFrame,
    targets: list[str],
    merges: list[MergeSpec],
    geo_id: str,
    *,
    min_expansion_factor: float,
    max_expansion_factor: float,
    min_weight: float | None,
    max_weight: float | None,
    max_iterations: int,
) -> ZoneInput:
    """Build ``ZoneInput`` arrays for a single geography zone."""
    incidence, ctrl_targets, master_idx, row_labels = _build_incidence(
        zone_seed,
        zone_totals,
        targets,
    )

    zone_merges = [m for m in merges if m.zones is None or geo_id in m.zones]
    if zone_merges:
        incidence, ctrl_targets, master_idx = _apply_merges(
            incidence,
            ctrl_targets,
            row_labels,
            master_idx,
            zone_merges,
        )

    if "base_weight" not in zone_seed.columns:
        msg = (
            "Seed table missing 'base_weight' column. "
            "Run compute_base_weights() before balance_weights()."
        )
        raise ValueError(msg)
    initial = zone_seed["base_weight"].to_numpy().astype(np.float64)
    lb, ub = _bounds(
        initial,
        ctrl_targets,
        master_idx,
        min_expansion_factor,
        max_expansion_factor,
        min_weight,
        max_weight,
    )

    return ZoneInput(
        hh_ids=zone_seed["hh_id"],
        incidence=incidence,
        initial=initial,
        lb=lb,
        ub=ub,
        targets=ctrl_targets,
        master_idx=master_idx,
        max_iterations=max_iterations,
        geo_id=geo_id,
    )


# -- Balancing --------------------------------------------------------------


def _balance_zone(zone: ZoneInput) -> ZoneResult:
    """Run ``np_balancer_numba`` for a single zone and return results."""
    n = len(zone.initial)

    w_final, _relax, status = np_balancer_numba(
        sample_count=n,
        control_count=zone.incidence.shape[0],
        master_control_index=zone.master_idx,
        incidence=zone.incidence,
        weights_initial=zone.initial,
        weights_lower_bound=zone.lb,
        weights_upper_bound=zone.ub,
        controls_constraint=zone.targets,
        controls_importance=np.ones(zone.incidence.shape[0], dtype=np.float64),
        max_iterations=zone.max_iterations,
    )

    converged, iters, delta, gamma = status
    zs = ZoneStatus(zone.geo_id, bool(converged), int(iters), float(delta), float(gamma))

    level = logging.INFO if converged else logging.WARNING
    logger.log(
        level,
        "Zone %s: %s in %d iters (delta=%.2e)",
        zone.geo_id,
        "converged" if converged else "NOT CONVERGED",
        iters,
        delta,
    )

    frame = pl.DataFrame(
        {
            "hh_id": zone.hh_ids,
            "hh_weight": w_final,
            "geo_id": [zone.geo_id] * n,
        }
    )
    return ZoneResult(frame, zs)


# -- Incidence matrix -------------------------------------------------------


def _build_incidence(
    zone_seed: pl.DataFrame,
    zone_totals: pl.DataFrame,
    targets: list[str],
) -> tuple[np.ndarray, np.ndarray, int, list[tuple[str, str]]]:
    """Build incidence matrix + target vector for one zone.

    Returns:
    -------
    incidence : np.ndarray
        Shape ``(n_controls, n_households)``.
    targets : np.ndarray
        Target totals, one per control row.
    master_control_index : int
        Row index of the total-HH constraint.
    row_labels : list[tuple[str, str]]
        ``(control_name, member_name)`` for each row.  The master row
        is labelled ``("_total_hh", "_total_hh")``.
    """
    n = len(zone_seed)
    rows: list[np.ndarray] = []
    tgt: list[float] = []
    labels: list[tuple[str, str]] = []

    # Row 0: total-HH control (incidence = 1 for every household)
    master_idx = 0
    rows.append(np.ones(n, dtype=np.float64))
    labels.append(("_total_hh", "_total_hh"))
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
            member = _member_name(ctrl, cat_val)
            if ctrl.level == ControlLevel.HOUSEHOLD:
                col = zone_seed[name].to_numpy()
                rows.append((col == cat_val).astype(np.float64))
            else:
                col_name = f"{name}__{member}"
                if col_name in zone_seed.columns:
                    rows.append(zone_seed[col_name].to_numpy().astype(np.float64))
                else:
                    rows.append(np.zeros(n, dtype=np.float64))
            labels.append((name, member))
            tgt.append(float(target_val))

    return np.vstack(rows), np.array(tgt, dtype=np.float64), master_idx, labels


def _member_name(ctrl: ControlTarget, cat_val: int) -> str:
    """Map a category int to its lowercase enum member name."""
    for value, name in ctrl.valid_members:
        if value == cat_val:
            return name.lower()
    msg = f"Category {cat_val} not in {ctrl.name}.valid_members"
    raise ValueError(msg)


# -- Merge helpers -----------------------------------------------------------


def _apply_merges(
    incidence: np.ndarray,
    targets: np.ndarray,
    row_labels: list[tuple[str, str]],
    master_idx: int,
    merges: list[MergeSpec],
) -> tuple[np.ndarray, np.ndarray, int]:
    """Collapse incidence rows + target entries for merged categories.

    For each ``MergeSpec``, every group maps several base member names to
    a single merged row.  The incidence rows are summed element-wise and
    the target values are summed.  Original rows are replaced by the
    single merged row.

    Parameters
    ----------
    incidence : np.ndarray
        Shape ``(n_controls, n_households)``.
    targets : np.ndarray
        One entry per row.
    row_labels : list[tuple[str, str]]
        ``(control_name, member_name)`` per row.  **Modified in place**
        to reflect the merged labels.
    master_idx : int
        Row index of the total-HH master control.
    merges : list[MergeSpec]
        Merge specifications to apply.

    Returns:
    -------
    incidence, targets, master_idx
        Reduced arrays and (possibly shifted) master index.
    """
    for spec in merges:
        for merged_label, base_members in spec.groups.items():
            # Find row indices matching this control + base members
            idxs = [
                i
                for i, (ctrl, member) in enumerate(row_labels)
                if ctrl == spec.control and member in base_members
            ]
            if len(idxs) < 2:  # noqa: PLR2004
                continue  # nothing to merge (0 or 1 match)

            # Sum incidence rows and target entries
            merged_row = incidence[idxs].sum(axis=0)
            merged_target = targets[idxs].sum()

            # Keep the first index, mark rest for removal
            keep = idxs[0]
            remove = set(idxs[1:])

            incidence[keep] = merged_row
            targets[keep] = merged_target
            row_labels[keep] = (spec.control, merged_label)

            # Remove collapsed rows (reverse order to preserve indices)
            keep_mask = [i not in remove for i in range(len(row_labels))]
            incidence = incidence[keep_mask]
            targets = targets[keep_mask]
            row_labels[:] = [lbl for i, lbl in enumerate(row_labels) if i not in remove]

            # Recompute master_idx after removal
            master_idx = next(i for i, (c, _) in enumerate(row_labels) if c == "_total_hh")

    return incidence, targets, master_idx


# -- Bounds ------------------------------------------------------------------


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
