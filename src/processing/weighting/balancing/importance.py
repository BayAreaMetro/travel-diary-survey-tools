"""MOE-based importance weight calculation.

Uses PUMS successive-difference replicate weights (WGTP1-80 / PWGTP1-80)
to estimate the Margin of Error (MOE) for each weighted control total,
then converts to importance weights inversely proportional to the
coefficient of variation (CV).

Controls with higher sampling uncertainty (larger CV) receive *lower*
importance so the balancer doesn't chase noisy targets.  Controls absent
from the returned dict (e.g. structural totals whose MOE is meaningless)
fall back to the balancer's default importance.
"""

import logging

import numpy as np
import polars as pl

from processing.weighting.controls.base import ControlLevel, ControlTarget
from processing.weighting.controls.registry import CONTROLS

logger = logging.getLogger(__name__)

DEFAULT_IMPORTANCE = 100.0

_N_REPLICATES = 80


def compute_moe_importance(
    hh_df: pl.DataFrame,
    person_df: pl.DataFrame,
    target_names: list[str],
    *,
    geo_col: str = "ctrl_geoid",
) -> dict[str, float]:
    """Compute per-control importance weights from PUMS replicate-weight MOE.

    Parameters
    ----------
    hh_df : pl.DataFrame
        Crosswalk-allocated PUMS households with ``_xw_WGTP`` and
        ``_xw_WGTP1`` … ``_xw_WGTP80`` columns (plus recoded controls).
    person_df : pl.DataFrame
        Crosswalk-allocated PUMS persons with ``_xw_PWGTP`` and
        ``_xw_PWGTP1`` … ``_xw_PWGTP80`` columns (plus recoded controls).
    target_names : list[str]
        Control registry names to compute importance for.
    geo_col : str
        Geography column (default ``"ctrl_geoid"``).

    Returns:
    -------
    dict[str, float]
        ``{control_name: importance}`` for controls where MOE could be
        computed.  Controls with no PUMS records (data sparsity) are
        omitted -- the balancer will apply its default importance.

    Raises:
    ------
    ValueError
        If a target name is not in the control registry, or if required
        replicate weight / control columns are missing from the data.
    """
    control_cvs: dict[str, float] = {}

    for name in target_names:
        ctrl = CONTROLS.get(name)
        if ctrl is None:
            msg = f"Unknown control {name!r} — not in registry"
            raise ValueError(msg)

        cv = _control_cv(ctrl, hh_df, person_df, geo_col)
        if cv is not None:
            control_cvs[name] = cv

    importance = _normalize_cvs(control_cvs)

    logger.info(
        "MOE importance: %d controls computed, median=%.1f",
        len(control_cvs),
        np.median(list(importance.values())) if importance else 0,
    )
    return importance


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _control_cv(
    ctrl: ControlTarget,
    hh_df: pl.DataFrame,
    person_df: pl.DataFrame,
    geo_col: str,
) -> float | None:
    """Compute a representative CV for one control across all zones.

    Returns the *median CV across cells* (geo x category), or ``None``
    if no PUMS records exist for this control's categories.
    """
    if ctrl.level == ControlLevel.HOUSEHOLD:
        source, wt_col, rep_prefix = hh_df, "_xw_WGTP", "_xw_WGTP"
    else:
        source, wt_col, rep_prefix = person_df, "_xw_PWGTP", "_xw_PWGTP"

    rep_cols = [f"{rep_prefix}{i}" for i in range(1, _N_REPLICATES + 1)]
    missing = [c for c in rep_cols if c not in source.columns]

    if missing:
        msg = f"Replicate weight columns missing for {ctrl.name}: {missing[:3]}..."
        raise ValueError(msg)
    if ctrl.name not in source.columns:
        msg = f"Control column {ctrl.name!r} not found in data"
        raise ValueError(msg)

    valid_values = [v for v, _ in ctrl.valid_members]
    cells = (
        source.filter(pl.col(ctrl.name).is_in(valid_values))
        .group_by([geo_col, ctrl.name])
        .agg([pl.col(wt_col).sum(), *(pl.col(c).sum() for c in rep_cols)])
    )
    if len(cells) == 0:
        return None

    estimates = cells[wt_col].to_numpy()
    rep_matrix = cells.select(rep_cols).to_numpy()  # (n_cells, 80)

    # Successive-difference variance, vectorised across all cells
    diff = rep_matrix - estimates[:, np.newaxis]
    se = np.sqrt((4.0 / _N_REPLICATES) * np.sum(diff**2, axis=1))

    mask = estimates > 0
    if not mask.any():
        return None

    return float(np.median(se[mask] / estimates[mask]))


def _normalize_cvs(control_cvs: dict[str, float]) -> dict[str, float]:
    """Convert per-control CVs to importance weights.

    Lower CV → higher importance.  Normalized so that the median
    importance equals ``DEFAULT_IMPORTANCE``.

    Uses ``importance = 1 / sqrt(CV)`` to dampen extreme ratios.
    """
    if not control_cvs:
        return {}

    raw = {name: 1.0 / np.sqrt(max(cv, 1e-12)) for name, cv in control_cvs.items()}
    median_raw = float(np.median(list(raw.values())))
    scale = DEFAULT_IMPORTANCE / median_raw
    return {name: val * scale for name, val in raw.items()}
