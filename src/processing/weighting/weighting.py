"""Top-level weighting pipeline step.

Orchestrates the full weighting pipeline as a single ``@step`` entry point:

1. Load/fetch PUMS microdata
2. Recode PUMS -> control categories
3. Build weighted control totals by geography
4. Recode survey -> same control categories
5. Build household seed table with person incidence
6. Run maximum-entropy balancer per zone
7. Join ``hh_weight`` to households; propagate to all downstream tables
"""

import logging

import polars as pl

from pipeline.decoration import step
from processing.weighting.core.balancer import balance_weights
from processing.weighting.core.control_data import (
    ControlSpec,
    build_control_totals,
    recode_pums_households,
    recode_pums_persons,
)
from processing.weighting.core.pums_data import load_pums_from_files
from processing.weighting.core.seed_data import (
    build_seed_table,
    recode_survey_households,
    recode_survey_persons,
)
from processing.weighting.core.weight_propagation import (
    collect_tables,
    non_null_tables,
    propagate_weights,
    safe_join_weight,
)

logger = logging.getLogger(__name__)


@step()
def weighting(  # noqa: PLR0913
    # -- Config params (from YAML) --------------------------------------
    pums_households: str,
    pums_persons: str,
    controls: list[dict],
    *,
    geo_col: str = "PUMA",
    survey_geo_col: str = "puma_id",
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
    pums_households, pums_persons : str
        Paths to PUMS household/person CSV or Parquet files.
    controls : list[dict]
        Control specifications, each with ``name`` and optional ``aggregations``.
        Names must match keys in the ``CONTROLS`` registry.
    geo_col : str
        Geography column in PUMS data (default ``"PUMA"``).
    survey_geo_col : str
        Geography column in survey households (default ``"puma_id"``).
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

    # -- Parse control specs ----------------------------------------
    specs = [ControlSpec(**c) for c in controls]
    target_names = [s.name for s in specs]
    logger.info("Controls: %s", target_names)

    # -- 1. Load PUMS -----------------------------------------------
    pums_hh, pums_per = load_pums_from_files(pums_households, pums_persons)

    # -- 2. Recode PUMS ---------------------------------------------
    pums_hh = recode_pums_households(pums_hh, pums_per, target_names)
    pums_per = recode_pums_persons(pums_per, target_names)

    # -- 3. Build control totals ------------------------------------
    control_totals = build_control_totals(pums_hh, pums_per, specs, geo_col=geo_col)
    logger.info(
        "Control totals: %d zones, %d PUMS HHs, %d PUMS persons",
        len(control_totals.geo_ids),
        control_totals.pums_hh_count,
        control_totals.pums_person_count,
    )

    # -- 4. Recode survey -------------------------------------------
    hh_recoded = recode_survey_households(households, persons, target_names)
    per_recoded = recode_survey_persons(persons, target_names)

    # -- 5. Build seed table ----------------------------------------
    seed = build_seed_table(hh_recoded, per_recoded, target_names, geo_col=survey_geo_col)

    # -- 6. Balance -------------------------------------------------
    weights_df, statuses = balance_weights(
        seed,
        control_totals,
        target_names,
        geo_col=survey_geo_col,
        max_expansion_factor=max_expansion_factor,
        min_expansion_factor=min_expansion_factor,
        min_weight=min_weight,
        max_weight=max_weight,
        max_iterations=max_iterations,
        n_workers=n_workers,
    )

    n_failed = sum(not s.converged for s in statuses)
    if n_failed:
        logger.warning("%d of %d zones did not converge", n_failed, len(statuses))

    # -- 7. Attach & propagate weights ------------------------------
    households = safe_join_weight(
        households,
        weights_df.select("hh_id", "hh_weight"),
        "hh_id",
    )
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

    return non_null_tables(tables)
