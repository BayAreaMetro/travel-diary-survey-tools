"""Initial (base) expansion weights for the balancer.

Before balancing, each survey household needs a
starting weight that reflects its basic expansion factor — the number of
real-world households it represents.  Without meaningful initial weights,
the Newton-Raphson solver starts from ``1.0`` and must bridge the gap to
expansion factors of hundreds or thousands, quickly slamming into the
expansion-factor constraints.

This module provides two paths:

1. **Response inversion** (default) — ``target_hh_pop / n_responses``
   per zone.  Works whenever PUMS-derived control totals are available
   (always, in our pipeline).

2. **Sample plan** — a ``SamplePlan`` object mapping zones to sampling
   strata.  Zone-level populations are sourced from the crosswalk
   (Census block population), not from the sample plan CSV itself.
   Each household is attributed to its stratum and receives a
   stratum-specific initial weight.

The public entry point is ``compute_base_weights``, which adds a
``base_weight`` column to the seed table.
"""

import logging
from pathlib import Path

import polars as pl

from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import CONTROLS
from processing.weighting.specs import ControlTotals, SamplePlan

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# I/O
# ---------------------------------------------------------------------------
def load_sample_plan(path: str | Path) -> SamplePlan:
    """Read a sample-plan CSV into a :class:`SamplePlan`.

    Parameters
    ----------
    path : str | Path
        Path to a CSV file.  Must contain at minimum: ``geo_id`` and
        ``sample_segment``.  Population totals are sourced from the
        crosswalk, not from the CSV.

    Returns:
    -------
    SamplePlan

    Raises:
    ------
    FileNotFoundError
        If *path* does not exist.
    ValueError
        If required columns are missing (raised by ``SamplePlan``).
    """
    p = Path(path)
    if not p.exists():
        msg = f"Sample plan file not found: {p}"
        raise FileNotFoundError(msg)
    df = pl.read_csv(p)
    logger.info("Loaded sample plan from %s (%d rows)", p, len(df))
    return SamplePlan(strata=df)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def compute_base_weights(
    seed: pl.DataFrame,
    control_totals: ControlTotals,
    targets: list[str],
    geo_col: str = "ctrl_geoid",
    *,
    sample_plan: str | Path | SamplePlan | None = None,
    zone_populations: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Add ``base_weight`` column to the seed table.

    Parameters
    ----------
    seed : pl.DataFrame
        Household seed table from ``build_seed_table``.  Must contain
        ``hh_id`` and *geo_col*.
    control_totals : ControlTotals
        PUMS-derived targets from ``build_control_totals``.
    targets : list[str]
        Control registry names (used to identify the master HH control).
    geo_col : str
        Geography column on *seed*.
    sample_plan : str | Path | SamplePlan | None
        Optional sample plan.  Accepts a file path (loaded via
        :func:`load_sample_plan`) or an already-loaded
        :class:`SamplePlan`.  When ``None``, default response
        inversion is used.
    zone_populations : pl.DataFrame | None
        Census-derived population totals per zone with columns
        ``[geo_id, target_population]``.  Required when *sample_plan*
        is provided; sourced from :attr:`PumaCrosswalk.zone_populations`.

    Returns:
    -------
    pl.DataFrame
        *seed* with an additional ``base_weight`` column (Float64).

    Raises:
    ------
    ValueError
        If no household-level control is found in *targets*, or if a zone
        has zero survey responses (should never happen if the seed is
        correctly filtered).
    """
    if sample_plan is not None:
        plan = sample_plan if isinstance(sample_plan, SamplePlan) else load_sample_plan(sample_plan)
        if zone_populations is None:
            msg = (
                "zone_populations is required when using a sample plan. "
                "Pass PumaCrosswalk.zone_populations from the crosswalk step."
            )
            raise ValueError(msg)
        zone_weights = _zone_weights_from_plan(seed, plan, geo_col, zone_populations)
    else:
        logger.info("Using PUMS-target response inversion for base weights")
        zone_weights = _zone_weights_from_response_inversion(seed, control_totals, targets, geo_col)

    return seed.join(
        zone_weights,
        left_on=pl.col(geo_col).cast(pl.Utf8),
        right_on="geo_id",
        how="left",
    )


# ---------------------------------------------------------------------------
# Strategy: response inversion (default)
# ---------------------------------------------------------------------------
def _zone_weights_from_response_inversion(
    seed: pl.DataFrame,
    control_totals: ControlTotals,
    targets: list[str],
    geo_col: str,
) -> pl.DataFrame:
    """Return ``(geo_id, base_weight)`` via zone_target_hh_pop / zone_n_responses.

    Uses the first household-level control to derive the total-HH
    population target per zone — the same logic that ``_build_incidence``
    uses for the total control row.
    Total control var is just used as a reference point to get the zone-level population targets;
    the actual base weights are derived from the total HH population per zone,
    not the category-level targets.
    """
    hh_names = [t for t in targets if CONTROLS[t].level == ControlLevel.HOUSEHOLD]
    if not hh_names:
        msg = (
            f"Cannot compute base weights: no household-level control found in targets={targets!r}"
        )
        raise ValueError(msg)

    ref_var_name = hh_names[0]

    # Total HH population target per zone (sum across categories)
    zone_targets = (
        control_totals.totals.filter(pl.col("control_name") == ref_var_name)
        .group_by("geo_id")
        .agg(pl.col("target_total").sum().alias("_zone_target_hh"))
    )

    # Count survey responses per zone
    zone_counts = (
        seed.group_by(geo_col)
        .agg(pl.len().alias("_zone_n_responses"))
        .rename({geo_col: "geo_id"})
        .with_columns(pl.col("geo_id").cast(pl.Utf8))
    )

    # Join and derive base weight
    zone_weights = zone_targets.join(zone_counts, on="geo_id", how="inner")

    zeros = zone_weights.filter(pl.col("_zone_n_responses") == 0)
    if len(zeros) > 0:
        bad = zeros["geo_id"].to_list()
        msg = f"Zones with zero survey responses: {bad}"
        raise ValueError(msg)

    zone_weights = zone_weights.with_columns(
        (pl.col("_zone_target_hh") / pl.col("_zone_n_responses")).alias("base_weight")
    )

    # Per-zone and region-level diagnostics
    region_target = zone_weights["_zone_target_hh"].sum()
    region_responses = zone_weights["_zone_n_responses"].sum()
    region_weighted = (zone_weights["base_weight"] * zone_weights["_zone_n_responses"]).sum()

    zw = zone_weights.sort("geo_id").with_columns(
        (pl.col("base_weight") * pl.col("_zone_n_responses")).alias("_init_total")
    )
    zw_wide = pl.concat(
        [
            zw.select(
                "geo_id", "_zone_target_hh", "_zone_n_responses", "base_weight", "_init_total"
            ),
            pl.DataFrame(
                {
                    "geo_id": ["REGION"],
                    "_zone_target_hh": [region_target],
                    "_zone_n_responses": [region_responses],
                    "base_weight": [None],
                    "_init_total": [region_weighted],
                }
            ),
        ],
        how="diagonal_relaxed",
    ).rename(
        {
            "geo_id": "Zone",
            "_zone_target_hh": "Target HH",
            "_zone_n_responses": "Responses",
            "base_weight": "Base Wt",
            "_init_total": "Init Total",
        }
    )
    logger.info(
        "Base weights (response inversion, reference=%s):\n%s",
        ref_var_name,
        zw_wide,
    )

    zone_weights = zone_weights.select("geo_id", "base_weight")

    return zone_weights


# ---------------------------------------------------------------------------
# Strategy: explicit sample plan (future)
# ---------------------------------------------------------------------------
def _zone_weights_from_plan(
    seed: pl.DataFrame,
    plan: SamplePlan,
    geo_col: str,
    zone_populations: pl.DataFrame,
) -> pl.DataFrame:
    """Return ``(geo_id, base_weight)`` via segment_target_pop / segment_n_responses.

    Population totals per zone come from the crosswalk (Census blocks),
    not from the sample plan CSV.  The plan only needs ``geo_id`` and
    ``sample_segment``.

    1. Attach ``target_population`` from *zone_populations* to the plan.
    2. Map each zone → sample_segment via the plan.
    3. Sum ``target_population`` per segment.
    4. Count actual survey responses per segment from the seed.
    5. ``base_weight = segment_pop / segment_responses``.
    6. Collapse back to ``(geo_id, base_weight)``.
    """
    # Zone → segment lookup
    zone_segment = plan.strata.select(
        pl.col("geo_id").cast(pl.Utf8),
        pl.col("sample_segment").cast(pl.Utf8),
    )

    # Attach crosswalk-derived population to each zone
    zone_with_pop = zone_segment.join(
        zone_populations.with_columns(pl.col("geo_id").cast(pl.Utf8)),
        on="geo_id",
        how="left",
    )
    missing_pop = zone_with_pop.filter(pl.col("target_population").is_null())
    if len(missing_pop) > 0:
        bad = missing_pop["geo_id"].to_list()
        msg = f"Zones in sample plan have no crosswalk population: {bad}"
        raise ValueError(msg)

    # Segment-level target population (sum zone pops within each segment)
    seg_pop = (
        zone_with_pop.group_by("sample_segment")
        .agg(pl.col("target_population").sum().alias("_seg_target_pop"))
        .with_columns(pl.col("sample_segment").cast(pl.Utf8))
    )

    # Count actual survey responses per segment
    seed_with_seg = seed.join(
        zone_segment,
        left_on=pl.col(geo_col).cast(pl.Utf8),
        right_on="geo_id",
        how="left",
    )
    seg_counts = seed_with_seg.group_by("sample_segment").agg(pl.len().alias("_seg_n_responses"))

    # Derive weight per segment
    seg_weights = seg_pop.join(seg_counts, on="sample_segment", how="left")

    zeros = seg_weights.filter(
        pl.col("_seg_n_responses").is_null() | (pl.col("_seg_n_responses") == 0)
    )
    if len(zeros) > 0:
        bad = zeros["sample_segment"].to_list()
        msg = f"Sample plan segments with zero survey responses: {bad}"
        raise ValueError(msg)

    seg_weights = seg_weights.with_columns(
        (pl.col("_seg_target_pop") / pl.col("_seg_n_responses")).alias("base_weight")
    ).select("sample_segment", "base_weight")

    logger.info(
        "Base weights (sample plan): min=%.1f, mean=%.1f, median=%.1f, max=%.1f across %d segments",
        seg_weights["base_weight"].min(),
        seg_weights["base_weight"].mean(),
        seg_weights["base_weight"].median(),
        seg_weights["base_weight"].max(),
        len(seg_weights),
    )

    # Zone → segment → weight
    return zone_segment.join(seg_weights, on="sample_segment", how="left").select(
        "geo_id", "base_weight"
    )
