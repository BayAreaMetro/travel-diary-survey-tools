"""Dwell-time and recurrence distributions for the location-registry gate (#71).

The person-location registry promotes an observed destination to a habitual
worksite/campus only when a respondent both *stays* there (dwell filters brief
gig/delivery stops) and *returns* there (recurrence filters one-off meetings).
This script reports both distributions so those two cutoffs
(``RegistryGateConfig.min_dwell_minutes`` / ``min_distinct_days``) are chosen
from the data rather than guessed.

Run against canonical pipeline output::

    uv run python scripts/dwell_gate_analysis.py \
        --linked-trips /path/to/linked_trips.parquet \
        --persons      /path/to/persons.parquet \
        [--cluster-decimals 3]

Dwell source: ``d_activity_duration`` (linked-trip field, #68) when present,
excluding its sentinels (-1 = destination is home, -2 = last trip of the
person-day). On older output without that column, dwell is reconstructed as the
next departure minus this arrival within a person-day.

Reported, per relevant destination purpose and split by whether the person has a
*reported* fixed location of that kind (proxy for "has a primary"):
  - dwell percentiles and a text histogram (look for the short-stay noise spike)
  - retention at candidate dwell gates
  - a recurrence table: distinct person-locations surviving each
    (min_distinct_days x min_dwell) combination
"""

import argparse
from pathlib import Path

import polars as pl

# PurposeCategory codes (data_canon.codebook.trips.PurposeCategory)
WORK = 2
WORK_RELATED = 3
SCHOOL = 4
SCHOOL_RELATED = 5

# (label, destination purpose codes, person column proving a reported location)
CLASSES = [
    ("WORK", [WORK], "work_lat"),
    ("WORK_RELATED", [WORK_RELATED], "work_lat"),
    ("SCHOOL", [SCHOOL], "school_lat"),
    ("SCHOOL_RELATED", [SCHOOL_RELATED], "school_lat"),
]

SENTINELS = (-1, -2)
PERCENTILES = [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]
CANDIDATE_GATES_MIN = [10, 15, 20, 30, 45, 60, 90, 120, 180]
RECURRENCE_DAYS = [1, 2, 3]
RECURRENCE_DWELL = [15, 30, 45]


def _load(path: str) -> pl.DataFrame:
    p = Path(path)
    if p.suffix == ".parquet":
        return pl.read_parquet(p)
    return pl.read_csv(p, infer_schema_length=10000)


def _dwell_column(linked_trips: pl.DataFrame) -> pl.DataFrame:
    """Return linked_trips with a ``dwell_min`` column of real dwell minutes."""
    if "d_activity_duration" in linked_trips.columns:
        return linked_trips.filter(
            pl.col("d_activity_duration").is_not_null()
            & ~pl.col("d_activity_duration").is_in(SENTINELS)
        ).with_columns(pl.col("d_activity_duration").cast(pl.Float64).alias("dwell_min"))

    # Fallback for pre-#68 output: dwell = next depart - this arrive, per day.
    if not {"depart_time", "arrive_time", "day_id"}.issubset(linked_trips.columns):
        msg = "Need d_activity_duration, or depart_time/arrive_time/day_id to reconstruct dwell."
        raise SystemExit(msg)
    lt = linked_trips.sort(["person_id", "day_id", "arrive_time"])
    lt = lt.with_columns(
        (pl.col("depart_time").shift(-1).over(["person_id", "day_id"]) - pl.col("arrive_time"))
        .dt.total_minutes()
        .alias("dwell_min")
    )
    return lt.filter(pl.col("dwell_min").is_not_null() & (pl.col("dwell_min") >= 0))


def _text_histogram(minutes: pl.Series, bins: int = 20, width: int = 46) -> str:
    vals = minutes.drop_nulls()
    if vals.is_empty():
        return "  (no data)"
    hi = max(float(vals.quantile(0.97) or vals.max() or 1.0), 1.0)
    step = hi / bins
    counts = []
    for i in range(bins):
        a, b = i * step, (i + 1) * step
        upper = pl.col("m") <= b if i == bins - 1 else pl.col("m") < b
        counts.append((a, b, vals.to_frame("m").filter((pl.col("m") >= a) & upper).height))
    peak = max((c for _, _, c in counts), default=1) or 1
    return "\n".join(
        f"  {a:6.0f}-{b:<6.0f} | {'#' * round(width * c / peak)} {c}" for a, b, c in counts
    )


def _report_class(sub: pl.DataFrame, reported_col: str) -> None:
    for label, has_reported in [
        ("person HAS a reported fixed location (primary likely)", True),
        ("person has NO reported fixed location (no primary)", False),
    ]:
        cond = (
            pl.col(reported_col).is_not_null() if has_reported else pl.col(reported_col).is_null()
        )
        grp = sub.filter(cond)
        print(f"\n-- {label}: {grp.height} trip ends")
        if grp.is_empty():
            continue
        d = grp["dwell_min"]
        pcts = " ".join(f"p{int(p * 100)}={d.quantile(p):.0f}" for p in PERCENTILES)
        print(f"   dwell(min): min={d.min():.0f} {pcts} max={d.max():.0f} mean={d.mean():.0f}")
        print(_text_histogram(d))
        print("   retention by candidate dwell gate:")
        for g in CANDIDATE_GATES_MIN:
            kept = grp.filter(pl.col("dwell_min") >= g).height
            print(f"     >= {g:3d} min: {kept:6d} ({100 * kept / grp.height:5.1f}%)")


def _report_recurrence(sub: pl.DataFrame, cluster_decimals: int) -> None:
    """Distinct person-locations surviving each (min_days x min_dwell) gate."""
    clustered = (
        sub.with_columns(
            pl.col("d_lat").round(cluster_decimals).alias("_clat"),
            pl.col("d_lon").round(cluster_decimals).alias("_clon"),
        )
        .group_by(["person_id", "_clat", "_clon"])
        .agg(
            pl.col("day_id").n_unique().alias("distinct_days"),
            pl.col("dwell_min").max().alias("max_dwell"),
        )
    )
    print(
        f"\n   recurrence (cluster coords to {cluster_decimals}dp, "
        f"{clustered.height} distinct person-locations):"
    )
    for days in RECURRENCE_DAYS:
        for dwell in RECURRENCE_DWELL:
            kept = clustered.filter(
                (pl.col("distinct_days") >= days) & (pl.col("max_dwell") >= dwell)
            ).height
            print(f"     >= {days} day(s) AND max_dwell >= {dwell:3d} min: {kept:6d}")


def analyse(linked_trips: pl.DataFrame, persons: pl.DataFrame, cluster_decimals: int) -> None:
    """Print dwell and recurrence distributions for each destination purpose class."""
    lt = _dwell_column(linked_trips).join(
        persons.select(["person_id", "work_lat", "school_lat"]),
        on="person_id",
        how="left",
    )
    print(f"Total real dwell observations: {lt.height}")
    for name, purpose_codes, reported_col in CLASSES:
        sub = lt.filter(pl.col("d_purpose_category").is_in(purpose_codes))
        print("\n" + "=" * 72)
        print(f"{name}  (d_purpose_category in {purpose_codes})   n={sub.height}")
        print("=" * 72)
        if sub.is_empty():
            print("  no qualifying trip ends")
            continue
        _report_class(sub, reported_col)
        if name in ("WORK_RELATED", "SCHOOL_RELATED") and {"d_lat", "d_lon"}.issubset(sub.columns):
            _report_recurrence(sub, cluster_decimals)


def main() -> None:
    """Parse CLI arguments and run the dwell/recurrence analysis."""
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--linked-trips", required=True)
    ap.add_argument("--persons", required=True)
    ap.add_argument("--cluster-decimals", type=int, default=3)
    args = ap.parse_args()
    analyse(_load(args.linked_trips), _load(args.persons), args.cluster_decimals)


if __name__ == "__main__":
    main()
