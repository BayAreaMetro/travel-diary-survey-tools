"""Household diary comparison functionality.

NOTE: Time field comparison is excluded due to a known encoding difference:
- Legacy code uses HHMM format (e.g., 1244 for 12:44 PM)
- New pipeline uses minutes since midnight (e.g., 764 for 12:44 PM)
This is a logical error in the legacy code. The new pipeline format is
correct per DaySim specifications.
"""

import logging

import polars as pl

logger = logging.getLogger(__name__)

# Display columns for tours and trips
TOUR_COLS = [
    "hhno",
    "pno",
    "day",
    "tour",
    "pdpurp",
    "tlvorig",
    "tardest",
    "tarorig",
    "mode",
]
TRIP_COLS = [
    "hhno",
    "pno",
    "tour",
    "deptm",
    "dorp",
    "mode",
    "otaz",
    "dtaz",
]

# Columns to skip in comparison (shown for reference but not matched)
# due to known encoding differences
SKIP_COLS = [
    "tlvorig",
    "tardest",
    "tarorig",
    "tldest",  # Tour times
    "deptm",
    "arrtm",  # Trip times
]

# Key columns to match tours (person, tour sequence, purpose, mode)
# NOTE: Tour numbers are per-day in new pipeline, across-survey in legacy.
# This makes direct tour-number comparison impossible. We match on what we can.
TOUR_MATCH_COLS = ["pno", "pdpurp", "mode"]


def _compare_tours(
    leg_tours: pl.DataFrame,
    new_tours: pl.DataFrame,
) -> dict[str, int]:
    """Compare tours between legacy and new data.

    Returns:
        Dictionary with match statistics:
        - total_legacy: Total tours in legacy
        - total_new: Total tours in new
        - matched: Number of tours that match
        - unmatched_legacy: Tours only in legacy
        - unmatched_new: Tours only in new
    """
    total_leg = len(leg_tours)
    total_new = len(new_tours)

    if total_leg == 0 and total_new == 0:
        return {
            "total_legacy": 0,
            "total_new": 0,
            "matched": 0,
            "unmatched_legacy": 0,
            "unmatched_new": 0,
        }

    # Use available columns for matching
    match_cols = [
        c
        for c in TOUR_MATCH_COLS
        if c in leg_tours.columns and c in new_tours.columns
    ]

    if not match_cols or total_leg == 0 or total_new == 0:
        # Can't match, all are unmatched
        return {
            "total_legacy": total_leg,
            "total_new": total_new,
            "matched": 0,
            "unmatched_legacy": total_leg,
            "unmatched_new": total_new,
        }

    # Create composite keys for matching
    leg_keys = leg_tours.select(match_cols)
    new_keys = new_tours.select(match_cols)

    # Find matches using inner join
    matched = leg_keys.join(new_keys, on=match_cols, how="inner")
    num_matched = len(matched)

    return {
        "total_legacy": total_leg,
        "total_new": total_new,
        "matched": num_matched,
        "unmatched_legacy": total_leg - num_matched,
        "unmatched_new": total_new - num_matched,
    }


def _format_df(
    df: pl.DataFrame,
    cols: list[str],
    sort_by: list[str],
    max_rows: int | None = None,
) -> str:
    """Format dataframe for display."""
    if len(df) == 0:
        return ""
    available = [c for c in cols if c in df.columns]
    result = df.select(available).sort(sort_by)
    return str(result.head(max_rows) if max_rows else result)


def _display_tours(
    legacy_hh_tours: pl.DataFrame,
    new_hh_tours: pl.DataFrame,
) -> None:
    """Display tour comparison for a household."""
    output = [
        "\n--- Tours ---",
        "(Note: Time fields shown but not compared due to legacy "
        "encoding error)",
        "",
        f"Legacy Tours (n={len(legacy_hh_tours)}):",
        _format_df(legacy_hh_tours, TOUR_COLS, ["pno", "tour"]),
        "",
        f"New Pipeline Tours (n={len(new_hh_tours)}):",
        _format_df(new_hh_tours, TOUR_COLS, ["pno", "tour"]),
    ]
    logger.info("\n".join(output))


def _display_trips(
    legacy_hh_trips: pl.DataFrame,
    new_hh_trips: pl.DataFrame,
) -> None:
    """Display trip comparison for a household."""
    output = [
        "\n--- Trips ---",
        "(Note: Time fields shown but not compared due to legacy "
        "encoding error)",
        "",
        f"Legacy Trips (n={len(legacy_hh_trips)}):",
        _format_df(legacy_hh_trips, TRIP_COLS, ["pno", "tour", "deptm"], 20),
        "",
        f"New Pipeline Trips (n={len(new_hh_trips)}):",
        _format_df(new_hh_trips, TRIP_COLS, ["pno", "tour", "deptm"], 20),
    ]
    logger.info("\n".join(output))


def display_household_detail(
    hhno: int,
    legacy_data: dict[str, pl.DataFrame],
    new_data: dict[str, pl.DataFrame],
) -> None:
    """Display detailed comparison for a single household."""
    sep = "=" * 80
    legacy_tours = legacy_data["tour"].filter(pl.col("hhno") == hhno)
    new_tours = new_data["tour"].filter(pl.col("hhno") == hhno)
    legacy_trips = legacy_data["trip"].filter(pl.col("hhno") == hhno)
    new_trips = new_data["trip"].filter(pl.col("hhno") == hhno)

    tour_match = _compare_tours(legacy_tours, new_tours)

    logger.info(
        "\n%s\nHOUSEHOLD %s DIARY COMPARISON\n%s\n\n"
        "Legacy: %d tours, %d trips\n"
        "New:    %d tours, %d trips\n"
        "Tour matching: %d matched, %d unmatched legacy, %d unmatched new",
        sep,
        hhno,
        sep,
        len(legacy_tours),
        len(legacy_trips),
        len(new_tours),
        len(new_trips),
        tour_match["matched"],
        tour_match["unmatched_legacy"],
        tour_match["unmatched_new"],
    )

    if len(legacy_tours) or len(new_tours):
        _display_tours(legacy_tours, new_tours)
    if len(legacy_trips) or len(new_trips):
        _display_trips(legacy_trips, new_trips)


def compare_household_diaries(
    hhno_list: list[int],
    legacy_data: dict[str, pl.DataFrame],
    new_data: dict[str, pl.DataFrame],
    sample_pct: float = 100.0,
) -> tuple[list[dict], dict[str, int | float]]:
    """Compare daily diaries for specific households.

    Args:
        hhno_list: List of household IDs to compare
        legacy_data: Dictionary of legacy DataFrames
        new_data: Dictionary of new pipeline DataFrames
        sample_pct: Percentage of households to check (0-100)

    Returns:
        Tuple of (failures, summary_stats) where:
        - failures: List of household mismatch details
        - summary_stats: Dictionary with overall statistics
    """
    # Determine sample size
    sample_size = max(1, int(len(hhno_list) * sample_pct / 100))
    sample_list = hhno_list[:sample_size]

    logger.info(
        "\nChecking %s of %s households (%.1f%%) for mismatches...",
        f"{sample_size:,}",
        f"{len(hhno_list):,}",
        sample_pct,
    )

    failures = []
    total_tour_matches = 0
    total_legacy_tours = 0
    total_new_tours = 0

    for i, hhno in enumerate(sample_list, 1):
        if i % 1000 == 0:
            logger.info(
                "  Processed %s / %s households...",
                f"{i:,}",
                f"{sample_size:,}",
            )

        leg_tours = legacy_data["tour"].filter(pl.col("hhno") == hhno)
        new_tours = new_data["tour"].filter(pl.col("hhno") == hhno)
        leg_trips = legacy_data["trip"].filter(pl.col("hhno") == hhno)
        new_trips = new_data["trip"].filter(pl.col("hhno") == hhno)

        tour_match = _compare_tours(leg_tours, new_tours)
        total_tour_matches += tour_match["matched"]
        total_legacy_tours += tour_match["total_legacy"]
        total_new_tours += tour_match["total_new"]

        has_mismatch = (
            len(leg_tours) != len(new_tours)
            or len(leg_trips) != len(new_trips)
            or tour_match["matched"] != tour_match["total_legacy"]
        )

        if has_mismatch:
            failures.append(
                {
                    "hhno": hhno,
                    "legacy_tours": len(leg_tours),
                    "new_tours": len(new_tours),
                    "legacy_trips": len(leg_trips),
                    "new_trips": len(new_trips),
                    "tours_matched": tour_match["matched"],
                    "tours_unmatched_legacy": tour_match["unmatched_legacy"],
                    "tours_unmatched_new": tour_match["unmatched_new"],
                }
            )

    # Report summary
    sep = "=" * 80
    mismatch_rate = len(failures) / sample_size * 100 if sample_size else 0
    tour_match_rate = (
        total_tour_matches / total_legacy_tours * 100
        if total_legacy_tours > 0
        else 0
    )

    summary_stats = {
        "total_households": len(hhno_list),
        "households_checked": sample_size,
        "households_with_mismatches": len(failures),
        "mismatch_rate": mismatch_rate,
        "total_legacy_tours": total_legacy_tours,
        "total_new_tours": total_new_tours,
        "tours_matched": total_tour_matches,
        "tour_match_rate": tour_match_rate,
    }

    logger.info(
        "\n%s\nHOUSEHOLD DIARY COMPARISON SUMMARY\n%s\n"
        "Total households in dataset: %s\n"
        "Households checked: %s (%.1f%%)\n"
        "Households with mismatches: %s\n"
        "Mismatch rate: %.2f%%\n\n"
        "Tour Statistics:\n"
        "  Legacy tours: %s\n"
        "  New tours: %s\n"
        "  Tours matched: %s\n"
        "  Tour match rate: %.2f%%",
        sep,
        sep,
        f"{len(hhno_list):,}",
        f"{sample_size:,}",
        sample_pct,
        f"{len(failures):,}",
        mismatch_rate,
        f"{total_legacy_tours:,}",
        f"{total_new_tours:,}",
        f"{total_tour_matches:,}",
        tour_match_rate,
    )

    if not failures:
        logger.info("\n✓ All checked households match!")

    return failures, summary_stats
