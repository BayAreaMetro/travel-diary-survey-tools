"""Compare old Daysim results to the new pipeline results."""

import logging
import random
from pathlib import Path

import polars as pl

from pipeline.pipeline import Pipeline

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
)

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

# Legacy Daysim output directory
LEGACY_DIR = Path(
    "M:/Data/HomeInterview/Bay Area Travel Study 2023/"
    "Data/Processed/test/03b-assign_day/wt-wkday_3day"
)

# New pipeline config
CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"

# Number of households to sample for detailed comparison
NUM_SAMPLE_HOUSEHOLDS = 5


# ---------------------------------------------------------------------
# Load Functions
# ---------------------------------------------------------------------


def load_legacy_data() -> dict[str, pl.DataFrame]:
    """Load legacy Daysim CSV files from the old pipeline.

    Returns:
        Dictionary with keys: hh, person, personday, tour, trip
    """
    logger.info("Loading legacy Daysim data...")

    legacy_data = {
        "hh": pl.read_csv(LEGACY_DIR / "hh.csv"),
        "person": pl.read_csv(LEGACY_DIR / "person.csv"),
        "personday": pl.read_csv(LEGACY_DIR / "personday.csv"),
        "tour": pl.read_csv(LEGACY_DIR / "tour.csv"),
        "trip": pl.read_csv(LEGACY_DIR / "trip.csv"),
    }

    logger.info("  Households: %s", f"{len(legacy_data['hh']):,}")
    logger.info("  Persons: %s", f"{len(legacy_data['person']):,}")
    logger.info("  Person-days: %s", f"{len(legacy_data['personday']):,}")
    logger.info("  Tours: %s", f"{len(legacy_data['tour']):,}")
    logger.info("  Trips: %s", f"{len(legacy_data['trip']):,}")

    return legacy_data


def load_new_pipeline_data() -> dict[str, pl.DataFrame]:
    """Load new pipeline Daysim-formatted tables from cache.

    Returns:
        Dictionary with keys: hh, person, personday, tour, trip
    """
    logger.info("\nLoading new pipeline data...")

    # Initialize pipeline with empty steps (load from cache only)
    pipeline = Pipeline(
        config_path=str(CONFIG_PATH),
        steps=[],
        caching=True,
    )

    # Load Daysim-formatted tables
    new_data = {
        "hh": pipeline.get_data("households_daysim"),
        "person": pipeline.get_data("persons_daysim"),
        "personday": pipeline.get_data("days_daysim"),
        "tour": pipeline.get_data("tours_daysim"),
        "trip": pipeline.get_data("linked_trips_daysim"),
    }

    logger.info("  Households: %s", f"{len(new_data['hh']):,}")
    logger.info("  Persons: %s", f"{len(new_data['person']):,}")
    logger.info("  Person-days: %s", f"{len(new_data['personday']):,}")
    logger.info("  Tours: %s", f"{len(new_data['tour']):,}")
    logger.info("  Trips: %s", f"{len(new_data['trip']):,}")

    return new_data


# ---------------------------------------------------------------------
# Comparison Functions
# ---------------------------------------------------------------------


def compare_row_counts(
    legacy_data: dict[str, pl.DataFrame],
    new_data: dict[str, pl.DataFrame],
) -> None:
    """Compare row counts between legacy and new pipeline data.

    Args:
        legacy_data: Dictionary of legacy DataFrames
        new_data: Dictionary of new pipeline DataFrames
    """
    separator = "=" * 80
    output_lines = [
        "",
        separator,
        "ROW COUNT COMPARISON",
        separator,
        "",
        f"{'Table':<15} {'Legacy':<12} {'New':<12} "
        f"{'Difference':<12} {'% Diff':<10}",
        "-" * 80,
    ]

    tables = ["hh", "person", "personday", "tour", "trip"]
    table_names = ["Households", "Persons", "Person-days", "Tours", "Trips"]

    for table, name in zip(tables, table_names, strict=False):
        legacy_count = len(legacy_data[table])
        new_count = len(new_data[table])
        diff = new_count - legacy_count
        pct_diff = (diff / legacy_count * 100) if legacy_count > 0 else 0

        output_lines.append(
            f"{name:<15} {legacy_count:<12,} {new_count:<12,} "
            f"{diff:+12,} {pct_diff:+9.2f}%"
        )

    logger.info("\n".join(output_lines))


def compare_columns(
    legacy_data: dict[str, pl.DataFrame],
    new_data: dict[str, pl.DataFrame],
) -> None:
    """Compare column names between legacy and new pipeline data.

    Args:
        legacy_data: Dictionary of legacy DataFrames
        new_data: Dictionary of new pipeline DataFrames
    """
    separator = "=" * 80
    output_lines = [
        "",
        separator,
        "COLUMN COMPARISON",
        separator,
    ]

    tables = ["hh", "person", "personday", "tour", "trip"]
    table_names = ["Households", "Persons", "Person-days", "Tours", "Trips"]

    for table, name in zip(tables, table_names, strict=False):
        legacy_cols = set(legacy_data[table].columns)
        new_cols = set(new_data[table].columns)

        common_cols = sorted(legacy_cols & new_cols)
        legacy_only = sorted(legacy_cols - new_cols)
        new_only = sorted(new_cols - legacy_cols)

        output_lines.append("")
        output_lines.append(f"--- {name} ---")
        output_lines.append(
            f"Total columns: Legacy={len(legacy_cols)}, "
            f"New={len(new_cols)}, Common={len(common_cols)}"
        )

        if legacy_only:
            output_lines.append("")
            output_lines.append(
                f"Columns in legacy missing from new ({len(legacy_only)}):"
            )
            output_lines.append("  " + ", ".join(legacy_only))

        if new_only and legacy_only:
            output_lines.append("")
            output_lines.append(f"Columns only in new ({len(new_only)}):")
            output_lines.append("  " + ", ".join(new_only))

        if not legacy_only:
            output_lines.append("")
            output_lines.append("✓ Columns match")

    logger.info("\n".join(output_lines))


def compare_household_diaries(
    hhno_list: list[int],
    legacy_data: dict[str, pl.DataFrame],
    new_data: dict[str, pl.DataFrame],
) -> None:
    """Compare daily diaries for specific households.

    Args:
        hhno_list: List of household IDs to compare
        legacy_data: Dictionary of legacy DataFrames
        new_data: Dictionary of new pipeline DataFrames
    """
    for hhno in hhno_list:
        separator = "=" * 80
        output_lines = [
            "",
            separator,
            f"HOUSEHOLD {hhno} DIARY COMPARISON",
            separator,
        ]

        # Get household data from both sources
        legacy_hh_tours = legacy_data["tour"].filter(pl.col("hhno") == hhno)
        new_hh_tours = new_data["tour"].filter(pl.col("hhno") == hhno)

        legacy_hh_trips = legacy_data["trip"].filter(pl.col("hhno") == hhno)
        new_hh_trips = new_data["trip"].filter(pl.col("hhno") == hhno)

        output_lines.append("")
        output_lines.append(
            f"Legacy: {len(legacy_hh_tours)} tours, "
            f"{len(legacy_hh_trips)} trips"
        )
        output_lines.append(
            f"New:    {len(new_hh_tours)} tours, {len(new_hh_trips)} trips"
        )

        logger.info("\n".join(output_lines))
        if len(legacy_hh_tours) > 0 or len(new_hh_tours) > 0:
            tour_output = ["\n--- Tours ---", ""]
            tour_output.append(f"Legacy Tours (n={len(legacy_hh_tours)}):")
            if len(legacy_hh_tours) > 0:
                tour_cols = [
                    "hhno",
                    "pno",
                    "tour",
                    "pdpurp",
                    "tlvorig",
                    "tardest",
                    "tarorig",
                    "tldest",
                    "mode",
                ]
                available_cols = [
                    c for c in tour_cols if c in legacy_hh_tours.columns
                ]
                tour_output.append(
                    str(
                        legacy_hh_tours.select(available_cols).sort(
                            ["pno", "tour"]
                        )
                    )
                )

            tour_output.append("")
            tour_output.append(f"New Pipeline Tours (n={len(new_hh_tours)}):")
            if len(new_hh_tours) > 0:
                tour_cols = [
                    "hhno",
                    "pno",
                    "tour",
                    "pdpurp",
                    "tlvorig",
                    "tardest",
                    "tarorig",
                    "tldest",
                    "mode",
                ]
                available_cols = [
                    c for c in tour_cols if c in new_hh_tours.columns
                ]
                tour_output.append(
                    str(
                        new_hh_tours.select(available_cols).sort(
                            ["pno", "tour"]
                        )
                    )
                )

            logger.info("\n".join(tour_output))

        # Show trips side by side
        if len(legacy_hh_trips) > 0 or len(new_hh_trips) > 0:
            trip_output = ["\n--- Trips ---", ""]
            trip_output.append(f"Legacy Trips (n={len(legacy_hh_trips)}):")
            trip_cols = [
                "hhno",
                "pno",
                "tour",
                "deptm",
                "dorp",
                "mode",
                "otaz",
                "dtaz",
            ]
            if len(legacy_hh_trips) > 0:
                available_cols = [
                    c for c in trip_cols if c in legacy_hh_trips.columns
                ]
                trip_output.append(
                    str(
                        legacy_hh_trips.select(available_cols)
                        .sort(["pno", "tour", "deptm"])
                        .head(20)
                    )
                )

            trip_output.append("")
            trip_output.append(f"New Pipeline Trips (n={len(new_hh_trips)}):")
            if len(new_hh_trips) > 0:
                available_cols = [
                    c for c in trip_cols if c in new_hh_trips.columns
                ]
                trip_output.append(
                    str(
                        new_hh_trips.select(available_cols)
                        .sort(["pno", "tour", "deptm"])
                        .head(20)
                    )
                )

            logger.info("\n".join(trip_output))


def print_summary_statistics(
    legacy_data: dict[str, pl.DataFrame],
    new_data: dict[str, pl.DataFrame],
) -> None:
    """Print summary statistics comparing key distributions.

    Args:
        legacy_data: Dictionary of legacy DataFrames
        new_data: Dictionary of new pipeline DataFrames
    """
    separator = "=" * 80
    output_lines = [
        "",
        separator,
        "SUMMARY STATISTICS",
        separator,
    ]
    logger.info("\n".join(output_lines))

    # Tour purpose distribution
    if (
        "pdpurp" in legacy_data["tour"].columns
        and "pdpurp" in new_data["tour"].columns
    ):
        output_lines = [
            "",
            "--- Tour Purpose Distribution ---",
            "",
            "Legacy:",
            str(
                legacy_data["tour"]
                .group_by("pdpurp")
                .agg(pl.len().alias("count"))
                .sort("pdpurp")
            ),
            "",
            "New Pipeline:",
            str(
                new_data["tour"]
                .group_by("pdpurp")
                .agg(pl.len().alias("count"))
                .sort("pdpurp")
            ),
        ]
        logger.info("\n".join(output_lines))

    # Tour mode distribution
    if (
        "mode" in legacy_data["tour"].columns
        and "mode" in new_data["tour"].columns
    ):
        output_lines = [
            "",
            "--- Tour Mode Distribution ---",
            "",
            "Legacy:",
            str(
                legacy_data["tour"]
                .group_by("mode")
                .agg(pl.len().alias("count"))
                .sort("mode")
            ),
            "",
            "New Pipeline:",
            str(
                new_data["tour"]
                .group_by("mode")
                .agg(pl.len().alias("count"))
                .sort("mode")
            ),
        ]
        logger.info("\n".join(output_lines))

    # Trip mode distribution
    if (
        "mode" in legacy_data["trip"].columns
        and "mode" in new_data["trip"].columns
    ):
        output_lines = [
            "",
            "--- Trip Mode Distribution ---",
            "",
            "Legacy:",
            str(
                legacy_data["trip"]
                .group_by("mode")
                .agg(pl.len().alias("count"))
                .sort("mode")
            ),
            "",
            "New Pipeline:",
            str(
                new_data["trip"]
                .group_by("mode")
                .agg(pl.len().alias("count"))
                .sort("mode")
            ),
        ]
        logger.info("\n".join(output_lines))

    # TAZ coverage (households)
    output_lines = ["", "--- Household TAZ Coverage ---"]
    if "hhtaz" in legacy_data["hh"].columns:
        legacy_null_taz = (
            legacy_data["hh"]
            .filter(pl.col("hhtaz").is_null() | (pl.col("hhtaz") == -1))
            .height
        )
        output_lines.append(
            f"Legacy: {legacy_null_taz:,} households with missing/invalid TAZ"
        )

    if "hhtaz" in new_data["hh"].columns:
        new_null_taz = (
            new_data["hh"]
            .filter(pl.col("hhtaz").is_null() | (pl.col("hhtaz") == -1))
            .height
        )
        output_lines.append(
            f"New:    {new_null_taz:,} households with missing/invalid TAZ"
        )

    logger.info("\n".join(output_lines))

    # Weight totals
    output_lines = ["", "--- Weight Totals ---"]
    if "hhwgt" in legacy_data["hh"].columns:
        legacy_hh_weight = legacy_data["hh"]["hhwgt"].sum()
        output_lines.append(
            f"Legacy household weight total: {legacy_hh_weight:,.2f}"
        )

    if "hhwgt" in new_data["hh"].columns:
        new_hh_weight = new_data["hh"]["hhwgt"].sum()
        output_lines.append(
            f"New household weight total:    {new_hh_weight:,.2f}"
        )

    logger.info("\n".join(output_lines))


# ---------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------


if __name__ == "__main__":
    # Load data from both sources
    legacy = load_legacy_data()
    new = load_new_pipeline_data()

    # Compare row counts
    compare_row_counts(legacy, new)

    # Compare columns
    compare_columns(legacy, new)

    # Sample a few households that exist in both datasets
    # Find households that exist in both datasets
    legacy_hhnos = set(legacy["hh"]["hhno"].unique().to_list())
    new_hhnos = set(new["hh"]["hhno"].unique().to_list())
    common_hhnos = sorted(legacy_hhnos & new_hhnos)
    pct_overlap = (
        len(common_hhnos) / len(legacy_hhnos) * 100
        if len(legacy_hhnos) > 0
        else 0
    )

    msg = (
        f"\n{'=' * 80}\n"
        "SAMPLING HOUSEHOLDS FOR DETAILED COMPARISON\n"
        f"{'=' * 80}\n"
        f"Total households in legacy data: {len(legacy_hhnos):,}\n"
        f"Total households in new data:    {len(new_hhnos):,}\n"
        f"Percent overlap:                 {pct_overlap:.2f}%\n"
    )
    logger.info(msg)

    # Sample households for detailed comparison
    if len(common_hhnos) >= NUM_SAMPLE_HOUSEHOLDS:
        random.seed(42)
        sample_hhnos = random.sample(common_hhnos, NUM_SAMPLE_HOUSEHOLDS)
        logger.info("\nSampling households: %s", sample_hhnos)
        compare_household_diaries(sample_hhnos, legacy, new)
    else:
        logger.info("\nNot enough common households to sample.")

    # Print summary statistics
    print_summary_statistics(legacy, new)
