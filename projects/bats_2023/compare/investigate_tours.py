"""Investigate tour extraction differences between legacy and new pipeline."""

import logging
from pathlib import Path

import polars as pl
from helpers import load_legacy_data, load_new_pipeline_data

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(message)s")

# Legacy Daysim output directory
LEGACY_DIR = Path(
    "M:/Data/HomeInterview/Bay Area Travel Study 2023/"
    "Data/Processed/test/03b-assign_day/wt-wkday_3day"
)

# New pipeline config
CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"
CACHE_DIR = Path(__file__).parent.parent.parent.parent / ".cache"


def analyze_household_tours(hhno: int, legacy: dict, new: dict) -> None:
    """Analyze tour structure for a specific household."""
    logger.info("\n%s", "=" * 80)
    logger.info("ANALYZING HOUSEHOLD %s", hhno)
    logger.info("%s", "=" * 80)

    # Get tours
    leg_tours = (
        legacy["tour"].filter(pl.col("hhno") == hhno).sort(["pno", "tour"])
    )
    new_tours = new["tour"].filter(pl.col("hhno") == hhno).sort(["pno", "tour"])

    # Get trips for context
    leg_trips = (
        legacy["trip"]
        .filter(pl.col("hhno") == hhno)
        .sort(["pno", "tour", "deptm"])
    )
    new_trips = (
        new["trip"]
        .filter(pl.col("hhno") == hhno)
        .sort(["pno", "tour", "deptm"])
    )

    logger.info(
        "\nTour counts: Legacy=%s, New=%s", len(leg_tours), len(new_tours)
    )
    logger.info(
        "Trip counts: Legacy=%s, New=%s", len(leg_trips), len(new_trips)
    )

    # Show tour structure
    logger.info("\nLegacy Tours:")
    logger.info(
        str(
            leg_tours.select(
                [
                    "pno",
                    "tour",
                    "pdpurp",
                    "tlvorig",
                    "tardest",
                    "tarorig",
                    "parent",
                    "subtrs",
                ]
            )
        )
    )

    logger.info("\nNew Tours:")
    logger.info(
        str(
            new_tours.select(
                [
                    "pno",
                    "tour",
                    "pdpurp",
                    "tlvorig",
                    "tardest",
                    "tarorig",
                    "parent",
                    "subtrs",
                ]
            )
        )
    )

    # For each person, show their trip pattern
    for pno in leg_tours["pno"].unique().sort():
        logger.info("\n--- Person %s Trip Pattern ---", pno)

        person_leg_trips = leg_trips.filter(pl.col("pno") == pno)
        person_new_trips = new_trips.filter(pl.col("pno") == pno)

        logger.info("\nLegacy trips:")
        if len(person_leg_trips) > 0:
            logger.info(
                str(
                    person_leg_trips.select(
                        ["tour", "deptm", "dorp", "dpurp", "otaz", "dtaz"]
                    ).head(15)
                )
            )

        logger.info("\nNew trips:")
        if len(person_new_trips) > 0:
            logger.info(
                str(
                    person_new_trips.select(
                        ["tour", "deptm", "dorp", "dpurp", "otaz", "dtaz"]
                    ).head(15)
                )
            )


if __name__ == "__main__":
    # Load data
    legacy = load_legacy_data(LEGACY_DIR)
    new = load_new_pipeline_data(CONFIG_PATH, cache_dir=CACHE_DIR)

    # Look at households from the comparison that had mismatches
    # Household 23000339: 8 tours in both, but only 1 matched
    # Household 23000748: 2 tours, both matched (good example)
    # Household 23000909: 9→8 tours, 3 matched
    # Household 23000954: 5 tours each, 1 matched
    HOUSEHOLD_23000339 = 23000339
    HOUSEHOLD_23000748 = 23000748
    HOUSEHOLD_23000909 = 23000909
    HOUSEHOLD_23000954 = 23000954
    problem_households = [
        HOUSEHOLD_23000339,
        HOUSEHOLD_23000748,
        HOUSEHOLD_23000909,
        HOUSEHOLD_23000954,
    ]

    for hhno in problem_households:
        analyze_household_tours(hhno, legacy, new)

    # Investigate tour numbering strategy
    logger.info("\n%s", "=" * 80)
    logger.info("TOUR NUMBERING ANALYSIS")
    logger.info("%s", "=" * 80)

    # Check if new pipeline resets tour numbers per day
    sample_tours = (
        new["tour"]
        .head(100)
        .select(["hhno", "pno", "day", "tour", "pdpurp", "parent"])
    )

    logger.info("\nSample of new pipeline tours (first 100):")
    logger.info(str(sample_tours))

    # Check day column - does it exist in legacy?
    logger.info("\nLegacy tour columns:")
    logger.info(str(legacy["tour"].columns))

    logger.info("\nNew tour columns:")
    logger.info(str(new["tour"].columns))

    # Check if tours are numbered per-day or across days
    EXAMPLE_HOUSEHOLD = 23000339
    EXAMPLE_PERSON = 1
    person_sample = (
        new["tour"]
        .filter(
            (pl.col("hhno") == EXAMPLE_HOUSEHOLD)
            & (pl.col("pno") == EXAMPLE_PERSON)
        )
        .select(["hhno", "pno", "day", "tour", "pdpurp", "tlvorig"])
    )

    logger.info("\nExample person's tours across days:")
    logger.info(str(person_sample))
