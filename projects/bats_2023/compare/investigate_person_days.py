"""Investigate person-days differences between legacy and new pipeline."""

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


if __name__ == "__main__":
    # Load data
    legacy = load_legacy_data(LEGACY_DIR)
    new = load_new_pipeline_data(CONFIG_PATH, cache_dir=CACHE_DIR)

    # Get person-day tables
    legacy_pd = legacy["personday"]
    new_pd = new["personday"]

    logger.info("\n%s", "=" * 80)
    logger.info("PERSON-DAY INVESTIGATION")
    logger.info("%s", "=" * 80)

    # Check unique households
    legacy_hhnos = set(legacy_pd["hhno"].unique().to_list())
    new_hhnos = set(new_pd["hhno"].unique().to_list())

    logger.info("\nUnique households with person-days:")
    logger.info("  Legacy: %s", f"{len(legacy_hhnos):,}")
    logger.info("  New:    %s", f"{len(new_hhnos):,}")
    logger.info("  Missing in new: %s", f"{len(legacy_hhnos - new_hhnos):,}")
    logger.info("  Added in new:   %s", f"{len(new_hhnos - legacy_hhnos):,}")

    # Check unique persons
    legacy_persons_list = (
        legacy_pd.select([pl.col("hhno"), pl.col("pno")])
        .unique()
        .with_columns(
            person_key=pl.concat_str(
                [pl.col("hhno"), pl.lit("_"), pl.col("pno")]
            )
        )
    )
    new_persons_list = (
        new_pd.select([pl.col("hhno"), pl.col("pno")])
        .unique()
        .with_columns(
            person_key=pl.concat_str(
                [pl.col("hhno"), pl.lit("_"), pl.col("pno")]
            )
        )
    )

    legacy_persons = set(legacy_persons_list["person_key"].to_list())
    new_persons = set(new_persons_list["person_key"].to_list())

    logger.info("\nUnique persons with person-days:")
    logger.info("  Legacy: %s", f"{len(legacy_persons):,}")
    logger.info("  New:    %s", f"{len(new_persons):,}")
    logger.info(
        "  Missing in new: %s", f"{len(legacy_persons - new_persons):,}"
    )
    logger.info(
        "  Added in new:   %s", f"{len(new_persons - legacy_persons):,}"
    )

    # Check person-days per person distribution
    legacy_pd_per_person = (
        legacy_pd.group_by(["hhno", "pno"])
        .agg(pl.len().alias("num_days"))
        .group_by("num_days")
        .agg(pl.len().alias("count"))
        .sort("num_days")
    )

    new_pd_per_person = (
        new_pd.group_by(["hhno", "pno"])
        .agg(pl.len().alias("num_days"))
        .group_by("num_days")
        .agg(pl.len().alias("count"))
        .sort("num_days")
    )

    logger.info("\nPerson-days per person distribution:")
    logger.info("\nLegacy:")
    logger.info(str(legacy_pd_per_person))
    logger.info("\nNew Pipeline:")
    logger.info(str(new_pd_per_person))

    # Check if there are specific day types or patterns
    if "dayno" in legacy_pd.columns and "dayno" in new_pd.columns:
        legacy_dayno_dist = (
            legacy_pd.group_by("dayno")
            .agg(pl.len().alias("count"))
            .sort("dayno")
        )
        new_dayno_dist = (
            new_pd.group_by("dayno").agg(pl.len().alias("count")).sort("dayno")
        )

        logger.info("\nDay number distribution:")
        logger.info("\nLegacy:")
        logger.info(str(legacy_dayno_dist))
        logger.info("\nNew Pipeline:")
        logger.info(str(new_dayno_dist))

    # Sample some persons who have different numbers of person-days
    common_persons = legacy_persons & new_persons
    if common_persons:
        # Find persons with different day counts
        mismatches = []
        for person_key in list(common_persons)[:1000]:  # Check first 1000
            parts = person_key.split("_")
            hhno, pno = int(parts[0]), int(parts[1])

            legacy_count = len(
                legacy_pd.filter(
                    (pl.col("hhno") == hhno) & (pl.col("pno") == pno)
                )
            )
            new_count = len(
                new_pd.filter((pl.col("hhno") == hhno) & (pl.col("pno") == pno))
            )

            if legacy_count != new_count:
                mismatches.append(
                    {
                        "hhno": hhno,
                        "pno": pno,
                        "legacy_days": legacy_count,
                        "new_days": new_count,
                    }
                )

        if mismatches:
            logger.info("\nSample persons with different day counts (first 5):")
            for mm in mismatches[:5]:
                logger.info(
                    "  HH %s, Person %s: Legacy=%s, New=%s",
                    mm["hhno"],
                    mm["pno"],
                    mm["legacy_days"],
                    mm["new_days"],
                )

                # Show actual person-day records
                legacy_days = legacy_pd.filter(
                    (pl.col("hhno") == mm["hhno"])
                    & (pl.col("pno") == mm["pno"])
                ).select(["hhno", "pno", "dayno", "dow", "trpdist"])

                new_days = new_pd.filter(
                    (pl.col("hhno") == mm["hhno"])
                    & (pl.col("pno") == mm["pno"])
                ).select(["hhno", "pno", "dayno", "dow", "trpdist"])

                logger.info("\n  Legacy person-days:")
                logger.info(str(legacy_days))
                logger.info("\n  New person-days:")
                logger.info(str(new_days))
                logger.info("")

    # Check missing persons - are they mostly 1-day people?
    missing_persons = legacy_persons - new_persons
    if missing_persons:
        # Get day counts for missing persons
        missing_sample = list(missing_persons)[:100]
        missing_day_counts = []

        for person_key in missing_sample:
            parts = person_key.split("_")
            hhno, pno = int(parts[0]), int(parts[1])
            legacy_count = len(
                legacy_pd.filter(
                    (pl.col("hhno") == hhno) & (pl.col("pno") == pno)
                )
            )
            missing_day_counts.append(legacy_count)

        # Count distribution
        from collections import Counter

        dist = Counter(missing_day_counts)
        logger.info(
            "\nDay count distribution for missing persons (sample of 100):"
        )
        for days in sorted(dist.keys()):
            logger.info("  %s day(s): %s persons", days, dist[days])
