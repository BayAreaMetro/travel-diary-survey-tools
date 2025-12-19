"""Format mandatory locations for CT-RAMP specification."""

import logging

import polars as pl

logger = logging.getLogger(__name__)


def format_mandatory_location(
    persons: pl.DataFrame,
    households: pl.DataFrame,
) -> pl.DataFrame:
    """Format mandatory locations (work/school) to CT-RAMP specification.

    Transforms person and household data to create mandatory location records
    for persons with work or school locations.

    Args:
        persons: DataFrame with canonical person fields including:
            - person_id, hh_id, person_num
            - person_type (CTRAMP classified)
            - age, employment_category, student_category
            - work_taz, school_taz
        households: DataFrame with canonical household fields including:
            - hh_id, home_taz, income

    Returns:
        DataFrame with CT-RAMP mandatory location fields:
        - HHID, PersonID, PersonNum
        - HomeTAZ, Income
        - PersonType, PersonAge
        - EmploymentCategory, StudentCategory
        - WorkLocation, SchoolLocation

    Notes:
        - Excludes model-only fields (walk subzones)
        - Filters to only persons with work OR school locations
    """
    logger.info("Formatting mandatory location data for CT-RAMP")

    # Join persons with households to get income and home TAZ
    mandatory_loc = persons.join(
        households.select(["hh_id", "home_taz", "income"]),
        on="hh_id",
        how="left",
    )

    # Filter to only persons with work or school locations
    mandatory_loc = mandatory_loc.filter(
        (pl.col("work_taz").is_not_null() & (pl.col("work_taz") > 0))
        | (pl.col("school_taz").is_not_null() & (pl.col("school_taz") > 0))
    )

    # Map to CT-RAMP column names
    mandatory_loc = mandatory_loc.select(
        [
            pl.col("hh_id").alias("HHID"),
            pl.col("home_taz").cast(pl.Int64).alias("HomeTAZ"),
            (pl.col("income") / 2000).cast(pl.Int64).alias("Income"),
            pl.col("person_id").alias("PersonID"),
            pl.col("person_num").alias("PersonNum"),
            pl.col("person_type").alias("PersonType"),
            pl.col("age").alias("PersonAge"),
            pl.col("employment_category").alias("EmploymentCategory"),
            pl.col("student_category").alias("StudentCategory"),
            pl.col("work_taz")
            .fill_null(0)
            .cast(pl.Int64)
            .alias("WorkLocation"),
            pl.col("school_taz")
            .fill_null(0)
            .cast(pl.Int64)
            .alias("SchoolLocation"),
        ]
    )

    logger.info("Formatted %d mandatory location records", len(mandatory_loc))
    return mandatory_loc
