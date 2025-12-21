"""Format mandatory locations for CT-RAMP specification."""

import logging

import polars as pl

from data_canon.codebook.ctramp import EmploymentCategory, StudentCategory
from data_canon.codebook.persons import Employment, Student

from .ctramp_config import CTRAMPConfig

logger = logging.getLogger(__name__)


def format_mandatory_location(
    persons: pl.DataFrame,
    households: pl.DataFrame,
    config: CTRAMPConfig,
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
        config: CT-RAMP configuration with income_base_year_dollars

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

    # Check if persons has work/school location columns
    # If not, return empty DataFrame (no mandatory locations)
    if (
        "work_taz" not in persons.columns
        and "school_taz" not in persons.columns
    ):
        return pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "taz": pl.Int64,
                "WorkLocation": pl.Int64,
                "SchoolLocation": pl.Int64,
            }
        )

    # Join persons with households to get income and home TAZ
    mandatory_loc = persons.join(
        households.select(["hh_id", "home_taz", "income"]),
        on="hh_id",
        how="left",
    )

    # Compute employment_category from employment
    mandatory_loc = mandatory_loc.with_columns(
        pl.when(
            pl.col("employment").is_in(
                [
                    Employment.EMPLOYED_FULLTIME.value,
                    Employment.EMPLOYED_PARTTIME.value,
                ]
            )
        )
        .then(pl.lit(EmploymentCategory.EMPLOYED.value))
        .otherwise(pl.lit(EmploymentCategory.NOT_EMPLOYED.value))
        .alias("employment_category")
    )

    # Compute student_category from student
    mandatory_loc = mandatory_loc.with_columns(
        pl.when(
            pl.col("student").is_in(
                [
                    Student.FULLTIME_INPERSON.value,
                    Student.PARTTIME_INPERSON.value,
                    Student.FULLTIME_ONLINE.value,
                    Student.PARTTIME_ONLINE.value,
                ]
            )
        )
        .then(pl.lit(StudentCategory.STUDENT.value))
        .otherwise(pl.lit(StudentCategory.NOT_STUDENT.value))
        .alias("student_category")
    )

    # Filter to only persons with work or school locations
    # Add columns as null if they don't exist
    if "work_taz" not in mandatory_loc.columns:
        mandatory_loc = mandatory_loc.with_columns(
            pl.lit(None).cast(pl.Int64).alias("work_taz")
        )
    if "school_taz" not in mandatory_loc.columns:
        mandatory_loc = mandatory_loc.with_columns(
            pl.lit(None).cast(pl.Int64).alias("school_taz")
        )

    mandatory_loc = mandatory_loc.filter(
        (pl.col("work_taz").is_not_null() & (pl.col("work_taz") > 0))
        | (pl.col("school_taz").is_not_null() & (pl.col("school_taz") > 0))
    )

    # Map to CT-RAMP column names
    mandatory_loc = mandatory_loc.select(
        [
            pl.col("hh_id").alias("HHID"),
            pl.col("home_taz").cast(pl.Int64).alias("HomeTAZ"),
            (pl.col("income") / config.income_base_year_dollars)
            .cast(pl.Int64)
            .alias("Income"),
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
