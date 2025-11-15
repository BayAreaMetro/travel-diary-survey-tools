"""Module for deriving person type from person attributes."""
import polars as pl

from ..data.codebook import AgeCodes, PersonType


def derive_person_type(persons: pl.DataFrame) -> pl.DataFrame:
    """Derive person_type from person attributes.

    This replicates the pptyp logic from the old pipeline's 02a-reformat
    step, converting employment/student/age data into person type categories.

    Person types (see PersonType enum):
        FULL_TIME_WORKER (1): Full-time worker
        PART_TIME_WORKER (2): Part-time worker
        RETIRED (3): Non-working adult 65+
        NON_WORKER (4): Non-working adult < 65
        UNIVERSITY_STUDENT (5): University student
        HIGH_SCHOOL_STUDENT (6): High school student 16+
        CHILD_5_15 (7): Child 5-15
        CHILD_UNDER_5 (8): Child 0-4

    Args:
        persons: DataFrame with age column (categorical AgeCodes),
                 employment, student, school_type columns

    Returns:
        DataFrame with added person_type column

    Note:
        Age is a categorical variable (see AgeCodes enum):
        1=under 5, 2=5-15, 3=16-17, 4=18-24, 5=25-34, etc.
    """
    return persons.with_columns(
        person_type=pl.when(pl.col("age") == AgeCodes.AGE_UNDER_5)
        .then(pl.lit(PersonType.CHILD_UNDER_5))
        .when(pl.col("age") == AgeCodes.AGE_5_TO_15)
        .then(pl.lit(PersonType.CHILD_5_15))
        # Age 16-17:
        .when(
            (pl.col("age") == AgeCodes.AGE_16_TO_17)
            & pl.col("employment").is_in([1, 3, 8])
        )
        .then(pl.lit(PersonType.FULL_TIME_WORKER))
        .when(
            (pl.col("age") == AgeCodes.AGE_16_TO_17)
            & pl.col("student").is_in([0, 1, 3, 4])
        )
        .then(pl.lit(PersonType.HIGH_SCHOOL_STUDENT))
        # Age 18-24:
        .when(
            (pl.col("age") == AgeCodes.AGE_18_TO_24)
            & pl.col("employment").is_in([1, 3, 8])
        )
        .then(pl.lit(PersonType.FULL_TIME_WORKER))
        .when(
            (pl.col("age") == AgeCodes.AGE_18_TO_24)
            & pl.col("school_type").is_in([4, 7])
            & pl.col("student").is_in([0, 1, 3, 4])
        )
        .then(pl.lit(PersonType.HIGH_SCHOOL_STUDENT))
        .when(
            (pl.col("age") == AgeCodes.AGE_18_TO_24)
            & pl.col("student").is_in([0, 1, 3, 4])
        )
        .then(pl.lit(PersonType.UNIVERSITY_STUDENT))
        .when(
            (pl.col("age") == AgeCodes.AGE_18_TO_24)
            & pl.col("employment").is_in([2, 3, 7])
        )
        .then(pl.lit(PersonType.PART_TIME_WORKER))
        # Age 25-64 (working age):
        .when(
            pl.col("age").is_in([
                AgeCodes.AGE_25_TO_34,
                AgeCodes.AGE_35_TO_44,
                AgeCodes.AGE_45_TO_54,
                AgeCodes.AGE_55_TO_64,
            ])
            & pl.col("employment").is_in([1, 3, 8])
        )
        .then(pl.lit(PersonType.FULL_TIME_WORKER))
        .when(
            pl.col("age").is_in([
                AgeCodes.AGE_25_TO_34,
                AgeCodes.AGE_35_TO_44,
                AgeCodes.AGE_45_TO_54,
                AgeCodes.AGE_55_TO_64,
            ])
            & pl.col("student").is_in([0, 1, 3, 4])
        )
        .then(pl.lit(PersonType.UNIVERSITY_STUDENT))
        .when(
            pl.col("age").is_in([
                AgeCodes.AGE_25_TO_34,
                AgeCodes.AGE_35_TO_44,
                AgeCodes.AGE_45_TO_54,
                AgeCodes.AGE_55_TO_64,
            ])
            & pl.col("employment").is_in([2, 3, 7])
        )
        .then(pl.lit(PersonType.PART_TIME_WORKER))
        .when(
            pl.col("age").is_in([
                AgeCodes.AGE_25_TO_34,
                AgeCodes.AGE_35_TO_44,
                AgeCodes.AGE_45_TO_54,
                AgeCodes.AGE_55_TO_64,
            ])
        )
        .then(pl.lit(PersonType.NON_WORKER))
        # Age 65+:
        .otherwise(pl.lit(PersonType.RETIRED))
        .cast(pl.Int64)
    )
