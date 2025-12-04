"""Person formatting for DaySim output."""

import logging

import polars as pl

from data_canon.codebook.daysim import DaysimStudentType
from data_canon.codebook.persons import (
    Employment,
    PersonType,
    SchoolType,
    Student,
)

from .mappings import (
    AGE_MAP,
    GENDER_MAP,
    STUDENT_MAP,
    WORK_PARK_MAP,
    AgeThreshold,
)

logger = logging.getLogger(__name__)


def compute_day_completeness(days: pl.DataFrame) -> pl.DataFrame:
    """Compute day completeness indicators for survey weighting.

    Creates person-level completeness indicators by day of week and calculates
    total complete days for different weekday periods (3-day, 4-day, 5-day).

    Args:
        days: DataFrame with columns [person_id, is_complete, travel_dow]

    Returns:
        DataFrame with columns:
        - hhno, pno: Household and person identifiers
        - mon_complete through sun_complete: Binary indicators (0/1)
        - num_days_complete_3dayweekday: Sum of Tue+Wed+Thu
        - num_days_complete_4dayweekday: Sum of Mon+Tue+Wed+Thu
        - num_days_complete_5dayweekday: Sum of Mon+Tue+Wed+Thu+Fri
    """
    logger.info("Computing day completeness indicators")

    # Pivot days by person and day of week
    return (
        days.select(["person_id", "is_complete", "travel_dow"])
        .pivot(index="person_id", on="travel_dow", values="is_complete")
        .fill_null(0)
        .with_columns(
            # Extract hhno and pno from person_id (person_id = hhno*100 + pno)
            hhno=(pl.col("person_id") // 100),
            pno=(pl.col("person_id") % 100),
            # Compute weekday aggregates
            num_days_complete_3dayweekday=pl.sum_horizontal(["2", "3", "4"]),
            num_days_complete_4dayweekday=pl.sum_horizontal(
                ["1", "2", "3", "4"]
            ),
            num_days_complete_5dayweekday=pl.sum_horizontal(
                ["1", "2", "3", "4", "5"]
            ),
        )
        .select(
            [
                "hhno",
                "pno",
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7",
                "num_days_complete_3dayweekday",
                "num_days_complete_4dayweekday",
                "num_days_complete_5dayweekday",
            ]
        )
        .rename(
            {
                "1": "mon_complete",
                "2": "tue_complete",
                "3": "wed_complete",
                "4": "thu_complete",
                "5": "fri_complete",
                "6": "sat_complete",
                "7": "sun_complete",
            }
        )
    )


def format_persons(
    persons: pl.DataFrame, day_completeness: pl.DataFrame | None
) -> pl.DataFrame:
    """Format person data to DaySim specification.

    Applies mapping dictionaries and derives person type (pptyp) and worker
    type (pwtyp) based on age, employment, and student status.

    Person type (pptyp) cascading logic:
    - Age < 5: Child 0-4 (type 8)
    - Age < 16: Child 5-15 (type 7)
    - Full-time employed: Full-time worker (type 1)
    - Age 16-17 and student: High school 16+ (type 6)
    - Age 18-24 and high school: High school 16+ (type 6)
    - Age >= 18 and student: University student (type 5)
    - Part-time/self-employed: Part-time worker (type 2)
    - Age < 65: Non-working adult (type 4)
    - Age >= 65: Non-working senior (type 3)

    Args:
        persons: DataFrame with canonical person fields
        day_completeness: Optional DataFrame with day completeness indicators

    Returns:
        DataFrame with DaySim person fields
    """
    logger.info("Formatting person data")

    # Rename columns to DaySim naming convention
    persons_daysim = persons.rename(
        {
            "hh_id": "hhno",
            "person_num": "pno",
            "work_lon": "pwxcord",
            "work_lat": "pwycord",
            "school_lon": "psxcord",
            "school_lat": "psycord",
            "work_taz": "pwtaz",
            "work_maz": "pwpcl",
            "school_taz": "pstaz",
            "school_maz": "pspcl",
        }
    )

    # Apply basic field mappings and transformations
    persons_daysim = persons_daysim.with_columns(
        # Fill null coordinates with -1
        pl.col(["pwtaz", "pwpcl", "pstaz", "pspcl"]).fill_null(-1),
        # Map age categories to midpoint ages
        pagey=pl.col("age").replace(AGE_MAP),
        # Map gender codes
        pgend=pl.col("gender").replace(GENDER_MAP),
        # Map student status
        pstyp=pl.col("student")
        .replace(STUDENT_MAP)
        .fill_null(DaysimStudentType.NOT_STUDENT.value),
        # Map work parking
        ppaidprk=pl.col("work_park").replace_strict(WORK_PARK_MAP),
    )

    # Derive person type (pptyp) using cascading logic
    persons_daysim = persons_daysim.with_columns(
        pptyp=pl.when(pl.col("pagey") < AgeThreshold.CHILD_PRESCHOOL)
        .then(pl.lit(PersonType.CHILD_UNDER_5.value))
        .when(pl.col("pagey") < AgeThreshold.CHILD_SCHOOL)
        .then(pl.lit(PersonType.CHILD_5_15.value))
        # Age >= 16:
        .when(
            pl.col("employment").is_in(
                [
                    Employment.EMPLOYED_FULLTIME.value,
                    Employment.EMPLOYED_SELF.value,
                    Employment.EMPLOYED_FURLOUGHED.value,
                    Employment.EMPLOYED_UNPAID.value,
                ]
            )
        )
        .then(pl.lit(PersonType.FULL_TIME_WORKER.value))
        # Age >= 16 and not full-time employed:
        .when(
            (pl.col("pagey") < AgeThreshold.YOUNG_ADULT)  # 16-17
            & (
                pl.col("student").is_in(
                    [
                        Student.FULLTIME_INPERSON.value,
                        Student.PARTTIME_INPERSON.value,
                        Student.PARTTIME_ONLINE.value,
                        Student.FULLTIME_ONLINE.value,
                    ]
                )
            )
        )
        .then(pl.lit(PersonType.HIGH_SCHOOL_STUDENT.value))
        .when(
            (pl.col("pagey") < AgeThreshold.ADULT)  # 18-24
            & (
                pl.col("school_type").is_in(
                    [
                        SchoolType.HOME_SCHOOL.value,
                        SchoolType.HIGH_SCHOOL.value,
                    ]
                )
            )
            & (
                pl.col("student").is_in(
                    [
                        Student.FULLTIME_INPERSON.value,
                        Student.PARTTIME_INPERSON.value,
                        Student.PARTTIME_ONLINE.value,
                        Student.FULLTIME_ONLINE.value,
                    ]
                )
            )
        )
        .then(pl.lit(PersonType.HIGH_SCHOOL_STUDENT.value))
        # Age >= 18:
        .when(
            pl.col("student").is_in(
                [
                    Student.FULLTIME_INPERSON.value,
                    Student.PARTTIME_INPERSON.value,
                    Student.PARTTIME_ONLINE.value,
                    Student.FULLTIME_ONLINE.value,
                ]
            )
        )
        .then(pl.lit(PersonType.UNIVERSITY_STUDENT.value))
        .when(
            pl.col("employment").is_in(
                [
                    Employment.EMPLOYED_PARTTIME.value,
                    Employment.EMPLOYED_SELF.value,
                    Employment.EMPLOYED_UNPAID.value,
                ]
            )
        )
        .then(pl.lit(PersonType.PART_TIME_WORKER.value))
        .when(pl.col("pagey") < AgeThreshold.SENIOR)
        .then(pl.lit(PersonType.NON_WORKER.value))
        .otherwise(pl.lit(PersonType.RETIRED.value))
    )

    # Derive worker type (pwtyp) from person type and employment
    persons_daysim = persons_daysim.with_columns(
        pwtyp=pl.when(
            pl.col("pptyp").is_in(
                [
                    PersonType.FULL_TIME_WORKER.value,
                    PersonType.PART_TIME_WORKER.value,
                ]
            )
        )
        .then(pl.col("pptyp"))  # direct mapping for workers
        .when(
            pl.col("pptyp").is_in(
                [
                    PersonType.UNIVERSITY_STUDENT.value,
                    PersonType.HIGH_SCHOOL_STUDENT.value,
                ]
            )
            & pl.col("employment").is_in(
                [
                    Employment.EMPLOYED_FULLTIME.value,
                    Employment.EMPLOYED_PARTTIME.value,
                    Employment.EMPLOYED_SELF.value,
                ]
            )
        )
        .then(pl.lit(PersonType.PART_TIME_WORKER.value))
        .otherwise(pl.lit(0))  # non-worker
    )

    # Set work/school locations to -1 if person is not worker/student
    persons_daysim = persons_daysim.with_columns(
        pwtaz=pl.when(pl.col("pwtyp") != 0)
        .then(pl.col("pwtaz"))
        .otherwise(pl.lit(-1)),
        pwpcl=pl.when(pl.col("pwtyp") != 0)
        .then(pl.col("pwpcl"))
        .otherwise(pl.lit(-1)),
        pwxcord=pl.when(pl.col("pwtyp") != 0)
        .then(pl.col("pwxcord"))
        .otherwise(pl.lit(-1)),
        pwycord=pl.when(pl.col("pwtyp") != 0)
        .then(pl.col("pwycord"))
        .otherwise(pl.lit(-1)),
        pstaz=pl.when(pl.col("pstyp") != 0)
        .then(pl.col("pstaz"))
        .otherwise(pl.lit(-1)),
        pspcl=pl.when(pl.col("pstyp") != 0)
        .then(pl.col("pspcl"))
        .otherwise(pl.lit(-1)),
        psxcord=pl.when(pl.col("pstyp") != 0)
        .then(pl.col("psxcord"))
        .otherwise(pl.lit(-1)),
        psycord=pl.when(pl.col("pstyp") != 0)
        .then(pl.col("psycord"))
        .otherwise(pl.lit(-1)),
    )

    # Add default expansion factor
    persons_daysim = persons_daysim.with_columns(
        psexpfac=pl.lit(1.0),
        pwautime=pl.lit(-1),  # auto time to work (not available)
    )

    # Join day completeness if available
    if day_completeness is not None:
        persons_daysim = persons_daysim.join(
            day_completeness, on=["hhno", "pno"], how="left"
        )

    # Select DaySim person fields
    person_cols = [
        "hhno",
        "pno",
        "pptyp",
        "pagey",
        "pgend",
        "pwtyp",
        "pwpcl",
        "pwtaz",
        "pstyp",
        "pspcl",
        "pstaz",
        "ppaidprk",
        "pwautime",
        "pwxcord",
        "pwycord",
        "psxcord",
        "psycord",
        "psexpfac",
    ]

    # Add day completeness columns if available
    if day_completeness is not None:
        person_cols.extend(
            [
                "mon_complete",
                "tue_complete",
                "wed_complete",
                "thu_complete",
                "fri_complete",
                "sat_complete",
                "sun_complete",
                "num_days_complete_3dayweekday",
                "num_days_complete_4dayweekday",
                "num_days_complete_5dayweekday",
            ]
        )

    return persons_daysim.select(person_cols).sort(by=["hhno", "pno"])
