"""Person formatting for CT-RAMP output.

Transforms canonical person data into CT-RAMP model format, including:
- Person type classification based on age, employment, and student status
- Activity pattern determination (Mandatory/Non-mandatory/Home)
- Free parking choice based on commute subsidies
- Tour frequency placeholder values

Note: Some fields like activity_pattern, imf_choice, and inmf_choice require
tour data and are set to placeholder values. These would be populated from
actual tour extraction results in a full pipeline.
"""

import logging

import polars as pl

from data_canon.codebook.ctramp import (
    FreeParkingChoice,
)
from data_canon.codebook.ctramp import (
    PersonType as CTRAMPPersonType,
)
from data_canon.codebook.persons import CommuteSubsidy

from .mappings import (
    EMPLOYMENT_MAP,
    GENDER_MAP,
    SCHOOL_TYPE_MAP,
    STUDENT_MAP,
    AgeThreshold,
)

logger = logging.getLogger(__name__)


def classify_person_type(
    age: int, employment: int, student: int, school_type: int
) -> int:
    """Classify person type based on age, employment, and student status.

    CT-RAMP person type classification:
    1. Full-time worker
    2. Part-time worker
    3. University student
    4. Nonworker
    5. Retired
    6. Student of non-driving age
    7. Student of driving age
    8. Child too young for school

    Args:
        age: Person age
        employment: Employment status code
        student: Student status code
        school_type: School type code

    Returns:
        int:
    """
    # Map to intermediate categories
    emp_status = EMPLOYMENT_MAP.get(employment, "not_employed")
    is_student = STUDENT_MAP.get(student, "not_student") == "student"
    school_cat = SCHOOL_TYPE_MAP.get(school_type, "not_student")

    # Classification logic based on age ranges
    if age < AgeThreshold.PRESCHOOL:
        person_type = CTRAMPPersonType.CHILD_TOO_YOUNG.value
    elif age < AgeThreshold.ELEMENTARY:
        # 5-15 years old
        person_type = (
            CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value
            if is_student
            else CTRAMPPersonType.CHILD_TOO_YOUNG.value
        )
    elif age < AgeThreshold.DRIVING_AGE:
        # 16-17 years old
        person_type = (
            CTRAMPPersonType.STUDENT_DRIVING_AGE.value
            if is_student
            else CTRAMPPersonType.NONWORKER.value
        )
    elif emp_status == "full_time":
        # 18+ years old with employment
        person_type = CTRAMPPersonType.FULL_TIME_WORKER.value
    elif emp_status == "part_time":
        person_type = CTRAMPPersonType.PART_TIME_WORKER.value
    elif age >= AgeThreshold.RETIREMENT:
        # Not employed, retirement age
        person_type = CTRAMPPersonType.RETIRED.value
    elif school_cat == "college":
        # Not employed, college student
        person_type = CTRAMPPersonType.UNIVERSITY_STUDENT.value
    else:
        # Not employed, not student, not retired
        person_type = CTRAMPPersonType.NONWORKER.value

    return person_type


def determine_free_parking_eligibility(commute_subsidy: int) -> int:
    """Determine free parking eligibility from commute subsidy.

    Args:
        commute_subsidy: Commute subsidy code

    Returns:
        Free parking choice code (1=free, 2=pay)
    """
    if commute_subsidy in [
        CommuteSubsidy.FREE_PARK.value,
        CommuteSubsidy.DISCOUNT_PARKING.value,
    ]:
        return FreeParkingChoice.PARK_FOR_FREE.value
    return FreeParkingChoice.PAY_TO_PARK.value


def format_persons(
    persons: pl.DataFrame,
) -> pl.DataFrame:
    """Format person data to CT-RAMP specification.

    Transforms person data from canonical format to CT-RAMP format.
    Key transformations:
    - Classify person type based on age, employment, student status
    - Map gender to m/f format
    - Determine free parking eligibility
    - Set placeholder values for activity patterns and tour frequencies

    Args:
        persons: DataFrame with canonical person fields including:
            - person_id: Unique person ID
            - hh_id: Household ID
            - person_num: Person number within household
            - age: Person age
            - gender: Gender code
            - employment: Employment status
            - student: Student status
            - school_type: Type of school attending
            - commute_subsidy: Commute subsidy type
            - value_of_time: Value of time ($/hour)

    Returns:
        DataFrame with CT-RAMP person fields:
        - hh_id: Household ID
        - person_id: Person ID
        - person_num: Person number
        - age: Person age
        - gender: Gender (m/f)
        - type: Person type (1-8)
        - value_of_time: Value of time ($2000/hour)
        - fp_choice: Free parking choice (1/2)
        - activity_pattern: Daily activity pattern (M/N/H)
        - imf_choice: Individual mandatory tour frequency
        - inmf_choice: Individual non-mandatory tour frequency
        - wfh_choice: Work from home choice (0/1)

    Notes:
        - activity_pattern set to 'H' (home) as placeholder
        - imf_choice set to 0 (no mandatory tours) as placeholder
        - inmf_choice set to 1 (minimum valid value) as placeholder
        - wfh_choice set to 0 (no work from home) as placeholder
        - These would be populated from tour data in full pipeline
    """
    logger.info("Formatting person data for CT-RAMP")

    # Apply person type classification
    persons_ctramp = persons.with_columns(
        pl.struct(["age", "employment", "student", "school_type"])
        .map_elements(
            lambda x: classify_person_type(
                x["age"], x["employment"], x["student"], x["school_type"]
            ),
            return_dtype=pl.Int64,
        )
        .alias("type")
    )

    # Map gender (convert int enum to string "m"/"f")
    persons_ctramp = persons_ctramp.with_columns(
        pl.col("gender")
        .fill_null(-1)
        .replace_strict(GENDER_MAP)
        .alias("gender")
    )

    # Determine free parking eligibility
    persons_ctramp = persons_ctramp.with_columns(
        pl.col("commute_subsidy")
        .fill_null(-1)
        .map_elements(
            determine_free_parking_eligibility,
            return_dtype=pl.Int64,
        )
        .alias("fp_choice")
    )

    # Add placeholder fields for activity patterns and tour frequencies
    # These would normally be derived from tour extraction
    persons_ctramp = persons_ctramp.with_columns(
        # Activity pattern: H=home (placeholder for persons with no tours)
        activity_pattern=pl.lit("H"),
        # Individual mandatory tour frequency: 0 (placeholder)
        imf_choice=pl.lit(0),
        # Individual non-mandatory tour frequency: 1 (minimum valid)
        inmf_choice=pl.lit(1),
        # Work from home choice: 0 (no)
        wfh_choice=pl.lit(0),
    )

    # Ensure value_of_time exists and has default if missing
    if "value_of_time" not in persons_ctramp.columns:
        persons_ctramp = persons_ctramp.with_columns(
            value_of_time=pl.lit(15.0)  # Default $15/hour
        )

    # Select final columns in CT-RAMP order
    persons_ctramp = persons_ctramp.select(
        [
            "hh_id",
            "person_id",
            "person_num",
            "age",
            "gender",
            "type",
            "value_of_time",
            "fp_choice",
            "activity_pattern",
            "imf_choice",
            "inmf_choice",
            "wfh_choice",
        ]
    )

    logger.info("Formatted %d persons for CT-RAMP output", len(persons_ctramp))

    return persons_ctramp
