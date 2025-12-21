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
from data_canon.codebook.persons import AgeCategory, CommuteSubsidy

from .ctramp_config import CTRAMPConfig
from .mappings import (
    EMPLOYMENT_MAP,
    SCHOOL_TYPE_MAP,
    STUDENT_MAP,
    get_gender_map,
)

logger = logging.getLogger(__name__)


def aggregate_tour_statistics(
    tours: pl.DataFrame,
) -> pl.DataFrame:
    """Aggregate tour statistics by person for activity patterns.

    Computes:
    - activity_pattern: M (mandatory), N (non-mandatory), H (no tours)
    - imf_choice: Count of mandatory tours (work/school)
    - inmf_choice: Count of non-mandatory tours
    - wfh_choice: 1 if person has work-from-home activity, 0 otherwise

    Args:
        tours: DataFrame with tour_purpose field (CTRAMP formatted)

    Returns:
        DataFrame with person_id and aggregated statistics
    """
    # Handle empty tours DataFrame
    if len(tours) == 0:
        return pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "imf_choice": pl.Int64,
                "inmf_choice": pl.Int64,
                "wfh_choice": pl.Int64,
                "activity_pattern": pl.String,
            }
        )

    # Classify tours as mandatory vs non-mandatory
    # Mandatory: work_*, school_*, university
    # Non-mandatory: everything else
    mandatory_purposes = [
        "work_low",
        "work_med",
        "work_high",
        "work_very high",
        "school_grade",
        "school_high",
        "university",
    ]

    tour_stats = tours.with_columns(
        pl.col("tour_purpose").is_in(mandatory_purposes).alias("is_mandatory")
    )

    # Aggregate by person
    person_stats = tour_stats.group_by("person_id").agg(
        [
            # Count mandatory and non-mandatory tours
            pl.col("is_mandatory").sum().alias("imf_choice"),
            (~pl.col("is_mandatory")).sum().alias("inmf_choice"),
            # Check if any work-from-home (no tours with trips/stops)
            # Note: WFH logic would need trip data; for now set to 0
            pl.lit(0).alias("wfh_choice"),
        ]
    )

    # Determine activity pattern
    person_stats = person_stats.with_columns(
        pl.when(pl.col("imf_choice") > 0)
        .then(pl.lit("M"))  # Mandatory
        .when(pl.col("inmf_choice") > 0)
        .then(pl.lit("N"))  # Non-mandatory
        .otherwise(pl.lit("H"))  # Home (no tours)
        .alias("activity_pattern")
    )

    return person_stats.select(
        [
            "person_id",
            "activity_pattern",
            "imf_choice",
            "inmf_choice",
            "wfh_choice",
        ]
    )


def classify_person_type(
    age: int,
    employment: int,
    student: int,
    school_type: int,
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
        age: AgeCategory enum value (1-11)
        employment: Employment status code
        student: Student status code
        school_type: School type code

    Returns:
        int: CT-RAMP person type code

    Note:
        Age is an AgeCategory enum:
        1=UNDER_5, 2=5_TO_15, 3=16_TO_17, 4=18_TO_24, 5=25_TO_34,
        6=35_TO_44, 7=45_TO_54, 8=55_TO_64, 9=65_TO_74, 10=75_TO_84,
        11=85_AND_UP
    """
    # Map to intermediate categories
    emp_status = EMPLOYMENT_MAP.get(employment, "not_employed")
    is_student = STUDENT_MAP.get(student, "not_student") == "student"
    school_cat = SCHOOL_TYPE_MAP.get(school_type, "not_student")

    # Classification based on AgeCategory enum
    if age == AgeCategory.AGE_UNDER_5.value:
        person_type = CTRAMPPersonType.CHILD_TOO_YOUNG.value
    elif age == AgeCategory.AGE_5_TO_15.value:
        person_type = (
            CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value
            if is_student
            else CTRAMPPersonType.NONWORKER.value
        )
    elif age == AgeCategory.AGE_16_TO_17.value:
        if is_student:
            person_type = CTRAMPPersonType.STUDENT_DRIVING_AGE.value
        else:
            person_type = CTRAMPPersonType.NONWORKER.value
    elif emp_status == "full_time":
        person_type = CTRAMPPersonType.FULL_TIME_WORKER.value
    elif emp_status == "part_time":
        person_type = CTRAMPPersonType.PART_TIME_WORKER.value
    elif is_student and school_cat in ("elementary", "high_school"):
        person_type = CTRAMPPersonType.STUDENT_DRIVING_AGE.value
    elif age >= AgeCategory.AGE_65_TO_74.value:  # Retired
        person_type = CTRAMPPersonType.RETIRED.value
    elif school_cat == "college":
        person_type = CTRAMPPersonType.UNIVERSITY_STUDENT.value
    else:
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
    tours: pl.DataFrame,
    config: CTRAMPConfig,
) -> pl.DataFrame:
    """Format person data to CT-RAMP specification.

    Transforms person data from canonical format to CT-RAMP format.
    Key transformations:
    - Classify person type based on age, employment, student status
    - Map gender to m/f format
    - Determine free parking eligibility
    - Aggregate activity patterns and tour frequencies from tour data

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
        tours: DataFrame with formatted tour data including tour_purpose_ctramp
        config: CT-RAMP configuration with age thresholds

    Returns:
        DataFrame with CT-RAMP person fields:
        - hh_id: Household ID
        - person_id: Person ID
        - person_num: Person number
        - age: Person age
        - gender: Gender (m/f)
        - type: Person type (1-8)
        - value_of_time: Value of time ($/hour)
        - fp_choice: Free parking choice (1/2)
        - activity_pattern: Daily activity pattern (M/N/H)
        - imf_choice: Individual mandatory tour frequency
        - inmf_choice: Individual non-mandatory tour frequency
        - wfh_choice: Work from home choice (0/1)

    Notes:
        - activity_pattern: M=mandatory tours, N=non-mandatory only, H=no tours
        - imf_choice: Count of mandatory tours (work/school)
        - inmf_choice: Count of non-mandatory tours
        - wfh_choice: Work from home indicator (currently always 0)
    """
    logger.info("Formatting person data for CT-RAMP")

    # Apply person type classification
    persons_ctramp = persons.with_columns(
        pl.struct(["age", "employment", "student", "school_type"])
        .map_elements(
            lambda x: classify_person_type(
                x["age"],
                x["employment"],
                x["student"],
                x["school_type"],
            ),
            return_dtype=pl.Int64,
        )
        .alias("type")
    )

    # Map gender (convert int enum to string "m"/"f")
    gender_map = get_gender_map(config)
    persons_ctramp = persons_ctramp.with_columns(
        pl.col("gender")
        .fill_null(-1)
        .replace_strict(gender_map)
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

    # Aggregate tour statistics from tour data
    tour_stats = aggregate_tour_statistics(tours)

    # Join tour statistics, filling with defaults for persons with no tours
    persons_ctramp = persons_ctramp.join(
        tour_stats, on="person_id", how="left"
    ).with_columns(
        [
            pl.col("activity_pattern").fill_null("H"),  # Home if no tours
            pl.col("imf_choice").fill_null(0),  # 0 mandatory tours
            pl.col("inmf_choice").fill_null(0),  # 0 non-mandatory tours
            pl.col("wfh_choice").fill_null(0),  # No work from home
        ]
    )

    # Note: value_of_time is model output, not survey data
    # If it exists in the input, keep it; otherwise it will be null

    output_cols = [
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

    # if value_of_time is not in input, drop
    if "value_of_time" not in persons_ctramp.columns:
        output_cols.pop("value_of_time")

    # Select final columns in CT-RAMP order
    persons_ctramp = persons_ctramp.select(output_cols)

    logger.info("Formatted %d persons for CT-RAMP output", len(persons_ctramp))

    return persons_ctramp
