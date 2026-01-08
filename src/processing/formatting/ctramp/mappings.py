"""Mapping dictionaries for CT-RAMP formatting.

This module contains lookup tables and mappings to transform canonical
survey data into CT-RAMP model format.
"""

import polars as pl

from data_canon.codebook.ctramp import CTRAMPMode, CTRAMPPersonType
from data_canon.codebook.persons import Employment, Gender, SchoolType, Student
from data_canon.codebook.persons import PersonType as CanonicalPersonType
from data_canon.codebook.trips import ModeType, Purpose, PurposeCategory

# Canonical PersonType to CT-RAMP PersonType mapping
PERSON_TYPE_TO_CTRAMP = {}
for c in CanonicalPersonType:
    if hasattr(CTRAMPPersonType, c.name):
        PERSON_TYPE_TO_CTRAMP[c.value] = getattr(CTRAMPPersonType, c.name).value
    else:
        msg = f"No matching CT-RAMP PersonType for {c.name}"
        raise ValueError(msg)


GENDER_MAP = {
    Gender.MALE.value: "m",
    Gender.FEMALE.value: "f",
    # Only 2 genders coded in CT-RAMP. All else get mapped to default.
    # Gender.NON_BINARY.value: ?...,
    # Gender.OTHER.value: ?...,
    # Gender.PNTA.value: ?...,
    # -1: ?...,
}

# Employment to person type component
EMPLOYMENT_MAP = {
    Employment.EMPLOYED_FULLTIME.value: "full_time",
    Employment.EMPLOYED_PARTTIME.value: "part_time",
    Employment.UNEMPLOYED_NOT_LOOKING.value: "not_employed",
    Employment.MISSING.value: "not_employed",
    -1: "not_employed",
}

# Student to person type component
STUDENT_MAP = {
    Student.NONSTUDENT.value: "not_student",
    Student.FULLTIME_INPERSON.value: "student",
    Student.PARTTIME_INPERSON.value: "student",
    Student.FULLTIME_ONLINE.value: "student",
    Student.PARTTIME_ONLINE.value: "student",
    Student.MISSING.value: "not_student",
    -1: "not_student",
}

# School type to student category
SCHOOL_TYPE_MAP = {
    SchoolType.PRESCHOOL.value: "not_student",
    SchoolType.ELEMENTARY.value: "grade_school",
    SchoolType.MIDDLE_SCHOOL.value: "grade_school",
    SchoolType.HIGH_SCHOOL.value: "high_school",
    SchoolType.VOCATIONAL.value: "college",
    SchoolType.COLLEGE_2YEAR.value: "college",
    SchoolType.COLLEGE_4YEAR.value: "college",
    SchoolType.GRADUATE_SCHOOL.value: "college",
    SchoolType.HOME_SCHOOL.value: "grade_school",
    SchoolType.OTHER.value: "not_student",
    SchoolType.MISSING.value: "not_student",
    -1: "not_student",
}

# PurposeCategory to Joint Tour Frequency (JTF) group mapping
# Maps canonical tour purposes to JTF category strings used for joint tour classification
# This is an internal mapping used in CTRAMP processing to get joint tour frequencies
# based on tour purpose categories.
PURPOSECATEGORY_TO_JTF_GROUP = {
    # Shopping
    PurposeCategory.SHOP.value: "S",
    # Maintenance/errands
    PurposeCategory.ERRAND.value: "M",
    # Eating out
    PurposeCategory.MEAL.value: "E",
    # Visiting/social/recreational
    PurposeCategory.SOCIALREC.value: "V",
    # Discretionary - Work/School (typically not joint, but possible)
    PurposeCategory.WORK.value: "D",
    PurposeCategory.WORK_RELATED.value: "D",
    PurposeCategory.SCHOOL.value: "D",
    PurposeCategory.SCHOOL_RELATED.value: "D",
    # Discretionary - Escort
    PurposeCategory.ESCORT.value: "D",
    # Discretionary - Other activities
    PurposeCategory.OTHER.value: "D",
    # Discretionary - Home/overnight (not typical joint tour destinations)
    PurposeCategory.HOME.value: "D",
    PurposeCategory.OVERNIGHT.value: "D",
    # Discretionary - Mode change (transfer point, not a tour purpose)
    PurposeCategory.CHANGE_MODE.value: "D",
    # Discretionary - Data quality issues
    PurposeCategory.MISSING.value: "D",
    PurposeCategory.PNTA.value: "D",
    PurposeCategory.NOT_IMPUTABLE.value: "D",
}


def map_purpose_to_ctramp(
    purpose: pl.Expr,
    income: pl.Expr,
    school_type: pl.Expr,
    income_low_threshold: int,
    income_med_threshold: int,
    income_high_threshold: int,
) -> pl.Expr:
    """Map canonical trip purpose to CTRAMP purpose string.

    CTRAMP requires detailed purpose strings that distinguish work income
    levels (low/med/high/very high) and school types (grade/high/university).

    Args:
        purpose: Polars expression for canonical purpose
            (from trips.Purpose enum)
        income: Polars expression for household income (absolute dollars)
        school_type: Polars expression for school type
            (from persons.SchoolType enum):
              - K12: 5-7
              - College/Grad: 11-13
              - Not student/Missing: other values
        income_low_threshold: Income threshold for low bracket
        income_med_threshold: Income threshold for med bracket
        income_high_threshold: Income threshold for high bracket

    Returns:
        Polars expression resolving to CTRAMP purpose string
    """
    # Compute student category from student and school_type enums
    # College/grad -> "College or higher"
    # K-12 -> "Grade or high school"
    # Not student or missing -> "Not student"
    student_category = (
        pl.when(
            school_type.is_in(
                [
                    SchoolType.COLLEGE_2YEAR.value,
                    SchoolType.COLLEGE_4YEAR.value,
                    SchoolType.GRADUATE_SCHOOL.value,
                ]
            )
        )
        .then(pl.lit("College or higher"))
        .when(
            school_type.is_in(
                [
                    SchoolType.ELEMENTARY.value,
                    SchoolType.MIDDLE_SCHOOL.value,
                    SchoolType.HIGH_SCHOOL.value,
                ]
            )
        )
        .then(pl.lit("Grade or high school"))
        .otherwise(pl.lit("Not student"))
    )
    # Home purpose
    home_expr = pl.when(purpose == Purpose.HOME.value).then(pl.lit("Home"))

    # Work purposes - segmented by income
    work_purposes = [
        Purpose.PRIMARY_WORKPLACE.value,
        Purpose.WORK_ACTIVITY.value,
    ]
    work_income_segmentation = (
        pl.when(income < income_low_threshold)
        .then(pl.lit("work_low"))
        .when(income < income_med_threshold)
        .then(pl.lit("work_med"))
        .when(income < income_high_threshold)
        .then(pl.lit("work_high"))
        .otherwise(pl.lit("work_very high"))
    )
    work_expr = home_expr.when(purpose.is_in(work_purposes)).then(work_income_segmentation)

    # School purposes - segmented by student type
    k12_purposes = [Purpose.K12_SCHOOL.value, Purpose.DAYCARE.value, Purpose.SCHOOL.value]
    school_segmentation_expr = (
        pl.when(student_category == "College or higher")
        .then(pl.lit("university"))
        .when(student_category == "Grade or high school")
        .then(pl.lit("school_high"))
        .otherwise(pl.lit("school_grade"))
    )
    school_expr = work_expr.when(purpose.is_in(k12_purposes)).then(school_segmentation_expr)
    university_expr = school_expr.when(purpose == Purpose.COLLEGE.value).then(pl.lit("university"))

    # At-work sub-tour purposes
    atwork_expr = university_expr.when(purpose == Purpose.WORK_ACTIVITY.value).then(
        pl.lit("atwork_business")
    )
    eatout_expr = atwork_expr.when(purpose == Purpose.DINING.value).then(pl.lit("eatout"))

    # Escort purposes
    escort_purposes = [
        Purpose.DROP_OFF.value,
        Purpose.PICK_UP.value,
        Purpose.ACCOMPANY.value,
    ]
    escort_segmentation_expr = (
        pl.when(student_category.is_in(["College or higher", "Grade or high school"]))
        .then(pl.lit("escort_kids"))
        .otherwise(pl.lit("escort_no kids"))
    )
    escort_expr = eatout_expr.when(purpose.is_in(escort_purposes)).then(escort_segmentation_expr)

    # Shopping
    shopping_purposes = [
        Purpose.GROCERY.value,
        Purpose.ROUTINE_SHOPPING.value,
        Purpose.MAJOR_SHOPPING.value,
        Purpose.SHOPPING_ERRANDS.value,
    ]
    shopping_expr = escort_expr.when(purpose.is_in(shopping_purposes)).then(pl.lit("shopping"))

    # Social/recreation
    social_purposes = [
        Purpose.SOCIAL.value,
        Purpose.ENTERTAINMENT.value,
        Purpose.EXERCISE.value,
    ]
    social_expr = shopping_expr.when(purpose.is_in(social_purposes)).then(pl.lit("social"))

    # Maintenance/errands
    maintenance_purposes = [
        Purpose.MEDICAL.value,
        Purpose.ERRAND_NO_APPT.value,
        Purpose.ERRAND_WITH_APPT.value,
    ]
    maintenance_expr = social_expr.when(purpose.is_in(maintenance_purposes)).then(
        pl.lit("othmaint")
    )

    # Discretionary
    discretionary_purposes = [
        Purpose.RELIGIOUS_CIVIC.value,
        Purpose.FAMILY_ACTIVITY.value,
    ]
    discretionary_expr = maintenance_expr.when(purpose.is_in(discretionary_purposes)).then(
        pl.lit("othdiscr")
    )

    # Default fallback
    return discretionary_expr.otherwise(pl.lit("othdiscr"))


def map_purpose_category_to_ctramp(
    purpose_category: pl.Expr,
    income: pl.Expr,
    school_type: pl.Expr,
    income_low_threshold: int,
    income_med_threshold: int,
    income_high_threshold: int,
) -> pl.Expr:
    """Map canonical PurposeCategory to CTRAMP purpose string.

    CTRAMP requires detailed purpose strings that distinguish work income
    levels (low/med/high/very high) and school types (grade/high/university).

    Args:
        purpose_category: Polars expression for canonical purpose category
            (from trips.PurposeCategory enum)
        income: Polars expression for household income (absolute dollars)
        school_type: Polars expression for school type
            (from persons.SchoolType enum)
        income_low_threshold: Income threshold for low bracket
        income_med_threshold: Income threshold for med bracket
        income_high_threshold: Income threshold for high bracket

    Returns:
        Polars expression resolving to CTRAMP purpose string
    """
    # Compute student category from school_type enum
    student_category = (
        pl.when(
            school_type.is_in(
                [
                    SchoolType.COLLEGE_2YEAR.value,
                    SchoolType.COLLEGE_4YEAR.value,
                    SchoolType.GRADUATE_SCHOOL.value,
                ]
            )
        )
        .then(pl.lit("College or higher"))
        .when(
            school_type.is_in(
                [
                    SchoolType.ELEMENTARY.value,
                    SchoolType.MIDDLE_SCHOOL.value,
                    SchoolType.HIGH_SCHOOL.value,
                ]
            )
        )
        .then(pl.lit("Grade or high school"))
        .otherwise(pl.lit("Not student"))
    )

    # Home purpose
    home_expr = pl.when(purpose_category == PurposeCategory.HOME.value).then(pl.lit("Home"))

    # Work purposes - segmented by income
    work_income_segmentation = (
        pl.when(income < income_low_threshold)
        .then(pl.lit("work_low"))
        .when(income < income_med_threshold)
        .then(pl.lit("work_med"))
        .when(income < income_high_threshold)
        .then(pl.lit("work_high"))
        .otherwise(pl.lit("work_very high"))
    )
    work_expr = home_expr.when(
        purpose_category.is_in([PurposeCategory.WORK.value, PurposeCategory.WORK_RELATED.value])
    ).then(work_income_segmentation)

    # School purposes - segmented by student type
    school_segmentation_expr = (
        pl.when(student_category == "College or higher")
        .then(pl.lit("university"))
        .when(student_category == "Grade or high school")
        .then(pl.lit("school_high"))
        .otherwise(pl.lit("school_grade"))
    )
    school_expr = work_expr.when(
        purpose_category.is_in([PurposeCategory.SCHOOL.value, PurposeCategory.SCHOOL_RELATED.value])
    ).then(school_segmentation_expr)

    # At-work sub-tour (work-related)
    atwork_expr = school_expr.when(purpose_category == PurposeCategory.WORK_RELATED.value).then(
        pl.lit("atwork_business")
    )

    # Eating out
    eatout_expr = atwork_expr.when(purpose_category == PurposeCategory.MEAL.value).then(
        pl.lit("eatout")
    )

    # Escort
    escort_segmentation_expr = (
        pl.when(student_category.is_in(["College or higher", "Grade or high school"]))
        .then(pl.lit("escort_kids"))
        .otherwise(pl.lit("escort_no kids"))
    )
    escort_expr = eatout_expr.when(purpose_category == PurposeCategory.ESCORT.value).then(
        escort_segmentation_expr
    )

    # Shopping
    shopping_expr = escort_expr.when(purpose_category == PurposeCategory.SHOP.value).then(
        pl.lit("shopping")
    )

    # Social/recreation
    social_expr = shopping_expr.when(purpose_category == PurposeCategory.SOCIALREC.value).then(
        pl.lit("social")
    )

    # Maintenance/errands
    maintenance_expr = social_expr.when(purpose_category == PurposeCategory.ERRAND.value).then(
        pl.lit("othmaint")
    )

    # Discretionary - all others
    return maintenance_expr.otherwise(pl.lit("othdiscr"))


def map_mode_to_ctramp(mode_type: pl.Expr, num_travelers: pl.Expr) -> pl.Expr:
    """Map canonical mode_type to CTRAMP mode integer code.

    Args:
        mode_type: Polars expression for canonical mode_type
            (from ModeType enum)
        num_travelers: Polars expression for number of travelers in vehicle

    Returns:
        Polars expression resolving to CTRAMP mode integer code
    """
    # Walk mode
    walk_expr = pl.when(mode_type == ModeType.WALK.value).then(pl.lit(CTRAMPMode.WALK.value))

    # Bike and micromobility modes
    bike_modes = [
        ModeType.BIKE.value,
        ModeType.BIKESHARE.value,
        ModeType.SCOOTERSHARE.value,
    ]
    bike_expr = walk_expr.when(mode_type.is_in(bike_modes)).then(pl.lit(CTRAMPMode.BIKE.value))

    # School bus
    school_bus_expr = bike_expr.when(mode_type == ModeType.SCHOOL_BUS.value).then(
        pl.lit(CTRAMPMode.SCHOOL_BUS.value)
    )

    # Transit modes (default to walk access)
    transit_modes = [ModeType.TRANSIT.value, ModeType.FERRY.value]
    transit_expr = school_bus_expr.when(mode_type.is_in(transit_modes)).then(
        pl.lit(CTRAMPMode.WALK_TRANSIT_WALK.value)
    )

    # Car modes - distinguish by occupancy
    car_modes = [
        ModeType.CAR.value,
        ModeType.CARSHARE.value,
        ModeType.TAXI.value,
        ModeType.TNC.value,
    ]
    car_occupancy_segmentation = (
        pl.when(num_travelers == 1)
        .then(pl.lit(CTRAMPMode.DRIVE_ALONE.value))
        .when(num_travelers == 2)  # noqa: PLR2004
        .then(pl.lit(CTRAMPMode.SHARED_RIDE_2.value))
        .otherwise(pl.lit(CTRAMPMode.SHARED_RIDE_3_PLUS.value))
    )
    car_expr = transit_expr.when(mode_type.is_in(car_modes)).then(car_occupancy_segmentation)

    # Default to drive alone for unknown modes
    return car_expr.otherwise(pl.lit(CTRAMPMode.DRIVE_ALONE.value))


# Validate mapping completeness at module load time
_all_purpose_categories = {pc.value for pc in PurposeCategory}
_mapped_categories = set(PURPOSECATEGORY_TO_JTF_GROUP.keys())
_missing_categories = _all_purpose_categories - _mapped_categories
if _missing_categories:
    msg = f"Missing PurposeCategory mappings in PURPOSECATEGORY_TO_JTF_GROUP: {_missing_categories}"
    raise ValueError(msg)
_duplicate_check = len(PURPOSECATEGORY_TO_JTF_GROUP)
if _duplicate_check != len(_all_purpose_categories):
    msg = "Duplicate keys found in PURPOSECATEGORY_TO_JTF_GROUP mapping"
    raise ValueError(msg)
