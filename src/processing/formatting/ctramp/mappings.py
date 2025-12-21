"""Mapping dictionaries for CT-RAMP formatting.

This module contains lookup tables and mappings to transform canonical
survey data into CT-RAMP model format. Mappings include:
- Income category to midpoint value conversions
- Person type classification logic
- Activity pattern codes
"""

from data_canon.codebook.households import IncomeDetailed, IncomeFollowup
from data_canon.codebook.persons import Employment, Gender, SchoolType, Student
from utils.helpers import get_income_midpoint

from .ctramp_config import CTRAMPConfig


def get_income_detailed_midpoint(config: CTRAMPConfig) -> dict[int, int]:
    """Get income detailed midpoint mapping using config parameters.

    Args:
        config: CT-RAMP configuration with income edge case values

    Returns:
        Dictionary mapping IncomeDetailed enum values to midpoint dollars
    """
    midpoint_map = {}
    for income_cat in IncomeDetailed:
        try:
            midpoint = get_income_midpoint(
                income_cat,
                config.income_under_minimum,
                config.income_top_category,
            )
            midpoint_map[income_cat.value] = midpoint
        except ValueError:
            # PNTA and other special cases
            midpoint_map[income_cat.value] = -1

    # Handle missing/unknown
    midpoint_map[-1] = -1
    return midpoint_map


def get_income_followup_midpoint(config: CTRAMPConfig) -> dict[int, int]:
    """Get income followup midpoint mapping using config parameters.

    Args:
        config: CT-RAMP configuration with income edge case values

    Returns:
        Dictionary mapping IncomeFollowup enum values to midpoint dollars
    """
    midpoint_map = {}
    for income_cat in IncomeFollowup:
        try:
            midpoint = get_income_midpoint(
                income_cat,
                config.income_under_minimum,
                config.income_top_category,
            )
            midpoint_map[income_cat.value] = midpoint
        except ValueError:
            # PNTA, Missing, and other special cases
            midpoint_map[income_cat.value] = -1

    # Handle missing/unknown
    midpoint_map[-1] = -1
    return midpoint_map


def get_gender_map(config: CTRAMPConfig) -> dict[int, str]:
    """Get gender mapping using config parameter for missing/non-binary.

    Args:
        config: CT-RAMP configuration with gender_default_for_missing

    Returns:
        Dictionary mapping Gender enum values to CTRAMP gender strings
    """
    default_gender = config.gender_default_for_missing
    return {
        Gender.MALE.value: "m",
        Gender.FEMALE.value: "f",
        Gender.NON_BINARY.value: default_gender,
        Gender.OTHER.value: default_gender,
        Gender.PNTA.value: default_gender,
        -1: default_gender,  # Missing
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
