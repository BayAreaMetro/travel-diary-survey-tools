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


# Deprecated: Use get_income_detailed_midpoint(config) instead
INCOME_DETAILED_TO_MIDPOINT = {
    IncomeDetailed.INCOME_UNDER15.value: 10000,
    IncomeDetailed.INCOME_15TO25.value: 20000,
    IncomeDetailed.INCOME_25TO35.value: 30000,
    IncomeDetailed.INCOME_35TO50.value: 42500,
    IncomeDetailed.INCOME_50TO75.value: 62500,
    IncomeDetailed.INCOME_75TO100.value: 87500,
    IncomeDetailed.INCOME_100TO150.value: 125000,
    IncomeDetailed.INCOME_150TO200.value: 175000,
    IncomeDetailed.INCOME_200TO250.value: 225000,
    IncomeDetailed.INCOME_250_OR_MORE.value: 300000,
    IncomeDetailed.PNTA.value: -1,
    -1: -1,
}

# Deprecated: Use get_income_followup_midpoint(config) instead
INCOME_FOLLOWUP_TO_MIDPOINT = {
    IncomeFollowup.INCOME_UNDER25.value: 12500,
    IncomeFollowup.INCOME_25TO50.value: 37500,
    IncomeFollowup.INCOME_50TO75.value: 62500,
    IncomeFollowup.INCOME_75TO100.value: 87500,
    IncomeFollowup.INCOME_100TO200.value: 150000,
    IncomeFollowup.INCOME_200_OR_MORE.value: 300000,
    IncomeFollowup.MISSING.value: -1,
    IncomeFollowup.PNTA.value: -1,
    -1: -1,
}

# Deprecated: Use get_gender_map(config) instead
GENDER_MAP = {
    Gender.MALE.value: "m",
    Gender.FEMALE.value: "f",
    Gender.NON_BINARY.value: "f",
    Gender.OTHER.value: "f",
    Gender.PNTA.value: "f",
    -1: "f",
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
