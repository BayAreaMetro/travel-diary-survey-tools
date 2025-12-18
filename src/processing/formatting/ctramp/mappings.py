"""Mapping dictionaries for CT-RAMP formatting.

This module contains lookup tables and mappings to transform canonical
survey data into CT-RAMP model format. Mappings include:
- Income category to midpoint value conversions
- Person type classification logic
- Age thresholds for categorization
- Activity pattern codes
"""

from data_canon.codebook.households import IncomeDetailed, IncomeFollowup
from data_canon.codebook.persons import Employment, Gender, SchoolType, Student

# Income category to midpoint mapping (in $2000)
# CT-RAMP uses income in year 2000 dollars
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
    IncomeDetailed.PNTA.value: -1,  # Prefer not to answer
    -1: -1,  # Missing/unknown
}

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

# Gender mapping: canonical -> CTRAMP (m/f)
GENDER_MAP = {
    Gender.MALE.value: "m",
    Gender.FEMALE.value: "f",
    # NOTE: Need to fix this!!! default to female if non-binary/other
    Gender.NON_BINARY.value: "f",
    Gender.OTHER.value: "f",
    Gender.PNTA.value: "f",
    -1: "f",  # Default to female for missing
}


# Age thresholds for person type classification
class AgeThreshold:
    """Age thresholds for CT-RAMP person type classification."""

    PRESCHOOL = 5  # Under 5 = too young for school
    ELEMENTARY = 16  # 5-15 = elementary/middle school age
    DRIVING_AGE = 18  # 16-17 = driving age students
    RETIREMENT = 65  # 65+ with no employment = retired


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
