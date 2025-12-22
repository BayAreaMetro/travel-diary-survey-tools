"""Mapping dictionaries for CT-RAMP formatting.

This module contains lookup tables and mappings to transform canonical
survey data into CT-RAMP model format.
"""

from data_canon.codebook.ctramp import PersonType as CTRAMPPersonType
from data_canon.codebook.persons import Employment, Gender, SchoolType, Student
from data_canon.codebook.persons import PersonType as CanonicalPersonType

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
