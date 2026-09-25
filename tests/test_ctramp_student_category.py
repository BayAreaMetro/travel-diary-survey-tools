"""Unit tests for CT-RAMP student category classification.

Tests the three-tier logic for deriving student_category:
1. Valid student status + valid school type -> map by school level
2. Missing data -> age-based fallback for children 5-17
3. Catch-all -> NOT_STUDENT

Note: These tests use AgeCategory enum values (e.g., AGE_5_TO_15=2), not continuous
ages, since format_persons.py computes student_category BEFORE converting age to continuous.

Every case here sets ``school_taz=0``, so the school-location rules -- the
highest-priority branch of the expression -- are not exercised by this file.
"""

import polars as pl
import pytest

from data_canon.codebook.ctramp import CTRAMPEmploymentCategory, CTRAMPStudentCategory
from data_canon.codebook.persons import AgeCategory, Employment, SchoolType, Student
from processing.formatting.ctramp.person_mappings import (
    EMPLOYMENT_TO_CTRAMP,
    ctramp_person_type_expression,
)
from processing.formatting.ctramp.student_mappings import (
    ctramp_student_category_expression,
    log_student_category_warnings,
)

GRADE = CTRAMPStudentCategory.GRADE_OR_HIGH_SCHOOL.value
COLLEGE = CTRAMPStudentCategory.COLLEGE_OR_HIGHER.value
NOT_STUDENT = CTRAMPStudentCategory.NOT_STUDENT.value


def _classify(age: AgeCategory, student: Student | None, school_type: SchoolType | None) -> int:
    """Run the student category expression over one person and return the category."""
    df = pl.DataFrame(
        {
            "person_id": [1],
            "age": [age.value],
            "student": [None if student is None else student.value],
            "school_type": [None if school_type is None else school_type.value],
            "school_taz": [0],
        },
        schema_overrides={"student": pl.Int64, "school_type": pl.Int64},
    )
    result = df.with_columns(ctramp_student_category_expression().alias("student_category"))
    return result["student_category"][0]


class TestStudentCategoryClassification:
    """Tests for CTRAMP student category derivation with age-based fallbacks."""

    @pytest.mark.parametrize(
        ("school_type", "expected_category"),
        [
            (SchoolType.ELEMENTARY, GRADE),
            (SchoolType.MIDDLE_SCHOOL, GRADE),
            (SchoolType.HIGH_SCHOOL, GRADE),
            (SchoolType.HOME_SCHOOL, GRADE),
            (SchoolType.COLLEGE_2YEAR, COLLEGE),
            (SchoolType.COLLEGE_4YEAR, COLLEGE),
            (SchoolType.GRADUATE_SCHOOL, COLLEGE),
            (SchoolType.VOCATIONAL, COLLEGE),
            (SchoolType.DAYCARE, NOT_STUDENT),
            (SchoolType.PRESCHOOL, NOT_STUDENT),
            (SchoolType.ATHOME, NOT_STUDENT),
        ],
    )
    def test_valid_student_and_school_type(
        self, school_type: SchoolType, expected_category: int
    ) -> None:
        """Tier 1: a full-time student is placed by the level of their school."""
        actual = _classify(AgeCategory.AGE_18_TO_24, Student.FULLTIME_INPERSON, school_type)
        assert actual == expected_category

    @pytest.mark.parametrize(
        ("age", "expected_category"),
        [
            # Children 5-17 default to GRADE_OR_HIGH_SCHOOL (compulsory education).
            (AgeCategory.AGE_5_TO_15, GRADE),
            (AgeCategory.AGE_16_TO_17, GRADE),
            # Everyone else defaults to NOT_STUDENT: no fallback below 5 or over 17.
            (AgeCategory.AGE_UNDER_5, NOT_STUDENT),
            (AgeCategory.AGE_18_TO_24, NOT_STUDENT),
            (AgeCategory.AGE_25_TO_34, NOT_STUDENT),
            (AgeCategory.AGE_35_TO_44, NOT_STUDENT),
            (AgeCategory.AGE_65_TO_74, NOT_STUDENT),
        ],
    )
    def test_missing_data_age_fallback(self, age: AgeCategory, expected_category: int) -> None:
        """Tier 2: with both fields missing, age alone decides."""
        assert _classify(age, Student.MISSING, SchoolType.MISSING) == expected_category

    @pytest.mark.parametrize(
        ("age", "student", "school_type", "expected_category"),
        [
            # A missing or unusable value in EITHER field sends a child to the
            # age fallback, whatever the other field says. This was the original
            # bug report: children with student=MISSING but a real school_type
            # were coming out NOT_STUDENT.
            (AgeCategory.AGE_5_TO_15, Student.MISSING, SchoolType.ELEMENTARY, GRADE),
            (AgeCategory.AGE_5_TO_15, Student.MISSING, SchoolType.MIDDLE_SCHOOL, GRADE),
            (AgeCategory.AGE_5_TO_15, Student.MISSING, SchoolType.HIGH_SCHOOL, GRADE),
            (AgeCategory.AGE_5_TO_15, Student.FULLTIME_INPERSON, SchoolType.MISSING, GRADE),
            (AgeCategory.AGE_16_TO_17, Student.FULLTIME_INPERSON, SchoolType.MISSING, GRADE),
            # PNTA and OTHER are unusable school types, same as MISSING.
            (AgeCategory.AGE_5_TO_15, Student.NONSTUDENT, SchoolType.PNTA, GRADE),
            (AgeCategory.AGE_5_TO_15, Student.NONSTUDENT, SchoolType.OTHER, GRADE),
            (AgeCategory.AGE_5_TO_15, Student.MISSING, SchoolType.PNTA, GRADE),
            (AgeCategory.AGE_5_TO_15, Student.FULLTIME_INPERSON, SchoolType.OTHER, GRADE),
            # A child reported as a non-student still gets the fallback: the
            # report is contradicted by compulsory education.
            (AgeCategory.AGE_5_TO_15, Student.NONSTUDENT, SchoolType.MISSING, GRADE),
            (AgeCategory.AGE_16_TO_17, Student.NONSTUDENT, SchoolType.MISSING, GRADE),
            (AgeCategory.AGE_5_TO_15, Student.NONSTUDENT, SchoolType.COLLEGE_4YEAR, GRADE),
            # Adults get no fallback. A reported non-student, or one whose
            # status is missing, is NOT_STUDENT...
            (AgeCategory.AGE_25_TO_34, Student.NONSTUDENT, SchoolType.MISSING, NOT_STUDENT),
            (AgeCategory.AGE_25_TO_34, Student.NONSTUDENT, SchoolType.COLLEGE_4YEAR, NOT_STUDENT),
            (AgeCategory.AGE_25_TO_34, Student.MISSING, SchoolType.PNTA, NOT_STUDENT),
            # ...but an adult who IS a student with an unusable school type is
            # assumed to be in college.
            (AgeCategory.AGE_25_TO_34, Student.FULLTIME_INPERSON, SchoolType.OTHER, COLLEGE),
            # Nulls behave as MISSING throughout.
            (AgeCategory.AGE_5_TO_15, None, SchoolType.ELEMENTARY, GRADE),
            (AgeCategory.AGE_25_TO_34, None, SchoolType.MISSING, NOT_STUDENT),
            (AgeCategory.AGE_16_TO_17, Student.FULLTIME_INPERSON, None, GRADE),
            (AgeCategory.AGE_18_TO_24, Student.FULLTIME_INPERSON, None, COLLEGE),
            (AgeCategory.AGE_5_TO_15, None, None, GRADE),
            (AgeCategory.AGE_16_TO_17, None, None, GRADE),
            (AgeCategory.AGE_25_TO_34, None, None, NOT_STUDENT),
        ],
    )
    def test_missing_or_unusable_combinations(
        self,
        age: AgeCategory,
        student: Student | None,
        school_type: SchoolType | None,
        expected_category: int,
    ) -> None:
        """Tier 2 and 3 over the real-world data-quality combinations."""
        assert _classify(age, student, school_type) == expected_category

    def test_valid_data_honored_despite_age_mismatch(self) -> None:
        """Valid data wins over age: a 5-15 year old in college is COLLEGE_OR_HIGHER."""
        df = pl.DataFrame(
            {
                "person_id": [1, 2],
                "age": [AgeCategory.AGE_5_TO_15.value, AgeCategory.AGE_18_TO_24.value],
                "student": [Student.FULLTIME_INPERSON.value, Student.FULLTIME_INPERSON.value],
                "school_type": [SchoolType.COLLEGE_4YEAR.value, SchoolType.ELEMENTARY.value],
                "school_taz": [0, 0],
            }
        )

        result = df.with_columns(ctramp_student_category_expression().alias("student_category"))

        # Valid data should be honored (will trigger warnings but classification is correct)
        assert result["student_category"][0] == COLLEGE
        assert result["student_category"][1] == GRADE


# Column defaults for the warning fixtures; each row overrides what it needs.
_PERSON_DEFAULTS = {
    "employment": Employment.UNEMPLOYED_NOT_LOOKING,
    "student": Student.NONSTUDENT,
    "school_type": SchoolType.MISSING,
    "school_taz": 0,
    "work_taz": 0,
}


def _people(rows: list[dict]) -> pl.DataFrame:
    """Build a persons frame with student_category, employment_category and person_type.

    Derived in the same order as ``format_persons``: the two category columns
    first, then ``person_type`` reading them.
    """
    filled = [
        {
            "person_id": i,
            **{k: getattr(v, "value", v) for k, v in {**_PERSON_DEFAULTS, **row}.items()},
        }
        for i, row in enumerate(rows, start=1)
    ]
    df = pl.DataFrame(filled).with_columns(
        ctramp_student_category_expression().alias("student_category"),
        pl.col("employment")
        .replace_strict(
            EMPLOYMENT_TO_CTRAMP,
            default=CTRAMPEmploymentCategory.NOT_EMPLOYED.value,
        )
        .alias("employment_category"),
    )
    return df.with_columns(ctramp_person_type_expression().alias("person_type"))


class TestStudentCategoryWarnings:
    """``log_student_category_warnings`` counts the data-quality problems it finds.

    The counters are diagnostic -- they never change the output frame -- but each
    is guarded by its own branch, so the cases stay as rows of one table.
    """

    @pytest.mark.parametrize(
        ("rows", "expected_warnings"),
        [
            pytest.param(
                [
                    {"age": AgeCategory.AGE_5_TO_15, "student": Student.MISSING},
                    {"age": AgeCategory.AGE_16_TO_17, "student": Student.MISSING},
                    {"age": AgeCategory.AGE_18_TO_24, "student": Student.MISSING},
                ],
                {"missing_data_used_fallback": 2},
                id="only_children_use_the_age_fallback",
            ),
            pytest.param(
                [
                    {
                        "age": AgeCategory.AGE_UNDER_5,
                        "student": Student.FULLTIME_INPERSON,
                        "school_type": SchoolType.PRESCHOOL,
                    },
                    {
                        "age": AgeCategory.AGE_UNDER_5,
                        "student": Student.PARTTIME_INPERSON,
                        "school_type": SchoolType.DAYCARE,
                    },
                ],
                {"preschool_students": 2},
                id="students_under_five_are_impossible",
            ),
            pytest.param(
                [
                    # Teen in elementary, child in college, adult home-schooled.
                    {
                        "age": AgeCategory.AGE_16_TO_17,
                        "student": Student.FULLTIME_INPERSON,
                        "school_type": SchoolType.ELEMENTARY,
                    },
                    {
                        "age": AgeCategory.AGE_5_TO_15,
                        "student": Student.FULLTIME_INPERSON,
                        "school_type": SchoolType.COLLEGE_4YEAR,
                    },
                    {
                        "age": AgeCategory.AGE_35_TO_44,
                        "student": Student.PARTTIME_INPERSON,
                        "school_type": SchoolType.HOME_SCHOOL,
                    },
                ],
                {"age_inappropriate_school_types": 3},
                id="age_inappropriate_school_types",
            ),
            pytest.param(
                [
                    {
                        "age": AgeCategory.AGE_5_TO_15,
                        "student": Student.FULLTIME_INPERSON,
                        "school_type": SchoolType.ELEMENTARY,
                    },
                    {
                        "age": AgeCategory.AGE_18_TO_24,
                        "student": Student.FULLTIME_INPERSON,
                        "school_type": SchoolType.COLLEGE_4YEAR,
                    },
                    {"age": AgeCategory.AGE_25_TO_34, "student": Student.NONSTUDENT},
                ],
                {},
                id="valid_data_warns_about_nothing",
            ),
            pytest.param(
                [
                    # Full-time employment beats full-time study, so persons 1
                    # and 2 are FULL_TIME_WORKER with a school location but no
                    # work location. Person 3 is a non-student, which the rule
                    # ignores; person 4 has a work location.
                    {
                        "age": AgeCategory.AGE_18_TO_24,
                        "employment": Employment.EMPLOYED_FULLTIME,
                        "student": Student.FULLTIME_INPERSON,
                        "school_type": SchoolType.COLLEGE_4YEAR,
                        "school_taz": 100,
                    },
                    {
                        "age": AgeCategory.AGE_25_TO_34,
                        "employment": Employment.EMPLOYED_FULLTIME,
                        "student": Student.PARTTIME_INPERSON,
                        "school_type": SchoolType.COLLEGE_2YEAR,
                        "school_taz": 150,
                    },
                    {
                        "age": AgeCategory.AGE_18_TO_24,
                        "employment": Employment.EMPLOYED_FULLTIME,
                        "student": Student.NONSTUDENT,
                    },
                    {
                        "age": AgeCategory.AGE_25_TO_34,
                        "employment": Employment.EMPLOYED_FULLTIME,
                        "student": Student.NONSTUDENT,
                        "work_taz": 200,
                    },
                ],
                {"fulltime_workers_no_work_location": 2},
                id="working_students_without_a_work_location",
            ),
        ],
    )
    def test_warning_counts(self, rows, expected_warnings, standard_config) -> None:
        """Each row lists the persons and the exact counters they should produce."""
        warnings = log_student_category_warnings(_people(rows), standard_config)

        assert warnings == expected_warnings
