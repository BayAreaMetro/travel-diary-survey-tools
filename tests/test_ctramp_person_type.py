"""Unit tests for CT-RAMP person type classification.

Comprehensive tests using a hybrid approach:
- Critical explicit test cases for known bugs and priority rules
- Property-based tests with Hypothesis for invariant checking
"""

import polars as pl
import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from data_canon.codebook.ctramp import CTRAMPEmploymentCategory, CTRAMPPersonType
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    SchoolType,
    Student,
)
from processing.formatting.ctramp.format_persons import (
    enrich_persons_with_person_type,
)
from processing.formatting.ctramp.person_mappings import (
    EMPLOYMENT_TO_CTRAMP,
    ctramp_person_type_expression,
)
from processing.formatting.ctramp.student_mappings import (
    ctramp_student_category_expression,
)


def _person_type(df: pl.DataFrame) -> int:
    """Classify one person the way ``format_persons`` does.

    The formatter derives ``student_category`` and ``employment_category``
    first, and person type reads those, so each rule lives in one place.
    Classifying straight from the raw columns would exercise a path
    production does not take.
    """
    if "school_taz" not in df.columns:
        df = df.with_columns(pl.lit(0).alias("school_taz"))
    df = df.with_columns(
        ctramp_student_category_expression().alias("student_category"),
        pl.col("employment")
        .replace_strict(
            EMPLOYMENT_TO_CTRAMP,
            default=CTRAMPEmploymentCategory.NOT_EMPLOYED.value,
        )
        .alias("employment_category"),
    )
    typed = df.with_columns(ctramp_person_type_expression().alias("person_type"))
    return typed["person_type"][0]


class TestPersonTypeClassification:
    """Comprehensive tests for CTRAMP person type classification.

    Uses a hybrid approach:
    - Critical explicit test cases for known bugs and priority rules
    - Property-based tests with Hypothesis for invariant checking
    """

    @pytest.mark.parametrize(
        ("age", "employment", "student", "school_type", "expected_type", "description"),
        [
            # === CRITICAL BUG CASES - Age 16-17 ===
            # These test the bug where 16-17 were classified as NON_WORKER/RETIRED
            (
                AgeCategory.AGE_16_TO_17,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.STUDENT_DRIVING_AGE,
                "BUG FIX: Age 16-17, not student/not employed → STUDENT_DRIVING_AGE",
            ),
            (
                # Reported non-student, but school age with no school location, so
                # ctramp_student_category_expression recodes them to
                # GRADE_OR_HIGH_SCHOOL on its "contradictory data, use age fallback"
                # rule. Person type then applies "children stay children regardless
                # of employment" and the reported full-time job does not win.
                # This pins what the pipeline does; see the recode question raised
                # alongside it.
                AgeCategory.AGE_16_TO_17,
                Employment.EMPLOYED_FULLTIME,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.STUDENT_DRIVING_AGE,
                "Age 16-17 recoded to student by age fallback → student wins",
            ),
            # === CRITICAL BUG CASES - Age 18-24 ===
            (
                AgeCategory.AGE_18_TO_24,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.NON_WORKER,
                "Age 18-24, not student/not worker → NON_WORKER",
            ),
            (
                AgeCategory.AGE_18_TO_24,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.FULLTIME_INPERSON,
                SchoolType.HIGH_SCHOOL,
                CTRAMPPersonType.STUDENT_DRIVING_AGE,
                "Age 18-24, high school → STUDENT_DRIVING_AGE",
            ),
            (
                AgeCategory.AGE_18_TO_24,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.FULLTIME_INPERSON,
                SchoolType.COLLEGE_4YEAR,
                CTRAMPPersonType.UNIVERSITY_STUDENT,
                "Age 18-24, college → UNIVERSITY_STUDENT",
            ),
            # === PRIORITY RULES - Employment vs Student ===
            # Full-time employment beats full-time student status
            (
                AgeCategory.AGE_18_TO_24,
                Employment.EMPLOYED_FULLTIME,
                Student.FULLTIME_INPERSON,
                SchoolType.COLLEGE_4YEAR,
                CTRAMPPersonType.FULL_TIME_WORKER,
                "FT worker + FT college student → employment wins",
            ),
            (
                AgeCategory.AGE_35_TO_44,
                Employment.EMPLOYED_FULLTIME,
                Student.FULLTIME_ONLINE,
                SchoolType.COLLEGE_4YEAR,
                CTRAMPPersonType.FULL_TIME_WORKER,
                "Working adult + FT college → employment wins",
            ),
            # Full-time employment beats part-time student
            (
                AgeCategory.AGE_18_TO_24,
                Employment.EMPLOYED_FULLTIME,
                Student.PARTTIME_INPERSON,
                SchoolType.COLLEGE_4YEAR,
                CTRAMPPersonType.FULL_TIME_WORKER,
                "FT worker + PT college student → employment wins",
            ),
            (
                AgeCategory.AGE_35_TO_44,
                Employment.EMPLOYED_FULLTIME,
                Student.PARTTIME_ONLINE,
                SchoolType.COLLEGE_4YEAR,
                CTRAMPPersonType.FULL_TIME_WORKER,
                "FT worker + PT college student → employment wins",
            ),
            # === PRIORITY RULES - Age 65+ employed → worker types ===
            (
                AgeCategory.AGE_65_TO_74,
                Employment.EMPLOYED_FULLTIME,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.FULL_TIME_WORKER,
                "Age 65+, working FT → FULL_TIME_WORKER (employment wins)",
            ),
            (
                AgeCategory.AGE_75_TO_84,
                Employment.EMPLOYED_PARTTIME,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.PART_TIME_WORKER,
                "Age 75+, working PT → PART_TIME_WORKER (employment wins)",
            ),
            # 65+ non-employed → RETIRED
            (
                AgeCategory.AGE_65_TO_74,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.RETIRED,
                "Age 65+, not employed → RETIRED",
            ),
            # === REPRESENTATIVE CASES - Each person type ===
            (
                AgeCategory.AGE_UNDER_5,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.CHILD_UNDER_5,
                "Under 5",
            ),
            (
                AgeCategory.AGE_5_TO_15,
                Employment.UNEMPLOYED_NOT_LOOKING,
                Student.FULLTIME_INPERSON,
                SchoolType.ELEMENTARY,
                CTRAMPPersonType.STUDENT_NON_DRIVING_AGE,
                "Elementary student",
            ),
            (
                AgeCategory.AGE_35_TO_44,
                Employment.EMPLOYED_PARTTIME,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.PART_TIME_WORKER,
                "Part-time worker",
            ),
            (
                AgeCategory.AGE_35_TO_44,
                Employment.UNEMPLOYED_LOOKING,
                Student.NONSTUDENT,
                SchoolType.MISSING,
                CTRAMPPersonType.NON_WORKER,
                "Unemployed looking",
            ),
        ],
    )
    def test_critical_person_type_cases(
        self, age, employment, student, school_type, expected_type, description
    ):
        """Test critical edge cases and known bugs with explicit test cases."""
        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": school_type.value,
                }
            ]
        )

        person_type = _person_type(df)

        assert person_type == expected_type.value, (
            f"Failed: {description}\n"
            f"  Age: {age.name} ({age.value})\n"
            f"  Employment: {employment.name}\n"
            f"  Student: {student.name}\n"
            f"  School Type: {school_type.name}\n"
            f"  Expected: {expected_type.name} ({expected_type.value})\n"
            f"  Got: {person_type}"
        )

    # === PARAMETRIZED TESTS FOR WORKING COLLEGE STUDENTS ===

    @given(
        age=st.just(AgeCategory.AGE_UNDER_5),
        employment=st.sampled_from(list(Employment)),
        student=st.sampled_from(list(Student)),
        school_type=st.sampled_from(list(SchoolType)),
    )
    def test_property_age_under_5_always_child_under_5(self, age, employment, student, school_type):
        """Property: Anyone under 5 must always be classified as CHILD_UNDER_5."""
        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": school_type.value,
                }
            ]
        )

        person_type = _person_type(df)

        assert person_type == CTRAMPPersonType.CHILD_UNDER_5.value, (
            f"Age under 5 must be CHILD_UNDER_5, got {person_type}"
        )

    @given(
        age=st.sampled_from(list(AgeCategory)),
        employment=st.sampled_from(
            [Employment.EMPLOYED_FULLTIME, Employment.EMPLOYED_SELF, Employment.EMPLOYED_UNPAID]
        ),
        student=st.sampled_from(list(Student)),
        school_type=st.sampled_from(list(SchoolType)),
    )
    def test_property_fulltime_employment_precedence(self, age, employment, student, school_type):
        """Property: from 18 up, full-time employment wins over student status.

        Below 18 the age rules dominate and employment never wins: under 5 is
        CHILD_UNDER_5, 5-15 is STUDENT_NON_DRIVING_AGE, and 16-17 is
        STUDENT_DRIVING_AGE whenever the student category says grade or high
        school -- which, for a school-age person with no school location, it does
        even when they reported being a non-student. Those ages are covered by
        the explicit table above rather than restated here.

        EMPLOYED_UNPAID is part-time under EMPLOYMENT_TO_CTRAMP, so it is out of
        scope for a full-time property.
        """
        assume(
            age
            not in [
                AgeCategory.AGE_UNDER_5,
                AgeCategory.AGE_5_TO_15,
                AgeCategory.AGE_16_TO_17,
            ]
        )
        assume(employment != Employment.EMPLOYED_UNPAID)

        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": school_type.value,
                }
            ]
        )

        person_type = _person_type(df)

        assert person_type == CTRAMPPersonType.FULL_TIME_WORKER.value, (
            f"Full-time employed person aged {age.name} should be FULL_TIME_WORKER "
            f"whatever their student status ({student.name}, {school_type.name}), "
            f"got {person_type}"
        )

    @given(
        age=st.sampled_from(
            [
                AgeCategory.AGE_18_TO_24,
                AgeCategory.AGE_25_TO_34,
                AgeCategory.AGE_35_TO_44,
                AgeCategory.AGE_45_TO_54,
            ]
        ),
        employment=st.sampled_from(
            [Employment.UNEMPLOYED_NOT_LOOKING, Employment.UNEMPLOYED_LOOKING]
        ),
        student=st.sampled_from(
            [Student.FULLTIME_INPERSON, Student.FULLTIME_ONLINE, Student.PARTTIME_INPERSON]
        ),
        school_type=st.sampled_from(
            [
                SchoolType.COLLEGE_2YEAR,
                SchoolType.COLLEGE_4YEAR,
                SchoolType.GRADUATE_SCHOOL,
                SchoolType.VOCATIONAL,
            ]
        ),
    )
    def test_property_college_students_are_university_type(
        self, age, employment, student, school_type
    ):
        """Property: College students (not employed full-time) should be UNIVERSITY_STUDENT."""
        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": school_type.value,
                }
            ]
        )

        person_type = _person_type(df)

        assert person_type == CTRAMPPersonType.UNIVERSITY_STUDENT.value, (
            f"College student (age {age.name}, {school_type.name}) "
            f"should be UNIVERSITY_STUDENT, got {person_type}"
        )

    @given(
        age=st.sampled_from(list(AgeCategory)),
        employment=st.sampled_from(list(Employment)),
        student=st.sampled_from(list(Student)),
        school_type=st.sampled_from(list(SchoolType)),
    )
    def test_property_age_person_type_consistency(self, age, employment, student, school_type):
        """Property: Age determines valid person types - certain age/type combinations are impossible.

        Age-based constraints:
        - Age < 5 must be CHILD_UNDER_5
        - Age 5-15 cannot be FULL_TIME_WORKER, PART_TIME_WORKER, UNIVERSITY_STUDENT, or RETIRED
        - Age 16-17 cannot be CHILD_UNDER_5, CHILD_NON_DRIVING_AGE, or RETIRED
        - Age 18-64 cannot be CHILD_UNDER_5, CHILD_NON_DRIVING_AGE, or RETIRED
        - Age 65+ must be RETIRED, FT/PT_WORKER, or UNIVERSITY_STUDENT
        """  # noqa: E501
        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": school_type.value,
                }
            ]
        )

        person_type = _person_type(df)

        # Age < 5: Must be CHILD_UNDER_5
        if age == AgeCategory.AGE_UNDER_5:
            assert person_type == CTRAMPPersonType.CHILD_UNDER_5.value, (
                f"Age < 5 must be CHILD_UNDER_5, got {person_type}"
            )

        # Age 5-15: Cannot be adult worker types, university, or retired
        if age == AgeCategory.AGE_5_TO_15:
            assert person_type not in [
                CTRAMPPersonType.FULL_TIME_WORKER.value,
                CTRAMPPersonType.PART_TIME_WORKER.value,
                CTRAMPPersonType.UNIVERSITY_STUDENT.value,
                CTRAMPPersonType.RETIRED.value,
                CTRAMPPersonType.CHILD_UNDER_5.value,
            ], f"Age 5-15 cannot be FT/PT worker, university student, or retired, got {person_type}"

        # Age 16-17: Cannot be young children or retired
        if age == AgeCategory.AGE_16_TO_17:
            assert person_type not in [
                CTRAMPPersonType.CHILD_UNDER_5.value,
                CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value,
                CTRAMPPersonType.RETIRED.value,
            ], f"Age 16-17 cannot be young children or retired, got {person_type}"

        # Age 18-64: Cannot be any child type or retired
        if age in [
            AgeCategory.AGE_18_TO_24,
            AgeCategory.AGE_25_TO_34,
            AgeCategory.AGE_35_TO_44,
            AgeCategory.AGE_45_TO_54,
            AgeCategory.AGE_55_TO_64,
        ]:
            # Exception: 18-24 can be STU_DRIVING_AGE if they're in high school
            if (
                age == AgeCategory.AGE_18_TO_24
                and person_type == CTRAMPPersonType.STUDENT_DRIVING_AGE.value
            ):
                # This is allowed for high school students
                pass
            else:
                assert person_type not in [
                    CTRAMPPersonType.CHILD_UNDER_5.value,
                    CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value,
                    CTRAMPPersonType.RETIRED.value,
                ], f"Age {age.name} cannot be young children or retired, got {person_type}"

                if age != AgeCategory.AGE_18_TO_24:
                    assert person_type != CTRAMPPersonType.STUDENT_DRIVING_AGE.value, (
                        f"Age {age.name} cannot be STUDENT_DRIVING_AGE, got {person_type}"
                    )

        # Age 65+: RETIRED unless employed (then worker types)
        if age in [AgeCategory.AGE_65_TO_74, AgeCategory.AGE_75_TO_84, AgeCategory.AGE_85_AND_UP]:
            valid_senior_types = [
                CTRAMPPersonType.RETIRED.value,
                CTRAMPPersonType.FULL_TIME_WORKER.value,
                CTRAMPPersonType.PART_TIME_WORKER.value,
                CTRAMPPersonType.UNIVERSITY_STUDENT.value,
            ]
            assert person_type in valid_senior_types, (
                f"Age 65+ must be RETIRED, FT/PT_WORKER, or UNIVERSITY_STUDENT, got {person_type}"
            )

    @given(
        age=st.sampled_from(list(AgeCategory)),
        employment=st.sampled_from(list(Employment)),
        student=st.sampled_from(list(Student)),
        school_type=st.sampled_from(list(SchoolType)),
    )
    def test_property_employment_person_type_consistency(
        self, age, employment, student, school_type
    ):
        """Property: Employment status and person type must be consistent.

        Employment-based constraints:
        - FULL_TIME_WORKER must have full-time employment (unless age overrides)
        - PART_TIME_WORKER must have part-time employment (unless age overrides)
        - Child types generally shouldn't have full-time employment
        """
        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": school_type.value,
                }
            ]
        )

        person_type = _person_type(df)

        # If classified as FULL_TIME_WORKER, must have full-time employment
        # (EMPLOYED_UNPAID is now treated as part-time)
        if person_type == CTRAMPPersonType.FULL_TIME_WORKER.value:
            assert employment in [
                Employment.EMPLOYED_FULLTIME,
                Employment.EMPLOYED_SELF,
            ], f"FULL_TIME_WORKER must have full-time employment, got {employment.name}"

        # If classified as PART_TIME_WORKER, must have part-time employment
        # (EMPLOYED_UNPAID is treated as part-time)
        if person_type == CTRAMPPersonType.PART_TIME_WORKER.value:
            assert employment in [
                Employment.EMPLOYED_PARTTIME,
                Employment.EMPLOYED_UNPAID,
            ], f"PART_TIME_WORKER must have part-time employment, got {employment.name}"

        # Young children shouldn't have full-time employment
        # (if they do, age-based rules should override)
        if person_type in [
            CTRAMPPersonType.CHILD_UNDER_5.value,
            CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value,
        ]:
            # These types should never have been workers - age overrides employment
            assert age in [AgeCategory.AGE_UNDER_5, AgeCategory.AGE_5_TO_15], (
                f"Child types should only appear for young ages, got age {age.name}"
            )

    @given(
        age=st.sampled_from(
            [
                AgeCategory.AGE_18_TO_24,
                AgeCategory.AGE_25_TO_34,
                AgeCategory.AGE_35_TO_44,
                AgeCategory.AGE_45_TO_54,
                AgeCategory.AGE_55_TO_64,
            ]
        ),
        employment=st.sampled_from(list(Employment)),
        student=st.sampled_from(
            [
                Student.FULLTIME_INPERSON,
                Student.FULLTIME_ONLINE,
                Student.PARTTIME_INPERSON,
                Student.PARTTIME_ONLINE,
            ]
        ),
    )
    def test_property_student_missing_school_type_age_18_plus(self, age, employment, student):
        """Property: Students age 18+ with MISSING school_type should be UNIVERSITY_STUDENT.

        This validates that students with ambiguous/missing school type data
        are classified as university students rather than falling through to
        age-based catch-alls (STUDENT_DRIVING_AGE for 18-24, NON_WORKER for 25+).
        """
        df = pl.DataFrame(
            [
                {
                    "age": age.value,
                    "employment": employment.value,
                    "student": student.value,
                    "school_type": SchoolType.MISSING.value,
                }
            ]
        )

        person_type = _person_type(df)

        # Exception: Full-time employment overrides student status
        # (EMPLOYED_UNPAID is now part-time, so it doesn't override)
        if employment not in [
            Employment.EMPLOYED_FULLTIME,
            Employment.EMPLOYED_SELF,
        ]:
            assert person_type == CTRAMPPersonType.UNIVERSITY_STUDENT.value, (
                f"Student age {age.name} with MISSING school_type "
                f"should be UNIVERSITY_STUDENT (got {person_type}), "
                f"unless full-time employed"
            )


class TestTheCategoriesAreRequired:
    """Person type reads the derived categories; it does not fall back to raw columns.

    It used to. The raw-column branch was unreachable -- every caller derives
    both categories first -- and it disagreed with the categories on school-age
    non-students, so it pinned a classification the pipeline never produced.
    Refusing is what keeps the rule in one place.
    """

    @pytest.mark.parametrize(
        "present",
        [
            pytest.param([], id="neither"),
            pytest.param(["employment_category"], id="student_category_missing"),
            pytest.param(["student_category"], id="employment_category_missing"),
        ],
    )
    def test_a_frame_without_them_is_refused_rather_than_guessed_at(self, present):
        """Either category missing is a caller error, and the message says how to fix it."""
        df = pl.DataFrame(
            [
                {
                    "person_id": 1,
                    "age": AgeCategory.AGE_25_TO_34.value,
                    "employment": Employment.EMPLOYED_FULLTIME.value,
                    "student": Student.NONSTUDENT.value,
                    "school_type": SchoolType.MISSING.value,
                }
            ]
        ).with_columns([pl.lit(0).alias(col) for col in present])

        with pytest.raises(ValueError, match="which person type is derived from"):
            enrich_persons_with_person_type(df)
