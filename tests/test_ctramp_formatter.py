"""Unit tests for CT-RAMP formatter.

Tests person type classification, household formatting, and end-to-end
transformation from canonical survey data to CT-RAMP model format.
"""

import polars as pl
import pytest

from data_canon.codebook.ctramp import FreeParkingChoice
from data_canon.codebook.ctramp import PersonType as CTRAMPPersonType
from data_canon.codebook.households import IncomeDetailed, IncomeFollowup
from data_canon.codebook.persons import (
    CommuteSubsidy,
    Employment,
    Gender,
    SchoolType,
    Student,
)
from processing.formatting.ctramp.format_ctramp import format_ctramp
from processing.formatting.ctramp.format_households import format_households
from processing.formatting.ctramp.format_persons import (
    classify_person_type,
    determine_free_parking_eligibility,
    format_persons,
)
from tests.fixtures.ctramp_test_data import (
    CTRAMPScenarioBuilder,
    CTRAMPTestDataBuilder,
)


class TestPersonTypeClassification:
    """Tests for person type classification logic."""

    def test_full_time_worker(self):
        """Test classification of full-time worker."""
        person_type = classify_person_type(
            age=35,
            employment=Employment.EMPLOYED_FULLTIME.value,
            student=Student.NONSTUDENT.value,
            school_type=SchoolType.MISSING.value,
        )
        assert person_type == CTRAMPPersonType.FULL_TIME_WORKER.value

    def test_part_time_worker(self):
        """Test classification of part-time worker."""
        person_type = classify_person_type(
            age=28,
            employment=Employment.EMPLOYED_PARTTIME.value,
            student=Student.NONSTUDENT.value,
            school_type=SchoolType.MISSING.value,
        )
        assert person_type == CTRAMPPersonType.PART_TIME_WORKER.value

    def test_university_student(self):
        """Test classification of university student."""
        person_type = classify_person_type(
            age=20,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.FULLTIME_INPERSON.value,
            school_type=SchoolType.COLLEGE_4YEAR.value,
        )
        assert person_type == CTRAMPPersonType.UNIVERSITY_STUDENT.value

    def test_retired_person(self):
        """Test classification of retired person."""
        person_type = classify_person_type(
            age=70,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.NONSTUDENT.value,
            school_type=SchoolType.MISSING.value,
        )
        assert person_type == CTRAMPPersonType.RETIRED.value

    def test_nonworker(self):
        """Test classification of non-worker (under 65)."""
        person_type = classify_person_type(
            age=45,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.NONSTUDENT.value,
            school_type=SchoolType.MISSING.value,
        )
        assert person_type == CTRAMPPersonType.NONWORKER.value

    def test_driving_age_student(self):
        """Test classification of driving age student."""
        person_type = classify_person_type(
            age=16,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.FULLTIME_INPERSON.value,
            school_type=SchoolType.HIGH_SCHOOL.value,
        )
        assert person_type == CTRAMPPersonType.STUDENT_DRIVING_AGE.value

    def test_non_driving_age_student(self):
        """Test classification of non-driving age student."""
        person_type = classify_person_type(
            age=10,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.FULLTIME_INPERSON.value,
            school_type=SchoolType.ELEMENTARY.value,
        )
        assert person_type == CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value

    def test_child_too_young(self):
        """Test classification of child too young for school."""
        person_type = classify_person_type(
            age=3,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.NONSTUDENT.value,
            school_type=SchoolType.PRESCHOOL.value,
        )
        assert person_type == CTRAMPPersonType.CHILD_TOO_YOUNG.value


class TestFreeParkingEligibility:
    """Tests for free parking eligibility determination."""

    def test_free_parking_subsidy(self):
        """Test free parking with subsidy."""
        fp_choice = determine_free_parking_eligibility(
            CommuteSubsidy.FREE_PARK.value
        )
        assert fp_choice == FreeParkingChoice.PARK_FOR_FREE.value

    def test_discount_parking_subsidy(self):
        """Test discounted parking subsidy."""
        fp_choice = determine_free_parking_eligibility(
            CommuteSubsidy.DISCOUNT_PARKING.value
        )
        assert fp_choice == FreeParkingChoice.PARK_FOR_FREE.value

    def test_no_parking_subsidy(self):
        """Test no parking subsidy."""
        fp_choice = determine_free_parking_eligibility(
            CommuteSubsidy.NONE.value
        )
        assert fp_choice == FreeParkingChoice.PAY_TO_PARK.value

    def test_other_subsidy_types(self):
        """Test other subsidy types (transit, etc.)."""
        fp_choice = determine_free_parking_eligibility(
            CommuteSubsidy.TRANSIT.value
        )
        assert fp_choice == FreeParkingChoice.PAY_TO_PARK.value


class TestHouseholdFormatting:
    """Tests for household formatting."""

    def test_basic_household_formatting(self):
        """Test basic household formatting with all required fields."""
        households = pl.DataFrame(
            [
                CTRAMPTestDataBuilder.create_household(
                    hh_id=1,
                    home_taz=100,
                    num_people=2,
                    num_vehicles=1,
                    num_workers=1,
                    income_detailed=IncomeDetailed.INCOME_75TO100,
                )
            ]
        )

        result = format_households(households)

        assert len(result) == 1
        assert result["hh_id"][0] == 1
        assert result["taz"][0] == 100
        assert result["income"][0] == 87500  # Midpoint of 75-100k
        assert result["autos"][0] == 1
        assert result["size"][0] == 2
        assert result["workers"][0] == 1
        assert result["jtf_choice"][0] == -4

    def test_income_fallback_logic(self):
        """Test income fallback from detailed to followup."""
        households = pl.DataFrame(
            [
                CTRAMPTestDataBuilder.create_household(
                    hh_id=1,
                    income_detailed=None,
                    income_followup=IncomeFollowup.INCOME_50TO75,
                )
            ]
        )

        result = format_households(households)

        assert result["income"][0] == 62500  # Midpoint of 50-75k from followup


class TestPersonFormatting:
    """Tests for person formatting."""

    def test_basic_person_formatting(self):
        """Test basic person formatting with all required fields."""
        persons = pl.DataFrame(
            [
                CTRAMPTestDataBuilder.create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    age=35,
                    gender=Gender.MALE,
                    employment=Employment.EMPLOYED_FULLTIME,
                    student=Student.NONSTUDENT,
                    commute_subsidy=CommuteSubsidy.FREE_PARK,
                )
            ]
        )

        result = format_persons(persons)

        assert len(result) == 1
        assert result["hh_id"][0] == 1
        assert result["person_id"][0] == 101
        assert result["person_num"][0] == 1
        assert result["age"][0] == 35
        assert result["gender"][0] == "m"
        assert result["type"][0] == CTRAMPPersonType.FULL_TIME_WORKER.value
        assert result["fp_choice"][0] == FreeParkingChoice.PARK_FOR_FREE.value
        assert result["activity_pattern"][0] == "H"  # Placeholder
        assert result["imf_choice"][0] == 0  # Placeholder
        assert result["inmf_choice"][0] == 1  # Placeholder
        assert result["wfh_choice"][0] == 0  # Placeholder

    def test_gender_mapping(self):
        """Test gender mapping to m/f format."""
        persons = pl.DataFrame(
            [
                CTRAMPTestDataBuilder.create_person(
                    person_id=101, gender=Gender.FEMALE
                ),
                CTRAMPTestDataBuilder.create_person(
                    person_id=102, gender=Gender.MALE
                ),
                CTRAMPTestDataBuilder.create_person(
                    person_id=103, gender=Gender.OTHER
                ),
            ]
        )

        result = format_persons(persons)

        assert result["gender"][0] == "f"
        assert result["gender"][1] == "m"
        assert result["gender"][2] == "f"  # Defaults to f

    def test_default_value_of_time(self):
        """Test default value of time when missing."""
        persons = pl.DataFrame(
            [
                {
                    "person_id": 101,
                    "hh_id": 1,
                    "person_num": 1,
                    "age": 35,
                    "gender": Gender.MALE.value,
                    "employment": Employment.EMPLOYED_FULLTIME.value,
                    "student": Student.NONSTUDENT.value,
                    "school_type": SchoolType.MISSING.value,
                    "commute_subsidy": CommuteSubsidy.NONE.value,
                    # No value_of_time field
                }
            ]
        )

        result = format_persons(persons)

        assert result["value_of_time"][0] == 15.0  # Default


class TestEndToEndFormatting:
    """Tests for end-to-end CT-RAMP formatting."""

    def test_single_adult_household(self):
        """Test formatting of single adult household."""
        (
            households,
            persons,
        ) = CTRAMPScenarioBuilder.create_single_adult_household()

        result = format_ctramp(persons, households)

        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]

        assert len(households_ctramp) == 1
        assert len(persons_ctramp) == 1
        assert households_ctramp["hh_id"][0] == 1
        assert (
            persons_ctramp["type"][0] == CTRAMPPersonType.FULL_TIME_WORKER.value
        )

    def test_family_household(self):
        """Test formatting of family household with multiple person types."""
        households, persons = CTRAMPScenarioBuilder.create_family_household()

        result = format_ctramp(persons, households)

        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]

        assert len(households_ctramp) == 1
        assert len(persons_ctramp) == 4

        # Check person types
        person_types = persons_ctramp["type"].to_list()
        assert CTRAMPPersonType.FULL_TIME_WORKER.value in person_types
        assert CTRAMPPersonType.PART_TIME_WORKER.value in person_types
        assert CTRAMPPersonType.STUDENT_DRIVING_AGE.value in person_types
        assert CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value in person_types

    def test_retired_household(self):
        """Test formatting of retired household."""
        households, persons = CTRAMPScenarioBuilder.create_retired_household()

        result = format_ctramp(persons, households)

        persons_ctramp = result["persons_ctramp"]

        assert len(persons_ctramp) == 2
        assert all(
            pt == CTRAMPPersonType.RETIRED.value
            for pt in persons_ctramp["type"].to_list()
        )

    def test_university_student_household(self):
        """Test formatting of university student household."""
        (
            households,
            persons,
        ) = CTRAMPScenarioBuilder.create_university_student_household()

        result = format_ctramp(persons, households)

        persons_ctramp = result["persons_ctramp"]

        assert len(persons_ctramp) == 1
        assert (
            persons_ctramp["type"][0]
            == CTRAMPPersonType.UNIVERSITY_STUDENT.value
        )

    def test_drop_missing_taz(self):
        """Test filtering households without valid TAZ."""
        households = pl.DataFrame(
            [
                CTRAMPTestDataBuilder.create_household(hh_id=1, home_taz=100),
                CTRAMPTestDataBuilder.create_household(hh_id=2, home_taz=None),
                CTRAMPTestDataBuilder.create_household(hh_id=3, home_taz=-1),
            ]
        )

        persons = pl.DataFrame(
            [
                CTRAMPTestDataBuilder.create_person(person_id=101, hh_id=1),
                CTRAMPTestDataBuilder.create_person(person_id=201, hh_id=2),
                CTRAMPTestDataBuilder.create_person(person_id=301, hh_id=3),
            ]
        )

        result = format_ctramp(persons, households, drop_missing_taz=True)

        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]

        # Only household 1 should remain
        assert len(households_ctramp) == 1
        assert len(persons_ctramp) == 1
        assert households_ctramp["hh_id"][0] == 1
        assert persons_ctramp["hh_id"][0] == 1

    def test_keep_missing_taz_when_disabled(self):
        """Test keeping households without TAZ when filtering is disabled."""
        households = pl.DataFrame(
            [
                CTRAMPTestDataBuilder.create_household(hh_id=1, home_taz=100),
                CTRAMPTestDataBuilder.create_household(hh_id=2, home_taz=None),
            ]
        )

        persons = pl.DataFrame(
            [
                CTRAMPTestDataBuilder.create_person(person_id=101, hh_id=1),
                CTRAMPTestDataBuilder.create_person(person_id=201, hh_id=2),
            ]
        )

        result = format_ctramp(persons, households, drop_missing_taz=False)

        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]

        # Both households should remain
        assert len(households_ctramp) == 2
        assert len(persons_ctramp) == 2


class TestColumnPresence:
    """Tests to ensure all required CT-RAMP columns are present."""

    def test_household_columns(self):
        """Test that all required household columns are present."""
        (
            households,
            _,
        ) = CTRAMPScenarioBuilder.create_single_adult_household()
        result = format_households(households)

        required_columns = [
            "hh_id",
            "taz",
            "income",
            "autos",
            "jtf_choice",
            "size",
            "workers",
        ]

        for col in required_columns:
            assert col in result.columns, f"Missing column: {col}"

    def test_person_columns(self):
        """Test that all required person columns are present."""
        (
            _,
            persons,
        ) = CTRAMPScenarioBuilder.create_single_adult_household()
        result = format_persons(persons)

        required_columns = [
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

        for col in required_columns:
            assert col in result.columns, f"Missing column: {col}"


class TestModelOutputFieldExclusion:
    """Tests to ensure model-output fields are excluded from survey data.

    These tests validate that fields marked as 'Model output only, not
    derivable from survey data' in the CT-RAMP models are NOT present in
    formatted survey data. They will fail for unimplemented formatters
    (tours, trips, mandatory locations) as a reminder to implement them.
    """

    def test_household_excludes_model_output_fields(self):
        """Test that household formatter excludes model-output fields."""
        (
            households,
            _,
        ) = CTRAMPScenarioBuilder.create_single_adult_household()
        result = format_households(households)

        # Fields that should NOT be present in survey-formatted data
        excluded_fields = [
            "walk_subzone",
            "humanVehicles",
            "autonomousVehicles",
            "auto_suff",
            "ao_rn",
            "fp_rn",
            "cdap_rn",
            "imtf_rn",
            "imtod_rn",
            "immc_rn",
            "jtf_rn",
            "jtl_rn",
            "jtod_rn",
            "jmc_rn",
            "inmtf_rn",
            "inmtl_rn",
            "inmtod_rn",
            "inmmc_rn",
            "awf_rn",
            "awl_rn",
            "awtod_rn",
            "awmc_rn",
            "stf_rn",
            "stl_rn",
        ]

        for field in excluded_fields:
            assert field not in result.columns, (
                f"Model-output field '{field}' "
                "should not be in household output"
            )

    def test_person_excludes_model_output_fields(self):
        """Test that person formatter excludes model-output fields (if any)."""
        (
            _,
            persons,
        ) = CTRAMPScenarioBuilder.create_single_adult_household()
        result = format_persons(persons)

        # PersonCTRAMPModel has no model-output-only optional fields
        # This test ensures no model-output fields are accidentally added
        all_columns = result.columns

        # Verify no random number or walk segment fields
        for col in all_columns:
            assert not col.endswith("_rn"), (
                f"Random number field '{col}' should not be in person output"
            )
            assert "walk" not in col.lower() or col in ["wfh_choice"], (
                f"Walk segment field '{col}' should not be in person output"
            )

    def test_mandatory_location_excludes_model_output_fields(self):
        """Test mandatory location formatter excludes model-output fields.

        NOTE: This test will fail until mandatory location formatter is
        implemented.
        """
        # This is a placeholder test for TDD - will fail as a reminder
        # to exclude HomeSubZone, SchoolSubZone, WorkSubZone when implementing
        pytest.fail(
            "Mandatory location formatter not yet implemented. "
            "When implementing, ensure HomeSubZone, SchoolSubZone, "
            "WorkSubZone are excluded from survey-formatted output."
        )

    def test_individual_tour_excludes_model_output_fields(self):
        """Test individual tour formatter excludes model-output fields.

        NOTE: This test will fail until individual tour formatter is
        implemented.
        """
        # This is a placeholder test for TDD - will fail as a reminder
        # to exclude model-output fields when implementing
        pytest.fail(
            "Individual tour formatter not yet implemented. "
            "When implementing, ensure these fields are excluded from "
            "survey data: avAvailable, dest_walk_segment, "
            "orig_walk_segment, dcLogsum, origTaxiWait, destTaxiWait, "
            "origSingleTNCWait, destSingleTNCWait, origSharedTNCWait, "
            "destSharedTNCWait. Include sampleRate only if derivable "
            "from survey weights."
        )

    def test_individual_trip_excludes_model_output_fields(self):
        """Test individual trip formatter excludes model-output fields.

        NOTE: This test will fail until individual trip formatter is
        implemented.
        """
        # This is a placeholder test for TDD - will fail as a reminder
        pytest.fail(
            "Individual trip formatter not yet implemented. "
            "When implementing, ensure these fields are excluded from "
            "survey data: avAvailable, taxiWait, singleTNCWait, "
            "sharedTNCWait, orig_walk_segment, dest_walk_segment. "
            "Include sampleRate only if derivable from survey weights."
        )

    def test_joint_tour_excludes_model_output_fields(self):
        """Test that joint tour formatter excludes model-output fields.

        NOTE: This test will fail until joint tour formatter is implemented.
        """
        # This is a placeholder test for TDD - will fail as a reminder
        pytest.fail(
            "Joint tour formatter not yet implemented. "
            "When implementing, ensure orig_walk_segment and dest_walk_segment "
            "are excluded from survey-formatted output."
        )

    def test_joint_trip_excludes_model_output_fields(self):
        """Test that joint trip formatter excludes model-output fields.

        NOTE: This test will fail until joint trip formatter is implemented.
        """
        # This is a placeholder test for TDD - will fail as a reminder
        pytest.fail(
            "Joint trip formatter not yet implemented. "
            "When implementing, ensure orig_walk_segment and dest_walk_segment "
            "are excluded from survey-formatted output."
        )
