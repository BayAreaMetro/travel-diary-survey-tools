"""Unit tests for CT-RAMP formatter.

Tests person type classification, household formatting, tour formatting,
and end-to-end transformation from canonical survey data to CT-RAMP model
format.
"""

from datetime import datetime, time

import polars as pl
import pytest

from data_canon.codebook.ctramp import FreeParkingChoice
from data_canon.codebook.ctramp import PersonType as CTRAMPPersonType
from data_canon.codebook.generic import BooleanYesNo
from data_canon.codebook.households import IncomeDetailed, IncomeFollowup
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    Gender,
    SchoolType,
    Student,
)
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import Purpose
from processing.formatting.ctramp.ctramp_config import CTRAMPConfig
from processing.formatting.ctramp.format_ctramp import format_ctramp
from processing.formatting.ctramp.format_households import format_households
from processing.formatting.ctramp.format_persons import (
    classify_person_type,
    format_persons,
)
from processing.formatting.ctramp.format_tours import (
    format_individual_tour,
    format_joint_tour,
)
from tests.fixtures import (
    create_family_household,
    create_household,
    create_linked_trip,
    create_person,
    create_retired_household,
    create_single_adult_household,
    create_tour,
    create_university_student_household,
    get_tour_schema,
)


@pytest.fixture
def standard_config():
    """Standard test configuration with explicit parameters."""
    return CTRAMPConfig(
        income_low_threshold=60000,  # $60k
        income_med_threshold=150000,  # $150k
        income_high_threshold=240000,  # $240k
        income_base_year_dollars=2023,
        age_adult=4,  # AgeCategory.AGE_18_TO_24.value
    )


class TestPersonTypeClassification:
    """Tests for person type classification logic."""

    def test_full_time_worker(self):
        """Test classification of full-time worker."""
        person_type = classify_person_type(
            age=AgeCategory.AGE_35_TO_44.value,
            employment=Employment.EMPLOYED_FULLTIME.value,
            student=Student.NONSTUDENT.value,
            school_type=SchoolType.MISSING.value,
        )
        assert person_type == CTRAMPPersonType.FULL_TIME_WORKER.value

    def test_part_time_worker(self):
        """Test classification of part-time worker."""
        person_type = classify_person_type(
            age=AgeCategory.AGE_25_TO_34.value,
            employment=Employment.EMPLOYED_PARTTIME.value,
            student=Student.NONSTUDENT.value,
            school_type=SchoolType.MISSING.value,
        )
        assert person_type == CTRAMPPersonType.PART_TIME_WORKER.value

    def test_university_student(self):
        """Test classification of university student."""
        person_type = classify_person_type(
            age=AgeCategory.AGE_18_TO_24.value,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.FULLTIME_INPERSON.value,
            school_type=SchoolType.COLLEGE_4YEAR.value,
        )
        assert person_type == CTRAMPPersonType.UNIVERSITY_STUDENT.value

    def test_retired_person(self):
        """Test classification of retired person."""
        person_type = classify_person_type(
            age=AgeCategory.AGE_65_TO_74.value,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.NONSTUDENT.value,
            school_type=SchoolType.MISSING.value,
        )
        assert person_type == CTRAMPPersonType.RETIRED.value

    def test_nonworker(self):
        """Test classification of non-worker (under 65)."""
        person_type = classify_person_type(
            age=AgeCategory.AGE_45_TO_54.value,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.NONSTUDENT.value,
            school_type=SchoolType.MISSING.value,
        )
        assert person_type == CTRAMPPersonType.NON_WORKER.value

    def test_driving_age_student(self):
        """Test classification of driving age student."""
        person_type = classify_person_type(
            age=AgeCategory.AGE_16_TO_17.value,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.FULLTIME_INPERSON.value,
            school_type=SchoolType.HIGH_SCHOOL.value,
        )
        assert person_type == CTRAMPPersonType.CHILD_DRIVING_AGE.value

    def test_non_driving_age_student(self):
        """Test classification of non-driving age student."""
        person_type = classify_person_type(
            age=AgeCategory.AGE_5_TO_15.value,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.FULLTIME_INPERSON.value,
            school_type=SchoolType.ELEMENTARY.value,
        )
        assert person_type == CTRAMPPersonType.CHILD_NON_DRIVING_AGE.value

    def test_child_too_young(self):
        """Test classification of child too young for school."""
        person_type = classify_person_type(
            age=AgeCategory.AGE_UNDER_5.value,
            employment=Employment.UNEMPLOYED_NOT_LOOKING.value,
            student=Student.NONSTUDENT.value,
            school_type=SchoolType.PRESCHOOL.value,
        )
        assert person_type == CTRAMPPersonType.CHILD_UNDER_5.value


class TestFreeParkingChoice:
    """Tests for free parking choice in person formatting."""

    def test_free_parking_used(self, standard_config):
        """Test free parking choice when free parking is used."""
        persons = pl.DataFrame(
            [
                create_person(
                    commute_subsidy_use_3=BooleanYesNo.YES,
                    commute_subsidy_use_4=BooleanYesNo.NO,
                )
            ]
        )
        result = format_persons(persons, pl.DataFrame(), standard_config)
        assert result["fp_choice"][0] == FreeParkingChoice.PARK_FOR_FREE.value

    def test_discount_parking_used(self, standard_config):
        """Test free parking choice when discounted parking is used."""
        persons = pl.DataFrame(
            [
                create_person(
                    commute_subsidy_use_3=BooleanYesNo.NO,
                    commute_subsidy_use_4=BooleanYesNo.YES,
                )
            ]
        )
        result = format_persons(persons, pl.DataFrame(), standard_config)
        assert result["fp_choice"][0] == FreeParkingChoice.PARK_FOR_FREE.value

    def test_both_parking_subsidies_used(self, standard_config):
        """Test free parking choice when both parking subsidies are used."""
        persons = pl.DataFrame(
            [
                create_person(
                    commute_subsidy_use_3=BooleanYesNo.YES,
                    commute_subsidy_use_4=BooleanYesNo.YES,
                )
            ]
        )
        result = format_persons(persons, pl.DataFrame(), standard_config)
        assert result["fp_choice"][0] == FreeParkingChoice.PARK_FOR_FREE.value

    def test_no_parking_subsidy_used(self, standard_config):
        """Test no parking subsidy used."""
        persons = pl.DataFrame(
            [
                create_person(
                    commute_subsidy_use_3=BooleanYesNo.NO,
                    commute_subsidy_use_4=BooleanYesNo.NO,
                )
            ]
        )
        result = format_persons(persons, pl.DataFrame(), standard_config)
        assert result["fp_choice"][0] == FreeParkingChoice.PAY_TO_PARK.value

    def test_missing_values_treated_as_no_subsidy(self, standard_config):
        """Test that missing (995) values are treated as no subsidy."""
        persons = pl.DataFrame(
            [
                create_person(
                    commute_subsidy_use_3=BooleanYesNo.MISSING,
                    commute_subsidy_use_4=BooleanYesNo.MISSING,
                )
            ]
        )
        result = format_persons(persons, pl.DataFrame(), standard_config)
        assert result["fp_choice"][0] == FreeParkingChoice.PAY_TO_PARK.value


class TestHouseholdFormatting:
    """Tests for household formatting."""

    def test_basic_household_formatting(self):
        """Test basic household formatting with all required fields."""
        households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_taz=100,
                    num_people=2,
                    num_vehicles=1,
                    num_workers=1,
                    income_detailed=IncomeDetailed.INCOME_75TO100,
                )
            ]
        )

        persons = pl.DataFrame(
            [
                {
                    "hh_id": 1,
                    "person_id": 1,
                    "employment": Employment.EMPLOYED_FULLTIME.value,
                },
                {
                    "hh_id": 1,
                    "person_id": 2,
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING.value,
                },
            ]
        )
        result = format_households(households, persons)

        assert len(result) == 1
        assert result["hh_id"][0] == 1
        assert result["taz"][0] == 100
        assert result["income"][0] == 87000  # Midpoint rounded to $1000
        assert result["autos"][0] == 0  # Placeholder value
        assert result["size"][0] == 2
        assert result["workers"][0] == 1
        assert result["jtf_choice"][0] == -4

    def test_income_fallback_logic(self):
        """Test income fallback from detailed to followup."""
        households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    income_detailed=None,
                    income_followup=IncomeFollowup.INCOME_50TO75,
                )
            ]
        )

        persons = pl.DataFrame(
            {"hh_id": [], "employment": []},
            schema={"hh_id": pl.Int64, "employment": pl.Int64},
        )
        result = format_households(households, persons)

        assert (
            result["income"][0] == 62000
        )  # Midpoint of 50-75k from followup rounded to $1000


class TestPersonFormatting:
    """Tests for person formatting."""

    def test_basic_person_formatting(self, standard_config):
        """Test basic person formatting with all required fields."""
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    age=AgeCategory.AGE_35_TO_44,
                    gender=Gender.MALE,
                    employment=Employment.EMPLOYED_FULLTIME,
                    student=Student.NONSTUDENT,
                    commute_subsidy_use_3=BooleanYesNo.YES,
                )
            ]
        )

        result = format_persons(persons, pl.DataFrame(), standard_config)

        assert len(result) == 1
        assert result["hh_id"][0] == 1
        assert result["person_id"][0] == 101
        assert result["person_num"][0] == 1
        assert result["age"][0] == AgeCategory.AGE_35_TO_44.value
        assert result["gender"][0] == "m"
        assert result["type"][0] == CTRAMPPersonType.FULL_TIME_WORKER.value
        assert result["fp_choice"][0] == FreeParkingChoice.PARK_FOR_FREE.value
        assert result["activity_pattern"][0] == "H"  # Placeholder
        assert result["imf_choice"][0] == 0  # Placeholder
        assert result["inmf_choice"][0] == 0  # Placeholder (default)
        assert result["wfh_choice"][0] == 0  # Placeholder

    def test_gender_mapping(self, standard_config):
        """Test gender mapping to m/f format."""
        persons = pl.DataFrame(
            [
                create_person(person_id=101, gender=Gender.FEMALE),
                create_person(person_id=102, gender=Gender.MALE),
                create_person(person_id=103, gender=Gender.OTHER),
            ]
        )

        result = format_persons(persons, pl.DataFrame(), standard_config)

        assert result["gender"][0] == "f"
        assert result["gender"][1] == "m"
        assert result["gender"][2] == "f"  # Defaults to f

    # value_of_time is optional - no need to test default


class TestEndToEndFormatting:
    """Tests for end-to-end CT-RAMP formatting."""

    def test_single_adult_household(self, standard_config):
        """Test formatting of single adult household."""
        (
            households,
            persons,
        ) = create_single_adult_household()

        result = format_ctramp(
            persons,
            households,
            linked_trips=pl.DataFrame(),
            tours=pl.DataFrame(),
            joint_trips=pl.DataFrame(),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_base_year_dollars=standard_config.income_base_year_dollars,
        )

        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]

        assert len(households_ctramp) == 1
        assert len(persons_ctramp) == 1
        assert households_ctramp["hh_id"][0] == 1
        assert (
            persons_ctramp["type"][0] == CTRAMPPersonType.FULL_TIME_WORKER.value
        )

    def test_family_household(self, standard_config):
        """Test formatting of family household with multiple person types."""
        households, persons = create_family_household()

        result = format_ctramp(
            persons,
            households,
            linked_trips=pl.DataFrame(),
            tours=pl.DataFrame(),
            joint_trips=pl.DataFrame(),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_base_year_dollars=standard_config.income_base_year_dollars,
        )

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

    def test_retired_household(self, standard_config):
        """Test formatting of retired household."""
        households, persons = create_retired_household()

        result = format_ctramp(
            persons,
            households,
            linked_trips=pl.DataFrame(),
            tours=pl.DataFrame(),
            joint_trips=pl.DataFrame(),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_base_year_dollars=standard_config.income_base_year_dollars,
        )

        persons_ctramp = result["persons_ctramp"]

        assert len(persons_ctramp) == 2
        assert all(
            pt == CTRAMPPersonType.RETIRED.value
            for pt in persons_ctramp["type"].to_list()
        )

    def test_university_student_household(self, standard_config):
        """Test formatting of university student household."""
        (
            households,
            persons,
        ) = create_university_student_household()

        result = format_ctramp(
            persons,
            households,
            linked_trips=pl.DataFrame(),
            tours=pl.DataFrame(),
            joint_trips=pl.DataFrame(),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_base_year_dollars=standard_config.income_base_year_dollars,
        )

        persons_ctramp = result["persons_ctramp"]

        assert len(persons_ctramp) == 1
        assert (
            persons_ctramp["type"][0]
            == CTRAMPPersonType.UNIVERSITY_STUDENT.value
        )

    def test_drop_missing_taz(self, standard_config):
        """Test filtering households without valid TAZ."""
        households = pl.DataFrame(
            [
                create_household(hh_id=1, home_taz=100),
                create_household(hh_id=2, home_taz=None),
                create_household(hh_id=3, home_taz=-1),
            ]
        )

        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1),
                create_person(person_id=201, hh_id=2),
                create_person(person_id=301, hh_id=3),
            ]
        )

        result = format_ctramp(
            persons,
            households,
            linked_trips=pl.DataFrame(),
            tours=pl.DataFrame(),
            joint_trips=pl.DataFrame(),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_base_year_dollars=standard_config.income_base_year_dollars,
            drop_missing_taz=True,
        )

        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]

        # Only household 1 should remain
        assert len(households_ctramp) == 1
        assert len(persons_ctramp) == 1
        assert households_ctramp["hh_id"][0] == 1
        assert persons_ctramp["hh_id"][0] == 1

    def test_keep_missing_taz_when_disabled(self, standard_config):
        """Test keeping households without TAZ when filtering is disabled."""
        households = pl.DataFrame(
            [
                create_household(hh_id=1, home_taz=100),
                create_household(hh_id=2, home_taz=None),
            ]
        )

        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1),
                create_person(person_id=201, hh_id=2),
            ]
        )

        result = format_ctramp(
            persons,
            households,
            linked_trips=pl.DataFrame(),
            tours=pl.DataFrame(),
            joint_trips=pl.DataFrame(),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_base_year_dollars=standard_config.income_base_year_dollars,
            drop_missing_taz=False,
        )

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
            persons,
        ) = create_single_adult_household()
        result = format_households(households, persons)

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

    def test_person_columns(self, standard_config):
        """Test that all required person columns are present."""
        (
            _,
            persons,
        ) = create_single_adult_household()
        result = format_persons(persons, pl.DataFrame(), standard_config)

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


class TestIndividualTourFormatting:
    """Tests for individual tour formatting."""

    def test_basic_work_tour(self, standard_config):
        """Test formatting of a basic work tour with outbound/inbound trips."""
        # Create canonical data
        households_canonical = pl.DataFrame(
            [
                create_household(
                    hh_id=1, income_detailed=IncomeDetailed.INCOME_100TO150
                )
            ]
        )
        persons_canonical = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    employment=Employment.EMPLOYED_FULLTIME,
                )
            ]
        )
        tours_canonical = pl.DataFrame(
            [
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    tour_purpose=Purpose.PRIMARY_WORKPLACE,
                    o_taz=100,
                    d_taz=200,
                    depart_time=datetime.combine(
                        datetime(2024, 1, 1), time(8, 0)
                    ),
                    arrive_time=datetime.combine(
                        datetime(2024, 1, 1), time(17, 0)
                    ),
                    student_category="Not student",
                )
            ],
            schema=get_tour_schema(),
        )

        # Format to CTRAMP (tours formatter needs formatted households/persons)
        households = format_households(households_canonical, persons_canonical)
        # Pass canonical persons for person_type and school_type

        tours = tours_canonical
        trips_canonical = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )
        trips = trips_canonical

        result = format_individual_tour(
            tours, trips, persons_canonical, households, standard_config
        )

        assert len(result) == 1
        assert result["tour_id"][0] == 1001
        assert result["hh_id"][0] == 1
        assert result["person_id"][0] == 101
        assert result["orig_taz"][0] == 100
        assert result["dest_taz"][0] == 200
        assert result["start_hour"][0] == 8
        assert result["end_hour"][0] == 17
        assert result["num_ob_stops"][0] == 1
        assert result["num_ib_stops"][0] == 1
        assert result["atWork_freq"][0] == 0  # No subtours
        # Purpose should be work_med (income 100-150k is in med bracket)
        assert result["tour_purpose"][0] == "work_med"

    def test_stop_counting_multiple_stops(self, standard_config):
        """Test stop counting with multiple outbound and inbound stops."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                # 3 outbound trips
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10003,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                # 2 inbound trips
                create_linked_trip(
                    trip_id=10004,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.INBOUND,
                ),
                create_linked_trip(
                    trip_id=10005,
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )

        # Format to CTRAMP first
        households_formatted = format_households(households, persons)

        result = format_individual_tour(
            tours,
            trips,
            persons,
            households_formatted,
            standard_config,
        )

        assert result["num_ob_stops"][0] == 3
        assert result["num_ib_stops"][0] == 2

    def test_subtour_counting(self, standard_config):
        """Test at-work tour frequency counting."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                # Primary work tour
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_purpose=Purpose.PRIMARY_WORKPLACE,
                ),
                # At-work subtour 1
                create_tour(
                    tour_id=1002,
                    person_id=101,
                    hh_id=1,
                    parent_tour_id=1001,
                    tour_purpose=Purpose.WORK_ACTIVITY,
                ),
                # At-work subtour 2
                create_tour(
                    tour_id=1003,
                    person_id=101,
                    hh_id=1,
                    parent_tour_id=1001,
                    tour_purpose=Purpose.DINING,
                ),
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                # Primary tour trips
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    person_id=101,
                    tour_direction=TourDirection.INBOUND,
                ),
                # Subtour 1 trips
                create_linked_trip(
                    trip_id=10003,
                    tour_id=1002,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10004,
                    tour_id=1002,
                    person_id=101,
                    tour_direction=TourDirection.INBOUND,
                ),
                # Subtour 2 trips
                create_linked_trip(
                    trip_id=10005,
                    tour_id=1003,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10006,
                    tour_id=1003,
                    person_id=101,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )

        # Format to CTRAMP
        households_formatted = format_households(households, persons)

        result = format_individual_tour(
            tours,
            trips,
            persons,
            households_formatted,
            standard_config,
        )

        # Primary tour should have 2 subtours
        primary_tour = result.filter(pl.col("tour_id") == 1001)
        assert primary_tour["atWork_freq"][0] == 2

        # Subtours should have 0 subtours
        subtour1 = result.filter(pl.col("tour_id") == 1002)
        subtour2 = result.filter(pl.col("tour_id") == 1003)
        assert subtour1["atWork_freq"][0] == 0
        assert subtour2["atWork_freq"][0] == 0

    def test_zero_trip_tour_validation(self, standard_config):
        """Test that tours with zero trips raise validation error."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame([])  # No trips!

        # Format to CTRAMP
        households_formatted = format_households(households, persons)

        with pytest.raises(ValueError, match="Found 1 tours with zero trips"):
            format_individual_tour(
                tours,
                trips,
                persons,
                households_formatted,
                standard_config,
            )

    def test_joint_tour_exclusion(self, standard_config):
        """Test that joint tours are excluded from individual tours."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                # Individual tour
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=None,
                ),
                # Joint tour (should be excluded)
                create_tour(
                    tour_id=1002,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=9001,
                ),
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    person_id=101,
                    tour_direction=TourDirection.INBOUND,
                ),
                create_linked_trip(
                    trip_id=10003,
                    tour_id=1002,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10004,
                    tour_id=1002,
                    person_id=101,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )

        # Format to CTRAMP
        households_formatted = format_households(households, persons)
        format_persons(persons, pl.DataFrame(), standard_config)

        result = format_individual_tour(
            tours,
            trips,
            persons,
            households_formatted,
            standard_config,
        )

        # Only individual tour should be included
        assert len(result) == 1
        assert result["tour_id"][0] == 1001


class TestJointTourFormatting:
    """Tests for joint tour formatting."""

    def test_basic_joint_tour(self, standard_config):
        """Test formatting of a basic joint tour."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    age=AgeCategory.AGE_35_TO_44,
                ),
                create_person(
                    person_id=102,
                    hh_id=1,
                    person_num=2,
                    age=AgeCategory.AGE_5_TO_15,
                ),
            ]
        )
        tours = pl.DataFrame(
            [
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=9001,
                    num_travelers=2,
                ),
                create_tour(
                    tour_id=1002,
                    person_id=102,
                    hh_id=1,
                    joint_tour_id=9001,
                    num_travelers=2,
                ),
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                    joint_tour_id=9001,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    person_id=101,
                    tour_direction=TourDirection.INBOUND,
                    joint_tour_id=9001,
                ),
            ]
        )

        # Format to CTRAMP
        households_formatted = format_households(households, persons)
        persons_formatted = format_persons(
            persons, pl.DataFrame(), standard_config
        )

        result = format_joint_tour(
            tours,
            trips,
            persons_formatted,
            households_formatted,
            standard_config,
        )

        assert len(result) == 1
        assert result["JointTourNum"][0] == 9001
        assert result["HHID"][0] == 1
        assert result["NumObStops"][0] == 1
        assert result["NumIbStops"][0] == 1
        # Composition: 1 adult + 1 child
        assert result["Composition"][0] == 3  # ADULTS_AND_CHILDREN

    def test_individual_tour_exclusion_joint_formatter(self, standard_config):
        """Test that individual tours are excluded from joint tours."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                # Individual tour (should be excluded)
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=None,
                ),
                # Joint tour
                create_tour(
                    tour_id=1002,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=9001,
                ),
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    tour_direction=TourDirection.INBOUND,
                ),
                create_linked_trip(
                    trip_id=10003,
                    tour_id=1002,
                    tour_direction=TourDirection.OUTBOUND,
                    joint_tour_id=9001,
                ),
                create_linked_trip(
                    trip_id=10004,
                    tour_id=1002,
                    tour_direction=TourDirection.INBOUND,
                    joint_tour_id=9001,
                ),
            ]
        )

        # Format to CTRAMP
        households_formatted = format_households(households, persons)
        persons_formatted = format_persons(
            persons, pl.DataFrame(), standard_config
        )

        result = format_joint_tour(
            tours,
            trips,
            persons_formatted,
            households_formatted,
            standard_config,
        )

        # Only joint tour should be included
        assert len(result) == 1
        assert result["JointTourNum"][0] == 9001

    def test_empty_joint_tours(self, standard_config):
        """Test that formatter handles no joint tours gracefully."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                # Only individual tours
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=None,
                )
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    tour_direction=TourDirection.OUTBOUND,
                ),
                create_linked_trip(
                    trip_id=10002,
                    tour_id=1001,
                    tour_direction=TourDirection.INBOUND,
                ),
            ]
        )

        # Format to CTRAMP
        households_formatted = format_households(households, persons)
        persons_formatted = format_persons(
            persons, pl.DataFrame(), standard_config
        )

        result = format_joint_tour(
            tours,
            trips,
            persons_formatted,
            households_formatted,
            standard_config,
        )

        # Should return empty DataFrame
        assert len(result) == 0
