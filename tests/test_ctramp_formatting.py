"""Unit tests for CT-RAMP formatter.

Tests formatting, field corrections, and end-to-end transformation from
canonical survey data to CT-RAMP model format.
"""

from datetime import datetime, time
from typing import get_args

import polars as pl
import pytest

from data_canon.codebook.ctramp import (
    AtWorkFreq,
    CTRAMPPersonType,
    FreeParkingChoice,
    JTFChoice,
    TourComposition,
    WFHChoice,
)
from data_canon.codebook.generic import BooleanYesNo
from data_canon.codebook.households import IncomeBroad
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    Gender,
    Student,
)
from data_canon.codebook.tours import TourDirection
from data_canon.codebook.trips import PurposeCategory
from data_canon.models.ctramp import (
    AllTripCTRAMPModel,
    HouseholdCTRAMPModel,
    IndividualTourCTRAMPModel,
    IndividualTripCTRAMPModel,
    JointTourCTRAMPModel,
    JointTripCTRAMPModel,
    PersonCTRAMPModel,
)
from processing.formatting.ctramp.format_ctramp import _drop_excess_fields, format_ctramp
from processing.formatting.ctramp.format_households import format_households
from processing.formatting.ctramp.format_joint_trips import format_joint_trip
from processing.formatting.ctramp.format_persons import format_persons
from processing.formatting.ctramp.format_tours import (
    format_individual_tour,
    format_joint_tour,
)
from processing.formatting.ctramp.format_trips import (
    format_individual_trip,
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
    days_for_persons,
    empty_joint_tours,
    empty_joint_trips,
    empty_linked_trips,
    empty_tours,
    empty_unlinked_trips,
    get_tour_schema,
)


def get_required_non_null_fields(model):
    """Get field names that are required and don't allow None.

    Args:
        model: Pydantic BaseModel class

    Returns:
        List of field names that are required (no | None in type)
    """
    required = []
    for name, field_info in model.model_fields.items():
        # Check if None is allowed in the type annotation
        # get_args returns empty tuple for non-generic types
        type_args = get_args(field_info.annotation)
        # If type_args is not empty and None is in the args, skip it
        if type_args and type(None) in type_args:
            continue  # Skip optional fields (have | None)
        required.append(name)
    return required


class TestFreeParkingChoice:
    """Tests for free parking choice in person formatting."""

    @pytest.mark.parametrize(
        ("use_free", "use_discounted", "expected"),
        [
            (BooleanYesNo.YES, BooleanYesNo.NO, FreeParkingChoice.PARK_FOR_FREE),
            (BooleanYesNo.NO, BooleanYesNo.YES, FreeParkingChoice.PARK_FOR_FREE),
            (BooleanYesNo.YES, BooleanYesNo.YES, FreeParkingChoice.PARK_FOR_FREE),
            (BooleanYesNo.NO, BooleanYesNo.NO, FreeParkingChoice.PAY_TO_PARK),
            # 995 is "missing", which is not evidence of a subsidy.
            (BooleanYesNo.MISSING, BooleanYesNo.MISSING, FreeParkingChoice.PAY_TO_PARK),
        ],
    )
    def test_fp_choice_from_parking_subsidies(
        self, use_free, use_discounted, expected, standard_config
    ):
        """Either subsidy in use means the person does not pay to park."""
        persons = pl.DataFrame(
            [
                create_person(
                    commute_subsidy_use_free_parking=use_free,
                    commute_subsidy_use_discounted_parking=use_discounted,
                )
            ]
        )
        result = format_persons(persons, pl.DataFrame(), standard_config)
        assert result["fp_choice"][0] == expected.value


class TestHouseholdFormatting:
    """Tests for household formatting."""

    def test_basic_household_formatting(self, standard_config):
        """Test basic household formatting with all required fields."""
        households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_taz=100,
                    num_people=2,
                    num_vehicles=1,
                    num_workers=1,
                    income_bin=IncomeBroad.INCOME_75TO100,
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
        tours = pl.DataFrame([], schema=get_tour_schema())
        result = format_households(households, persons, tours, standard_config)

        assert len(result) == 1
        assert result["hh_id"][0] == 1
        assert result["taz"][0] == 100
        assert result["income"][0] == 46277  # $87k (2023) midpoint deflated to $2000 (/1.88)
        assert result["autos"][0] == 1
        assert result["size"][0] == 2
        assert result["workers"][0] == 1
        assert result["jtf_choice"][0] == JTFChoice.NONE_NONE.value

    @pytest.mark.parametrize(
        ("income_column", "expected_income"),
        [
            # No income column at all: the bin midpoint stands in, deflated to
            # $2000 by income_survey_year_to_ctramp_year ($62k / 1.88).
            pytest.param(None, 32979, id="bin_midpoint_only"),
            # A null income still falls back to the bin midpoint...
            pytest.param([None], 32979, id="null_income_falls_back"),
            # ...but a reported income is kept, and only deflated.
            pytest.param([100000], 53191, id="reported_income_wins"),
        ],
    )
    def test_income_from_bin_or_reported_value(
        self, income_column, expected_income, standard_config
    ):
        """Income is the reported value where there is one, else the bin midpoint."""
        households = pl.DataFrame([create_household(hh_id=1, income_bin=IncomeBroad.INCOME_50TO75)])
        households = (
            households.drop("income")
            if income_column is None
            else households.with_columns(income=pl.Series(income_column, dtype=pl.Int64))
        )
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame([], schema=get_tour_schema())

        result = format_households(households, persons, tours, standard_config)

        assert result["income"][0] == expected_income


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
                    commute_subsidy_use_free_parking=BooleanYesNo.YES,
                )
            ]
        )

        result = format_persons(persons, pl.DataFrame(), standard_config)

        assert len(result) == 1
        assert result["hh_id"][0] == 1
        assert result["person_id"][0] == 101
        assert result["person_num"][0] == 1
        assert result["age"][0] == 39  # Midpoint of 35-44
        assert result["gender"][0] == "m"
        assert result["type"][0] == CTRAMPPersonType.FULL_TIME_WORKER.label
        assert result["fp_choice"][0] == FreeParkingChoice.PARK_FOR_FREE.value
        assert result["activity_pattern"][0] == "H"  # Placeholder
        assert result["imf_choice"][0] == 0  # Placeholder
        assert result["inmf_choice"][0] == 0  # Placeholder (default)
        assert result["wfh_choice"][0] == WFHChoice.NON_WORKER_OR_NO_WFH.value  # Placeholder

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
            linked_trips=empty_linked_trips(),
            tours=empty_tours(),
            joint_trips=empty_joint_trips(),
            unlinked_trips=empty_unlinked_trips(),
            joint_tours=empty_joint_tours(),
            days=days_for_persons(persons),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_survey_year_to_ctramp_year=standard_config.income_survey_year_to_ctramp_year,
            usability_profile="test",
        )

        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]

        assert len(households_ctramp) == 1
        assert len(persons_ctramp) == 1
        # CT-RAMP ids are person-day encoded: hh_id * 100 + day_num.
        assert households_ctramp["hh_id"][0] == 101
        assert persons_ctramp["type"][0] == CTRAMPPersonType.FULL_TIME_WORKER.label

    @pytest.mark.parametrize(
        ("build_household", "expected_types"),
        [
            pytest.param(
                create_family_household,
                [
                    CTRAMPPersonType.FULL_TIME_WORKER,
                    CTRAMPPersonType.PART_TIME_WORKER,
                    CTRAMPPersonType.STUDENT_DRIVING_AGE,
                    CTRAMPPersonType.STUDENT_NON_DRIVING_AGE,
                ],
                id="family",
            ),
            pytest.param(
                create_retired_household,
                [CTRAMPPersonType.RETIRED, CTRAMPPersonType.RETIRED],
                id="retired",
            ),
            pytest.param(
                create_university_student_household,
                [CTRAMPPersonType.UNIVERSITY_STUDENT],
                id="university_student",
            ),
        ],
    )
    def test_person_types_by_household_scenario(
        self, build_household, expected_types, standard_config
    ):
        """Each scenario builder produces exactly the person types it is named for."""
        households, persons = build_household()

        result = format_ctramp(
            persons,
            households,
            linked_trips=empty_linked_trips(),
            tours=empty_tours(),
            joint_trips=empty_joint_trips(),
            unlinked_trips=empty_unlinked_trips(),
            joint_tours=empty_joint_tours(),
            days=days_for_persons(persons),
            income_low_threshold=standard_config.income_low_threshold,
            income_med_threshold=standard_config.income_med_threshold,
            income_high_threshold=standard_config.income_high_threshold,
            income_survey_year_to_ctramp_year=standard_config.income_survey_year_to_ctramp_year,
            usability_profile="test",
        )

        assert len(result["households_ctramp"]) == 1
        assert sorted(result["persons_ctramp"]["type"].to_list()) == sorted(
            person_type.label for person_type in expected_types
        )


class TestColumnPresence:
    """Tests to ensure all required CT-RAMP columns are present."""

    def test_household_columns(self, standard_config):
        """Test that all required household columns are present."""
        households, persons = create_single_adult_household()
        tours = pl.DataFrame([], schema=get_tour_schema())
        result = format_households(households, persons, tours, standard_config)

        required_columns = get_required_non_null_fields(HouseholdCTRAMPModel)
        for col in required_columns:
            assert col in result.columns, f"Missing required column: {col}"

    def test_person_columns(self, standard_config):
        """Test that all required person columns are present."""
        _, persons = create_single_adult_household()
        result = format_persons(persons, pl.DataFrame(), standard_config)

        required_columns = get_required_non_null_fields(PersonCTRAMPModel)
        for col in required_columns:
            assert col in result.columns, f"Missing required column: {col}"

    def test_individual_tour_columns(self, standard_config):
        """Test that all required individual tour columns are present."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
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
                    trip_id=10002, tour_id=1001, person_id=101, tour_direction=TourDirection.INBOUND
                ),
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        required_columns = get_required_non_null_fields(IndividualTourCTRAMPModel)
        for col in required_columns:
            assert col in result.columns, f"Missing required column: {col}"

    def test_individual_trip_columns(self, standard_config):
        """Test that all required individual trip columns are present."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10001,
                    tour_id=1001,
                    person_id=101,
                    tour_direction=TourDirection.OUTBOUND,
                    depart_time=datetime.combine(datetime(2024, 1, 1), time(8, 30)),
                ),
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        tours_formatted = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )
        result = format_individual_trip(
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            tours_ctramp=tours_formatted,
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        required_columns = get_required_non_null_fields(IndividualTripCTRAMPModel)
        # parking_taz is optional in the model, so don't check for it
        for col in required_columns:
            assert col in result.columns, f"Missing required column: {col}"

    def test_joint_tour_columns(self, standard_config):
        """Test that all required joint tour columns are present."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1, person_num=1, age=AgeCategory.AGE_35_TO_44),
                create_person(person_id=102, hh_id=1, person_num=2, age=AgeCategory.AGE_5_TO_15),
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
                    tour_purpose=PurposeCategory.SHOP,
                ),
                create_tour(
                    tour_id=1002,
                    person_id=102,
                    hh_id=1,
                    joint_tour_id=9001,
                    num_travelers=2,
                    tour_purpose=PurposeCategory.SHOP,
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

        households_formatted = format_households(households, persons, tours, standard_config)
        persons_formatted = format_persons(persons, pl.DataFrame(), standard_config)
        result = format_joint_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            joint_tours_canonical=pl.DataFrame(),
            persons_canonical=persons_formatted,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        required_columns = get_required_non_null_fields(JointTourCTRAMPModel)
        for col in required_columns:
            assert col in result.columns, f"Missing required column: {col}"

    def test_joint_trip_columns(self, standard_config):
        """Test that all required joint trip columns are present."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1, person_num=1)])
        tours = pl.DataFrame(
            [
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    joint_tour_id=9001,
                    tour_purpose=PurposeCategory.SHOP,
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
                    hh_id=1,
                    joint_tour_id=9001,
                    joint_trip_id=90001,
                    tour_direction=TourDirection.OUTBOUND,
                )
            ]
        )
        joint_trips = pl.DataFrame(
            [
                {
                    "joint_trip_id": 90001,
                    "joint_tour_id": 9001,
                    "hh_id": 1,
                    "num_joint_travelers": 1,
                }
            ]
        )

        households_formatted = format_households(households, persons, tours, standard_config)
        result = format_joint_trip(
            joint_trips_canonical=joint_trips,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            tours_canonical=tours,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        required_columns = get_required_non_null_fields(JointTripCTRAMPModel)
        for col in required_columns:
            assert col in result.columns, f"Missing required column: {col}"


class TestIndividualTourFormatting:
    """Tests for individual tour formatting."""

    def test_basic_work_tour(self, standard_config):
        """Test formatting of a basic work tour with outbound/inbound trips."""
        # Create canonical data
        households_canonical = pl.DataFrame(
            [create_household(hh_id=1, income_bin=IncomeBroad.INCOME_75TO100)]
        )
        persons_canonical = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    employment=Employment.EMPLOYED_FULLTIME,
                    person_type=CTRAMPPersonType.FULL_TIME_WORKER.value,
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
                    tour_purpose=PurposeCategory.WORK,
                    o_taz=100,
                    d_taz=200,
                    origin_depart_time=datetime.combine(datetime(2024, 1, 1), time(8, 0)),
                    origin_arrive_time=datetime.combine(datetime(2024, 1, 1), time(17, 0)),
                    student_category="Not student",
                )
            ],
            schema=get_tour_schema(),
        )

        # Format to CTRAMP (tours formatter needs formatted households/persons)
        households = format_households(
            households_canonical, persons_canonical, tours_canonical, standard_config
        )
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
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons_canonical,
            households_ctramp=households,
            config=standard_config,
        )

        assert len(result) == 1
        assert result["tour_id"][0] == 0  # CTRAMP tour_id is 0-based (0 for first tour)
        assert result["hh_id"][0] == 1
        assert result["person_id"][0] == 101
        assert result["orig_taz"][0] == 100
        assert result["dest_taz"][0] == 200
        assert result["start_hour"][0] == 8
        assert result["end_hour"][0] == 17
        assert result["num_ob_stops"][0] == 0  # 1 OB trip = 0 stops
        assert result["num_ib_stops"][0] == 0  # 1 IB trip = 0 stops
        # A work tour with no subtours is NO_SUBTOUR (1); 0 means "tour is not at work".
        assert result["atWork_freq"][0] == AtWorkFreq.NO_SUBTOUR.value
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
        households_formatted = format_households(households, persons, tours, standard_config)

        result = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        assert result["num_ob_stops"][0] == 2  # 3 trips = 2 stops
        assert result["num_ib_stops"][0] == 1  # 2 trips = 1 stop

    def test_subtour_counting(self, standard_config):
        """Test at-work tour frequency counting."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [
                # Primary work tour (tour_num=1)
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    hh_id=1,
                    tour_num=1,
                    tour_purpose=PurposeCategory.WORK,
                ),
                # At-work subtour 1 (tour_num=2)
                create_tour(
                    tour_id=1002,
                    person_id=101,
                    hh_id=1,
                    tour_num=2,
                    parent_tour_id=1001,
                    tour_purpose=PurposeCategory.WORK_RELATED,
                ),
                # At-work subtour 2 (tour_num=3)
                create_tour(
                    tour_id=1003,
                    person_id=101,
                    hh_id=1,
                    tour_num=3,
                    parent_tour_id=1001,
                    tour_purpose=PurposeCategory.MEAL,
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
        households_formatted = format_households(households, persons, tours, standard_config)

        result = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        # Primary tour is 0-based (tour_id 0); its at-work subtours are encoded as
        # two-digit <1-based parent tour #><subtour #> -> 11 and 12.
        # atWork_freq is a CT-RAMP category, not a raw subtour count: this tour's
        # WORK_RELATED + MEAL subtours are one business and one eating out.
        primary_tour = result.filter(pl.col("tour_id") == 0)
        assert primary_tour["atWork_freq"][0] == AtWorkFreq.ONE_EAT_ONE_BUSINESS.value

        # Subtours are not themselves at work, so they take the not-at-work category.
        subtour1 = result.filter(pl.col("tour_id") == 11)
        subtour2 = result.filter(pl.col("tour_id") == 12)
        assert subtour1["atWork_freq"][0] == AtWorkFreq.NONE_NOT_WORK.value
        assert subtour2["atWork_freq"][0] == AtWorkFreq.NONE_NOT_WORK.value

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
        households_formatted = format_households(households, persons, tours, standard_config)

        with pytest.raises(ValueError, match="Found 1 tours with zero trips"):
            format_individual_tour(
                tours_canonical=tours,
                linked_trips_canonical=trips,
                unlinked_trips_canonical=pl.DataFrame(),
                persons_canonical=persons,
                households_ctramp=households_formatted,
                config=standard_config,
            )


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
                    tour_purpose=PurposeCategory.SHOP,
                ),
                create_tour(
                    tour_id=1002,
                    person_id=102,
                    hh_id=1,
                    joint_tour_id=9001,
                    num_travelers=2,
                    tour_purpose=PurposeCategory.SHOP,
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
        households_formatted = format_households(households, persons, tours, standard_config)

        result = format_joint_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            joint_tours_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_formatted,
            config=standard_config,
        )

        assert len(result) == 1
        assert result["tour_id"][0] == 0  # CTRAMP joint tour_id is 0-based per household
        assert result["hh_id"][0] == 1
        assert result["num_ob_stops"][0] == 0  # 1 trip = 0 stops
        assert result["num_ib_stops"][0] == 0  # 1 trip = 0 stops
        # Composition: 1 adult + 1 child
        assert result["tour_composition"][0] == TourComposition.ADULTS_AND_CHILDREN.value


LEVELS = [
    pytest.param("_households", "hh_weight", id="household"),
    pytest.param("_persons", "person_weight", id="person"),
    pytest.param("_tours", "tour_weight", id="tour"),
    # The trip table renames linked_trip_weight to trip_weight on the way out.
    pytest.param("_trips", "trip_weight", id="trip"),
]


class TestWeightsAndSampleRateFormatting:
    """``sampleRate`` is 1/weight, and exists only where a weight does.

    Each of the four levels carries its own weight column and derives its own
    rate from it, so the rule is checked per level rather than assumed to copy
    down from households. Zero and null have no inverse, and writing one would
    poison every total derived from the file, so the rate is null there.

    Each builder below takes the weights to put on the input rows, or ``None``
    to leave the weight column off the input entirely, and returns the formatted
    frame in input order.
    """

    @staticmethod
    def _households(weights, config):
        """Format one household per weight, each with a single person."""
        if weights is None:
            households = pl.DataFrame([create_household(hh_id=1)]).drop("hh_weight")
        else:
            households = pl.DataFrame(
                [create_household(hh_id=i, hh_weight=w) for i, w in enumerate(weights, start=1)],
                schema_overrides={"hh_weight": pl.Float64},
            )
        persons = pl.DataFrame(
            [create_person(person_id=100 * i + 1, hh_id=i) for i in range(1, len(households) + 1)]
        )
        tours = pl.DataFrame([], schema=get_tour_schema())
        return format_households(households, persons, tours, config).sort("hh_id")

    @staticmethod
    def _persons(weights, config):
        """Format one household's persons, one per weight."""
        if weights is None:
            persons = pl.DataFrame([create_person(person_id=101, hh_id=1)]).drop("person_weight")
        else:
            persons = pl.DataFrame(
                [
                    create_person(person_id=100 + i, hh_id=1, person_weight=w)
                    for i, w in enumerate(weights, start=1)
                ],
                schema_overrides={"person_weight": pl.Float64},
            )
        return format_persons(persons, pl.DataFrame(), config).sort("person_id")

    @staticmethod
    def _tours(weights, config):
        """Format one person's tours, one per weight, each a simple round trip."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        if weights is None:
            tours = pl.DataFrame(
                [create_tour(tour_id=1001, person_id=101, hh_id=1)],
                schema=get_tour_schema(),
            ).drop("tour_weight")
        else:
            tours = pl.DataFrame(
                [
                    create_tour(tour_id=1000 + i, person_id=101, hh_id=1, tour_num=i, tour_weight=w)
                    for i, w in enumerate(weights, start=1)
                ],
                schema={**get_tour_schema(), "tour_weight": pl.Float64},
            )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    trip_id=10000 + 2 * i + leg,
                    tour_id=tour_id,
                    person_id=101,
                    tour_direction=direction,
                )
                for i, tour_id in enumerate(tours["tour_id"].to_list())
                for leg, direction in enumerate((TourDirection.OUTBOUND, TourDirection.INBOUND))
            ]
        )
        households_ctramp = format_households(households, persons, tours, config)
        return format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_ctramp,
            config=config,
        ).sort("tour_id")

    @staticmethod
    def _trips(weights, config):
        """Format one tour's trips, one per weight."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame([create_person(person_id=101, hh_id=1)])
        tours = pl.DataFrame(
            [create_tour(tour_id=1001, person_id=101, hh_id=1)],
            schema=get_tour_schema(),
        )
        legs = (TourDirection.OUTBOUND, TourDirection.INBOUND, TourDirection.INBOUND)
        if weights is None:
            trips = pl.DataFrame(
                [
                    create_linked_trip(
                        linked_trip_id=10000 + i, tour_id=1001, person_id=101, tour_direction=leg
                    )
                    for i, leg in enumerate(legs[:2], start=1)
                ]
            ).drop("linked_trip_weight")
        else:
            trips = pl.DataFrame(
                [
                    create_linked_trip(
                        linked_trip_id=10000 + i,
                        tour_id=1001,
                        person_id=101,
                        tour_direction=legs[i - 1],
                        linked_trip_weight=w,
                    )
                    for i, w in enumerate(weights, start=1)
                ],
                schema_overrides={"linked_trip_weight": pl.Float64},
            )
        households_ctramp = format_households(households, persons, tours, config)
        tours_ctramp = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_ctramp,
            config=config,
        )
        return format_individual_trip(
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            tours_ctramp=tours_ctramp,
            persons_canonical=persons,
            households_ctramp=households_ctramp,
            config=config,
        ).sort("linked_trip_id")

    @pytest.mark.parametrize(("level", "weight_column"), LEVELS)
    def test_sample_rate_is_the_inverse_of_the_weight(self, level, weight_column, standard_config):
        """A usable weight inverts; zero and null produce no rate at all."""
        result = getattr(self, level)([2.5, 0.0, None], standard_config)

        assert result[weight_column].to_list() == [2.5, 0.0, None]
        assert result["sampleRate"].to_list() == [0.4, None, None]

    @pytest.mark.parametrize(("level", "weight_column"), LEVELS)
    def test_no_weight_column_means_no_sample_rate(self, level, weight_column, standard_config):
        """An unweighted input stays unweighted: neither column is invented."""
        result = getattr(self, level)(None, standard_config)

        assert weight_column not in result.columns
        assert "sampleRate" not in result.columns


class TestAllTables:
    """The unified All* tables: one row per person-record, joint travel included.

    CT-RAMP splits tours and trips across an individual file and a joint file with
    different weight conventions. These tables keep every record in the per-person
    form the canonical data already holds, so a total needs no reconciliation
    between the two.
    """

    @staticmethod
    def _household_with_one_joint_and_one_individual_tour():
        """Person 101 takes a solo tour; 101 and 102 share a joint tour."""
        households = pl.DataFrame([create_household(hh_id=1)])
        persons = pl.DataFrame(
            [
                create_person(person_id=101, hh_id=1, person_num=1),
                create_person(person_id=102, hh_id=1, person_num=2),
            ]
        )
        tours = pl.DataFrame(
            [
                create_tour(
                    tour_id=1001,
                    person_id=101,
                    tour_num=1,
                    tour_purpose=PurposeCategory.WORK,
                    tour_weight=10.0,
                ),
                create_tour(
                    tour_id=1002,
                    person_id=101,
                    tour_num=2,
                    joint_tour_id=5001,
                    tour_purpose=PurposeCategory.SOCIALREC,
                    num_travelers=2,
                    tour_weight=20.0,
                ),
                create_tour(
                    tour_id=1003,
                    person_id=102,
                    person_num=2,
                    tour_num=1,
                    joint_tour_id=5001,
                    tour_purpose=PurposeCategory.SOCIALREC,
                    num_travelers=2,
                    tour_weight=30.0,
                ),
            ],
            schema=get_tour_schema(),
        )
        trips = pl.DataFrame(
            [
                create_linked_trip(
                    linked_trip_id=t,
                    person_id=pid,
                    person_num=pnum,
                    tour_id=tid,
                    joint_tour_id=jid,
                    tour_direction=direction,
                    linked_trip_weight=w,
                )
                for t, pid, pnum, tid, jid, direction, w in [
                    (1, 101, 1, 1001, None, TourDirection.OUTBOUND, 10.0),
                    (2, 101, 1, 1001, None, TourDirection.INBOUND, 10.0),
                    (3, 101, 1, 1002, 5001, TourDirection.OUTBOUND, 20.0),
                    (4, 101, 1, 1002, 5001, TourDirection.INBOUND, 20.0),
                    (5, 102, 2, 1003, 5001, TourDirection.OUTBOUND, 30.0),
                    (6, 102, 2, 1003, 5001, TourDirection.INBOUND, 30.0),
                ]
            ]
        )
        return households, persons, tours, trips

    def _format(self, tours, trips, persons, households, standard_config, *, include_joint):
        households_ctramp = format_households(households, persons, tours, standard_config)
        tours_ctramp = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            persons_canonical=persons,
            households_ctramp=households_ctramp,
            config=standard_config,
            include_joint=include_joint,
        )
        trips_ctramp = format_individual_trip(
            linked_trips_canonical=trips,
            unlinked_trips_canonical=pl.DataFrame(),
            tours_ctramp=tours_ctramp,
            persons_canonical=persons,
            households_ctramp=households_ctramp,
            config=standard_config,
        )
        return tours_ctramp, trips_ctramp

    def test_joint_members_appear_as_their_own_person_tours(self, standard_config):
        """A joint tour contributes one row per participant, not one row per group."""
        households, persons, tours, trips = self._household_with_one_joint_and_one_individual_tour()

        individual, _ = self._format(
            tours, trips, persons, households, standard_config, include_joint=False
        )
        all_tours, _ = self._format(
            tours, trips, persons, households, standard_config, include_joint=True
        )

        # The individual table sees only the solo tour; All* sees all three.
        assert len(individual) == 1
        assert len(all_tours) == 3
        assert all_tours["joint_tour_id"].null_count() == 1

    def test_tour_weights_are_carried_through_unchanged(self, standard_config):
        """No summing or rescaling: each row keeps the weight it already had."""
        households, persons, tours, trips = self._household_with_one_joint_and_one_individual_tour()

        all_tours, _ = self._format(
            tours, trips, persons, households, standard_config, include_joint=True
        )

        assert sorted(all_tours["tour_weight"].to_list()) == [10.0, 20.0, 30.0]
        assert all_tours["tour_weight"].sum() == pytest.approx(tours["tour_weight"].sum())

    def test_all_trips_reconcile_with_the_canonical_trips(self, standard_config):
        """sum(all_trips) == sum(linked_trips): the whole point of the table."""
        households, persons, tours, trips = self._household_with_one_joint_and_one_individual_tour()

        _, all_trips = self._format(
            tours, trips, persons, households, standard_config, include_joint=True
        )

        assert len(all_trips) == len(trips)
        assert all_trips["trip_weight"].sum() == pytest.approx(trips["linked_trip_weight"].sum())

    def test_joint_trips_are_identifiable_within_the_unified_table(self, standard_config):
        """joint_trip_id / joint_tour_id survive, so shared travel stays findable."""
        households, persons, tours, trips = self._household_with_one_joint_and_one_individual_tour()

        _, all_trips = self._format(
            tours, trips, persons, households, standard_config, include_joint=True
        )

        shared = all_trips.filter(pl.col("joint_tour_id").is_not_null())
        assert len(shared) == 4

    def test_joint_columns_survive_the_model_trim(self):
        """The All* models declare the joint ids, so _drop_excess_fields keeps them.

        Output tables are trimmed to their model before being written. Without the
        joint ids on the model, they would be silently dropped on the way to disk
        and the unified tables would lose the only handle on shared travel.
        """
        frame = pl.DataFrame(
            {"hh_id": [1], "joint_trip_id": [500], "joint_tour_id": [900], "spurious": [1]}
        )

        kept = _drop_excess_fields(frame, AllTripCTRAMPModel)
        assert "joint_trip_id" in kept.columns
        assert "joint_tour_id" in kept.columns
        assert "spurious" not in kept.columns

        # The individual model has no such fields, so the same frame loses them.
        assert "joint_trip_id" not in _drop_excess_fields(frame, IndividualTripCTRAMPModel).columns
