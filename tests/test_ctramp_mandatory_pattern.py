from datetime import datetime, time
from typing import get_args

import polars as pl
import pytest

from data_canon.codebook.ctramp import (
    CTRAMPPersonType,
    CTRAMPPurpose,
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
    HouseholdCTRAMPModel,
    IndividualTourCTRAMPModel,
    IndividualTripCTRAMPModel,
    JointTourCTRAMPModel,
    JointTripCTRAMPModel,
    PersonCTRAMPModel,
)
from processing.formatting.ctramp.ctramp_config import CTRAMPConfig
from processing.formatting.ctramp.format_ctramp import format_ctramp
from processing.formatting.ctramp.format_households import format_households
from processing.formatting.ctramp.format_persons import format_persons
from processing.formatting.ctramp.format_tours import (
    format_individual_tour,
    format_joint_tour,
)
from processing.formatting.ctramp.format_trips import (
    format_individual_trip,
    format_joint_trip,
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
    empty_joint_trips,
    empty_linked_trips,
    empty_tours,
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
        age_adult=4,  # AGE_18_TO_24 = category 4 (18+ are adults)
    )


class TestWorkRelatedMapping:
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
            persons_canonical=persons_canonical,
            households_ctramp=households,
            config=standard_config,
        )

        assert len(result) == 1
        assert result["tour_id"][0] == 1  # CTRAMP tour_id is tour_num (1 for first tour)
        assert result["hh_id"][0] == 1
        assert result["person_id"][0] == 101
        assert result["orig_taz"][0] == 100
        assert result["dest_taz"][0] == 200
        assert result["start_hour"][0] == 8
        assert result["end_hour"][0] == 17
        assert result["num_ob_stops"][0] == 0  # 1 OB trip = 0 stops
        assert result["num_ib_stops"][0] == 0  # 1 IB trip = 0 stops
        assert result["atWork_freq"][0] == 0  # No subtours
        # Purpose should be work_med (income 100-150k is in med bracket)
        assert result["tour_purpose"][0] == "work_med" 

    def test_at_work_tour(self, standard_config):
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
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    person_type=CTRAMPPersonType.NON_WORKER.value,
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
                    tour_purpose=PurposeCategory.WORK_RELATED,
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
            persons_canonical=persons_canonical,
            households_ctramp=households,
            config=standard_config,
        )

        assert len(result) == 1
        assert result["tour_id"][0] == 1  # CTRAMP tour_id is tour_num (1 for first tour)
        assert result["hh_id"][0] == 1
        assert result["person_id"][0] == 101
        assert result["orig_taz"][0] == 100
        assert result["dest_taz"][0] == 200
        assert result["start_hour"][0] == 8
        assert result["end_hour"][0] == 17
        assert result["num_ob_stops"][0] == 0  # 1 OB trip = 0 stops
        assert result["num_ib_stops"][0] == 0  # 1 IB trip = 0 stops
        assert result["atWork_freq"][0] == 0  # No subtours
        # Purpose should be work_med (income 100-150k is in med bracket)
        assert result["tour_purpose"][0] == "othdiscr" 

    def test_at_work_tour_worker(self, standard_config):
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
                        tour_purpose=PurposeCategory.WORK_RELATED,
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
                persons_canonical=persons_canonical,
                households_ctramp=households,
                config=standard_config,
            )

            print(result['tour_purpose'])
            assert len(result) == 1
            assert result["tour_id"][0] == 1  # CTRAMP tour_id is tour_num (1 for first tour)
            assert result["hh_id"][0] == 1
            assert result["person_id"][0] == 101
            assert result["orig_taz"][0] == 100
            assert result["dest_taz"][0] == 200
            assert result["start_hour"][0] == 8
            assert result["end_hour"][0] == 17
            assert result["num_ob_stops"][0] == 0  # 1 OB trip = 0 stops
            assert result["num_ib_stops"][0] == 0  # 1 IB trip = 0 stops
            assert result["atWork_freq"][0] == 0  # No subtours
            # Purpose should be work_med (income 100-150k is in med bracket)
            assert result["tour_purpose"][0] == "atwork_business" 

