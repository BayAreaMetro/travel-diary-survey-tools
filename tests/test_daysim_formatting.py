"""Unit tests for DaySim formatter.

Tests person type classification, household composition, mode aggregation,
tour formatting, and end-to-end transformation from canonical survey data
to DaySim model format.
"""

from datetime import datetime

import polars as pl
import pytest

from data_canon.codebook.days import TravelDow
from data_canon.codebook.daysim import (
    DaysimDriverPassenger,
    DaysimMode,
    DaysimPathType,
    DaysimPersonType,
    DaysimPurpose,
)
from data_canon.codebook.households import (
    IncomeBroad,
    ResidenceRentOwn,
    ResidenceType,
)
from data_canon.codebook.persons import (
    AgeCategory,
    Employment,
    Gender,
    SchoolType,
    Student,
)
from data_canon.codebook.trips import (
    Driver,
    Mode,
    ModeType,
    PurposeCategory,
)
from processing.formatting.daysim.format_daysim import format_daysim
from processing.formatting.daysim.format_households import format_households
from processing.formatting.daysim.format_persons import (
    compute_day_completeness,
    format_persons,
)
from processing.formatting.daysim.format_tours import format_tours
from processing.formatting.daysim.format_trips import format_linked_trips
from processing.link_trips.link import link_trips
from tests.fixtures import (
    add_test_taz_maz_ids,
    create_day,
    create_household,
    create_multi_person_household_processed,
    create_person,
    create_simple_work_tour_processed,
    create_transit_commute_processed,
    create_unlinked_trip,
)
from tests.fixtures.locations import HOME_LOCATION, WORK_LOCATION


def _days(*schedule, person_id: int = 101, hh_id: int = 1, person_num: int = 1) -> list[dict]:
    """Day records for one person, one per ``(travel_dow, survey_complete)`` pair."""
    return [
        create_day(
            day_id=day_num,
            person_id=person_id,
            hh_id=hh_id,
            person_num=person_num,
            day_num=day_num,
            travel_dow=travel_dow,
            survey_complete=complete,
        )
        for day_num, (travel_dow, complete) in enumerate(schedule, start=1)
    ]


class TestDayCompleteness:
    """Tests for day completeness computation."""

    def test_compute_day_completeness_single_person_weekday(self):
        """Test day completeness for single person with one complete weekday."""
        result = compute_day_completeness(pl.DataFrame(_days((TravelDow.MONDAY, True))))

        assert len(result) == 1
        assert result["hhno"][0] == 1
        assert result["pno"][0] == 1
        assert result["mon_complete"][0] == 1
        assert result["tue_complete"][0] == 0
        assert result["num_days_complete_3dayweekday"][0] == 0  # Tue+Wed+Thu
        assert result["num_days_complete_4dayweekday"][0] == 1  # Mon+Tue+Wed+Thu
        assert result["num_days_complete_5dayweekday"][0] == 1  # Mon-Fri

    def test_compute_day_completeness_full_week(self):
        """Test day completeness for person with complete week."""
        result = compute_day_completeness(
            pl.DataFrame(
                _days(*[(TravelDow(i), True) for i in range(1, 8)], person_id=201, hh_id=2)
            )
        )

        assert len(result) == 1
        assert result["hhno"][0] == 2
        assert result["pno"][0] == 1
        assert result["mon_complete"][0] == 1
        assert result["sun_complete"][0] == 1
        assert result["num_days_complete_3dayweekday"][0] == 3
        assert result["num_days_complete_4dayweekday"][0] == 4
        assert result["num_days_complete_5dayweekday"][0] == 5

    def test_compute_day_completeness_incomplete_days(self):
        """Test day completeness with some incomplete days."""
        result = compute_day_completeness(
            pl.DataFrame(
                _days(
                    (TravelDow.TUESDAY, True),
                    (TravelDow.WEDNESDAY, False),
                    (TravelDow.THURSDAY, True),
                    person_id=301,
                    hh_id=3,
                )
            )
        )

        assert len(result) == 1
        assert result["tue_complete"][0] == 1
        assert result["wed_complete"][0] == 0
        assert result["thu_complete"][0] == 1
        assert result["num_days_complete_3dayweekday"][0] == 2  # Tue+Thu only

    def test_compute_day_completeness_multiple_persons(self):
        """Test day completeness with multiple persons."""
        days = pl.DataFrame(
            _days((TravelDow.MONDAY, True), person_id=101, person_num=1)
            + _days((TravelDow.MONDAY, False), person_id=102, person_num=2)
        )

        result = compute_day_completeness(days)

        assert len(result) == 2
        assert result.filter(pl.col("pno") == 1)["mon_complete"][0] == 1
        assert result.filter(pl.col("pno") == 2)["mon_complete"][0] == 0


class TestPersonFormatting:
    """Tests for person formatting and type classification."""

    @pytest.mark.parametrize(
        ("attributes", "expected"),
        [
            pytest.param(
                {
                    "employment": Employment.EMPLOYED_FULLTIME,
                    "age": AgeCategory.AGE_35_TO_44,
                    "work_mode": Mode.HOUSEHOLD_VEHICLE_1,
                    "work_taz": 200,
                    "work_maz": 2000,
                },
                {
                    "pptyp": DaysimPersonType.FULL_TIME_WORKER.value,
                    "pwtyp": 1,
                    "pagey": 40,  # Midpoint of AGE_35_TO_44
                    "pwtaz": 200,
                    "pwpcl": 2000,
                },
                id="full_time_worker",
            ),
            pytest.param(
                {
                    "employment": Employment.EMPLOYED_PARTTIME,
                    "age": AgeCategory.AGE_25_TO_34,
                    "work_mode": Mode.HOUSEHOLD_VEHICLE_1,
                    "work_taz": 200,
                    "work_maz": 2000,
                },
                {"pptyp": DaysimPersonType.PART_TIME_WORKER.value, "pwtyp": 2},
                id="part_time_worker",
            ),
            pytest.param(
                {
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING,
                    "student": Student.FULLTIME_INPERSON,
                    "age": AgeCategory.AGE_18_TO_24,
                    "school_taz": 300,
                    "school_maz": 3000,
                    "school_type": SchoolType.COLLEGE_4YEAR,
                    "work_mode": Mode.MISSING,
                },
                {
                    "pptyp": DaysimPersonType.UNIVERSITY_STUDENT.value,
                    "pwtaz": -1,  # No work location
                    "pstaz": 300,
                    "pspcl": 3000,
                },
                id="university_student",
            ),
            pytest.param(
                {
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING,
                    "student": Student.FULLTIME_INPERSON,
                    "age": AgeCategory.AGE_16_TO_17,
                    "school_taz": 150,
                    "school_maz": 1500,
                    "school_type": SchoolType.HIGH_SCHOOL,
                    "work_mode": Mode.MISSING,
                },
                {
                    "pptyp": DaysimPersonType.CHILD_DRIVING_AGE.value,
                    "pstaz": 150,
                    "pspcl": 1500,
                },
                id="high_school_student",
            ),
            pytest.param(
                {
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING,
                    "student": Student.NONSTUDENT,
                    "age": AgeCategory.AGE_65_TO_74,
                    "work_mode": Mode.MISSING,
                },
                {"pptyp": DaysimPersonType.RETIRED.value, "pwtaz": -1},
                id="retiree",
            ),
            pytest.param(
                {
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING,
                    "student": Student.NONSTUDENT,
                    "age": AgeCategory.AGE_25_TO_34,
                },
                {"pptyp": DaysimPersonType.NON_WORKER.value},
                id="non_working_adult",
            ),
            pytest.param(
                {
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING,
                    "student": Student.NONSTUDENT,
                    "age": AgeCategory.AGE_5_TO_15,
                },
                {"pptyp": DaysimPersonType.CHILD_NON_DRIVING_AGE.value, "pagey": 10},
                id="child_non_driving",
            ),
            pytest.param(
                {
                    "employment": Employment.UNEMPLOYED_NOT_LOOKING,
                    "student": Student.NONSTUDENT,
                    "age": AgeCategory.AGE_UNDER_5,
                },
                {"pptyp": DaysimPersonType.CHILD_UNDER_5.value, "pagey": 3},
                id="child_under_5",
            ),
        ],
    )
    def test_format_persons_person_type(self, attributes, expected):
        """Pptyp and the mandatory locations come from age, employment and study.

        ``person_type`` is never read off the input: ``format_persons`` derives
        it from ``pagey``/``employment``/``student``, so these rows pass none.
        """
        days_list = _days((TravelDow.MONDAY, True))
        persons = pl.DataFrame(
            [create_person(person_id=101, hh_id=1, person_num=1, days=days_list, **attributes)]
        )

        result = format_persons(persons, pl.DataFrame(days_list))

        assert len(result) == 1
        for column, value in expected.items():
            assert result[column][0] == value, column

    def test_format_persons_gender_mapping(self):
        """Test gender code mapping."""
        days_list = _days((TravelDow.MONDAY, True)) + _days(
            (TravelDow.MONDAY, True), person_id=102, person_num=2
        )
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=person_id,
                    hh_id=1,
                    person_num=person_num,
                    age=AgeCategory.AGE_35_TO_44,
                    employment=Employment.EMPLOYED_FULLTIME,
                    gender=gender,
                    days=[d for d in days_list if d["person_id"] == person_id],
                )
                for person_id, person_num, gender in (
                    (101, 1, Gender.MALE),
                    (102, 2, Gender.FEMALE),
                )
            ]
        )

        result = format_persons(persons, pl.DataFrame(days_list))

        assert result.filter(pl.col("pno") == 1)["pgend"][0] == 1  # Male
        assert result.filter(pl.col("pno") == 2)["pgend"][0] == 2  # Female


class TestHouseholdFormatting:
    """Tests for household formatting and composition."""

    def test_format_households_single_person(self):
        """Test household formatting with single person."""
        households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_taz=100,
                    home_maz=1000,
                    num_vehicles=1,
                    income_bin=IncomeBroad.INCOME_75TO100,
                    residence_rent_own=ResidenceRentOwn.OWN,
                    residence_type=ResidenceType.SFH,
                    num_workers=1,
                )
            ]
        )

        days_list = _days((TravelDow.MONDAY, True))
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=DaysimPersonType.FULL_TIME_WORKER,
                    days=days_list,
                )
            ]
        )

        persons_daysim = format_persons(persons, pl.DataFrame(days_list))

        result = format_households(households, persons_daysim)

        assert len(result) == 1
        assert result["hhno"][0] == 1
        assert result["hhsize"][0] == 1
        assert result["hhvehs"][0] == 1
        assert result["hhftw"][0] == 1  # One full-time worker
        assert result["hhtaz"][0] == 100
        assert "hhincome" in result.columns

    def test_format_households_multi_person_composition(self):
        """Test household composition with multiple person types."""
        households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_taz=100,
                    home_maz=1000,
                    num_people=4,
                    num_vehicles=2,
                    residence_rent_own=ResidenceRentOwn.OWN,
                    residence_type=ResidenceType.SFH,
                )
            ]
        )

        persons_daysim = pl.DataFrame(
            [
                {"hhno": 1, "pno": 1, "pptyp": DaysimPersonType.FULL_TIME_WORKER.value, "pwtyp": 1},
                {"hhno": 1, "pno": 2, "pptyp": DaysimPersonType.PART_TIME_WORKER.value, "pwtyp": 2},
                {
                    "hhno": 1,
                    "pno": 3,
                    "pptyp": DaysimPersonType.CHILD_DRIVING_AGE.value,
                    "pwtyp": 0,
                },
                {
                    "hhno": 1,
                    "pno": 4,
                    "pptyp": DaysimPersonType.CHILD_NON_DRIVING_AGE.value,
                    "pwtyp": 0,
                },
            ]
        )

        result = format_households(households, persons_daysim)

        assert result["hhsize"][0] == 4
        assert result["hhftw"][0] == 1  # One full-time worker
        assert result["hhptw"][0] == 1  # One part-time worker
        assert result["hhhsc"][0] == 1  # One high school student
        assert result["hh515"][0] == 1  # One child 5-15

    @pytest.mark.parametrize(
        ("income_kwargs", "expected_income"),
        [
            # Midpoint of $50,000-$74,999.
            pytest.param({"income_bin": IncomeBroad.INCOME_50TO75}, 62000, id="from_bin_midpoint"),
            # A pre-computed income passes through instead of the bin midpoint.
            pytest.param(
                {"income": 99999, "income_bin": IncomeBroad.INCOME_50TO75},
                99999,
                id="income_passthrough",
            ),
        ],
    )
    def test_format_households_income(self, income_kwargs, expected_income):
        """Hhincome is the bin midpoint unless a computed income is already there."""
        households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_maz=1000,
                    residence_rent_own=ResidenceRentOwn.OWN,
                    residence_type=ResidenceType.SFH,
                    **income_kwargs,
                )
            ]
        )

        persons_daysim = pl.DataFrame([{"hhno": 1, "pno": 1, "pptyp": 1, "pwtyp": 1}])

        result = format_households(households, persons_daysim)

        assert result["hhincome"][0] == expected_income


def _format_one_trip(**trip_kwargs) -> pl.DataFrame:
    """Link and format a single home-to-work trip built from ``trip_kwargs``."""
    persons = pl.DataFrame([create_person(person_id=101, hh_id=1, person_num=1)])
    unlinked_trips = pl.DataFrame(
        [
            create_unlinked_trip(
                unlinked_trip_id=1,
                person_id=101,
                hh_id=1,
                person_num=1,
                day_num=1,
                o_lat=HOME_LOCATION.lat,
                o_lon=HOME_LOCATION.lon,
                d_lat=WORK_LOCATION.lat,
                d_lon=WORK_LOCATION.lon,
                o_purpose_category=PurposeCategory.HOME,
                d_purpose_category=PurposeCategory.WORK,
                **trip_kwargs,
            )
        ]
    )

    linked = link_trips(
        unlinked_trips,
        change_mode_enum=PurposeCategory.CHANGE_MODE.value,
        transit_mode_enums=[Mode.BART.value, Mode.BUS_LOCAL.value],
        split_on_occupancy=False,
    )
    with_zones = add_test_taz_maz_ids(
        unlinked_trips=linked["unlinked_trips"],
        linked_trips=linked["linked_trips"],
        tours=None,
        persons=persons,
        households=None,
    )

    return format_linked_trips(
        with_zones["persons"], with_zones["unlinked_trips"], with_zones["linked_trips"]
    )


class TestTripFormatting:
    """Tests for trip mode aggregation and formatting."""

    @pytest.mark.parametrize(
        ("trip_kwargs", "expected_mode", "expected_dorp"),
        [
            pytest.param(
                {
                    "mode_1": Mode.HOUSEHOLD_VEHICLE_1,
                    "mode_type": ModeType.CAR,
                    "driver": Driver.DRIVER,
                    "num_travelers": 1,
                    "change_mode": False,
                },
                DaysimMode.SOV,
                DaysimDriverPassenger.DRIVER,
                id="sov",
            ),
            pytest.param(
                {
                    "mode_1": Mode.HOUSEHOLD_VEHICLE_1,
                    "mode_type": ModeType.CAR,
                    "driver": Driver.DRIVER,
                    "num_travelers": 2,
                },
                DaysimMode.HOV2,
                DaysimDriverPassenger.DRIVER,
                id="hov2",
            ),
            pytest.param(
                {
                    "mode_1": Mode.HOUSEHOLD_VEHICLE_1,
                    "mode_type": ModeType.CAR,
                    "driver": Driver.DRIVER,
                    "num_travelers": 4,
                },
                DaysimMode.HOV3,
                DaysimDriverPassenger.DRIVER,
                id="hov3",
            ),
            pytest.param(
                {
                    "mode_1": Mode.WALK,
                    "mode_type": ModeType.WALK,
                    "driver": Driver.MISSING,
                    "num_travelers": 1,
                },
                DaysimMode.WALK,
                DaysimDriverPassenger.NA,
                id="walk",
            ),
            pytest.param(
                {
                    "mode_1": Mode.BIKE,
                    "mode_type": ModeType.BIKE,
                    "driver": Driver.MISSING,
                    "num_travelers": 1,
                },
                DaysimMode.BIKE,
                DaysimDriverPassenger.NA,
                id="bike",
            ),
        ],
    )
    def test_format_linked_trips_mode_and_dorp(self, trip_kwargs, expected_mode, expected_dorp):
        """Occupancy decides SOV/HOV2/HOV3; a non-vehicle trip has no driver role."""
        result = _format_one_trip(**trip_kwargs)

        assert len(result) == 1
        assert result["mode"][0] == expected_mode.value
        assert result["dorp"][0] == expected_dorp.value

    def test_format_linked_trips_purpose_mapping(self):
        """Test purpose code mapping."""
        result = _format_one_trip()

        assert result["opurp"][0] == DaysimPurpose.HOME.value
        assert result["dpurp"][0] == DaysimPurpose.WORK.value

    def test_format_linked_trips_time_conversion(self):
        """Test time conversion to minutes after midnight."""
        result = _format_one_trip(
            depart_time=datetime(2023, 10, 15, 8, 30),  # 8:30 AM
            arrive_time=datetime(2023, 10, 15, 9, 15),  # 9:15 AM
        )

        assert result["deptm"][0] == 8 * 60 + 30  # 510 minutes
        assert result["arrtm"][0] == 9 * 60 + 15  # 555 minutes


class TestTourFormatting:
    """Tests for tour formatting."""

    def test_format_tours_purpose_and_times(self):
        """A work tour keeps its identity, purpose and clock times.

        Times are minutes after midnight: the fixture leaves home at 08:00,
        reaches work at 09:00, leaves at 17:00 and is home again at 18:00.
        """
        data = create_simple_work_tour_processed()

        result = format_tours(data["persons"], data["days"], data["linked_trips"], data["tours"])

        assert len(result) == 1
        assert result["hhno"][0] == 1
        assert result["pno"][0] == 1
        assert result["pdpurp"][0] == DaysimPurpose.WORK.value
        assert result["tlvorig"][0] == 8 * 60
        assert result["tardest"][0] == 9 * 60
        assert result["tlvdest"][0] == 17 * 60
        assert result["tarorig"][0] == 18 * 60


class TestEndToEndDaysimFormatting:
    """End-to-end integration tests for DaySim formatting."""

    def test_format_daysim_simple_work_tour(self):
        """Test end-to-end formatting with simple work tour scenario."""
        data = create_simple_work_tour_processed()

        result = format_daysim(
            data["persons"],
            data["households"],
            data["unlinked_trips"],
            data["linked_trips"],
            data["tours"],
            data["days"],
            usability_profile="test",
        )

        # Verify all expected keys present
        assert "households_daysim" in result
        assert "persons_daysim" in result
        assert "linked_trips_daysim" in result
        assert "tours_daysim" in result

        # One household, one person, one out-and-back work tour of two trips.
        assert len(result["households_daysim"]) == 1
        assert len(result["persons_daysim"]) == 1
        assert len(result["linked_trips_daysim"]) == 2
        assert len(result["tours_daysim"]) == 1

    def test_format_daysim_transit_commute(self):
        """Test end-to-end formatting with transit commute scenario."""
        data = create_transit_commute_processed()

        result = format_daysim(
            data["persons"],
            data["households"],
            data["unlinked_trips"],
            data["linked_trips"],
            data["tours"],
            data["days"],
            usability_profile="test",
        )

        # Verify transit mode detected
        trips_result = result["linked_trips_daysim"]
        assert len(trips_result) == 2  # 2 linked trips (AM and PM commute)
        # Both trips should be walk-to-transit
        assert all(trips_result["mode"] == DaysimMode.WALK_TRANSIT.value)
        # Path type should be BART
        assert all(trips_result["pathtype"] == DaysimPathType.BART.value)

    def test_format_daysim_multi_person_household(self):
        """Test end-to-end formatting with multi-person household."""
        data = create_multi_person_household_processed()

        result = format_daysim(
            data["persons"],
            data["households"],
            data["unlinked_trips"],
            data["linked_trips"],
            data["tours"],
            data["days"],
            usability_profile="test",
        )

        # Verify household composition
        hh_result = result["households_daysim"]
        assert hh_result["hhsize"][0] == 4
        assert hh_result["hhftw"][0] == 2
        assert hh_result["hhptw"][0] == 0

        # Verify person types
        persons_result = result["persons_daysim"]
        assert len(persons_result) == 4

    def test_format_daysim_output_schema(self):
        """Test that output DataFrames have expected DaySim columns."""
        data = create_simple_work_tour_processed()

        result = format_daysim(
            data["persons"],
            data["households"],
            data["unlinked_trips"],
            data["linked_trips"],
            data["tours"],
            data["days"],
            usability_profile="test",
        )

        expected_columns = {
            "households_daysim": [
                "hhno",
                "hhsize",
                "hhvehs",
                "hhftw",
                "hhptw",
                "hhtaz",
                "hhincome",
            ],
            "persons_daysim": ["hhno", "pno", "pptyp", "pagey", "pgend", "pwtyp"],
            "linked_trips_daysim": [
                "hhno",
                "pno",
                "day",
                "tripno",
                "mode",
                "pathtype",
                "dorp",
                "opurp",
                "dpurp",
            ],
            "tours_daysim": ["hhno", "pno", "day", "tour", "pdpurp"],
        }
        for table, columns in expected_columns.items():
            for col in columns:
                assert col in result[table].columns, f"{table} is missing {col}"
