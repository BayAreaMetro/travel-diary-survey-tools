"""Unit tests for DaySim formatter.

Tests person type classification, household composition, mode aggregation,
tour formatting, and end-to-end transformation from canonical survey data
to DaySim model format.
"""

import polars as pl

from data_canon.codebook.days import TravelDow
from data_canon.codebook.daysim import (
    DaysimPersonType,
)
from data_canon.codebook.households import (
    IncomeDetailed,
    IncomeFollowup,
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
    Mode,
)
from processing.formatting.daysim.format_households import format_households
from processing.formatting.daysim.format_persons import (
    compute_day_completeness,
    format_persons,
)
from tests.fixtures import (
    create_day,
    create_household,
    create_person,
)


class TestDayCompleteness:
    """Tests for day completeness computation."""

    def test_compute_day_completeness_single_person_weekday(self):
        """Test day completeness for single person with one complete weekday."""
        days = pl.DataFrame(
            [
                create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.MONDAY,
                    is_complete=True,
                )
            ]
        )

        result = compute_day_completeness(days)

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
        days = pl.DataFrame(
            [
                create_day(
                    day_id=i,
                    person_id=201,
                    hh_id=2,
                    person_num=1,
                    day_num=i,
                    travel_dow=TravelDow(i),
                    is_complete=True,
                )
                for i in range(1, 8)
            ]
        )

        result = compute_day_completeness(days)

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
        days = pl.DataFrame(
            [
                create_day(
                    day_id=1,
                    person_id=301,
                    hh_id=3,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.TUESDAY,
                    is_complete=True,
                ),
                create_day(
                    day_id=2,
                    person_id=301,
                    hh_id=3,
                    person_num=1,
                    day_num=2,
                    travel_dow=TravelDow.WEDNESDAY,
                    is_complete=False,
                ),
                create_day(
                    day_id=3,
                    person_id=301,
                    hh_id=3,
                    person_num=1,
                    day_num=3,
                    travel_dow=TravelDow.THURSDAY,
                    is_complete=True,
                ),
            ]
        )

        result = compute_day_completeness(days)

        assert len(result) == 1
        assert result["tue_complete"][0] == 1
        assert result["wed_complete"][0] == 0
        assert result["thu_complete"][0] == 1
        assert result["num_days_complete_3dayweekday"][0] == 2  # Tue+Thu only

    def test_compute_day_completeness_multiple_persons(self):
        """Test day completeness with multiple persons."""
        days = pl.DataFrame(
            [
                create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    travel_dow=TravelDow.MONDAY,
                    is_complete=True,
                ),
                create_day(
                    day_id=2,
                    person_id=102,
                    hh_id=1,
                    person_num=2,
                    travel_dow=TravelDow.MONDAY,
                    is_complete=False,
                ),
            ]
        )

        result = compute_day_completeness(days)

        assert len(result) == 2
        assert result.filter(pl.col("pno") == 1)["mon_complete"][0] == 1
        assert result.filter(pl.col("pno") == 2)["mon_complete"][0] == 0


class TestPersonFormatting:
    """Tests for person formatting and type classification."""

    def test_format_persons_full_time_worker(self):
        """Test person formatting for full-time worker."""
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    employment=Employment.EMPLOYED_FULLTIME,
                    age=AgeCategory.AGE_35_TO_44,
                    work_mode=Mode.HOUSEHOLD_VEHICLE_1,
                    work_taz=200,
                    work_maz=2000,
                    days=[{"day_id": 1, "person_id": 101, "is_complete": True}],
                )
            ]
        )

        days = pl.DataFrame(
            [
                create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.MONDAY,
                    is_complete=True,
                )
            ]
        )

        result = format_persons(persons, days)

        assert len(result) == 1
        assert result["hhno"][0] == 1
        assert result["pno"][0] == 1
        assert result["pptyp"][0] == DaysimPersonType.FULL_TIME_WORKER.value
        assert result["pwtyp"][0] == 1  # Full-time worker
        assert result["pagey"][0] == 40  # Midpoint of AGE_35_TO_44
        assert result["pwtaz"][0] == 200
        assert result["pwpcl"][0] == 2000

    def test_format_persons_part_time_worker(self):
        """Test person formatting for part-time worker."""
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=DaysimPersonType.PART_TIME_WORKER,
                    employment=Employment.EMPLOYED_PARTTIME,
                    age=AgeCategory.AGE_25_TO_34,
                    work_taz=200,
                    work_maz=2000,
                    work_mode=Mode.HOUSEHOLD_VEHICLE_1,
                    is_proxy=False,
                    num_days_complete=1,
                )
            ]
        )

        days = pl.DataFrame(
            [
                create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.TUESDAY,
                    is_complete=True,
                )
            ]
        )

        result = format_persons(persons, days)

        assert result["pptyp"][0] == DaysimPersonType.PART_TIME_WORKER.value
        assert result["pwtyp"][0] == 2  # Part-time worker

    def test_format_persons_university_student(self):
        """Test person formatting for university student."""
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=DaysimPersonType.UNIVERSITY_STUDENT,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.FULLTIME_INPERSON,
                    age=AgeCategory.AGE_18_TO_24,
                    work_lat=None,
                    work_lon=None,
                    work_taz=None,
                    work_maz=None,
                    school_taz=300,
                    school_maz=3000,
                    school_type=SchoolType.COLLEGE_4YEAR,
                    work_mode=Mode.MISSING,
                    is_proxy=False,
                    num_days_complete=1,
                )
            ]
        )

        days = pl.DataFrame(
            [
                create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.WEDNESDAY,
                    is_complete=True,
                )
            ]
        )

        result = format_persons(persons, days)

        assert result["pptyp"][0] == DaysimPersonType.UNIVERSITY_STUDENT.value
        assert result["pwtaz"][0] == -1  # No work location
        assert result["pstaz"][0] == 300  # School location
        assert result["pspcl"][0] == 3000

    def test_format_persons_high_school_student(self):
        """Test person formatting for high school student."""
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=DaysimPersonType.CHILD_DRIVING_AGE,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.FULLTIME_INPERSON,
                    age=AgeCategory.AGE_16_TO_17,
                    work_lat=None,
                    work_lon=None,
                    work_taz=None,
                    work_maz=None,
                    school_taz=150,
                    school_maz=1500,
                    school_type=SchoolType.HIGH_SCHOOL,
                    work_mode=Mode.MISSING,
                    is_proxy=False,
                    num_days_complete=1,
                )
            ]
        )

        days = pl.DataFrame(
            [
                create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.THURSDAY,
                    is_complete=True,
                )
            ]
        )

        result = format_persons(persons, days)

        assert result["pptyp"][0] == DaysimPersonType.CHILD_DRIVING_AGE.value
        assert result["pstaz"][0] == 150
        assert result["pspcl"][0] == 1500

    def test_format_persons_retiree(self):
        """Test person formatting for retiree."""
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    person_type=DaysimPersonType.RETIRED,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.NONSTUDENT,
                    age=AgeCategory.AGE_65_TO_74,
                    work_lat=None,
                    work_lon=None,
                    work_taz=None,
                    work_maz=None,
                    work_mode=Mode.MISSING,
                    is_proxy=False,
                    num_days_complete=1,
                )
            ]
        )

        days = pl.DataFrame(
            [
                create_day(
                    day_id=1,
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    day_num=1,
                    travel_dow=TravelDow.FRIDAY,
                    is_complete=True,
                )
            ]
        )

        result = format_persons(persons, days)

        assert result["pptyp"][0] == DaysimPersonType.RETIRED.value
        assert result["pwtaz"][0] == -1

    def test_format_persons_non_working_adult(self):
        """Test person formatting for non-working adult."""
        days_list = [
            create_day(
                day_id=1,
                person_id=101,
                hh_id=1,
                person_num=1,
                day_num=1,
                travel_dow=TravelDow.SATURDAY,
                is_complete=True,
            )
        ]
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    age=AgeCategory.AGE_25_TO_34,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.NONSTUDENT,
                    days=days_list,
                )
            ]
        )

        days = pl.DataFrame(days_list)

        result = format_persons(persons, days)

        assert result["pptyp"][0] == DaysimPersonType.NON_WORKER.value

    def test_format_persons_child_non_driving(self):
        """Test person formatting for child aged 5-15."""
        days_list = [
            create_day(
                day_id=1,
                person_id=101,
                hh_id=1,
                person_num=1,
                day_num=1,
                travel_dow=TravelDow.SUNDAY,
                is_complete=True,
            )
        ]
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    age=AgeCategory.AGE_5_TO_15,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.NONSTUDENT,
                    days=days_list,
                )
            ]
        )

        days = pl.DataFrame(days_list)

        result = format_persons(persons, days)

        assert result["pptyp"][0] == DaysimPersonType.CHILD_NON_DRIVING_AGE.value
        assert result["pagey"][0] == 10

    def test_format_persons_child_under_5(self):
        """Test person formatting for child under 5."""
        days_list = [
            create_day(
                day_id=1,
                person_id=101,
                hh_id=1,
                person_num=1,
                day_num=1,
                travel_dow=TravelDow.MONDAY,
                is_complete=True,
            )
        ]
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    age=AgeCategory.AGE_UNDER_5,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.NONSTUDENT,
                    days=days_list,
                )
            ]
        )

        days = pl.DataFrame(days_list)

        result = format_persons(persons, days)

        assert result["pptyp"][0] == DaysimPersonType.CHILD_UNDER_5.value
        assert result["pagey"][0] == 3

    def test_format_persons_with_day_completeness(self):
        """Test person formatting with day completeness indicators."""
        days_list = [
            create_day(
                day_id=1,
                person_id=101,
                hh_id=1,
                person_num=1,
                day_num=1,
                travel_dow=TravelDow.MONDAY,
                is_complete=True,
            ),
            create_day(
                day_id=2,
                person_id=101,
                hh_id=1,
                person_num=1,
                day_num=2,
                travel_dow=TravelDow.TUESDAY,
                is_complete=True,
            ),
            create_day(
                day_id=3,
                person_id=101,
                hh_id=1,
                person_num=1,
                day_num=3,
                travel_dow=TravelDow.WEDNESDAY,
                is_complete=True,
            ),
            create_day(
                day_id=4,
                person_id=101,
                hh_id=1,
                person_num=1,
                day_num=4,
                travel_dow=TravelDow.THURSDAY,
                is_complete=False,
            ),
            create_day(
                day_id=5,
                person_id=101,
                hh_id=1,
                person_num=1,
                day_num=5,
                travel_dow=TravelDow.FRIDAY,
                is_complete=False,
            ),
        ]
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    age=AgeCategory.AGE_35_TO_44,
                    employment=Employment.EMPLOYED_FULLTIME,
                    days=days_list,
                )
            ]
        )

        days = pl.DataFrame(days_list)

        result = format_persons(persons, days)

        assert "mon_complete" in result.columns
        assert result["mon_complete"][0] == 1
        assert result["tue_complete"][0] == 1
        assert result["wed_complete"][0] == 1
        # Because Thu is incomplete, only 2 complete days in Tue-Wed-Thu
        assert result["num_days_complete_3dayweekday"][0] == 2
        # Because Thu and Fri are incomplete, only 3 complete days in Mon-Thu
        assert result["num_days_complete_4dayweekday"][0] == 3
        # Because Thu and Fri are incomplete, only 3 complete days in Mon-Fri
        assert result["num_days_complete_5dayweekday"][0] == 3

    def test_format_persons_gender_mapping(self):
        """Test gender code mapping."""
        days_list = [
            create_day(
                day_id=1,
                person_id=101,
                hh_id=1,
                person_num=1,
                day_num=1,
                travel_dow=TravelDow.MONDAY,
                is_complete=True,
            ),
            create_day(
                day_id=2,
                person_id=102,
                hh_id=1,
                person_num=2,
                day_num=1,
                travel_dow=TravelDow.MONDAY,
                is_complete=True,
            ),
        ]
        persons = pl.DataFrame(
            [
                create_person(
                    person_id=101,
                    hh_id=1,
                    person_num=1,
                    age=AgeCategory.AGE_35_TO_44,
                    employment=Employment.EMPLOYED_FULLTIME,
                    gender=Gender.MALE,
                    days=[d for d in days_list if d["person_id"] == 101],
                ),
                create_person(
                    person_id=102,
                    hh_id=1,
                    person_num=2,
                    age=AgeCategory.AGE_35_TO_44,
                    employment=Employment.EMPLOYED_FULLTIME,
                    gender=Gender.FEMALE,
                    days=[d for d in days_list if d["person_id"] == 102],
                ),
            ]
        )

        days = pl.DataFrame(days_list)

        result = format_persons(persons, days)

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
                    income_detailed=IncomeDetailed.INCOME_75TO100,
                    residence_rent_own=ResidenceRentOwn.OWN,
                    residence_type=ResidenceType.SFH,
                    num_workers=1,
                )
            ]
        )

        # Create persons table, then format to get persons_daysim
        days_list = [
            create_day(
                day_id=1,
                person_id=101,
                hh_id=1,
                person_num=1,
                day_num=1,
                travel_dow=TravelDow.MONDAY,
                is_complete=True,
            )
        ]
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

        days = pl.DataFrame(days_list)

        persons_daysim = format_persons(persons, days)

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
                {
                    "hhno": 1,
                    "pno": 1,
                    "pptyp": DaysimPersonType.FULL_TIME_WORKER.value,
                    "pwtyp": 1,
                },
                {
                    "hhno": 1,
                    "pno": 2,
                    "pptyp": DaysimPersonType.PART_TIME_WORKER.value,
                    "pwtyp": 2,
                },
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

    def test_format_households_income_detailed(self):
        """Test income mapping from detailed income field."""
        households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_maz=1000,
                    income_detailed=IncomeDetailed.INCOME_50TO75,
                    income_followup=None,
                    residence_rent_own=ResidenceRentOwn.OWN,
                    residence_type=ResidenceType.SFH,
                )
            ]
        )

        persons_daysim = pl.DataFrame([{"hhno": 1, "pno": 1, "pptyp": 1, "pwtyp": 1}])

        result = format_households(households, persons_daysim)

        # Should use income_detailed midpoint (approximately 62500)
        assert result["hhincome"][0] > 50000
        assert result["hhincome"][0] < 75000

    def test_format_households_income_followup(self):
        """Test income mapping from followup income field."""
        households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_maz=1000,
                    income_detailed=None,
                    income_followup=IncomeFollowup.INCOME_100TO200,
                    residence_rent_own=ResidenceRentOwn.OWN,
                    residence_type=ResidenceType.SFH,
                )
            ]
        )

        persons_daysim = pl.DataFrame([{"hhno": 1, "pno": 1, "pptyp": 1, "pwtyp": 1}])

        result = format_households(households, persons_daysim)

        # Should use income_followup midpoint (150000 for INCOME_100TO200)
        assert result["hhincome"][0] == 150000

    def test_format_households_multiple_households(self):
        """Test formatting multiple households."""
        households = pl.DataFrame(
            [
                create_household(
                    hh_id=1,
                    home_taz=100,
                    home_maz=1000,
                    residence_rent_own=ResidenceRentOwn.OWN,
                    residence_type=ResidenceType.SFH,
                ),
                create_household(
                    hh_id=2,
                    home_taz=200,
                    home_maz=2000,
                    residence_rent_own=ResidenceRentOwn.RENT,
                    residence_type=ResidenceType.MULTIFAMILY,
                ),
            ]
        )

        persons_daysim = pl.DataFrame(
            [
                {"hhno": 1, "pno": 1, "pptyp": 1, "pwtyp": 1},
                {"hhno": 2, "pno": 1, "pptyp": 3, "pwtyp": 0},
            ]
        )

        result = format_households(households, persons_daysim)

        assert len(result) == 2
        assert result.filter(pl.col("hhno") == 1)["hhtaz"][0] == 100
        assert result.filter(pl.col("hhno") == 2)["hhtaz"][0] == 200
