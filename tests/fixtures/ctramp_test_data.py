"""Test fixtures for CT-RAMP formatter tests.

Provides factory methods for creating complete, valid canonical survey data
that can be used to test CT-RAMP formatting functions.
"""

import polars as pl

from data_canon.codebook.ctramp import WalkToTransitSubZone
from data_canon.codebook.households import (
    IncomeDetailed,
    IncomeFollowup,
)
from data_canon.codebook.persons import (
    CommuteSubsidy,
    Employment,
    Gender,
    SchoolType,
    Student,
)


class CTRAMPTestDataBuilder:
    """Factory for creating canonical survey data for CTRAMP formatter tests."""

    @staticmethod
    def create_household(
        hh_id: int = 1,
        home_taz: int = 100,
        home_walk_subzone: int = WalkToTransitSubZone.SHORT_WALK.value,
        num_people: int = 1,
        num_vehicles: int = 1,
        num_workers: int = 1,
        income_detailed: IncomeDetailed | None = IncomeDetailed.INCOME_75TO100,
        income_followup: IncomeFollowup | None = None,
        hh_weight: float = 1.0,
        **overrides,
    ) -> dict:
        """Create a complete canonical household record.

        Args:
            hh_id: Household ID
            home_taz: Home TAZ
            home_walk_subzone: Walk-to-transit subzone (0/1/2)
            num_people: Household size
            num_vehicles: Number of vehicles
            num_workers: Number of workers
            income_detailed: Detailed income category
            income_followup: Followup income category (if detailed is null)
            hh_weight: Household expansion factor
            **overrides: Override any default values

        Returns:
            Complete household record dict
        """
        return {
            "hh_id": hh_id,
            "home_taz": home_taz,
            "home_walk_subzone": home_walk_subzone,
            "num_people": num_people,
            "num_vehicles": num_vehicles,
            "num_workers": num_workers,
            "income_detailed": income_detailed.value
            if income_detailed
            else None,
            "income_followup": income_followup.value
            if income_followup
            else None,
            "hh_weight": hh_weight,
            **overrides,
        }

    @staticmethod
    def create_person(
        person_id: int = 101,
        hh_id: int = 1,
        person_num: int = 1,
        age: int = 35,
        gender: Gender = Gender.MALE,
        employment: Employment = Employment.EMPLOYED_FULLTIME,
        student: Student = Student.NONSTUDENT,
        school_type: SchoolType = SchoolType.MISSING,
        commute_subsidy: CommuteSubsidy = CommuteSubsidy.NONE,
        value_of_time: float = 15.0,
        **overrides,
    ) -> dict:
        """Create a complete canonical person record.

        Args:
            person_id: Person ID
            hh_id: Household ID
            person_num: Person number within household
            age: Person age
            gender: Gender enumeration
            employment: Employment status
            student: Student status
            school_type: Type of school attending
            commute_subsidy: Commute subsidy type
            value_of_time: Value of time ($/hour)
            **overrides: Override any default values

        Returns:
            Complete person record dict
        """
        return {
            "person_id": person_id,
            "hh_id": hh_id,
            "person_num": person_num,
            "age": age,
            "gender": gender.value,
            "employment": employment.value,
            "student": student.value,
            "school_type": school_type.value,
            "commute_subsidy": commute_subsidy.value,
            "value_of_time": value_of_time,
            **overrides,
        }


class CTRAMPScenarioBuilder:
    """Builder for creating CT-RAMP test scenarios with related data."""

    @staticmethod
    def create_single_adult_household():
        """Create a single full-time worker household."""
        household = pl.DataFrame(
            [
                CTRAMPTestDataBuilder.create_household(
                    hh_id=1,
                    home_taz=100,
                    num_people=1,
                    num_vehicles=1,
                    num_workers=1,
                    income_detailed=IncomeDetailed.INCOME_75TO100,
                )
            ]
        )

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

        return household, persons

    @staticmethod
    def create_family_household():
        """Create a household with working adults and school-age children."""
        household = pl.DataFrame(
            [
                CTRAMPTestDataBuilder.create_household(
                    hh_id=2,
                    home_taz=200,
                    num_people=4,
                    num_vehicles=2,
                    num_workers=2,
                    income_detailed=IncomeDetailed.INCOME_100TO150,
                )
            ]
        )

        persons = pl.DataFrame(
            [
                # Full-time worker (parent 1)
                CTRAMPTestDataBuilder.create_person(
                    person_id=201,
                    hh_id=2,
                    person_num=1,
                    age=40,
                    gender=Gender.FEMALE,
                    employment=Employment.EMPLOYED_FULLTIME,
                    student=Student.NONSTUDENT,
                    commute_subsidy=CommuteSubsidy.TRANSIT,
                ),
                # Part-time worker (parent 2)
                CTRAMPTestDataBuilder.create_person(
                    person_id=202,
                    hh_id=2,
                    person_num=2,
                    age=38,
                    gender=Gender.MALE,
                    employment=Employment.EMPLOYED_PARTTIME,
                    student=Student.NONSTUDENT,
                    commute_subsidy=CommuteSubsidy.NONE,
                ),
                # High school student (driving age)
                CTRAMPTestDataBuilder.create_person(
                    person_id=203,
                    hh_id=2,
                    person_num=3,
                    age=16,
                    gender=Gender.FEMALE,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.FULLTIME_INPERSON,
                    school_type=SchoolType.HIGH_SCHOOL,
                ),
                # Elementary school student (non-driving age)
                CTRAMPTestDataBuilder.create_person(
                    person_id=204,
                    hh_id=2,
                    person_num=4,
                    age=10,
                    gender=Gender.MALE,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.FULLTIME_INPERSON,
                    school_type=SchoolType.ELEMENTARY,
                ),
            ]
        )

        return household, persons

    @staticmethod
    def create_retired_household():
        """Create a retired couple household."""
        household = pl.DataFrame(
            [
                CTRAMPTestDataBuilder.create_household(
                    hh_id=3,
                    home_taz=300,
                    num_people=2,
                    num_vehicles=1,
                    num_workers=0,
                    income_detailed=IncomeDetailed.INCOME_50TO75,
                )
            ]
        )

        persons = pl.DataFrame(
            [
                CTRAMPTestDataBuilder.create_person(
                    person_id=301,
                    hh_id=3,
                    person_num=1,
                    age=70,
                    gender=Gender.MALE,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.NONSTUDENT,
                ),
                CTRAMPTestDataBuilder.create_person(
                    person_id=302,
                    hh_id=3,
                    person_num=2,
                    age=68,
                    gender=Gender.FEMALE,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.NONSTUDENT,
                ),
            ]
        )

        return household, persons

    @staticmethod
    def create_university_student_household():
        """Create a university student household."""
        household = pl.DataFrame(
            [
                CTRAMPTestDataBuilder.create_household(
                    hh_id=4,
                    home_taz=400,
                    num_people=1,
                    num_vehicles=0,
                    num_workers=0,
                    income_detailed=IncomeDetailed.INCOME_UNDER15,
                )
            ]
        )

        persons = pl.DataFrame(
            [
                CTRAMPTestDataBuilder.create_person(
                    person_id=401,
                    hh_id=4,
                    person_num=1,
                    age=20,
                    gender=Gender.FEMALE,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.FULLTIME_INPERSON,
                    school_type=SchoolType.COLLEGE_4YEAR,
                )
            ]
        )

        return household, persons

    @staticmethod
    def create_young_family_household():
        """Create a family with young children."""
        household = pl.DataFrame(
            [
                CTRAMPTestDataBuilder.create_household(
                    hh_id=5,
                    home_taz=500,
                    num_people=3,
                    num_vehicles=1,
                    num_workers=1,
                    income_detailed=IncomeDetailed.INCOME_100TO150,
                )
            ]
        )

        persons = pl.DataFrame(
            [
                # Working parent
                CTRAMPTestDataBuilder.create_person(
                    person_id=501,
                    hh_id=5,
                    person_num=1,
                    age=32,
                    gender=Gender.FEMALE,
                    employment=Employment.EMPLOYED_FULLTIME,
                    student=Student.NONSTUDENT,
                ),
                # Preschool child
                CTRAMPTestDataBuilder.create_person(
                    person_id=502,
                    hh_id=5,
                    person_num=2,
                    age=4,
                    gender=Gender.MALE,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.NONSTUDENT,
                    school_type=SchoolType.PRESCHOOL,
                ),
                # School-age child
                CTRAMPTestDataBuilder.create_person(
                    person_id=503,
                    hh_id=5,
                    person_num=3,
                    age=7,
                    gender=Gender.FEMALE,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.FULLTIME_INPERSON,
                    school_type=SchoolType.ELEMENTARY,
                ),
            ]
        )

        return household, persons
