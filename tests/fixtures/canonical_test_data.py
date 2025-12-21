"""Unified test fixtures for creating canonical survey data.

This module provides a single, comprehensive factory for creating canonical
survey data that can be used across all formatter tests (CTRAMP, Daysim, etc.).
All formatters consume canonical data, so having one unified builder ensures
consistency and reduces duplication.
"""

from datetime import UTC, datetime, time, timedelta
from typing import ClassVar

import polars as pl
import pytest

from data_canon.codebook.days import TravelDow
from data_canon.codebook.generic import BooleanYesNo, LocationType
from data_canon.codebook.households import (
    IncomeDetailed,
    IncomeFollowup,
    ResidenceRentOwn,
    ResidenceType,
)
from data_canon.codebook.persons import (
    AgeCategory,
    CommuteSubsidy,
    Employment,
    Gender,
    PersonType,
    SchoolType,
    Student,
    WorkParking,
)
from data_canon.codebook.tours import (
    TourCategory,
    TourDataQuality,
    TourDirection,
)
from data_canon.codebook.trips import (
    AccessEgressMode,
    Driver,
    Mode,
    ModeType,
    Purpose,
    PurposeCategory,
)

# Import processing modules for pytest fixtures
from processing.link_trips import link_trips
from processing.tours import extract_tours


class CanonicalTestDataBuilder:
    """Factory for creating canonical survey data for all formatter tests.

    This builder creates data in the canonical format - the intermediate format
    after survey processing but before model-specific formatting (CTRAMP,
    Daysim). All factory methods include sensible defaults and support
    **overrides for flexibility.
    """

    # Constants for common coordinates
    DEFAULT_COORDS: ClassVar[dict[str, tuple[float, float]]] = {
        "home": (37.70, -122.40),
        "work": (37.75, -122.45),
        "bart_home": (37.71, -122.41),
        "bart_work": (37.74, -122.44),
    }

    # Constants for transit modes
    DEFAULT_TRANSIT_MODE_CODES: ClassVar[list[int]] = [
        ModeType.TRANSIT.value,
        ModeType.FERRY.value,
        ModeType.LONG_DISTANCE.value,
    ]

    @staticmethod
    def _to_value(field):
        """Convert enum to value, handling None and non-enum types."""
        return field.value if hasattr(field, "value") else field

    @staticmethod
    def _add_optional_fields(record: dict, **optional_fields) -> None:
        """Add fields to record only if they are not None."""
        for field_name, field_value in optional_fields.items():
            if field_value is not None:
                record[field_name] = (
                    field_value.value
                    if hasattr(field_value, "value")
                    else field_value
                )

    @staticmethod
    def _default_times(
        depart_time, arrive_time, default_depart_hour=8, travel_minutes=30
    ):
        """Set default departure and arrival times."""
        if depart_time is None:
            depart_time = datetime.combine(
                datetime.now(tz=UTC).date(), time(default_depart_hour, 0)
            )
        if arrive_time is None:
            arrive_time = depart_time + timedelta(minutes=travel_minutes)
        return depart_time, arrive_time

    @staticmethod
    def create_household(
        hh_id: int = 1,
        home_lat: float | None = 37.70,
        home_lon: float | None = -122.40,
        home_taz: int = 100,
        home_maz: int | None = None,
        home_walk_subzone: int | None = None,
        num_people: int = 1,
        num_vehicles: int = 1,
        num_workers: int = 1,
        income_detailed: IncomeDetailed | None = IncomeDetailed.INCOME_75TO100,
        income_followup: IncomeFollowup | None = None,
        residence_type: ResidenceType | None = None,
        residence_rent_own: ResidenceRentOwn | None = None,
        hh_weight: float = 1.0,
        **overrides,
    ) -> dict:
        """Create a complete canonical household record.

        Includes all fields that may be required by various formatters.
        Optional fields (like home_maz, home_walk_subzone) are only included
        if explicitly provided or required by the formatter being tested.

        Args:
            hh_id: Household ID
            home_lat: Home latitude (optional, for MAZ-based models)
            home_lon: Home longitude (optional, for MAZ-based models)
            home_taz: Home TAZ (required for most models)
            home_maz: Home MAZ (optional, for Daysim)
            home_walk_subzone: Walk-to-transit subzone 0/1/2 (CTRAMP)
            num_people: Household size
            num_vehicles: Number of vehicles
            num_workers: Number of workers
            income_detailed: Detailed income category
            income_followup: Followup income category (if detailed is null)
            residence_type: Residence type (optional, for Daysim)
            residence_rent_own: Residence rent/own status (optional, for Daysim)
            hh_weight: Household expansion factor
            **overrides: Override any default values

        Returns:
            Complete household record dict
        """
        record = {
            "hh_id": hh_id,
            "home_taz": home_taz,
            "num_people": num_people,
            "num_vehicles": num_vehicles,
            "num_workers": num_workers,
            "income_detailed": CanonicalTestDataBuilder._to_value(
                income_detailed
            ),
            "income_followup": CanonicalTestDataBuilder._to_value(
                income_followup
            ),
            "hh_weight": hh_weight,
        }

        # Add optional fields only if provided
        CanonicalTestDataBuilder._add_optional_fields(
            record,
            home_lat=home_lat,
            home_lon=home_lon,
            home_maz=home_maz,
            home_walk_subzone=home_walk_subzone,
            residence_type=residence_type,
            residence_rent_own=residence_rent_own,
        )

        return {**record, **overrides}

    @staticmethod
    def create_person(  # noqa: C901, PLR0912, PLR0915
        person_id: int = 101,
        hh_id: int = 1,
        person_num: int = 1,
        age: AgeCategory | int = AgeCategory.AGE_35_TO_44,
        gender: Gender = Gender.MALE,
        employment: Employment | None = None,
        student: Student | None = None,
        school_type: SchoolType = SchoolType.MISSING,
        commute_subsidy: CommuteSubsidy = CommuteSubsidy.NONE,
        value_of_time: float = 15.0,
        # Optional location fields
        home_lat: float | None = None,
        home_lon: float | None = None,
        work_lat: float | None = None,
        work_lon: float | None = None,
        work_taz: int | None = None,
        work_maz: int | None = None,
        school_lat: float | None = None,
        school_lon: float | None = None,
        school_taz: int | None = None,
        school_maz: int | None = None,
        # Optional Daysim-specific fields
        work_parking: WorkParking | None = None,
        transit_pass: bool | None = None,
        usual_work_mode: Mode | None = None,
        # Legacy DaySim compatibility parameters
        person_type: PersonType | None = None,
        age_years: int | None = None,
        work_park: WorkParking | None = None,
        work_mode: Mode | None = None,
        is_proxy: bool | None = None,
        num_complete_days: int | None = None,
        **overrides,
    ) -> dict:
        """Create a complete canonical person record.

        Args:
            person_id: Person ID
            hh_id: Household ID
            person_num: Person number within household
            age: Person age category (AgeCategory enum) or legacy int
            gender: Gender enumeration
            employment: Employment status (auto-derived if None)
            student: Student status (auto-derived from person_type if None)
            school_type: Type of school (if student)
            commute_subsidy: Commute subsidy availability
            value_of_time: Value of time in $/hour
            home_lat: Home latitude (optional)
            home_lon: Home longitude (optional)
            work_lat: Work latitude (optional, defaults for workers)
            work_lon: Work longitude (optional, defaults for workers)
            work_taz: Work TAZ (optional)
            work_maz: Work MAZ (optional, for Daysim)
            school_lat: School latitude (optional)
            school_lon: School longitude (optional)
            school_taz: School TAZ (optional)
            school_maz: School MAZ (optional, for Daysim)
            work_parking: Work parking type (optional, for Daysim)
            transit_pass: Has transit pass (optional, for Daysim)
            usual_work_mode: Usual work mode (optional, for Daysim)
            person_type: Legacy DaySim person type (for auto-derivation)
            age_years: Legacy alias for age
            work_park: Legacy alias for work_parking
            work_mode: Legacy alias for usual_work_mode
            is_proxy: Legacy DaySim field
            num_complete_days: Legacy DaySim field
            **overrides: Override any default values

        Returns:
            Complete person record dict
        """
        # Handle legacy parameter aliases
        if age_years is not None:
            age = age_years
        if work_park is not None:
            work_parking = work_park
        if work_mode is not None:
            usual_work_mode = work_mode

        # Auto-derive employment, student, age from person_type if provided
        if person_type is not None:
            if employment is None:
                if person_type == PersonType.FULL_TIME_WORKER:
                    employment = Employment.EMPLOYED_FULLTIME
                elif person_type == PersonType.PART_TIME_WORKER:
                    employment = Employment.EMPLOYED_PARTTIME
                else:
                    employment = Employment.UNEMPLOYED_NOT_LOOKING

            if student is None:
                if person_type in [
                    PersonType.UNIVERSITY_STUDENT,
                    PersonType.HIGH_SCHOOL_STUDENT,
                ]:
                    student = Student.FULLTIME_INPERSON
                else:
                    student = Student.NONSTUDENT

            # Set default work coordinates for workers if not provided
            if person_type in [
                PersonType.FULL_TIME_WORKER,
                PersonType.PART_TIME_WORKER,
            ]:
                if work_lat is None:
                    work_lat = 37.75
                if work_lon is None:
                    work_lon = -122.45
                # Set default work_parking for workers
                if work_parking is None:
                    if work_taz is not None:
                        work_parking = WorkParking.FREE
                    else:
                        work_parking = WorkParking.NOT_APPLICABLE
            # Non-workers get NOT_APPLICABLE
            elif work_parking is None:
                work_parking = WorkParking.NOT_APPLICABLE

        # Set defaults if still None
        if employment is None:
            employment = Employment.EMPLOYED_FULLTIME
        if student is None:
            student = Student.NONSTUDENT

        record = {
            "person_id": person_id,
            "hh_id": hh_id,
            "person_num": person_num,
            "age": age.value if isinstance(age, AgeCategory) else age,
            "gender": gender.value,
            "employment": employment.value,
            "student": student.value,
            "school_type": school_type.value,
            "commute_subsidy": commute_subsidy.value,
            "value_of_time": value_of_time,
        }

        # For DaySim compatibility: if person_type was provided, always include
        # all location fields (even if None) since DaySim formatter expects them
        if person_type is not None:
            record["home_lat"] = home_lat
            record["home_lon"] = home_lon
            record["work_lat"] = work_lat
            record["work_lon"] = work_lon
            record["work_taz"] = work_taz
            record["work_maz"] = work_maz
            record["school_lat"] = school_lat
            record["school_lon"] = school_lon
            record["school_taz"] = school_taz
            record["school_maz"] = school_maz
            # Also add person_type field for DaySim
            record["person_type"] = person_type.value
            record["transit_pass"] = (
                transit_pass if transit_pass is not None else False
            )
            record["is_proxy"] = is_proxy if is_proxy is not None else False
            if usual_work_mode is None:
                usual_work_mode = Mode.MISSING
            record["usual_work_mode"] = usual_work_mode.value
            record["work_mode"] = usual_work_mode.value
        else:
            # Add optional location fields only if provided
            if home_lat is not None:
                record["home_lat"] = home_lat
            if home_lon is not None:
                record["home_lon"] = home_lon
            if work_lat is not None:
                record["work_lat"] = work_lat
            if work_lon is not None:
                record["work_lon"] = work_lon
            if work_taz is not None:
                record["work_taz"] = work_taz
            if work_maz is not None:
                record["work_maz"] = work_maz
            if school_lat is not None:
                record["school_lat"] = school_lat
            if school_lon is not None:
                record["school_lon"] = school_lon
            if school_taz is not None:
                record["school_taz"] = school_taz
            if school_maz is not None:
                record["school_maz"] = school_maz

        # Add Daysim-specific fields if provided
        if work_parking is not None:
            record["work_parking"] = work_parking.value
            if person_type is not None:
                record["work_park"] = work_parking.value
        if transit_pass is not None and person_type is None:
            # Only add if not already added via person_type path
            record["transit_pass"] = transit_pass
        if usual_work_mode is not None and person_type is None:
            # Only add if not already added via person_type path
            record["usual_work_mode"] = usual_work_mode.value
        if is_proxy is not None and person_type is None:
            # Only add if not already added via person_type path
            record["is_proxy"] = is_proxy
        if num_complete_days is not None:
            record["num_complete_days"] = num_complete_days

        return {**record, **overrides}

    @staticmethod
    def prepare_person_for_daysim(
        person_dict: dict, days: list[dict] | None = None
    ) -> dict:
        """Prepare a canonical person record for DaySim formatter.

        The DaySim formatter has specific requirements:
        1. Age must be an AgeCategory enum value (not actual age)
        2. Requires work_park field (not work_parking)
        3. Requires work_mode, transit_pass, is_proxy fields
        4. Requires num_days_complete computed from days
        5. All location fields must exist (even if None)

        Args:
            person_dict: Person record from create_person()
            days: Day records for this person (computes num_days_complete)

        Returns:
            Person record ready for DaySim formatter
        """
        record = person_dict.copy()

        # Add work_park field (DaySim legacy name for work_parking)
        if "work_park" not in record:
            if record.get("work_taz") is not None:
                record["work_park"] = WorkParking.FREE.value
            else:
                record["work_park"] = WorkParking.NOT_APPLICABLE.value

        # Add work_mode field if not present
        if "work_mode" not in record:
            record["work_mode"] = Mode.MISSING.value

        # Ensure all location fields exist (formatter expects them)
        for field in [
            "work_lat",
            "work_lon",
            "work_taz",
            "work_maz",
            "school_lat",
            "school_lon",
            "school_taz",
            "school_maz",
        ]:
            if field not in record:
                record[field] = None

        # Add transit_pass and is_proxy with defaults
        if "transit_pass" not in record:
            record["transit_pass"] = BooleanYesNo.NO.value
        if "is_proxy" not in record:
            record["is_proxy"] = BooleanYesNo.NO.value

        # Compute num_days_complete from days if provided
        if days is not None:
            num_complete = sum(
                1 for day in days if day.get("is_complete", False)
            )
            record["num_days_complete"] = num_complete
        elif "num_days_complete" not in record:
            # Default to 0 if no days provided and not already set
            record["num_days_complete"] = 0

        return record

    @staticmethod
    def prepare_household_for_daysim(household_dict: dict) -> dict:
        """Prepare a canonical household record for DaySim formatter.

        The DaySim formatter has specific requirements:
        1. Requires home_maz field (even if None)
        2. Requires residence_type and residence_rent_own fields

        Args:
            household_dict: Household record from create_household()

        Returns:
            Household record ready for DaySim formatter
        """
        record = household_dict.copy()

        # Add home_maz if not present
        if "home_maz" not in record:
            record["home_maz"] = None

        # Add residence fields if not present
        if "residence_type" not in record:
            record["residence_type"] = ResidenceType.SFH.value
        if "residence_rent_own" not in record:
            record["residence_rent_own"] = ResidenceRentOwn.OWN.value

        return record

    @staticmethod
    def create_tour(
        tour_id: int = 1001,
        person_id: int = 101,
        hh_id: int = 1,
        person_num: int = 1,
        day_id: int = 1,
        day_num: int = 1,
        tour_num: int = 1,
        tour_purpose: Purpose = Purpose.PRIMARY_WORKPLACE,
        tour_category: TourCategory = TourCategory.COMPLETE,
        o_taz: int = 100,
        d_taz: int = 200,
        o_maz: int | None = None,
        d_maz: int | None = None,
        o_lat: float | None = None,
        o_lon: float | None = None,
        d_lat: float | None = None,
        d_lon: float | None = None,
        o_location_type: int = 0,
        d_location_type: int = 0,
        depart_time: datetime | None = None,
        arrive_time: datetime | None = None,
        origin_depart_time: datetime | None = None,
        origin_arrive_time: datetime | None = None,
        dest_depart_time: datetime | None = None,
        dest_arrive_time: datetime | None = None,
        travel_dow: TravelDow = TravelDow.MONDAY,
        num_trips: int = 2,
        num_travelers: int = 1,
        tour_mode: Mode = Mode.MISSING,
        student_category: str = "Not student",
        data_quality: TourDataQuality = TourDataQuality.VALID,
        joint_tour_id: int | None = None,
        parent_tour_id: int | None = None,
        **overrides,
    ) -> dict:
        """Create a complete canonical tour record.

        Args:
            tour_id: Tour ID
            person_id: Person ID
            hh_id: Household ID
            person_num: Person number
            day_id: Day ID
            day_num: Day number
            tour_num: Tour number within the day
            tour_purpose: Tour purpose
            tour_category: Tour category (mandatory/non-mandatory/at-work)
            o_taz: Origin TAZ
            d_taz: Destination TAZ
            o_maz: Origin MAZ (optional, for Daysim)
            d_maz: Destination MAZ (optional, for Daysim)
            o_lat: Origin latitude (optional)
            o_lon: Origin longitude (optional)
            d_lat: Destination latitude (optional)
            d_lon: Destination longitude (optional)
            o_location_type: Origin location type
            d_location_type: Destination location type
            depart_time: Departure time (defaults to 8 AM today)
            arrive_time: Arrival time (defaults to 5 PM today)
            origin_depart_time: Origin departure time
            origin_arrive_time: Origin arrival time
            dest_depart_time: Destination departure time
            dest_arrive_time: Destination arrival time
            travel_dow: Day of week
            num_trips: Number of trips on tour
            num_travelers: Number of people (1 individual, 2+ joint)
            tour_mode: Primary tour mode
            student_category: Student category for work/school tours
            data_quality: Data quality flag
            joint_tour_id: Joint tour ID (None for individual tours)
            parent_tour_id: Parent tour (None for primary tours)
            **overrides: Override any default values

        Returns:
            Complete tour record dict
        """
        # Default times if not provided
        if depart_time is None:
            depart_time = datetime.combine(
                datetime.now(tz=UTC).date(), time(8, 0)
            )
        if arrive_time is None:
            arrive_time = datetime.combine(
                datetime.now(tz=UTC).date(), time(17, 0)
            )
        if origin_depart_time is None:
            origin_depart_time = depart_time
        if origin_arrive_time is None:
            origin_arrive_time = depart_time + (arrive_time - depart_time) / 2
        if dest_depart_time is None:
            dest_depart_time = depart_time + (arrive_time - depart_time) / 2
        if dest_arrive_time is None:
            dest_arrive_time = arrive_time

        record = {
            "tour_id": tour_id,
            "person_id": person_id,
            "hh_id": hh_id,
            "person_num": person_num,
            "day_id": day_id,
            "day_num": day_num,
            "tour_num": tour_num,
            "tour_purpose": tour_purpose.value,
            "tour_category": tour_category.value,
            "o_taz": o_taz,
            "d_taz": d_taz,
            "o_lat": o_lat,
            "o_lon": o_lon,
            "d_lat": d_lat,
            "d_lon": d_lon,
            "o_location_type": o_location_type,
            "d_location_type": d_location_type,
            "depart_time": depart_time,
            "arrive_time": arrive_time,
            "origin_depart_time": origin_depart_time,
            "origin_arrive_time": origin_arrive_time,
            "dest_depart_time": dest_depart_time,
            "dest_arrive_time": dest_arrive_time,
            "travel_dow": travel_dow.value,
            "num_trips": num_trips,
            "num_travelers": num_travelers,
            "tour_mode": tour_mode.value,
            "student_category": student_category,
            "data_quality": data_quality.value,
            "joint_tour_id": joint_tour_id,
            "parent_tour_id": parent_tour_id,
        }

        # Add optional MAZ fields if provided
        if o_maz is not None:
            record["o_maz"] = o_maz
        if d_maz is not None:
            record["d_maz"] = d_maz

        return {**record, **overrides}

    @staticmethod
    def create_unlinked_trip(  # noqa: C901, PLR0912
        trip_id: int = 10001,
        _linked_trip_id: int | None = None,
        person_id: int = 101,
        hh_id: int = 1,
        day_id: int = 1,
        person_num: int = 1,
        day_num: int = 1,
        o_taz: int = 100,
        d_taz: int = 200,
        o_maz: int | None = None,
        d_maz: int | None = None,
        o_lat: float | None = None,
        o_lon: float | None = None,
        d_lat: float | None = None,
        d_lon: float | None = None,
        depart_time: datetime | None = None,
        arrive_time: datetime | None = None,
        travel_time: int = 30,
        duration_minutes: float | None = None,
        distance_meters: float = 1000.0,
        trip_weight: float = 1.0,
        transit_access: int = 0,
        transit_egress: int = 0,
        travel_dow: TravelDow = TravelDow.MONDAY,
        mode: Mode = Mode.HOUSEHOLD_VEHICLE,
        mode_type: ModeType = ModeType.CAR,
        o_purpose: Purpose | None = None,
        o_purpose_category: PurposeCategory | None = None,
        d_purpose: Purpose | None = None,
        d_purpose_category: PurposeCategory | None = None,
        purpose: Purpose | None = None,
        purpose_category: PurposeCategory | None = None,
        driver: Driver = Driver.DRIVER,
        access_mode: AccessEgressMode | None = None,
        egress_mode: AccessEgressMode | None = None,
        num_travelers: int = 1,
        change_mode: bool = False,
        **overrides,
    ) -> dict:
        """Create an unlinked trip record (raw trip segment before linking).

        Unlinked trips represent individual trip segments as reported by the
        traveler, before they are linked into journeys and organized into tours.
        These are the inputs to the link_trips processing step.

        Args:
            trip_id: Trip ID
            person_id: Person ID
            hh_id: Household ID
            day_id: Day ID (links trip to a specific day)
            person_num: Person number within household
            day_num: Day number in survey period
            o_taz: Origin TAZ
            d_taz: Destination TAZ
            o_maz: Origin MAZ (optional, for Daysim)
            d_maz: Destination MAZ (optional, for Daysim)
            o_lat: Origin latitude (optional)
            o_lon: Origin longitude (optional)
            d_lat: Destination latitude (optional)
            d_lon: Destination longitude (optional)
            depart_time: Departure time (defaults to 8 AM)
            arrive_time: Arrival time (defaults to 8:30 AM)
            travel_time: Travel time in minutes
            duration_minutes: Trip duration in minutes
            distance_meters: Trip distance in meters
            trip_weight: Trip expansion weight
            transit_access: Transit access flag
            transit_egress: Transit egress flag
            travel_dow: Day of week
            mode: Specific mode
            mode_type: Mode type (car/transit/walk/bike)
            o_purpose: Origin purpose (deprecated, use o_purpose_category)
            o_purpose_category: Origin purpose category (link_trips)
            d_purpose: Destination purpose (deprecated)
            d_purpose_category: Destination purpose category (link_trips)
            purpose: Legacy purpose field (backward compatibility)
            purpose_category: Legacy purpose category (backward compat)
            driver: Driver status
            access_mode: Access mode for transit (optional)
            egress_mode: Egress mode for transit (optional)
            num_travelers: Number of travelers
            change_mode: Whether this is a change mode location (for linking)
            **overrides: Override any default values

        Returns:
            Complete unlinked trip record dict (no tour_id, trip_num,
            tour_direction)
        """
        # Default times if not provided
        if depart_time is None:
            depart_time = datetime.combine(
                datetime.now(tz=UTC).date(), time(8, 0)
            )
        if arrive_time is None:
            arrive_time = datetime.combine(
                datetime.now(tz=UTC).date(), time(8, travel_time)
            )

        record = {
            "trip_id": trip_id,
            "person_id": person_id,
            "hh_id": hh_id,
            "day_id": day_id,
            "person_num": person_num,
            "day_num": day_num,
            "o_taz": o_taz,
            "d_taz": d_taz,
            "depart_time": depart_time,
            "arrive_time": arrive_time,
            "travel_time": travel_time,
            "duration_minutes": duration_minutes
            if duration_minutes is not None
            else float(travel_time),
            "distance_meters": distance_meters,
            "trip_weight": trip_weight,
            "transit_access": transit_access,
            "transit_egress": transit_egress,
            "travel_dow": travel_dow.value,
            "mode": mode.value,
            "mode_type": mode_type.value,
            "driver": driver.value,
            "num_travelers": num_travelers,
            "change_mode": change_mode,
        }

        # Add purpose fields (prefer o_/d_ versions for link_trips)
        if o_purpose_category is not None:
            record["o_purpose_category"] = o_purpose_category.value
        elif purpose_category is not None:
            # Fallback to legacy purpose_category
            record["o_purpose_category"] = purpose_category.value

        if d_purpose_category is not None:
            record["d_purpose_category"] = d_purpose_category.value
        elif purpose_category is not None:
            # Fallback to legacy purpose_category
            record["d_purpose_category"] = purpose_category.value

        if o_purpose is not None:
            record["o_purpose"] = o_purpose.value
        elif purpose is not None:
            record["o_purpose"] = purpose.value

        if d_purpose is not None:
            record["d_purpose"] = d_purpose.value
        elif purpose is not None:
            record["d_purpose"] = purpose.value

        # Legacy fields for backward compatibility
        if purpose is not None:
            record["purpose"] = purpose.value
        if purpose_category is not None:
            record["purpose_category"] = purpose_category.value

        # Add optional location fields if provided
        if o_maz is not None:
            record["o_maz"] = o_maz
        if d_maz is not None:
            record["d_maz"] = d_maz

        # Always include lat/lon fields (link_trips requires them even if None)
        record["o_lat"] = o_lat
        record["o_lon"] = o_lon
        record["d_lat"] = d_lat
        record["d_lon"] = d_lon

        # Add optional transit fields if provided
        if access_mode is not None:
            record["access_mode"] = access_mode.value
        if egress_mode is not None:
            record["egress_mode"] = egress_mode.value

        return {**record, **overrides}

    @staticmethod
    def create_linked_trip(
        linked_trip_id: int = 1,
        person_id: int = 101,
        hh_id: int = 1,
        person_num: int = 1,
        day_id: int | None = None,
        day_num: int = 1,
        travel_dow: TravelDow = TravelDow.MONDAY,
        linked_trip_num: int = 1,
        tour_id: int = 1,
        depart_time: datetime | None = None,
        arrive_time: datetime | None = None,
        origin_lat: float = 37.70,
        origin_lon: float = -122.40,
        origin_taz: int = 100,
        origin_maz: int = 1000,
        origin_purpose: PurposeCategory = PurposeCategory.HOME,
        dest_lat: float = 37.75,
        dest_lon: float = -122.45,
        dest_taz: int = 200,
        dest_maz: int = 2000,
        dest_purpose: PurposeCategory = PurposeCategory.WORK,
        mode: Mode = Mode.HOUSEHOLD_VEHICLE_1,
        mode_type: ModeType = ModeType.CAR,
        driver: Driver = Driver.DRIVER,
        num_travelers: int = 1,
        distance_meters: float = 8046.72,
        distance_miles: float | None = None,
        num_unlinked_trips: int = 1,
        tour_direction: int = 1,  # 1=OUTBOUND, 2=INBOUND
        access_mode: AccessEgressMode | None = None,
        egress_mode: AccessEgressMode | None = None,
        **overrides,
    ) -> dict:
        """Create a complete canonical linked trip record.

        Args:
            linked_trip_id: Linked trip ID
            person_id: Person ID
            hh_id: Household ID
            person_num: Person number
            day_id: Day ID (optional)
            day_num: Day number
            travel_dow: Day of week
            linked_trip_num: Linked trip number
            tour_id: Parent tour ID
            depart_time: Departure datetime
            arrive_time: Arrival datetime
            origin_lat: Origin latitude
            origin_lon: Origin longitude
            origin_taz: Origin TAZ
            origin_maz: Origin MAZ
            origin_purpose: Origin purpose
            dest_lat: Destination latitude
            dest_lon: Destination longitude
            dest_taz: Destination TAZ
            dest_maz: Destination MAZ
            dest_purpose: Destination purpose
            mode: Aggregated mode enum
            mode_type: Aggregated mode type enum
            driver: Driver status
            num_travelers: Number of travelers
            distance_meters: Trip distance in meters
            distance_miles: Trip distance in miles (optional)
            num_unlinked_trips: Number of component unlinked trips
            tour_direction: Tour direction (1=OUTBOUND, 2=INBOUND)
            access_mode: Transit access mode (for transit trips)
            egress_mode: Transit egress mode (for transit trips)
            **overrides: Override any default values

        Returns:
            Complete linked trip record dict
        """
        # Default times if not provided
        depart_time, arrive_time = CanonicalTestDataBuilder._default_times(
            depart_time, arrive_time, default_depart_hour=8, travel_minutes=30
        )

        # Calculate distance_miles if not provided
        if distance_miles is None:
            distance_miles = distance_meters / 1609.34

        record = {
            "linked_trip_id": linked_trip_id,
            "person_id": person_id,
            "hh_id": hh_id,
            "person_num": person_num,
            "day_num": day_num,
            "travel_dow": CanonicalTestDataBuilder._to_value(travel_dow),
            "linked_trip_num": linked_trip_num,
            "tour_id": tour_id,
            "depart_time": depart_time,
            "arrive_time": arrive_time,
            "duration_minutes": int(
                (arrive_time - depart_time).total_seconds() / 60
            ),
            "o_lat": origin_lat,
            "o_lon": origin_lon,
            "o_taz": origin_taz,
            "o_maz": origin_maz,
            "o_purpose_category": CanonicalTestDataBuilder._to_value(
                origin_purpose
            ),
            "d_lat": dest_lat,
            "d_lon": dest_lon,
            "d_taz": dest_taz,
            "d_maz": dest_maz,
            "d_purpose_category": CanonicalTestDataBuilder._to_value(
                dest_purpose
            ),
            "mode": CanonicalTestDataBuilder._to_value(mode),
            "mode_type": CanonicalTestDataBuilder._to_value(mode_type),
            "driver": CanonicalTestDataBuilder._to_value(driver),
            "num_travelers": num_travelers,
            "distance_meters": distance_meters,
            "distance_miles": distance_miles,
            "num_unlinked_trips": num_unlinked_trips,
            "tour_direction": tour_direction,
            "linked_trip_weight": 1.0,
            # Always include access/egress mode fields (formatter expects them)
            "access_mode": CanonicalTestDataBuilder._to_value(access_mode),
            "egress_mode": CanonicalTestDataBuilder._to_value(egress_mode),
        }

        # Add optional fields
        CanonicalTestDataBuilder._add_optional_fields(
            record,
            day_id=day_id,
        )

        return {**record, **overrides}

    @staticmethod
    def create_day(
        day_id: int = 1,
        person_id: int = 101,
        hh_id: int = 1,
        person_num: int = 1,
        day_num: int = 1,
        travel_date: datetime | None = None,
        travel_dow: TravelDow = TravelDow.MONDAY,
        is_complete: bool = True,
        num_trips: int = 0,
        day_weight: float = 1.0,
        **overrides,
    ) -> dict:
        """Create a day record for multi-day scenarios.

        Day records track which days each person provided diary data for,
        including completeness and basic trip counts. Used primarily for
        DaySim formatting which requires day-level data.

        Args:
            day_id: Day ID
            person_id: Person ID
            hh_id: Household ID
            person_num: Person number within household (for DaySim)
            day_num: Day number in survey period (for DaySim)
            travel_date: Travel date (defaults to today)
            travel_dow: Day of week
            is_complete: Day complete (person at home at start/end)
            num_trips: Number of trips on this day
            day_weight: Day expansion factor (for DaySim)
            **overrides: Override any default values

        Returns:
            Complete day record dict
        """
        if travel_date is None:
            travel_date = datetime.now(tz=UTC).date()

        record = {
            "day_id": day_id,
            "person_id": person_id,
            "hh_id": hh_id,
            "person_num": person_num,
            "day_num": day_num,
            "travel_date": travel_date,
            "travel_dow": CanonicalTestDataBuilder._to_value(travel_dow),
            "is_complete": is_complete,
            "num_trips": num_trips,
            "day_weight": day_weight,
        }

        return {**record, **overrides}

    @staticmethod
    def get_tour_schema() -> dict[str, type]:
        """Get Polars schema for tour DataFrames with optional int fields.

        Use this when creating tour DataFrames to ensure columns with None
        values get the correct Int64 type instead of Null type.

        Example:
            tours = pl.DataFrame(
                [builder.create_tour(...)],
                schema=builder.get_tour_schema()
            )
        """
        return {
            "tour_id": pl.Int64,
            "person_id": pl.Int64,
            "hh_id": pl.Int64,
            "person_num": pl.Int64,
            "tour_purpose": pl.Int64,
            "tour_category": pl.Int64,
            "o_taz": pl.Int64,
            "d_taz": pl.Int64,
            "depart_time": pl.Datetime,
            "arrive_time": pl.Datetime,
            "travel_dow": pl.Int64,
            "num_trips": pl.Int64,
            "num_travelers": pl.Int64,
            "tour_mode": pl.Int64,
            "student_category": pl.String,
            "data_quality": pl.Int64,
            "joint_tour_id": pl.Int64,
            "parent_tour_id": pl.Int64,
        }

    @staticmethod
    def create_trip(
        trip_id: int = 10001,
        tour_id: int = 1001,
        person_id: int = 101,
        hh_id: int = 1,
        trip_num: int = 1,
        tour_direction: TourDirection = TourDirection.OUTBOUND,
        o_taz: int = 100,
        d_taz: int = 200,
        o_maz: int | None = None,
        d_maz: int | None = None,
        o_location_type: LocationType = LocationType.HOME,
        d_location_type: LocationType = LocationType.WORK,
        depart_time: datetime | None = None,
        arrive_time: datetime | None = None,
        travel_time: int = 30,
        travel_dow: TravelDow = TravelDow.MONDAY,
        mode: Mode = Mode.MISSING,
        mode_type: ModeType = ModeType.CAR,
        purpose: Purpose = Purpose.PRIMARY_WORKPLACE,
        purpose_category: PurposeCategory = PurposeCategory.WORK,
        driver: Driver = Driver.DRIVER,
        access_mode: AccessEgressMode | None = None,
        egress_mode: AccessEgressMode | None = None,
        num_travelers: int = 1,
        **overrides,
    ) -> dict:
        """Create a complete canonical trip record.

        Args:
            trip_id: Trip ID
            tour_id: Parent tour ID
            person_id: Person ID
            hh_id: Household ID
            trip_num: Trip number within tour
            tour_direction: Outbound/inbound/subtour
            o_taz: Origin TAZ
            d_taz: Destination TAZ
            o_maz: Origin MAZ (optional, for Daysim)
            d_maz: Destination MAZ (optional, for Daysim)
            o_location_type: Origin location type
            d_location_type: Destination location type
            depart_time: Departure time (defaults to 8 AM)
            arrive_time: Arrival time (defaults to 8:30 AM)
            travel_time: Travel time in minutes
            travel_dow: Day of week
            mode: Specific mode
            mode_type: Mode type (car/transit/walk/bike)
            purpose: Trip purpose
            purpose_category: Purpose category
            driver: Driver status
            access_mode: Access mode for transit (optional)
            egress_mode: Egress mode for transit (optional)
            num_travelers: Number of travelers
            **overrides: Override any default values

        Returns:
            Complete trip record dict
        """
        # Default times if not provided
        if depart_time is None:
            depart_time = datetime.combine(
                datetime.now(tz=UTC).date(), time(8, 0)
            )
        if arrive_time is None:
            arrive_time = datetime.combine(
                datetime.now(tz=UTC).date(), time(8, travel_time)
            )

        record = {
            "trip_id": trip_id,
            "tour_id": tour_id,
            "person_id": person_id,
            "hh_id": hh_id,
            "trip_num": trip_num,
            "tour_direction": tour_direction.value,
            "o_taz": o_taz,
            "d_taz": d_taz,
            "o_location_type": o_location_type.value,
            "d_location_type": d_location_type.value,
            "depart_time": depart_time,
            "arrive_time": arrive_time,
            "travel_time": travel_time,
            "travel_dow": travel_dow.value,
            "mode": mode.value,
            "mode_type": mode_type.value,
            "purpose": purpose.value,
            "purpose_category": purpose_category.value,
            "driver": driver.value,
            "num_travelers": num_travelers,
        }

        # Add optional MAZ fields if provided
        if o_maz is not None:
            record["o_maz"] = o_maz
        if d_maz is not None:
            record["d_maz"] = d_maz

        # Add optional transit fields if provided
        if access_mode is not None:
            record["access_mode"] = access_mode.value
        if egress_mode is not None:
            record["egress_mode"] = egress_mode.value

        return {**record, **overrides}

    # Scenario builders - Common test patterns
    # =========================================

    @staticmethod
    def simple_work_tour(
        hh_id: int = 1,
        person_id: int = 101,
        home_taz: int = 100,
        work_taz: int = 200,
        home_maz: int | None = None,
        work_maz: int | None = None,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Create a simple home → work → home tour scenario (2 trips).

        Args:
            hh_id: Household ID
            person_id: Person ID
            home_taz: Home TAZ
            work_taz: Work TAZ
            home_maz: Home MAZ (optional, for DaySim)
            work_maz: Work MAZ (optional, for DaySim)

        Returns:
            Tuple of (households, persons, days, unlinked_trips) DataFrames
        """
        builder = CanonicalTestDataBuilder

        # Define coordinates
        home_lat, home_lon = builder.DEFAULT_COORDS["home"]
        work_lat, work_lon = builder.DEFAULT_COORDS["work"]

        # Create household
        household = builder.create_household(
            hh_id=hh_id,
            home_taz=home_taz,
            home_maz=home_maz,
            home_lat=home_lat,
            home_lon=home_lon,
            num_people=1,
            num_workers=1,
        )

        # Create person
        person = builder.create_person(
            person_id=person_id,
            hh_id=hh_id,
            person_num=1,
            age=AgeCategory.AGE_35_TO_44,
            employment=Employment.EMPLOYED_FULLTIME,
            work_taz=work_taz,
            work_maz=work_maz,
            work_lat=work_lat,
            work_lon=work_lon,
        )

        # Prepare person for DaySim/tour extraction
        person = builder.prepare_person_for_daysim(person)

        # Create day
        day = builder.create_day(
            day_id=1,
            person_id=person_id,
            hh_id=hh_id,
            num_trips=2,
            is_complete=True,
        )

        # Create unlinked trips: Home → Work → Home
        # Include lat/lon coordinates for location classification

        trip1 = builder.create_unlinked_trip(
            trip_id=1,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=home_taz,
            d_taz=work_taz,
            o_maz=home_maz,
            d_maz=work_maz,
            o_lat=home_lat,
            o_lon=home_lon,
            d_lat=work_lat,
            d_lon=work_lon,
            o_purpose_category=PurposeCategory.HOME,
            d_purpose_category=PurposeCategory.WORK,
            purpose=Purpose.PRIMARY_WORKPLACE,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(8, 0)
            ),
            arrive_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(9, 0)
            ),
        )

        trip2 = builder.create_unlinked_trip(
            trip_id=2,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=work_taz,
            d_taz=home_taz,
            o_maz=work_maz,
            d_maz=home_maz,
            o_lat=work_lat,
            o_lon=work_lon,
            d_lat=home_lat,
            d_lon=home_lon,
            o_purpose_category=PurposeCategory.WORK,
            d_purpose_category=PurposeCategory.HOME,
            purpose=Purpose.HOME,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(17, 0)
            ),
            arrive_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(18, 0)
            ),
        )

        # Prepare household for DaySim/tour extraction
        household = builder.prepare_household_for_daysim(household)

        households = pl.DataFrame([household])
        persons = pl.DataFrame([person])
        days = pl.DataFrame([day])
        unlinked_trips = pl.DataFrame([trip1, trip2])

        return households, persons, days, unlinked_trips

    @staticmethod
    def transit_commute(
        hh_id: int = 1,
        person_id: int = 101,
        home_taz: int = 100,
        work_taz: int = 200,
        home_maz: int | None = None,
        work_maz: int | None = None,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Create transit commute scenario with walk-BART-walk (6 trips).

        Pattern: Home → (walk) → BART → (BART) → BART → (walk) → Work
        → (return)

        Args:
            hh_id: Household ID
            person_id: Person ID
            home_taz: Home TAZ
            work_taz: Work TAZ
            home_maz: Home MAZ (optional)
            work_maz: Work MAZ (optional)

        Returns:
            Tuple of (households, persons, days, unlinked_trips) DataFrames
        """
        builder = CanonicalTestDataBuilder

        # Define coordinates
        home_lat, home_lon = builder.DEFAULT_COORDS["home"]
        bart_home_lat, bart_home_lon = builder.DEFAULT_COORDS["bart_home"]
        bart_work_lat, bart_work_lon = builder.DEFAULT_COORDS["bart_work"]
        work_lat, work_lon = builder.DEFAULT_COORDS["work"]

        household = builder.create_household(
            hh_id=hh_id,
            home_taz=home_taz,
            home_maz=home_maz,
            home_lat=home_lat,
            home_lon=home_lon,
            num_people=1,
            num_workers=1,
        )

        person = builder.create_person(
            person_id=person_id,
            hh_id=hh_id,
            person_num=1,
            age=AgeCategory.AGE_35_TO_44,
            employment=Employment.EMPLOYED_FULLTIME,
            work_taz=work_taz,
            work_maz=work_maz,
            work_lat=work_lat,
            work_lon=work_lon,
        )

        person = builder.prepare_person_for_daysim(person)

        day = builder.create_day(
            day_id=1,
            person_id=person_id,
            hh_id=hh_id,
            num_trips=6,
            is_complete=True,
        )

        # Morning commute: Home → walk to BART → BART → walk to work
        trip1 = builder.create_unlinked_trip(
            trip_id=1,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=home_taz,
            d_taz=home_taz,
            o_maz=home_maz,
            d_maz=home_maz,
            o_lat=home_lat,
            o_lon=home_lon,
            d_lat=bart_home_lat,
            d_lon=bart_home_lon,
            o_purpose_category=PurposeCategory.HOME,
            d_purpose_category=PurposeCategory.CHANGE_MODE,
            mode=Mode.WALK,
            mode_type=ModeType.WALK,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(7, 50)
            ),
            arrive_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(8, 0)
            ),
            travel_time=10,
        )

        trip2 = builder.create_unlinked_trip(
            trip_id=2,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=home_taz,
            d_taz=work_taz,
            o_maz=home_maz,
            d_maz=work_maz,
            o_lat=bart_home_lat,
            o_lon=bart_home_lon,
            d_lat=bart_work_lat,
            d_lon=bart_work_lon,
            o_purpose_category=PurposeCategory.CHANGE_MODE,
            d_purpose_category=PurposeCategory.CHANGE_MODE,
            mode=Mode.BART,
            mode_type=ModeType.TRANSIT,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(8, 5)
            ),
            arrive_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(8, 35)
            ),
            travel_time=30,
        )

        trip3 = builder.create_unlinked_trip(
            trip_id=3,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=work_taz,
            d_taz=work_taz,
            o_maz=work_maz,
            d_maz=work_maz,
            o_lat=bart_work_lat,
            o_lon=bart_work_lon,
            d_lat=work_lat,
            d_lon=work_lon,
            o_purpose_category=PurposeCategory.CHANGE_MODE,
            d_purpose_category=PurposeCategory.WORK,
            mode=Mode.WALK,
            mode_type=ModeType.WALK,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(8, 35)
            ),
            arrive_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(8, 45)
            ),
            travel_time=10,
        )

        # Evening commute: Work → walk to BART → BART → walk home
        trip4 = builder.create_unlinked_trip(
            trip_id=4,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=work_taz,
            d_taz=work_taz,
            o_maz=work_maz,
            d_maz=work_maz,
            o_lat=work_lat,
            o_lon=work_lon,
            d_lat=bart_work_lat,
            d_lon=bart_work_lon,
            o_purpose_category=PurposeCategory.WORK,
            d_purpose_category=PurposeCategory.CHANGE_MODE,
            mode=Mode.WALK,
            mode_type=ModeType.WALK,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(17, 0)
            ),
            arrive_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(17, 10)
            ),
            travel_time=10,
        )

        trip5 = builder.create_unlinked_trip(
            trip_id=5,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=work_taz,
            d_taz=home_taz,
            o_maz=work_maz,
            d_maz=home_maz,
            o_lat=bart_work_lat,
            o_lon=bart_work_lon,
            d_lat=bart_home_lat,
            d_lon=bart_home_lon,
            o_purpose_category=PurposeCategory.CHANGE_MODE,
            d_purpose_category=PurposeCategory.CHANGE_MODE,
            mode=Mode.BART,
            mode_type=ModeType.TRANSIT,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(17, 15)
            ),
            arrive_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(17, 45)
            ),
            travel_time=30,
        )

        trip6 = builder.create_unlinked_trip(
            trip_id=6,
            person_id=person_id,
            hh_id=hh_id,
            o_taz=home_taz,
            d_taz=home_taz,
            o_maz=home_maz,
            d_maz=home_maz,
            o_lat=bart_home_lat,
            o_lon=bart_home_lon,
            d_lat=home_lat,
            d_lon=home_lon,
            o_purpose_category=PurposeCategory.CHANGE_MODE,
            d_purpose_category=PurposeCategory.HOME,
            mode=Mode.WALK,
            mode_type=ModeType.WALK,
            depart_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(17, 45)
            ),
            arrive_time=datetime.combine(
                datetime.now(tz=UTC).date(), time(17, 55)
            ),
            travel_time=10,
        )

        household = builder.prepare_household_for_daysim(household)

        households = pl.DataFrame([household])
        persons = pl.DataFrame([person])
        days = pl.DataFrame([day])
        unlinked_trips = pl.DataFrame(
            [trip1, trip2, trip3, trip4, trip5, trip6]
        )

        return households, persons, days, unlinked_trips

    @staticmethod
    def multi_stop_tour(
        hh_id: int = 1,
        person_id: int = 101,
        home_taz: int = 100,
        work_taz: int = 200,
        stop_taz: int = 150,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Create a work tour with intermediate stop (4 trips).

        Pattern: Home → Work → Stop (lunch/errand) → Work → Home

        Args:
            hh_id: Household ID
            person_id: Person ID
            home_taz: Home TAZ
            work_taz: Work TAZ
            stop_taz: Intermediate stop TAZ

        Returns:
            Tuple of (households, persons, days, unlinked_trips) DataFrames
        """
        builder = CanonicalTestDataBuilder

        household = builder.create_household(
            hh_id=hh_id, home_taz=home_taz, num_people=1, num_workers=1
        )

        person = builder.create_person(
            person_id=person_id,
            hh_id=hh_id,
            person_num=1,
            age=35,
            employment=Employment.EMPLOYED_FULLTIME,
            work_taz=work_taz,
        )

        day = builder.create_day(
            day_id=1, person_id=person_id, hh_id=hh_id, num_trips=4
        )

        # Home → Work → Stop → Work → Home
        trips = [
            builder.create_unlinked_trip(
                trip_id=1,
                person_id=person_id,
                hh_id=hh_id,
                o_taz=home_taz,
                d_taz=work_taz,
                purpose=Purpose.PRIMARY_WORKPLACE,
                depart_time=datetime.combine(
                    datetime.now(tz=UTC).date(), time(8, 0)
                ),
            ),
            builder.create_unlinked_trip(
                trip_id=2,
                person_id=person_id,
                hh_id=hh_id,
                o_taz=work_taz,
                d_taz=stop_taz,
                purpose=Purpose.EAT_OUT,
                depart_time=datetime.combine(
                    datetime.now(tz=UTC).date(), time(12, 0)
                ),
            ),
            builder.create_unlinked_trip(
                trip_id=3,
                person_id=person_id,
                hh_id=hh_id,
                o_taz=stop_taz,
                d_taz=work_taz,
                purpose=Purpose.PRIMARY_WORKPLACE,
                depart_time=datetime.combine(
                    datetime.now(tz=UTC).date(), time(13, 0)
                ),
            ),
            builder.create_unlinked_trip(
                trip_id=4,
                person_id=person_id,
                hh_id=hh_id,
                o_taz=work_taz,
                d_taz=home_taz,
                purpose=Purpose.HOME,
                depart_time=datetime.combine(
                    datetime.now(tz=UTC).date(), time(17, 0)
                ),
            ),
        ]

        households = pl.DataFrame([household])
        persons = pl.DataFrame([person])
        days = pl.DataFrame([day])
        unlinked_trips = pl.DataFrame(trips)

        return households, persons, days, unlinked_trips

    @staticmethod
    def multi_tour_day(
        hh_id: int = 1,
        person_id: int = 101,
        home_taz: int = 100,
        work_taz: int = 200,
        shop_taz: int = 150,
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Create a person with multiple tours in one day.

        Pattern: Home → Work → Home → Shopping → Home

        Args:
            hh_id: Household ID
            person_id: Person ID
            home_taz: Home TAZ
            work_taz: Work TAZ
            shop_taz: Shopping TAZ

        Returns:
            Tuple of (households, persons, days, unlinked_trips) DataFrames
        """
        builder = CanonicalTestDataBuilder

        household = builder.create_household(
            hh_id=hh_id, home_taz=home_taz, num_people=1, num_workers=1
        )

        person = builder.create_person(
            person_id=person_id,
            hh_id=hh_id,
            person_num=1,
            age=35,
            employment=Employment.EMPLOYED_FULLTIME,
            work_taz=work_taz,
        )

        day = builder.create_day(
            day_id=1, person_id=person_id, hh_id=hh_id, num_trips=4
        )

        # Home → Work → Home → Shop → Home
        trips = [
            builder.create_unlinked_trip(
                trip_id=1,
                person_id=person_id,
                hh_id=hh_id,
                o_taz=home_taz,
                d_taz=work_taz,
                purpose=Purpose.PRIMARY_WORKPLACE,
                depart_time=datetime.combine(
                    datetime.now(tz=UTC).date(), time(8, 0)
                ),
            ),
            builder.create_unlinked_trip(
                trip_id=2,
                person_id=person_id,
                hh_id=hh_id,
                o_taz=work_taz,
                d_taz=home_taz,
                purpose=Purpose.HOME,
                depart_time=datetime.combine(
                    datetime.now(tz=UTC).date(), time(17, 0)
                ),
            ),
            builder.create_unlinked_trip(
                trip_id=3,
                person_id=person_id,
                hh_id=hh_id,
                o_taz=home_taz,
                d_taz=shop_taz,
                purpose=Purpose.SHOPPING,
                depart_time=datetime.combine(
                    datetime.now(tz=UTC).date(), time(19, 0)
                ),
            ),
            builder.create_unlinked_trip(
                trip_id=4,
                person_id=person_id,
                hh_id=hh_id,
                o_taz=shop_taz,
                d_taz=home_taz,
                purpose=Purpose.HOME,
                depart_time=datetime.combine(
                    datetime.now(tz=UTC).date(), time(20, 0)
                ),
            ),
        ]

        households = pl.DataFrame([household])
        persons = pl.DataFrame([person])
        days = pl.DataFrame([day])
        unlinked_trips = pl.DataFrame(trips)

        return households, persons, days, unlinked_trips

    @staticmethod
    def multi_person_household(
        hh_id: int = 1,
        num_workers: int = 2,
        num_students: int = 1,
        num_children: int = 1,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Create a multi-person household with various person types.

        Args:
            hh_id: Household ID
            num_workers: Number of workers (full-time)
            num_students: Number of students (university)
            num_children: Number of children (age 5-15)

        Returns:
            Tuple of (households, persons) DataFrames (no trips/days)
        """
        builder = CanonicalTestDataBuilder

        total_people = num_workers + num_students + num_children

        household = builder.create_household(
            hh_id=hh_id,
            num_people=total_people,
            num_workers=num_workers,
            num_vehicles=num_workers,
        )

        persons = []
        person_num = 1

        # Add workers
        for i in range(num_workers):
            persons.append(
                builder.create_person(
                    person_id=100 + person_num,
                    hh_id=hh_id,
                    person_num=person_num,
                    age=AgeCategory.AGE_35_TO_44
                    if i == 0
                    else AgeCategory.AGE_45_TO_54,
                    employment=Employment.EMPLOYED_FULLTIME,
                    work_taz=200,
                )
            )
            person_num += 1

        # Add students
        for _ in range(num_students):
            persons.append(
                builder.create_person(
                    person_id=100 + person_num,
                    hh_id=hh_id,
                    person_num=person_num,
                    age=AgeCategory.AGE_18_TO_24,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.FULLTIME_INPERSON,
                    school_type=SchoolType.COLLEGE_4YEAR,
                    school_taz=300,
                )
            )
            person_num += 1

        # Add children
        for _ in range(num_children):
            persons.append(
                builder.create_person(
                    person_id=100 + person_num,
                    hh_id=hh_id,
                    person_num=person_num,
                    age=AgeCategory.AGE_5_TO_15,
                    employment=Employment.UNEMPLOYED_NOT_LOOKING,
                    student=Student.NONSTUDENT,
                    school_type=SchoolType.ELEMENTARY,
                )
            )
            person_num += 1

        # Prepare household for DaySim/tour extraction
        household = builder.prepare_household_for_daysim(household)

        households = pl.DataFrame([household])

        # Prepare persons for DaySim/tour extraction
        persons_prepared = []
        for p in persons:
            prepared = CanonicalTestDataBuilder.prepare_person_for_daysim(p)
            persons_prepared.append(prepared)

        persons_df = pl.DataFrame(persons_prepared)

        return households, persons_df

    @staticmethod
    def create_single_adult_household():
        """Create a single full-time worker household for CTRAMP tests."""
        household = CanonicalTestDataBuilder.create_household(
            hh_id=1,
            home_taz=100,
            num_people=1,
            num_vehicles=1,
            num_workers=1,
            income_detailed=IncomeDetailed.INCOME_75TO100,
        )

        person = CanonicalTestDataBuilder.create_person(
            person_id=101,
            hh_id=1,
            person_num=1,
            age=AgeCategory.AGE_35_TO_44,
            gender=Gender.MALE,
            employment=Employment.EMPLOYED_FULLTIME,
            student=Student.NONSTUDENT,
            commute_subsidy=CommuteSubsidy.FREE_PARK,
            person_type=None,  # Don't set person_type - let CTRAMP derive it
        )

        return pl.DataFrame([household]), pl.DataFrame([person])

    @staticmethod
    def create_family_household():
        """Create household with working adults and school-age children.

        For CTRAMP tests.
        """
        household = CanonicalTestDataBuilder.create_household(
            hh_id=2,
            home_taz=200,
            num_people=4,
            num_vehicles=2,
            num_workers=2,
            income_detailed=IncomeDetailed.INCOME_100TO150,
        )

        persons = [
            # Full-time worker (parent 1)
            CanonicalTestDataBuilder.create_person(
                person_id=201,
                hh_id=2,
                person_num=1,
                age=AgeCategory.AGE_35_TO_44,
                gender=Gender.FEMALE,
                employment=Employment.EMPLOYED_FULLTIME,
                student=Student.NONSTUDENT,
                person_type=None,
            ),
            # Part-time worker (parent 2)
            CanonicalTestDataBuilder.create_person(
                person_id=202,
                hh_id=2,
                person_num=2,
                age=AgeCategory.AGE_35_TO_44,
                gender=Gender.MALE,
                employment=Employment.EMPLOYED_PARTTIME,
                student=Student.NONSTUDENT,
                person_type=None,
            ),
            # High school student
            CanonicalTestDataBuilder.create_person(
                person_id=203,
                hh_id=2,
                person_num=3,
                age=AgeCategory.AGE_16_TO_17,
                gender=Gender.FEMALE,
                employment=Employment.UNEMPLOYED_NOT_LOOKING,
                student=Student.FULLTIME_INPERSON,
                school_type=SchoolType.HIGH_SCHOOL,
                person_type=None,
            ),
            # Child under 16
            CanonicalTestDataBuilder.create_person(
                person_id=204,
                hh_id=2,
                person_num=4,
                age=AgeCategory.AGE_5_TO_15,
                gender=Gender.MALE,
                employment=Employment.UNEMPLOYED_NOT_LOOKING,
                student=Student.FULLTIME_INPERSON,
                school_type=SchoolType.ELEMENTARY,
                person_type=None,
            ),
        ]

        return pl.DataFrame([household]), pl.DataFrame(persons)

    @staticmethod
    def create_retired_household():
        """Create a household with retired persons for CTRAMP tests."""
        household = CanonicalTestDataBuilder.create_household(
            hh_id=3,
            home_taz=300,
            num_people=2,
            num_vehicles=1,
            num_workers=0,
            income_detailed=IncomeDetailed.INCOME_50TO75,
        )

        persons = [
            CanonicalTestDataBuilder.create_person(
                person_id=301,
                hh_id=3,
                person_num=1,
                age=AgeCategory.AGE_65_TO_74,
                gender=Gender.MALE,
                employment=Employment.UNEMPLOYED_NOT_LOOKING,
                student=Student.NONSTUDENT,
                person_type=None,
            ),
            CanonicalTestDataBuilder.create_person(
                person_id=302,
                hh_id=3,
                person_num=2,
                age=AgeCategory.AGE_65_TO_74,
                gender=Gender.FEMALE,
                employment=Employment.UNEMPLOYED_NOT_LOOKING,
                student=Student.NONSTUDENT,
                person_type=None,
            ),
        ]

        return pl.DataFrame([household]), pl.DataFrame(persons)

    @staticmethod
    def create_university_student_household():
        """Create a household with university students for CTRAMP tests."""
        household = CanonicalTestDataBuilder.create_household(
            hh_id=4,
            home_taz=400,
            num_people=1,
            num_vehicles=1,
            num_workers=0,
            income_detailed=IncomeDetailed.INCOME_35TO50,
        )

        person = CanonicalTestDataBuilder.create_person(
            person_id=401,
            hh_id=4,
            person_num=1,
            age=AgeCategory.AGE_18_TO_24,
            gender=Gender.FEMALE,
            employment=Employment.UNEMPLOYED_NOT_LOOKING,
            student=Student.FULLTIME_INPERSON,
            school_type=SchoolType.COLLEGE_4YEAR,
            person_type=None,
        )

        return pl.DataFrame([household]), pl.DataFrame([person])

    # ==========================================================================
    # Scenario Builders with Pipeline Processing
    # ==========================================================================
    # These methods create unlinked trips and process them through the
    # link_trips → extract_tours pipeline, returning ready-to-use test data.

    @staticmethod
    def create_simple_work_tour_processed(
        hh_id: int = 1,
        person_id: int = 101,
    ) -> dict[str, pl.DataFrame]:
        """Create simple work tour processed through pipeline.

        Returns processed data ready for formatter testing.

        Returns:
            Dict with keys: households, persons, days, unlinked_trips,
                           linked_trips, tours
        """
        households, persons, days, unlinked_trips = (
            CanonicalTestDataBuilder.simple_work_tour(hh_id, person_id)
        )
        return process_scenario_through_pipeline(
            households, persons, days, unlinked_trips
        )

    @staticmethod
    def create_transit_commute_processed() -> dict[str, pl.DataFrame]:
        """Create transit commute scenario with all processing applied.

        Returns:
            Dict with keys: households, persons, days, unlinked_trips,
                           linked_trips, tours
        """
        households, persons, days, unlinked_trips = (
            CanonicalTestDataBuilder.transit_commute()
        )
        return process_scenario_through_pipeline(
            households, persons, days, unlinked_trips
        )

    @staticmethod
    def create_multi_person_household_processed(
        hh_id: int = 1,
    ) -> dict[str, pl.DataFrame]:
        """Create multi-person household processed through pipeline.

        Returns:
            Dict with keys: households, persons, days, unlinked_trips,
                           linked_trips, tours
        """
        households, persons = CanonicalTestDataBuilder.multi_person_household(
            hh_id=hh_id
        )

        # Create simple days for all persons
        days_list = []
        unlinked_trips_list = []
        trip_id = 1

        for person in persons.iter_rows(named=True):
            person_id = person["person_id"]
            person_num = person["person_num"]

            days_list.append(
                CanonicalTestDataBuilder.create_day(
                    day_id=person_id,
                    person_id=person_id,
                    hh_id=hh_id,
                    person_num=person_num,
                    day_num=1,
                )
            )

            # Create simple home->work->home trips for workers
            if person.get("work_taz"):
                unlinked_trips_list.extend(
                    [
                        CanonicalTestDataBuilder.create_unlinked_trip(
                            trip_id=trip_id,
                            person_id=person_id,
                            hh_id=hh_id,
                            day_id=person_id,
                            person_num=person_num,
                            o_purpose_category=PurposeCategory.HOME,
                            d_purpose_category=PurposeCategory.WORK,
                            o_taz=households["home_taz"][0],
                            d_taz=person["work_taz"],
                        ),
                        CanonicalTestDataBuilder.create_unlinked_trip(
                            trip_id=trip_id + 1,
                            person_id=person_id,
                            hh_id=hh_id,
                            day_id=person_id,
                            person_num=person_num,
                            o_purpose_category=PurposeCategory.WORK,
                            d_purpose_category=PurposeCategory.HOME,
                            o_taz=person["work_taz"],
                            d_taz=households["home_taz"][0],
                        ),
                    ]
                )
                trip_id += 2

        days = pl.DataFrame(days_list)
        unlinked_trips = (
            pl.DataFrame(unlinked_trips_list)
            if unlinked_trips_list
            else pl.DataFrame()
        )

        if not unlinked_trips.is_empty():
            return process_scenario_through_pipeline(
                households, persons, days, unlinked_trips
            )
        return {
            "households": households,
            "persons": persons,
            "days": days,
            "unlinked_trips": unlinked_trips,
            "linked_trips": pl.DataFrame(),
            "tours": pl.DataFrame(),
        }


# ==============================================================================
# Pytest Fixtures - Processed Scenarios
# ==============================================================================
# These fixtures run scenarios through the link_trips → extract_tours pipeline
# using production defaults, providing processed data for formatter tests.


@pytest.fixture(scope="module")
def simple_work_tour_processed():
    """Simple work tour processed through link_trips and extract_tours.

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                        linked_trips, tours
    """
    return CanonicalTestDataBuilder.create_simple_work_tour_processed()


@pytest.fixture(scope="module")
def multi_stop_tour_processed():
    """Multi-stop work tour processed through link_trips and extract_tours.

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                        linked_trips, tours
    """
    households, persons, days, unlinked_trips = (
        CanonicalTestDataBuilder.multi_stop_tour()
    )
    return process_scenario_through_pipeline(
        households, persons, days, unlinked_trips
    )


@pytest.fixture(scope="module")
def multi_tour_day_processed():
    """Multi-tour day processed through link_trips and extract_tours.

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                        linked_trips, tours
    """
    households, persons, days, unlinked_trips = (
        CanonicalTestDataBuilder.multi_tour_day()
    )
    return process_scenario_through_pipeline(
        households, persons, days, unlinked_trips
    )


# ==============================================================================
# Helper Functions
# ==============================================================================


def process_scenario_through_pipeline(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    days: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """Process a scenario through link_trips → extract_tours pipeline.

    Utility function for tests that need to process custom scenarios through
    the full pipeline with production defaults.

    Args:
        households: Households DataFrame
        persons: Persons DataFrame
        days: Days DataFrame
        unlinked_trips: Unlinked trips DataFrame

    Returns:
        Dict with keys: households, persons, days, unlinked_trips,
                        linked_trips, tours
    """
    # Link trips (using config.yaml defaults)
    link_result = link_trips(
        unlinked_trips,
        change_mode_code=PurposeCategory.CHANGE_MODE.value,
        transit_mode_codes=CanonicalTestDataBuilder.DEFAULT_TRANSIT_MODE_CODES,
        max_dwell_time=180,  # in minutes
        dwell_buffer_distance=100,  # in meters
    )
    linked_trips = link_result["linked_trips"]
    unlinked_trips = link_result[
        "unlinked_trips"
    ]  # Use updated unlinked trips with linked_trip_id

    # Extract tours (using config.yaml defaults)
    tour_result = extract_tours(
        persons=persons,
        households=households,
        unlinked_trips=unlinked_trips,
        linked_trips=linked_trips,
    )

    return {
        "households": households,
        "persons": persons,
        "days": days,
        "unlinked_trips": tour_result["unlinked_trips"],
        "linked_trips": tour_result["linked_trips"],
        "tours": tour_result["tours"],
    }
