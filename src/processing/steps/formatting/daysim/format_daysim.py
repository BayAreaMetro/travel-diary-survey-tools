"""DaySim Formatting Step.

Transforms canonical survey data (persons, households, trips, days) into
DaySim model format. DaySim is an activity-based travel demand model
requiring specific data structures and coding schemes.

Key transformations:
1. Person data: Map demographic categories, employment types, and student
   status to DaySim person types (pptyp)
2. Household data: Process income categories and dwelling types, compute
   household composition
3. Trip data: Convert travel purposes, modes, path types, and
   driver/passenger roles to DaySim codes
4. Day completeness: Calculate weekday aggregates for survey weighting
   (optional)

This step produces core DaySim tables (persons, households, trips) without
tour-dependent outputs. Tour-dependent formatting (PersonDay, Tour tables)
should be handled in a separate tour extraction step.
"""

import logging

import polars as pl

from data_canon.codebook.daysim import (
    DaysimDriverPassenger,
    DaysimMode,
    DaysimPathType,
    DaysimStudentType,
    VehicleOccupancy,
)
from data_canon.codebook.persons import (
    Employment,
    PersonType,
    SchoolType,
    Student,
)
from data_canon.codebook.trips import (
    Driver,
    Mode,
    ModeType,
)
from processing.decoration import step

from .mappings import (
    AGE_MAP,
    DROVE_ACCESS_EGRESS,
    GENDER_MAP,
    INCOME_DETAILED_TO_MIDPOINT,
    INCOME_FOLLOWUP_TO_MIDPOINT,
    PURPOSE_MAP,
    STUDENT_MAP,
    WORK_PARK_MAP,
    AgeThreshold,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Helper Functions
# =============================================================================

def _compute_day_completeness(days: pl.DataFrame | None) -> pl.DataFrame | None:
    """Compute day completeness indicators for survey weighting.

    Creates person-level completeness indicators by day of week and calculates
    total complete days for different weekday periods (3-day, 4-day, 5-day).

    Args:
        days: DataFrame with columns [person_id, is_complete, travel_dow]
              If None, returns None (day completeness not available)

    Returns:
        DataFrame with columns:
        - hhno, pno: Household and person identifiers
        - mon_complete through sun_complete: Binary indicators (0/1)
        - num_days_complete_3dayweekday: Sum of Tue+Wed+Thu
        - num_days_complete_4dayweekday: Sum of Mon+Tue+Wed+Thu
        - num_days_complete_5dayweekday: Sum of Mon+Tue+Wed+Thu+Fri
    """
    if days is None:
        return None

    logger.info("Computing day completeness indicators")

    # Pivot days by person and day of week
    return (
        days
        .select(["person_id", "is_complete", "travel_dow"])
        .pivot(index="person_id", on="travel_dow", values="is_complete")
        .fill_null(0)
        .with_columns(
            # Extract hhno and pno from person_id (person_id = hhno*100 + pno)
            hhno=(pl.col("person_id") // 100),
            pno=(pl.col("person_id") % 100),
            # Compute weekday aggregates
            num_days_complete_3dayweekday=pl.sum_horizontal(
                ["2", "3", "4"]
            ),
            num_days_complete_4dayweekday=pl.sum_horizontal(
                ["1", "2", "3", "4"]
            ),
            num_days_complete_5dayweekday=pl.sum_horizontal(
                ["1", "2", "3", "4", "5"]
            ),
        )
        .select([
            "hhno", "pno",
            "1", "2", "3", "4", "5", "6", "7",
            "num_days_complete_3dayweekday",
            "num_days_complete_4dayweekday",
            "num_days_complete_5dayweekday",
        ])
        .rename({
            "1": "mon_complete",
            "2": "tue_complete",
            "3": "wed_complete",
            "4": "thu_complete",
            "5": "fri_complete",
            "6": "sat_complete",
            "7": "sun_complete",
        })
    )


def _format_persons(
    persons: pl.DataFrame,
    day_completeness: pl.DataFrame | None
) -> pl.DataFrame:
    """Format person data to DaySim specification.

    Applies mapping dictionaries and derives person type (pptyp) and worker
    type (pwtyp) based on age, employment, and student status.

    Person type (pptyp) cascading logic:
    - Age < 5: Child 0-4 (type 8)
    - Age < 16: Child 5-15 (type 7)
    - Full-time employed: Full-time worker (type 1)
    - Age 16-17 and student: High school 16+ (type 6)
    - Age 18-24 and high school: High school 16+ (type 6)
    - Age >= 18 and student: University student (type 5)
    - Part-time/self-employed: Part-time worker (type 2)
    - Age < 65: Non-working adult (type 4)
    - Age >= 65: Non-working senior (type 3)

    Args:
        persons: DataFrame with canonical person fields
        day_completeness: Optional DataFrame with day completeness indicators

    Returns:
        DataFrame with DaySim person fields
    """
    logger.info("Formatting person data")

    # Apply basic mappings
    persons_daysim = (
        persons
        .rename({
            "hh_id": "hhno",
            "person_num": "pno",
            "work_lon": "pwxcord",
            "work_lat": "pwycord",
            "school_lon": "psxcord",
            "school_lat": "psycord",
            "work_taz": "pwtaz",
            "work_maz": "pwpcl",
            "school_taz": "pstaz",
            "school_maz": "pspcl",
        })
        .with_columns(
            # Fill null coordinates with -1
            pl.col(["pwxcord", "pwycord", "psxcord", "psycord"]).fill_null(-1),
            # Map age categories to midpoint ages
            pagey=pl.col("age").replace(AGE_MAP),
            # Map gender codes
            pgend=pl.col("gender").replace(GENDER_MAP),
            # Map student status
            pstyp=pl.col("student").replace(STUDENT_MAP).fill_null(
                DaysimStudentType.NOT_STUDENT.value
            ),
            # Map work parking
            ppaidprk=pl.col("work_park").replace(WORK_PARK_MAP),
        )
    )

    # Derive person type (pptyp) using cascading logic
    persons_daysim = persons_daysim.with_columns(
        pptyp=pl.when(pl.col("pagey") < AgeThreshold.CHILD_PRESCHOOL)
        .then(pl.lit(PersonType.CHILD_UNDER_5.value))
        .when(pl.col("pagey") < AgeThreshold.CHILD_SCHOOL)
        .then(pl.lit(PersonType.CHILD_5_15.value))
        # Age >= 16:
        .when(
            pl.col("employment").is_in([
                Employment.EMPLOYED_FULLTIME.value,
                Employment.EMPLOYED_SELF.value,
                Employment.EMPLOYED_NOTWORKING.value,
            ])
        )
        .then(pl.lit(PersonType.FULL_TIME_WORKER.value))
        # Age >= 16 and not full-time employed:
        .when(
            (pl.col("pagey") < AgeThreshold.YOUNG_ADULT)  # 16-17
            & (pl.col("student").is_in([
                Student.FULLTIME_INPERSON.value,
                Student.PARTTIME_INPERSON.value,
                Student.PARTTIME_ONLINE.value,
                Student.FULLTIME_ONLINE.value,
            ]))
        )
        .then(pl.lit(PersonType.HIGH_SCHOOL_STUDENT.value))
        .when(
            (pl.col("pagey") < AgeThreshold.ADULT)  # 18-24
            & (pl.col("school_type").is_in([
                SchoolType.HOME_SCHOOL.value,
                SchoolType.HIGH_SCHOOL.value,
            ]))
            & (pl.col("student").is_in([
                Student.FULLTIME_INPERSON.value,
                Student.PARTTIME_INPERSON.value,
                Student.PARTTIME_ONLINE.value,
                Student.FULLTIME_ONLINE.value,
            ]))
        )
        .then(pl.lit(PersonType.HIGH_SCHOOL_STUDENT.value))
        # Age >= 18:
        .when(
            pl.col("student").is_in([
                Student.FULLTIME_INPERSON.value,
                Student.PARTTIME_INPERSON.value,
                Student.PARTTIME_ONLINE.value,
                Student.FULLTIME_ONLINE.value,
            ])
        )
        .then(pl.lit(PersonType.UNIVERSITY_STUDENT.value))
        .when(
            pl.col("employment").is_in([
                Employment.EMPLOYED_PARTTIME.value,
                Employment.EMPLOYED_SELF.value,
                Employment.EMPLOYED_UNPAID.value,
            ])
        )
        .then(pl.lit(PersonType.PART_TIME_WORKER.value))
        .when(pl.col("pagey") < AgeThreshold.SENIOR)
        .then(pl.lit(PersonType.NON_WORKER.value))
        .otherwise(pl.lit(PersonType.RETIRED.value))
    )

    # Derive worker type (pwtyp) from person type and employment
    persons_daysim = persons_daysim.with_columns(
        pwtyp=pl.when(
            pl.col("pptyp").is_in([
                PersonType.FULL_TIME_WORKER.value,
                PersonType.PART_TIME_WORKER.value,
            ])
        )
        .then(pl.col("pptyp"))  # direct mapping for workers
        .when(
            pl.col("pptyp").is_in([
                PersonType.UNIVERSITY_STUDENT.value,
                PersonType.HIGH_SCHOOL_STUDENT.value,
            ])
            & pl.col("employment").is_in([
                Employment.EMPLOYED_FULLTIME.value,
                Employment.EMPLOYED_PARTTIME.value,
                Employment.EMPLOYED_SELF.value,
            ])
        )
        .then(pl.lit(PersonType.PART_TIME_WORKER.value))
        .otherwise(pl.lit(0))  # non-worker
    )

    # Set work/school locations to -1 if person is not worker/student
    persons_daysim = persons_daysim.with_columns(
        pwtaz=pl.when(pl.col("pwtyp") != 0)
            .then(pl.col("pwtaz"))
            .otherwise(pl.lit(-1)),
        pwpcl=pl.when(pl.col("pwtyp") != 0)
            .then(pl.col("pwpcl"))
            .otherwise(pl.lit(-1)),
        pwxcord=pl.when(pl.col("pwtyp") != 0)
            .then(pl.col("pwxcord"))
            .otherwise(pl.lit(-1)),
        pwycord=pl.when(pl.col("pwtyp") != 0)
            .then(pl.col("pwycord"))
            .otherwise(pl.lit(-1)),
        pstaz=pl.when(pl.col("pstyp") != 0)
            .then(pl.col("pstaz"))
            .otherwise(pl.lit(-1)),
        pspcl=pl.when(pl.col("pstyp") != 0)
            .then(pl.col("pspcl"))
            .otherwise(pl.lit(-1)),
        psxcord=pl.when(pl.col("pstyp") != 0)
            .then(pl.col("psxcord"))
            .otherwise(pl.lit(-1)),
        psycord=pl.when(pl.col("pstyp") != 0)
            .then(pl.col("psycord"))
            .otherwise(pl.lit(-1)),
    )

    # Add default expansion factor
    persons_daysim = persons_daysim.with_columns(
        psexpfac=pl.lit(1.0),
        pwautime=pl.lit(-1),  # auto time to work (not available)
    )

    # Join day completeness if available
    if day_completeness is not None:
        persons_daysim = persons_daysim.join(
            day_completeness,
            on=["hhno", "pno"],
            how="left"
        )

    # Select DaySim person fields
    person_cols = [
        "hhno",
        "pno",
        "pptyp",
        "pagey",
        "pgend",
        "pwtyp",
        "pwpcl",
        "pwtaz",
        "pstyp",
        "pspcl",
        "pstaz",
        "ppaidprk",
        "pwautime",
        "pwxcord",
        "pwycord",
        "psxcord",
        "psycord",
        "psexpfac",
    ]

    # Add day completeness columns if available
    if day_completeness is not None:
        person_cols.extend([
            "mon_complete",
            "tue_complete",
            "wed_complete",
            "thu_complete",
            "fri_complete",
            "sat_complete",
            "sun_complete",
            "num_days_complete_3dayweekday",
            "num_days_complete_4dayweekday",
            "num_days_complete_5dayweekday",
        ])

    return persons_daysim.select(person_cols).sort(by=["hhno", "pno"])


def _format_households(
    households: pl.DataFrame,
    persons: pl.DataFrame
) -> pl.DataFrame:
    """Format household data to DaySim specification.

    Calculates household composition from person data and applies income
    fallback logic.

    Household composition fields:
    - hhftw: Full-time workers
    - hhptw: Part-time workers
    - hhret: Retirees (non-working seniors)
    - hhoad: Other adults (non-working < 65)
    - hhuni: University students
    - hhhsc: High school students 16+
    - hh515: Children 5-15
    - hhcu5: Children 0-4

    Args:
        households: DataFrame with canonical household fields
        persons: DataFrame with formatted DaySim person fields

    Returns:
        DataFrame with DaySim household fields
    """
    logger.info("Formatting household data")

    # Calculate household composition from persons
    hh_composition = (
        persons
        .group_by("hhno")
        .agg(
            hhftw=(pl.col("pptyp") == PersonType.FULL_TIME_WORKER.value).sum(),
            hhptw=(pl.col("pptyp") == PersonType.PART_TIME_WORKER.value).sum(),
            hhret=(pl.col("pptyp") == PersonType.RETIRED.value).sum(),
            hhoad=(pl.col("pptyp") == PersonType.NON_WORKER.value).sum(),
            hhuni=(
                pl.col("pptyp") == PersonType.UNIVERSITY_STUDENT.value
            ).sum(),
            hhhsc=(
                pl.col("pptyp") == PersonType.HIGH_SCHOOL_STUDENT.value
            ).sum(),
            hh515=(pl.col("pptyp") == PersonType.CHILD_5_15.value).sum(),
            hhcu5=(pl.col("pptyp") == PersonType.CHILD_UNDER_5.value).sum(),
        )
    )

    # Format households
    households_daysim = (
        households
        .rename({
            "hh_id": "hhno",
            "home_maz": "hhparcel",
            "home_taz": "hhtaz",
            "home_lon": "hxcord",
            "home_lat": "hycord",
            "num_people": "hhsize",
            "num_vehicles": "hhvehs",
        })
        .with_columns(
            # Map income categories to midpoint values
            pl.col("income_detailed").replace(INCOME_DETAILED_TO_MIDPOINT),
            pl.col("income_followup").replace(INCOME_FOLLOWUP_TO_MIDPOINT),
        )
        .with_columns(
            # Use income_detailed if available, otherwise income_followup
            hhincome=pl.when(pl.col("income_detailed") > 0)
            .then(pl.col("income_detailed"))
            .otherwise(pl.col("income_followup"))
        )
        .join(hh_composition, on="hhno", how="left")
        .with_columns(
            # Add default expansion factor and sample type
            hhexpfac=pl.lit(1.0),
            samptype=pl.lit(0),
        )
    )

    # Select DaySim household fields
    hh_cols = [
        "hhno",
        "hhsize",
        "hhvehs",
        "hhftw",
        "hhptw",
        "hhret",
        "hhoad",
        "hhuni",
        "hhhsc",
        "hh515",
        "hhcu5",
        "hhincome",
        "hhparcel",
        "hhtaz",
        "hxcord",
        "hycord",
        "hhexpfac",
        "samptype",
    ]

    return households_daysim.select(hh_cols).sort(by="hhno")


def _format_trips(trips: pl.DataFrame) -> pl.DataFrame:
    """Format trip data to DaySim specification.

    Computes DaySim mode, path type, and driver/passenger codes from survey
    data.

    Args:
        trips: DataFrame with canonical trip fields

    Returns:
        DataFrame with DaySim trip fields
    """
    logger.info("Formatting trip data")

    # Rename and prepare trip data
    trips_daysim = (
        trips
        .rename({
            "hh_id": "hhno",
            "person_num": "pno",
            "trip_num": "tripno",
            "travel_dow": "day",
            "o_taz": "otaz",
            "o_maz": "opcl",
            "d_taz": "dtaz",
            "d_maz": "dpcl",
            "o_lon": "oxcord",
            "o_lat": "oycord",
            "d_lon": "dxcord",
            "d_lat": "dycord",
            "o_purpose_category": "opurp",
            "d_purpose_category": "dpurp",
        })
        .with_columns(
            # Fill null coordinates with -1
            pl.col(["oxcord", "oycord", "dxcord", "dycord"]).fill_null(-1),
            # Convert times to DaySim format (HHMM)
            deptm=(pl.col("depart_hour") * 100 + pl.col("depart_minute")),
            arrtm=(pl.col("arrive_hour") * 100 + pl.col("arrive_minute")),
            # Map purposes
            opurp=pl.col("opurp").replace(PURPOSE_MAP),
            dpurp=pl.col("dpurp").replace(PURPOSE_MAP),
        )
    )

    # Compute DaySim mode
    trips_daysim = trips_daysim.with_columns(
        mode=pl.when(pl.col("mode_type") == ModeType.WALK.value)
        .then(pl.lit(DaysimMode.WALK.value))
        .when(
            pl.col("mode_type").is_in([
                ModeType.BIKE.value,
                ModeType.BIKESHARE.value,
                ModeType.SCOOTERSHARE.value,
            ])
        )
        .then(pl.lit(DaysimMode.BIKE.value))
        .when(
            pl.col("mode_type").is_in([
                ModeType.CAR.value,
                ModeType.CARSHARE.value,
            ])
        )
        .then(
            pl.when(pl.col("num_travelers") == VehicleOccupancy.SOV.value)
            .then(pl.lit(DaysimMode.SOV.value))
            .when(pl.col("num_travelers") == VehicleOccupancy.HOV2.value)
            .then(pl.lit(DaysimMode.HOV2.value))
            .when(pl.col("num_travelers") > VehicleOccupancy.HOV3_MIN.value)
            .then(pl.lit(DaysimMode.HOV3.value))
        )
        .when(
            pl.col("mode_type").is_in([
                ModeType.TAXI.value,
                ModeType.TNC.value,
            ])
        )
        .then(pl.lit(DaysimMode.TNC.value))
        .when(pl.col("mode_type") == ModeType.SCHOOL_BUS.value)
        .then(pl.lit(DaysimMode.SCHOOL_BUS.value))
        .when(pl.col("mode_type") == ModeType.SHUTTLE_VANPOOL.value)
        .then(pl.lit(DaysimMode.HOV3.value))  # shuttle/vanpool as HOV3+
        .when(
            pl.col("mode_type").is_in([
                ModeType.FERRY.value,
                ModeType.TRANSIT.value,
            ])
            | (
                (pl.col("mode_type") == ModeType.LONG_DISTANCE.value)
                & (pl.col("mode_1") == Mode.RAIL_INTERCITY.value)
            )
        )
        .then(
            pl.when(
                pl.col("transit_access").is_in(DROVE_ACCESS_EGRESS)
                | pl.col("transit_egress").is_in(DROVE_ACCESS_EGRESS)
            )
            .then(pl.lit(DaysimMode.DRIVE_TRANSIT.value))
            .otherwise(pl.lit(DaysimMode.WALK_TRANSIT.value))
        )
        .otherwise(pl.lit(DaysimMode.OTHER.value))
    )

    # Compute DaySim path type
    trips_daysim = trips_daysim.with_columns(
        path=pl.when(pl.col("mode_type") == ModeType.CAR.value)
        .then(pl.lit(DaysimPathType.FULL_NETWORK.value))
        .when(
            pl.col("mode").is_in([
                DaysimMode.WALK_TRANSIT.value,
                DaysimMode.DRIVE_TRANSIT.value,
            ])
        )
        .then(
            pl.when(
                pl.any_horizontal(
                    pl.col("mode_1", "mode_2", "mode_3", "mode_4")
                    == Mode.FERRY.value
                )
            )
            .then(pl.lit(DaysimPathType.FERRY.value))
            .when(
                pl.any_horizontal(
                    pl.col("mode_1", "mode_2", "mode_3", "mode_4")
                    == Mode.BART.value
                )
            )
            .then(pl.lit(DaysimPathType.BART.value))
            .when(
                pl.any_horizontal(
                    pl.col("mode_1", "mode_2", "mode_3", "mode_4").is_in([
                        Mode.RAIL_INTERCITY.value,
                        Mode.RAIL_OTHER.value,
                        Mode.BUS_EXPRESS.value,
                    ])
                )
            )
            .then(pl.lit(DaysimPathType.PREMIUM.value))
            .when(
                pl.any_horizontal(
                    pl.col("mode_1", "mode_2", "mode_3", "mode_4").is_in([
                        Mode.MUNI_METRO.value,
                        Mode.RAIL.value,
                        Mode.STREETCAR.value,
                    ])
                )
            )
            .then(pl.lit(DaysimPathType.LRT.value))
            .otherwise(pl.lit(DaysimPathType.BUS.value))
        )
        .otherwise(pl.lit(DaysimPathType.NONE.value))
    )

    # Compute DaySim driver/passenger code
    trips_daysim = trips_daysim.with_columns(
        dorp=pl.when(
            pl.col("mode").is_in([
                DaysimMode.SOV.value,
                DaysimMode.HOV2.value,
                DaysimMode.HOV3.value,
            ])
        )
        .then(
            pl.when(
                pl.col("driver").is_in([
                    Driver.DRIVER.value,
                    Driver.BOTH.value,
                ])
            )
            .then(pl.lit(DaysimDriverPassenger.DRIVER.value))
            .when(pl.col("driver") == Driver.PASSENGER.value)
            .then(pl.lit(DaysimDriverPassenger.PASSENGER.value))
            .otherwise(pl.lit(DaysimDriverPassenger.MISSING.value))
        )
        .when(pl.col("mode") == DaysimMode.TNC.value)
        .then(
            pl.when(pl.col("num_travelers") == VehicleOccupancy.SOV.value)
            .then(pl.lit(DaysimDriverPassenger.TNC_ALONE.value))
            .when(pl.col("num_travelers") == VehicleOccupancy.HOV2.value)
            .then(pl.lit(DaysimDriverPassenger.TNC_2.value))
            .when(pl.col("num_travelers") > VehicleOccupancy.HOV3_MIN.value)
            .then(pl.lit(DaysimDriverPassenger.TNC_3PLUS.value))
        )
        .otherwise(pl.lit(DaysimDriverPassenger.NA.value))
    )

    # Add default expansion factor
    trips_daysim = trips_daysim.with_columns(
        trexpfac=pl.lit(1.0),
    )

    # Select DaySim trip fields
    trip_cols = [
        "hhno",
        "pno",
        "day",
        "tripno",
        "opurp",
        "dpurp",
        "opcl",
        "otaz",
        "dpcl",
        "dtaz",
        "mode",
        "path",
        "dorp",
        "deptm",
        "arrtm",
        "oxcord",
        "oycord",
        "dxcord",
        "dycord",
        "trexpfac",
    ]

    return (
        trips_daysim
        .select(trip_cols)
        .sort(by=["hhno", "pno", "day", "tripno"])
    )


# =============================================================================
# Main Step Function
# =============================================================================

@step(validate=False)
def format_daysim(
    persons: pl.DataFrame,
    households: pl.DataFrame,
    trips: pl.DataFrame,
    days: pl.DataFrame | None = None,
) -> dict[str, pl.DataFrame]:
    """Format canonical survey data to DaySim model specification.

    Transforms person, household, and trip data from canonical format to DaySim
    format required by the activity-based travel demand model. This includes:
    - Person type classification based on age, employment, and student status
    - Household composition calculation from person data
    - Trip mode, path type, and driver/passenger code derivation
    - Day completeness computation for survey weighting (optional)

    This step produces core DaySim tables without tour-dependent outputs.
    Tour-dependent formatting (PersonDay table with tour counts) should be
    handled in a separate tour extraction step.

    Args:
        persons: Canonical person data with demographic and location fields
        households: Canonical household data with income and dwelling fields
        trips: Canonical trip data with mode, purpose, and timing fields
        days: Optional day-level data for completeness calculation

    Returns:
        Dictionary with keys:
        - daysim_persons: Formatted person data
        - daysim_households: Formatted household data
        - daysim_trips: Formatted trip data
    """
    logger.info("Starting DaySim formatting")

    # Compute day completeness if days data provided
    day_completeness = _compute_day_completeness(days)

    # Format each table
    daysim_persons = _format_persons(persons, day_completeness)
    logger.info("Formatted %d persons", len(daysim_persons))

    daysim_households = _format_households(households, daysim_persons)
    logger.info("Formatted %d households", len(daysim_households))

    daysim_trips = _format_trips(trips)
    logger.info("Formatted %d trips", len(daysim_trips))

    logger.info("DaySim formatting complete")

    return {
        "daysim_persons": daysim_persons,
        "daysim_households": daysim_households,
        "daysim_trips": daysim_trips,
    }
