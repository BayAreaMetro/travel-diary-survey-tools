"""Codebook definitions for CT-RAMP related enumerations."""

import polars as pl

from data_canon.codebook.persons import SchoolType
from data_canon.codebook.trips import ModeType, Purpose
from data_canon.core.labeled_enum import LabeledEnum


class FreeParkingChoice(LabeledEnum):
    """Enumeration for free parking choice categories."""

    PARK_FOR_FREE = 1, "park for free"
    PAY_TO_PARK = 2, "pay to park"


class MandatoryTourFrequency(LabeledEnum):
    """Enumeration for mandatory tour frequency categories."""

    ONE_WORK_TOUR = 1, "one work tour"
    TWO_WORK_TOURS = 2, "two work tours"
    ONE_SCHOOL_TOUR = 3, "one school tour"
    TWO_SCHOOL_TOURS = 4, "two school tours"
    WORK_AND_SCHOOL = 5, "one work tour and one school tour"


class TourComposition(LabeledEnum):
    """Enumeration for tour composition categories."""

    ADULTS_ONLY = 1, "adults only"
    CHILDREN_ONLY = 2, "children only"
    ADULTS_AND_CHILDREN = 3, "adults and children"


class WalkToTransitSubZone(LabeledEnum):
    """Enumeration for walk-to-transit subzone categories."""

    CANNOT_WALK = 0, "cannot walk to transit"
    SHORT_WALK = 1, "short-walk"
    LONG_WALK = 2, "long-walk"


# NOTE: This is basically a re-map of the canonical PersonType enum
# Can we just use that directly instead of redefining here?
class PersonType(LabeledEnum):
    """Enumeration for person type categories."""

    FULL_TIME_WORKER = 1, "Full-time worker"
    PART_TIME_WORKER = 2, "Part-time worker"
    UNIVERSITY_STUDENT = 3, "University student"
    NON_WORKER = 4, "Nonworker"
    RETIRED = 5, "Retired"
    CHILD_NON_DRIVING_AGE = 6, "Child of non-driving age"
    CHILD_DRIVING_AGE = 7, "Child of driving age"
    CHILD_UNDER_5 = 8, "Child too young for school"


class EmploymentCategory(LabeledEnum):
    """Enumeration for employment category."""

    EMPLOYED = 1, "Employed"
    NOT_EMPLOYED = 2, "Not employed"


class StudentCategory(LabeledEnum):
    """Enumeration for student category."""

    STUDENT = 1, "Student"
    NOT_STUDENT = 2, "Not a student"


class CTRAMPMode(LabeledEnum):
    """CTRAMP travel mode codes.

    Reference: TravelModes#tour-and-trip-modes on MTC modeling wiki.
    """

    DRIVE_ALONE = 1, "Drive alone"
    SHARED_RIDE_2 = 2, "Shared ride 2"
    SHARED_RIDE_3_PLUS = 3, "Shared ride 3+"
    WALK = 4, "Walk"
    BIKE = 5, "Bike"
    WALK_TRANSIT_WALK = 6, "Walk-transit-walk"
    DRIVE_TRANSIT_WALK = 7, "Drive-transit-walk"
    WALK_TRANSIT_DRIVE = 8, "Walk-transit-drive"
    SCHOOL_BUS = 9, "School bus"


class TripModeType(LabeledEnum):
    """Enumeration for trip mode type categories."""

    DA = 1, "Drive alone"
    DA_TOLL = 2, "Drive alone - toll"
    SR2 = 3, "Shared ride 2"
    SR2_TOLL = 4, "Shared ride 2 - toll"
    SR3 = 5, "Shared ride 3+"
    SR3_TOLL = 6, "Shared ride 3+ - toll"
    WALK = 7, "Walk"
    BIKE = 8, "Bike"
    WLK_LOC_WLK = 9, "Walk to local bus"
    WLK_LRF_WLK = 10, "Walk to light rail or ferry"
    WLK_EXP_WLK = 11, "Walk to express bus"
    WLK_HVY_WLK = 12, "Walk to heavy rail"
    WLK_COM_WLK = 13, "Walk to commuter rail"
    DRV_LOC_WLK = 14, "Drive to local bus"
    DRV_LRF_WLK = 15, "Drive to light rail or ferry"
    DRV_EXP_WLK = 16, "Drive to express bus"
    DRV_HVY_WLK = 17, "Drive to heavy rail"
    DRV_COM_WLK = 18, "Drive to commuter rail"
    TAXI = 19, "Taxi"
    TNC = 20, "TNC - single party"
    TNC2 = 21, "TNC - shared"


def map_purpose_to_ctramp(
    purpose: pl.Expr,
    income: pl.Expr,
    school_type: pl.Expr,
    income_low_threshold: int,
    income_med_threshold: int,
    income_high_threshold: int,
) -> pl.Expr:
    """Map canonical trip purpose to CTRAMP purpose string.

    CTRAMP requires detailed purpose strings that distinguish work income
    levels (low/med/high/very high) and school types (grade/high/university).

    Args:
        purpose: Polars expression for canonical purpose
            (from trips.Purpose enum)
        income: Polars expression for household income (absolute dollars)
        school_type: Polars expression for school type
            (from persons.SchoolType enum):
              - K12: 5-7
              - College/Grad: 11-13
              - Not student/Missing: other values
        income_low_threshold: Income threshold for low bracket
        income_med_threshold: Income threshold for med bracket
        income_high_threshold: Income threshold for high bracket

    Returns:
        Polars expression resolving to CTRAMP purpose string
    """
    # Compute student category from student and school_type enums
    # College/grad -> "College or higher"
    # K-12 -> "Grade or high school"
    # Not student or missing -> "Not student"
    student_category = (
        pl.when(
            school_type.is_in(
                [
                    SchoolType.COLLEGE_2YEAR.value,
                    SchoolType.COLLEGE_4YEAR.value,
                    SchoolType.GRADUATE_SCHOOL.value,
                ]
            )
        )
        .then(pl.lit("College or higher"))
        .when(
            school_type.is_in(
                [
                    SchoolType.ELEMENTARY.value,
                    SchoolType.MIDDLE_SCHOOL.value,
                    SchoolType.HIGH_SCHOOL.value,
                ]
            )
        )
        .then(pl.lit("Grade or high school"))
        .otherwise(pl.lit("Not student"))
    )
    # Home purpose
    home_expr = pl.when(purpose == Purpose.HOME.value).then(pl.lit("Home"))

    # Work purposes - segmented by income
    work_purposes = [
        Purpose.PRIMARY_WORKPLACE.value,
        Purpose.WORK_ACTIVITY.value,
    ]
    work_income_segmentation = (
        pl.when(income < income_low_threshold)
        .then(pl.lit("work_low"))
        .when(income < income_med_threshold)
        .then(pl.lit("work_med"))
        .when(income < income_high_threshold)
        .then(pl.lit("work_high"))
        .otherwise(pl.lit("work_very high"))
    )
    work_expr = home_expr.when(purpose.is_in(work_purposes)).then(work_income_segmentation)

    # School purposes - segmented by student type
    k12_purposes = [Purpose.K12_SCHOOL.value, Purpose.DAYCARE.value]
    school_segmentation_expr = (
        pl.when(student_category == "College or higher")
        .then(pl.lit("university"))
        .when(student_category == "Grade or high school")
        .then(pl.lit("school_high"))
        .otherwise(pl.lit("school_grade"))
    )
    school_expr = work_expr.when(purpose.is_in(k12_purposes)).then(school_segmentation_expr)
    university_expr = school_expr.when(purpose == Purpose.COLLEGE.value).then(pl.lit("university"))

    # At-work sub-tour purposes
    atwork_expr = university_expr.when(purpose == Purpose.WORK_ACTIVITY.value).then(
        pl.lit("atwork_business")
    )
    eatout_expr = atwork_expr.when(purpose == Purpose.DINING.value).then(pl.lit("eatout"))

    # Escort purposes
    escort_purposes = [
        Purpose.DROP_OFF.value,
        Purpose.PICK_UP.value,
        Purpose.ACCOMPANY.value,
    ]
    escort_segmentation_expr = (
        pl.when(student_category.is_in(["College or higher", "Grade or high school"]))
        .then(pl.lit("escort_kids"))
        .otherwise(pl.lit("escort_no kids"))
    )
    escort_expr = eatout_expr.when(purpose.is_in(escort_purposes)).then(escort_segmentation_expr)

    # Shopping
    shopping_purposes = [
        Purpose.GROCERY.value,
        Purpose.ROUTINE_SHOPPING.value,
        Purpose.MAJOR_SHOPPING.value,
    ]
    shopping_expr = escort_expr.when(purpose.is_in(shopping_purposes)).then(pl.lit("shopping"))

    # Social/recreation
    social_purposes = [
        Purpose.SOCIAL.value,
        Purpose.ENTERTAINMENT.value,
        Purpose.EXERCISE.value,
    ]
    social_expr = shopping_expr.when(purpose.is_in(social_purposes)).then(pl.lit("social"))

    # Maintenance/errands
    maintenance_purposes = [
        Purpose.MEDICAL.value,
        Purpose.ERRAND_NO_APPT.value,
        Purpose.ERRAND_WITH_APPT.value,
    ]
    maintenance_expr = social_expr.when(purpose.is_in(maintenance_purposes)).then(
        pl.lit("othmaint")
    )

    # Discretionary
    discretionary_purposes = [
        Purpose.RELIGIOUS_CIVIC.value,
        Purpose.FAMILY_ACTIVITY.value,
    ]
    discretionary_expr = maintenance_expr.when(purpose.is_in(discretionary_purposes)).then(
        pl.lit("othdiscr")
    )

    # Default fallback
    return discretionary_expr.otherwise(pl.lit("othdiscr"))


def map_mode_to_ctramp(mode_type: pl.Expr, num_travelers: pl.Expr) -> pl.Expr:
    """Map canonical mode_type to CTRAMP mode integer code.

    Args:
        mode_type: Polars expression for canonical mode_type
            (from ModeType enum)
        num_travelers: Polars expression for number of travelers in vehicle

    Returns:
        Polars expression resolving to CTRAMP mode integer code
    """
    # Walk mode
    walk_expr = pl.when(mode_type == ModeType.WALK.value).then(pl.lit(CTRAMPMode.WALK.value))

    # Bike and micromobility modes
    bike_modes = [
        ModeType.BIKE.value,
        ModeType.BIKESHARE.value,
        ModeType.SCOOTERSHARE.value,
    ]
    bike_expr = walk_expr.when(mode_type.is_in(bike_modes)).then(pl.lit(CTRAMPMode.BIKE.value))

    # School bus
    school_bus_expr = bike_expr.when(mode_type == ModeType.SCHOOL_BUS.value).then(
        pl.lit(CTRAMPMode.SCHOOL_BUS.value)
    )

    # Transit modes (default to walk access)
    transit_modes = [ModeType.TRANSIT.value, ModeType.FERRY.value]
    transit_expr = school_bus_expr.when(mode_type.is_in(transit_modes)).then(
        pl.lit(CTRAMPMode.WALK_TRANSIT_WALK.value)
    )

    # Car modes - distinguish by occupancy
    car_modes = [
        ModeType.CAR.value,
        ModeType.CARSHARE.value,
        ModeType.TAXI.value,
        ModeType.TNC.value,
    ]
    car_occupancy_segmentation = (
        pl.when(num_travelers == 1)
        .then(pl.lit(CTRAMPMode.DRIVE_ALONE.value))
        .when(num_travelers == 2)  # noqa: PLR2004
        .then(pl.lit(CTRAMPMode.SHARED_RIDE_2.value))
        .otherwise(pl.lit(CTRAMPMode.SHARED_RIDE_3_PLUS.value))
    )
    car_expr = transit_expr.when(mode_type.is_in(car_modes)).then(car_occupancy_segmentation)

    # Default to drive alone for unknown modes
    return car_expr.otherwise(pl.lit(CTRAMPMode.DRIVE_ALONE.value))
