"""DaySim Formatting Mappings and Custom Steps."""

import logging
from enum import IntEnum

from data_canon.codebook.daysim import (
    DaysimGender,
    DaysimPaidParking,
    DaysimPathType,
    DaysimPurpose,
    DaysimResidenceOwnership,
    DaysimResidenceType,
    DaysimStudentType,
)
from data_canon.codebook.households import (
    IncomeDetailed,
    IncomeFollowup,
    ResidenceRentOwn,
    ResidenceType,
)
from data_canon.codebook.persons import (
    AgeCategory,
    Gender,
    Student,
    WorkParking,
)
from data_canon.codebook.trips import (
    AccessEgressMode,
    Mode,
    PurposeCategory,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Age Thresholds for Person Type Classification
# =============================================================================

class AgeThreshold(IntEnum):
    """Age thresholds for person type classification."""

    CHILD_PRESCHOOL = 5  # Age < 5: child 0-4
    CHILD_SCHOOL = 16    # Age < 16: child 5-15
    YOUNG_ADULT = 18     # Age < 18: high school age
    ADULT = 25           # Age < 25: university age
    SENIOR = 65          # Age < 65: working age adult


# =============================================================================
# Enum-to-Enum and Enum-to-Value Mappings
# =============================================================================

# Age category to midpoint age for DaySim
AGE_TO_MIDPOINT = {
    AgeCategory.AGE_UNDER_5: 3,
    AgeCategory.AGE_5_TO_15: 10,
    AgeCategory.AGE_16_TO_17: 16,
    AgeCategory.AGE_18_TO_24: 21,
    AgeCategory.AGE_25_TO_34: 30,
    AgeCategory.AGE_35_TO_44: 40,
    AgeCategory.AGE_45_TO_54: 50,
    AgeCategory.AGE_55_TO_64: 60,
    AgeCategory.AGE_65_TO_74: 70,
    AgeCategory.AGE_75_TO_84: 80,
    AgeCategory.AGE_85_AND_UP: 90,
}

# Gender to DaySim gender codes
GENDER_TO_DAYSIM = {
    Gender.FEMALE: DaysimGender.FEMALE,
    Gender.MALE: DaysimGender.MALE,
    Gender.NON_BINARY: DaysimGender.OTHER,
    Gender.OTHER: DaysimGender.OTHER,
    Gender.MISSING: DaysimGender.MISSING,
    Gender.PNTA: DaysimGender.MISSING,
}

# Student status to DaySim student type
STUDENT_TO_DAYSIM = {
    Student.FULLTIME_INPERSON: DaysimStudentType.FULL_TIME,
    Student.PARTTIME_INPERSON: DaysimStudentType.PART_TIME,
    Student.NONSTUDENT: DaysimStudentType.NOT_STUDENT,
    Student.PARTTIME_ONLINE: DaysimStudentType.PART_TIME,
    Student.FULLTIME_ONLINE: DaysimStudentType.FULL_TIME,
    Student.MISSING: DaysimStudentType.MISSING,
}

# Work parking to DaySim paid parking (simplified from survey to binary)
WORK_PARK_TO_DAYSIM = {
    WorkParking.FREE: DaysimPaidParking.FREE,
    WorkParking.EMPLOYER_PAYS_ALL: DaysimPaidParking.FREE,
    WorkParking.EMPLOYER_DISCOUNT: DaysimPaidParking.PAID,
    WorkParking.PERSONAL_PAY: DaysimPaidParking.PAID,
    WorkParking.MISSING: DaysimPaidParking.MISSING,
    WorkParking.NOT_APPLICABLE: DaysimPaidParking.MISSING,
    WorkParking.DONT_KNOW: DaysimPaidParking.MISSING,
}

# Residence ownership to DaySim codes
RESIDENCE_OWN_TO_DAYSIM = {
    ResidenceRentOwn.OWN: DaysimResidenceOwnership.OWN,
    ResidenceRentOwn.RENT: DaysimResidenceOwnership.RENT,
    ResidenceRentOwn.NOPAYMENT_EMPLOYER: DaysimResidenceOwnership.OTHER,
    ResidenceRentOwn.NOPAYMENT_OTHER: DaysimResidenceOwnership.OTHER,
    ResidenceRentOwn.OTHER: DaysimResidenceOwnership.OTHER,
    ResidenceRentOwn.MISSING: DaysimResidenceOwnership.MISSING,
    ResidenceRentOwn.PNTA: DaysimResidenceOwnership.MISSING,
}

# Residence type to DaySim codes
RESIDENCE_TYPE_TO_DAYSIM = {
    ResidenceType.SFH: DaysimResidenceType.SINGLE_FAMILY,
    ResidenceType.TOWNHOUSE: DaysimResidenceType.DUPLEX_TOWNHOUSE,
    ResidenceType.MULTIFAMILY: DaysimResidenceType.APARTMENT,
    ResidenceType.CONDO_5TO50_UNITS: DaysimResidenceType.APARTMENT,
    ResidenceType.CONDO_50PLUS_UNITS: DaysimResidenceType.APARTMENT,
    ResidenceType.SENIOR: DaysimResidenceType.APARTMENT,
    ResidenceType.MANUFACTURED: DaysimResidenceType.MOBILE_HOME,
    ResidenceType.GROUP_QUARTERS: DaysimResidenceType.DORM,
    ResidenceType.MISSING: DaysimResidenceType.MISSING,
    ResidenceType.BOAT_RV: DaysimResidenceType.OTHER,
}

# Income categories to midpoint values (detailed from survey)
INCOME_DETAILED_TO_MIDPOINT = {
    IncomeDetailed.INCOME_UNDER15: 7500,
    IncomeDetailed.INCOME_15TO25: 20000,
    IncomeDetailed.INCOME_25TO35: 30000,
    IncomeDetailed.INCOME_35TO50: 42500,
    IncomeDetailed.INCOME_50TO75: 62500,
    IncomeDetailed.INCOME_75TO100: 87500,
    IncomeDetailed.INCOME_100TO150: 125000,
    IncomeDetailed.INCOME_150TO200: 175000,
    IncomeDetailed.INCOME_200TO250: 225000,
    IncomeDetailed.INCOME_250_OR_MORE: 350000,
    IncomeDetailed.PNTA: -1,
}

# Income followup categories to midpoint values
INCOME_FOLLOWUP_TO_MIDPOINT = {
    IncomeFollowup.INCOME_UNDER25: 12500,
    IncomeFollowup.INCOME_25TO50: 37500,
    IncomeFollowup.INCOME_50TO75: 62500,
    IncomeFollowup.INCOME_75TO100: 87500,
    IncomeFollowup.INCOME_100TO200: 150000,
    IncomeFollowup.INCOME_200_OR_MORE: 250000,
    IncomeFollowup.MISSING: -1,
    IncomeFollowup.PNTA: -1,
}

# Purpose category to DaySim purpose codes
PURPOSE_TO_DAYSIM = {
    PurposeCategory.HOME: DaysimPurpose.HOME,
    PurposeCategory.WORK: DaysimPurpose.WORK,
    PurposeCategory.WORK_RELATED: DaysimPurpose.PERSONAL_BUSINESS,
    PurposeCategory.SCHOOL: DaysimPurpose.SCHOOL,
    PurposeCategory.SCHOOL_RELATED: DaysimPurpose.SCHOOL,
    PurposeCategory.ESCORT: DaysimPurpose.ESCORT,
    PurposeCategory.SHOP: DaysimPurpose.SHOP,
    PurposeCategory.MEAL: DaysimPurpose.MEAL,
    PurposeCategory.SOCIALREC: DaysimPurpose.SOCIAL_REC,
    PurposeCategory.ERRAND: DaysimPurpose.PERSONAL_BUSINESS,
    PurposeCategory.CHANGE_MODE: DaysimPurpose.CHANGE_MODE,
    PurposeCategory.OVERNIGHT: DaysimPurpose.OTHER,
    PurposeCategory.OTHER: DaysimPurpose.OTHER,
    PurposeCategory.MISSING: DaysimPurpose.OTHER,  # Map to OTHER rather than -1
    PurposeCategory.PNTA: DaysimPurpose.OTHER,
    PurposeCategory.NOT_IMPUTABLE: DaysimPurpose.OTHER,
}

# Transit mode to DaySim path type mapping
TRANSIT_MODE_TO_PATH_TYPE = {
    Mode.FERRY: DaysimPathType.FERRY,
    Mode.BART: DaysimPathType.BART,
    Mode.RAIL_INTERCITY: DaysimPathType.PREMIUM,
    Mode.RAIL_OTHER: DaysimPathType.PREMIUM,
    Mode.BUS_EXPRESS: DaysimPathType.PREMIUM,
    Mode.MUNI_METRO: DaysimPathType.LRT,
    Mode.RAIL: DaysimPathType.LRT,
    Mode.STREETCAR: DaysimPathType.LRT,
}

# Access/egress mode codes that indicate drove to transit
DROVE_ACCESS_EGRESS = [
    AccessEgressMode.TNC.value,
    AccessEgressMode.CAR_HOUSEHOLD.value,
    AccessEgressMode.CAR_OTHER.value,
    AccessEgressMode.DROPOFF_HOUSEHOLD.value,
    AccessEgressMode.DROPOFF_OTHER.value,
]


# =============================================================================
# Convert Enum Mappings to Integer Dictionaries for Polars
# =============================================================================

# Polars replace() requires integer keys, so convert enum mappings
AGE_MAP = {k.value: v for k, v in AGE_TO_MIDPOINT.items()}
GENDER_MAP = {k.value: v.value for k, v in GENDER_TO_DAYSIM.items()}
STUDENT_MAP = {k.value: v.value for k, v in STUDENT_TO_DAYSIM.items()}
WORK_PARK_MAP = {k: v.value for k, v in WORK_PARK_TO_DAYSIM.items()}
PURPOSE_MAP = {k.value: v.value for k, v in PURPOSE_TO_DAYSIM.items()}
