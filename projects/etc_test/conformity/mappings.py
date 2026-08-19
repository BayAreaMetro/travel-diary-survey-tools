"""The ETC vendor codebook, expressed as maps onto canonical codes.

Every entry here is a claim about how one of the vendor's answer codes
corresponds to a ``data_canon`` code.  Where the two codebooks genuinely
disagree -- a distinction one side draws and the other does not -- the comment
above the map says so, and those comments are the source list for the schema
changes to ask the vendor for.

Kept apart from the conversion logic in :mod:`conform_etc` because it is
reference data, not behaviour: it is read and argued over far more often than
it is executed.
"""

from data_canon.codebook.days import MadeTravel, NoTravelReason
from data_canon.codebook.households import IncomeBroad, ResidenceRentOwn
from data_canon.codebook.persons import (
    AgeCategory,
    CommuteFreq,
    Education,
    Employment,
    Gender,
    Industry,
    JobType,
    Occupation,
    Race,
    Relationship,
    SchoolType,
    WorkParking,
)
from data_canon.codebook.trips import Driver, Mode, ModeType, Purpose, PurposeCategory, TNCType

# Identifier construction ------------------------------------------------------
# The vendor keys everything on a 6-digit household "Sample Number" plus a
# within-household person number and a within-day trip number.  Canonical ids
# are nested from those so they stay stable and human-readable.
ID_STRIDE = 100

# Vendor answer codes used in comparisons (the vendor reuses these everywhere)
V_YES = 1
V_NO = 2
V_PROXY_RETRIEVAL = 4  # "I will provide it for them online or over the phone"


# Household mappings -----------------------------------------------------------
RENT_OWN = {
    1: ResidenceRentOwn.OWN.value,
    2: ResidenceRentOwn.RENT.value,
    3: ResidenceRentOwn.NOPAYMENT_EMPLOYER.value,
    4: ResidenceRentOwn.NOPAYMENT_OTHER.value,
    96: ResidenceRentOwn.OTHER.value,
    98: ResidenceRentOwn.MISSING.value,
    99: ResidenceRentOwn.PNTA.value,
}

# The vendor asks income in 26 narrow brackets; canonical keeps the broad bin
# for weighting and the midpoint for DaySim's continuous household income.
INCOME_BIN = {
    **dict.fromkeys(range(1, 5), IncomeBroad.INCOME_UNDER25.value),
    **dict.fromkeys(range(5, 10), IncomeBroad.INCOME_25TO50.value),
    **dict.fromkeys(range(10, 15), IncomeBroad.INCOME_50TO75.value),
    **dict.fromkeys(range(15, 20), IncomeBroad.INCOME_75TO100.value),
    **dict.fromkeys(range(20, 26), IncomeBroad.INCOME_100TO200.value),
    26: IncomeBroad.INCOME_200_OR_MORE.value,
    98: IncomeBroad.MISSING.value,
    99: IncomeBroad.PNTA.value,
}

# Bracket midpoints in dollars.  The open-ended top bracket uses the same 1.25x
# upper-bound estimate as utils.helpers.get_income_midpoint.
INCOME_MIDPOINT = {
    1: 5_000,
    2: 12_500,
    3: 17_500,
    4: 22_500,
    5: 27_500,
    6: 32_500,
    7: 37_500,
    8: 42_500,
    9: 47_500,
    10: 52_500,
    11: 57_500,
    12: 62_500,
    13: 67_500,
    14: 72_500,
    15: 77_500,
    16: 82_500,
    17: 87_500,
    18: 92_500,
    19: 97_500,
    20: 102_500,
    21: 110_000,
    22: 120_000,
    23: 130_000,
    24: 142_500,
    25: 175_000,
    26: 225_000,
}


# Person mappings --------------------------------------------------------------
# NOTE the code flip: the vendor uses 1=Male, canonical uses 1=Female.
GENDER = {
    1: Gender.MALE.value,
    2: Gender.FEMALE.value,
    96: Gender.OTHER.value,
    99: Gender.PNTA.value,
}

# The vendor orders vocational training last; canonical places it between high
# school and an associate's degree, so this is a reorder, not a pass-through.
EDUCATION = {
    1: Education.LESS_HIGH_SCHOOL.value,
    2: Education.HIGHSCHOOL.value,
    3: Education.SOME_COLLEGE.value,
    4: Education.ASSOCIATE.value,
    5: Education.BACHELORS.value,
    6: Education.GRAD.value,
    7: Education.VOCATIONAL.value,
    98: Education.MISSING.value,
    99: Education.PNTA.value,
}

RELATIONSHIP = {
    1: Relationship.SELF.value,
    2: Relationship.SPOUSE_PARTNER.value,
    3: Relationship.PARENT.value,
    4: Relationship.SIBLING.value,
    5: Relationship.CHILD.value,
    6: Relationship.OTHER_RELATIVE.value,
    7: Relationship.NONRELATIVE.value,
    8: Relationship.NONRELATIVE.value,
}

# Canonical Employment has a single self-employed code, so the vendor's
# full-time and part-time self-employment collapse together.
EMPLOYMENT_STATUS = {
    1: Employment.EMPLOYED_FULLTIME.value,
    2: Employment.EMPLOYED_PARTTIME.value,
    3: Employment.EMPLOYED_SELF.value,
    4: Employment.EMPLOYED_SELF.value,
    5: Employment.EMPLOYED_UNPAID.value,
    98: Employment.MISSING.value,
    99: Employment.MISSING.value,
}

# Canonical Employment distinguishes only looking / not looking, so retired,
# homemaker and disabled all land on UNEMPLOYED_NOT_LOOKING.
NOT_EMPLOYED = {
    1: Employment.UNEMPLOYED_NOT_LOOKING.value,
    2: Employment.EMPLOYED_UNPAID.value,
    3: Employment.UNEMPLOYED_NOT_LOOKING.value,
    4: Employment.UNEMPLOYED_NOT_LOOKING.value,
    5: Employment.UNEMPLOYED_LOOKING.value,
    6: Employment.UNEMPLOYED_NOT_LOOKING.value,
    7: Employment.UNEMPLOYED_NOT_LOOKING.value,
    96: Employment.MISSING.value,
    98: Employment.MISSING.value,
    99: Employment.MISSING.value,
}

SCHOOL_TYPE = {
    1: SchoolType.DAYCARE.value,
    2: SchoolType.ELEMENTARY.value,
    3: SchoolType.MIDDLE_SCHOOL.value,
    4: SchoolType.HIGH_SCHOOL.value,
    5: SchoolType.VOCATIONAL.value,
    6: SchoolType.COLLEGE_2YEAR.value,
    7: SchoolType.COLLEGE_4YEAR.value,
    8: SchoolType.GRADUATE_SCHOOL.value,
    96: SchoolType.OTHER.value,
    98: SchoolType.MISSING.value,
    99: SchoolType.PNTA.value,
}

# Vendor occupation 1-23 and industry 1-20 already follow the SOC and NAICS
# orders that canonical uses, so those ranges pass straight through.  The
# vendor's "not applicable" answer is left unmapped so non-workers stay null.
OCCUPATION = {
    **{code: code for code in range(1, 24)},
    24: Occupation.OTHER_PLEASE_SPECIFY.value,
    96: Occupation.OTHER_PLEASE_SPECIFY.value,
    98: Occupation.MISSING.value,
    99: Occupation.MISSING.value,
}

INDUSTRY = {
    **{code: code for code in range(1, 21)},
    21: Industry.OTHER_SPECIFY.value,
    96: Industry.OTHER_SPECIFY.value,
    98: Industry.MISSING.value,
    99: Industry.MISSING.value,
}

JOB_TYPE = {
    1: JobType.FIXED.value,
    2: JobType.WFH.value,
    3: JobType.HYBRID.value,
    4: JobType.VARIES.value,
    5: JobType.DELIVERY.value,
}

WORK_PARKING = {
    1: WorkParking.FREE.value,
    2: WorkParking.EMPLOYER_PAYS_ALL.value,
    3: WorkParking.EMPLOYER_DISCOUNT.value,
    4: WorkParking.EMPLOYER_DISCOUNT.value,
    5: WorkParking.PERSONAL_PAY.value,
    6: WorkParking.PERSONAL_PAY.value,
    7: WorkParking.PERSONAL_PAY.value,
    8: WorkParking.NOT_APPLICABLE.value,
    98: WorkParking.DONT_KNOW.value,
}
# Employer-provided parking subsidies, read off that same question.
WORK_PARKING_FREE_CODES = (2,)
WORK_PARKING_DISCOUNT_CODES = (3, 4)

# The vendor collapses 2-3 days a week into one answer while canonical keeps
# them apart, so that bucket is reported as 3 days.
COMMUTE_FREQ = {
    1: CommuteFreq.DAYS_6_7.value,
    2: CommuteFreq.DAYS_5.value,
    3: CommuteFreq.DAYS_4.value,
    4: CommuteFreq.DAYS_3.value,
    5: CommuteFreq.DAY_1.value,
    6: CommuteFreq.DAYS_1_3_PER_MONTH.value,
    7: CommuteFreq.LESS_THAN_MONTHLY.value,
    8: CommuteFreq.NEVER.value,
}
# Telecommute frequency adds two "never" answers for why the person does not.
TELEWORK_FREQ = {**COMMUTE_FREQ, 9: CommuteFreq.NEVER.value}

# Fallback only: the vendor's own age bands, used when numeric age is absent.
# Vendor bands 2 (5-13) and 3 (14-15) both fall inside canonical AGE_5_TO_15.
VENDOR_AGE_CATEGORY = {
    1: AgeCategory.AGE_UNDER_5.value,
    2: AgeCategory.AGE_5_TO_15.value,
    3: AgeCategory.AGE_5_TO_15.value,
    4: AgeCategory.AGE_16_TO_17.value,
    5: AgeCategory.AGE_18_TO_24.value,
    6: AgeCategory.AGE_25_TO_34.value,
    7: AgeCategory.AGE_35_TO_44.value,
    8: AgeCategory.AGE_45_TO_54.value,
    9: AgeCategory.AGE_55_TO_64.value,
    10: AgeCategory.AGE_65_TO_74.value,
    11: AgeCategory.AGE_75_TO_84.value,
    12: AgeCategory.AGE_85_AND_UP.value,
}

# Upper bound (exclusive) of each canonical age band; ages at or above the last
# bound fall into AGE_85_AND_UP.
AGE_BREAKS = (
    (5, AgeCategory.AGE_UNDER_5.value),
    (16, AgeCategory.AGE_5_TO_15.value),
    (18, AgeCategory.AGE_16_TO_17.value),
    (25, AgeCategory.AGE_18_TO_24.value),
    (35, AgeCategory.AGE_25_TO_34.value),
    (45, AgeCategory.AGE_35_TO_44.value),
    (55, AgeCategory.AGE_45_TO_54.value),
    (65, AgeCategory.AGE_55_TO_64.value),
    (75, AgeCategory.AGE_65_TO_74.value),
    (85, AgeCategory.AGE_75_TO_84.value),
)

# The vendor asks race and Hispanic origin as ONE multi-select.  Splitting it
# loses detail canonical can hold: Asian and Pacific Islander arrive merged,
# Middle Eastern / North African has no canonical home and lands on OTHER, and
# the Puerto Rican / Cuban distinctions are never asked at all.
RACE_ETHNICITY_TO_RACE = {
    1: Race.AFAM.value,
    3: Race.ASIAN.value,
    4: Race.NATIVE.value,
    5: Race.OTHER.value,
    6: Race.WHITE.value,
    96: Race.OTHER.value,
}
HISPANIC_CODE = 2
PNTA_CODE = 99


# Day mappings -----------------------------------------------------------------
MADE_TRAVEL = {
    1: MadeTravel.YES.value,
    2: MadeTravel.NO.value,
    # "Away from the residence for the entire day" is not a canonical answer;
    # it is recorded as no in-diary travel.
    96: MadeTravel.NO.value,
}

NO_TRAVEL_REASON = {
    1: NoTravelReason.NOWORK.value,
    2: NoTravelReason.WFH.value,
    3: NoTravelReason.HANGOUT.value,
    4: NoTravelReason.HANGOUT.value,
    5: NoTravelReason.HOMESCHOOL.value,
    6: NoTravelReason.HOLIDAY.value,
    7: NoTravelReason.WEATHER.value,
    8: NoTravelReason.SICK.value,
    9: NoTravelReason.OTHER.value,
    10: NoTravelReason.OTHER.value,
    11: NoTravelReason.NO_TRANSPORT.value,
    12: NoTravelReason.DELIVERY.value,
    13: NoTravelReason.OTHER.value,
    96: NoTravelReason.OTHER.value,
    98: NoTravelReason.DONT_KNOW.value,
    99: NoTravelReason.PNTA.value,
}


# Trip mappings ----------------------------------------------------------------
MODE_TYPE = {
    1: ModeType.CAR.value,
    2: ModeType.CAR.value,
    3: ModeType.CAR.value,
    4: ModeType.CAR.value,
    5: ModeType.TAXI.value,
    6: ModeType.TNC.value,
    7: ModeType.SCHOOL_BUS.value,
    8: ModeType.CAR.value,
    9: ModeType.TRANSIT.value,
    10: ModeType.TRANSIT.value,
    11: ModeType.TRANSIT.value,
    12: ModeType.FERRY.value,
    13: ModeType.SHUTTLE.value,
    14: ModeType.BIKE.value,
    15: ModeType.BIKESHARE.value,
    16: ModeType.WALK.value,
    96: ModeType.OTHER.value,
    98: ModeType.MISSING.value,
    99: ModeType.MISSING.value,
}

MODE = {
    1: Mode.HOUSEHOLD_VEHICLE.value,
    2: Mode.HOUSEHOLD_VEHICLE.value,
    3: Mode.CAR_OTHER.value,
    4: Mode.MOTORCYCLE.value,
    5: Mode.TAXI.value,
    6: Mode.TNC.value,
    7: Mode.BUS_SCHOOL.value,
    8: Mode.VANPOOL.value,
    9: Mode.BUS_LOCAL.value,
    10: Mode.RAIL.value,
    11: Mode.PARATRANSIT.value,
    12: Mode.FERRY.value,
    13: Mode.SHUTTLE.value,
    14: Mode.BICYCLE.value,
    15: Mode.MICROMOBILITY.value,
    16: Mode.WALK.value,
    96: Mode.OTHER_UNKNOWN.value,
    98: Mode.MISSING.value,
    99: Mode.MISSING.value,
}

# Follow-up questions that sharpen the headline mode.
BICYCLE_USED = {
    1: Mode.BIKE.value,
    2: Mode.BIKE_ELECTRIC.value,
    3: Mode.BIKE_BORROWED.value,
    4: Mode.BIKE_SHARE.value,
    5: Mode.BIKE_SHARE_ELECTRIC.value,
    6: Mode.BIKE_RENTED.value,
}
BIKE_SHARE_CODES = (4, 5)

MICROMOBILITY_DEVICE = {
    1: Mode.MOPED.value,
    2: Mode.SCOOTER_SHARE.value,
    3: Mode.SKATE.value,
    4: Mode.OTHER_ALT.value,
}
SCOOTER_SHARE_CODE = 2

TNC_TYPE = {
    1: TNCType.POOLED.value,
    2: TNCType.REGULAR.value,
    3: TNCType.PREMIUM.value,
    98: TNCType.UNKNOWN.value,
}

DRIVER = {
    1: Driver.DRIVER.value,
    2: Driver.PASSENGER.value,
    3: Driver.BOTH.value,
}

# Vendor "Activity Type Code" -> canonical Purpose.  Working at home maps to
# HOME: canonical has no at-home work purpose, and the trip that ends there is
# a trip home.  Telework is carried on the person instead (telework_freq).
ACTIVITY_PURPOSE = {
    1: Purpose.HOME.value,
    2: Purpose.HOME.value,
    3: Purpose.PRIMARY_WORKPLACE.value,
    4: Purpose.OTHER_WORK.value,
    5: Purpose.WORK_ACTIVITY.value,
    6: Purpose.SCHOOL.value,
    7: Purpose.OTHER_CLASS.value,
    8: Purpose.ERRAND_NO_APPT.value,
    9: Purpose.ROUTINE_SHOPPING.value,
    10: Purpose.ERRAND_WITH_APPT.value,
    11: Purpose.DINING.value,
    12: Purpose.ENTERTAINMENT.value,
    13: Purpose.ENTERTAINMENT.value,
    14: Purpose.RELIGIOUS_CIVIC.value,
    15: Purpose.SOCIAL.value,
    16: Purpose.WORK_TRAVEL.value,
    17: Purpose.WORK_TRAVEL.value,
    18: Purpose.ESCORT_WORK.value,
    19: Purpose.ESCORT_SCHOOL.value,
    20: Purpose.ESCORT_CHILDCARE.value,
    21: Purpose.PICK_UP_AND_DROP_OFF.value,
    22: Purpose.MODE_CHANGE.value,
    96: Purpose.OTHER.value,
    98: Purpose.MISSING.value,
    99: Purpose.PNTA.value,
}

# "Type of Place" is a second, independent question that narrows some
# activities: (activity codes, place codes) -> sharper purpose.
PLACE_REFINEMENTS = (
    ((6,), (10,), Purpose.DAYCARE.value),
    ((6,), (11, 12, 13), Purpose.K12_SCHOOL.value),
    ((6,), (14,), Purpose.COLLEGE.value),
    ((8,), (16,), Purpose.GAS.value),
    ((9,), (17,), Purpose.GROCERY.value),
    ((10,), (9,), Purpose.MEDICAL.value),
    ((13,), (18, 21), Purpose.EXERCISE.value),
    ((1, 2), (2,), Purpose.OTHER_RESIDENCE.value),
)
# Overnight lodging overrides whatever activity was reported there.
LODGING_PLACE_CODES = (22,)

PURPOSE_CATEGORY = {
    Purpose.HOME.value: PurposeCategory.HOME.value,
    Purpose.OTHER_RESIDENCE.value: PurposeCategory.OTHER.value,
    Purpose.PRIMARY_WORKPLACE.value: PurposeCategory.WORK.value,
    Purpose.OTHER_WORK.value: PurposeCategory.WORK.value,
    Purpose.WORK_ACTIVITY.value: PurposeCategory.WORK_RELATED.value,
    Purpose.WORK_TRAVEL.value: PurposeCategory.WORK_RELATED.value,
    Purpose.SCHOOL.value: PurposeCategory.SCHOOL.value,
    Purpose.DAYCARE.value: PurposeCategory.SCHOOL.value,
    Purpose.K12_SCHOOL.value: PurposeCategory.SCHOOL.value,
    Purpose.COLLEGE.value: PurposeCategory.SCHOOL.value,
    Purpose.OTHER_CLASS.value: PurposeCategory.SCHOOL_RELATED.value,
    Purpose.ERRAND_NO_APPT.value: PurposeCategory.ERRAND.value,
    Purpose.ERRAND_WITH_APPT.value: PurposeCategory.ERRAND.value,
    Purpose.GAS.value: PurposeCategory.ERRAND.value,
    Purpose.MEDICAL.value: PurposeCategory.ERRAND.value,
    Purpose.ROUTINE_SHOPPING.value: PurposeCategory.SHOP.value,
    Purpose.GROCERY.value: PurposeCategory.SHOP.value,
    Purpose.DINING.value: PurposeCategory.MEAL.value,
    Purpose.ENTERTAINMENT.value: PurposeCategory.SOCIALREC.value,
    Purpose.EXERCISE.value: PurposeCategory.SOCIALREC.value,
    Purpose.RELIGIOUS_CIVIC.value: PurposeCategory.SOCIALREC.value,
    Purpose.SOCIAL.value: PurposeCategory.SOCIALREC.value,
    Purpose.ESCORT_WORK.value: PurposeCategory.ESCORT.value,
    Purpose.ESCORT_SCHOOL.value: PurposeCategory.ESCORT.value,
    Purpose.ESCORT_CHILDCARE.value: PurposeCategory.ESCORT.value,
    Purpose.PICK_UP_AND_DROP_OFF.value: PurposeCategory.ESCORT.value,
    Purpose.MODE_CHANGE.value: PurposeCategory.CHANGE_MODE.value,
    Purpose.TEMP_LODGING.value: PurposeCategory.OVERNIGHT.value,
    Purpose.OTHER.value: PurposeCategory.OTHER.value,
    Purpose.MISSING.value: PurposeCategory.MISSING.value,
    Purpose.PNTA.value: PurposeCategory.PNTA.value,
}
