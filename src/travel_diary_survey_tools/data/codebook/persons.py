"""Codebook enumerations for person table."""

from .labeled_enum import LabeledEnum


class AgeCategory(LabeledEnum):
    """age value labels."""

    canonical_field_name = "age"

    UNDER_5 = (1, "Under 5")
    AGE_5_TO_15 = (2, "5 to 15")
    AGE_16_TO_17 = (3, "16 to 17")
    AGE_18_TO_24 = (4, "18 to 24")
    AGE_25_TO_34 = (5, "25 to 34")
    AGE_35_TO_44 = (6, "35 to 44")
    AGE_45_TO_54 = (7, "45 to 54")
    AGE_55_TO_64 = (8, "55 to 64")
    AGE_65_TO_74 = (9, "65 to 74")
    AGE_75_TO_84 = (10, "75 to 84")
    AGE_85_AND_UP = (11, "85 and up")


class CanDrive(LabeledEnum):
    """can_drive value labels."""

    canonical_field_name = "can_drive"

    NO = (0, "No, does not drive")
    YES = (1, "Yes, drives")
    MISSING = (995, "Missing Response")

class CanTelework(LabeledEnum):
    """can_telework value labels."""

    canonical_field_name = "can_telework"

    YES = (1, "Yes")
    NO = (2, "No")
    MISSING = (995, "Missing Response")

class CommuteFreq(LabeledEnum):
    """commute_freq value labels."""

    canonical_field_name = "commute_freq"

    COMMUTE_6_7_DAYS = (0, "Commute 6-7 days a week")
    COMMUTE_5_DAYS = (2, "Commute 5 days a week")
    COMMUTE_4_DAYS = (3, "Commute 4 days a week")
    COMMUTE_3_DAYS = (4, "Commute 3 days a week")
    COMMUTE_2_DAYS = (5, "Commute 2 days a week")
    COMMUTE_1_DAY = (6, "Commute 1 day a week")
    COMMUTE_1_3_PER_MONTH = (7, "Commute 1-3 days a month")
    LESS_THAN_MONTHLY = (8, "Less than monthly")
    MISSING = (995, "Missing Response")
    NEVER = (996, "Never")


class CommuteSubsidy(LabeledEnum):
    """commute_subsidy value labels, parent class to be referenced by specific subsidy types."""  # noqa: E501

    canonical_field_name = "commute_subsidy"

    FREE_PARK = (1, "Free parking provided by employer")
    DISCOUNT_PARKING = (2, "Discounted (partially subsidized) parking provided by employer")  # noqa: E501
    TRANSIT = (3, "Free/discounted transit fare provided by employer")
    VANPOOL = (4, "Free/discounted vanpool service provided by employer")
    CASH_IN_LIEU = (5, "Cash in lieu for carpooling, biking, or walking")
    TNC = (6, "Free/discounted rideshare / TNC (e.g., Uber, Lyft) provided by employer")  # noqa: E501
    CARSHARE = (7, "Free/discounted carshare membership provided by employer (e.g., Zipcar, Car2Go)")  # noqa: E501
    SHUTTLE = (8, "Free/discounted shuttle service to/from work provided by employer")  # noqa: E501
    BIKESHARE = (9, "Free/discounted bikeshare membership provided by employer")
    BIKE_MAINTENANCE = (10, "Free/discounted bike maintenance or bike parking provided by employer")  # noqa: E501
    OTHER = (11, "Other commute subsidy provided by employer")
    NONE = (12, "No commute subsidies provided by employer")
    DONT_KNOW = (13, "Don't know")

class CommuteSubsidyOffered(CommuteSubsidy):
    """commute_subsidy_offered value labels."""

    canonical_field_name = "commute_subsidy_offered"

class CommuteSubsidyUsed(CommuteSubsidy):
    """commute_subsidy_used value labels."""

    canonical_field_name = "commute_subsidy_used"


class Education(LabeledEnum):
    """education value labels."""

    canonical_field_name = "education"

    LESS_HIGH_SCHOOL = (1, "Less than high school")
    HIGHSCHOOL = (2, "High school graduate/GED")
    SOME_COLLEGE = (3, "Some college, no degree")
    VOCATIONAL = (4, "Vocational/technical training")
    ASSOCIATE = (5, "Associate degree")
    BACHELORS = (6, "Bachelor's degree")
    GRAD = (7, "Graduate/post-graduate degree")
    MISSING = (995, "Missing Response")
    PNTA = (999, "Prefer not to answer")

class Employment(LabeledEnum):
    """employment value labels."""

    canonical_field_name = "employment"

    EMPLOYED_FULLTIME = (1, "Employed full-time")
    EMPLOYED_PARTTIME = (2, "Employed part-time")
    EMPLOYED_SELF = (3, "Self-employed")
    EMPLOYED_NOTWORKING = (4, "Employed but not currently working (e.g., on leave, furloughed)")  # noqa: E501
    UNEMPLOYED_LOOKING = (5, "Unemployed and looking for work")
    UNEMPLOYED_NOT_LOOKING = (6, "Not employed and not looking for work (e.g., full-time parent, full-time student, or retired)")  # noqa: E501
    # NOTE This should include some number of hours per week
    EMPLOYED_UNPAID = (8, "Unpaid volunteer or intern")
    MISSING = (995, "Missing Response")
    # NOTE: This should be broken out into multiple categories if possible
    # UNEMPLOYED_PARENT = (6, "Not employed and not looking, full-time parent")  # noqa: E501, ERA001
    # UNEMPLOYED_STUDENT = (7, "Not employed and not looking, enrolled as full-time student")  # noqa: E501, ERA001
    # UNEMPLOYED_RETIRED = (8, "Not employed and not working, retired")  # noqa: E501, ERA001

class Ethnicity(LabeledEnum):
    """ethnicity value labels."""

    canonical_field_name = "ethnicity"

    NOT_HISPANIC = (1, "Not Hispanic or Latino")
    MEXICAN = (2, "Mexican, Mexican American, Chicano")
    PUERTO_RICAN = (3, "Puerto Rican")
    CUBAN = (4, "Cuban")
    OTHER = (5, "Other Hispanic or Latino")
    MISSING = (995, "Missing Response")
    PNTA = (999, "Prefer not to answer")

class Gender(LabeledEnum):
    """gender value labels."""

    canonical_field_name = "gender"

    FEMALE = (1, "Female")
    MALE = (2, "Male")
    NON_BINARY = (4, "Non-binary")
    MISSING = (995, "Missing Response")
    OTHER = (997, "Other/prefer to self-describe")
    PNTA = (999, "Prefer not to answer")

class Industry(LabeledEnum):
    """industry value labels."""

    canonical_field_name = "industry"

    AGRICULTURE = (1, "Agriculture, Forestry, Fishing, and Hunting")
    MINING = (2, "Mining, Quarrying, and Oil and Gas Extraction")
    UTILITIES = (3, "Utilities")
    CONSTRUCTION = (4, "Construction")
    MANUFACTURING = (5, "Manufacturing")
    WHOLESALE_TRADE = (6, "Wholesale Trade")
    RETAIL_TRADE = (7, "Retail Trade")
    TRANSPORTATION = (8, "Transportation and Warehousing")
    INFORMATION = (9, "Information")
    FINANCE_AND_INSURANCE = (10, "Finance and Insurance")
    REALESTATE = (11, "Real Estate and Rental and Leasing")
    PROFESSIONAL = (12, "Professional, Scientific, and Technical Services")
    MANAGEMENT = (13, "Management of Companies and Enteprises")
    ADMINISTRATIVE = (14, "Administrative and Support and Waste Management and Remediation Services")  # noqa: E501
    EDUCATIONAL = (15, "Educational Services")
    HEALTH_AND_SOCIAL = (16, "Health Care and Social Assistance")
    ARTS_AND_RECREATION = (17, "Arts, Entertainment, and Recreation")
    ACCOMMODATION = (18, "Accommodation and Food Services")
    OTHER = (19, "Other Services (except Public Administration)")
    PUBLIC_ADMINISTRATION = (20, "Public Administration")
    MISSING = (995, "Missing Response")
    OTHER_SPECIFY = (997, "Other, please specify")

class JobCommuteType(LabeledEnum):
    """job_commute_type value labels."""

    canonical_field_name = "job_commute_type"

    FIXED = (1, "Go to one work location ONLY (outside of home)")
    VARIES = (2, "Work location regularly varies (different offices/jobsites)")
    WFH = (3, "Work ONLY from home or remotely (telework, self-employed)")
    DELIVERY = (4, "Drive/bike/travel for work (driver, sales, deliveries)")
    HYBRID = (5, "Work remotely some days and travel to a work location some days")  # noqa: E501
    MISSING = (995, "Missing Response")


class NumJobs(LabeledEnum):
    """num_jobs value labels."""

    canonical_field_name = "num_jobs"

    VALUE_1_JOB = (1, "1 job")
    VALUE_2_JOBS = (2, "2 jobs")
    VALUE_3_JOBS = (3, "3 jobs")
    VALUE_4_JOBS = (4, "4 jobs")
    VALUE_5_JOBS = (5, "5 jobs")
    VALUE_6_OR_MORE_JOBS = (6, "6 or more jobs")
    MISSING = (995, "Missing Response")

class Occupation(LabeledEnum):
    """occupation value labels."""

    canonical_field_name = "occupation"

    MANAGEMENT = (1, "Management")
    BUSINESS_FINANCE = (2, "Business and Financial Operations")
    COMPUTER_MATH = (3, "Computer and Mathematical")
    ARCH_ENG = (4, "Architecture and Engineering")
    SCIENCE = (5, "Life, Physical, and Social Science")
    COMMUNITY_SOCIAL = (6, "Community and Social Service")
    LEGAL = (7, "Legal")
    EDUCATION = (8, "Educational Instruction and Library")
    ARTS_MEDIA = (9, "Arts, Design, Entertainment, Sports, and Media")
    HEALTHCARE_PROFESSIONAL = (10, "Healthcare Practitioners and Technical")
    HEALTHCARE_SUPPORT = (11, "Healthcare Support")
    PROTECTIVE = (12, "Protective Service")
    FOOD_SERVICE = (13, "Food Preparation and Serving Related")
    CLEANING_MAINTENANCE = (14, "Building and Grounds Cleaning and Maintenance")
    PERSONAL_CARE = (15, "Personal Care and Service")
    SALES = (16, "Sales and Related")
    OFFICE_ADMIN = (17, "Office and Administrative Support")
    FARMING_FISHING = (18, "Farming, Fishing, and Forestry")
    CONSTRUCTION = (19, "Construction and Extraction")
    INSTALLATION_REPAIR = (20, "Installation, Maintenance, and Repair")
    PRODUCTION = (21, "Production")
    TRANSPORTATION = (22, "Transportation and Material Moving")
    MILITARY = (23, "Military Specific")
    MISSING = (995, "Missing Response")
    OTHER_PLEASE_SPECIFY = (997, "Other, please specify")

class Race(LabeledEnum):
    """race value labels."""

    canonical_field_name = "race"
    field_description = "Grouped race for the respondent"

    AFAM = (1, "African American or Black")
    NATIVE = (2, "American Indian or Alaska Native")
    ASIAN = (3, "Asian")
    PACIFIC = (4, "Native Hawaiian or Other Pacific Islander")
    WHITE = (5, "White")
    OTHER = (6, "Some other race")
    MULTI = (7, "Multiple races")
    MISSING = (995, "Missing Response")
    PNTA = (999, "Prefer not to answer")

# Individual race fields to capture multiple selections
class RaceAfam(LabeledEnum):
    """race_afam value labels."""

    canonical_field_name = "race_afam"
    field_description = "Indicates whether the person identifies as African American or Black"  # noqa: E501

    NOT_SELECTED = (0, "Not selected")
    SELECTED = (1, "Selected")
    MISSING = (995, "Missing Response")

class RaceNative(LabeledEnum):
    """race_native value labels."""

    canonical_field_name = "race_native"
    field_description = "Indicates whether the person identifies as American Indian or Alaska Native"  # noqa: E501

    NOT_SELECTED = (0, "Not selected")
    SELECTED = (1, "Selected")
    MISSING = (995, "Missing Response")

class RaceAsian(LabeledEnum):
    """race_asian value labels."""

    canonical_field_name = "race_asian"
    field_description = "Indicates whether the person identifies as Asian"

    NOT_SELECTED = (0, "Not selected")
    SELECTED = (1, "Selected")
    MISSING = (995, "Missing Response")

class RacePacific(LabeledEnum):
    """race_pacific value labels."""

    canonical_field_name = "race_pacific"
    field_description = "Indicates whether the person identifies as Native Hawaiian or Other Pacific Islander"  # noqa: E501

    NOT_SELECTED = (0, "Not selected")
    SELECTED = (1, "Selected")
    MISSING = (995, "Missing Response")

class RaceWhite(LabeledEnum):
    """race_white value labels."""

    canonical_field_name = "race_white"
    field_description = "Indicates whether the person identifies as White"

    NOT_SELECTED = (0, "Not selected")
    SELECTED = (1, "Selected")
    MISSING = (995, "Missing Response")

class RaceOther(LabeledEnum):
    """race_other value labels."""

    canonical_field_name = "race_other"
    field_description = "Indicates whether the person identifies as some other race"  # noqa: E501

    NOT_SELECTED = (0, "Not selected")
    SELECTED = (1, "Selected")
    MISSING = (995, "Missing Response")

class RacePNTA(LabeledEnum):
    """race_pnta value labels."""

    canonical_field_name = "race_pnta"
    field_description = "Indicates whether the person prefers not to answer the race question"  # noqa: E501

    NOT_SELECTED = (0, "Not selected")
    SELECTED = (1, "Selected")
    MISSING = (995, "Missing Response")

class Relationship(LabeledEnum):
    """relationship value labels."""

    canonical_field_name = "relationship"
    field_description = "Indicates the relationship of the person to the primary respondent"  # noqa: E501

    SELF = (0, "Self")
    SPOUSE_PARTNER = (1, "Spouse, partner")
    CHILD = (2, "Child or child-in-law")
    PARENT = (3, "Parent or parent-in-law")
    SIBLING = (4, "Sibling or sibling-in-law")
    OTHER_RELATIVE = (5, "Other relative (grandchild, cousin)")
    NONRELATIVE = (6, "Nonrelative (friend, roommate, household help)")

class RemoteClassFreq(LabeledEnum):
    """remote_class_freq value labels."""

    canonical_field_name = "remote_class_freq"

    REMOTESCHOOL_6_7_DAYS = (1, "6-7 days a week")
    REMOTESCHOOL_5_DAYS = (2, "5 days a week")
    REMOTESCHOOL_4_DAYS = (3, "4 days a week")
    REMOTESCHOOL_3_DAYS = (4, "3 days a week")
    REMOTESCHOOL_2_DAYS = (5, "2 days a week")
    REMOTESCHOOL_1_DAY = (6, "1 day a week")
    REMOTESCHOOL_1_3_PER_MONTH = (7, "1-3 days a month")
    LESS_THAN_MONTHLY = (8, "Less than monthly")
    MISSING = (995, "Missing Response")
    NEVER = (996, "Never")


class ResidenceRentOwn(LabeledEnum):
    """residence_rent_own value labels."""

    canonical_field_name = "residence_rent_own"

    OWN = (1, "Own/buying (paying a mortgage)")
    RENT = (2, "Rent")
    NOPAYMENT_EMPLOYER = (3, "Housing provided by job or military")
    NOPAYMENT_OTHER = (4, "Provided by family or friend without payment or rent")
    MISSING = (995, "Missing Response")
    OTHER = (997, "Other")
    PNTA = (999, "Prefer not to answer")

class ResidenceType(LabeledEnum):
    """residence_type value labels."""

    canonical_field_name = "residence_type"

    DETACHEDSFH = (1, "Single-family house (detached house)")
    TOWNHOUSE = (2, "Single-family house attached to one or more houses (rowhouse or townhouse)")  # noqa: E501
    MULTIFAMILY = (3, "Building with 2-4 units (duplexes, triplexes, quads)")
    CONDO_5TO50_UNITS = (4, "Building with 5-49 apartments/condos")
    CONDO_50PLUS_UNITS = (5, "Building with 50 or more apartments/condos")
    SENIOR = (6, "Senior or age-restricted apartments/condos")
    MANUFACTURED = (7, "Manufactured home/mobile home/trailer")
    GROUP_QUARTERS = (9, "Dorm, group quarters, or institutional housing")
    MISSING = (995, "Missing Response")
    BOAT_RV = (997, "Other (e.g., boat, RV, van)")

class SchoolFreq(LabeledEnum):
    """school_freq value labels."""

    canonical_field_name = "school_freq"

    SCHOOL_6_7_DAYS = (1, "6-7 days a week")
    SCHOOL_5_DAYS = (2, "5 days a week")
    SCHOOL_4_DAYS = (3, "4 days a week")
    SCHOOL_3_DAYS = (4, "3 days a week")
    SCHOOL_2_DAYS = (5, "2 days a week")
    SCHOOL_1_DAY = (6, "1 day a week")
    SCHOOL_1_3_PER_MONTH = (7, "1-3 days a month")
    LESS_THAN_MONTHLY = (8, "Less than monthly")
    MISSING = (995, "Missing Response")
    NEVER = (996, "Never")

class SchoolInRegion(LabeledEnum):
    """school_in_region value labels."""

    canonical_field_name = "school_in_region"

    NO = (0, "No")
    YES = (1, "Yes")
    MISSING = (995, "Missing Response")

class SchoolMode(LabeledEnum):
    """school_mode value labels."""

    canonical_field_name = "school_mode"

    WALK = (1, "Walk (or jog/wheelchair)")
    SCHOOL_BUS = (24, "School bus")
    MEDICAL_TRANS = (27, "Medical transportation service")
    HOUSEHOLD_VEHICLE = (100, "Household vehicle (or motorcycle)")
    OTHER_VEHICLE = (101, "Other vehicle (e.g., friend's car, rental, carshare, work car)")  # noqa: E501
    SHUTTLE = (102, "Bus, shuttle, or vanpool")
    BICYCLE = (103, "Bicycle")
    OTHER = (104, "Other")
    RAIL = (105, "Rail (e.g., train, light rail, trolley, BART, MUNI Metro)")
    TNC = (106, "Uber/Lyft, taxi, car service")
    MICROMOBILITY = (107, "Micromobility (e.g., scooter, moped, skateboard)")
    MISSING = (995, "Missing Response")

class WorkMode(LabeledEnum):
    """work_mode value labels."""

    canonical_field_name = "work_mode"

    WALK = (1, "Walk (or jog/wheelchair)")
    MEDICAL_TRANSPORTATION_SERVICE = (27, "Medical transportation service")
    HOUSEHOLD_VEHICLE_OR_MOTORCYCLE = (100, "Household vehicle (or motorcycle)")
    OTHER_VEHICLE_E_G_FRIENDS_CAR_RENTAL_CARSHARE_WORK_CAR = (101, "Other vehicle (e.g., friend's car, rental, carshare, work car)")
    BUS_SHUTTLE_OR_VANPOOL = (102, "Bus, shuttle, or vanpool")
    BICYCLE = (103, "Bicycle")
    OTHER = (104, "Other")
    RAIL_E_G_TRAIN_LIGHT_RAIL_TROLLEY_BART_MUNI_METRO = (105, "Rail (e.g., train, light rail, trolley, BART, MUNI Metro)")
    UBER_OR_LYFT_TAXI_CAR_SERVICE = (106, "Uber/Lyft, taxi, car service")
    MICROMOBILITY_E_G_SCOOTER_MOPED_SKATEBOARD = (107, "Micromobility (e.g., scooter, moped, skateboard)")
    MISSING = (995, "Missing Response")




class SchoolType(LabeledEnum):
    """school_type value labels."""

    canonical_field_name = "school_type"

    ATHOME = (1, "Cared for at home")
    DAYCARE = (2, "Daycare outside home")
    PRESCHOOL = (3, "Preschool")
    HOME_SCHOOL = (4, "Home school")
    ELEMENTARY = (5, "Elementary school (public, private, charter)")
    MIDDLE_SCHOOL = (6, "Middle school (public, private, charter)")
    HIGH_SCHOOL = (7, "High school (public, private, charter)")
    VOCATIONAL = (10, "Vocational/technical school")
    COLLEGE_2YEAR = (11, "2-year college")
    COLLEGE_4YEAR = (12, "4-year college")
    GRADUATE_SCHOOL = (13, "Graduate or professional school")
    MISSING = (995, "Missing Response")
    OTHER = (997, "Other")

class SecondHome(LabeledEnum):
    """second_home value labels."""

    canonical_field_name = "second_home"
    field_description = "Indicates whether the person regularly spends nights at a second home"  # noqa: E501

    NO = (0, "Does not regularly spend night at second home")
    YES = (1, "Regularly spends night at second home")
    MISSING = (995, "Missing Response")


class SecondHomeInRegion(LabeledEnum):
    """second_home_in_region value labels."""

    canonical_field_name = "second_home_in_region"

    NO = (0, "No")
    YES = (1, "Yes")
    MISSING = (995, "Missing Response")

class Student(LabeledEnum):
    """student value labels."""

    canonical_field_name = "student"

    FULLTIME_INPERSON = (0, "Full-time student, currently attending some or all classes in-person")  # noqa: E501
    PARTTIME_INPERSON = (1, "Part-time student, currently attending some or all classes in-person")  # noqa: E501
    NONSTUDENT = (2, "Not a student")
    PARTTIME_ONLINE = (3, "Part-time student, ONLY online classes")
    FULLTIME_ONLINE = (4, "Full-time student, ONLY online classes")
    MISSING = (995, "Missing Response")

class TeleworkFreq(LabeledEnum):
    """telework_freq value labels."""

    canonical_field_name = "telework_freq"

    WFH_6_7_DAYS = (1, "6-7 days a week")
    WFH_5_DAYS = (2, "5 days a week")
    WFH_4_DAYS = (3, "4 days a week")
    WFH_3_DAYS = (4, "3 days a week")
    WFH_2_DAYS = (5, "2 days a week")
    WFH_1_DAY = (6, "1 day a week")
    WFH_1_3_DAYS_A_MONTH = (7, "1-3 days a month")
    LESS_THAN_MONTHLY = (8, "Less than monthly")
    MISSING = (995, "Missing Response")
    NEVER = (996, "Never")

class Vehicle(LabeledEnum):
    """vehicle value labels."""

    canonical_field_name = "vehicle"
    field_description = "Indicates the vehicle the person primarily drives"

    HOUSEHOLD_VEHICLE_1 = (6, "Household vehicle 1")
    HOUSEHOLD_VEHICLE_2 = (7, "Household vehicle 2")
    HOUSEHOLD_VEHICLE_3 = (8, "Household vehicle 3")
    HOUSEHOLD_VEHICLE_4 = (9, "Household vehicle 4")
    HOUSEHOLD_VEHICLE_5 = (10, "Household vehicle 5")
    HOUSEHOLD_VEHICLE_6 = (11, "Household vehicle 6")
    HOUSEHOLD_VEHICLE_7 = (12, "Household vehicle 7")
    CARSHARE = (18, "A carshare vehicle (e.g., ZipCar)")
    MISSING = (995, "Missing Response")
    NONE = (996, "None (I do not drive a vehicle)")
    OTHER_VEHICLE = (997, "Other vehicle")
