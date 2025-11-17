"""Codebook enumerations for trip table."""

from data_canon.labeled_enum import LabeledEnum


class Purpose(LabeledEnum):
    """Base class for purpose value labels."""

    HOME = (1, "Went home")
    WORK_VOLUNTEER = (2, "Went to work, work-related, volunteer-related")
    SCHOOL = (3, "Attended school/class")
    SHOPPING_ERRANDS = (4, "Appointment, shopping, or errands (e.g., gas)")
    ESCORT = (5, "Dropped off, picked up, or accompanied another person")
    SOCIAL_LEISURE = (7, "Social, leisure, religious, entertainment activity")
    PRIMARY_WORKPLACE = (10, "Went to primary workplace")
    WORK_ACTIVITY = (11, "Went to work-related activity (e.g., meeting, delivery, worksite)")  # noqa: E501
    VOLUNTEERING = (13, "Volunteering")
    OTHER_WORK = (14, "Other work-related")
    K12_SCHOOL = (21, "Attend K-12 school")
    COLLEGE = (22, "Attend college/university")
    OTHER_CLASS = (23, "Attend other type of class (e.g., cooking class)")
    OTHER_EDUCATION = (24, "Attend other education-related activity (e.g., field trip)")  # noqa: E501
    VOCATIONAL = (25, "Attend vocational education class")
    DAYCARE = (26, "Attend daycare or preschool")
    GROCERY = (30, "Grocery shopping")
    GAS = (31, "Got gas")
    ROUTINE_SHOPPING = (32, "Other routine shopping (e.g., pharmacy)")
    ERRAND_NO_APPT = (33, "Errand without appointment (e.g., post office)")
    MEDICAL = (34, "Medical visit (e.g., doctor, dentist)")
    MAJOR_SHOPPING = (36, "Shopping for major item (e.g., furniture, car)")
    ERRAND_WITH_APPT = (37, "Errand with appointment (e.g., haircut)")
    OTHER_ACTIVITY = (44, "Other activity only (e.g., attend meeting, pick-up or drop-off item)")  # noqa: E501
    PICK_UP = (45, "Pick someone up")
    DROP_OFF = (46, "Drop someone off")
    ACCOMPANY = (47, "Accompany someone only (e.g., go along for the ride)")
    PICK_UP_AND_DROP_OFF = (48, "BOTH pick up AND drop off")
    DINING = (50, "Dined out, got coffee, or take-out")
    EXERCISE = (51, "Exercise or recreation (e.g., gym, jog, bike, walk dog)")
    SOCIAL = (52, "Social activity (e.g., visit friends/relatives)")
    ENTERTAINMENT = (53, "Leisure/entertainment/cultural (e.g., cinema, museum, park)")  # noqa: E501
    RELIGIOUS_CIVIC = (54, "Religious/civic/volunteer activity")
    FAMILY_ACTIVITY = (56, "Family activity (e.g., watch child's game)")
    MODE_CHANGE = (60, "Changed or transferred mode (e.g., waited for bus or exited bus)")  # noqa: E501
    OTHER_ERRAND = (61, "Other errand")
    OTHER_SOCIAL = (62, "Other social")
    OTHER = (99, "Other reason")
    OTHER_RESIDENCE = (150, "Went to another residence (e.g., someone else's home, second home)")  # noqa: E501
    TEMP_LODGING = (152, "Went to temporary lodging (e.g., hotel, vacation rental)")  # noqa: E501
    MISSING = (995, "Missing Response")
    PNTA = (999, "Prefer not to answer")
    NOT_IMPUTABLE = (996, "Not imputable")

class PurposeCategory(LabeledEnum):
    """d_purpose_category value labels."""

    HOME = (1, "Home")
    WORK = (2, "Work")
    WORK_RELATED = (3, "Work related")
    SCHOOL = (4, "School")
    SCHOOL_RELATED = (5, "School related")
    ESCORT = (6, "Escort")
    SHOP = (7, "Shop")
    MEAL = (8, "Meal")
    SOCIAL_OR_RECREATIONAL = (9, "Social or recreational")
    ERRAND = (10, "Errand")
    CHANGE_MODE = (11, "Change mode")
    OVERNIGHT = (12, "Overnight")
    OTHER = (13, "Other")
    NOT_IMPUTABLE = (996, "Not imputable")


class PurposeToCategoryMap:
    """Mapping from detailed purpose codes to purpose categories."""
    # Need to populate this...


class Driver(LabeledEnum):
    """driver value labels."""

    DRIVER = (1, "Driver")
    PASSENGER = (2, "Passenger")
    BOTH = (3, "Both (switched drivers during trip)")
    MISSING = (995, "Missing Response")


class Mode(LabeledEnum):
    """mode value labels."""

    WALK_OR_JOG_OR_WHEELCHAIR = (1, "Walk/jog/wheelchair")
    STANDARD_BICYCLE_MY_HOUSEHOLDS = (2, "Standard bicycle (household)")
    BORROWED_BICYCLE = (3, "Borrowed bicycle")
    OTHER_RENTED_BICYCLE = (4, "Other rented bicycle")
    OTHER = (5, "Other")
    HOUSEHOLD_VEHICLE_1 = (6, "Household vehicle 1")
    HOUSEHOLD_VEHICLE_2 = (7, "Household vehicle 2")
    HOUSEHOLD_VEHICLE_3 = (8, "Household vehicle 3")
    HOUSEHOLD_VEHICLE_4 = (9, "Household vehicle 4")
    HOUSEHOLD_VEHICLE_5 = (10, "Household vehicle 5")
    HOUSEHOLD_VEHICLE_6 = (11, "Household vehicle 6")
    HOUSEHOLD_VEHICLE_7 = (12, "Household vehicle 7")
    HOUSEHOLD_VEHICLE_8 = (13, "Household vehicle 8")
    HOUSEHOLD_VEHICLE_9 = (14, "Household vehicle 9")
    HOUSEHOLD_VEHICLE_10 = (15, "Household vehicle 10")
    OTHER_VEHICLE_IN_HOUSEHOLD = (16, "Other vehicle (household)")
    RENTAL_CAR = (17, "Rental car")
    CARSHARE_SERVICE = (18, "Carshare (Zipcar, etc.)")
    VANPOOL = (21, "Vanpool")
    OTHER_VEHICLE_NOT_MY_HOUSEHOLDS = (22, "Other vehicle (non-household)")
    LOCAL_PUBLIC_BUS = (23, "Local public bus")
    SCHOOL_BUS = (24, "School bus")
    INTERCITY_BUS = (25, "Intercity bus (Greyhound, etc.)")
    OTHER_PRIVATE_SHUTTLE_OR_BUS = (26, "Private shuttle/bus")
    PARATRANSIT_OR_DIAL_A_RIDE = (27, "Paratransit/Dial-A-Ride")
    OTHER_BUS = (28, "Other bus")
    BART = (30, "BART")
    AIRPLANE_OR_HELICOPTER = (31, "Airplane/helicopter")
    CAR_FROM_WORK = (33, "Work car")
    FRIEND_OR_RELATIVE_OR_COLLEAGUES_CAR = (34, "Friend/relative/colleague car")
    REGULAR_TAXI = (36, "Regular taxi")
    UNIVERSITY_OR_COLLEGE_SHUTTLE_OR_BUS = (38, "University/college shuttle")
    INTERCITY_OR_COMMUTER_RAIL = (41, "Intercity/commuter rail (ACE, Amtrak, Caltrain)")  # noqa: E501
    OTHER_RAIL = (42, "Other rail")
    SKATEBOARD_OR_ROLLERBLADE = (43, "Skateboard/rollerblade")
    GOLF_CART = (44, "Golf cart")
    ATV = (45, "ATV")
    OTHER_MOTORCYCLE_IN_HOUSEHOLD = (47, "Motorcycle (household)")
    UBER_LYFT_OR_OTHER_SMARTPHONE_APP_RIDE_SERVICE = (49, "Rideshare (Uber, Lyft, etc.)")  # noqa: E501
    MUNI_METRO = (53, "MUNI Metro")
    OTHER_MOTORCYCLE_NOT_MY_HOUSEHOLDS = (54, "Motorcycle (non-household)")
    EXPRESS_BUS_OR_TRANSBAY_BUS = (55, "Express/Transbay bus")
    PEER_TO_PEER_CAR_RENTAL = (59, "Peer-to-peer rental (Turo, etc.)")
    OTHER_HIRED_CAR_SERVICE = (60, "Hired car (black car, limo)")
    RAPID_TRANSIT_BUS_BRT = (61, "Rapid transit bus (BRT)")
    EMPLOYER_PROVIDED_SHUTTLE_OR_BUS = (62, "Employer shuttle/bus")
    MEDICAL_TRANSPORTATION_SERVICE = (63, "Medical transportation")
    LOCAL_PRIVATE_BUS = (67, "Local private bus")
    CABLE_CAR_OR_STREETCAR = (68, "Cable car/streetcar")
    BIKE_SHARE_STANDARD_BICYCLE = (69, "Bike-share (standard)")
    BIKE_SHARE_ELECTRIC_BICYCLE = (70, "Bike-share (electric)")
    MOPED_SHARE = (73, "Moped-share (Scoot, etc.)")
    SEGWAY = (74, "Segway")
    OTHER_75 = (75, "Other")
    CARPOOL_MATCH = (76, "Carpool match (Waze, etc.)")
    PERSONAL_SCOOTER_OR_MOPED_NOT_SHARED = (77, "Personal scooter/moped")
    PUBLIC_FERRY_OR_WATER_TAXI = (78, "Ferry/water taxi")
    OTHER_BOAT = (80, "Other boat (kayak, etc.)")
    ELECTRIC_BICYCLE_MY_HOUSEHOLDS = (82, "Electric bicycle (household)")
    SCOOTER_SHARE = (83, "Scooter-share (Bird, Lime, etc.)")
    HOUSEHOLD_VEHICLE_OR_MOTORCYCLE = (100, "Household vehicle/motorcycle")
    OTHER_VEHICLE = (101, "Other vehicle (rental, carshare, etc.)")
    BUS_SHUTTLE_OR_VANPOOL = (102, "Bus/shuttle/vanpool")
    BICYCLE = (103, "Bicycle")
    OTHER_104 = (104, "Other")
    RAIL = (105, "Rail (train, BART, MUNI, etc.)")
    UBER_OR_LYFT_TAXI_OR_CAR_SERVICE = (106, "Uber/Lyft/taxi/car service")
    MICROMOBILITY = (107, "Micromobility (scooter, moped, etc.)")
    MISSING = (995, "Missing Response")


class ModeType(LabeledEnum):
    """mode_type value labels."""

    WALK = (1, "Walk")
    BIKE = (2, "Bike")
    BIKESHARE = (3, "Bikeshare")
    SCOOTERSHARE = (4, "Scootershare")
    TAXI = (5, "Taxi")
    TNC = (6, "TNC")
    OTHER = (7, "Other")
    CAR = (8, "Car")
    CARSHARE = (9, "Carshare")
    SCHOOL_BUS = (10, "School bus")
    SHUTTLE_OR_VANPOOL = (11, "Shuttle/vanpool")
    FERRY = (12, "Ferry")
    TRANSIT = (13, "Transit")
    LONG_DISTANCE_PASSENGER = (14, "Long distance passenger")
    MISSING = (995, "Missing Response")

class ModeTypeMap:
    """Mapping from detailed mode codes to mode types."""
    # Need to populate this...


class AccessEgressMode(LabeledEnum):
    """transit_access value labels."""

    WALKED_OR_JOGGED_OR_WHEELCHAIR = (1, "Walked (or jogged/wheelchair)")
    BICYCLE = (2, "Bicycle")
    TRANSFERRED_FROM_ANOTHER_BUS = (3, "Transferred from another bus")
    MICROMOBILITY_E_G_SCOOTER_MOPED_SKATEBOARD = (4, "Micromobility (e.g., scooter, moped, skateboard)")  # noqa: E501
    TRANSFERRED_FROM_OTHER_TRANSIT_E_G_RAIL_AIR = (5, "Transferred from other transit (e.g., rail, air)")  # noqa: E501
    UBER_OR_LYFT_TAXI_OR_CAR_SERVICE = (6, "Uber/Lyft, taxi, or car service")
    DROVE_AND_PARKED_MY_OWN_HOUSEHOLDS_VEHICLE_OR_MOTORCYCLE = (7, "Drove and parked my own household's vehicle (or motorcycle)")  # noqa: E501
    DROVE_AND_PARKED_ANOTHER_VEHICLE_OR_MOTORCYCLE = (8, "Drove and parked another vehicle (or motorcycle)")  # noqa: E501
    GOT_DROPPED_OFF_IN_MY_OWN_HOUSEHOLDS_VEHICLE_OR_MOTORCYCLE = (9, "Got dropped off in my own household's vehicle (or motorcycle)")  # noqa: E501
    GOT_DROPPED_OFF_IN_ANOTHER_VEHICLE_OR_MOTORCYCLE = (10, "Got dropped off in another vehicle (or motorcycle)")  # noqa: E501
    MISSING = (995, "Missing Response")
    OTHER = (997, "Other")
