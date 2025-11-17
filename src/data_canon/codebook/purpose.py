"""Codebook enumerations for trip purpose codes in trip table."""

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
    WORK_ACTIVITY = (11, "Went to work-related activity (e.g., meeting, delivery, worksite)")
    VOLUNTEERING = (13, "Volunteering")
    OTHER_WORK = (14, "Other work-related")
    K12_SCHOOL = (21, "Attend K-12 school")
    COLLEGE = (22, "Attend college/university")
    OTHER_CLASS = (23, "Attend other type of class (e.g., cooking class)")
    OTHER_EDUCATION = (24, "Attend other education-related activity (e.g., field trip)")
    VOCATIONAL = (25, "Attend vocational education class")
    DAYCARE = (26, "Attend daycare or preschool")
    GROCERY = (30, "Grocery shopping")
    GAS = (31, "Got gas")
    ROUTINE_SHOPPING = (32, "Other routine shopping (e.g., pharmacy)")
    ERRAND_NO_APPT = (33, "Errand without appointment (e.g., post office)")
    MEDICAL = (34, "Medical visit (e.g., doctor, dentist)")
    MAJOR_SHOPPING = (36, "Shopping for major item (e.g., furniture, car)")
    ERRAND_WITH_APPT = (37, "Errand with appointment (e.g., haircut)")
    OTHER_ACTIVITY = (44, "Other activity only (e.g., attend meeting, pick-up or drop-off item)")
    PICK_UP = (45, "Pick someone up")
    DROP_OFF = (46, "Drop someone off")
    ACCOMPANY = (47, "Accompany someone only (e.g., go along for the ride)")
    PICK_UP_AND_DROP_OFF = (48, "BOTH pick up AND drop off")
    DINING = (50, "Dined out, got coffee, or take-out")
    EXERCISE = (51, "Exercise or recreation (e.g., gym, jog, bike, walk dog)")
    SOCIAL = (52, "Social activity (e.g., visit friends/relatives)")
    ENTERTAINMENT = (53, "Leisure/entertainment/cultural (e.g., cinema, museum, park)")
    RELIGIOUS_CIVIC = (54, "Religious/civic/volunteer activity")
    FAMILY_ACTIVITY = (56, "Family activity (e.g., watch child's game)")
    MODE_CHANGE = (60, "Changed or transferred mode (e.g., waited for bus or exited bus)")
    OTHER_ERRAND = (61, "Other errand")
    OTHER_SOCIAL = (62, "Other social")
    OTHER = (99, "Other reason")
    OTHER_RESIDENCE = (150, "Went to another residence (e.g., someone else's home, second home)")
    TEMP_LODGING = (152, "Went to temporary lodging (e.g., hotel, vacation rental)")
    MISSING = (995, "Missing Response")
    PNTA = (999, "Prefer not to answer")
    NOT_IMPUTABLE = (996, "Not imputable")

class PurposeCategory(LabeledEnum):
    """d_purpose_category value labels."""

    canonical_field_name = "d_purpose_category"

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
