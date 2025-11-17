"""Codebook enumerations for trip table."""

from data_canon.labeled_enum import LabeledEnum

from .days import TravelDow
from .mode import Mode, ModeType
from .purpose import Purpose, PurposeCategory


class ArriveDow(TravelDow):
    """arrive_dow value labels."""

    canonical_field_name = "arrive_dow"

class DepartDow(TravelDow):
    """depart_dow value labels."""

    canonical_field_name = "depart_dow"


class DPurpose(Purpose):
    """d_purpose value labels."""

    canonical_field_name = "d_purpose"

class DPurposeCategory(PurposeCategory):
    """d_purpose_category value labels."""

    canonical_field_name = "d_purpose_category"

class Driver(LabeledEnum):
    """driver value labels."""

    canonical_field_name = "driver"

    DRIVER = (1, "Driver")
    PASSENGER = (2, "Passenger")
    BOTH_SWITCHED_DRIVERS_DURING_TRIP = (3, "Both (switched drivers during trip)")
    MISSING = (995, "Missing Response")

class HhMember1(LabeledEnum):
    """hh_member_1 value labels."""

    canonical_field_name = "hh_member_1"

    NO = (0, "No")
    YES = (1, "Yes")

class HhMember2(LabeledEnum):
    """hh_member_2 value labels."""

    canonical_field_name = "hh_member_2"

    NO = (0, "No")
    YES = (1, "Yes")
    MISSING = (995, "Missing Response")

class HhMember3(LabeledEnum):
    """hh_member_3 value labels."""

    canonical_field_name = "hh_member_3"

    NO = (0, "No")
    YES = (1, "Yes")
    MISSING = (995, "Missing Response")

class HhMember4(LabeledEnum):
    """hh_member_4 value labels."""

    canonical_field_name = "hh_member_4"

    NO = (0, "No")
    YES = (1, "Yes")
    MISSING = (995, "Missing Response")

class HhMember5(LabeledEnum):
    """hh_member_5 value labels."""

    canonical_field_name = "hh_member_5"

    NO = (0, "No")
    YES = (1, "Yes")
    MISSING = (995, "Missing Response")

class HhMember6(LabeledEnum):
    """hh_member_6 value labels."""

    canonical_field_name = "hh_member_6"

    NO = (0, "No")
    YES = (1, "Yes")
    MISSING = (995, "Missing Response")

class HhMember7(LabeledEnum):
    """hh_member_7 value labels."""

    canonical_field_name = "hh_member_7"

    NO = (0, "No")
    YES = (1, "Yes")
    MISSING = (995, "Missing Response")

class HhMember8(LabeledEnum):
    """hh_member_8 value labels."""

    canonical_field_name = "hh_member_8"

    NO = (0, "No")
    YES = (1, "Yes")
    MISSING = (995, "Missing Response")

class IsAccess(LabeledEnum):
    """is_access value labels."""

    canonical_field_name = "is_access"

    IS_NOT_ACCESS_LEG = (0, "Is not access leg")
    IS_ACCESS_LEG = (1, "Is access leg")
    MISSING = (995, "Missing Response")

class IsEgress(LabeledEnum):
    """is_egress value labels."""

    canonical_field_name = "is_egress"

    IS_EGRESS_LEG = (0, "Is egress leg")
    IS_NOT_EGRESS_LEG = (1, "Is not egress leg")
    MISSING = (995, "Missing Response")

class IsTransit(LabeledEnum):
    """is_transit value labels."""

    canonical_field_name = "is_transit"

    IS_TRANSIT_LEG = (0, "Is transit leg")
    IS_NOT_TRANSIT_LEG = (1, "Is not transit leg")
    MISSING = (995, "Missing Response")

class ManagedLaneUse(LabeledEnum):
    """managed_lane_use value labels."""

    canonical_field_name = "managed_lane_use"

    YES = (1, "Yes")
    NO = (2, "No")
    MISSING = (995, "Missing Response")

class Mode1(Mode):
    """mode_1 value labels."""

    canonical_field_name = "mode_1"

class Mode2(Mode):
    """mode_2 value labels."""

    canonical_field_name = "mode_2"

class Mode3(Mode):
    """mode_3 value labels."""

    canonical_field_name = "mode_3"

class Mode4(Mode):
    """mode_4 value labels."""

    canonical_field_name = "mode_4"

class ModeType(ModeType):
    """mode_type value labels."""

    canonical_field_name = "mode_type"


class NumHhTravelers(LabeledEnum):
    """num_hh_travelers value labels."""

    canonical_field_name = "num_hh_travelers"

    VALUE_1_TRAVELER = (1, "1 traveler")
    VALUE_2_TRAVELERS = (2, "2 travelers")
    VALUE_3_TRAVELERS = (3, "3 travelers")
    VALUE_4_TRAVELERS = (4, "4 travelers")
    VALUE_5_TRAVELERS = (5, "5 travelers")
    VALUE_6_TRAVELERS = (6, "6 travelers")
    VALUE_7_TRAVELERS = (7, "7 travelers")
    VALUE_8_TRAVELERS = (8, "8 travelers")
    VALUE_9_TRAVELERS = (9, "9 travelers")
    VALUE_10_TRAVELERS = (10, "10 travelers")
    VALUE_11_TRAVELERS = (11, "11 travelers")
    VALUE_12_TRAVELERS = (12, "12 travelers")
    VALUE_13_OR_MORE_TRAVELERS = (13, "13 or more travelers")
    MISSING = (995, "Missing Response")

class NumNonHhTravelers(LabeledEnum):
    """num_non_hh_travelers value labels."""

    canonical_field_name = "num_non_hh_travelers"

    VALUE_0_TRAVELERS = (0, "0 travelers")
    VALUE_1_TRAVELER = (1, "1 traveler")
    VALUE_2_TRAVELERS = (2, "2 travelers")
    VALUE_3_TRAVELERS = (3, "3 travelers")
    VALUE_4_TRAVELERS = (4, "4 travelers")
    VALUE_5_PLUS_TRAVELERS = (5, "5+ travelers")
    MISSING = (995, "Missing Response")

class NumTravelers(LabeledEnum):
    """num_travelers value labels."""

    canonical_field_name = "num_travelers"

    VALUE_0 = (0, "0")
    VALUE_1_TRAVELER = (1, "1 traveler")
    VALUE_2_TRAVELERS = (2, "2 travelers")
    VALUE_3_TRAVELERS = (3, "3 travelers")
    VALUE_4_TRAVELERS = (4, "4 travelers")
    VALUE_5_PLUS_TRAVELERS = (5, "5+ travelers")
    MISSING = (995, "Missing Response")

class OPurpose(Purpose):
    """o_purpose value labels."""

    canonical_field_name = "o_purpose"

class OPurposeCategory(PurposeCategory):
    """o_purpose_category value labels."""

    canonical_field_name = "o_purpose_category"

class ParkLocation(LabeledEnum):
    """park_location value labels."""

    canonical_field_name = "park_location"

    HOME_DRIVEWAY_OR_GARAGE_YOURS_OR_SOMEONE_ELSES = (1, "Home driveway/garage (yours or someone else's)")
    PARKING_LOT_OR_GARAGE = (3, "Parking lot/garage")
    ON_STREET_PARKING = (4, "On-street parking")
    PARK_AND_RIDE_LOT = (5, "Park & Ride lot")
    DIDNT_PARK_WAITED_DROP_OFF_DRIVE_THRU_GAS = (6, "Didn't park (waited, drop-off, drive-thru, gas)")
    OTHER = (7, "Other")
    MISSING = (995, "Missing Response")

class ParkType(LabeledEnum):
    """park_type value labels."""

    canonical_field_name = "park_type"

    FREE_PARKING_NO_COST_AT_ALL = (1, "Free parking (no cost at all)")
    USED_A_PARKING_PASS_ANY_TYPE = (2, "Used a parking pass (any type)")
    PAID_VIA_CASH_CREDIT_CARD_OR_TICKETS = (3, "Paid via cash, credit card, or ticket(s)")
    PARKING_RESERVATION_SERVICE_E_G_SPOTHERO_PARKMOBILE = (4, "Parking reservation service (e.g., SpotHero, ParkMobile)")
    OTHER = (6, "Other")
    MISSING = (995, "Missing Response")

class SpeedFlag(LabeledEnum):
    """speed_flag value labels."""

    canonical_field_name = "speed_flag"

    NO = (0, "No")
    YES = (1, "Yes")

class TaxiPay(LabeledEnum):
    """taxi_pay value labels."""

    canonical_field_name = "taxi_pay"

    KNOWS_AMOUNT_PAID = (1, "Knows amount paid")
    DONT_KNOW = (2, "Don't know")
    MISSING = (995, "Missing Response")

class TaxiType(LabeledEnum):
    """taxi_type value labels."""

    canonical_field_name = "taxi_type"

    I_PAID_THE_FARE_MYSELF_NO_REIMBURSEMENT = (1, "I paid the fare myself (no reimbursement)")
    EMPLOYER_PAID_I_AM_REIMBURSED = (2, "Employer paid (I am reimbursed)")
    SPLIT_OR_SHARED_FARE_WITH_OTHERS = (3, "Split/shared fare with other(s)")
    SOMEONE_ELSE_PAID_100_PERCENT_ALL_OF_FARE = (4, "Someone else paid 100% (all of fare)")
    OTHER = (5, "Other")
    MISSING = (995, "Missing Response")

class TncReplace1(LabeledEnum):
    """tnc_replace_1 value labels."""

    canonical_field_name = "tnc_replace_1"

    NOT_SELECTED = (0, "Not selected")
    SELECTED = (1, "Selected")
    MISSING = (995, "Missing Response")

class TncReplace2(LabeledEnum):
    """tnc_replace_2 value labels."""

    canonical_field_name = "tnc_replace_2"

    NOT_SELECTED = (0, "Not selected")
    SELECTED = (1, "Selected")
    MISSING = (995, "Missing Response")

class TncReplace3(LabeledEnum):
    """tnc_replace_3 value labels."""

    canonical_field_name = "tnc_replace_3"

    NOT_SELECTED = (0, "Not selected")
    SELECTED = (1, "Selected")
    MISSING = (995, "Missing Response")

class TncReplace4(LabeledEnum):
    """tnc_replace_4 value labels."""

    canonical_field_name = "tnc_replace_4"

    NOT_SELECTED = (0, "Not selected")
    SELECTED = (1, "Selected")
    MISSING = (995, "Missing Response")

class TncReplace5(LabeledEnum):
    """tnc_replace_5 value labels."""

    canonical_field_name = "tnc_replace_5"

    NOT_SELECTED = (0, "Not selected")
    SELECTED = (1, "Selected")
    MISSING = (995, "Missing Response")

class TncReplace6(LabeledEnum):
    """tnc_replace_6 value labels."""

    canonical_field_name = "tnc_replace_6"

    NOT_SELECTED = (0, "Not selected")
    SELECTED = (1, "Selected")
    MISSING = (995, "Missing Response")

class TncReplace7(LabeledEnum):
    """tnc_replace_7 value labels."""

    canonical_field_name = "tnc_replace_7"

    NOT_SELECTED = (0, "Not selected")
    SELECTED = (1, "Selected")
    MISSING = (995, "Missing Response")

class TncReplace8(LabeledEnum):
    """tnc_replace_8 value labels."""

    canonical_field_name = "tnc_replace_8"

    NOT_SELECTED = (0, "Not selected")
    SELECTED = (1, "Selected")
    MISSING = (995, "Missing Response")

class TncReplace9(LabeledEnum):
    """tnc_replace_9 value labels."""

    canonical_field_name = "tnc_replace_9"

    NOT_SELECTED = (0, "Not selected")
    SELECTED = (1, "Selected")
    MISSING = (995, "Missing Response")

class TncType(LabeledEnum):
    """tnc_type value labels."""

    canonical_field_name = "tnc_type"

    POOLED_E_G_UBERPOOL_LYFT_SHARED = (1, "Pooled (e.g., UberPool, Lyft Shared)")
    REGULAR_E_G_UBERX_UBERXL_LYFT_LYFTXL = (2, "Regular (e.g., UberX, UberXL, Lyft, LyftXL)")
    PREMIUM_E_G_UBERBLACK_LYFT_LUX = (3, "Premium (e.g., UberBlack, Lyft Lux)")
    MISSING = (995, "Missing Response")
    DONT_KNOW = (998, "Don't know")

class TncWait(LabeledEnum):
    """tnc_wait value labels."""

    canonical_field_name = "tnc_wait"

    MINUTES = (1, "Minutes:")
    MISSING = (995, "Missing Response")
    DONT_KNOW = (998, "Don't know")

class TraceQualityFlag(LabeledEnum):
    """trace_quality_flag value labels."""

    canonical_field_name = "trace_quality_flag"

    NO = (0, "No")
    YES = (1, "Yes")

class TransitAccess(LabeledEnum):
    """transit_access value labels."""

    canonical_field_name = "transit_access"

    WALKED_OR_JOGGED_OR_WHEELCHAIR = (1, "Walked (or jogged/wheelchair)")
    BICYCLE = (2, "Bicycle")
    TRANSFERRED_FROM_ANOTHER_BUS = (3, "Transferred from another bus")
    MICROMOBILITY_E_G_SCOOTER_MOPED_SKATEBOARD = (4, "Micromobility (e.g., scooter, moped, skateboard)")
    TRANSFERRED_FROM_OTHER_TRANSIT_E_G_RAIL_AIR = (5, "Transferred from other transit (e.g., rail, air)")
    UBER_OR_LYFT_TAXI_OR_CAR_SERVICE = (6, "Uber/Lyft, taxi, or car service")
    DROVE_AND_PARKED_MY_OWN_HOUSEHOLDS_VEHICLE_OR_MOTORCYCLE = (7, "Drove and parked my own household's vehicle (or motorcycle)")
    DROVE_AND_PARKED_ANOTHER_VEHICLE_OR_MOTORCYCLE = (8, "Drove and parked another vehicle (or motorcycle)")
    GOT_DROPPED_OFF_IN_MY_OWN_HOUSEHOLDS_VEHICLE_OR_MOTORCYCLE = (9, "Got dropped off in my own household's vehicle (or motorcycle)")
    GOT_DROPPED_OFF_IN_ANOTHER_VEHICLE_OR_MOTORCYCLE = (10, "Got dropped off in another vehicle (or motorcycle)")
    MISSING = (995, "Missing Response")
    OTHER = (997, "Other")

class TransitEgress(LabeledEnum):
    """transit_egress value labels."""

    canonical_field_name = "transit_egress"

    WALKED_OR_JOGGED_OR_WHEELCHAIR = (1, "Walked (or jogged/wheelchair)")
    BICYCLE = (2, "Bicycle")
    TRANSFERRED_TO_ANOTHER_BUS = (3, "Transferred to another bus")
    MICROMOBILITY_E_G_SCOOTER_MOPED_SKATEBOARD = (4, "Micromobility (e.g., scooter, moped, skateboard)")
    TRANSFERRED_TO_OTHER_TRANSIT_E_G_RAIL_AIR = (5, "Transferred to other transit (e.g., rail, air)")
    UBER_OR_LYFT_TAXI_OR_CAR_SERVICE = (6, "Uber/Lyft, taxi, or car service")
    DROVE_MY_OWN_HOUSEHOLDS_VEHICLE_OR_MOTORCYCLE = (7, "Drove my own household's vehicle (or motorcycle)")
    DROVE_ANOTHER_VEHICLE_OR_MOTORCYCLE = (8, "Drove another vehicle (or motorcycle)")
    GOT_PICKED_UP_IN_MY_OWN_HOUSEHOLDS_VEHICLE_OR_MOTORCYCLE = (9, "Got picked up in my own household's vehicle (or motorcycle)")
    GOT_PICKED_UP_IN_ANOTHER_VEHICLE_OR_MOTORCYCLE = (10, "Got picked up in another vehicle (or motorcycle)")
    MISSING = (995, "Missing Response")
    OTHER = (997, "Other")

class TransitType(LabeledEnum):
    """transit_type value labels."""

    canonical_field_name = "transit_type"

    FREE_NO_COST_AT_ALL = (1, "Free (no cost at all)")
    PASS_LOADED_ON_A_CLIPPER_CARD = (2, "Pass loaded on a Clipper Card")
    CASH_CREDIT_CARD_OR_TICKETS = (3, "Cash, credit card, or ticket(s)")
    DONT_KNOW = (4, "Don't know")
    USED_A_TRANSFER_FROM_A_PREVIOUS_TRANSIT_TRIP = (6, "Used a transfer from a previous transit trip")
    CLIPPER_CASH = (7, "Clipper Cash")
    MISSING = (995, "Missing Response")

class TripSurveyComplete(LabeledEnum):
    """trip_survey_complete value labels."""

    canonical_field_name = "trip_survey_complete"

    NO = (0, "No")
    YES = (1, "Yes")
    MISSING = (995, "Missing Response")
