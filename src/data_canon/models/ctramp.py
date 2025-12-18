"""Data models for DaySim file formats.

Based on https://github.com/BayAreaMetro/modeling-website/wiki/DataDictionary
"""
# ruff: noqa: E501 N815

from pydantic import BaseModel, Field


class HouseholdCTRAMPModel(BaseModel):
    """Household results from Travel Model (CT-RAMP format)."""

    hh_id: int = Field(description="Unique household ID number")
    taz: int = Field(
        ge=1,
        le=1454,
        description="Transportation analysis zone of home location",
    )
    walk_subzone: int = Field(
        ge=0,
        le=2,
        description="Walk to transit sub-zone (0=cannot walk to transit, 1=short-walk, 2=long-walk)",
    )
    income: int = Field(ge=0, description="Annual household income ($2000)")
    autos: int = Field(ge=0, description="Household automobiles")
    jtf_choice: int = Field(
        ge=-4, le=21, description="Number and type of household joint tours"
    )
    size: int = Field(ge=1, description="Number of persons in the household")
    workers: int = Field(
        ge=0,
        description="Number of full- or part-time workers in the household",
    )
    auto_suff: int | None = Field(None, description="Incorrectly coded; ignore")
    # NOTE: Are these needed for summarizing?
    ao_rn: int = Field(
        description="Random number for automobile ownership model"
    )
    fp_rn: int = Field(description="Random number for free parking model")
    cdap_rn: int = Field(
        description="Random number for coordinated daily activity pattern model"
    )
    imtf_rn: int = Field(
        description="Random number for individual mandatory tour frequency model"
    )
    imtod_rn: int = Field(
        description="Random number for individual mandatory tour time-of-day model"
    )
    immc_rn: int = Field(
        description="Random number for individual mandatory mode choice model"
    )
    jtf_rn: int = Field(
        description="Random number for joint tour frequency model"
    )
    jtl_rn: int = Field(
        description="Random number for joint tour location choice model"
    )
    jtod_rn: int = Field(
        description="Random number for joint tour time-of-day model"
    )
    jmc_rn: int = Field(
        description="Random number for joint tour mode choice model"
    )
    inmtf_rn: int = Field(
        description="Random number for individual non-mandatory tour frequency model"
    )
    inmtl_rn: int = Field(
        description="Random number for individual non-mandatory location choice model"
    )
    inmtod_rn: int = Field(
        description="Random number for individual non-mandatory time-of-day model"
    )
    inmmc_rn: int = Field(
        description="Random number for individual non-mandatory mode choice model"
    )
    awf_rn: int = Field(description="Random number for at-work frequency model")
    awl_rn: int = Field(
        description="Random number for at-work location choice model"
    )
    awtod_rn: int = Field(
        description="Random number for at-work time-of-day model"
    )
    awmc_rn: int = Field(
        description="Random number for at-work mode choice model"
    )
    stf_rn: int = Field(description="Random number for stop frequency model")
    stl_rn: int = Field(
        description="Random number for stop location choice model"
    )
    humanVehicles: int = Field(
        ge=0, description="Household automobiles, human-driven (New in TM1.5)"
    )
    autonomousVehicles: int = Field(
        ge=0, description="Household automobiles, autonomous (New in TM1.5)"
    )


class PersonCTRAMPModel(BaseModel):
    """Person results from Travel Model (CT-RAMP format)."""

    hh_id: int = Field(description="Unique household ID number")
    person_id: int = Field(description="Unique person ID number")
    person_num: int = Field(
        ge=1, description="Person number unique to the household"
    )
    age: int = Field(ge=0, description="Person age")
    gender: str = Field(description="Person gender (m=male, f=female)")
    type: str = Field(
        description=(
            "Person lifestage/employment type (Full-time worker, Part-time worker, University student, Nonworker, "
            "Retired, Student of non-driving age, Student of driving age, Child too young for school)"
        )
    )
    value_of_time: float = Field(
        ge=0, description="Value of time ($2000 per hour)"
    )
    fp_choice: int = Field(
        ge=1,
        le=2,
        description="Free parking eligibility choice (1=park for free, 2=pay to park)",
    )
    activity_pattern: str = Field(
        description="Primary daily activity pattern (M=mandatory, N=non-mandatory, H=home)"
    )
    imf_choice: int = Field(
        ge=1,
        le=5,
        description=(
            "Individual mandatory tour frequency (1=one work tour, 2=two work tours, 3=one school tour, "
            "4=two school tours, 5=one work tour and one school tour)"
        ),
    )
    inmf_choice: int = Field(
        ge=1, le=96, description="Individual non-mandatory tour frequency"
    )
    wfh_choice: int = Field(
        ge=0,
        le=1,
        description="Works from home choice (0=non-worker or workers who don't work from home, 1=workers who work from home) - Added in TM1.6",
    )


class MandatoryLocationCTRAMPModel(BaseModel):
    """Mandatory tour locations from Travel Model (CT-RAMP format)."""

    HHID: int = Field(description="Unique household ID number")
    HomeTAZ: int = Field(
        ge=1, le=1454, description="Home transportation analysis zone"
    )
    HomeSubZone: int = Field(
        ge=0,
        le=2,
        description="Walk to transit home sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )
    Income: int = Field(ge=0, description="Annual household income ($2000)")
    PersonID: int = Field(description="Unique person ID number")
    PersonNum: int = Field(
        ge=1, description="Person number unique to the household"
    )
    PersonType: int = Field(
        ge=1,
        le=8,
        description=(
            "Person lifestage/employment type (1=Full-time worker; 2=Part-time worker; 3=University student; "
            "4=Nonworker; 5=Retired; 6=Student of non-driving age; 7=Student of driving age; "
            "8=Child too young for school)"
        ),
    )
    PersonAge: int = Field(ge=0, description="Person age")
    EmploymentCategory: str = Field(
        description='Employment category ("Full-time worker", "Part-time worker", "Not employed")'
    )
    StudentCategory: str = Field(
        description='Student category ("College or higher", "Grade or high school", "Not student")'
    )
    WorkLocation: int = Field(
        ge=0,
        le=1454,
        description="Work location transportation analysis zone (1-1454, or 0 if no workplace is selected)",
    )
    WorkSubZone: int = Field(
        ge=0,
        le=2,
        description="Walk to transit work sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )
    SchoolLocation: int = Field(
        ge=0,
        le=1454,
        description="School location transportation analysis zone (1-1454, or 0 if no school is selected)",
    )
    SchoolSubZone: int = Field(
        ge=0,
        le=2,
        description="Walk to transit school sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )


class IndividualTourCTRAMPModel(BaseModel):
    """Individual tours from Travel Model (CT-RAMP format)."""

    hh_id: int = Field(description="Unique household ID number")
    person_id: int = Field(description="Unique person ID number")
    person_num: int = Field(
        ge=1, description="Person number unique to the household"
    )
    person_type: int = Field(
        ge=1,
        le=8,
        description=(
            "Person lifestage/employment type (1=Full-time worker; 2=Part-time worker; 3=University student; "
            "4=Nonworker; 5=Retired; 6=Student of non-driving age; 7=Student of driving age; "
            "8=Child too young for school)"
        ),
    )
    tour_id: int = Field(
        ge=0, le=4, description="Individual tour number unique to the person"
    )
    tour_category: str = Field(
        description='Type of tour ("MANDATORY", "INDIVIDUAL_NON_MANDATORY", "AT_WORK")'
    )
    tour_purpose: str = Field(
        description=(
            'Tour purpose, given the type of tour ("work_low", "work_med", "work_high", "work_very high", '
            '"university", "school_high", "school_grade", "atwork_business", "atwork_eat", "atwork_maint", '
            '"eatout", "escort_kids", "escort_no kids", "othdiscr", "othmaint", "shopping", "social")'
        )
    )
    orig_taz: int = Field(
        ge=1, le=1454, description="Origin transportation analysis zone"
    )
    orig_walk_segment: int = Field(
        ge=0,
        le=2,
        description="Walk to transit origin sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )
    dest_taz: int = Field(
        ge=1, le=1454, description="Destination transportation analysis zone"
    )
    dest_walk_segment: int = Field(
        ge=0,
        le=2,
        description="Walk to transit destination sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )
    start_hour: int = Field(
        ge=5,
        le=23,
        description="Start time of the tour (5=5am-6am, ..., 23=11pm-midnight)",
    )
    end_hour: int = Field(
        ge=5,
        le=23,
        description="End time of the tour (5=5am-6am, ..., 23=11pm-midnight)",
    )
    tour_mode: int = Field(
        description="Primary travel mode for the tour (see TravelModes#tour-and-trip-modes)"
    )
    atWork_freq: int = Field(
        description="Number of at work sub-tours (non-zero only for work tours)"
    )
    num_ob_stops: int = Field(
        description="Number of out-bound stops on the tour"
    )
    num_ib_stops: int = Field(
        description="Number of in-bound stops on the tour"
    )
    avAvailable: int = Field(
        description="Autonomous vehicle available, added in Travel Model 1.5"
    )
    dcLogsum: float | None = Field(
        None, description="To document, added in Travel Model 1.5"
    )
    sampleRate: float | None = Field(
        None, description="To document, added in Travel Model 1.5"
    )
    origTaxiWait: float | None = Field(
        None, description="To document, added in Travel Model 1.5"
    )
    destTaxiWait: float | None = Field(
        None, description="To document, added in Travel Model 1.5"
    )
    origSingleTNCWait: float | None = Field(
        None, description="To document, added in Travel Model 1.5"
    )
    destSingleTNCWait: float | None = Field(
        None, description="To document, added in Travel Model 1.5"
    )
    origSharedTNCWait: float | None = Field(
        None, description="To document, added in Travel Model 1.5"
    )
    destSharedTNCWait: float | None = Field(
        None, description="To document, added in Travel Model 1.5"
    )


class IndividualTripCTRAMPModel(BaseModel):
    """Individual trips from Travel Model (CT-RAMP format)."""

    hh_id: int = Field(description="Unique household ID number")
    person_id: int = Field(description="Unique person ID number")
    person_num: int = Field(
        ge=1, description="Person number unique to the household"
    )
    tour_id: int = Field(
        description=(
            "Individual tour number unique to the person. "
            "0 (1 individual tour), 1, 2, 3, or 4 (5 individual tours), or, for at work sub-tours, "
            "a two-digit integer where the first digit is the individual tour number (1-based) and the second digit is the sub-tour number (e.g. 12, is second sub-tour made on tour number 1, which has a tour_id of 0)."
        )
    )
    stop_id: int = Field(
        description="Stop number unique to half tour; in order of trips; -1 if this is a half tour"
    )
    inbound: int = Field(
        ge=0,
        le=1,
        description="Inbound stop indicator (1 if the trip is on the inbound leg of the tour, 0 otherwise)",
    )
    tour_purpose: str = Field(
        description=(
            'Tour purpose, given the type of tour ("work_low", "work_med", "work_high", "work_very high", '
            '"university", "school_high", "school_grade", "atwork_business", "atwork_eat", "atwork_maint", '
            '"eatout", "escort_kids", "escort_no kids", "othdiscr", "othmaint", "shopping", "social")'
        )
    )
    orig_purpose: str = Field(
        description=(
            'Purpose at the origin end of the trip ("Home", "work_low", "work_med", "work_high", "work_very high", '
            '"university", "school_high", "school_grade", "atwork_business", "atwork_eat", "atwork_maint", '
            '"eatout", "escort_kids", "escort_no kids", "othdiscr", "othmaint", "shopping", "social")'
        )
    )
    dest_purpose: str = Field(
        description=(
            'Purpose at the destination end of the trip ("Home", "work_low", "work_med", "work_high", "work_very high", '
            '"university", "school_high", "school_grade", "atwork_business", "atwork_eat", "atwork_maint", '
            '"eatout", "escort_kids", "escort_no kids", "othdiscr", "othmaint", "shopping", "social")'
        )
    )
    orig_taz: int = Field(
        ge=1, le=1454, description="Origin transportation analysis zone"
    )
    orig_walk_segment: int = Field(
        ge=0,
        le=2,
        description="Walk to transit origin sub-zone (0=cannot walk to transit; 1=short walk; 2=long walk)",
    )
    dest_taz: int = Field(
        ge=1, le=1454, description="Destination transportation analysis zone"
    )
    dest_walk_segment: int = Field(
        ge=0,
        le=2,
        description="Walk to transit destination sub-zone (0=cannot walk to transit; 1=short walk; 2=long walk)",
    )
    parking_taz: int = Field(
        ge=0,
        le=1454,
        description="Transportation analysis zone in which the trip maker(s) park (0 if no parking zone is selected)",
    )
    depart_hour: int = Field(
        ge=5,
        le=23,
        description="Time of departure for the trip (5=5am-6am, ..., 23=11pm-midnight)",
    )
    trip_mode: int = Field(
        description="Travel mode for the trip (see TravelModes#tour-and-trip-modes)"
    )
    tour_mode: int = Field(
        description="Primary travel mode for the tour (see TravelModes#tour-and-trip-modes)"
    )
    tour_category: str = Field(
        description='The type of tour for which this trip is a part ("AT_WORK", "INDIVIDUAL_NON_MANDATORY", "MANDATORY")'
    )
    avAvailable: int | None = Field(
        None,
        description="Does the household have an autonomous vehicle available for this tour?",
    )
    sampleRate: float | None = Field(
        None, description="This household represents 1/sampleRate households"
    )
    taxiWait: float | None = Field(None, description="TBD")
    singleTNCWait: float | None = Field(None, description="TBD")
    sharedTNCWait: float | None = Field(None, description="TBD")


class JointTourCTRAMPModel(BaseModel):
    """Joint tours from Travel Model (CT-RAMP format)."""

    hh_id: int = Field(description="Unique household ID number")
    tour_id: int = Field(
        ge=0,
        description="Joint tour number unique to the household (0=first joint tour, 1=second, etc.)",
    )
    tour_category: str = Field(
        description='Type of joint tour ("JOINT_NON_MANDATORY")'
    )
    tour_purpose: str = Field(
        description='Purpose of the joint tour ("eatout", "othdiscr", "othmaint", "shopping", "social")'
    )
    tour_composition: int = Field(
        ge=1,
        le=3,
        description="Type of tour composition (1=adults only; 2=children only; 3=adults and children)",
    )
    tour_participants: str = Field(
        description="Household members participating in the tour (space-separated person_num values)"
    )
    orig_taz: int = Field(
        ge=1, le=1454, description="Origin transportation analysis zone"
    )
    orig_walk_segment: int = Field(
        ge=0,
        le=2,
        description="Walk to transit origin sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )
    dest_taz: int = Field(
        ge=1, le=1454, description="Destination transportation analysis zone"
    )
    dest_walk_segment: int = Field(
        ge=0,
        le=2,
        description="Walk to transit destination sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )
    start_hour: int = Field(
        ge=5,
        le=23,
        description="Start time of the tour (5=5am-6am, ..., 23=11pm-midnight)",
    )
    end_hour: int = Field(
        ge=5,
        le=23,
        description="End time of the tour (5=5am-6am, ..., 23=11pm-midnight)",
    )
    tour_mode: int = Field(
        description="Primary travel mode for the tour (see TravelModes#tour-and-trip-modes)"
    )
    num_ob_stops: int = Field(
        ge=0, description="Number of out-bound (from home) stops on the tour"
    )
    num_ib_stops: int = Field(
        ge=0, description="Number of in-bound (to home) stops on the tour"
    )


class JointTripCTRAMPModel(BaseModel):
    """Joint trip results from Travel Model (CT-RAMP format)."""

    hh_id: int = Field(description="Unique household ID number")
    tour_id: int = Field(
        ge=0,
        description="Joint tour number unique to the household (0=first joint tour, 1=second, etc.)",
    )
    stop_id: int = Field(
        description="Stop number unique to half tour; in order of trips; -1 if this is a half tour"
    )
    inbound: int = Field(
        ge=0,
        le=1,
        description="Inbound stop indicator (1 if the stop is on the inbound leg of the tour, 0 otherwise)",
    )
    tour_purpose: str = Field(
        description='Purpose of the joint tour ("eatout", "othdiscr", "othmaint", "shopping", "social")'
    )
    orig_purpose: str = Field(
        description='Purpose at the origin end of the trip ("Home", "eatout", "othdiscr", "othmaint", "shopping", "social")'
    )
    dest_purpose: str = Field(
        description='Purpose at the destination end of the trip ("Home", "eatout", "othdiscr", "othmaint", "shopping", "social")'
    )
    orig_taz: int = Field(
        ge=1, le=1454, description="Origin transportation analysis zone"
    )
    orig_walk_segment: int = Field(
        ge=0,
        le=2,
        description="Walk to transit origin sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )
    dest_taz: int = Field(
        ge=1, le=1454, description="Destination transportation analysis zone"
    )
    dest_walk_segment: int = Field(
        ge=0,
        le=2,
        description="Walk to transit destination sub-zone (0=cannot walk to transit; 1=short-walk; 2=long-walk)",
    )
    parking_taz: int = Field(
        ge=0,
        le=1454,
        description="Transportation analysis zone in which the trip maker(s) park (0 if no parking zone is selected)",
    )
    depart_hour: int = Field(
        ge=5,
        le=23,
        description="Time of departure for the trip (5=5am-6am, ..., 23=11pm-midnight)",
    )
    trip_mode: int = Field(
        description="Travel mode for the trip (see TravelModes#tour-and-trip-modes)"
    )
    num_participants: int = Field(
        ge=2, description="Number of participants on the tour"
    )
    tour_mode: int = Field(
        description="Primary travel mode for the tour (see TravelModes#tour-and-trip-modes)"
    )
    tour_category: str = Field(
        description='Tour category ("JOINT_NON_MANDATORY")'
    )
