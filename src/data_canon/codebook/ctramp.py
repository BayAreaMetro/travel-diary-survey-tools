"""Codebook definitions for CT-RAMP related enumerations."""

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


class PersonType(LabeledEnum):
    """Enumeration for person type categories."""

    FULL_TIME_WORKER = 1, "Full-time worker"
    PART_TIME_WORKER = 2, "Part-time worker"
    UNIVERSITY_STUDENT = 3, "University student"
    NONWORKER = 4, "Nonworker"
    RETIRED = 5, "Retired"
    STUDENT_NON_DRIVING_AGE = 6, "Student of non-driving age"
    STUDENT_DRIVING_AGE = 7, "Student of driving age"
    CHILD_TOO_YOUNG = 8, "Child too young for school"
