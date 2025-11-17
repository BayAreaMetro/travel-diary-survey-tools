"""Configuration models for tour building parameters."""

from pydantic import BaseModel

from data_canon.codebook import LocationType, ModeType, PersonType, TripPurpose


class TourConfig(BaseModel):
    """Configuration model for tour building parameters."""

    class DistanceThresholds(BaseModel):
        """Distance thresholds for location matching (in meters)."""
        home: float = 100  # meters
        work: float = 100  # meters
        school: float = 100  # meters

    class ModeHierarchy(BaseModel):
        """Hierarchy levels for mode types (higher = more important)."""
        walk: int = 1
        bike: int = 2
        auto: int = 3
        transit: int = 4
        drive_transit: int = 5

    


# DEFAULT_CONFIG = {
#     "distance_thresholds": {
#         LocationType.HOME: 100.0,
#         LocationType.WORK: 100.0,
#         LocationType.SCHOOL: 100.0,
#     },
#     "mode_hierarchy": {
#         # Maps mode codes to hierarchy levels (higher = more important)
#         ModeType.WALK: 1,
#         ModeType.BIKE: 2,
#         ModeType.AUTO: 3,
#         ModeType.TRANSIT: 4,
#         ModeType.DRIVE_TRANSIT: 5,
#     },
#     "purpose_priority_by_person_category": {
#         # Priority order for determining primary tour purpose
#         # Lower number = higher priority
#         PersonType.WORKER: {
#             TripPurpose.WORK: 1,  # Highest priority for workers
#             TripPurpose.SCHOOL: 2,
#             TripPurpose.ESCORT: 3,
#             # All other purposes get default priority of 4
#         },
#         PersonType.STUDENT: {
#             TripPurpose.SCHOOL: 1,  # Highest priority for students
#             TripPurpose.WORK: 2,
#             TripPurpose.ESCORT: 3,
#         },
#         PersonType.OTHER: {
#             TripPurpose.WORK: 1,
#             TripPurpose.SCHOOL: 2,
#             TripPurpose.ESCORT: 3,
#         },
#     },
#     "default_purpose_priority": 4,
#     "person_type_mapping": {
#         # Maps person_type codes to categories for priority lookup
#         PersonType.FULL_TIME_WORKER: PersonType.WORKER,
#         PersonType.PART_TIME_WORKER: PersonType.WORKER,
#         PersonType.RETIRED: PersonType.OTHER,
#         PersonType.NON_WORKER: PersonType.OTHER,
#         PersonType.UNIVERSITY_STUDENT: PersonType.STUDENT,
#         PersonType.HIGH_SCHOOL_STUDENT: PersonType.STUDENT,
#         PersonType.CHILD_5_15: PersonType.STUDENT,
#         PersonType.CHILD_UNDER_5: PersonType.OTHER,
#     },
# }
