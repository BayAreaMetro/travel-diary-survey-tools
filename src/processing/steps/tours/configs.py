"""Configuration models for tour building parameters."""

from pydantic import BaseModel, Field

from data_canon.codebook.generic import LocationType
from data_canon.codebook.persons import PersonType
from data_canon.codebook.trips import ModeType, PurposeCategory


# Define person category groupings (simplified categories for tour logic)
class PersonCategory:
    """Simplified person categories for tour purpose prioritization."""

    WORKER = "worker"
    STUDENT = "student"
    OTHER = "other"


class TourConfig(BaseModel):
    """Configuration model for tour building parameters.

    This config uses Pydantic for validation and provides type-safe access
    to tour building parameters including distance thresholds, mode
    hierarchies, and purpose priorities.
    """

    # Distance thresholds for location matching (in meters)
    distance_thresholds: dict[LocationType, float] = Field(
        default={
            LocationType.HOME: 100.0,
            LocationType.WORK: 100.0,
            LocationType.SCHOOL: 100.0,
        },
        description=(
            "Distance thresholds in meters for matching trip ends "
            "to known locations"
        ),
    )

    # Mode hierarchy: position in list determines priority
    # (later in list = higher priority for tour mode assignment)
    mode_hierarchy: list[ModeType] = Field(
        default=[
            ModeType.WALK,
            ModeType.BIKE,
            ModeType.BIKESHARE,
            ModeType.SCOOTERSHARE,
            ModeType.CAR,
            ModeType.CARSHARE,
            ModeType.TAXI,
            ModeType.TNC,
            ModeType.SHUTTLE_OR_VANPOOL,
            ModeType.SCHOOL_BUS,
            ModeType.FERRY,
            ModeType.TRANSIT,
            ModeType.LONG_DISTANCE_PASSENGER,
        ],
        description=(
            "Ordered list of mode types by priority - "
            "later in list = higher priority"
        ),
    )

    # Purpose priority by person category: lower number = higher priority
    purpose_priority_by_persontype: dict[
        str, dict[PurposeCategory, int]
    ] = Field(
        default={
            PersonCategory.WORKER: {
                PurposeCategory.WORK: 1,
                PurposeCategory.WORK_RELATED: 1,
                PurposeCategory.SCHOOL: 2,
                PurposeCategory.SCHOOL_RELATED: 2,
                PurposeCategory.ESCORT: 3,
            },
            PersonCategory.STUDENT: {
                PurposeCategory.SCHOOL: 1,
                PurposeCategory.SCHOOL_RELATED: 1,
                PurposeCategory.WORK: 2,
                PurposeCategory.WORK_RELATED: 2,
                PurposeCategory.ESCORT: 3,
            },
            PersonCategory.OTHER: {
                PurposeCategory.WORK: 1,
                PurposeCategory.WORK_RELATED: 1,
                PurposeCategory.SCHOOL: 2,
                PurposeCategory.SCHOOL_RELATED: 2,
                PurposeCategory.ESCORT: 3,
            },
        },
        description=(
            "Priority order for determining tour purpose by person "
            "category (lower = higher priority)"
        ),
    )

    # Default priority for purposes not explicitly listed
    default_purpose_priority: int = Field(
        default=4,
        description=(
            "Default priority value for purposes not in "
            "category-specific mappings"
        ),
    )

    # Map detailed person types to simplified categories for priority lookup
    person_type_mapping: dict[PersonType, str] = Field(
        default={
            PersonType.FULL_TIME_WORKER: PersonCategory.WORKER,
            PersonType.PART_TIME_WORKER: PersonCategory.WORKER,
            PersonType.RETIRED: PersonCategory.OTHER,
            PersonType.NON_WORKER: PersonCategory.OTHER,
            PersonType.UNIVERSITY_STUDENT: PersonCategory.STUDENT,
            PersonType.HIGH_SCHOOL_STUDENT: PersonCategory.STUDENT,
            PersonType.CHILD_5_15: PersonCategory.STUDENT,
            PersonType.CHILD_UNDER_5: PersonCategory.OTHER,
        },
        description=(
            "Maps detailed person types to simplified categories "
            "for tour logic"
        ),
    )

    class Config:
        """Pydantic model configuration."""
        arbitrary_types_allowed = True  # Allow enum types
