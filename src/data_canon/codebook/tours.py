"""Tour building step for processing travel diary data."""

from data_canon.labeled_enum import LabeledEnum

class TourType(LabeledEnum):
    """d_tour_type value labels."""

    HOME_BASED = (1, "Home-based tour")
    WORK_BASED = (2, "Work-based tour")