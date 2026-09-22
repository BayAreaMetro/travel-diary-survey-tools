"""Rules for matching trip ends to habitual locations, and for finding them."""

from pydantic import BaseModel, Field

from data_canon.codebook.trips import Purpose

DEFAULT_BUFFER_METERS = 300.0


class MatchConfig(BaseModel):
    """The one rule for whether a point is at a habitual location.

    Every step that asks the question takes this, so projects set the buffer
    once (a top-level config key referenced by each step) and the answer cannot
    differ between them.
    """

    buffer_meters: float = Field(
        default=DEFAULT_BUFFER_METERS,
        gt=0,
        description=(
            "Distance in metres within which two points are the same place. It "
            "groups stops into one location, decides a found place is a reported "
            "one, decides a trip end is at a location, and decides a tour came "
            "back to its primary destination. Reported locations are geocoded "
            "from what the respondent typed while trip ends are GPS, and people "
            "walk about a site, so "
            "it is a few hundred metres rather than a building's width. Set it "
            "to suit the survey's location noise."
        ),
    )


class HabitualLocationConfig(MatchConfig):
    """Rules for finding observed locations, on top of the match rule.

    Recurrence is deliberately not required (``min_distinct_days`` defaults to
    1): some survey platforms collect a single travel day, and most people go to
    work once a day anyway. Stay length is what separates a workplace from a
    brief stop.
    """

    min_dwell_minutes: float = Field(
        default=90.0,
        ge=0,
        description=(
            "Minimum length of one stop for it to count as evidence of a "
            "workplace or school, unless its purpose has its own entry in "
            "min_dwell_minutes_by_purpose."
        ),
    )
    min_dwell_minutes_by_purpose: dict[Purpose, float] = Field(
        default={Purpose.COLLEGE: 45.0},
        description=(
            "Per-purpose overrides of min_dwell_minutes. College is lower "
            "because a student may attend one class and leave."
        ),
    )
    min_distinct_days: int = Field(
        default=1,
        ge=1,
        description=(
            "Optional minimum number of distinct travel days an observed location "
            "was visited. Defaults to 1 (no filter)."
        ),
    )
