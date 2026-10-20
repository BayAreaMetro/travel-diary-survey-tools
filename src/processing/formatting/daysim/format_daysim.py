"""DaySim Formatting Step.

Transforms canonical survey data (persons, households, trips, tours, days) into
DaySim activity-based travel demand model format, applying model-specific coding
schemes and data structures. See [DaySim Data Models](../../models/daysim.md).

This module orchestrates specialized formatting for each table type, applying
DaySim-specific integer codes for categorical variables while maintaining
referential integrity across tables.

# Components

* [`format_persons`][processing.formatting.daysim.format_persons]: Produces persons_daysim
  table consistent with [`PersonDaysimModel`][data_canon.models.daysim.PersonDaysimModel].
* [`format_households`][processing.formatting.daysim.format_households]: Produces
  households_daysim table consistent with
  [`HouseholdDaysimModel`][data_canon.models.daysim.HouseholdDaysimModel].
* [`format_linked_trips`][processing.formatting.daysim.format_trips]: Produces
  trips_daysim table consistent with
  [`LinkedTripDaysimModel`][data_canon.models.daysim.LinkedTripDaysimModel].
* [`format_tours`][processing.formatting.daysim.format_tours]: Produces tours_daysim table
  consistent with [`TourDaysimModel`][data_canon.models.daysim.TourDaysimModel].
* [`format_days`][processing.formatting.daysim.format_days]: Produces days_daysim table
  consistent with [`PersonDayDaysimModel`][data_canon.models.daysim.PersonDayDaysimModel].

# Record Selection

The step keeps the records its ``usability_profile`` admits (``usable_<profile>``),
so partial tours, invalid tours and records without a TAZ are left out by the
profile (its ``tour_closes_at`` and ``zone_coverage``), not by this step.

# Implementation Notes

- DaySim requires specific integer codes for categorical variables
- Formatting maintains referential integrity across tables
- TAZ (Traffic Analysis Zone) assignment critical for model application
- Person type classification affects downstream choice model applicability
- Mode/purpose hierarchies ensure consistent coding
- Output validates against DaySim data specifications
- Days with invalid/partial tours become "no travel" days in the model
"""

import logging

import polars as pl

from pipeline.decoration import step
from processing.formatting.usable_records import keep_usable
from processing.formatting.usual_workplace import fill_usual_workplace

from .format_days import format_days
from .format_households import format_households
from .format_persons import format_persons
from .format_tours import format_tours
from .format_trips import format_linked_trips

logger = logging.getLogger(__name__)


@step(
    requires={
        "households": {"residence_rent_own", "residence_type"},
        "persons": {"person_num", "work_park", "work_mode", "is_proxy"},
        "days": {"travel_dow"},
        "unlinked_trips": {"tour_id"},
        "linked_trips": {
            "tour_id",
            "tour_direction",
            "driver",
            "access_mode",
            "egress_mode",
        },
        "tours": {"origin_linked_trip_id", "dest_linked_trip_id"},
    },
)
def format_daysim(
    persons: pl.DataFrame,
    households: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
    days: pl.DataFrame,
    usability_profile: str,
    habitual_locations: pl.DataFrame | None = None,
    habitual_location_days: pl.DataFrame | None = None,
) -> dict[str, pl.DataFrame]:
    """Format canonical survey data to DaySim model specification.

    Converts canonical survey tables to DaySim activity-based travel demand model
    format. See module docstring for complete component descriptions.

    Args:
        persons: Person attributes in canonical format. Required columns:
            person_id, hh_id, age, employment, student, etc.
        households: Household attributes in canonical format. Required columns:
            hh_id, home_taz, income, etc.
        unlinked_trips: Individual trip segments with mode, purpose, and timing.
        linked_trips: Journey records with coordinates, mode, purpose, and timing.
        tours: Tour records with purpose, timing, and location fields.
        days: Person-day records for completeness calculation.
        usability_profile: Which usability profile decides the record universe.
            Required: with several profiles stamped there is no defensible
            default, and naming a different one from the CT-RAMP formatter or
            the weighting means those outputs describe different universes.
        habitual_locations: Habitual locations, zoned by ``add_zone_ids``. An
            employed person who reported no workplace takes their most-visited
            observed workplace as ``pwtaz``/``pwpcl`` (see
            [`usual_workplace`][processing.formatting.usual_workplace]). None
            leaves the reported workplaces alone.
        habitual_location_days: Time spent at each habitual location per day,
            which picks the most-visited workplace.

    Returns:
        Dictionary containing:
            - households_daysim: Formatted household data with person type
              composition and income categories
            - persons_daysim: Formatted person data with person type, day pattern,
              and completeness flags
            - days_daysim: Formatted day-level data with summaries
            - linked_trips_daysim: Formatted trip data with DaySim mode, path type,
              and driver/passenger codes
            - tours_daysim: Formatted tour data with DaySim purpose codes and
              timing

    !!! Example

        ```python

        result = format_daysim(
            persons=canonical_persons,
            households=canonical_households,
            unlinked_trips=canonical_unlinked_trips,
            linked_trips=canonical_linked_trips,
            tours=canonical_tours,
            days=canonical_days,
            usability_profile="daysim",
        )
        households_daysim = result["households_daysim"]
        persons_daysim = result["persons_daysim"]
        ```
    """
    logger.info("Starting DaySim formatting")

    # One gate, read from the profile the config names. DaySim selects nothing
    # of its own: the same verdict decides the CT-RAMP output and the weighting,
    # so the three describe one universe rather than three implementations of it.
    gated = keep_usable(
        {
            "households": households,
            "persons": persons,
            "days": days,
            "tours": tours,
            "linked_trips": linked_trips,
            "unlinked_trips": unlinked_trips,
        },
        usability_profile,
    )
    households = gated["households"]
    persons = gated["persons"]
    days = gated["days"]
    tours = gated["tours"]
    linked_trips = gated["linked_trips"]
    unlinked_trips = gated["unlinked_trips"]

    # The survey asks for a workplace only when the job has one fixed location;
    # for anyone else employed, their trips may show one.
    persons = fill_usual_workplace(
        persons,
        habitual_locations,
        habitual_location_days,
        zone_names=[z for z in ("taz", "maz") if f"work_{z}" in persons.columns],
    )

    # Format each table

    # Format persons, includes day for completeness computation
    persons_daysim = format_persons(persons, days)

    # Format households, requires the daysim formatted person types
    households_daysim = format_households(households, persons_daysim)

    # Format days
    days_daysim = format_days(persons, days, tours)

    # Format linked trips
    linked_trips_daysim = format_linked_trips(persons, unlinked_trips, linked_trips)

    # Format tours
    tours_daysim = format_tours(persons, days, linked_trips, tours)

    logger.info("DaySim formatting complete")

    return {
        "households_daysim": households_daysim,
        "persons_daysim": persons_daysim,
        "days_daysim": days_daysim,
        "linked_trips_daysim": linked_trips_daysim,
        "tours_daysim": tours_daysim,
    }
