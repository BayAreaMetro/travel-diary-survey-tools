"""DaySim Formatting Step.

Transforms canonical survey data (persons, households, trips, tours) into
DaySim model format. DaySim is an activity-based travel demand model
requiring specific data structures and coding schemes.

This module serves as the main orchestrator, delegating formatting of each
table type to specialized modules:
- format_persons: Person type classification and day completeness
- format_households: Household composition and income processing
- format_trips: Linked trip mode, path type, and driver/passenger codes
- format_tours: Tour purpose, timing, and location mapping
"""

import logging

import polars as pl

from pipeline.decoration import step

from .format_households import format_households
from .format_persons import compute_day_completeness, format_persons
from .format_tours import format_tours
from .format_trips import format_linked_trips

logger = logging.getLogger(__name__)


@step()
def format_daysim(
    persons: pl.DataFrame,
    households: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
    days: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    """Format canonical survey data to DaySim model specification.

    Transforms person, household, trip, and tour data from canonical format to
    DaySim format required by the activity-based travel demand model. This
    includes:
    - Person type classification based on age, employment, and student status
    - Household composition calculation from person data
    - Trip mode, path type, and driver/passenger code derivation
    - Tour purpose, timing, and location mapping
    - Day completeness computation for survey weighting (optional)

    Args:
        persons: Canonical person data with demographic and location fields
        households: Canonical household data with income and dwelling fields
        unlinked_trips: Canonical unlinked trip data with mode, purpose, and
            timing fields
        linked_trips: Canonical linked trip data with mode, purpose, and
            timing fields
        tours: Canonical tour data with purpose, timing, and location fields
        days: Day-level data for completeness calculation

    Returns:
        Dictionary with keys:
        - households_daysim: Formatted household data
        - persons_daysim: Formatted person data
        - trips_daysim: Formatted trip data
        - tours_daysim: Formatted tour data
    """
    logger.info("Starting DaySim formatting")

    # Compute day completeness if days data provided
    day_completeness = None
    if days is not None:
        day_completeness = compute_day_completeness(days)

    # Format each table
    persons_daysim = format_persons(persons, day_completeness)
    logger.info("Formatted %d persons", len(persons_daysim))

    households_daysim = format_households(households, persons_daysim)
    logger.info("Formatted %d households", len(households_daysim))

    linked_trips_daysim = format_linked_trips(
        persons, unlinked_trips, linked_trips
    )
    logger.info("Formatted %d trips", len(linked_trips_daysim))

    tours_daysim = format_tours(persons, days, linked_trips, tours)
    logger.info("Formatted %d tours", len(tours_daysim))

    # Drop any households that do not have a MAZ/TAZ assigned
    households_daysim = households_daysim.filter(
        (households_daysim["hhtaz"].is_not_null())
        & (households_daysim["hhtaz"] != -1)
    )

    persons_daysim = persons_daysim.filter(
        pl.col("hhno").is_in(households_daysim["hhno"].implode())
    )
    linked_trips_daysim = linked_trips_daysim.filter(
        pl.col("hhno").is_in(households_daysim["hhno"].implode())
    )
    tours_daysim = tours_daysim.filter(
        pl.col("hhno").is_in(households_daysim["hhno"].implode())
    )

    logger.info("DaySim formatting complete")

    return {
        "households_daysim": households_daysim,
        "persons_daysim": persons_daysim,
        "linked_trips_daysim": linked_trips_daysim,
        "tours_daysim": tours_daysim,
    }
