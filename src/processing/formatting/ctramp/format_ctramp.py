"""CT-RAMP Formatting Step.

Transforms canonical survey data (persons, households, tours, trips) into
CT-RAMP model format. CT-RAMP (Coordinated Travel - Regional Activity Modeling
Platform) is an activity-based travel demand model requiring specific data
structures and coding schemes.

This module serves as the main orchestrator, delegating formatting of each
table type to specialized modules:
- format_persons: Person type classification, free parking, activity patterns
- format_households: Household income, vehicle counts, and composition
- format_tours_trips: Tours, trips, and mandatory locations
"""

import logging

import polars as pl

from pipeline.decoration import step

from .ctramp_config import CTRAMPConfig
from .format_households import format_households
from .format_persons import format_persons
from .format_tours import (
    format_individual_tour,
    format_joint_tour,
    format_mandatory_location,
)
from .format_trips import format_individual_trip, format_joint_trip

logger = logging.getLogger(__name__)


@step()
def format_ctramp(  # noqa: D417, PLR0913
    persons: pl.DataFrame,
    households: pl.DataFrame,
    linked_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
    joint_trips: pl.DataFrame | None = None,
    drop_missing_taz: bool = True,
    income_low_threshold: int | None = None,
    income_med_threshold: int | None = None,
    income_high_threshold: int | None = None,
) -> dict[str, pl.DataFrame]:
    """Format canonical survey data to CT-RAMP model specification.

    Transforms person, household, tour, and trip data from canonical format to
    CT-RAMP format required by the activity-based travel demand model. This
    includes:
    - Person type classification based on age, employment, and student status
    - Free parking eligibility determination
    - Household income conversion to CT-RAMP brackets
    - Vehicle counts (human-driven and autonomous)
    - TAZ and walk-to-transit subzone mapping
    - Mandatory locations (work/school)
    - Individual and joint tours
    - Individual and joint trips

    Args:
        persons: Canonical person data with demographic fields
        households: Canonical household data with income and dwelling fields
        linked_trips: Canonical linked trip data (optional)
        tours: Canonical tour data (optional)
        joint_trips: Aggregated joint trip data (optional)
        drop_missing_taz: If True, remove households without valid TAZ IDs

    Returns:
        Dictionary with keys:
        - households_ctramp: Formatted household data
        - persons_ctramp: Formatted person data
        - mandatory_location_ctramp: Mandatory location data
        - individual_tour_ctramp: Individual tour data
        - individual_trip_ctramp: Individual trip data
        - joint_tour_ctramp: Joint tour data
        - joint_trip_ctramp: Joint trip data

    Example:
        >>> result = format_ctramp(persons, households, linked_trips, tours)
        >>> households_ctramp = result["households_ctramp"]
        >>> persons_ctramp = result["persons_ctramp"]
        >>> tours_ctramp = result["individual_tour_ctramp"]
    """
    # Validate configuration parameters
    config = CTRAMPConfig(
        income_low_threshold=income_low_threshold,
        income_med_threshold=income_med_threshold,
        income_high_threshold=income_high_threshold,
        drop_missing_taz=drop_missing_taz,
    )

    logger.info("Starting CT-RAMP formatting")

    # Drop any households that do not have a TAZ assigned
    if config.drop_missing_taz:
        n_og_households = len(households)
        n_og_persons = len(persons)

        households = households.filter(
            households["home_taz"].is_not_null()
            & (households["home_taz"] != -1)
        )
        persons = persons.filter(
            pl.col("hh_id").is_in(households["hh_id"].implode())
        )

        # Also filter tours and trips if provided
        if tours is not None:
            tours = tours.filter(
                pl.col("hh_id").is_in(households["hh_id"].implode())
            )
        if linked_trips is not None:
            linked_trips = linked_trips.filter(
                pl.col("hh_id").is_in(households["hh_id"].implode())
            )
        if joint_trips is not None:
            joint_trips = joint_trips.filter(
                pl.col("hh_id").is_in(households["hh_id"].implode())
            )

        logger.info(
            "Dropped %d households without TAZ with %d persons; "
            "%d households and %d persons remain",
            n_og_households - len(households),
            n_og_persons - len(persons),
            len(households),
            len(persons),
        )

    # Format each table
    households_ctramp = format_households(households)
    persons_ctramp = format_persons(persons)

    result = {
        "households_ctramp": households_ctramp,
        "persons_ctramp": persons_ctramp,
    }

    # Format mandatory locations
    mandatory_location_ctramp = format_mandatory_location(persons, households)
    result["mandatory_location_ctramp"] = mandatory_location_ctramp

    # Format tours and trips if data provided
    if tours is not None:
        individual_tour_ctramp = format_individual_tour(
            tours, persons, households, config=config
        )
        result["individual_tour_ctramp"] = individual_tour_ctramp

        joint_tour_ctramp = format_joint_tour(
            tours, persons, households, config=config
        )
        if len(joint_tour_ctramp) > 0:
            result["joint_tour_ctramp"] = joint_tour_ctramp

    if linked_trips is not None and tours is not None:
        individual_trip_ctramp = format_individual_trip(
            linked_trips, tours, persons, households, config=config
        )
        result["individual_trip_ctramp"] = individual_trip_ctramp

    if joint_trips is not None and tours is not None:
        joint_trip_ctramp = format_joint_trip(
            joint_trips, tours, households, config=config
        )
        if len(joint_trip_ctramp) > 0:
            result["joint_trip_ctramp"] = joint_trip_ctramp

    logger.info("CT-RAMP formatting complete")

    return result
