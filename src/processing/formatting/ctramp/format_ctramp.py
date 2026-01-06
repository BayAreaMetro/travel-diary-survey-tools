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
from .format_mandatory_location import format_mandatory_location
from .format_persons import format_persons
from .format_tours import format_individual_tour, format_joint_tour
from .format_trips import format_individual_trip, format_joint_trip

logger = logging.getLogger(__name__)


@step()
def format_ctramp(  # noqa: D417, PLR0913
    persons: pl.DataFrame,
    households: pl.DataFrame,
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
    joint_trips: pl.DataFrame,
    income_low_threshold: int,
    income_med_threshold: int,
    income_high_threshold: int,
    income_base_year_dollars: int,
    drop_missing_taz: bool = True,
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
        linked_trips: Canonical linked trip data (required)
        tours: Canonical tour data (required)
        joint_trips: Aggregated joint trip data (required)
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
        income_base_year_dollars=income_base_year_dollars,
        drop_missing_taz=drop_missing_taz,
    )

    logger.info("Starting CT-RAMP formatting")

    # Drop any households that do not have a TAZ assigned
    if config.drop_missing_taz:
        n_og_households = len(households)
        n_og_persons = len(persons)

        households = households.filter(
            households["home_taz"].is_not_null() & (households["home_taz"] != -1)
        )
        persons = persons.filter(pl.col("hh_id").is_in(households["hh_id"].implode()))

        # Filter tours and trips (skip if empty to avoid errors)
        if len(tours) > 0:
            tours = tours.filter(pl.col("hh_id").is_in(households["hh_id"].implode()))
        if len(linked_trips) > 0:
            linked_trips = linked_trips.filter(pl.col("hh_id").is_in(households["hh_id"].implode()))
        if len(joint_trips) > 0:
            joint_trips = joint_trips.filter(pl.col("hh_id").is_in(households["hh_id"].implode()))

        logger.info(
            "Dropped %d households without TAZ with %d persons; "
            "%d households and %d persons remain",
            n_og_households - len(households),
            n_og_persons - len(persons),
            len(households),
            len(persons),
        )

    # Format each table
    households_ctramp = format_households(households, persons)

    # Format tours - use empty DataFrame with proper schema if no tours exist
    if len(tours) == 0:
        individual_tours_ctramp = pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "tour_purpose": pl.String,
            }
        )
    else:
        individual_tours_ctramp = format_individual_tour(
            tours, linked_trips, persons, households_ctramp, config
        )

    # Format persons with tour statistics (works with empty or populated tours)
    persons_ctramp = format_persons(persons, individual_tours_ctramp, config)

    # Format mandatory locations - needs canonical households (home_taz)
    # but we need income from formatted households, so rejoin home_taz
    households_with_taz = households_ctramp.join(
        households.select(["hh_id", "home_taz"]), on="hh_id", how="left"
    )
    mandatory_location_ctramp = format_mandatory_location(persons, households_with_taz, config)

    # Add formatted tours to results
    joint_tours_ctramp = format_joint_tour(
        tours, linked_trips, persons_ctramp, households_ctramp, config
    )

    individual_trips_ctramp = format_individual_trip(
        linked_trips, tours, persons, households_ctramp, config=config
    )

    joint_trips_ctramp = format_joint_trip(
        joint_trips, linked_trips, tours, households_ctramp, config=config
    )

    logger.info("CT-RAMP formatting complete")

    # Prepare result dictionary
    result = {
        "households_ctramp": households_ctramp,
        "persons_ctramp": persons_ctramp,
        "individual_trips_ctramp": individual_trips_ctramp,
        "individual_tours_ctramp": individual_tours_ctramp,
        "joint_trips_ctramp": joint_trips_ctramp,
        "joint_tours_ctramp": joint_tours_ctramp,
        "mandatory_locations_ctramp": mandatory_location_ctramp,
    }

    return result
