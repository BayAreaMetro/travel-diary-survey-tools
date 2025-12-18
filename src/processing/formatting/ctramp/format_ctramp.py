"""CT-RAMP Formatting Step.

Transforms canonical survey data (persons, households) into CT-RAMP model
format. CT-RAMP (Coordinated Travel - Regional Activity Modeling Platform) is
an activity-based travel demand model requiring specific data structures and
coding schemes.

This module serves as the main orchestrator, delegating formatting of each
table type to specialized modules:
- format_persons: Person type classification, free parking, activity patterns
- format_households: Household income, vehicle counts, and composition

Future Extensions:
- Tours and trips will require modifications to tour extraction algorithm
  to handle joint tours and CT-RAMP specific tour types
- Individual tour and trip formatting modules to be added
- Joint tour and stop location formatting
"""

import logging

import polars as pl

from pipeline.decoration import step

from .format_households import format_households
from .format_persons import format_persons

logger = logging.getLogger(__name__)


@step()
def format_ctramp(
    persons: pl.DataFrame,
    households: pl.DataFrame,
    drop_missing_taz: bool = True,
) -> dict[str, pl.DataFrame]:
    """Format canonical survey data to CT-RAMP model specification.

    Transforms person and household data from canonical format to CT-RAMP
    format required by the activity-based travel demand model. This includes:
    - Person type classification based on age, employment, and student status
    - Free parking eligibility determination
    - Household income conversion to $2000 values
    - Vehicle counts (human-driven and autonomous)
    - TAZ and walk-to-transit subzone mapping

    Args:
        persons: Canonical person data with demographic fields
        households: Canonical household data with income and dwelling fields
        drop_missing_taz: If True, remove households without valid TAZ IDs

    Returns:
        Dictionary with keys:
        - households_ctramp: Formatted household data
        - persons_ctramp: Formatted person data

    Notes:
        - Random number fields (ao_rn, fp_rn, etc.) are excluded as they are
          simulation-specific and not needed for survey data
        - Activity patterns and tour frequencies are set to placeholder values
          as they require tour data not yet processed
        - Tours, trips, and joint tours will be added in future updates once
          the tour extraction algorithm is modified to handle CT-RAMP
          joint tour structures

    Example:
        >>> result = format_ctramp(persons, households)
        >>> households_ctramp = result["households_ctramp"]
        >>> persons_ctramp = result["persons_ctramp"]
    """
    logger.info("Starting CT-RAMP formatting")

    # Drop any households that do not have a TAZ assigned
    if drop_missing_taz:
        n_og_households = len(households)
        n_og_persons = len(persons)

        households = households.filter(
            households["home_taz"].is_not_null()
            & (households["home_taz"] != -1)
        )
        persons = persons.filter(
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

    logger.info("CT-RAMP formatting complete")

    return {
        "households_ctramp": households_ctramp,
        "persons_ctramp": persons_ctramp,
    }
