"""Household formatting for CT-RAMP output.

Transforms canonical household data into CT-RAMP model format, including:
- Income conversion to $2000 midpoint values
- TAZ and subzone mapping
- Vehicle counts (standard and autonomous)

Note: Random number fields (ao_rn, fp_rn, etc.) are excluded as they are
simulation-specific and not needed for survey data summary/validation.
"""

import logging

import polars as pl

from .mappings import INCOME_DETAILED_TO_MIDPOINT, INCOME_FOLLOWUP_TO_MIDPOINT

logger = logging.getLogger(__name__)


def format_households(
    households: pl.DataFrame,
) -> pl.DataFrame:
    """Format household data to CT-RAMP specification.

    Transforms household data from canonical format to CT-RAMP format.
    Key transformations:
    - Rename fields to CT-RAMP conventions
    - Convert income categories to midpoint values
    - Map TAZ and walk-to-transit subzone
    - Set vehicle counts

    Args:
        households: DataFrame with canonical household fields including:
            - hh_id: Household ID
            - home_taz: Home TAZ
            - home_walk_subzone: Walk-to-transit subzone
            - income_detailed: Detailed income category
            - income_followup: Follow-up income category
            - num_vehicles: Number of vehicles
            - num_people: Household size
            - num_workers: Number of workers
            - hh_weight: Household expansion factor

    Returns:
        DataFrame with CT-RAMP household fields:
        - hh_id: Household ID
        - taz: Home TAZ
        - walk_subzone: Walk-to-transit subzone (0/1/2)
        - income: Annual household income ($2000)
        - autos: Number of automobiles
        - size: Number of persons
        - workers: Number of workers
        - humanVehicles: Human-driven vehicles
        - autonomousVehicles: Autonomous vehicles (set to 0)
        - jtf_choice: Joint tour frequency (set to -4 = not yet modeled)

    Notes:
        - Random number fields (ao_rn, fp_rn, etc.) are excluded as they
          are simulation-specific
        - auto_suff field is excluded (incorrectly coded per documentation)
        - Joint tour frequency (jtf_choice) is set to -4 as a placeholder
        - Autonomous vehicles are set to 0 (not yet available in survey data)
    """
    logger.info("Formatting household data for CT-RAMP")

    # Rename columns to CT-RAMP naming convention
    households_ctramp = households.rename(
        {
            # Keep hh_id as is
            "home_taz": "taz",
            "home_walk_subzone": "walk_subzone",
            "num_vehicles": "autos",
            "num_people": "size",
            "num_workers": "workers",
        }
    )

    # Map income categories to midpoint values ($2000)
    households_ctramp = households_ctramp.with_columns(
        pl.col("income_detailed")
        .fill_null(-1)
        .replace_strict(INCOME_DETAILED_TO_MIDPOINT),
        pl.col("income_followup")
        .fill_null(-1)
        .replace_strict(INCOME_FOLLOWUP_TO_MIDPOINT),
    )

    # Use income_detailed if available, otherwise income_followup
    households_ctramp = households_ctramp.with_columns(
        income=pl.when(pl.col("income_detailed") > 0)
        .then(pl.col("income_detailed"))
        .otherwise(pl.col("income_followup"))
    )

    # Add CT-RAMP specific fields
    households_ctramp = households_ctramp.with_columns(
        # Human-driven vehicles = all vehicles (no autonomous in survey data)
        humanVehicles=pl.col("autos"),
        # Autonomous vehicles = 0 (not yet available in survey data)
        autonomousVehicles=pl.lit(0),
        # Joint tour frequency = -4 (not yet modeled/determined)
        jtf_choice=pl.lit(-4),
    )

    # Ensure walk_subzone is valid (0/1/2)
    households_ctramp = households_ctramp.with_columns(
        pl.when(pl.col("walk_subzone").is_null())
        .then(pl.lit(0))
        .otherwise(pl.col("walk_subzone"))
        .alias("walk_subzone")
    )

    # Select final columns in CT-RAMP order
    households_ctramp = households_ctramp.select(
        [
            "hh_id",
            "taz",
            "walk_subzone",
            "income",
            "autos",
            "jtf_choice",
            "size",
            "workers",
            "humanVehicles",
            "autonomousVehicles",
        ]
    )

    logger.info(
        "Formatted %d households for CT-RAMP output", len(households_ctramp)
    )

    return households_ctramp
