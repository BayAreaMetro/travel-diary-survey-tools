"""Household formatting for CT-RAMP output.

Transforms canonical household data into CT-RAMP model format, including:
- Income conversion to $2000 midpoint values
- TAZ mapping

Note: Model-output fields (walk_subzone, humanVehicles, autonomousVehicles,
random number fields, auto_suff) are excluded as they are not derivable from
survey data.
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
        - income: Annual household income ($2000)
        - autos: Number of automobiles
        - size: Number of persons
        - workers: Number of workers
        - jtf_choice: Joint tour frequency (set to -4 = not yet modeled)

    Notes:
        - Model-output fields (walk_subzone, humanVehicles, autonomousVehicles,
          random number fields, auto_suff) are excluded as they are not
          derivable from survey data
        - Joint tour frequency (jtf_choice) is set to -4 as a placeholder
    """
    logger.info("Formatting household data for CT-RAMP")

    # Rename columns to CT-RAMP naming convention
    households_ctramp = households.rename(
        {
            # Keep hh_id as is
            "home_taz": "taz",
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
        # Joint tour frequency = -4 (not yet modeled/determined)
        jtf_choice=pl.lit(-4),
    )

    # Select final columns in CT-RAMP order
    households_ctramp = households_ctramp.select(
        [
            "hh_id",
            "taz",
            "income",
            "autos",
            "jtf_choice",
            "size",
            "workers",
        ]
    )

    logger.info(
        "Formatted %d households for CT-RAMP output", len(households_ctramp)
    )

    return households_ctramp
