"""Household formatting for CT-RAMP.

Transforms canonical household data into CT-RAMP model format, including:
- Income conversion to $2000 midpoint values
- TAZ mapping

Note: Model-output fields (walk_subzone, humanVehicles, autonomousVehicles,
random number fields, auto_suff) are excluded as they are not derivable from
survey data.
"""

import logging

import polars as pl

from data_canon.codebook.households import IncomeDetailed, IncomeFollowup
from data_canon.codebook.persons import Employment
from utils.helpers import get_income_midpoint

logger = logging.getLogger(__name__)


def format_households(
    households: pl.DataFrame,
    persons: pl.DataFrame,
) -> pl.DataFrame:
    """Format household data to CT-RAMP specification.

    Transforms household data from canonical format to CT-RAMP format.
    Key transformations:
    - Rename fields to CT-RAMP conventions
    - Convert income categories to midpoint values
    - Compute household aggregates (size, workers, vehicles)
    - Map TAZ

    Args:
        households: DataFrame with canonical household fields including:
            - hh_id: Household ID
            - home_taz: Home TAZ
            - income_detailed: Detailed income category
            - income_followup: Follow-up income category
        persons: DataFrame with person data for computing household aggregates:
            - hh_id: Household ID
            - employment: Employment status
            - (other person fields)

    Returns:
        DataFrame with CT-RAMP household fields:
        - hh_id: Household ID
        - taz: Home TAZ
        - income: Annual household income (midpoint value)
        - autos: Number of automobiles (0 if no vehicle data)
        - size: Number of persons
        - workers: Number of workers
        - jtf_choice: Joint tour frequency (set to -4 = not yet modeled)

    Notes:
        - Model-output fields (walk_subzone, humanVehicles, autonomousVehicles,
          random number fields, auto_suff) are excluded as they are not
          derivable from survey data
        - Joint tour frequency (jtf_choice) is set to -4 as a placeholder
        - Vehicles (autos) set to 0 as placeholder; should be computed from
          vehicle table if available
    """
    logger.info("Formatting household data for CT-RAMP")

    # Compute household aggregates from persons table
    household_aggregates = persons.group_by("hh_id").agg(
        [
            pl.len().alias("size"),
            # Count employed persons
            pl.col("employment")
            .is_in(
                [
                    Employment.EMPLOYED_FULLTIME.value,
                    Employment.EMPLOYED_PARTTIME.value,
                    Employment.EMPLOYED_SELF.value,
                    Employment.EMPLOYED_UNPAID.value,
                    Employment.EMPLOYED_FURLOUGHED.value,
                ]
            )
            .sum()
            .alias("workers"),
        ]
    )

    # Join aggregates with households
    households_ctramp = households.join(
        household_aggregates, on="hh_id", how="left"
    )

    # Rename columns to CT-RAMP naming convention
    households_ctramp = households_ctramp.rename(
        {
            "home_taz": "taz",
        }
    )

    # Map income categories to midpoint values
    income_detailed_map = {
        income_cat.value: get_income_midpoint(income_cat)
        for income_cat in IncomeDetailed
        if "Prefer not to answer" not in income_cat.label
        and "Missing" not in income_cat.label
    }
    income_followup_map = {
        income_cat.value: get_income_midpoint(income_cat)
        for income_cat in IncomeFollowup
        if "Prefer not to answer" not in income_cat.label
        and "Missing" not in income_cat.label
    }

    households_ctramp = households_ctramp.with_columns(
        pl.col("income_detailed")
        .fill_null(-1)
        .replace_strict(income_detailed_map, default=-1),
        pl.col("income_followup")
        .fill_null(-1)
        .replace_strict(income_followup_map, default=-1),
    )

    # Use income_detailed if available, otherwise income_followup
    households_ctramp = households_ctramp.with_columns(
        income=pl.when(pl.col("income_detailed") > 0)
        .then(pl.col("income_detailed"))
        .otherwise(pl.col("income_followup"))
    )

    # Add CT-RAMP specific fields
    households_ctramp = households_ctramp.with_columns(
        [
            # Joint tour frequency = -4 (not yet modeled/determined)
            pl.lit(-4).alias("jtf_choice"),
            # Vehicles = 0 as placeholder (TODO: compute from vehicle table)
            pl.lit(0).alias("autos"),
        ]
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

    logger.info("Formatted %d households for CT-RAMP", len(households_ctramp))

    return households_ctramp
