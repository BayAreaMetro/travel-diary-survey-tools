"""Runner script for the BATS 2023 DaySim processing pipeline."""

import logging
import os
from pathlib import Path

import polars as pl

from data_canon.models.survey import PersonDayModel
from processing.decoration import step
from processing.pipeline import Pipeline
from processing.utils.helpers import add_time_columns, expr_haversine

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

os.system(r"net use M: \\models.ad.mtc.ca.gov\data\models")  # noqa: S605, S607
os.system(r"net use X: \\model3-a\Model3A-Share")  # noqa: S605, S607

# Path to the YAML config file you provided
CONFIG_PATH = Path(__file__).parent / "config_daysim.yaml"


# ---------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)

# Optional: project-specific custom step functions
# You can define or import them here if needed
@step()
def custom_cleaning(
    # households: pl.DataFrame,
    persons: pl.DataFrame,
    days: pl.DataFrame,
    unlinked_trips: pl.DataFrame
    ) -> dict[str, pl.DataFrame]:
    """Custom cleaning steps go here, not in the main pipeline."""
    # CLEANUP UNLINKED TRIPS =================================
    # Much wow...
    unlinked_trips = unlinked_trips.rename({"arrive_second": "arrive_seconds"})

    # Add time columns if missing
    unlinked_trips = add_time_columns(unlinked_trips)

    # "Correct" trips when depart_time > arrive_time, flip them
    # including the separate hours, minutes, seconds columns
    # Create a swap condition to reuse
    swap_condition = pl.col("depart_time") > pl.col("arrive_time")
    # Swap depart/arrive columns when depart_time > arrive_time
    swap_cols = [
        ("depart_time", "arrive_time"),
        ("depart_hour", "arrive_hour"),
        ("depart_minute", "arrive_minute"),
        ("depart_seconds", "arrive_seconds"),
    ]

    unlinked_trips = unlinked_trips.with_columns(
        [
            pl.when(swap_condition).then(pl.col(b)).otherwise(pl.col(a)).alias(a)
            for a, b in swap_cols
        ] +
        [
            pl.when(swap_condition).then(pl.col(a)).otherwise(pl.col(b)).alias(b)
            for a, b in swap_cols
        ]
    )

    # Replace any -1 value in *_purpose columns with 995 missing code
    unlinked_trips = unlinked_trips.with_columns(
        [
            pl.when(pl.col(col_name) == -1)
            .then(996)
            .otherwise(pl.col(col_name))
            .alias(col_name)
            for col_name in [
                "o_purpose",
                "d_purpose",
                "o_purpose_category",
                "d_purpose_category",
            ]
        ]
    )

    # If distance is null, recalculate it from lat/lon
    unlinked_trips = unlinked_trips.with_columns(
        pl.when(pl.col("distance_meters").is_null())
            .then(
                expr_haversine(
                    pl.col("o_lon"),
                    pl.col("o_lat"),
                    pl.col("d_lon"),
                    pl.col("d_lat"),
                )
            )
            .otherwise(pl.col("distance_meters"))
            .alias("distance_meters")
    )

    # If duration_minutes is null, recalculate it from depart/arrive times
    unlinked_trips = unlinked_trips.with_columns(
        pl.when(pl.col("duration_minutes").is_null())
            .then(
                (pl.col("arrive_time") - pl.col("depart_time")).dt.total_minutes()
            )
            .otherwise(pl.col("duration_minutes"))
            .alias("duration_minutes")
    )

    # ADD DAYS FOR PERSONS WITHOUT DAYS =================================
    # Find persons without days
    persons_without_days = persons.filter(
        ~pl.col("person_id").is_in(days["person_id"].unique().implode())
    )

    # Get travel_dow from other household members' days
    days_for_dow = (
        days
        .select(["hh_id", "travel_dow"])
        .filter(pl.col("hh_id").is_in(persons_without_days["hh_id"].unique().implode()))
        .unique()
    )

    # Create a default day for each person without days
    dummy_days = (
        persons_without_days
        .join(days_for_dow, on="hh_id", how="left")
        .with_columns(
            # Construct default day_id (person_id * 100 + travel_dow)
            (pl.col("person_id") * 100 + pl.col("travel_dow")).alias("day_id")
        )
        .select(PersonDayModel.model_json_schema().get("properties").keys())
    )
    # Add dummy days to days dataframe
    days = pl.concat([days, dummy_days], how="diagonal")

    return {"unlinked_trips": unlinked_trips, "days": days}


def custom_postprocessing(
    households_daysim: pl.DataFrame,
    persons_daysim: pl.DataFrame,
    days_daysim: pl.DataFrame,
    linked_trips_daysim: pl.DataFrame,
    tours_daysim: pl.DataFrame,
    ) -> dict[str, pl.DataFrame]:
    """Custom post-processing steps go here, not in the main pipeline."""
    return {
        "households_daysim": households_daysim,
        "persons_daysim": persons_daysim,
        "days_daysim": days_daysim,
        "linked_trips_daysim": linked_trips_daysim,
        "tours_daysim": tours_daysim,
    }


custom_steps = {
    "custom_cleaning": custom_cleaning,
    "custom_postprocessing": custom_postprocessing,
}

# ---------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("Starting BATS 2023 DaySim Processing Pipeline")

    pipeline = Pipeline(config_path=CONFIG_PATH, custom_steps=custom_steps)
    result = pipeline.run()

    logger.info("Pipeline finished successfully.")
