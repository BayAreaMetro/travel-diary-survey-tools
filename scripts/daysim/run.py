"""Runner script for the BATS 2023 DaySim processing pipeline."""

import logging
import os
from pathlib import Path

import polars as pl

from processing.pipeline.decoration import step
from processing.pipeline.pipeline import Pipeline
from processing.utils.helpers import add_time_columns

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
@step(validate_input=False, validate_output=True)
def custom_cleaning(unlinked_trips: pl.DataFrame) -> dict[str, pl.DataFrame]:
    """Custom cleaning steps go here, not in the main pipeline."""
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
    return {"unlinked_trips": unlinked_trips}

custom_steps = {
    "custom_cleaning": custom_cleaning,
}

# ---------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("Starting BATS 2023 DaySim Processing Pipeline")

    pipeline = Pipeline(config_path=CONFIG_PATH, custom_steps=custom_steps)
    result = pipeline.run()

    logger.info("Pipeline finished successfully.")
