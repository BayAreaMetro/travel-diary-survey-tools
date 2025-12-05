"""Runner script for the BATS 2023 DaySim processing pipeline."""

import logging
from pathlib import Path

from transit_analysis import summarize_transit_trips

from pipeline.pipeline import Pipeline
from processing import link_trips, load_data, write_data
from processing.cleaning.clean_bats_2023 import clean_2023_bats

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

# os.system(r"net use M: \\models.ad.mtc.ca.gov\data\models")  # noqa: ERA001
# os.system(r"net use X: \\model3-a\Model3A-Share")  # noqa: ERA001

# Path to the YAML config file you provided
CONFIG_PATH = Path(__file__).parent / "config.yaml"

# ---------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger(__name__)


# Set up custom steps dictionary ----------------------------------
processing_steps = [
    load_data,
    clean_2023_bats,
    link_trips,
    summarize_transit_trips,
    write_data,
]


# ---------------------------------------------------------------------
if __name__ == "__main__":
    logger.info("Starting BATS 2023 DaySim Processing Pipeline")

    pipeline = Pipeline(config_path=CONFIG_PATH, steps=processing_steps)
    result = pipeline.run()

    logger.info("Pipeline finished successfully.")
