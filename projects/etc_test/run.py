"""Runner script for the ETC vendor test-data pipeline."""

import argparse
import logging
import os
import shutil
from pathlib import Path

from conformity.check_vendor_data import check_etc_data
from conformity.conform_etc import conform_etc
from dotenv import load_dotenv

from data_canon.models import (
    ctramp as ctramp_models,
)
from pipeline.pipeline import Pipeline
from processing import (
    add_existing_weights,
    add_zone_ids,
    cascade_completeness,
    compute_weights,
    detect_joint_trips,
    extract_tours,
    format_ctramp,
    format_daysim,
    imputation,
    link_trips,
    load_data,
    write_data,
)

# Load environment variables from .env file
# Useful for CENSUS API keys or other sensitive information
# that should not be committed to version control.
# Make sure to create a .env file in the project root with the necessary variables.
load_dotenv()


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

logger = logging.getLogger(__name__)

# For MTC network drives that seem to keep unmapping within python VM sessions
# Check if network drives are mapped; if not, map them
drives = {
    "M:": r"\\models.ad.mtc.ca.gov\data\models",
    "X:": r"\\model3-a\Model3A-Share",
}

for drive, path in drives.items():
    if not Path(drive).exists():
        logger.info("Mapping network drive %s to %s", drive, path)
        os.system(f"net use {drive} {path}")  # noqa: S605

# Path to the YAML config file you provided
CONFIG_PATH = Path(__file__).parent / "config.yaml"


# Set up custom steps dictionary ----------------------------------
processing_steps = [
    load_data,
    check_etc_data,
    conform_etc,
    add_zone_ids,
    link_trips,
    detect_joint_trips,
    imputation,
    extract_tours,
    cascade_completeness,
    format_ctramp,
    format_daysim,
    write_data,
    add_existing_weights,
    compute_weights,
]


new_models = {
    # CT-RAMP models
    "households_ctramp": ctramp_models.HouseholdCTRAMPModel,
    "persons_ctramp": ctramp_models.PersonCTRAMPModel,
    "mandatory_locations_ctramp": ctramp_models.MandatoryLocationCTRAMPModel,
    "individual_tours_ctramp": ctramp_models.IndividualTourCTRAMPModel,
    "individual_trips_ctramp": ctramp_models.IndividualTripCTRAMPModel,
    "joint_tours_ctramp": ctramp_models.JointTourCTRAMPModel,
    "joint_trips_ctramp": ctramp_models.JointTripCTRAMPModel,
}

# ---------------------------------------------------------------------
if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="ETC vendor test-data pipeline")
    parser.add_argument(
        "--clear-cache",
        action="store_true",
        help="Clear the pipeline cache before running",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to the YAML config file (default: config.yaml next to this script)",
    )
    args = parser.parse_args()

    config_path = Path(args.config) if args.config else CONFIG_PATH

    logger.info("Starting ETC vendor test-data pipeline")
    logger.info("Using config: %s", config_path)

    cache_dir = Path(".cache/etc_test")
    pipeline = Pipeline(
        config_path=config_path,
        steps=processing_steps,
        caching=cache_dir,
        data_models=new_models,
        log_file_mode="w" if args.clear_cache else "a",
    )

    # Save a copy of the config used into the output directory
    output_dir = pipeline.config.get("output_dir")
    if output_dir:
        saved_config = Path(output_dir) / "pipeline_etc.yaml"
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        shutil.copy2(config_path, saved_config)
        logger.info("Saved config copy to %s", saved_config)

    # Clear cache if requested
    if args.clear_cache and pipeline.cache:
        pipeline.cache.clear()
        logger.info("Cleared pipeline cache at %s", cache_dir)

    result = pipeline.run()

    logger.info("Pipeline finished successfully.")
