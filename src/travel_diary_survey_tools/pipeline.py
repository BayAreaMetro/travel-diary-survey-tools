"""Simple pipeline runner for travel diary survey processing."""

import logging
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from travel_diary_survey_tools import DaysimFormatter, TourBuilder, link_trips

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Pipeline:
    """Travel diary survey processing pipeline."""

    def __init__(self, config_path: str | Path) -> None:
        """Initialize pipeline with configuration.

        Args:
            config_path: Path to YAML config file

        """
        self.config_path = Path(config_path)
        self.config = self._load_config()

        # Canonical tables - always present
        self.household: pl.DataFrame | None = None
        self.person: pl.DataFrame | None = None
        self.day: pl.DataFrame | None = None
        self.unlinked_trips: pl.DataFrame | None = None
        self.linked_trips: pl.DataFrame | None = None
        self.tours: pl.DataFrame | None = None

        # Map step names to methods
        self.steps = {
            "cleaning": self.load_data,
            "linking": self.link_trips,
            "tour_building": self.build_tours,
            "daysim_formatting": self.format_daysim,
            "formatting": self.save_outputs,
        }

    def _load_config(self) -> dict[str, Any]:
        """Load and parse YAML config file with shorthand replacement."""
        with self.config_path.open() as f:
            config = yaml.safe_load(f)

        variables = {
            k: v
            for k, v in config.items()
            if k != "pipeline" and isinstance(v, str)
        }

        def expand_dict(d: dict) -> None:
            for key, value in d.items():
                if isinstance(value, str):
                    expanded_value = value
                    for var_name, var_value in variables.items():
                        expanded_value = expanded_value.replace(
                            f"{{{{ {var_name} }}}}", var_value
                        )
                    d[key] = expanded_value
                elif isinstance(value, dict):
                    expand_dict(value)
                elif isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            expand_dict(item)

        return expand_dict(config)

    def run(self) -> None:
        """Run pipeline steps defined in config.yaml."""
        for step_config in self.config["pipeline"]["steps"]:
            step_name = step_config["name"]
            logger.info("Running step: %s", step_name)

            step_func = self.steps[step_name]
            step_func(step_config)

            # Check if outputs required for step
            if "outputs" in step_config:
                self.save_outputs(step_config)

            logger.info("Completed step: %s", step_name)

        logger.info("Pipeline completed!")

    def link_trips(self, step_config: dict[str, Any]) -> None:
        """Link trips based on mode changes and dwell times.

        Args:
            step_config: Step configuration from YAML

        """
        # Load raw trips
        unlinked_trip_path = step_config["input"]["unlinked_trip"]
        self.unlinked_trips = pl.read_csv(unlinked_trip_path)

        # Link trips using travel_diary_survey_tools
        self.unlinked_trips, self.linked_trips = link_trips.link_trips(
            self.unlinked_trips
        )

    def build_tours(self, step_config: dict[str, Any]) -> None:
        """Build tour structures from linked trips.

        Args:
            step_config: Step configuration from YAML

        """
        # Load persons data
        person_path = step_config["input"]["person"]
        persons = pl.read_csv(person_path)

        # Build tours
        builder = TourBuilder(persons, step_config.get("parameters", {}))
        self.linked_trips, tours = builder.build_tours(self.linked_trips)

        # Store tours as attribute
        self.tours = tours

    def format_daysim(self, step_config: dict[str, Any]) -> None:
        """Format data to DaySim model specification.

        Args:
            step_config: Step configuration from YAML

        """
        logger.info("Formatting data to DaySim specification")

        # Initialize formatter
        formatter = DaysimFormatter(step_config.get("parameters", {}))

        # Load day completeness if path provided
        day_completeness = None
        if "day_completeness_path" in step_config.get("inputs", {}):
            day_path = step_config["inputs"]["day_completeness_path"]
            logger.info("Loading day completeness from: %s", day_path)
            day_completeness = formatter.load_day_completeness(day_path)

        # Format each table
        if self.person is not None:
            logger.info("Formatting person data")
            self.person = formatter.format_person(self.person, day_completeness)

        if self.household is not None:
            logger.info("Formatting household data")
            self.household = formatter.format_household(
                self.household, self.person
            )

        if self.unlinked_trips is not None:
            logger.info("Formatting trip data")
            self.unlinked_trips = formatter.format_trip(self.unlinked_trips)

        logger.info("DaySim formatting completed")

    def save_outputs(self, step_config: dict[str, Any]) -> None:
        """Save processed data to output files.

        Args:
            step_config: Step configuration from YAML

        """
        for output_name, output_path in step_config["outputs"].items():
            if hasattr(self, output_name):
                df = getattr(self, output_name)
                if df is not None:
                    df.write_csv(output_path)
                    logger.info(
                        "Saved output %s to %s",
                        output_name,
                        output_path,
                    )
                else:
                    logger.warning(
                        "Output %s is None, not saving to %s",
                        output_name,
                        output_path,
                    )
            else:
                logger.warning(
                    "No attribute %s found for output, skipping save to %s",
                    output_name,
                    output_path,
                )
