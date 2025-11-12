"""Main entry point for running the processing pipeline."""

import argparse
from pathlib import Path

from travel_diary_survey_tools.pipeline import Pipeline


def main() -> None:
    """Run the travel diary processing pipeline.

    This function serves as the command-line interface entry point for
    processing travel diary survey data. It accepts a configuration file path
    as a command-line argument and executes the processing pipeline.

    Usage:
        python run.py <config_path>
        Where <config_path> is the path to a YAML configuration file.

    Example:
        python run.py config/processing_config.yaml

    Args:
        None (arguments are parsed from command line)

    Raises:
        FileNotFoundError: If the specified configuration file does not exist.
    Command-line Arguments:
        config (str): Path to the YAML configuration file that defines the
                      processing pipeline parameters.
    """
    parser = argparse.ArgumentParser(
        description="Process travel diary survey data"
    )
    parser.add_argument(
        "config",
        type=str,
        help="Path to YAML configuration file",
    )

    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        msg = f"Config file not found: {config_path}"
        raise FileNotFoundError(msg)

    pipeline = Pipeline(config_path)
    pipeline.run()


if __name__ == "__main__":
    main()
