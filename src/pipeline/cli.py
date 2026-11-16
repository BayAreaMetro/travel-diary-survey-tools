"""The ``surveypipe`` command: run a project from its config file.

```
uv run surveypipe run projects/bats_2023/config.yaml
```

A project's ``run.py`` calls [`run`][pipeline.cli.run] too, so pressing run in an
editor and typing the command go through the same code.
"""

import argparse
from pathlib import Path

from dotenv import load_dotenv

from data_canon.core.dataclass import CanonicalData
from pipeline.pipeline import Pipeline


def run(config_path: str | Path) -> CanonicalData:
    """Run the pipeline a config describes.

    Loads the nearest ``.env`` at or above the config's folder first, so the
    roots it sets (``MTC_DATA``, ``CENSUS_KEY`` and so on) reach the config.

    Args:
        config_path: The project's YAML config.

    Returns:
        The tables at the end of the run.
    """
    config_path = Path(config_path).resolve()
    env = _nearest_env(config_path.parent)
    if env is not None:
        load_dotenv(env)
    return Pipeline(config_path).run()


def _nearest_env(folder: Path) -> Path | None:
    """The ``.env`` in *folder* or the closest folder above it, if any."""
    return next((d / ".env" for d in (folder, *folder.parents) if (d / ".env").is_file()), None)


def main(argv: list[str] | None = None) -> None:
    """Entry point of the ``surveypipe`` command."""
    parser = argparse.ArgumentParser(
        prog="surveypipe", description="Run a survey pipeline project from its config file."
    )
    commands = parser.add_subparsers(dest="command", required=True)
    run_cmd = commands.add_parser("run", help="Run the pipeline a config describes.")
    run_cmd.add_argument("config", type=Path, help="Path to the project's YAML config.")
    args = parser.parse_args(argv)
    if args.command == "run":
        run(args.config)
