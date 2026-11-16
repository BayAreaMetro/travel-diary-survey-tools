"""Run BATS 2019 from the editor: the same as ``surveypipe run projects/bats_2019/config.yaml``."""

from pathlib import Path

from pipeline.cli import run

if __name__ == "__main__":
    run(Path(__file__).parent / "config.yaml")
