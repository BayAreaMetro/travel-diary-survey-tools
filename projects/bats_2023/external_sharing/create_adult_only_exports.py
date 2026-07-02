"""Create adult-only BATS 2023 external sharing CSV exports.

This script joins ``age`` from ``persons_2023.csv`` onto the trip/day files in
the external sharing staging folder, keeps only records with ``age >= 4``, and
writes the filtered outputs to a sibling ``external_sharing_adult_only`` folder.

Usage:
    python create_adult_only_exports.py --input-dir "E:\path\to\external_sharing"
    python create_adult_only_exports.py --input-dir "E:\Box\Modeling and Surveys\Surveys\Travel Diary Survey\BATS_2023\External_Sharing_Staging\external_sharing"
"""

import argparse
import logging
import shutil
from datetime import datetime
from pathlib import Path

import polars as pl


logger = logging.getLogger(__name__)

TARGET_FILES = [
    "linked_trips_2023.csv",
    "days_2023.csv",
    "unlinked_trips_2023.csv",
]


def load_person_ages(input_directory: Path) -> pl.DataFrame:
    """Load the person age lookup table from persons_2023.csv."""
    persons_file = input_directory / "persons_2023.csv"
    if not persons_file.exists():
        raise FileNotFoundError(f"Missing required file: {persons_file}")

    persons = pl.read_csv(persons_file).select(["person_id", "age"])
    return persons.unique(subset=["person_id"])


def process_file(input_file: Path, output_file: Path, person_ages: pl.DataFrame) -> dict:
    """Join age, filter adults, and write the result to disk."""
    logger.info(f"Processing: {input_file.name}")
    df = pl.read_csv(input_file)
    logger.info(f"  Read {len(df):,} rows, {len(df.columns)} columns")

    joined = df.join(person_ages, on="person_id", how="left")
    adult_only = joined.filter(pl.col("age") >= 4)
    dropped_rows = len(df) - len(adult_only)

    logger.info(f"  Dropped {dropped_rows:,} rows with age < 4 or missing age")

    logger.info(f"  After join/filter: {len(adult_only):,} rows, {len(adult_only.columns)} columns")
    adult_only.write_csv(output_file)
    logger.info(f"  Wrote: {output_file}")

    return {
        "input_file": str(input_file),
        "output_file": str(output_file),
        "rows_before": len(df),
        "rows_filtered_out": dropped_rows,
        "rows_after": len(adult_only),
    }


def copy_other_files(input_directory: Path, output_directory: Path) -> list[str]:
    """Copy every non-target file from the input directory into the output directory."""
    copied_files = []
    target_names = set(TARGET_FILES)

    for source_file in sorted(input_directory.iterdir()):
        if not source_file.is_file():
            continue

        if source_file.name in target_names:
            continue

        destination_file = output_directory / source_file.name
        if destination_file.exists():
            logger.info(f"Skipped existing file: {destination_file.name}")
            continue

        try:
            shutil.copy2(source_file, destination_file)
        except PermissionError as exc:
            logger.warning(f"Could not copy {source_file.name}: {exc}")
            continue

        copied_files.append(source_file.name)
        logger.info(f"Copied: {source_file.name} -> {destination_file}")

    return copied_files


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Join age from persons_2023.csv onto trip/day files and keep adults age 4+.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        required=True,
        help="Directory containing persons_2023.csv, linked_trips_2023.csv, days_2023.csv, and unlinked_trips_2023.csv",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory for the adult-only outputs. Defaults to sibling external_sharing_adult_only.",
    )
    args = parser.parse_args()

    input_dir = args.input_dir
    output_dir = args.output_dir or input_dir.parent / "external_sharing_adult_only"
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now()
    log_file = output_dir / f"create_adult_only_exports_{timestamp.strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()],
        force=True,
    )
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    logger.info("=" * 80)
    logger.info("Create Adult-Only External Sharing Exports - BATS 2023")
    logger.info("=" * 80)
    logger.info(f"Timestamp: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Input directory: {input_dir}")
    logger.info(f"Output directory: {output_dir}")
    logger.info("Filtering to records with age >= 4")
    logger.info("")

    person_ages = load_person_ages(input_dir)
    logger.info(f"Loaded {len(person_ages):,} unique person age records")
    logger.info("")

    results = []
    for file_name in TARGET_FILES:
        input_file = input_dir / file_name
        if not input_file.exists():
            raise FileNotFoundError(f"Missing required file: {input_file}")

        output_file = output_dir / file_name
        results.append(process_file(input_file, output_file, person_ages))
        logger.info("")

    copied_files = copy_other_files(input_dir, output_dir)
    logger.info("")

    logger.info("=" * 80)
    logger.info("Processing Complete")
    logger.info("=" * 80)
    for result in results:
        logger.info(
            f"{Path(result['input_file']).name}: {result['rows_before']:,} rows -> {result['rows_after']:,} rows",
        )
        logger.info(f"  Dropped {result['rows_filtered_out']:,} rows")
    if copied_files:
        logger.info(f"Copied {len(copied_files):,} additional files: {', '.join(copied_files)}")
    logger.info(f"Log file: {log_file}")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()