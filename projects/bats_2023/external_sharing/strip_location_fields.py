"""Post-processing script to strip location fields from BATS 2023 survey data.

This script removes all fields containing '_lat' or '_lon' (case-insensitive) from
CSV files in the survey output directory to prepare data for external sharing.

Usage:
    python strip_location_fields.py
    python strip_location_fields.py --survey-dir "E:\path\to\survey"
    python strip_location_fields.py --skip-tours --skip-joint-trips
    python strip_location_fields.py --survey-dir "E:\Box\Modeling and Surveys\Surveys\Travel Diary Survey\BATS_2023\External_Sharing_Staging\survey" --skip-tours --skip-joint-trips
"""

import argparse
import logging
from datetime import datetime
from pathlib import Path

import polars as pl

# Configure logging
logger = logging.getLogger(__name__)


def contains_location_keywords(column_name: str) -> bool:
    """Check if column name contains location suffix keywords.

    Args:
        column_name: Name of the column to check

    Returns:
        True if column contains '_lat' or '_lon' (case-insensitive)
    """
    col_lower = column_name.lower()
    return "_lat" in col_lower or "_lon" in col_lower


def strip_location_fields(df: pl.DataFrame, file_name: str) -> tuple[pl.DataFrame, list[str]]:
    """Remove columns containing lat/lon from dataframe.

    Args:
        df: Input dataframe
        file_name: Name of the file being processed (for logging)

    Returns:
        Tuple of (stripped dataframe, list of removed column names)
    """
    columns_to_drop = [col for col in df.columns if contains_location_keywords(col)]

    if columns_to_drop:
        logger.info(f"  Removing {len(columns_to_drop)} location fields: {columns_to_drop}")
        df = df.drop(columns_to_drop)
    else:
        logger.info("  No location fields found")

    return df, columns_to_drop


def should_skip_file(file_stem: str, skip_files: set[str]) -> bool:
    """Check whether a file stem should be skipped.

    The skip options are defined by logical file families like ``tours`` and
    ``joint_trips``, while the actual filenames may include a suffix such as
    ``_2023``.
    """
    return any(file_stem == skip_file or file_stem.startswith(f"{skip_file}_") for skip_file in skip_files)


def process_directory(input_directory: Path, output_directory: Path, skip_files: set[str] | None = None) -> dict:
    """Process all CSV files in input directory and write results to output directory.

    Args:
        input_directory: Path to directory containing CSV files
        output_directory: Path to directory where processed files should be written
        skip_files: Set of file stems to skip

    Returns:
        Dictionary with processing results
    """
    skip_files = skip_files or set()

    if not input_directory.exists():
        logger.error(f"Directory not found: {input_directory}")
        return {}

    output_directory.mkdir(parents=True, exist_ok=True)

    csv_files = list(input_directory.glob("*.csv"))
    if not csv_files:
        logger.warning(f"No CSV files found in {input_directory}")
        return {}

    logger.info(f"Found {len(csv_files)} CSV files to process")
    logger.info(f"Output directory: {output_directory}")
    logger.info("")

    results = {}

    for csv_file in sorted(csv_files):
        if should_skip_file(csv_file.stem, skip_files):
            logger.info(f"Skipping: {csv_file.name}")
            results[csv_file.name] = {
                "status": "skipped",
                "reason": "excluded by option",
                "output_file": str(output_directory / csv_file.name),
            }
            logger.info("")
            continue

        logger.info(f"Processing: {csv_file.name}")

        try:
            df = pl.read_csv(csv_file)
            logger.info(f"  Read {len(df):,} rows, {len(df.columns)} columns")

            df_stripped, removed_fields = strip_location_fields(df, csv_file.name)

            output_file = output_directory / csv_file.name
            if output_file.exists():
                logger.warning(f"  Output already exists, skipping write: {output_file}")
                results[csv_file.name] = {
                    "status": "skipped",
                    "rows": len(df),
                    "columns_before": len(df.columns),
                    "columns_after": len(df_stripped.columns),
                    "fields_removed": removed_fields,
                    "output_file": str(output_file),
                }
            else:
                df_stripped.write_csv(output_file)
                logger.info(f"  Wrote {len(df_stripped):,} rows, {len(df_stripped.columns)} columns")
                logger.info(f"  File written: {output_file}")

                results[csv_file.name] = {
                    "status": "success",
                    "rows": len(df),
                    "columns_before": len(df.columns),
                    "columns_after": len(df_stripped.columns),
                    "fields_removed": removed_fields,
                    "output_file": str(output_file),
                }

        except Exception as e:
            logger.error(f"  Error processing {csv_file.name}: {e}")
            results[csv_file.name] = {
                "status": "error",
                "error": str(e),
            }

        logger.info("")

    return results


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Strip location fields from BATS 2023 survey CSV files.")
    parser.add_argument(
        "--survey-dir",
        type=Path,
        required=True,
        help="Directory containing the input survey CSV files",
    )
    parser.add_argument("--skip-tours", action="store_true", help="Skip tours.csv")
    parser.add_argument("--skip-joint-trips", action="store_true", help="Skip joint_trips.csv")
    args = parser.parse_args()

    survey_dir = args.survey_dir
    output_dir = survey_dir.parent / "external_sharing"
    timestamp = datetime.now()
    stamp = timestamp.strftime("%Y%m%d_%H%M%S")
    skip_files = set()

    if args.skip_tours:
        skip_files.add("tours")

    if args.skip_joint_trips:
        skip_files.add("joint_trips")

    output_dir.mkdir(parents=True, exist_ok=True)

    log_file = output_dir / f"strip_location_fields_{stamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )

    logger.info("=" * 80)
    logger.info("Strip Location Fields - BATS 2023 Survey Data")
    logger.info("=" * 80)
    logger.info(f"Timestamp: {timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Input directory: {survey_dir}")
    logger.info(f"Output directory: {output_dir}")
    logger.info("")
    logger.info("Removing all fields containing '_lat' or '_lon' (case-insensitive)")
    if skip_files:
        logger.info(f"Skipping files: {', '.join(sorted(skip_files))}")
    logger.info("")

    results = process_directory(survey_dir, output_dir, skip_files=skip_files)

    if results:
        logger.info("=" * 80)
        logger.info("Processing Complete")
        logger.info("=" * 80)

        successful = sum(1 for r in results.values() if r.get("status") == "success")
        skipped = sum(1 for r in results.values() if r.get("status") == "skipped")
        total_removed = sum(
            len(r.get("fields_removed", []))
            for r in results.values()
            if r.get("status") in {"success", "skipped"}
        )

        logger.info(f"Files written: {successful}")
        logger.info(f"Files skipped: {skipped}")
        logger.info(f"Total fields removed: {total_removed}")
        logger.info(f"Log file: {log_file}")
        logger.info("=" * 80)
    else:
        logger.warning("No files were processed")


if __name__ == "__main__":
    main()
