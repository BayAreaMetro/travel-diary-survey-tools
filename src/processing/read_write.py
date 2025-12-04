"""Loads all canonical tables from input paths."""

import logging

import geopandas as gpd
import polars as pl

from pipeline.decoration import step

logger = logging.getLogger(__name__)


@step()
def load_data(
    input_paths: dict[str, str],
) -> dict[str, pl.DataFrame | gpd.GeoDataFrame]:
    """Load all canonical tables from input paths."""
    data = {}

    for table, path in input_paths.items():
        logger.info("Loading %s...", table)

        # If .csv file, use polars to read
        if path.endswith(".csv"):
            data[table] = pl.read_csv(path)
        elif path.endswith(".parquet"):
            data[table] = pl.read_parquet(path)
        elif path.endswith((".shp", ".shp.zip")):
            data[table] = gpd.read_file(path)
        else:
            msg = f"Unsupported file format for table {table}: {path}"
            raise ValueError(msg)

    logger.info("All data loaded successfully.")
    return data


@step()
def write_data(
    output_paths: dict[str, str],
    canonical_data: dict[str, pl.DataFrame | gpd.GeoDataFrame],
) -> None:
    """Write all canonical tables to output paths."""
    for table, path in output_paths.items():
        logger.info("Writing %s to %s...", table, path)

        df = getattr(canonical_data, table)

        # If .csv file, use polars to write
        if path.endswith(".csv"):
            df.write_csv(path)
        elif path.endswith(".parquet"):
            df.write_parquet(path)
        elif path.endswith((".shp", ".shp.zip")):
            if not isinstance(df, gpd.GeoDataFrame):
                msg = f"Expected GeoDataFrame for table {table}, got {type(df)}"
                raise ValueError(msg)
            df.to_file(path)
        else:
            msg = f"Unsupported file format for table {table}: {path}"
            raise ValueError(msg)

    logger.info("All data written successfully.")
