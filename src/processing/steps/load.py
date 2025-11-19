"""Loads all canonical tables from input paths."""

import logging

import geopandas as gpd
import polars as pl

from processing.decoration import step

logger = logging.getLogger(__name__)

@step(validate=False)
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
