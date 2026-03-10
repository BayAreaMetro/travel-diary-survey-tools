"""PUMS microdata I/O.

Downloads ACS PUMS 1-year microdata via the Census API (cenpy) or loads
from local CSV / Parquet files.  Handles type-casting of Census API string
responses to proper numeric dtypes.

Transformation (recoding, aggregation) lives in ``control_data``.
"""

import logging
from dataclasses import dataclass

import cenpy
import polars as pl

from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import pums_variables

logger = logging.getLogger(__name__)

# Infrastructure vars that don't come from any ControlTarget
_HH_INFRA = {"SERIALNO", "PUMA", "STATE", "WGTP", "TYPEHUGQ"}
_PERSON_INFRA = {"SERIALNO", "SPORDER", "PUMA", "STATE", "PWGTP"}

# Derived dynamically from the registry + infrastructure
_HH_VARS = _HH_INFRA | pums_variables(ControlLevel.HOUSEHOLD)
_PERSON_VARS = _PERSON_INFRA | pums_variables(ControlLevel.PERSON)


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class PUMSSource:
    """Configuration for PUMS data source.

    Parameters
    ----------
    state_fips : str
        Two-digit FIPS code for the state (e.g. "06" for California).
    pums_year : int
        ACS 1-year PUMS vintage (e.g. 2022).
    puma_ids : list[str] | None
        Optional list of PUMA codes to fetch. If None, fetches all PUMAs in
        the state (can be large).
    """

    state_fips: str
    pums_year: int
    puma_ids: list[str] | None = None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def fetch_pums_data(
    source: PUMSSource,
    extra_hh_vars: set[str] | None = None,
    extra_person_vars: set[str] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Download PUMS household and person microdata from the Census API.

    Parameters
    ----------
    source : PUMSSource
        State, year, and optional PUMA filter.
    extra_hh_vars, extra_person_vars : set[str] | None
        Additional PUMS variable names to fetch beyond the defaults.

    Returns:
    -------
    (households, persons) : tuple[pl.DataFrame, pl.DataFrame]
        Polars DataFrames with PUMS data, typed to appropriate dtypes.
    """
    hh_vars = sorted(_HH_VARS | (extra_hh_vars or set()))
    person_vars = sorted(_PERSON_VARS | (extra_person_vars or set()))

    dataset_name = f"ACSPUMS1Y{source.pums_year}"
    logger.info("Connecting to Census API: %s", dataset_name)
    conn = cenpy.remote.APIConnection(dataset_name)

    # Build geography query
    if source.puma_ids is not None:
        puma_str = ",".join(source.puma_ids)
        geo_unit = f"public use microdata area:{puma_str}"
    else:
        geo_unit = "public use microdata area:*"
    geo_filter = {"state": source.state_fips}

    # Format PUMA list for logging
    if source.puma_ids is None:
        puma_display = "all"
    elif len(source.puma_ids) <= 5:  # noqa: PLR2004
        puma_display = str(source.puma_ids)
    else:
        puma_display = f"{len(source.puma_ids)} pumas"

    logger.info(
        "Fetching household PUMS (%d variables, state=%s, pumas=%s)",
        len(hh_vars),
        source.state_fips,
        puma_display,
    )
    hh_pd = conn.query(cols=hh_vars, geo_unit=geo_unit, geo_filter=geo_filter)

    logger.info(
        "Fetching person PUMS (%d variables, state=%s, pumas=%s)",
        len(person_vars),
        source.state_fips,
        puma_display,
    )
    person_pd = conn.query(cols=person_vars, geo_unit=geo_unit, geo_filter=geo_filter)

    # Convert to polars and cast types
    hh_df = _cast_pums_types(pl.from_pandas(hh_pd), _HH_VARS | (extra_hh_vars or set()))
    person_df = _cast_pums_types(
        pl.from_pandas(person_pd), _PERSON_VARS | (extra_person_vars or set())
    )

    # Filter to housing units only (TYPEHUGQ == 1)
    if "TYPEHUGQ" in hh_df.columns:
        hh_df = hh_df.filter(pl.col("TYPEHUGQ") == 1)

    logger.info(
        "Fetched %d household records, %d person records",
        len(hh_df),
        len(person_df),
    )
    return hh_df, person_df


def load_pums_from_files(
    hh_path: str,
    person_path: str,
    state_fips: str | None = None,
    puma_ids: list[str] | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Load PUMS data from local CSV/Parquet files.

    Parameters
    ----------
    hh_path : str
        Path to household PUMS file.
    person_path : str
        Path to person PUMS file.
    state_fips : str | None
        Optional filter to a specific state.
    puma_ids : list[str] | None
        Optional filter to specific PUMAs.

    Returns:
    -------
    (households, persons) : tuple[pl.DataFrame, pl.DataFrame]
    """
    hh_ext = hh_path.rsplit(".", 1)[-1].lower()
    person_ext = person_path.rsplit(".", 1)[-1].lower()

    if hh_ext == "parquet":
        hh_df = pl.read_parquet(hh_path)
    else:
        hh_df = pl.read_csv(hh_path, infer_schema_length=10_000)

    if person_ext == "parquet":
        person_df = pl.read_parquet(person_path)
    else:
        person_df = pl.read_csv(person_path, infer_schema_length=10_000)

    # Cast types
    hh_df = _cast_pums_types(hh_df, set(hh_df.columns) & _HH_VARS)
    person_df = _cast_pums_types(person_df, set(person_df.columns) & _PERSON_VARS)

    # Filter
    if "TYPEHUGQ" in hh_df.columns:
        hh_df = hh_df.filter(pl.col("TYPEHUGQ") == 1)

    if state_fips is not None and "ST" in hh_df.columns:
        hh_df = hh_df.filter(pl.col("ST") == state_fips)
        person_df = person_df.filter(pl.col("ST") == state_fips)

    if puma_ids is not None and "PUMA" in hh_df.columns:
        hh_df = hh_df.filter(pl.col("PUMA").is_in(puma_ids))
        person_df = person_df.filter(pl.col("PUMA").is_in(puma_ids))

    logger.info(
        "Loaded %d household records, %d person records from files",
        len(hh_df),
        len(person_df),
    )
    return hh_df, person_df


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------
def _cast_pums_types(df: pl.DataFrame, expected_vars: set[str]) -> pl.DataFrame:
    """Cast PUMS columns from string (Census API) to numeric types."""
    # String ID columns that should stay as strings
    string_cols = {"SERIALNO", "PUMA", "ST"}

    numeric_casts = []
    for col_name in df.columns:
        if col_name in string_cols:
            numeric_casts.append(pl.col(col_name).cast(pl.Utf8))
        elif col_name in expected_vars:
            # Try int first, fall back to float for income-like fields
            if col_name in ("HINCP", "PWGTP", "WGTP", "WKHP"):
                numeric_casts.append(
                    pl.col(col_name).cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False)
                )
            else:
                numeric_casts.append(
                    pl.col(col_name).cast(pl.Utf8).str.strip_chars().cast(pl.Int32, strict=False)
                )

    if numeric_casts:
        df = df.with_columns(numeric_casts)
    return df
