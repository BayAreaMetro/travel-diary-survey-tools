"""PUMS microdata I/O.

Downloads ACS PUMS 1-year microdata directly from the Census Bureau API or
loads from local CSV / Parquet files.  Handles type-casting of Census API
string responses to proper numeric dtypes.

Transformation (recoding, aggregation) lives in ``control_data``.
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import requests
from tqdm import tqdm

from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import pums_variables

logger = logging.getLogger(__name__)

# Infrastructure vars that don't come from any ControlTarget
_HH_INFRA = {"SERIALNO", "PUMA", "STATE", "WGTP", "TYPEHUGQ"}
_PERSON_INFRA = {"SERIALNO", "SPORDER", "PUMA", "STATE", "PWGTP"}

# Replicate weight columns for variance estimation (80 per table)
_HH_REPLICATE_WEIGHTS = {f"WGTP{i}" for i in range(1, 81)}
_PERSON_REPLICATE_WEIGHTS = {f"PWGTP{i}" for i in range(1, 81)}

# Derived dynamically from the registry + infrastructure
_HH_VARS = _HH_INFRA | pums_variables(ControlLevel.HOUSEHOLD)
_PERSON_VARS = _PERSON_INFRA | pums_variables(ControlLevel.PERSON)

# Census API settings
_CENSUS_BASE = "https://api.census.gov/data"
_MAX_COLS_PER_REQUEST = 48  # Census API caps ~50 variables per GET
_MAX_API_WORKERS = 4


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
    load_replicate_weights: bool = False,
    cache_dir: Path | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Download PUMS household and person microdata from the Census API.

    Parameters
    ----------
    source : PUMSSource
        State, year, and optional PUMA filter.
    extra_hh_vars, extra_person_vars : set[str] | None
        Additional PUMS variable names to fetch beyond the defaults.
    load_replicate_weights : bool
        If ``True``, also fetch the 80 replicate weight columns per table
        (``WGTP1``-``WGTP80`` and ``PWGTP1``-``PWGTP80``).  Required for
        MOE-based importance calculation.
    cache_dir : Path | None
        If set, raw PUMS data is cached as parquet files under
        ``cache_dir/pums/``.  Subsequent calls with the same state/year
        load from cache instead of hitting the API.

    Returns:
    -------
    (households, persons) : tuple[pl.DataFrame, pl.DataFrame]
        Polars DataFrames with PUMS data, typed to appropriate dtypes.
    """
    hh_extra = extra_hh_vars or set()
    person_extra = extra_person_vars or set()
    if load_replicate_weights:
        hh_extra = hh_extra | _HH_REPLICATE_WEIGHTS
        person_extra = person_extra | _PERSON_REPLICATE_WEIGHTS
    hh_vars = sorted(_HH_VARS | hh_extra)
    person_vars = sorted(_PERSON_VARS | person_extra)

    # Check cache first
    if cache_dir is not None:
        pums_dir = cache_dir / "pums"
        tag = f"{source.state_fips}_{source.pums_year}"
        hh_cache = pums_dir / f"{tag}_hh.parquet"
        per_cache = pums_dir / f"{tag}_person.parquet"
        if hh_cache.exists() and per_cache.exists():
            hh_df = pl.read_parquet(hh_cache)
            person_df = pl.read_parquet(per_cache)
            logger.info(
                "Loaded PUMS from cache (%d HH, %d persons)",
                len(hh_df),
                len(person_df),
            )
            return hh_df, person_df

    dataset_name = f"ACSPUMS1Y{source.pums_year}"
    base_url = f"{_CENSUS_BASE}/{source.pums_year}/acs/acs1/pums"
    puma_geo = ",".join(source.puma_ids) if source.puma_ids else "*"
    if source.puma_ids and len(puma_geo) >= _MAX_COLS_PER_REQUEST:
        puma_label = f"{len(source.puma_ids)} PUMAs"
    else:
        puma_label = puma_geo
    logger.info(
        "Fetching PUMS from Census API: %s (PUMAs: %s)",
        dataset_name,
        puma_label,
    )

    hh_df = _fetch_table(base_url, hh_vars, source.state_fips, puma_geo, label="households")
    person_df = _fetch_table(base_url, person_vars, source.state_fips, puma_geo, label="persons")

    # Cast types
    hh_df = _cast_pums_types(hh_df, _HH_VARS | hh_extra)
    person_df = _cast_pums_types(person_df, _PERSON_VARS | person_extra)

    # Filter to housing units only (TYPEHUGQ == 1)
    if "TYPEHUGQ" in hh_df.columns:
        hh_df = hh_df.filter(pl.col("TYPEHUGQ") == 1)

    # Save to cache
    if cache_dir is not None:
        hh_cache.parent.mkdir(parents=True, exist_ok=True)
        hh_df.write_parquet(hh_cache)
        person_df.write_parquet(per_cache)
        logger.info("Cached PUMS data to %s", hh_cache.parent)

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
    load_replicate_weights: bool = False,
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
    load_replicate_weights : bool
        If True, retain WGTP1-80 and PWGTP1-80 replicate weight columns.

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

    # Determine expected vars for type casting
    hh_known = set(hh_df.columns) & _HH_VARS
    person_known = set(person_df.columns) & _PERSON_VARS
    if load_replicate_weights:
        hh_known |= set(hh_df.columns) & _HH_REPLICATE_WEIGHTS
        person_known |= set(person_df.columns) & _PERSON_REPLICATE_WEIGHTS

    # Cast types
    hh_df = _cast_pums_types(hh_df, hh_known)
    person_df = _cast_pums_types(person_df, person_known)

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


def _census_get(
    base_url: str,
    cols: list[str],
    state_fips: str,
    puma_geo: str,
    *,
    label: str = "",
) -> list[list[str]]:
    """Execute a single Census API GET and return the JSON rows.

    Streams the response with a ``tqdm`` progress bar when *label* is
    provided.  Raises ``RuntimeError`` on HTTP or API errors.
    """
    params = {
        "get": ",".join(cols),
        "for": f"public use microdata area:{puma_geo}",
        "in": f"state:{state_fips}",
    }
    resp = requests.get(base_url, params=params, timeout=120, stream=True)
    resp.raise_for_status()

    total = int(resp.headers.get("content-length", 0))
    chunks: list[bytes] = []
    with tqdm(
        total=total or None,
        unit="B",
        unit_scale=True,
        desc=label or "Census API",
        leave=False,
    ) as bar:
        for chunk in resp.iter_content(chunk_size=64 * 1024):
            chunks.append(chunk)
            bar.update(len(chunk))

    data = json.loads(b"".join(chunks))
    if isinstance(data, dict) and "error" in data:
        msg = f"Census API error: {data['error']}"
        raise RuntimeError(msg)
    return data


def _json_to_polars(rows: list[list[str]]) -> pl.DataFrame:
    """Convert Census API JSON (header + data rows) to a Polars DataFrame."""
    header = rows[0]
    return pl.DataFrame(
        {col: [row[i] for row in rows[1:]] for i, col in enumerate(header)},
        schema=dict.fromkeys(header, pl.Utf8),
    )


def _fetch_table(
    base_url: str,
    all_cols: list[str],
    state_fips: str,
    puma_geo: str,
    *,
    label: str = "table",
) -> pl.DataFrame:
    """Fetch a full PUMS table, chunking columns if needed.

    The Census API limits ~50 variables per request.  When *all_cols*
    exceeds that, we split into chunks (each including ``SERIALNO`` as a
    join key) and fetch them in parallel, then join horizontally.
    """
    join_key = "SERIALNO"

    if len(all_cols) <= _MAX_COLS_PER_REQUEST:
        rows = _census_get(base_url, all_cols, state_fips, puma_geo, label=label)
        df = _json_to_polars(rows)
        logger.info("  %s: %d rows x %d cols", label, len(df), len(df.columns))
        return df

    # Split into chunks, each including the join key
    non_key = [c for c in all_cols if c != join_key]
    chunks: list[list[str]] = []
    for i in range(0, len(non_key), _MAX_COLS_PER_REQUEST - 1):
        chunk = [join_key, *non_key[i : i + _MAX_COLS_PER_REQUEST - 1]]
        chunks.append(chunk)

    n_chunks = len(chunks)
    logger.info(
        "  %s: %d cols across %d requests",
        label,
        len(all_cols),
        n_chunks,
    )

    # Fetch chunks in parallel
    parts: list[pl.DataFrame] = [None] * n_chunks  # type: ignore[list-item]
    with ThreadPoolExecutor(max_workers=min(_MAX_API_WORKERS, n_chunks)) as pool:
        futures = {
            pool.submit(
                _census_get,
                base_url,
                chunk,
                state_fips,
                puma_geo,
                label=f"{label} [{i + 1}/{n_chunks}]",
            ): i
            for i, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            idx = futures[future]
            parts[idx] = _json_to_polars(future.result())

    # Join chunks on SERIALNO (first chunk is base, others add columns)
    result = parts[0]
    for part in parts[1:]:
        new_cols = [c for c in part.columns if c not in result.columns]
        result = result.hstack(part.select(new_cols))

    logger.info("  %s: %d rows x %d cols", label, len(result), len(result.columns))
    return result


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
