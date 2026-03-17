"""Census TIGER geography loading.

Uses `pygris <https://github.com/walkerke/pygris>`_ to download and cache
PUMA and block shapefiles from the Census Bureau's TIGER/Line server.

Block population comes from the **decennial census** TABBLOCK product
(``POP20`` from 2020 decennial, ``POP10`` from 2010 decennial).  Only
decennial TIGER vintages include these population columns — regular
annual files do not.  Column names are normalised to ``block_pop`` /
``puma_id`` so downstream code is vintage-agnostic.

Processed GeoDataFrames are cached as GeoParquet under
``<pipeline-cache-dir>/census_geo/`` for fast repeat loads.
"""

import logging
from collections.abc import Callable
from pathlib import Path

import geopandas as gpd
import pygris

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Vintage helpers
# ---------------------------------------------------------------------------


def puma_vintage_for_pums_year(pums_year: int) -> int:
    """Return the PUMA geography vintage for a given PUMS year.

    ACS PUMS 2022+ uses 2020 PUMAs; 2012-2021 uses 2010 PUMAs.
    """
    if pums_year >= 2022:  # noqa: PLR2004
        return 2020
    if pums_year >= 2012:  # noqa: PLR2004
        return 2010
    msg = f"PUMS year {pums_year} is before 2012; not supported."
    raise ValueError(msg)


def _find_column(gdf: gpd.GeoDataFrame, *prefixes: str) -> str:
    """Find the first column matching ``{prefix}20`` or ``{prefix}10``.

    Raises ``ValueError`` if none of the candidates exist.
    """
    cols = set(gdf.columns)
    for suffix in ("20", "10"):
        for prefix in prefixes:
            candidate = f"{prefix}{suffix}"
            if candidate in cols:
                return candidate
    msg = (
        f"Cannot find column {'/'.join(prefixes)}[20|10] in shapefile. "
        f"Available: {sorted(cols)}"
    )
    raise ValueError(msg)


# ---------------------------------------------------------------------------
# GeoParquet cache
# ---------------------------------------------------------------------------


def _cached_geoparquet(
    key: str,
    builder: Callable[[], gpd.GeoDataFrame],
    cache_dir: Path | None = None,
) -> gpd.GeoDataFrame:
    """Return a GeoDataFrame, building and caching as GeoParquet if needed."""
    if cache_dir is None:
        return builder()
    path = cache_dir / "census_geo" / f"{key}.parquet"
    if path.exists():
        logger.debug("Cache hit: %s", path)
        return gpd.read_parquet(path)
    gdf = builder()
    path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(path)
    logger.debug("Cached → %s", path)
    return gdf


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_puma_gdf(
    state_fips: str,
    pums_year: int,
    *,
    cache_dir: Path | None = None,
) -> gpd.GeoDataFrame:
    """Download PUMA polygons for *state_fips* via pygris.

    Requests the TIGER file for *pums_year* directly — each vintage
    carries the matching PUMA IDs (``PUMACE10`` or ``PUMACE20``).

    Returns a GeoDataFrame with ``puma_id`` (str) and ``geometry``.
    """
    vintage = puma_vintage_for_pums_year(pums_year)

    def _build() -> gpd.GeoDataFrame:
        logger.info("Downloading %d-vintage PUMAs (TIGER %d) for state %s",
                     vintage, pums_year, state_fips)
        gdf = pygris.pumas(state=state_fips, year=pums_year, cache=True)
        id_col = _find_column(gdf, "PUMACE")
        return (
            gdf[[id_col, "geometry"]]
            .rename(columns={id_col: "puma_id"})
            .assign(puma_id=lambda df: df["puma_id"].astype(str))
        )

    return _cached_geoparquet(f"puma_{state_fips}_{vintage}", _build, cache_dir)


def get_block_gdf(
    state_fips: str,
    pums_year: int,
    *,
    cache_dir: Path | None = None,
) -> gpd.GeoDataFrame:
    """Download Census block polygons with population for *state_fips*.

    Uses the decennial TABBLOCK product matching the PUMA vintage
    (year=2010 for pre-2022 PUMS, year=2020 for 2022+) — the only
    vintages that include population columns.

    Returns a GeoDataFrame with ``block_id`` (str), ``block_pop`` (int),
    and ``geometry``.
    """
    vintage = puma_vintage_for_pums_year(pums_year)

    def _build() -> gpd.GeoDataFrame:
        logger.info("Downloading decennial %d blocks for state %s", vintage, state_fips)
        gdf = pygris.blocks(state=state_fips, year=vintage, cache=True)
        geoid_col = _find_column(gdf, "GEOID")
        pop_col = _find_column(gdf, "POP")
        return (
            gdf[[geoid_col, pop_col, "geometry"]]
            .rename(columns={geoid_col: "block_id", pop_col: "block_pop"})
            .assign(
                block_id=lambda df: df["block_id"].astype(str),
                block_pop=lambda df: df["block_pop"].astype(int),
            )
        )

    return _cached_geoparquet(f"block_{state_fips}_{vintage}", _build, cache_dir)
