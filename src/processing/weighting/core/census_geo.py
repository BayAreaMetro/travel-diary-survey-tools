"""Census TIGER geography loading.

Uses `pygris <https://github.com/walkerke/pygris>`_ to download and cache
PUMA and block shapefiles from the Census Bureau's TIGER/Line server.

Block shapefiles (TABBLOCK20) include ``POP20`` and ``HOUSING20`` columns
from the 2020 decennial census -- no separate population table is needed.

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
# PUMA vintage helpers
# ---------------------------------------------------------------------------

# PUMA vintage -> TIGER year that pygris should request.
_PUMA_TIGER_YEAR: dict[int, int] = {2020: 2023, 2010: 2021}


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


# ---------------------------------------------------------------------------
# GeoParquet cache
# ---------------------------------------------------------------------------
def _cached_geoparquet(
    key: str,
    builder: Callable[[], gpd.GeoDataFrame],
    cache_dir: Path | None = None,
) -> gpd.GeoDataFrame:
    """Return a GeoDataFrame from the parquet cache, building it if absent.

    When *cache_dir* is ``None`` the builder is called every time (no
    caching).  Otherwise the result is persisted as GeoParquet under
    ``<cache_dir>/census_geo/<key>.parquet``.
    """
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

    Returns a GeoDataFrame with ``puma_id`` (str) and ``geometry``.
    """
    vintage = puma_vintage_for_pums_year(pums_year)
    tiger_year = _PUMA_TIGER_YEAR[vintage]

    def _build() -> gpd.GeoDataFrame:
        logger.info("Downloading %d-vintage PUMAs for state %s", vintage, state_fips)
        gdf = pygris.pumas(state=state_fips, year=tiger_year, cache=True)
        id_col = "PUMACE20" if vintage == 2020 else "PUMACE10"  # noqa: PLR2004
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
    """Download Census block polygons for *state_fips* via pygris.

    Returns a GeoDataFrame with ``block_id`` (str), ``pop20`` (int),
    and ``geometry``.
    """
    vintage = puma_vintage_for_pums_year(pums_year)
    tiger_year = _PUMA_TIGER_YEAR[vintage]

    def _build() -> gpd.GeoDataFrame:
        logger.info("Downloading %d-vintage blocks for state %s", vintage, state_fips)
        gdf = pygris.blocks(state=state_fips, year=tiger_year, cache=True)
        return (
            gdf[["GEOID20", "POP20", "geometry"]]
            .rename(columns={"GEOID20": "block_id", "POP20": "pop20"})
            .assign(
                block_id=lambda df: df["block_id"].astype(str),
                pop20=lambda df: df["pop20"].astype(int),
            )
        )

    return _cached_geoparquet(f"block_{state_fips}_{vintage}", _build, cache_dir)
