"""Census TIGER geography loading.

Uses `pygris <https://github.com/walkerke/pygris>`_ to download and cache
PUMA and block shapefiles from the Census Bureau's TIGER/Line server.

Block shapefiles (TABBLOCK20) include ``POP20`` and ``HOUSING20`` columns
from the 2020 decennial census -- no separate population table is needed.
"""

import logging

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
# Public API
# ---------------------------------------------------------------------------
def get_puma_gdf(
    state_fips: str,
    pums_year: int,
) -> gpd.GeoDataFrame:
    """Download PUMA polygons for *state_fips* via pygris.

    Returns a GeoDataFrame with ``puma_id`` (str) and ``geometry``.
    """
    vintage = puma_vintage_for_pums_year(pums_year)
    tiger_year = _PUMA_TIGER_YEAR[vintage]
    logger.info(
        "Downloading %d-vintage PUMAs for state %s",
        vintage,
        state_fips,
    )
    gdf = pygris.pumas(state=state_fips, year=tiger_year, cache=True)
    id_col = "PUMACE20" if vintage == 2020 else "PUMACE10"  # noqa: PLR2004
    return (
        gdf[[id_col, "geometry"]]
        .rename(columns={id_col: "puma_id"})
        .assign(puma_id=lambda df: df["puma_id"].astype(str))
    )


def get_block_gdf(state_fips: str, pums_year: int) -> gpd.GeoDataFrame:
    """Download 2020-vintage Census block polygons for *state_fips* via pygris.

    Returns a GeoDataFrame with ``block_id`` (str), ``pop20`` (int),
    and ``geometry``.
    """
    vintage = puma_vintage_for_pums_year(pums_year)
    tiger_year = _PUMA_TIGER_YEAR[vintage]
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
