"""Data preparation sub-package (PUMS I/O, control totals, survey seed, geography).

Orchestrates the external data pipeline that feeds the balancer:

1. **Census geography** (``census_geo``) -- download and cache TIGER PUMA
   and block shapefiles via pygris.  TABBLOCK20 files include ``POP20``
   directly from the 2020 decennial census.
2. **Geography crosswalk** (``crosswalk``) -- build a population-weighted
   allocation table from PUMAs to custom project zones using rasterised
   Census block population and ``exactextract`` fractional zonal statistics.
3. **PUMS data** (``pums_data``) -- fetch ACS 1-year PUMS microdata from the
   Census API (or load from local files).  Column chunking when >48 columns,
   Parquet caching, streaming progress bars.
4. **Control data** (``control_data``) -- recode PUMS variables, apply the
   crosswalk to distribute totals into custom zones, aggregate marginals.
5. **Seed data** (``seed_data``) -- recode canonical survey variables into
   the same bin/group categories as the PUMS controls.
"""
