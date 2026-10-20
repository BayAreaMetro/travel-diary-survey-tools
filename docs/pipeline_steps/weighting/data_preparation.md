# Data Preparation

::: processing.weighting.data_prep
    options:
      show_root_heading: true
      show_docstring_description: true
      members: false

::: processing.weighting.data_prep.crosswalk
    options:
      show_root_heading: true
      members:
        - TargetZoneConfig
        - GeographyConfig

<!-- Its own directive so its methods show: the site-wide filter hides lowercase names. -->
::: processing.weighting.data_prep.crosswalk.PumaCrosswalk
    options:
      show_root_heading: true
      filters:
        - "!^_"

::: processing.weighting.data_prep.census_geo
    options:
      show_root_heading: true
      members:
        - puma_vintage_for_pums_year
        - get_puma_gdf
        - get_block_gdf

::: processing.weighting.data_prep.pums_data
    options:
      show_root_heading: true
      members:
        - fetch_pums_data
        - load_pums_from_files

::: processing.weighting.core.specs
    options:
      show_root_heading: true
      members:
        - PUMSSource

::: processing.weighting.data_prep.control_data
    options:
      show_root_heading: true
      members:
        - ControlSpec
        - ControlTotals
        - recode_pums_households
        - recode_pums_persons
        - build_control_totals
        - apply_zone_groups

::: processing.weighting.data_prep.seed_data
    options:
      show_root_heading: true
      members:
        - recode_survey_households
        - recode_survey_persons

::: processing.weighting.data_prep.incidence
    options:
      show_root_heading: true
      members:
        - build_incidence_table
