# Weighting

::: processing.weighting
    options:
      show_root_heading: true
      show_docstring_description: true
      members: false

## Pipeline Steps

::: processing.weighting.weighting
    options:
      show_root_heading: true
      members:
        - weighting

::: processing.weighting.existing_weights
    options:
      show_root_heading: true
      members:
        - ExistingWeightConfig
        - add_existing_weights

## Data Preparation

::: processing.weighting.data_prep
    options:
      show_root_heading: true
      show_docstring_description: true
      members: false

::: processing.weighting.data_prep.crosswalk
    options:
      show_root_heading: true
      members:
        - PumaCrosswalk

::: processing.weighting.data_prep.census_geo
    options:
      show_root_heading: true
      filters:
        - "!^_"

::: processing.weighting.data_prep.pums_data
    options:
      show_root_heading: true
      filters:
        - "!^_"

::: processing.weighting.data_prep.control_data
    options:
      show_root_heading: true
      filters:
        - "!^_"

::: processing.weighting.data_prep.seed_data
    options:
      show_root_heading: true
      filters:
        - "!^_"

## Balancing

::: processing.weighting.balancing
    options:
      show_root_heading: true
      show_docstring_description: true
      members: false

::: processing.weighting.balancing.balancer
    options:
      show_root_heading: true
      members:
        - balance_weights
        - MergeSpec
        - ZoneStatus

::: processing.weighting.balancing.base_weights
    options:
      show_root_heading: true
      filters:
        - "!^_"

::: processing.weighting.balancing.importance
    options:
      show_root_heading: true
      members:
        - compute_moe_importance

::: processing.weighting.balancing.weight_propagation
    options:
      show_root_heading: true
      members:
        - propagate_weights
        - WEIGHT_CONFIG_MAPPING
        - CARRY_FORWARD
        - AGGREGATE

## Controls

::: processing.weighting.controls
    options:
      show_root_heading: true
      show_docstring_description: true
      members: false

::: processing.weighting.controls.base
    options:
      show_root_heading: true
      members:
        - ControlLevel
        - ControlTarget

::: processing.weighting.controls.registry
    options:
      show_root_heading: true
      members:
        - CONTROLS
        - resolve_targets
        - pums_variables

::: processing.weighting.controls.enums
    options:
      show_root_heading: true
      filters:
        - "!^_"

::: processing.weighting.controls.household
    options:
      show_root_heading: true
      filters:
        - "!^_"

::: processing.weighting.controls.person
    options:
      show_root_heading: true
      filters:
        - "!^_"

## Validation

::: processing.weighting.validation
    options:
      show_root_heading: true
      show_docstring_description: true
      members: false

::: processing.weighting.validation.checksums
    options:
      show_root_heading: true
      filters:
        - "!^_"

::: processing.weighting.validation.weight_checks
    options:
      show_root_heading: true
      filters:
        - "!^_"

## Diagnostics

::: processing.weighting.diagnostics
    options:
      show_root_heading: true
      show_docstring_description: true
      members: false

::: processing.weighting.diagnostics.report
    options:
      show_root_heading: true
      members:
        - generate_report

::: processing.weighting.diagnostics.charts
    options:
      show_root_heading: true
      filters:
        - "!^_"

::: processing.weighting.diagnostics.data
    options:
      show_root_heading: true
      filters:
        - "!^_"

::: processing.weighting.diagnostics.tables
    options:
      show_root_heading: true
      filters:
        - "!^_"
