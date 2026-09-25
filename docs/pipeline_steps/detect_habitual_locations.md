::: processing.habitual_locations
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members: false

## The step

::: processing.habitual_locations.detect_habitual_locations
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - detect_habitual_locations
      filters:
        - "!^logger$"
        - "!^_"

## Delivering the reported locations

Survey cleaning builds the delivered table with this helper.

::: processing.habitual_locations.reported
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - reported_habitual_locations
      filters:
        - "!^logger$"
        - "!^_"

## Configuration

::: processing.habitual_locations.habitual_location_configs
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - MatchConfig
        - HabitualLocationConfig

## Matching a trip end

::: processing.habitual_locations.matching
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - match_trip_ends
