# Add Zone IDs

Assigns each location in the survey to the zones of every configured zone
system (for example TAZ and MAZ). Each system adds one column per location,
named after the zone system: `home_taz`, `work_taz`, `o_taz` and so on. A
point outside every zone can be snapped to the nearest one within
`max_snap_distance`, or left null.

It runs after `extract_tours`, so tours get zones too, and before
`cascade_completeness`, so a usability profile can require that a record has
a zone.

::: processing.add_zone_ids.add_zone_ids
    options:
      show_root_heading: false
      show_root_toc_entry: false
      members:
        - add_zone_ids
      filters:
        - "!^logger$"
        - "!^_"
