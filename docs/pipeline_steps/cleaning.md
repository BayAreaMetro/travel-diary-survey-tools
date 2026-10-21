
# Cleaning

Cleaning is project-specific: each project writes its own step, such as
`clean_2023_bats` in `projects/bats_2023/clean_bats_2023.py`, and its runner
passes it to `Pipeline(steps=[...])` with the shared steps. It runs right after
`load_data` and turns the vendor's tables into canonical ones, so every later
step can rely on the canonical schema.

What a cleaner does depends on its survey. The BATS cleaners:

- recode vendor codes and missing-value markers to the canonical enums (for
  example 2019's `-9998` to `995`), and derive canonical fields such as
  `income_bin`, `race` and `ethnicity`;
- add a field the survey did not ask as an all-null column (2019's
  `commute_freq`), rather than inferring it;
- fix records the vendor got wrong, such as 2019 trips whose depart and arrive
  times are swapped;
- set `survey_complete` from the vendor's completeness flags, at the day level
  and up, for `cascade_completeness` to cascade;
- deliver `habitual_locations` with the survey's reported locations (below).

A cleaning step lives outside `processing`, so it need not declare a
`requires` / `produces` contract.

## Delivering habitual locations

Every cleaning step delivers `habitual_locations` holding the survey's reported
locations, built with
[`reported_habitual_locations`](detect_habitual_locations.md#delivering-the-reported-locations).
Pass any further reported locations, such as a vendor's second home, as
`extra`. The `detect_habitual_locations` step adds the observed ones after trip
linking.
