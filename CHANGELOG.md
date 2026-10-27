# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-10-27

### Added

- `compute_weights` step: balances households to Census controls ([#38]).
- `add_existing_weights` step: uses the survey vendor's weights ([#12], [#15]).
- One set of weights per usability profile, named `<level>_weight_<profile>` ([#124], [#141]).
- Weighting diagnostics dashboard covering every profile and the vendor's weights ([#126]).
- `imputation` step with k-nearest neighbors, random forest and MICE ([#33], [#48]).
- Usability profiles: `usable_<profile>` on every table, set by `tour_closes_at`, `household_day_needs`, `zone_coverage`, `joint_tours` and `travel_days` ([#79], [#112], [#119], [#136], [#170]).
- `survey_complete` on every table, from the vendor's day verdicts ([#83], [#84], [#166]).
- `detect_habitual_locations` step with `habitual_locations` and `habitual_location_days` tables ([#78], [#128]).
- `household_days` and `joint_tours` tables ([#83], [#167]).
- Person fields `industry`, `occupation`, `education`, `surveyable`, `survey_tech` and `exclusion_reason` ([#67], [#77], [#90], [#149], [#172]).
- Tour field `tour_type`; linked trip fields `d_activity_duration`, `o_location_type` and `d_location_type` ([#68], [#86]).
- Duration-weighted tour purpose scoring, the new default; `tour_purpose_method: hierarchy` keeps the old rule ([#82]).
- Tour data quality codes naming the defect: `OTHER_HOME`, `PARTIAL_DAY_SPLIT`, `PARTIAL_DIARY_EDGE`, `NO_DESTINATION`, `SPATIAL_GAP` ([#79], [#105], [#128]).
- Alternate workplaces ([#70], [#78]).
- CT-RAMP formatter ([#6]), with `aoResults`, `cdapResults`, `AllTours` and `AllTrips` tables ([#73], [#97]), survey distances ([#25]), `industry_empsix` ([#76]) and ferry mode codes 22 and 23 ([#131]).
- BATS 2019 project, processed the same way as BATS 2023 ([#20], [#115]).
- Zone snapping within `max_snap_distance` ([#63]).
- Config paths built from per-machine roots in `.env` ([#117]).
- Steps declare the columns they read and write ([#44], [#103], [#149]).
- Config models for step settings ([#143]).
- `--config` argument; the config used is copied to the output folder ([#66]).
- Run log records the code commit and diff ([#40]).
- Excel codebook of labeled enums ([#40]).
- Enum labels accepted in configs ([#35]).
- Required settings with no default: `split_on_occupancy` ([#102]), `write_only_canonical` ([#109]), `include_observed_homes` ([#134]), `household_size_top_group` and `preserve_travel_totals` ([#136]), `child_day_complete` ([#166]), `exclude_survey_tech` ([#172]).
- End-to-end tests on synthetic data, with an output baseline ([#61], [#113], [#118], [#123]).
- 750-line limit on `src/` modules ([#145]).
- Docs site and Developer Reference ([#31], [#157]).
- GPLv3 license ([#59]).

### Changed

- Formatters read the usability verdict instead of dropping records themselves ([#79], [#121], [#122]).
- Weights pass household → person → day → trip, and a dropped record's weight stays in its household ([#83], [#84], [#136]).
- A usable record without a weight stops the run ([#174]).
- Joint trips and joint tours carry the sum of their members' weights ([#97]).
- `joint_trips` lists member trips only, 18 columns down to 7 ([#97]).
- Day completeness is the vendor's verdict ([#166]).
- All profiles take Tuesday to Thursday only ([#170]).
- BATS 2023 uses GPS-tracked persons only, on the June 2025 data and the vendor's weights ([#172], [#174]).
- Work-related tours by employed persons count as work in CT-RAMP and DaySim ([#138]).
- Tour destination is the primary destination, not the last stop ([#69], [#72]).
- Habitual locations need explicit evidence, within one 300 m buffer ([#128], [#134]).
- Joint tour ids identify one outing ([#64]).
- `write_data` writes only declared columns ([#109]).
- Output tables are sorted, and runs are reproducible ([#66], [#75], [#163]).
- Renamed `trip_id` to `unlinked_trip_id` and `trip_weight` to `unlinked_trip_weight` ([#13]).
- Renamed `complete` to `survey_complete`, profiles to `ctramp`/`daysim`/`analysis`, and `usability_flag_col` to `usability_profile` ([#125]).
- Renamed `days` to `person_days`, `day_id` to `person_day_id` and `day_weight` to `person_day_weight` ([#167]).
- Renamed step `flag_model_usable` to `cascade_completeness` ([#89]).
- Renamed `add_existing_weights` keys to the weight column names ([#98]).
- Renamed `extract_tours` setting `habitual_locations` to `habitual_match` ([#140]).
- `compute_weights` settings grouped under `census:`, `balancing:` and `diagnostics:` ([#143]).
- `add_zone_ids` runs before `cascade_completeness` ([#119]).
- Unknown config keys raise ([#140]).
- Person type derived in each formatter ([#27]).
- DaySim writes missing locations and zones as `-1` ([#42]).
- Faster tour extraction, validation, zone assignment and weighting ([#127]).
- Dependencies aligned with ActivitySim ([#50]).
- Long modules split ([#98], [#129], [#147]); duplicated tests removed ([#130]).
- Docs updated, built strictly, and deployed from `develop` ([#116], [#153], [#155], [#156]).

### Removed

- `format_ctramp` settings `drop_missing_taz` and `drop_invalid_tours` ([#121]).
- `format_daysim` settings `drop_partial_tours`, `drop_missing_taz` and `drop_invalid_tours` ([#121]).
- `cascade_completeness` setting `require_valid_tours` ([#89]).
- Weighting settings `usability_profile`, `keep_name` and `exclude_incompletes` ([#141]).
- `TourDirection.SUBTOUR` ([#88]).
- Tour data quality codes `SINGLE_TRIP`, `LOOP_TRIP` and `INDETERMINATE` ([#105]).
- Columns `single_trip_tour` and `person_category` ([#106]).
- Coordinate, time and zone columns on `joint_trips` ([#97]).

### Fixed

- Config keys silently ignored; joint trips ran the buffer method instead of Mahalanobis ([#140]).
- Config shorthands referring to other shorthands didn't resolve ([#101]).
- PUMS columns joined to the wrong households ([#114]).
- At-work subtours always dropped ([#86]), with no direction ([#88]) and a missing return leg ([#110]).
- Negative dwell time on 43% of linked trips ([#108]).
- CT-RAMP joint trips pointed at the wrong joint tour, and `sampleRate` double-counted ([#97]).
- One-member joint groups in CT-RAMP output ([#119], [#121], [#122]).
- CT-RAMP person-type codes 6 and 7 swapped; income not deflated ([#91]).
- Persons written without a home zone ([#122]).
- Vendor weights lost when a profile dropped records ([#136]).
- Validation failures on unsurveyable persons ([#90]), tours between two homes ([#134]) and one-trip tours ([#114]).
- Unregistered steps now fail before data loads ([#89]).
- Weighting failed on every run ([#114]).
- BATS 2023 crashed building mandatory locations ([#40]).
- BATS 2019 marked every household complete ([#115]), had no `tnc_type` ([#120]), miscoded `mode_type` ([#37]) and stopped before the end ([#168]).
- Validation and DaySim output bugs ([#16], [#22], [#62]).

## [0.1.0] - 2026-01-10

### Added

- SFCTA DaySim processing pipeline (Drew Cooper, 2025-05).
- Mode standardization (Joanne, 2025-06).
- MTC config for TM1 zones (Flavia Tsang, 2025-07).
- Trip linkage (2025-07).
- ARC survey processing (Suzanne Childress, 2025-11).
- Pipeline package: canonical data models, YAML-configured steps, cache and logging ([#2], [#4]).
- Steps `load_data`, `link_trips`, `detect_joint_trips`, `extract_tours`, `format_daysim`, `add_zone_ids` and `write_data` ([#2], [#4], [#5], [#8]).
- BATS 2023 project ([#2], [#5]).
- CI and pre-commit linting ([#5], [#7], [#9]).

### Changed

- Renamed `daysim_pipeline` to `survey_processing` (2025-09).
- Legacy DaySim scripts archived ([#2]).
- Household-scoped `joint_trip_id` ([#8]).

### Fixed

- `rmove_only` not passed to downstream steps (2025-10).

[unreleased]: https://github.com/BayAreaMetro/travel-diary-survey-tools/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/BayAreaMetro/travel-diary-survey-tools/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/BayAreaMetro/travel-diary-survey-tools/releases/tag/v0.1.0
[#2]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/2
[#4]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/4
[#5]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/5
[#6]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/6
[#7]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/7
[#8]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/8
[#9]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/9
[#12]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/12
[#13]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/13
[#15]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/15
[#16]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/16
[#20]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/20
[#22]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/22
[#25]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/25
[#27]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/27
[#31]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/31
[#33]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/33
[#35]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/35
[#37]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/37
[#38]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/38
[#40]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/40
[#42]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/42
[#44]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/44
[#48]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/48
[#50]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/50
[#59]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/59
[#61]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/61
[#62]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/62
[#63]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/63
[#64]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/64
[#66]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/66
[#67]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/67
[#68]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/68
[#69]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/69
[#70]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/70
[#72]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/72
[#73]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/73
[#75]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/75
[#76]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/76
[#77]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/77
[#78]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/78
[#79]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/79
[#82]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/82
[#83]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/83
[#84]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/84
[#86]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/86
[#88]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/88
[#89]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/89
[#90]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/90
[#91]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/91
[#97]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/97
[#98]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/98
[#101]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/101
[#102]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/102
[#103]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/103
[#105]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/105
[#106]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/106
[#108]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/108
[#109]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/109
[#110]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/110
[#112]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/112
[#113]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/113
[#114]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/114
[#115]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/115
[#116]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/116
[#117]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/117
[#118]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/118
[#119]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/119
[#120]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/120
[#121]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/121
[#122]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/122
[#123]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/123
[#124]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/124
[#125]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/125
[#126]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/126
[#127]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/127
[#128]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/128
[#129]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/129
[#130]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/130
[#131]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/131
[#134]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/134
[#136]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/136
[#138]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/138
[#140]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/140
[#141]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/141
[#143]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/143
[#145]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/145
[#147]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/147
[#149]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/149
[#153]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/153
[#155]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/155
[#156]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/156
[#157]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/157
[#163]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/163
[#166]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/166
[#167]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/167
[#168]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/168
[#170]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/170
[#172]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/172
[#174]: https://github.com/BayAreaMetro/travel-diary-survey-tools/pull/174
