# Choosing the dwell cutoff for observed locations

The registry stores one row per known location, so a person can hold several of
a kind. For example:

| person_id | location_type | location_num | is_primary | source | n_days | dwell_minutes | what it represents |
|---|---|---|---|---|---|---|---|
| 1 | WORK | 1 | true | reported | – | – | office worker's main office |
| 1 | WORK | 2 | false | observed | 4 | 210 | a second worksite they regularly visit |
| 2 | WORK | 1 | _null_ | observed | 3 | 180 | travelling worker — no single main office |
| 2 | WORK | 2 | _null_ | observed | 2 | 150 | travelling worker — another worksite |

The registry records a work or school location it observes in a person's travel
when they spent enough time there. This note shows the dwell-time distributions
that cutoff is read from, using BATS-2023 output.

- **Reproduce:** `uv run python scripts/dwell_gate_analysis.py --linked-trips
  <linked_trips.parquet> --persons <persons.parquet>`.
- **Pools:** work = work + work-related trips; school = school + school-related.
- **Per location:** destinations are grouped to ~110 m (3 decimal places); each
  location's dwell is its longest observed stay. `n_days` is the number of
  distinct days it was visited.
- **Dwell** excludes the two `d_activity_duration` sentinels (`-1` home end,
  `-2` last trip of the day).

## Distributions

Percentile columns are dwell minutes; the `≥N` columns are the share of
locations kept at that cutoff.

**Work (work + work-related) — 20,239 locations**

| platform | n_days | locations | p25 | p50 | p75 | p90 | ≥15m | ≥30m | ≥45m | ≥60m |
|----------|--------|-----------|-----|-----|-----|-----|------|------|------|------|
| rmove    | 1      | 14,365    | 7   | 58  | 224 | 452 | 67%  | 59%  | 54%  | 50%  |
| rmove    | 2      | 2,698     | 133 | 327 | 495 | 557 | 92%  | 87%  | 85%  | 83%  |
| rmove    | 3      | 1,318     | 264 | 431 | 528 | 585 | 96%  | 95%  | 94%  | 93%  |
| rmove    | 4+     | 1,188     | 332 | 488 | 548 | 617 | 99%  | 98%  | 98%  | 97%  |
| browser  | 1      | 627       | 285 | 463 | 540 | 595 | 96%  | 95%  | 93%  | 92%  |
| call     | 1      | 43        | 165 | 390 | 490 | 541 | 100% | 100% | 98%  | 91%  |

**School (school + school-related) — 3,320 locations**

| platform | n_days | locations | p25 | p50 | p75 | p90 | ≥15m | ≥30m | ≥45m | ≥60m |
|----------|--------|-----------|-----|-----|-----|-----|------|------|------|------|
| rmove    | 1      | 2,248     | 20  | 96  | 307 | 470 | 77%  | 73%  | 69%  | 63%  |
| rmove    | 2      | 507       | 93  | 270 | 454 | 540 | 86%  | 85%  | 84%  | 81%  |
| rmove    | 3      | 158       | 121 | 362 | 490 | 571 | 92%  | 88%  | 87%  | 85%  |
| rmove    | 4+     | 125       | 243 | 416 | 530 | 590 | 93%  | 91%  | 91%  | 90%  |
| browser  | 1      | 274       | 181 | 380 | 439 | 515 | 91%  | 90%  | 89%  | 86%  |
| call     | 1      | 8         | 118 | 390 | 400 | 450 | 100% | 100% | 100% | 88%  |

## Reading the tables

Only the **rmove single-day** row carries a large mass of very short stays (its
p25 is 7 minutes for work); every other row is already concentrated at long
dwells before any cutoff. So the cutoff's job is mainly to drop brief single-day
stops without touching the clearly-substantial locations. A **30-minute** cutoff
does that: it removes the short-stay stops while keeping essentially all of the
longer-stay and repeat-visit locations.

## On the number of days

`n_days` looks like a strong signal — dwell rises steadily with it — but it is
only available where the survey collects multiple travel days. Of respondents
with trips, rmove has a median of 6 days, while **browserMove and call-center
respondents each provide a single day (about 17% of respondents)**. Filtering on
`n_days ≥ 2` would therefore exclude those platforms entirely. It is recorded on
each location for later use, but the population rule uses dwell only.
