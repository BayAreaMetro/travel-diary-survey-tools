# Location-registry population gate: dwell x recurrence x platform

Reference data for tuning the person-location registry population gate
(`RegistryGateConfig`, issue #71), which promotes an observed destination to a
habitual **WORK** location. Three signals establish a location:

1. **Reported** — the survey usual-location question (`source = REPORTED`).
2. **Long dwell** — the respondent spends substantial time there (`dwell_minutes`).
3. **Repeat across days** — a recurring visit (`n_days`), *available only when the
   diary spans multiple days*.

- **Source:** BATS-2023 canonical output (`linked_trips` + `persons`).
- **Reproduce:** `uv run python scripts/dwell_gate_analysis.py --linked-trips
  <linked_trips.parquet> --persons <persons.parquet>`.
- **Dwell** = `d_activity_duration` (#68), excluding sentinels (`-1` home, `-2`
  last trip of day). Numbers below are from a pre-#68 cache (dwell reconstructed
  as next-depart − arrive); the shipped script prefers `d_activity_duration`.
- **Per-location** statistic = the **max** observed dwell in a coordinate cluster
  (rounded to 3 dp ≈ 110 m), which is what the gate uses.

## Recurrence is not uniformly available (the platform caveat)

`n_days` depends on how many travel days the diary platform collects:

| diary_platform | persons w/ trips | median days | % exactly 1 day |
|----------------|------------------|-------------|-----------------|
| rmove          | 12,080           | 6           | 2.6%            |
| browser (bMove)| 1,901            | 1           | **100%**        |
| call center    | 235              | 1           | **100%**        |

**17% of respondents get a single travel day**, so a `n_days ≥ 2` requirement can
*never* fire for them — it is an rmove-only signal. A gate that hard-requires
recurrence silently yields zero observed locations for browserMove and call
center, regardless of whether those respondents have a real alternate worksite.

## Dwell distribution by platform x recurrence (WORK_RELATED)

Per-location max dwell, so the coupling between the three signals is visible
rather than hidden behind a median.

**rmove** — recurrence and dwell are strongly coupled; short-stop noise collapses
as `n_days` rises:

| n_days | locations | p25 | p50 | p75 | p90 | ≥30 min | ≥60 min |
|--------|-----------|-----|-----|-----|-----|---------|---------|
| 1      | 11,455    | 5   | 32  | 134 | 312 | 51.0%   | 40.6%   |
| 2      | 1,293     | 31  | 155 | 399 | 529 | 75.4%   | 66.9%   |
| 3      | 430       | 98  | 277 | 481 | 567 | 86.3%   | 81.2%   |
| 4+     | 280       | 190 | 427 | 527 | 593 | 93.2%   | 89.3%   |

The `n_days = 1` cell is ~50% short-stop noise (a large 0–32 min spike); by 3+
days it is overwhelmingly full-day worksites. Recurrence is a strong quality
signal on rmove.

**Single-day platforms are a different, cleaner population.** Their one day of
data is recall-based, so only salient stops are reported — the 2-minute GPS
pass-throughs that flood rmove's single-day trace are simply absent:

| platform (all n_days = 1) | locations | p25 | p50 | p75 | ≥30 min | ≥60 min |
|---------------------------|-----------|-----|-----|-----|---------|---------|
| rmove                     | 11,455    | 5   | 32  | 134 | 51.0%   | 40.6%   |
| browser (bMove)           | 147       | 40  | 148 | 420 | 78.9%   | 70.1%   |
| call center               | 13        | 55  | 115 | 157 | 92.3%   | 69.2%   |

A browserMove single-day WORK_RELATED location (p50 = 148 min) looks like an
rmove **2-day** location (p50 = 155 min), not an rmove single-day one (p50 = 32
min). So the dwell distribution that would need recurrence to clean it up on
rmove is *already* clean on the single-day platforms.

## Gate survivors, by platform

Person-locations passing `n_days ≥ D` **and** `max_dwell ≥ M`:

| platform | 1d/30m | 2d/30m | 3d/30m |
|----------|--------|--------|--------|
| rmove    | 7,446  | 1,607  | 632    |
| browser  | 116    | **0**  | **0**  |
| call     | 12     | **0**  | **0**  |

The `2d/30m` column is the current `RegistryGateConfig` default. It keeps 1,607
habitual rmove worksites and **zero** from the single-day platforms — the bias
this analysis exists to surface.

## Implication for the gate (open design question)

The distributions argue against a single hard `dwell AND days` conjunction and
for treating the three signals as **graded evidence recorded on the row**
(`source`, `dwell_minutes`, `n_days` are all stored), with the population rule
platform-aware:

- **Multi-day diary (rmove):** recurrence is available and strongly denoises, so
  `n_days ≥ 2` with a modest dwell bar (≥30 min) is well supported.
- **Single-day diary (browserMove, call center):** recurrence is unavailable, but
  the recall-filtered dwell distribution is already ≈ rmove's multi-day one, so a
  **dwell-only** rule (at a comparable bar) admits mostly substantial activity,
  not gig noise. These would carry weaker provenance (no recurrence corroboration).

This also folds #70's per-day alternate-work notion in as the single-day branch
of one mechanism — {habitual multi-day} ∪ {dominant single-day activity} —
distinguished by provenance rather than kept as a parallel system.

The **step-1 code deliberately does not decide this**: `derive_observed_work_
locations` applies a uniform `n_days ≥ min_distinct_days` gate (hence rmove-only
today), and the platform-aware rule lands with the classification rewire in the
follow-up, once the trade-off above is settled.
