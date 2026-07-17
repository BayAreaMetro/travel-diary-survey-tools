# Location-registry population gate: dwell & recurrence distributions

Reference data for tuning the person-location registry population gate
(`RegistryGateConfig`, issue #71). The registry promotes an observed destination
to a habitual **WORK** location only when a respondent both **stays** there and
**returns** there. This note shows the distributions those two cutoffs are drawn
from, so the gate is set from data rather than guessed.

- **Source:** BATS-2023 canonical output (`linked_trips` + `persons`),
  254,333 real dwell observations.
- **Reproduce:** `uv run python scripts/dwell_gate_analysis.py --linked-trips
  <linked_trips.parquet> --persons <persons.parquet>`.
- **Dwell** = `d_activity_duration` (linked-trip field, #68) excluding sentinels
  (`-1` destination is home, `-2` last trip of the person-day). The numbers below
  were reconstructed as *next departure − this arrival* from a pre-#68 cache; the
  shipped script prefers `d_activity_duration` and yields the same shape.
- **Split** by whether the person reported a fixed location of that kind
  (`work_lat`/`school_lat` non-null) — a proxy for "has a primary".

## Why two knobs, not one

Dwell alone cannot separate a habitual alternate worksite from a one-off
all-day offsite; recurrence alone cannot separate a worksite from a place a gig
worker passes through daily for two minutes. The gate needs **both**:

| Knob | `RegistryGateConfig` field | Filters out |
|------|----------------------------|-------------|
| Minimum dwell | `min_dwell_minutes` | brief gig/delivery/pass-through stops |
| Minimum distinct days | `min_distinct_days` | genuine but one-off meetings/offsites |

## Dwell by destination purpose (median & short-stay noise)

| Purpose | split | n | p50 dwell | short-stay spike | retain ≥30 min |
|---------|-------|---|-----------|------------------|----------------|
| WORK | has reported | 15,176 | 289 min | small | **92.1%** |
| WORK | no reported | 51 | 411 min | small | 90.2% |
| WORK_RELATED | has reported | 12,442 | 60 min | ~40% at 0–28 min | **59.1%** |
| WORK_RELATED | no reported | 5,667 | 37 min | ~44% at 0–25 min | 53.5% |
| SCHOOL | has reported | 2,712 | 321 min | school is long-dwell | 78.1% |
| SCHOOL_RELATED | has reported | 1,655 | 83 min | ~25% at 0–24 min | 73.7% |

Reading it:

- **WORK** destinations (the usual workplace) are genuinely long-dwell — a 30-min
  gate keeps 92% and the short-stay spike is minor. Work rows barely need gating.
- **WORK_RELATED** is where the noise lives: a large 0–28 min spike (~40% of
  observations) sits under a long tail. This is exactly the population the
  observed-worksite derivation draws from, and where the dwell gate earns its
  keep — a 30-min cutoff removes the noise spike while retaining ~59% of the
  signal band (median 60 min).

## Recurrence is the stronger filter (WORK_RELATED clusters)

Clustering each person's WORK_RELATED destinations to ~110 m (3 dp) gives 13,618
distinct person-locations. Applying both knobs:

| gate | person-locations kept |
|------|-----------------------|
| ≥1 day, dwell ≥15 min | 8,823 |
| ≥1 day, dwell ≥30 min | 7,574 |
| **≥2 days, dwell ≥30 min** | **1,607** |
| ≥2 days, dwell ≥45 min | 1,526 |
| ≥3 days, dwell ≥30 min | 632 |

The `≥2 distinct days` requirement is decisive: ~88% of WORK_RELATED locations
are visited on only one day and fall away, leaving ~1,600 genuinely habitual
alternate worksites survey-wide — a plausible count for real multi-site workers.
Raising dwell from 30→45 min barely moves the survivor set (1,607→1,526); raising
days from 2→3 roughly quarters it (1,607→632).

## Chosen defaults

```
RegistryGateConfig(
    min_dwell_minutes = 30.0,   # clears the WORK_RELATED 0–28 min noise spike
    min_distinct_days = 2,      # the decisive habitual-vs-one-off filter
    cluster_decimals  = 3,      # ~110 m repeat-visit clustering
)
```

These are **starting points for review**, exposed as config precisely so they can
be tuned with the distributions above in view. Two open calls for the reviewer:

1. **Single-day alternate work.** `min_distinct_days = 2` deliberately excludes a
   worksite seen on only one survey day. That is the right rule for a *habitual*
   registry, but it is a stricter notion than #70's per-day alternate-work
   detection (which promotes a single day's longest work-related stay). If
   downstream needs per-day alternate work, that is a distinct, looser signal —
   worth keeping separate from the habitual registry rather than loosening this
   gate.
2. **Dwell statistic.** The gate uses the **max** observed dwell per cluster
   (`dwell_minutes`), i.e. "held them for ≥30 min at least once". A median would
   be stricter. Max is used here so a worksite is not disqualified by one short
   visit; revisit if it admits too much.

School-related locations show the same shape (median 83 min, ~25% short-stay
noise) and would gate the same way; the registry currently derives observed
locations for **work** only, and school is a straightforward extension.
