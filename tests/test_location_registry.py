"""Tests for the tall person-location registry (issue #71).

Covers:
- Reported scalar locations unpivot into tall rows and reproduce the input coords
- Observed work/school locations are recorded when they pass the dwell cutoff;
  recurrence (n_days) is recorded but not filtered on by default (tunable)
- The combined registry numbers locations primary-first, de-duplicates observed
  locations coinciding with a reported one, and conforms to PersonLocationModel
"""

import polars as pl

from data_canon.codebook.generic import LocationSource, LocationType
from data_canon.codebook.trips import PurposeCategory
from data_canon.models.survey import PersonLocationModel
from processing.tours.location_registry import (
    RegistryGateConfig,
    build_location_registry,
    build_reported_registry,
    derive_observed_locations,
)

WORK = PurposeCategory.WORK.value
WORK_RELATED = PurposeCategory.WORK_RELATED.value
SCHOOL = PurposeCategory.SCHOOL.value
SCHOOL_RELATED = PurposeCategory.SCHOOL_RELATED.value
SHOP = PurposeCategory.SHOP.value

USUAL_WORK = (37.85, -122.45)
ALT_WORK = (37.95, -122.55)
ALT_SCHOOL = (37.99, -122.59)

# Weekly dwell-profile columns (1=Mon..7=Sun); _linked_trip maps day_id to one.
DWELL_COLS = [
    "dwell_mon",
    "dwell_tue",
    "dwell_wed",
    "dwell_thu",
    "dwell_fri",
    "dwell_sat",
    "dwell_sun",
]


def _days_seen(row) -> int:
    """Number of days-of-week with a recorded dwell (derived recurrence)."""
    return sum(row[c] is not None for c in DWELL_COLS)


def _person_locations() -> pl.DataFrame:
    """Two people: person 1 has home+work+school, person 2 only home (nulls)."""
    return pl.DataFrame(
        {
            "person_id": [1, 2],
            "home_lat": [37.80, 37.70],
            "home_lon": [-122.40, -122.30],
            "work_lat": [USUAL_WORK[0], None],
            "work_lon": [USUAL_WORK[1], None],
            "school_lat": [37.90, None],
            "school_lon": [-122.50, None],
        }
    )


def _linked_trip(person_id, day_id, lat, lon, dwell, purpose=WORK_RELATED, dow=None):
    return {
        "person_id": person_id,
        "day_id": day_id,
        "d_lat": lat,
        "d_lon": lon,
        "d_purpose_category": purpose,
        "d_activity_duration": dwell,
        "travel_dow": dow if dow is not None else ((day_id - 1) % 7) + 1,
    }


# --- build_reported_registry ------------------------------------------------


def test_reported_registry_reproduces_scalar_coordinates():
    """Reported scalars unpivot to tall rows preserving type, coords, provenance."""
    reg = build_reported_registry(_person_locations())

    # person 1: home, work, school. person 2: home only (work/school null skipped)
    assert reg.height == 4
    p1 = reg.filter(pl.col("person_id") == 1).sort("location_type")
    assert p1["location_type"].to_list() == [
        LocationType.HOME.value,
        LocationType.WORK.value,
        LocationType.SCHOOL.value,
    ]
    work = p1.filter(pl.col("location_type") == LocationType.WORK.value)
    assert work["lat"].item() == USUAL_WORK[0]
    assert work["lon"].item() == USUAL_WORK[1]

    # provenance + primacy for reported rows
    assert reg["source"].unique().to_list() == [LocationSource.REPORTED.value]
    assert reg["is_primary"].unique().to_list() == [True]
    for col in DWELL_COLS:
        assert reg[col].null_count() == reg.height


def test_reported_registry_skips_null_locations():
    """A person with only a home (null work/school) yields a single home row."""
    reg = build_reported_registry(_person_locations())
    p2 = reg.filter(pl.col("person_id") == 2)
    assert p2.height == 1
    assert p2["location_type"].item() == LocationType.HOME.value


# --- derive_observed_locations (the dwell cutoff) ---------------------------


def test_worksite_with_sufficient_dwell_is_recorded():
    """A place with a long-enough stay becomes an OBSERVED WORK row."""
    trips = pl.DataFrame(
        [
            _linked_trip(1, 10, *ALT_WORK, 120),
            _linked_trip(1, 11, *ALT_WORK, 130),
            _linked_trip(1, 12, *ALT_WORK, 110),
        ]
    )
    observed = derive_observed_locations(trips)
    assert observed.height == 1
    row = observed.row(0, named=True)
    assert row["location_type"] == LocationType.WORK.value
    assert row["source"] == LocationSource.OBSERVED.value
    assert row["is_primary"] is None
    # days 10/11/12 map to Wed/Thu/Fri; each records that day's dwell
    assert row["dwell_wed"] == 120
    assert row["dwell_thu"] == 130
    assert row["dwell_fri"] == 110
    assert row["dwell_mon"] is None
    assert _days_seen(row) == 3
    assert abs(row["lat"] - ALT_WORK[0]) < 1e-6


def test_single_day_location_is_recorded():
    """A single-day visit is admitted (recurrence is not filtered)."""
    trips = pl.DataFrame([_linked_trip(1, 10, *ALT_WORK, 300)])  # day 10 -> Wed
    observed = derive_observed_locations(trips)
    assert observed.height == 1
    row = observed.row(0, named=True)
    assert _days_seen(row) == 1
    assert row["dwell_wed"] == 300


def test_short_dwell_cluster_excluded_by_dwell_cutoff():
    """A cluster of brief stops stays below the dwell cutoff and is dropped."""
    trips = pl.DataFrame(
        [
            _linked_trip(1, 10, *ALT_WORK, 8),
            _linked_trip(1, 11, *ALT_WORK, 12),
            _linked_trip(1, 12, *ALT_WORK, 5),
        ]
    )
    assert derive_observed_locations(trips).height == 0


def test_dwell_sentinels_are_ignored():
    """Sentinel dwell values (-1, -2) count toward neither dwell nor n_days."""
    trips = pl.DataFrame(
        [
            _linked_trip(1, 10, *ALT_WORK, -1),  # home sentinel
            _linked_trip(1, 11, *ALT_WORK, -2),  # last-trip-of-day sentinel
            _linked_trip(1, 12, *ALT_WORK, 90),  # one real visit
            _linked_trip(2, 20, *ALT_WORK, -1),  # person 2: only sentinels
            _linked_trip(2, 21, *ALT_WORK, -2),
        ]
    )
    observed = derive_observed_locations(trips)
    # person 2 (sentinels only) drops out; person 1 keeps only the real day
    assert observed["person_id"].to_list() == [1]
    row = observed.row(0, named=True)
    assert _days_seen(row) == 1  # day 12 -> Friday only
    assert row["dwell_fri"] == 90


def test_non_work_or_school_purposes_ignored():
    """Destinations outside the work/school pools (e.g. shopping) are ignored."""
    trips = pl.DataFrame(
        [
            _linked_trip(1, 10, *ALT_WORK, 200, purpose=SHOP),
            _linked_trip(1, 11, *ALT_WORK, 200, purpose=SHOP),
        ]
    )
    assert derive_observed_locations(trips).height == 0


def test_work_purpose_contributes_to_pool():
    """A WORK-purpose destination (not only WORK_RELATED) is pooled as work."""
    trips = pl.DataFrame([_linked_trip(1, 10, *ALT_WORK, 300, purpose=WORK)])
    observed = derive_observed_locations(trips)
    assert observed.height == 1
    assert observed.row(0, named=True)["location_type"] == LocationType.WORK.value


def test_school_pool_derives_observed_school():
    """School + school-related destinations pool into an OBSERVED SCHOOL row."""
    trips = pl.DataFrame(
        [
            _linked_trip(1, 10, *ALT_SCHOOL, 200, purpose=SCHOOL),
            _linked_trip(1, 11, *ALT_SCHOOL, 210, purpose=SCHOOL_RELATED),
        ]
    )
    observed = derive_observed_locations(trips)
    assert observed.height == 1
    row = observed.row(0, named=True)
    assert row["location_type"] == LocationType.SCHOOL.value
    assert _days_seen(row) == 2  # days 10/11 -> Wed/Thu


def test_weekly_dwell_profile_is_recorded_per_day():
    """An observed location records the dwell on each day-of-week it was seen."""
    trips = pl.DataFrame(
        [
            _linked_trip(1, 10, *ALT_WORK, 120, dow=1),  # Monday
            _linked_trip(1, 11, *ALT_WORK, 130, dow=3),  # Wednesday
        ]
    )
    row = derive_observed_locations(trips).row(0, named=True)
    assert row["dwell_mon"] == 120
    assert row["dwell_wed"] == 130
    assert row["dwell_tue"] is None
    assert _days_seen(row) == 2


def test_reported_locations_have_no_dwell_profile():
    """Reported locations carry no per-day dwell (all null)."""
    reg = build_reported_registry(_person_locations())
    for col in DWELL_COLS:
        assert reg[col].null_count() == reg.height


def test_dwell_cutoff_is_tunable():
    """The dwell cutoff is the tuning knob: 20 min fails 30, passes 15."""
    trips = pl.DataFrame([_linked_trip(1, 10, *ALT_WORK, 20)])
    assert derive_observed_locations(trips).height == 0
    loose = RegistryGateConfig(min_dwell_minutes=15)
    assert derive_observed_locations(trips, loose).height == 1


def test_recurrence_filter_is_optional_and_off_by_default():
    """min_distinct_days defaults off; setting it applies a recurrence filter."""
    trips = pl.DataFrame([_linked_trip(1, 10, *ALT_WORK, 300)])
    assert derive_observed_locations(trips).height == 1  # default: single day kept
    strict = RegistryGateConfig(min_distinct_days=2)
    assert derive_observed_locations(trips, strict).height == 0


# --- build_location_registry (combined) -------------------------------------


def test_reported_only_registry_when_no_trips():
    """Without linked trips the registry is exactly the reported locations."""
    reg = build_location_registry(_person_locations())
    assert reg.height == 4
    assert reg["source"].unique().to_list() == [LocationSource.REPORTED.value]
    assert reg["location_num"].unique().to_list() == [1]


def test_observed_worksite_numbered_after_reported_primary():
    """Reported work is location_num 1 (primary); observed work follows as 2."""
    trips = pl.DataFrame(
        [
            _linked_trip(1, 10, *ALT_WORK, 120),
            _linked_trip(1, 11, *ALT_WORK, 130),
        ]
    )
    reg = build_location_registry(_person_locations(), trips)
    work = reg.filter(
        (pl.col("person_id") == 1) & (pl.col("location_type") == LocationType.WORK.value)
    ).sort("location_num")
    assert work["location_num"].to_list() == [1, 2]
    assert work["source"].to_list() == [
        LocationSource.REPORTED.value,
        LocationSource.OBSERVED.value,
    ]
    # reported one is primary; the observed site is known NOT to be primary
    assert work["is_primary"].to_list() == [True, False]


def test_observed_location_without_reported_primary_has_unknown_primacy():
    """With no reported location of that kind, observed primacy is unknown (None)."""
    # person 2 has no reported work; a multi-site worker with no single office.
    trips = pl.DataFrame(
        [
            _linked_trip(2, 20, *ALT_WORK, 200),
            _linked_trip(2, 21, *USUAL_WORK, 210),
        ]
    )
    reg = build_location_registry(_person_locations(), trips)
    work = reg.filter(
        (pl.col("person_id") == 2) & (pl.col("location_type") == LocationType.WORK.value)
    )
    assert work.height == 2
    assert work["source"].unique().to_list() == [LocationSource.OBSERVED.value]
    assert work["is_primary"].to_list() == [None, None]


def test_observed_coinciding_with_reported_work_is_deduped():
    """An observed cluster on the reported workplace is not re-added."""
    trips = pl.DataFrame(
        [
            _linked_trip(1, 10, *USUAL_WORK, 120),
            _linked_trip(1, 11, *USUAL_WORK, 130),
        ]
    )
    reg = build_location_registry(_person_locations(), trips)
    work = reg.filter(
        (pl.col("person_id") == 1) & (pl.col("location_type") == LocationType.WORK.value)
    )
    assert work.height == 1
    assert work["source"].item() == LocationSource.REPORTED.value


def test_registry_rows_conform_to_model():
    """Every combined-registry row validates against PersonLocationModel."""
    trips = pl.DataFrame(
        [
            _linked_trip(1, 10, *ALT_WORK, 120),
            _linked_trip(1, 11, *ALT_WORK, 130),
            _linked_trip(1, 12, *ALT_SCHOOL, 200, purpose=SCHOOL),
        ]
    )
    reg = build_location_registry(_person_locations(), trips)
    for row in reg.iter_rows(named=True):
        PersonLocationModel(**row)  # raises on any schema violation
