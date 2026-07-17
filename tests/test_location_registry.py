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


def _linked_trip(person_id, day_id, lat, lon, dwell, purpose=WORK_RELATED):
    return {
        "person_id": person_id,
        "day_id": day_id,
        "d_lat": lat,
        "d_lon": lon,
        "d_purpose_category": purpose,
        "d_activity_duration": dwell,
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
    assert reg["n_days"].null_count() == reg.height
    assert reg["dwell_minutes"].null_count() == reg.height


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
    assert row["n_days"] == 3
    assert row["dwell_minutes"] == 130  # max observed dwell
    assert abs(row["lat"] - ALT_WORK[0]) < 1e-6


def test_single_day_location_is_recorded_with_ndays_one():
    """A single-day visit is admitted (recurrence is not filtered) and n_days=1."""
    trips = pl.DataFrame([_linked_trip(1, 10, *ALT_WORK, 300)])
    observed = derive_observed_locations(trips)
    assert observed.height == 1
    assert observed.row(0, named=True)["n_days"] == 1


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
    assert row["n_days"] == 1
    assert row["dwell_minutes"] == 90


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
    assert row["n_days"] == 2


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
    assert work["is_primary"].to_list() == [True, None]


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
