"""The pipeline step: reporting completeness once, then one pass per profile."""

import polars as pl

from data_canon.core.dataclass import CanonicalData
from pipeline.decoration import step

from .descriptions import _log_gate_summary, _register_profile_columns
from .profiles import parse_usability_profiles
from .survey_complete import cascade_complete
from .usable_cascade import stamp_usable


@step(
    requires={
        "household_days": {"hh_day_id", "hh_id"},
        "days": {"day_id", "hh_id", "hh_day_id", "survey_complete"},
        "tours": {"tour_id", "day_id", "survey_complete"},
    },
    produces={
        "households": {"survey_complete", "usable_<profile>"},
        "persons": {"survey_complete", "usable_<profile>"},
        "household_days": {"survey_complete", "usable_<profile>"},
        "days": {"usable_<profile>"},
        "unlinked_trips": {"survey_complete", "usable_<profile>", "joint_tour_id_<profile>"},
        "linked_trips": {
            "survey_complete",
            "usable_<profile>",
            "joint_trip_id_<profile>",
            "joint_tour_id_<profile>",
        },
        "tours": {"survey_complete", "usable_<profile>", "joint_tour_id_<profile>"},
        "joint_trips": {"usable_<profile>"},
        "joint_tours": {"usable_<profile>"},
    },
)
def cascade_completeness(
    households: pl.DataFrame | None = None,
    persons: pl.DataFrame | None = None,
    household_days: pl.DataFrame | None = None,
    days: pl.DataFrame | None = None,
    unlinked_trips: pl.DataFrame | None = None,
    linked_trips: pl.DataFrame | None = None,
    joint_trips: pl.DataFrame | None = None,
    tours: pl.DataFrame | None = None,
    joint_tours: pl.DataFrame | None = None,
    usability_profiles: dict[str, dict[str, str]] | None = None,
    canonical_data: CanonicalData | None = None,
) -> dict[str, pl.DataFrame]:
    """Cascade ``survey_complete`` through the hierarchy and stamp one column per profile.

    Walks both kinds of flag across every table so they are internally consistent
    with whatever was set upstream (the vendor's day-level completeness, any
    manual adjustments in the project cleaner, and the structural verdicts from
    tour extraction):

    * ``survey_complete`` -- survey reporting completeness, cascaded hh <-> person <->
      day <-> trip/tour, and over household-days. Never narrowed by model criteria, and never
      configurable: if it is wrong the fix belongs upstream in the cleaner.
    * one column per usability profile -- the subset of ``survey_complete`` that
      profile admits.

    A profile states its standard on four axes: which home has to close a tour,
    what the household-date has to show, which zone system must hold every
    location, and what a joint tour has to be. Several can be stamped in one run
    so different consumers can hold different standards -- a joint-tour model
    needs whole households and fully joint tours, a trip-level estimation does
    not:

    ```yaml
    usability_profiles:
      ctramp:
        tour_closes_at: primary_home
        household_day_needs: all_members
        zone_coverage: taz
        joint_tours: fully_joint
      analysis:
        tour_closes_at: anywhere
        household_day_needs: nothing
        zone_coverage: none
        joint_tours: partial
    ```

    Nothing is implicit: every column stamped is named in config and every
    profile answers every axis. Consumers then choose which to honour by name
    (``weight_profiles`` in the weighting, ``usability_profile`` in each
    formatter), and none of
    them re-derive completeness or tour validity -- the formatters read the
    column and raise if it is absent. ``survey_complete`` is always available as the
    floor without being declared.

    See [`stamp_usable`][processing.completeness.stamp_usable] for the per-level
    rules and [`parse_usability_profiles`]
    [processing.completeness.parse_usability_profiles] for the vocabulary.

    Args:
        households: Canonical households.
        persons: Canonical persons.
        household_days: Canonical household-days, built by the project cleaner.
        days: Canonical person-days.
        unlinked_trips: Canonical unlinked trips.
        linked_trips: Canonical linked trips.
        joint_trips: Aggregated joint trips.
        tours: Canonical tours (with ``tour_data_quality`` / ``tour_category``).
        joint_tours: Aggregated joint tours.
        usability_profiles: Profile name -> ``{tour_closes_at, household_day_needs,
            zone_coverage, joint_tours}``.
            Required, and every profile answers every axis: there is no default
            at either level, so a verdict never appears unasked and a column's
            meaning is never implicit.
        canonical_data: Injected by the pipeline. Profile columns are named from
            config and so cannot be model fields; they are registered here with
            a description of what each admits, which keeps them in the delivered
            output and documented rather than silently dropped.

    Returns:
        The provided tables, each with ``survey_complete`` reconciled and one column
        per profile added.

    Raises:
        ValueError: If ``usability_profiles`` is missing or malformed.
    """
    if usability_profiles is None:
        msg = (
            "cascade_completeness requires usability_profiles. There is no default "
            "because the choice decides what every downstream consumer can read. "
            "Declare at least one profile, e.g. 'ctramp: {tour_closes_at: "
            "primary_home, household_day_needs: all_members}'."
        )
        raise ValueError(msg)
    profiles = parse_usability_profiles(usability_profiles)

    tables: dict[str, pl.DataFrame | None] = {
        "households": households,
        "persons": persons,
        "household_days": household_days,
        "days": days,
        "unlinked_trips": unlinked_trips,
        "linked_trips": linked_trips,
        "joint_trips": joint_trips,
        "tours": tours,
        "joint_tours": joint_tours,
    }

    cascade_complete(tables)
    for profile in profiles:
        stamp_usable(tables, profile)
    _log_gate_summary(tables, profiles)

    if canonical_data is not None:
        _register_profile_columns(tables, profiles, canonical_data)

    return {name: df for name, df in tables.items() if df is not None}
