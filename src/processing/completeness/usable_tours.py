"""The tour fuse: the one place a usability verdict is decided rather than read.

Every other level counts or inherits. A tour is where reporting completeness,
an admissible structure, household-date coherence and zone coverage are ANDed
into a single verdict, and the rest of the cascade flows from it.
"""

import polars as pl

from data_canon.codebook.tours import TourCategory

from .profiles import _ADMITTED_QUALITY, PRIMARY_HOME, UsabilityProfile
from .zone_coverage import _home_zone_ok, _trips_with_zones_per_tour, _zone_expr


def _flag_tours(
    tables: dict[str, pl.DataFrame | None],
    cols: UsabilityProfile,
) -> None:
    """Stamp the usable flag on tours, in place.

    A tour is usable when its structure is admissible, it sits on a coherent
    household-date, and -- where the profile names a geography -- every one of its
    trips can be addressed in it, not merely its own endpoints. Without the coherence term
    CT-RAMP (which reads the tour's flag) would keep a tour the weighting has
    zeroed. ``hh_day_survey_complete`` is available because the reverse cascade runs
    before this.

    Subtours then take their parent's verdict on top of their own: an at-work
    subtour is travel *within* its parent tour, so keeping one whose parent was
    dropped would leave CT-RAMP an AT_WORK tour hanging off a tour that is not
    in the output, and would strand the parent's ``atWork_freq``.
    """
    tours = tables.get("tours")
    if tours is None or "survey_complete" not in tours.columns:
        return

    usable = _tour_usable_expr(
        has_quality="tour_data_quality" in tours.columns,
        has_category="tour_category" in tours.columns,
        closes_at=cols.tour_closes_at,
    ) & _zone_expr(tours, "tours", cols)

    # ...and every leg of it, not only its anchor and primary destination. See
    # _trips_with_zones_per_tour for why the endpoints are not enough.
    per_tour = _trips_with_zones_per_tour(tables, cols)
    if per_tour is not None and "tour_id" in tours.columns:
        tours = tours.join(per_tour, on="tour_id", how="left")
        # A tour with no trips has no leg that could be missing a zone.
        usable = usable & pl.col("_trip_has_zone").fill_null(value=True)
    else:
        per_tour = None

    # The household's own home has to have a zone too, or the tour belongs to
    # a household the consumer cannot write. See _home_zone_ok on why this is
    # joined in rather than rolled down.
    home = _home_zone_ok(tables, cols)
    if home is not None and "hh_id" in tours.columns:
        tours = tours.join(home, on="hh_id", how="left")
        usable = usable & pl.col("_home_zone_ok").fill_null(value=False)
    else:
        home = None

    days = tables.get("days")
    coherence_required = cols.needs_whole_household_day
    if (
        not coherence_required
        or days is None
        or "hh_day_survey_complete" not in days.columns
        or "day_id" not in tours.columns
    ):
        flagged = tours.with_columns(usable.alias(cols.flag))
    else:
        coherence = days.select(
            "day_id", pl.col("hh_day_survey_complete").alias("_hh_day_survey_complete")
        ).unique(subset="day_id")
        flagged = (
            tours.join(coherence, on="day_id", how="left")
            .with_columns(
                (usable & pl.col("_hh_day_survey_complete").fill_null(value=False)).alias(cols.flag)
            )
            .drop("_hh_day_survey_complete")
        )

    if home is not None:
        flagged = flagged.drop("_home_zone_ok")
    if per_tour is not None:
        flagged = flagged.drop("_trip_has_zone")
    tables["tours"] = _flag_subtours_from_parent(flagged, cols)


def _flag_subtours_from_parent(
    tours: pl.DataFrame,
    cols: UsabilityProfile,
) -> pl.DataFrame:
    """Reduce each subtour's usable flag by its parent tour's verdict.

    Primary tours self-reference (``parent_tour_id == tour_id``) and so are
    unaffected. A subtour whose parent is missing entirely is left on its own
    verdict rather than silently dropped -- the parent's absence is a different
    defect, and dropping here would hide it.
    """
    if "parent_tour_id" not in tours.columns or "tour_id" not in tours.columns:
        return tours

    parent_usable = tours.select(
        pl.col("tour_id").alias("parent_tour_id"),
        pl.col(cols.flag).alias("_parent_usable"),
    )
    return (
        tours.join(parent_usable, on="parent_tour_id", how="left")
        .with_columns(
            (pl.col(cols.flag) & pl.col("_parent_usable").fill_null(value=True)).alias(cols.flag)
        )
        .drop("_parent_usable")
    )


def _tour_usable_expr(
    *,
    has_quality: bool,
    has_category: bool,
    closes_at: str,
) -> pl.Expr:
    """Tour-level usability for one closure setting.

    A tour qualifies when its (cascaded) reporting is complete and its quality
    code is one the setting admits:

    * ``primary_home`` -- VALID only: a whole round trip back to the home it
      left. The anchor is home for a home-based tour and the workplace for an
      at-work subtour, so one criterion admits both.
    * ``any_home`` -- also ``OTHER_HOME``. That tour leaves from or returns to
      another home of this person's, such as a second home, rather than the
      primary one; the trips are whole and only the anchor differs.
    * ``anywhere`` -- also the two open ends, ``PARTIAL_DAY_SPLIT`` and
      ``PARTIAL_DIARY_EDGE``. The tour stops somewhere unexpected and you want
      the trips anyway.

    ``NO_DESTINATION`` and ``SPATIAL_GAP`` are admitted by no setting: they mark
    missing data rather than an open end.

    The ``tour_category`` term only applies at ``primary_home``. Past that the
    admitted codes include tours that are open by construction, so a category
    term would reject what the quality term beside it admits -- the two columns
    state the same fact about where a tour ends.

    Args:
        has_quality: Whether the frame carries ``tour_data_quality``.
        has_category: Whether the frame carries ``tour_category``.
        closes_at: One of :data:`TOUR_CLOSES_AT`.
    """
    admitted = [q.value for q in _ADMITTED_QUALITY[closes_at]]

    # Each term is filled before it is combined. A null descriptor means the
    # structure was never established, which is not an admission; leaving the
    # null to propagate would put a three-valued verdict in a boolean column and
    # make every consumer decide separately what an unknown means.
    structural = pl.lit(value=True)
    if has_quality:
        structural = structural & pl.col("tour_data_quality").is_in(admitted).fill_null(value=False)
    if has_category and closes_at == PRIMARY_HOME:
        structural = structural & (
            (pl.col("tour_category") == TourCategory.COMPLETE.value).fill_null(value=False)
        )

    return pl.col("survey_complete").fill_null(value=False) & structural
