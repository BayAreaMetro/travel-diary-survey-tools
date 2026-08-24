"""Tour building step for processing travel diary data."""

from data_canon.core.labeled_enum import LabeledEnum


class TourType(LabeledEnum):
    """What a tour is anchored on -- the ``tour_type`` column.

    A tour's anchor is the place it departs from and returns to. Home-based
    tours anchor at home; subtours anchor at the anchor of the tour that
    contains them (see ``detect_anchor_based_subtours``). This is orthogonal to
    :class:`TourCategory`, which says how *completely* a tour is observed
    against whichever anchor it has.
    """

    HOME_BASED = (1, "Home-based tour")
    WORK_BASED = (2, "Work-based tour (at-work subtour)")
    SCHOOL_BASED = (3, "School-based tour (at-school subtour)")


class PersonCategory:
    """Simplified person categories for tour purpose prioritization."""

    WORKER = "worker"
    STUDENT = "student"
    OTHER = "other"


class TourCategory(LabeledEnum):
    """How completely a tour is observed against its own anchor.

    The anchor is home for a home-based tour and the workplace (or campus) for
    a subtour -- see :class:`TourType`. A tour is COMPLETE when it both departs
    from and returns to that anchor, so one criterion admits a home-to-home
    tour and a work-to-work at-work subtour alike. Which anchor applies is
    ``tour_type``; this enum never encodes it.
    """

    COMPLETE = (1, "Start at anchor, end at anchor")
    PARTIAL_END = (2, "Start at anchor, end away from anchor")
    PARTIAL_START = (3, "Start away from anchor, end at anchor")
    PARTIAL_BOTH = (4, "Start away from anchor, end away from anchor")


class TourDirection(LabeledEnum):
    """Half-tour classification.

    Every tour has exactly two halves relative to its own anchor and primary
    destination, at-work subtours included. Subtour *membership* is a property
    of the tour (``parent_tour_id`` / ``subtour_num`` / ``tour_type``), not a
    direction, so it is deliberately not represented here: encoding it as a
    third direction discarded the real direction that DaySim (``half``) and
    CT-RAMP (``inbound``) both require.
    """

    OUTBOUND = (1, "Outbound half-tour")
    INBOUND = (2, "Inbound half-tour")


class TourDataQuality(LabeledEnum):
    """What is structurally wrong with a tour, stated outright.

    A diagnostic column: it says *why* a tour is malformed rather than leaving a
    consumer to reconstruct it from ``tour_category`` and ``trip_count``. The
    codes are deliberately derivable from those two -- being explicit is the
    point -- but they are derived from nothing else. This is a **leaf** fact,
    computed once from the tour's own trips, so the completeness cascade can
    read it without the derivation ever pointing back at ``model_usable``.

    That boundary is what keeps it honest: reporting completeness (``complete``),
    household-date coherence (``hh_day_complete``) and the model gate
    (``model_usable``) live in their own columns and never leak in here. A tour
    can be structurally flawless and still be dropped because a housemate
    skipped that date -- that is not a fact about this tour.

    Values 2-4 mirror :class:`TourCategory` **value for value**, so a shape
    reason carries the same integer in both columns and cross-enum confusion is
    impossible by construction. Value 1 is deliberately unused: it is
    ``TourCategory.COMPLETE``, which is necessary but not sufficient for a valid
    tour -- a loop trip, a change-mode tour and a tour with a missing middle leg
    are all COMPLETE in shape yet still defective. Assigning 1 here would make
    the two columns disagree about what 1 means.
    """

    VALID = (0, "Valid tour")
    # 1 reserved: TourCategory.COMPLETE is never itself a defect.
    PARTIAL_END = (2, "Start at anchor, end away from anchor")
    PARTIAL_START = (3, "Start away from anchor, end at anchor")
    PARTIAL_BOTH = (4, "Start away from anchor, end away from anchor")
    SINGLE_TRIP = (5, "Single-trip tour")
    LOOP_TRIP = (6, "Anchor-to-anchor loop trip")
    CHANGE_MODE = (7, "Change mode as primary purpose (linking failure)")
    SPATIAL_GAP = (8, "Spatial gap between consecutive trips (missing leg)")
