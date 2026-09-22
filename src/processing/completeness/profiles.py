"""What a usability profile is, and how config states one.

A profile answers three axes explicitly -- which home has to close a tour, what
the household-date has to show, and which zone system has to hold every one of
the record's locations. Nothing here touches a table; it is the vocabulary the
rest of the package is written against.
"""

from dataclasses import dataclass

from data_canon.codebook.tours import TourDataQuality

# --- Profile axes ------------------------------------------------------------
# A profile answers every one, explicitly. There is no default for any: a column
# whose meaning depends on a value nobody wrote is the thing profiles exist to
# stop.

# Which home has to close a tour. The quality codes divide into other-home
# anchors, open ends and missing data, and this axis walks down the first two.
PRIMARY_HOME = "primary_home"  # VALID only
ANY_HOME = "any_home"  # + OTHER_HOME
ANYWHERE = "anywhere"  # + PARTIAL_DAY_SPLIT, PARTIAL_DIARY_EDGE
TOUR_CLOSES_AT = (PRIMARY_HOME, ANY_HOME, ANYWHERE)

# What the household-date has to show.
ALL_MEMBERS = "all_members"  # every surveyable member's day complete
NOTHING = "nothing"  # no household-date requirement
HOUSEHOLD_DAY_NEEDS = (ALL_MEMBERS, NOTHING)

# Which zone system has to be able to address the record. Unlike the other two
# axes this vocabulary is open: the value names a zone geography the zone step
# produced, so the legal set is whatever that step was configured to build.
NO_ZONE_COVERAGE = "none"  # no geographic requirement

# A profile is a name -- `ctramp`, `analysis` -- and every column belonging to it
# is that name suffixed onto a family: `usable_ctramp` for the verdict,
# `hh_day_ctramp` for the household-date reduction, `hh_weight_ctramp` for the
# weight fitted over it. One profile, one suffix, one rule per family.
USABLE_FAMILY = "usable"

# Survey reporting completeness, from the vendor. Deliberately not a profile: it
# answers "did we collect this record?", not "do the models take it?", and a
# consumer may name it to mean the whole valid survey. It has no family prefix
# because it belongs to no family.
SURVEY_COMPLETE = "survey_complete"


def usable_col_for(profile: str) -> str:
    """The column carrying *profile*'s verdict.

    Args:
        profile: A usability profile named in config, or
            :data:`SURVEY_COMPLETE` to mean the whole valid survey.

    Returns:
        ``usable_<profile>``, or :data:`SURVEY_COMPLETE` unchanged -- that column
        is vendor data rather than a verdict this pipeline reached, so it is
        available under its own name but not a member of the family.
    """
    if profile == SURVEY_COMPLETE:
        return SURVEY_COMPLETE
    return f"{USABLE_FAMILY}_{profile}"


# Quality codes each closure setting admits, cumulatively. NO_DESTINATION and
# SPATIAL_GAP appear nowhere: they are not open ends but missing data -- no
# tolerance for a tour that stops somewhere unexpected makes a missing leg
# present, or an activity that never happened happen.
_ADMITTED_QUALITY: dict[str, tuple[TourDataQuality, ...]] = {
    PRIMARY_HOME: (TourDataQuality.VALID,),
    ANY_HOME: (TourDataQuality.VALID, TourDataQuality.OTHER_HOME),
    ANYWHERE: (
        TourDataQuality.VALID,
        TourDataQuality.OTHER_HOME,
        TourDataQuality.PARTIAL_DAY_SPLIT,
        TourDataQuality.PARTIAL_DIARY_EDGE,
    ),
}


@dataclass(frozen=True)
class UsabilityProfile:
    """One usability standard, stated on both axes, and the columns it writes.

    A profile says which home has to close a tour, what the household-date has
    to show, and which zone system has to be able to address the record. Config
    gives all three explicitly; none may be left implicit there.

    Coverage is an axis rather than a universal because the zone systems differ:
    a trip outside one model's area may sit comfortably inside another's, so
    "has a zone" is a question only a named consumer can answer.

    Two things hold at every setting and so are not axes. A profile is always a
    subset of ``survey_complete`` -- a usability column admitting unreported records
    would be a different flag wearing the name. And no profile admits a tour
    with *missing data* (a missing leg, an activity that never happened), as
    opposed to one that merely ends somewhere unexpected.

    Two columns come out of a pass. ``flag`` is the per-record verdict consumers
    read. ``household_day`` records, per date, whether *all* surveyable members'
    days passed. It is computed for every profile even where that profile does
    not gate on it, so the column means the same thing everywhere and can still
    answer "which dates cost this household its usability".
    """

    name: str
    tour_closes_at: str
    household_day_needs: str
    # Config must state this like any other axis -- ``_one_profile`` rejects a
    # profile that omits it. The default serves in-process construction only,
    # where saying nothing about geography means asking nothing of it.
    zone_coverage: str = NO_ZONE_COVERAGE

    @property
    def flag(self) -> str:
        """Per-record verdict column."""
        return usable_col_for(self.name)

    @property
    def requires_zones(self) -> bool:
        """Whether this profile asks that a record's locations have a zone."""
        return self.zone_coverage != NO_ZONE_COVERAGE

    @property
    def household_day(self) -> str:
        """All-surveyable-members-that-date column."""
        return f"hh_day_{self.name}"

    @property
    def admitted_quality(self) -> tuple[TourDataQuality, ...]:
        """Tour quality codes this profile's closure setting admits."""
        return _ADMITTED_QUALITY[self.tour_closes_at]

    @property
    def needs_whole_household_day(self) -> bool:
        """Whether a date must show every surveyable member's day complete."""
        return self.household_day_needs == ALL_MEMBERS


_AXES: dict[str, tuple[str, ...]] = {
    "tour_closes_at": TOUR_CLOSES_AT,
    "household_day_needs": HOUSEHOLD_DAY_NEEDS,
}

# The axis whose values come from the zone step's configuration rather than a
# vocabulary fixed here, so it is required and typed but not membership-checked.
_ZONE_AXIS = "zone_coverage"

# Columns the reporting cascade owns. A profile taking one of these names would
# overwrite a survey fact with a modelling judgement.
_RESERVED_NAMES = ("survey_complete", "hh_day_survey_complete")


def parse_usability_profiles(spec: dict[str, dict[str, str]]) -> list[UsabilityProfile]:
    """Turn the configured profile block into profiles, or say why it cannot.

    Every profile answers every axis. A missing axis is an error rather than a
    default, because a default is exactly the implicit meaning profiles exist to
    remove: the config should say what a column means without the reader
    knowing a base rule.

    Args:
        spec: Mapping of profile name to ``{axis: value}``.

    Returns:
        One profile per entry, in declaration order.

    Raises:
        ValueError: If the block is empty, a name collides with a column the
            reporting cascade owns, or any axis is missing, unknown, or given a
            value outside its vocabulary.
    """
    if not spec:
        msg = (
            "usability_profiles is empty, so no usability column would be stamped and "
            "every downstream consumer would have nothing to read. Name at least one "
            "profile, giving every axis: "
            + ", ".join(f"{axis} ({'|'.join(values)})" for axis, values in _AXES.items())
            + f", {_ZONE_AXIS} (a zone_name add_zone_ids builds|{NO_ZONE_COVERAGE})."
        )
        raise ValueError(msg)

    profiles = [_one_profile(name, axes) for name, axes in spec.items()]
    return profiles


def _one_profile(name: str, axes: dict[str, str]) -> UsabilityProfile:
    """Build one profile from its configured axes, or say why it cannot.

    Raises:
        ValueError: If the name is reserved, or any axis is missing, unknown, or
            given a value outside its vocabulary.
    """
    if name in _RESERVED_NAMES:
        msg = (
            f"usability_profile '{name}' collides with a column the reporting "
            f"cascade owns ({', '.join(_RESERVED_NAMES)}). Those record what the "
            "survey collected, not what a model will take; pick another name."
        )
        raise ValueError(msg)
    if not isinstance(axes, dict):
        msg = (
            f"usability_profile '{name}' must give each axis a value, as "
            f"'{{{', '.join(f'{a}: ...' for a in _AXES)}}}'. Got: {axes!r}."
        )
        raise ValueError(msg)  # noqa: TRY004 - malformed config, not a caller type error

    for axis, allowed in _AXES.items():
        if axis not in axes:
            msg = (
                f"usability_profile '{name}' does not say '{axis}'. Every profile "
                f"answers every axis, so a column's meaning can be read off the "
                f"config alone. Legal values: {', '.join(allowed)}."
            )
            raise ValueError(msg)
        if axes[axis] not in allowed:
            msg = (
                f"usability_profile '{name}' sets {axis}: '{axes[axis]}', which is "
                f"not one of {', '.join(allowed)}."
            )
            raise ValueError(msg)

    # Coverage names a geography the zone step builds, so its legal values are
    # not knowable here. It is still required: silently defaulting to "no
    # geographic requirement" is exactly the implicit meaning profiles remove.
    if _ZONE_AXIS not in axes:
        msg = (
            f"usability_profile '{name}' does not say '{_ZONE_AXIS}'. Every profile "
            f"answers every axis. Give the zone_name of a geography add_zone_ids "
            f"builds, or '{NO_ZONE_COVERAGE}' to ask nothing of geography."
        )
        raise ValueError(msg)
    coverage = axes[_ZONE_AXIS]
    if not isinstance(coverage, str) or not coverage:
        msg = (
            f"usability_profile '{name}' sets {_ZONE_AXIS}: {coverage!r}. Give the "
            f"zone_name of a geography add_zone_ids builds, or "
            f"'{NO_ZONE_COVERAGE}'."
        )
        raise ValueError(msg)

    unknown = sorted(set(axes) - set(_AXES) - {_ZONE_AXIS})
    if unknown:
        msg = (
            f"usability_profile '{name}' names unknown axis/axes: "
            f"{', '.join(unknown)}. Known axes: {', '.join([*_AXES, _ZONE_AXIS])}."
        )
        raise ValueError(msg)

    return UsabilityProfile(
        name=name,
        tour_closes_at=axes["tour_closes_at"],
        household_day_needs=axes["household_day_needs"],
        zone_coverage=coverage,
    )
