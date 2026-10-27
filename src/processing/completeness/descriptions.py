"""What each stamped column means, in words -- for the log and for the writer.

A profile's columns are named in config, so they cannot be model fields. They
are described here instead: once into the run log, so an analyst can see what a
gate admitted, and once onto the canonical data, so the delivered output
carries the same sentence.
"""

import logging

import polars as pl

from data_canon.core.dataclass import CanonicalData

from .profiles import TourClosesAt, UsabilityProfile

logger = logging.getLogger(__name__)


def suggest_usability_columns(frame: pl.DataFrame) -> str:
    """A "did you mean" line naming the boolean columns *frame* actually has.

    For a consumer that was pointed at a usability column which is not there.
    The names come from config, so there is no pattern to match on: a rule like
    "ends in _usable" only holds until a project picks a name that does not fit
    it, and then the hint reports none rather than admitting it cannot tell.
    Every boolean column is offered instead. Some will be unrelated, which costs
    a reader one glance; a confident empty answer costs them a debugging session
    in the wrong step.
    """
    boolean = sorted(name for name, dtype in frame.schema.items() if dtype == pl.Boolean)
    if not boolean:
        return "It carries no boolean columns at all."
    return f"Did you mean one of these boolean columns? {', '.join(boolean)}."


_CLOSURE_TEXT = {
    TourClosesAt.PRIMARY_HOME: "returning to the home it left",
    TourClosesAt.ANY_HOME: (
        "returning to any home this person is known to have, so a tour closing "
        "at a second residence counts"
    ),
    TourClosesAt.ANYWHERE: (
        "ending anywhere, so a tour cut by the diary edge or resuming the next "
        "day counts; its trips are whole even though it does not close"
    ),
}


def _describe(profile: UsabilityProfile) -> dict[str, str]:
    """Column descriptions for one profile, for the generated-column registry."""
    household = (
        "every surveyable member's day complete on the same date"
        if profile.needs_whole_household_day
        else (
            "no household-date requirement; a member whose day is missing costs the "
            "others nothing, and the household's weight redistributes onto those "
            "who did report -- which preserves the household count but leaves the "
            "reporters standing in for the whole household"
        )
    )
    coverage = (
        f", with every location holding a '{profile.zone_coverage}' zone (the "
        "household's home included, so a household the consumer cannot write "
        "takes its tours and days with it)"
        if profile.requires_zones
        else ", asking nothing of geography"
    )
    joint = (
        "; a joint tour only where everyone shares every leg and none is work, "
        "work-related, school or escort"
        if profile.requires_fully_joint
        else "; joint tours as the survey detected them"
    )
    gate = (
        f"Usable under the '{profile.name}' profile: survey-complete, a tour "
        f"{_CLOSURE_TEXT[profile.tour_closes_at]}, and {household}{coverage}{joint}. "
        "Never admits a tour with a missing leg or no activity to anchor on."
    )
    joint_tour_id = (
        f"joint_tour_id as the '{profile.name}' profile sees it: the id where this "
        "record is on its joint tour, null where it is not. "
        + (
            "On it only where usable, where everyone on the tour made the same joint "
            "trips, and where the tour has two or more such people with one purpose "
            "that is not work, work-related, school or escort; a person who leaves for a leg "
            "travels alone, as an individual tour."
            if profile.requires_fully_joint
            else "On it where usable and the survey put it in a joint tour."
        )
    )
    joint_trip_id = (
        f"joint_trip_id as the '{profile.name}' profile sees it: the id where this "
        "trip is on its joint trip, null where it is not. "
        + (
            "On it only where usable and where its tour is on its joint tour."
            if profile.requires_fully_joint
            else "On it where usable and the survey put it in a joint trip."
        )
    )
    return {
        profile.flag: gate,
        profile.joint_tour_id: joint_tour_id,
        profile.joint_trip_id: joint_trip_id,
    }


def _describe_household_day(profile: UsabilityProfile) -> str:
    """What ``household_days.<flag>`` means under *profile*."""
    if profile.needs_whole_household_day:
        return (
            f"Every surveyable member's day is {profile.flag} on this travel_date. "
            f"A household needs at least one such date to be {profile.flag}."
        )
    return (
        f"Every surveyable member's day is {profile.flag} on this travel_date. "
        "Not a gate for this profile, which asks nothing of the household-date; "
        "recorded so a dropped household can still be traced to the dates that "
        "cost it."
    )


def _log_gate_summary(
    tables: dict[str, pl.DataFrame | None],
    profiles: list[UsabilityProfile],
) -> None:
    """Log, per table, how many records each profile admits."""
    for profile in profiles:
        lines = [
            f'Usability gate applied: "{profile.flag}".',
            "  " + _describe(profile)[profile.flag],
            "",
            f"  {'table':<16}{'rows':>10}{'survey_complete':>16}"
            f"{profile.flag:>18}{'newly unusable':>16}",
        ]
        for name, df in tables.items():
            if df is None or profile.flag not in df.columns:
                continue
            n = df.height
            n_complete = df.filter(pl.col("survey_complete").fill_null(value=False)).height
            n_usable = df.filter(pl.col(profile.flag)).height
            # Survey data that reported fine and the model still cannot use,
            # which is the column worth reading: the loss this gate adds alone.
            newly_unusable = n_complete - n_usable
            lines.append(
                f"  {name:<16}{n:>10,}{n_complete:>12,}{n_usable:>18,}{newly_unusable:>16,}"
            )
        logger.info("\n".join(lines))


def _register_profile_columns(
    tables: dict[str, pl.DataFrame | None],
    profiles: list[UsabilityProfile],
    canonical_data: CanonicalData,
) -> None:
    """Declare each stamped column to the writer, with what it means."""
    for profile in profiles:
        described = _describe(profile)
        for name, df in tables.items():
            if df is None:
                continue
            if name == "household_days":
                if profile.flag in df.columns:
                    canonical_data.register_generated_columns(
                        name, {profile.flag: _describe_household_day(profile)}
                    )
                continue
            present = {col: text for col, text in described.items() if col in df.columns}
            if present:
                canonical_data.register_generated_columns(name, present)
