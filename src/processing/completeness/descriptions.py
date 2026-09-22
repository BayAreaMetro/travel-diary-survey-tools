"""What each stamped column means, in words -- for the log and for the writer.

A profile's columns are named in config, so they cannot be model fields. They
are described here instead: once into the run log, so an analyst can see what a
gate admitted, and once onto the canonical data, so the delivered output
carries the same sentence.
"""

import logging

import polars as pl

from data_canon.core.dataclass import CanonicalData

from .profiles import ANY_HOME, ANYWHERE, PRIMARY_HOME, UsabilityProfile

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
    PRIMARY_HOME: "returning to the home it left",
    ANY_HOME: (
        "returning to any home this person is known to have, so a tour closing "
        "at a second residence counts"
    ),
    ANYWHERE: (
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
    gate = (
        f"Usable under the '{profile.name}' profile: survey-complete, a tour "
        f"{_CLOSURE_TEXT[profile.tour_closes_at]}, and {household}{coverage}. Never "
        "admits a tour with a missing leg or no activity to anchor on."
    )
    if profile.needs_whole_household_day:
        hh_day = (
            f"Every surveyable member's day is {profile.flag} on this travel_date. "
            f"A household needs at least one such date to be {profile.flag}."
        )
    else:
        hh_day = (
            f"Every surveyable member's day is {profile.flag} on this travel_date. "
            "Not a gate for this profile, which asks nothing of the household-date; "
            "recorded so a dropped household can still be traced to the dates that "
            "cost it."
        )
    return {profile.flag: gate, profile.household_day: hh_day}


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
            present = {col: text for col, text in described.items() if col in df.columns}
            if present:
                canonical_data.register_generated_columns(name, present)
