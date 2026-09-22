"""Canonical completeness and model-usability logic.

Two kinds of flag roll **up** from two atomic facts -- whether each **trip** was
surveyed, and each tour's structural validity:

* ``survey_complete`` -- was it reported? Rolls up from surveyed trips. One answer per
  run, never narrowed by model criteria and never configurable: if it is wrong,
  the fix belongs upstream in the project cleaner.
* one column per **usability profile** -- can the model consume it? Fuses each
  tour's structure with its household-day coherence, then gates the rest.

A profile states a standard on three axes -- which home has to close a tour,
what the household-date has to show, and which zone system has to be able to
address the record -- and a run may stamp several, so different consumers can
hold different standards. A joint-tour model needs whole households; a
trip-level estimation does not, and needs no model geography at all.

Every profile is named in config and answers every axis, so a column's meaning
reads off the config without knowing a base rule, and no verdict appears that
nobody asked for. Consumers name the profile they read
(``usability_flag_col``), and ``survey_complete`` is always available as the floor
beneath all of them.

Each flag comes from one place in the tree -- its **direction** -- via a counting
**operation** (the ``op`` column below). Naming the direction makes it clear that
``household-day`` and the joint entities are the same kind of thing: cross-person
groupings, not parent/child.

| direction | value comes from               | example                               |
|-----------|--------------------------------|---------------------------------------|
| self      | measured on the record         | a trip never filled out               |
| up        | aggregate your own children    | person with zero complete days        |
| down      | inherit your parent's verdict  | good trip on a dropped tour           |
| lateral   | aggregate a cross-person group | one member skips a day, whole date fails |


The flow, and the rule on each line:

```text
COMPLETE -- rolls up from surveyed trips     op        rule
------------------------------------------------------------------------
trip .................................   direct    trip_survey_complete (measured leaf)
 └ person-day ........................   ALL       all trips surveyed, else declared no-travel
    ├ person .........................   >=1       has >=1 complete day
    └ household-day ..................   ALL       all surveyable members complete that date
       └ household ...................   >=1       has >=1 complete household-day

USABLE -- rolls down from the tour fuse      op        rule
------------------------------------------------------------------------
household ...........................   >=1       has >=1 usable household-day
 ├ household-day ....................   ALL       all surveyable members' days usable that date
 └ person ...........................   >=1       has >=1 usable day
    └ day ...........................   >=1       >=1 usable tour (or a no-travel day)
       └ tour .......................   fuse      complete AND admitted-quality AND
                                                     hh-day complete AND has a zone
                                                     (the last three are the axes)
          ├ linked trip .............   inherit   takes its tour's verdict
          |  └ unlinked trip ........   inherit   takes its linked trip's verdict
          ├ joint tour ..............   >=2       >=2 usable member tours
          └ joint trip ..............   >=2       >=2 usable member linked trips

declared no-travel: num_reasons_no_travel >= 1, OR proxy_complete (a proxy filled the day in --
  this is how children, who file no trips themselves, still get a complete day).
surveyable: persons whose travel the survey could collect at all. Unsurveyable persons
  (unrelated members, e.g. roommates) have no day rows in the vendor data; where a source
  carries any, they neither veto the household-day ALL reductions nor inherit their verdict.
VALID feeds the fuse: trips -> linked trips -> tour, home-to-home, no missing legs.
op: ALL / >=1 / >=2 = quantity gate (count members vs threshold);
    direct = measured; inherit = take a neighbour's verdict; fuse = AND of conditions
```

Because each level reads its neighbours, the derivation order is load-bearing;
getting it wrong fails loudly (an unflagged member table raises), never silently.

This package is the one place the logic lives. The ``cascade_completeness``
pipeline step runs :func:`cascade_complete` once and then :func:`stamp_usable`
per profile; every downstream consumer only *reads* the resulting flags.

Modules:

- ``profiles``: what a profile is, and how config states one.
- ``survey_complete``: the reporting cascade, once per run.
- ``household_day``: the household-date reduction, for both kinds of flag.
- ``zone_coverage``: whether a record's locations have a zone.
- ``usable_tours``: the tour fuse, where a verdict is decided.
- ``usable_cascade``: that verdict, walked across every table.
- ``descriptions``: what each stamped column means, in words.
- ``cascade_completeness``: the pipeline step.
"""

from .cascade_completeness import cascade_completeness
from .descriptions import suggest_usability_columns
from .household_day import flag_household_day_complete, flag_household_day_usable
from .profiles import (
    ALL_MEMBERS,
    ANY_HOME,
    ANYWHERE,
    HOUSEHOLD_DAY_NEEDS,
    NO_ZONE_COVERAGE,
    NOTHING,
    PRIMARY_HOME,
    SURVEY_COMPLETE,
    TOUR_CLOSES_AT,
    USABLE_FAMILY,
    UsabilityProfile,
    parse_usability_profiles,
    usable_col_for,
)
from .survey_complete import cascade_complete, rollup_completeness, rollup_household_complete
from .usable_cascade import MIN_JOINT_PARTICIPANTS, compute_usability, stamp_usable

__all__ = [
    "ALL_MEMBERS",
    "ANYWHERE",
    "ANY_HOME",
    "HOUSEHOLD_DAY_NEEDS",
    "MIN_JOINT_PARTICIPANTS",
    "NOTHING",
    "NO_ZONE_COVERAGE",
    "PRIMARY_HOME",
    "SURVEY_COMPLETE",
    "TOUR_CLOSES_AT",
    "USABLE_FAMILY",
    "UsabilityProfile",
    "cascade_complete",
    "cascade_completeness",
    "compute_usability",
    "flag_household_day_complete",
    "flag_household_day_usable",
    "parse_usability_profiles",
    "rollup_completeness",
    "rollup_household_complete",
    "stamp_usable",
    "suggest_usability_columns",
    "usable_col_for",
]
