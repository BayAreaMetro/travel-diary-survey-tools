"""Generic codebook enums used across multiple data canon modules."""

from data_canon.core.labeled_enum import LabeledEnum


class Select(LabeledEnum):
    """Generic selected/not selected value labels."""

    SELECTED = (1, "Selected")
    NOT_SELECTED = (0, "Not selected")
    MISSING = (995, "Missing Response")


class YesNoMissing(LabeledEnum):
    """Generic yes/no/missing value labels."""

    YES = (1, "Yes")
    NO = (2, "No")
    MISSING = (995, "Missing Response")


class BooleanYesNo(LabeledEnum):
    """Generic boolean yes/no value labels."""

    YES = (1, "Yes")
    NO = (0, "No")
    MISSING = (995, "Missing Response")


class LocationType(LabeledEnum):
    """Classified location type of a trip end (origin or destination).

    Assigned during tour extraction by matching each trip end against a person's
    known locations (home, usual work, usual school) using purpose codes and
    distance thresholds.
    """

    HOME = (1, "Home")
    """The person's home."""

    WORK = (2, "Work")
    """The person's usual/reported workplace."""

    SCHOOL = (3, "School")
    """The person's usual/reported school."""

    OTHER = (4, "Other")
    """Any other location (shopping, social, errands, etc.)."""

    ALTERNATE_WORK = (5, "Alternate work")
    """A work location for the day that is *not* the person's usual workplace.

    Assigned when the person did not visit their usual workplace that day but did
    make work-related trips; the day's work location is taken to be the
    work-related destination where they spent the most time. Trip ends at that
    alternate location are marked ALTERNATE_WORK so the day's actual workplace is
    visible, and the tour anchored there is treated as a work tour (rather than
    work-related).
    """


class LocationSource(LabeledEnum):
    """Provenance of a habitual location in the person-location registry.

    Records *how* a registry location was established, which the wide scalar
    location columns cannot express. See ``PersonLocationModel``.
    """

    REPORTED = (1, "Reported")
    """Taken from the survey's usual home/work/school location questions."""

    OBSERVED = (2, "Observed")
    """Derived from recurring observed travel (e.g. a habitual alternate
    worksite the respondent visits repeatedly but did not report)."""

    IMPUTED = (3, "Imputed")
    """Filled in by imputation when neither reported nor observed."""
