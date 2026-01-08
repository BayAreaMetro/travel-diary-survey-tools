"""Codebook definitions for CT-RAMP related enumerations."""

import csv
import itertools
from dataclasses import dataclass
from pathlib import Path

from data_canon.core.labeled_enum import LabeledEnum


class FreeParkingChoice(LabeledEnum):
    """Enumeration for free parking choice categories."""

    PARK_FOR_FREE = 1, "park for free"
    PAY_TO_PARK = 2, "pay to park"


class MandatoryTourFrequency(LabeledEnum):
    """Enumeration for mandatory tour frequency categories."""

    ONE_WORK_TOUR = 1, "one work tour"
    TWO_WORK_TOURS = 2, "two work tours"
    ONE_SCHOOL_TOUR = 3, "one school tour"
    TWO_SCHOOL_TOURS = 4, "two school tours"
    WORK_AND_SCHOOL = 5, "one work tour and one school tour"


class TourComposition(LabeledEnum):
    """Enumeration for tour composition categories."""

    ADULTS_ONLY = 1, "adults only"
    CHILDREN_ONLY = 2, "children only"
    ADULTS_AND_CHILDREN = 3, "adults and children"


class WalkToTransitSubZone(LabeledEnum):
    """Enumeration for walk-to-transit subzone categories."""

    CANNOT_WALK = 0, "cannot walk to transit"
    SHORT_WALK = 1, "short-walk"
    LONG_WALK = 2, "long-walk"


# NOTE: This is basically a re-map of the canonical PersonType enum
# Can we just use that directly instead of redefining here?
class CTRAMPPersonType(LabeledEnum):
    """Enumeration for person type categories."""

    FULL_TIME_WORKER = 1, "Full-time worker"
    PART_TIME_WORKER = 2, "Part-time worker"
    UNIVERSITY_STUDENT = 3, "University student"
    NON_WORKER = 4, "Nonworker"
    RETIRED = 5, "Retired"
    CHILD_NON_DRIVING_AGE = 6, "Child of non-driving age"
    CHILD_DRIVING_AGE = 7, "Child of driving age"
    CHILD_UNDER_5 = 8, "Child too young for school"


class EmploymentCategory(LabeledEnum):
    """Enumeration for employment category."""

    EMPLOYED = 1, "Employed"
    NOT_EMPLOYED = 2, "Not employed"


class StudentCategory(LabeledEnum):
    """Enumeration for student category."""

    STUDENT = 1, "Student"
    NOT_STUDENT = 2, "Not a student"


class CTRAMPMode(LabeledEnum):
    """CTRAMP travel mode codes.

    Reference: TravelModes#tour-and-trip-modes on MTC modeling wiki.
    """

    DRIVE_ALONE = 1, "Drive alone"
    SHARED_RIDE_2 = 2, "Shared ride 2"
    SHARED_RIDE_3_PLUS = 3, "Shared ride 3+"
    WALK = 4, "Walk"
    BIKE = 5, "Bike"
    WALK_TRANSIT_WALK = 6, "Walk-transit-walk"
    DRIVE_TRANSIT_WALK = 7, "Drive-transit-walk"
    WALK_TRANSIT_DRIVE = 8, "Walk-transit-drive"
    SCHOOL_BUS = 9, "School bus"


class TripModeType(LabeledEnum):
    """Enumeration for trip mode type categories."""

    DA = 1, "Drive alone"
    DA_TOLL = 2, "Drive alone - toll"
    SR2 = 3, "Shared ride 2"
    SR2_TOLL = 4, "Shared ride 2 - toll"
    SR3 = 5, "Shared ride 3+"
    SR3_TOLL = 6, "Shared ride 3+ - toll"
    WALK = 7, "Walk"
    BIKE = 8, "Bike"
    WLK_LOC_WLK = 9, "Walk to local bus"
    WLK_LRF_WLK = 10, "Walk to light rail or ferry"
    WLK_EXP_WLK = 11, "Walk to express bus"
    WLK_HVY_WLK = 12, "Walk to heavy rail"
    WLK_COM_WLK = 13, "Walk to commuter rail"
    DRV_LOC_WLK = 14, "Drive to local bus"
    DRV_LRF_WLK = 15, "Drive to light rail or ferry"
    DRV_EXP_WLK = 16, "Drive to express bus"
    DRV_HVY_WLK = 17, "Drive to heavy rail"
    DRV_COM_WLK = 18, "Drive to commuter rail"
    TAXI = 19, "Taxi"
    TNC = 20, "TNC - single party"
    TNC2 = 21, "TNC - shared"


class JTFChoice(LabeledEnum):
    """Enumeration for joint tour frequency choice categories."""

    NONE_PRE_SCHOOL_ONLY = -4, "no joint tours, only pre-school children leave household"
    NONE_FEWER_THAN_2_LEAVE_HH = -3, "no joint tours, fewer than 2 people leave household"
    NONE_SINGLE_PERSON_HH = -2, "no joint tours, single person hh"
    NONE_NONE = 1, "no joint tours"
    ONE_SHOP = 2, "1 shop (S)"
    ONE_MAINT = 3, "1 maintenance (M)"
    ONE_EATOUT = 4, "1 eating out (E)"
    ONE_VISIT = 5, "1 visiting family/friends (V)"
    ONE_DISCR = 6, "1 discretionary (D)"
    TWO_SHOP = 7, "2 shop (SS)"
    ONE_SHOP_ONE_MAINT = 8, "1 shop, 1 maintenance (SM)"
    ONE_SHOP_ONE_EATOUT = 9, "1 shop, 1 eating out (SE)"
    ONE_SHOP_ONE_VISIT = 10, "1 shop, 1 visit (SV)"
    ONE_SHOP_ONE_DISCR = 11, "1 shop, 1 discretionary (SD)"
    TWO_MAINT = 12, "2 maintenance (MM)"
    ONE_MAINT_ONE_EATOUT = 13, "1 maintenance, 1 eating out (ME)"
    ONE_MAINT_ONE_VISIT = 14, "1 maintenance, 1 visit (MV)"
    ONE_MAINT_ONE_DISCR = 15, "1 maintenance, 1 discretionary (MD)"
    TWO_EATOUT = 16, "2 eating out"
    ONE_EATOUT_ONE_VISIT = 17, "1 eating out, 1 visit (EV)"
    ONE_EATOUT_ONE_DISCR = 18, "1 eating out, 1 discretionary (ED)"
    TWO_VISIT = 19, "2 visiting"
    ONE_VISIT_ONE_DISCR = 20, "1 visit, 1 discretionary"
    TWO_DISCR = 21, "2 discretionary"


class WFHChoice(LabeledEnum):
    """Enumeration for work-from-home choice categories."""

    NON_WORKER_OR_NO_WFH = 0, "non-worker or workers who don't work from home"
    WORKS_FROM_HOME = 1, "workers who work from home"


class IMFChoice(LabeledEnum):
    """Enumeration for individual mandatory tour frequency choice categories."""

    ONE_WORK = 1, "one work tour"
    TWO_WORK = 2, "two work tours"
    ONE_SCHOOL = 3, "one school tour"
    TWO_SCHOOL = 4, "two school tours"
    ONE_WORK_ONE_SCHOOL = 5, "one work tour and one school tour"


# Non-mandatory tour frequency alternatives ------------------------
@dataclass(frozen=True)
class Alternative:
    """Data class representing a non-mandatory tour frequency alternative."""

    code: int
    escort: int
    shopping: int
    othmaint: int
    othdiscr: int
    eatout: int
    social: int


def load_alternatives_from_csv(path: str | Path) -> dict[int, Alternative]:
    """Load alternatives from a CSV file."""
    mapping: dict[int, Alternative] = {}
    with Path(path).open(newline="", encoding="utf-8") as fh:
        rdr = csv.DictReader(fh)
        for row in rdr:
            code = int(row["a"])
            mapping[code] = Alternative(
                code=code,
                escort=int(row.get("escort", 0)),
                shopping=int(row.get("shopping", 0)),
                othmaint=int(row.get("othmaint", 0)),
                othdiscr=int(row.get("othdiscr", 0)),
                eatout=int(row.get("eatout", 0)),
                social=int(row.get("social", 0)),
            )
    return mapping


def build_alternatives(
    *, sizes: dict[str, int] | None = None, maxes: dict[str, int] | None = None
) -> dict[int, Alternative]:
    """Build all combinations of alternatives.

    Provide either:
    - `sizes`: mapping field -> number of levels (e.g. 2 for 0..1), OR
    - `maxes`: mapping field -> maximum value (inclusive), which will be converted to sizes by +1.

    The iteration order matches the CSV with fields varying at these rates
    (slowest -> fastest): escort, shopping, othmaint, eatout, social, othdiscr.
    """
    if sizes is None and maxes is None:
        msg = "either sizes or maxes must be provided"
        raise ValueError(msg)

    fields = ["escort", "shopping", "othmaint", "eatout", "social", "othdiscr"]

    # Convert maxes to sizes if needed
    if maxes is not None:
        sizes_map = {k: (maxes.get(k) or 1) + 1 for k in fields}
    else:
        sizes_map = {k: (sizes or {}).get(k) or 2 for k in fields}

    # Generate all combinations
    mapping: dict[int, Alternative] = {}
    for code, vals in enumerate(itertools.product(*[range(sizes_map[f]) for f in fields]), start=1):
        mapping[code] = Alternative(code=code, **dict(zip(fields, vals, strict=False)))
    return mapping


if __name__ == "__main__":
    # Example usage: print all alternatives
    csv_alternatives = load_alternatives_from_csv(
        "tests\\fixtures\\CTRAMP_IndividualNonMandatoryTourFrequencyAlternatives.csv"
    )
    # Use `maxes` (inclusive max frequencies) with the new API
    py_alternatives = build_alternatives(
        maxes={
            "escort": 2,
            "shopping": 1,
            "othmaint": 1,
            "othdiscr": 1,
            "eatout": 1,
            "social": 1,
        }
    )

    # Compare
    bads = 0
    for code in sorted(set(csv_alternatives.keys()).union(py_alternatives.keys())):
        alt_csv = csv_alternatives.get(code)
        alt_py = py_alternatives.get(code)
        if alt_csv != alt_py:
            bads += 1
            print(f"Code {code}: CSV={alt_csv}, PY={alt_py}")  # noqa: T201

    print(f"CTRAMP codebook module executed. Differences found: {bads}")  # noqa: T201
