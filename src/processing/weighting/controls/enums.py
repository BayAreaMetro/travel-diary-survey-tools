"""Custom IntEnum categories for weighting controls.

These define the **output bins** for the 8 collapsing controls where the
canonical survey granularity is reduced to fewer bins for weighting.  The
5 identity controls (income, education, race, ethnicity, age) reuse
canonical ``LabeledEnum`` directly and are not duplicated here.
"""

from enum import Enum, IntEnum
from itertools import product

# -- Household-level -------------------------------------------------------


class HHSizeCategory(IntEnum):
    """Household size bins (1-9, 10+)."""

    SIZE_1 = 1
    SIZE_2 = 2
    SIZE_3 = 3
    SIZE_4 = 4
    SIZE_5 = 5
    SIZE_6 = 6
    SIZE_7 = 7
    SIZE_8 = 8
    SIZE_9 = 9
    SIZE_10_PLUS = 10


class HHWorkersCategory(IntEnum):
    """Number of workers in household (0-4, 5+)."""

    WORKERS_0 = 0
    WORKERS_1 = 1
    WORKERS_2 = 2
    WORKERS_3 = 3
    WORKERS_4 = 4
    WORKERS_5_PLUS = 5


class HHVehiclesCategory(IntEnum):
    """Vehicles available to household (0-5, 6+)."""

    VEH_0 = 0
    VEH_1 = 1
    VEH_2 = 2
    VEH_3 = 3
    VEH_4 = 4
    VEH_5 = 5
    VEH_6_PLUS = 6


class HHChildrenCategory(IntEnum):
    """Number of children in household (0-4, 5+)."""

    CHILDREN_0 = 0
    CHILDREN_1 = 1
    CHILDREN_2 = 2
    CHILDREN_3 = 3
    CHILDREN_4 = 4
    CHILDREN_5_PLUS = 5


# -- Person-level ---------------------------------------------------------


class GenderCategory(IntEnum):
    """Gender bins for weighting (male / female / other).

    PUMS only has binary SEX.
    """

    MALE = 1
    FEMALE = 2


class EmploymentCategory(IntEnum):
    """Employment status for weighting (full / part / not employed)."""

    EMPLOYED_FULL = 1
    EMPLOYED_PART = 2
    NOT_EMPLOYED = 3


class CommuteModeCategory(IntEnum):
    """Commute mode for weighting."""

    NA = 0  # not a worker / doesn't commute
    MOSTLY_REMOTE = 1  # telework > commute (WFH in PUMS JWTRNS=11)
    DRIVE_ALONE = 2
    CARPOOL = 3
    TRANSIT = 4
    WALK = 5
    BIKE = 6
    OTHER = 7


class StudentCategory(IntEnum):
    """Student status (not student / K-12 / college)."""

    NOT_STUDENT = 0
    STUDENT_K12 = 1
    STUDENT_COLLEGE = 2


# -- Structural controls ---------------------------------------------------


class TotalCategory(IntEnum):
    """Single-category enum for h_total / p_total structural controls."""

    TOTAL = 1


# -- Cross-tabulation helpers ----------------------------------------------

_SENTINEL_NAMES = frozenset({"MISSING", "PNTA"})


def make_crosstab_enum(name: str, *control_enums: type[Enum]) -> type[IntEnum]:
    """Create an IntEnum for N-dimensional cross-tabulated control.

    Generates a cartesian product of member names from the input enums,
    excluding sentinel values (MISSING, PNTA). Each composite member is
    assigned a sequential integer value (0, 1, 2, ...).

    Parameters
    ----------
    name : str
        Name for the generated IntEnum class (e.g., "IncomeBySizeCategory").
    *control_enums : type[Enum]
        Base control category enums to cross-tabulate.

    Returns:
    -------
    type[IntEnum]
        New IntEnum with composite members from cartesian product.

    Examples:
    --------
    >>> IncomeSizeCategory = make_crosstab_enum(
    ...     "IncomeSizeCategory",
    ...     IncomeBroad,
    ...     HHSizeCategory
    ... )
    >>> # Creates: INC_UNDER_25K_SIZE_1 = 0, INC_UNDER_25K_SIZE_2 = 1, ...
    """
    # Extract non-sentinel members from each enum
    member_lists = []
    for enum_cls in control_enums:
        members = [m for m in enum_cls if m.name not in _SENTINEL_NAMES]
        member_lists.append(members)

    # Build cartesian product with sequential values
    members = {}
    for idx, member_combo in enumerate(product(*member_lists)):
        # Combine names: INC_UNDER_25K_SIZE_1_WORKERS_0
        composite_name = "_".join(m.name for m in member_combo)
        members[composite_name] = idx

    # Dynamically create IntEnum class
    return IntEnum(name, members)  # pyright: ignore[reportReturnType]
