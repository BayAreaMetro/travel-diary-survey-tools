"""Control registry and resolution helpers.

The ``CONTROLS`` dict is the single lookup table mapping control names
to :class:`ControlTarget` instances.  ``resolve_targets`` and
``pums_variables`` provide the main query API used by the rest of the
weighting pipeline.
"""

from processing.weighting.controls.base import ControlLevel, ControlTarget
from processing.weighting.controls.household import (
    HHChildrenControl,
    HHIncomeControl,
    HHSizeControl,
    HHTotalControl,
    HHVehiclesControl,
    HHWorkersControl,
)
from processing.weighting.controls.person import (
    AgeControl,
    CommuteModeControl,
    EducationControl,
    EmploymentControl,
    EthnicityControl,
    GenderControl,
    PersonTotalControl,
    RaceControl,
    StudentControl,
)

# ══════════════════════════════════════════════════════════════════════════
# Registry
# ══════════════════════════════════════════════════════════════════════════

CONTROLS: dict[str, ControlTarget] = {
    t.name: t
    for t in [
        HHTotalControl(),
        HHSizeControl(),
        HHIncomeControl(),
        HHWorkersControl(),
        HHVehiclesControl(),
        HHChildrenControl(),
        PersonTotalControl(),
        GenderControl(),
        EmploymentControl(),
        CommuteModeControl(),
        StudentControl(),
        EducationControl(),
        RaceControl(),
        EthnicityControl(),
        AgeControl(),
    ]
}


def resolve_targets(
    targets: list[str],
    level: ControlLevel | None = None,
) -> list[ControlTarget]:
    """Return ``ControlTarget`` objects for *targets*, optionally filtered."""
    bad = [t for t in targets if t not in CONTROLS]
    if bad:
        msg = f"Unknown targets: {bad}. Valid: {sorted(CONTROLS)}"
        raise ValueError(msg)
    ctrls = [CONTROLS[t] for t in targets]
    if level is not None:
        ctrls = [c for c in ctrls if c.level == level]
    return ctrls


def pums_variables(level: ControlLevel) -> set[str]:
    """PUMS variable names needed for all controls at *level*."""
    return {f for ctrl in CONTROLS.values() if ctrl.level == level for f in ctrl.pums_fields}
