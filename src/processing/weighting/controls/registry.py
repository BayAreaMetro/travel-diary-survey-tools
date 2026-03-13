"""Control registry and resolution helpers.

The ``CONTROLS`` dict is the single lookup table mapping control names
to :class:`ControlTarget` instances.  ``resolve_targets`` and
``pums_variables`` provide the main query API used by the rest of the
weighting pipeline.

Dynamic cross-tab creation:
``register_crosstab`` creates a CrosstabControlTarget instance at runtime
from dimension control names, allowing cross-tabs to be defined in YAML
config without requiring Python class definitions.
"""

import logging

from processing.weighting.controls.base import ControlLevel, ControlTarget, CrosstabControlTarget
from processing.weighting.controls.enums import make_crosstab_enum
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


def register_crosstab(name: str, dimension_names: list[str]) -> ControlTarget:
    """Dynamically create and register a crosstab control from dimension control names.

    Parameters
    ----------
    name : str
        Name for the cross-tab control (will be registered in CONTROLS).
    dimension_names : list[str]
        Names of dimension controls to cross-tabulate (must exist in CONTROLS).

    Returns:
    --------
    ControlTarget
        The newly created and registered CrosstabControlTarget instance.

    Raises:
    -------
    ValueError
        If any dimension control name is not found in CONTROLS registry, or
        if the cross-tab name already exists in CONTROLS.

    Examples:
    ---------
    >>> xtab = register_crosstab("h_size_by_income", ["h_size", "h_income"])
    >>> CONTROLS["h_size_by_income"]  # Now registered
    <CrosstabControlTarget: h_size_by_income>
    """
    # Check if already registered
    if name in CONTROLS:
        msg = f"Control '{name}' already exists in registry"
        raise ValueError(msg)

    # Look up dimension controls
    bad_dims = [d for d in dimension_names if d not in CONTROLS]
    if bad_dims:
        msg = f"Unknown dimension controls: {bad_dims}. Valid: {sorted(CONTROLS)}"
        raise ValueError(msg)

    dim_controls = tuple(CONTROLS[d] for d in dimension_names)

    # Verify all dimensions are at the same level
    levels = {ctrl.level for ctrl in dim_controls}
    if len(levels) > 1:
        msg = f"Cross-tab dimensions must be at the same level. Got: {levels}"
        raise ValueError(msg)

    level = dim_controls[0].level

    # Generate composite enum name (TitleCase)
    enum_name = "".join(d.title().replace("_", "") for d in dimension_names) + "Category"

    # Create enum from dimension enums
    composite_enum = make_crosstab_enum(enum_name, *[ctrl.categories for ctrl in dim_controls])

    # Create description
    dim_desc = " x ".join(ctrl.description for ctrl in dim_controls)
    description = f"{dim_desc} (cross-tab)"

    # Dynamically create CrosstabControlTarget subclass
    xtab_class = type(
        f"{name.title().replace('_', '')}Control",  # Class name
        (CrosstabControlTarget,),  # Base class
        {
            "name": name,
            "level": level,
            "description": description,
            "dim_controls": dim_controls,
            "categories": composite_enum,
            "survey_fields": (),  # Derived from dimensions
            "pums_fields": (),  # Derived from dimensions
        },
    )

    # Instantiate and register
    instance = xtab_class()
    CONTROLS[name] = instance

    return instance


logger = logging.getLogger(__name__)


def register_crosstabs_from_config(controls: list[dict]) -> None:
    """Register any cross-tab controls defined in config before parsing.

    Scans the controls list for entries with a 'dimensions' key and
    dynamically creates and registers CrosstabControlTarget instances.

    Parameters
    ----------
    controls : list[dict]
        Raw control definitions from YAML config.
    """
    for ctrl_def in controls:
        if "dimensions" in ctrl_def:
            name = ctrl_def["name"]
            dimensions = ctrl_def["dimensions"]

            # Skip if already registered (e.g., hardcoded cross-tab)
            if name in CONTROLS:
                logger.debug("Cross-tab '%s' already registered, skipping dynamic creation", name)
                continue

            logger.info(
                "Registering dynamic cross-tab: %s (dimensions: %s)",
                name,
                dimensions,
            )
            register_crosstab(name, dimensions)
