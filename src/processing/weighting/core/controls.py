"""Central control definitions for weighting.

Each control is a subclass of ``ControlTarget`` that bundles:

- output categories (``IntEnum`` or ``LabeledEnum``)
- source field names (survey and PUMS) — metadata for validation / PUMS fetch
- two Polars expressions: ``survey_expr()`` and ``pums_expr()``

This is the **single source of truth** for how raw values become control ints.

::

    PUMS  ──pums_expr──>  ControlTarget.categories  <──survey_expr──  Survey

Identity controls
-----------------
Five controls (``h_income``, ``p_education``, ``p_race``, ``p_ethnicity``,
``p_age``) reuse canonical ``LabeledEnum`` directly.  Sentinel values
(MISSING, PNTA) map to ``null``.

Collapsing controls
-------------------
Eight controls define a custom ``IntEnum`` and provide expressions that
collapse raw codes into coarser category ints.
"""

import logging
from enum import Enum

import polars as pl

from data_canon.codebook.households import IncomeBroad
from data_canon.codebook.persons import (
    AgeCategory,
    Education,
    Employment,
    Ethnicity,
    Gender,
    Race,
    SchoolType,
    Student,
)
from data_canon.codebook.pums import (
    PumsEsr,
    PumsHisp,
    PumsJwtrns,
    PumsRac1p,
    PumsSchg,
    PumsSchl,
    PumsSex,
    PumsThresholds,
)
from data_canon.codebook.trips import ModeType
from processing.weighting.core.control_enums import (
    CommuteModeCategory,
    EmploymentCategory,
    GenderCategory,
    HHChildrenCategory,
    HHSizeCategory,
    HHVehiclesCategory,
    HHWorkersCategory,
    StudentCategory,
)

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════

_SENTINEL_NAMES = frozenset({"MISSING", "PNTA"})


def _identity_expr(col: str, categories: type[Enum]) -> pl.Expr:
    """Pass-through valid values, null for sentinels or unknown.

    Basically, we use the canonical enum directly, just map sentinels to null.
    """
    sentinels = [m.value for m in categories if m.name in _SENTINEL_NAMES]
    valid = [m.value for m in categories if m.name not in _SENTINEL_NAMES]
    return (
        pl.when(pl.col(col).is_null() | pl.col(col).is_in(sentinels))
        .then(None)
        .when(pl.col(col).is_in(valid))
        .then(pl.col(col))
        .otherwise(None)
        .cast(pl.Int16)
    )


def _breakpoint_expr(col: str, categories: type[Enum]) -> pl.Expr:
    """Build a when/then chain from a LabeledEnum with ``BREAKPOINTS``.

    Zips ``categories.BREAKPOINTS`` with the non-sentinel members so that
    each breakpoint maps ``col < bp`` → the corresponding member value,
    with the final member as the ``otherwise`` catch-all.
    """
    members = [m for m in categories if m.name not in _SENTINEL_NAMES]
    breakpoints: list[int] = categories.BREAKPOINTS  # type: ignore[attr-defined]
    c = pl.col(col)
    expr = pl.when(c.is_null()).then(None)
    for bp, member in zip(breakpoints, members, strict=False):
        expr = expr.when(c < bp).then(member.value)
    return expr.otherwise(members[-1].value).cast(pl.Int16)


# ══════════════════════════════════════════════════════════════════════════
# Base class
# ══════════════════════════════════════════════════════════════════════════


class ControlLevel(str, Enum):
    """Whether a control is at the household or person level."""

    HOUSEHOLD = "household"
    PERSON = "person"


class ControlTarget:
    """Base class for a single weighting control.

    Subclasses set class attributes and override ``survey_expr`` /
    ``pums_expr`` to return native Polars expressions.

    Attributes (set by subclass)
    ----------------------------
    name : str              -- registry key, e.g. ``"h_size"``
    level : ControlLevel    -- HOUSEHOLD or PERSON
    description : str       -- human-readable label
    categories : type       -- IntEnum or LabeledEnum for output bins
    survey_fields : tuple   -- canonical survey column names (metadata)
    pums_fields : tuple     -- PUMS column names (metadata)
    """

    name: str
    level: ControlLevel
    description: str
    categories: type[Enum]
    survey_fields: tuple[str, ...]
    pums_fields: tuple[str, ...]

    def survey_expr(self) -> pl.Expr:
        """Polars expression mapping survey columns → control int (Int16)."""
        msg = f"{type(self).__name__}.survey_expr() not implemented"
        raise NotImplementedError(msg)

    def pums_expr(self) -> pl.Expr:
        """Polars expression mapping PUMS columns → control int (Int16)."""
        msg = f"{type(self).__name__}.pums_expr() not implemented"
        raise NotImplementedError(msg)

    @property
    def valid_members(self) -> list[tuple[int, str]]:
        """``(value, name)`` for each non-sentinel output category."""
        return [(m.value, m.name) for m in self.categories if m.name not in _SENTINEL_NAMES]


# ══════════════════════════════════════════════════════════════════════════
# Household-level controls
# ══════════════════════════════════════════════════════════════════════════


class HHSizeControl(ControlTarget):
    name = "h_size"
    level = ControlLevel.HOUSEHOLD
    description = "Household size"
    categories = HHSizeCategory
    survey_fields = ("_n_persons",)
    pums_fields = ("NP",)

    def survey_expr(self) -> pl.Expr:
        return pl.col("_n_persons").clip(1, 10).cast(pl.Int16)

    def pums_expr(self) -> pl.Expr:
        return pl.col("NP").clip(1, 10).cast(pl.Int16)


class HHIncomeControl(ControlTarget):
    name = "h_income"
    level = ControlLevel.HOUSEHOLD
    description = "Household income"
    categories = IncomeBroad
    survey_fields = ("income_broad",)
    pums_fields = ("HINCP",)

    def survey_expr(self) -> pl.Expr:
        return _identity_expr("income_broad", IncomeBroad)

    def pums_expr(self) -> pl.Expr:
        return _breakpoint_expr("HINCP", IncomeBroad)


class HHWorkersControl(ControlTarget):
    name = "h_workers"
    level = ControlLevel.HOUSEHOLD
    description = "Workers in household"
    categories = HHWorkersCategory
    survey_fields = ("_n_workers",)
    pums_fields = ("_n_workers",)

    def survey_expr(self) -> pl.Expr:
        return pl.col("_n_workers").clip(0, 5).cast(pl.Int16)

    def pums_expr(self) -> pl.Expr:
        return pl.col("_n_workers").clip(0, 5).cast(pl.Int16)


class HHVehiclesControl(ControlTarget):
    name = "h_vehicles"
    level = ControlLevel.HOUSEHOLD
    description = "Vehicles in household"
    categories = HHVehiclesCategory
    survey_fields = ("num_vehicles",)
    pums_fields = ("VEH",)

    def survey_expr(self) -> pl.Expr:
        return pl.col("num_vehicles").clip(0, 6).cast(pl.Int16)

    def pums_expr(self) -> pl.Expr:
        v = pl.col("VEH")
        return pl.when(v.is_null() | (v < 0)).then(None).otherwise(v.clip(0, 6)).cast(pl.Int16)


class HHChildrenControl(ControlTarget):
    name = "h_children"
    level = ControlLevel.HOUSEHOLD
    description = "Children in household"
    categories = HHChildrenCategory
    survey_fields = ("_n_children",)
    pums_fields = ("_n_children",)

    def survey_expr(self) -> pl.Expr:
        return pl.col("_n_children").clip(0, 5).cast(pl.Int16)

    def pums_expr(self) -> pl.Expr:
        return pl.col("_n_children").clip(0, 5).cast(pl.Int16)


# ══════════════════════════════════════════════════════════════════════════
# Person-level controls
# ══════════════════════════════════════════════════════════════════════════


class GenderControl(ControlTarget):
    name = "p_gender"
    level = ControlLevel.PERSON
    description = "Gender"
    categories = GenderCategory
    survey_fields = ("gender",)
    pums_fields = ("SEX",)

    _survey_map: dict[int, int] = {
        Gender.FEMALE.value: GenderCategory.FEMALE,
        Gender.MALE.value: GenderCategory.MALE,
    }

    _pums_map: dict[int, int] = {
        PumsSex.MALE.value: GenderCategory.MALE,
        PumsSex.FEMALE.value: GenderCategory.FEMALE,
    }

    def survey_expr(self) -> pl.Expr:
        return pl.col("gender").replace_strict(
            self._survey_map,
            default=None,
            return_dtype=pl.Int16,
        )

    def pums_expr(self) -> pl.Expr:
        return pl.col("SEX").replace_strict(
            self._pums_map,
            default=None,
            return_dtype=pl.Int16,
        )


class EmploymentControl(ControlTarget):
    name = "p_employment"
    level = ControlLevel.PERSON
    description = "Employment status"
    categories = EmploymentCategory
    survey_fields = ("employment",)
    pums_fields = ("ESR", "WKHP")

    _survey_map: dict[int, int] = {
        Employment.EMPLOYED_FULLTIME.value: EmploymentCategory.EMPLOYED_FULL,
        Employment.EMPLOYED_PARTTIME.value: EmploymentCategory.EMPLOYED_PART,
        Employment.EMPLOYED_SELF.value: EmploymentCategory.EMPLOYED_FULL,
        Employment.UNEMPLOYED_NOT_LOOKING.value: EmploymentCategory.NOT_EMPLOYED,
        Employment.UNEMPLOYED_LOOKING.value: EmploymentCategory.NOT_EMPLOYED,
        Employment.EMPLOYED_UNPAID.value: EmploymentCategory.NOT_EMPLOYED,
        Employment.EMPLOYED_FURLOUGHED.value: EmploymentCategory.EMPLOYED_PART,
    }

    _pums_employed_esr: list[int] = PumsEsr.EMPLOYED
    _pums_not_employed_esr: list[int] = PumsEsr.NOT_EMPLOYED

    def survey_expr(self) -> pl.Expr:
        return pl.col("employment").replace_strict(
            self._survey_map,
            default=None,
            return_dtype=pl.Int16,
        )

    def pums_expr(self) -> pl.Expr:
        """ESR + WKHP → employment category (WKHP < 35 = part-time)."""
        esr = pl.col("ESR")
        wkhp = pl.col("WKHP")
        return (
            pl.when(esr.is_null() | (esr == PumsEsr.NOT_IN_LABOR_FORCE.value))
            .then(None)
            .when(esr.is_in(self._pums_not_employed_esr))
            .then(EmploymentCategory.NOT_EMPLOYED)
            .when(
                esr.is_in(self._pums_employed_esr)
                & wkhp.is_not_null()
                & (wkhp > 0)
                & (wkhp < PumsThresholds.PART_TIME_HOURS),
            )
            .then(EmploymentCategory.EMPLOYED_PART)
            .when(esr.is_in(self._pums_employed_esr))
            .then(EmploymentCategory.EMPLOYED_FULL)
            .otherwise(None)
            .cast(pl.Int16)
        )


class CommuteModeControl(ControlTarget):
    name = "p_commute_mode"
    level = ControlLevel.PERSON
    description = "Commute mode"
    categories = CommuteModeCategory
    survey_fields = ("work_mode",)
    pums_fields = ("JWTRNS", "JWRIP")

    # ModeType → CommuteModeCategory (intermediate for building _survey_map)
    _mode_type_to_commute: dict[ModeType, int] = {
        ModeType.WALK: CommuteModeCategory.WALK,
        ModeType.BIKE: CommuteModeCategory.BIKE,
        ModeType.BIKESHARE: CommuteModeCategory.BIKE,
        ModeType.SCOOTERSHARE: CommuteModeCategory.OTHER,
        ModeType.CAR: CommuteModeCategory.DRIVE_ALONE,
        ModeType.CARSHARE: CommuteModeCategory.DRIVE_ALONE,
        ModeType.TRANSIT: CommuteModeCategory.TRANSIT,
        ModeType.FERRY: CommuteModeCategory.TRANSIT,
        ModeType.TNC: CommuteModeCategory.OTHER,
        ModeType.TAXI: CommuteModeCategory.OTHER,
        ModeType.SHUTTLE: CommuteModeCategory.OTHER,
        ModeType.SCHOOL_BUS: CommuteModeCategory.OTHER,
        ModeType.LONG_DISTANCE: CommuteModeCategory.OTHER,
        ModeType.OTHER: CommuteModeCategory.OTHER,
        ModeType.MISSING: CommuteModeCategory.NA,
    }

    # Mode.value → CommuteModeCategory  (outer-iterable trick for class scope)
    _survey_map: dict[int, int] = {
        mode.value: mtc.get(mtype, CommuteModeCategory.OTHER)
        for mtc in [_mode_type_to_commute]
        for mode, mtype in ModeType.from_mode().items()
    }

    _pums_map: dict[int, int] = {
        PumsJwtrns.CAR_TRUCK_VAN.value: CommuteModeCategory.DRIVE_ALONE,
        PumsJwtrns.BUS.value: CommuteModeCategory.TRANSIT,
        PumsJwtrns.STREETCAR.value: CommuteModeCategory.TRANSIT,
        PumsJwtrns.SUBWAY.value: CommuteModeCategory.TRANSIT,
        PumsJwtrns.RAILROAD.value: CommuteModeCategory.TRANSIT,
        PumsJwtrns.FERRYBOAT.value: CommuteModeCategory.TRANSIT,
        PumsJwtrns.TAXICAB.value: CommuteModeCategory.OTHER,
        PumsJwtrns.MOTORCYCLE.value: CommuteModeCategory.OTHER,
        PumsJwtrns.BICYCLE.value: CommuteModeCategory.BIKE,
        PumsJwtrns.WALKED.value: CommuteModeCategory.WALK,
        PumsJwtrns.WORKED_AT_HOME.value: CommuteModeCategory.WFH,
        PumsJwtrns.OTHER.value: CommuteModeCategory.OTHER,
    }

    def survey_expr(self) -> pl.Expr:
        """Null work_mode → NA (not a commuter)."""
        wm = pl.col("work_mode")
        return (
            pl.when(wm.is_null())
            .then(CommuteModeCategory.NA)
            .otherwise(
                wm.replace_strict(
                    self._survey_map,
                    default=CommuteModeCategory.OTHER,
                    return_dtype=pl.Int16,
                ),
            )
            .cast(pl.Int16)
        )

    def pums_expr(self) -> pl.Expr:
        """JWRIP >= 2 refines drive-alone to carpool."""
        jwtrns = pl.col("JWTRNS")
        base = jwtrns.replace_strict(
            self._pums_map,
            default=CommuteModeCategory.OTHER,
            return_dtype=pl.Int16,
        )
        return (
            pl.when(jwtrns.is_null() | (jwtrns == 0))
            .then(CommuteModeCategory.NA)
            .when(
                (jwtrns == PumsJwtrns.CAR_TRUCK_VAN.value)
                & pl.col("JWRIP").is_not_null()
                & (pl.col("JWRIP") >= PumsThresholds.CARPOOL_MIN_OCCUPANCY),
            )
            .then(CommuteModeCategory.CARPOOL)
            .otherwise(base)
            .cast(pl.Int16)
        )


class StudentControl(ControlTarget):
    name = "p_student"
    level = ControlLevel.PERSON
    description = "Student status"
    categories = StudentCategory
    survey_fields = ("student", "school_type")
    pums_fields = ("SCHG",)

    _k12_school_types: frozenset[int] = frozenset(
        {
            SchoolType.ATHOME.value,
            SchoolType.DAYCARE.value,
            SchoolType.PRESCHOOL.value,
            SchoolType.HOME_SCHOOL.value,
            SchoolType.ELEMENTARY.value,
            SchoolType.MIDDLE_SCHOOL.value,
            SchoolType.HIGH_SCHOOL.value,
        }
    )

    _college_school_types: frozenset[int] = frozenset(
        {
            SchoolType.VOCATIONAL.value,
            SchoolType.COLLEGE_2YEAR.value,
            SchoolType.COLLEGE_4YEAR.value,
            SchoolType.GRADUATE_SCHOOL.value,
        }
    )

    _pums_map: dict[int, int] = {
        PumsSchg.NOT_ATTENDING.value: StudentCategory.NOT_STUDENT,
        **dict.fromkeys(PumsSchg.K12, StudentCategory.STUDENT_K12),
        **dict.fromkeys(PumsSchg.COLLEGE, StudentCategory.STUDENT_COLLEGE),
    }

    def survey_expr(self) -> pl.Expr:
        stu = pl.col("student")
        stype = pl.col("school_type")
        return (
            pl.when(stu == Student.NONSTUDENT.value)
            .then(StudentCategory.NOT_STUDENT)
            .when(stu == Student.MISSING.value)
            .then(None)
            .when(stype.is_in(list(self._k12_school_types)))
            .then(StudentCategory.STUDENT_K12)
            .when(stype.is_in(list(self._college_school_types)))
            .then(StudentCategory.STUDENT_COLLEGE)
            .otherwise(StudentCategory.STUDENT_COLLEGE)
            .cast(pl.Int16)
        )

    def pums_expr(self) -> pl.Expr:
        return (
            pl.when(pl.col("SCHG").is_null())
            .then(StudentCategory.NOT_STUDENT)
            .otherwise(
                pl.col("SCHG").replace_strict(
                    self._pums_map,
                    default=StudentCategory.NOT_STUDENT,
                    return_dtype=pl.Int16,
                ),
            )
            .cast(pl.Int16)
        )


class EducationControl(ControlTarget):
    name = "p_education"
    level = ControlLevel.PERSON
    description = "Education attainment"
    categories = Education
    survey_fields = ("education",)
    pums_fields = ("SCHL",)

    _schl_map: dict[int, int] = {  # type: ignore[dict-item]
        **dict.fromkeys(PumsSchl.LESS_THAN_HS, Education.LESS_HIGH_SCHOOL.value),
        **dict.fromkeys(PumsSchl.HIGH_SCHOOL, Education.HIGHSCHOOL.value),
        **dict.fromkeys(PumsSchl.SOME_COLLEGE, Education.SOME_COLLEGE.value),
        PumsSchl.ASSOCIATE.value: Education.ASSOCIATE.value,
        PumsSchl.BACHELORS.value: Education.BACHELORS.value,
        **dict.fromkeys(PumsSchl.GRADUATE, Education.GRAD.value),
    }

    def survey_expr(self) -> pl.Expr:
        return _identity_expr("education", Education)

    def pums_expr(self) -> pl.Expr:
        return pl.col("SCHL").replace_strict(
            self._schl_map,
            default=None,
            return_dtype=pl.Int16,
        )


class RaceControl(ControlTarget):
    name = "p_race"
    level = ControlLevel.PERSON
    description = "Race"
    categories = Race
    survey_fields = ("race",)
    pums_fields = ("RAC1P",)

    _pums_map: dict[int, int] = {  # type: ignore[dict-item]
        PumsRac1p.WHITE.value: Race.WHITE.value,
        PumsRac1p.BLACK.value: Race.AFAM.value,
        PumsRac1p.AIAN.value: Race.NATIVE.value,
        PumsRac1p.ALASKA_NATIVE.value: Race.NATIVE.value,
        PumsRac1p.AIAN_BOTH.value: Race.NATIVE.value,
        PumsRac1p.ASIAN.value: Race.ASIAN.value,
        PumsRac1p.NHPI.value: Race.PACIFIC.value,
        PumsRac1p.OTHER.value: Race.OTHER.value,
        PumsRac1p.TWO_OR_MORE.value: Race.MULTI.value,
    }

    def survey_expr(self) -> pl.Expr:
        return _identity_expr("race", Race)

    def pums_expr(self) -> pl.Expr:
        return pl.col("RAC1P").replace_strict(
            self._pums_map,
            default=Race.OTHER.value,
            return_dtype=pl.Int16,
        )


class EthnicityControl(ControlTarget):
    name = "p_ethnicity"
    level = ControlLevel.PERSON
    description = "Hispanic/Latino ethnicity"
    categories = Ethnicity
    survey_fields = ("ethnicity",)
    pums_fields = ("HISP",)

    _pums_map: dict[int, int] = {  # type: ignore[dict-item]
        PumsHisp.NOT_HISPANIC.value: Ethnicity.NOT_HISPANIC.value,
        PumsHisp.MEXICAN.value: Ethnicity.MEXICAN.value,
        PumsHisp.PUERTO_RICAN.value: Ethnicity.PUERTO_RICAN.value,
        PumsHisp.CUBAN.value: Ethnicity.CUBAN.value,
    }

    def survey_expr(self) -> pl.Expr:
        return _identity_expr("ethnicity", Ethnicity)

    def pums_expr(self) -> pl.Expr:
        return pl.col("HISP").replace_strict(
            self._pums_map,
            default=Ethnicity.OTHER.value,
            return_dtype=pl.Int16,
        )


class AgeControl(ControlTarget):
    name = "p_age"
    level = ControlLevel.PERSON
    description = "Age"
    categories = AgeCategory
    survey_fields = ("age",)
    pums_fields = ("AGEP",)

    def survey_expr(self) -> pl.Expr:
        return _identity_expr("age", AgeCategory)

    def pums_expr(self) -> pl.Expr:
        return _breakpoint_expr("AGEP", AgeCategory)


# ══════════════════════════════════════════════════════════════════════════
# Registry
# ══════════════════════════════════════════════════════════════════════════

CONTROLS: dict[str, ControlTarget] = {
    t.name: t
    for t in [
        HHSizeControl(),
        HHIncomeControl(),
        HHWorkersControl(),
        HHVehiclesControl(),
        HHChildrenControl(),
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
