"""Incidence-sum checksums for the weighting pipeline.

Two complementary checks share a common reporting backend:

``check_recode_nulls``
    Runs on a recoded DataFrame *before* aggregation.  Detects records
    where a control column evaluated to null (a gap in the recode
    mapping).  Logs a **warning** but does not raise — the balancer can
    still produce weights (unconstrained records get the neighbourhood
    average).

``check_incidence_sums``
    Runs on the household-level seed table *after* aggregation.  For
    each person-level control it verifies that the incidence columns sum
    to at most ``h_size``.  An **overcount** (sum > expected) is a
    logic bug that would bias the balancer, so it raises ``ValueError``.
"""

import logging
from collections.abc import Iterator

import polars as pl

from processing.weighting.controls.base import ControlLevel, ControlTarget
from processing.weighting.controls.registry import CONTROLS

logger = logging.getLogger(__name__)


# Max number of failures to show in logs (with summary count if more)
_MAX_ROWS = 20


# -- shared helpers ------------------------------------------------------------


def _resolve(
    targets: list[str],
    level: ControlLevel | None = None,
) -> Iterator[tuple[str, ControlTarget]]:
    """Yield ``(name, ctrl)`` for known targets, optionally filtered by level."""
    for name in targets:
        ctrl = CONTROLS.get(name)
        if ctrl is not None and (level is None or ctrl.level == level):
            yield name, ctrl


def _emit(
    rows: list[tuple],
    *,
    source_label: str,
    header: str,
    col_header: str,
    row_fmt: str,
    raise_: bool,
) -> None:
    """Format *rows*, log them, and optionally raise ``ValueError``."""
    if not rows:
        return
    lines = [f"{header} ({source_label}):", col_header]
    lines.extend(row_fmt.format(*row) for row in rows[:_MAX_ROWS])
    n = len(rows)
    if n > _MAX_ROWS:
        lines.append(f"  ... and {n - _MAX_ROWS} more")
    lines.append(f"\n{n} total failure(s).")
    msg = "\n".join(lines)
    if raise_:
        logger.error(msg)
        raise ValueError(msg)
    logger.warning(msg)


# -- public API ---------------------------------------------


def check_recode_nulls(
    df: pl.DataFrame,
    targets: list[str],
    *,
    level: ControlLevel,
    id_col: str,
    source_label: str,
) -> None:
    """Warn about records whose control recode evaluated to null.

    Parameters
    ----------
    df : pl.DataFrame
        Recoded table (households or persons).
    targets : list[str]
        Control registry names to check.
    level : ControlLevel
        Which level of controls to check (``HOUSEHOLD`` or ``PERSON``).
    id_col : str
        Record identifier column (e.g. ``"hh_id"``, ``"person_id"``).
    source_label : str
        Human label for log messages (e.g. ``"PUMS"`` or ``"survey"``).
    """
    rows: list[tuple[str | int, str]] = []
    for name, _ctrl in _resolve(targets, level):
        if name not in df.columns:
            continue
        rows.extend((rid, name) for rid in df.filter(pl.col(name).is_null())[id_col].to_list())

    if not rows:
        logger.info(
            "Null recode check (%s, %s): all records classified",
            level.value,
            source_label,
        )
        return

    _emit(
        rows,
        source_label=source_label,
        header=f"Null recode values ({level.value})",
        col_header=f"  {id_col:<16} {'control':<20}",
        row_fmt="  {:<16} {:<20}",
        raise_=False,
    )


def check_incidence_sums(
    seed: pl.DataFrame,
    targets: list[str],
    *,
    source_label: str,
) -> None:
    """Raise on person-control overcounts in the seed table.

    Only person-level controls are checked — household-level nulls are
    caught earlier by :func:`check_recode_nulls`.

    Parameters
    ----------
    seed : pl.DataFrame
        Household-level seed table with ``hh_id``, ``h_size``, and
        ``{ctrl}__{member}`` incidence columns.
    targets : list[str]
        Control registry names.
    source_label : str
        Label for log messages.

    Raises:
    ------
    ValueError
        If any household has incidence sum > h_size for a person control.
    """
    rows: list[tuple[int | str, str, int, int]] = []

    for name, _ctrl in _resolve(targets, ControlLevel.PERSON):
        inc_cols = [c for c in seed.columns if c.startswith(f"{name}__")]
        if not inc_cols or "h_size" not in seed.columns:
            continue

        check = seed.select(
            "hh_id",
            pl.sum_horizontal(inc_cols).alias("actual"),
            pl.col("h_size").alias("expected"),
        ).filter(pl.col("actual") > pl.col("expected"))

        rows.extend(
            (row["hh_id"], name, int(row["actual"]), int(row["expected"]))
            for row in check.iter_rows(named=True)
        )

    if not rows:
        logger.info(
            "Incidence checksum (%s): all person controls pass",
            source_label,
        )
        return

    _emit(
        rows,
        source_label=source_label,
        header="Incidence overcount failures",
        col_header=f"  {'hh_id':<16} {'control':<20} {'actual':>8} {'expected':>8}",
        row_fmt="  {:<16} {:<20} {:>8} {:>8}",
        raise_=True,
    )
