"""Fractional imputation for null-deficient incidence rows.

When a survey record has a null control value (e.g. missing gender), the
incidence pivot produces all-zero member columns for that control — the
record effectively vanishes from that constraint.  This module replaces
those zeros with probability distributions predicted by a Random Forest
trained on complete PUMS data, implementing *fractional imputation*
(Kim & Fuller, 2004).

The max-entropy balancer accepts non-negative floats in the incidence
matrix, so fractional entries are valid without modification.

Public API
----------
:func:`fill_null_incidence`
    Top-level orchestrator called from the weighting pipeline.
"""

import logging

import numpy as np
import polars as pl
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import f1_score, log_loss
from sklearn.model_selection import cross_val_predict

from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import resolve_targets
from processing.weighting.data_prep.incidence import IncidenceBundle

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Null detection
# ---------------------------------------------------------------------------


def detect_null_rows(
    pivot_df: pl.DataFrame,
    member_columns: list[str],
) -> pl.Series:
    """Return a boolean mask where *all* member columns are zero.

    A row with ``sum(member_columns) == 0`` means the original encoded
    value was null — the pivot produced zeros for every category.

    Works on both person-level pivots (0/1 per person) and household-
    level incidence tables (counts or 0/1).
    """
    return pivot_df.select(pl.sum_horizontal(member_columns).eq(0)).to_series()


# ---------------------------------------------------------------------------
# RF training + probabilistic prediction
# ---------------------------------------------------------------------------


def _train_and_predict(
    train_pivot: pl.DataFrame,
    predict_pivot: pl.DataFrame,
    null_mask: pl.Series,
    member_cols: list[str],
    id_col: str,
    *,
    n_estimators: int = 100,
    random_state: int = 42,
) -> pl.DataFrame:
    """Train an RF classifier on complete rows and predict probabilities for null rows.

    Parameters
    ----------
    train_pivot : pl.DataFrame
        Complete data (PUMS pivot) — no nulls in *member_cols*.
    predict_pivot : pl.DataFrame
        Data with null rows (survey pivot).
    null_mask : pl.Series
        Boolean mask (True = needs prediction) aligned with *predict_pivot*.
    member_cols : list[str]
        The ``{ctrl}__{member}`` columns for the target control.
    id_col : str
        Row identifier column (``"person_id"`` or ``"hh_id"``).
    n_estimators : int
        Number of trees (default 100).
    random_state : int
        Seed for reproducibility.

    Returns:
    -------
    pl.DataFrame
        ``[id_col, member_col_1, member_col_2, ...]`` with predicted
        probabilities for each null row.  Probabilities sum to 1.0 per row.
    """
    # Features = all other __ columns in the pivot (exclude target + id columns)
    all_dunder_cols = sorted(c for c in train_pivot.columns if "__" in c)
    feature_cols = [c for c in all_dunder_cols if c not in member_cols]

    if not feature_cols:
        logger.warning(
            "No feature columns available for predicting %s — skipping",
            member_cols,
        )
        return pl.DataFrame()

    # Build training arrays
    x_train = train_pivot.select(feature_cols).to_numpy().astype(np.float32)
    # Label = which member column is 1 (argmax across member cols)
    y_train = train_pivot.select(member_cols).to_numpy().argmax(axis=1)

    # Skip if only one class present
    n_classes = len(np.unique(y_train))
    if n_classes < 2:  # noqa: PLR2004
        logger.warning(
            "Only %d class(es) in training data for %s — skipping",
            n_classes,
            member_cols,
        )
        return pl.DataFrame()

    clf = RandomForestClassifier(
        n_estimators=n_estimators,
        random_state=random_state,
        n_jobs=-1,
    )
    clf.fit(x_train, y_train)

    # Cross-validated diagnostics on training data
    try:
        cv_proba = cross_val_predict(clf, x_train, y_train, cv=5, method="predict_proba")
        cv_pred = cv_proba.argmax(axis=1)
        ll = log_loss(y_train, cv_proba)
        f1 = f1_score(y_train, cv_pred, average="macro")
        logger.info(
            "  CV diagnostics: log_loss=%.4f, f1_macro=%.4f (%d training rows, %d features)",
            ll,
            f1,
            len(y_train),
            len(feature_cols),
        )
    except (ValueError, IndexError):
        logger.debug("Cross-validation diagnostics skipped", exc_info=True)

    # Predict on null survey rows
    null_rows = predict_pivot.filter(null_mask)
    if null_rows.is_empty():
        return pl.DataFrame()

    x_predict = null_rows.select(feature_cols).to_numpy().astype(np.float32)
    probas = clf.predict_proba(x_predict)

    # Map RF class indices back to member columns
    # clf.classes_ contains the class labels (argmax indices 0..N-1)
    # Ensure probabilities align with member_cols order
    proba_df = pl.DataFrame({member_cols[cls]: probas[:, i] for i, cls in enumerate(clf.classes_)})
    # Fill any member columns not in clf.classes_ with 0
    for col in member_cols:
        if col not in proba_df.columns:
            proba_df = proba_df.with_columns(pl.lit(0.0).alias(col))

    proba_df = proba_df.with_columns(null_rows[id_col].alias(id_col))
    return proba_df


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def _overlay_columns(
    result: pl.DataFrame,
    fractions: pl.DataFrame,
    member_cols: list[str],
    hh_id_col: str,
    *,
    additive: bool,
) -> pl.DataFrame:
    """Overlay fractional predictions onto the incidence table.

    *additive* — if True, add fractions to existing counts (person-level);
    if False, replace zeros with fractions (HH-level).
    """
    for col in member_cols:
        frac_alias = f"_frac_{col}"
        result = result.join(
            fractions.select(hh_id_col, pl.col(col).alias(frac_alias)),
            on=hh_id_col,
            how="left",
        )
        if additive:
            result = result.with_columns(
                (pl.col(col).cast(pl.Float64) + pl.col(frac_alias).fill_null(0.0)).alias(col)
            )
        else:
            result = result.with_columns(
                pl.when(pl.col(frac_alias).is_not_null())
                .then(pl.col(frac_alias))
                .otherwise(pl.col(col))
                .alias(col)
            )
        result = result.drop(frac_alias)
    return result


def _fill_person_controls(
    result: pl.DataFrame,
    survey_bundle: IncidenceBundle,
    pums_bundle: IncidenceBundle,
    targets: list[str],
    hh_id_col: str,
) -> pl.DataFrame:
    """Impute person-level controls via per-person RF → aggregate to HH."""
    p_ctrls = [c for c in resolve_targets(targets, ControlLevel.PERSON) if not c.structural]
    person_id_col = "person_id" if "person_id" in survey_bundle.person_pivot.columns else "SPORDER"

    for ctrl in p_ctrls:
        member_cols = [f"{ctrl.name}__{m.lower()}" for _, m in ctrl.valid_members]
        if any(c not in survey_bundle.person_pivot.columns for c in member_cols):
            continue

        null_mask = detect_null_rows(survey_bundle.person_pivot, member_cols)
        n_null = null_mask.sum()
        if n_null == 0:
            continue

        n_total = len(survey_bundle.person_pivot)
        null_pct = 100.0 * n_null / n_total
        logger.info(
            "Person control '%s': %d/%d persons null (%.1f%%)",
            ctrl.name,
            n_null,
            n_total,
            null_pct,
        )
        if null_pct > 25:  # noqa: PLR2004
            logger.warning(
                "Person control '%s' has %.0f%% null — exceeds 25%%. "
                "Imputation will proceed, but this is inadvisable for "
                "weighting. Consider removing '%s' as a target.",
                ctrl.name,
                null_pct,
                ctrl.name,
            )

        proba_df = _train_and_predict(
            pums_bundle.person_pivot,
            survey_bundle.person_pivot,
            null_mask,
            member_cols,
            id_col=person_id_col,
        )
        if proba_df.is_empty():
            continue

        # Join hh_id onto proba_df for aggregation (proba_df only has person_id)
        person_to_hh = survey_bundle.person_pivot.select(person_id_col, hh_id_col).unique()
        proba_df = proba_df.join(person_to_hh, on=person_id_col, how="left")

        hh_fractions = proba_df.group_by(hh_id_col).agg([pl.col(c).sum() for c in member_cols])
        result = _overlay_columns(result, hh_fractions, member_cols, hh_id_col, additive=True)

    return result


def _fill_hh_controls(
    result: pl.DataFrame,
    pums_bundle: IncidenceBundle,
    targets: list[str],
    hh_id_col: str,
) -> pl.DataFrame:
    """Impute household-level controls via HH-level RF."""
    hh_ctrls = [c for c in resolve_targets(targets, ControlLevel.HOUSEHOLD) if not c.structural]
    for ctrl in hh_ctrls:
        member_cols = [f"{ctrl.name}__{m.lower()}" for _, m in ctrl.valid_members]
        if any(c not in result.columns for c in member_cols):
            continue

        null_mask = detect_null_rows(result, member_cols)
        n_null = null_mask.sum()
        if n_null == 0:
            continue

        n_total = len(result)
        null_pct = 100.0 * n_null / n_total
        logger.info(
            "HH control '%s': %d/%d households null (%.1f%%)",
            ctrl.name,
            n_null,
            n_total,
            null_pct,
        )
        if null_pct > 25:  # noqa: PLR2004
            logger.warning(
                "HH control '%s' has %.0f%% null — exceeds 25%%. "
                "Imputation will proceed, but this is inadvisable for "
                "weighting. Consider removing '%s' as a target.",
                ctrl.name,
                null_pct,
                ctrl.name,
            )

        proba_df = _train_and_predict(
            pums_bundle.incidence, result, null_mask, member_cols, id_col=hh_id_col
        )
        if proba_df.is_empty():
            continue

        result = _overlay_columns(result, proba_df, member_cols, hh_id_col, additive=False)

    return result


def fill_null_incidence(
    survey_bundle: IncidenceBundle,
    pums_bundle: IncidenceBundle,
    targets: list[str],
) -> pl.DataFrame:
    """Replace null-induced zeros in survey incidence with RF-predicted fractions.

    Parameters
    ----------
    survey_bundle : IncidenceBundle
        Survey incidence bundle (may have null-induced zeros).
    pums_bundle : IncidenceBundle
        PUMS incidence bundle (complete, no nulls — used as training data).
    targets : list[str]
        Control registry names.

    Returns:
    -------
    pl.DataFrame
        Modified survey incidence table with fractional values replacing
        zeros for null-deficient rows.
    """
    result = survey_bundle.incidence
    hh_id_col = "hh_id" if "hh_id" in result.columns else "SERIALNO"

    result = _fill_person_controls(result, survey_bundle, pums_bundle, targets, hh_id_col)
    result = _fill_hh_controls(result, pums_bundle, targets, hh_id_col)

    return result
