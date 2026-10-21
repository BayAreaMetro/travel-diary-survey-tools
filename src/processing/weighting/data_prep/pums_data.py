"""PUMS microdata I/O.

Downloads ACS PUMS 1-year microdata from the Census Bureau API (through
``pums_api``) or loads it from local CSV / Parquet files, checks it, and casts
the API's string responses to proper numeric dtypes. Downloads are cached as
Parquet at ``<cache_dir>/pums/{state}_{year}_{hh|person}.parquet``.

Transformation (recoding, aggregation) lives in ``control_data``.
"""

import difflib
import hashlib
import logging
from pathlib import Path

import polars as pl

from processing.weighting.controls.base import ControlLevel
from processing.weighting.controls.registry import pums_variables
from processing.weighting.core.specs import PUMSSource
from processing.weighting.data_prep.pums_api import (
    CENSUS_BASE,
    MAX_COLS_PER_REQUEST,
    MAX_FETCH_ATTEMPTS,
    available_variables,
    fetch_table,
)

logger = logging.getLogger(__name__)


# Infrastructure vars that don't come from any ControlTarget.
# Names use the **2020+** (canonical) convention; for older vintages the
# aliases in ``_PRE2020_ALIASES`` are requested instead and then renamed.
_HH_INFRA = {"SERIALNO", "PUMA", "STATE", "WGTP", "TYPEHUGQ"}


_PERSON_INFRA = {"SERIALNO", "SPORDER", "PUMA", "STATE", "PWGTP"}


# Variable renames between pre-2020 and 2020+ PUMS vintages.
# Keys = canonical (2020+) name, values = pre-2020 name.
_PRE2020_ALIASES: dict[str, str] = {
    "STATE": "ST",
    "TYPEHUGQ": "TYPE",
}


# Replicate weight columns for variance estimation (80 per table)
_HH_REPLICATE_WEIGHTS = {f"WGTP{i}" for i in range(1, 81)}


_PERSON_REPLICATE_WEIGHTS = {f"PWGTP{i}" for i in range(1, 81)}


#: TYPEHUGQ: 1 housing unit, 2 institutional GQ, 3 noninstitutional GQ.
_TYPEHUGQ_DOMAIN = (1, 2, 3)


# Derived dynamically from the registry + infrastructure
_HH_VARS = _HH_INFRA | pums_variables(ControlLevel.HOUSEHOLD)


_PERSON_VARS = _PERSON_INFRA | pums_variables(ControlLevel.PERSON)


def _apply_vintage_aliases(
    variables: list[str],
    available: set[str],
    pums_year: int,
) -> tuple[list[str], dict[str, str]]:
    """Swap canonical variable names for their pre-2020 equivalents when needed.

    Returns ``(api_vars, rename_map)`` where *api_vars* contains the names
    to actually request from the Census API and *rename_map* maps
    ``old_name -> canonical_name`` for post-fetch renaming.
    """
    if pums_year >= 2020 or not available:  # noqa: PLR2004
        return variables, {}

    rename_map: dict[str, str] = {}  # old -> canonical
    out: list[str] = []
    for var in variables:
        alias = _PRE2020_ALIASES.get(var)
        if alias and var not in available and alias in available:
            out.append(alias)
            rename_map[alias] = var
        else:
            out.append(var)
    if rename_map:
        logger.info(
            "Vintage aliases applied (pre-2020): %s",
            {v: k for k, v in rename_map.items()},
        )
    return out, rename_map


def _validate_variables(
    requested: list[str],
    available: set[str],
    label: str,
    *,
    optional: set[str] | None = None,
    aliases: dict[str, str] | None = None,
) -> list[str]:
    """Check *requested* variables against *available* from the Census API.

    If *available* is empty (pre-flight failed), returns *requested*
    unchanged as a fallback.

    Variables in *optional* (e.g. replicate weights) are silently dropped
    with a log message if missing.  Variables in *aliases* whose alias
    **is** present are accepted (they will be swapped later).  All other
    missing variables are **required** — the function raises ``ValueError``
    with the closest matching variable names as suggestions (likely a rename
    across Census vintages).
    """
    if not available:
        return requested

    optional = optional or set()
    aliases = aliases or {}
    valid: list[str] = []
    dropped_optional: list[str] = []
    missing_required: list[tuple[str, list[str]]] = []

    for var in requested:
        if var in available:
            valid.append(var)
        elif var in aliases and aliases[var] in available:
            # Known vintage alias — keep the canonical name; will be
            # swapped for the actual API name in _apply_vintage_aliases.
            valid.append(var)
        elif var in optional:
            dropped_optional.append(var)
        else:
            # Find similar names as suggestions
            similar = difflib.get_close_matches(var, sorted(available), n=5, cutoff=0.5)
            missing_required.append((var, similar))

    if dropped_optional:
        logger.info(
            "%s: %d optional variable(s) not in this vintage (dropped): %s",
            label,
            len(dropped_optional),
            dropped_optional[:10],
        )

    if missing_required:
        lines = []
        for var, similar in missing_required:
            hint = f"  similar: {similar}" if similar else "  (no similar names found)"
            lines.append(f"  - {var}\n{hint}")
        msg = (
            f"{label}: {len(missing_required)} required variable(s) not found in "
            f"this PUMS vintage.  This usually means the variable was renamed "
            f"across Census years.\n" + "\n".join(lines)
        )
        raise ValueError(msg)

    return valid


#: Columns identifying one row of each table. SERIALNO alone repeats once per
#: household member, so persons need SPORDER too.
_HH_KEYS = ("SERIALNO",)


_PERSON_KEYS = ("SERIALNO", "SPORDER")


#: A replicate weight is a perturbation of the full weight, so the two track each
#: other. Measured on clean 2023 California data every replicate correlates 0.77
#: with WGTP; misattributed ones sat at 0.20. This bound is deliberately loose
#: because the sound value is a property of the sample -- another state or
#: vintage need not give 0.77 -- so it is only asked to separate "tracks its
#: weight" from "does not". The sharp test is the agreement bound below.
_MIN_REPLICATE_CORRELATION = 0.5


#: Replicates of the same weight are built the same way, so they must correlate
#: with it to the same degree, whatever that degree is. Sound data agrees to
#: within 0.009 across all 80; a file whose replicates were partly misattributed
#: split into two populations 0.605 apart. Being relative, this needs no
#: assumption about the sound value, which is what makes it the stronger check.
#: It cannot stand alone: if *every* replicate were misattributed they would
#: agree with each other on being wrong, which is what the bound above catches.
_MAX_REPLICATE_CORRELATION_SPREAD = 0.1


def _replicate_weight_problems(
    df: pl.DataFrame, weight_col: str, replicates: set[str]
) -> list[str]:
    """Replicate weights that do not track their own full weight.

    Unlike the household-size check there is no cross-table fact to test these
    against, and a shuffled weight is still a plausible weight -- which is how
    65% of them stayed wrong across months of runs without anything noticing.
    Correlation with the full weight is the invariant that does hold.
    """
    present = sorted(replicates & set(df.columns))
    if weight_col not in df.columns or not present:
        return []
    occupied = df.filter(pl.col(weight_col) > 0)
    if occupied.height < 2:  # noqa: PLR2004 - a correlation needs two points
        return []

    measured = {}
    for col in present:
        corr = occupied.select(pl.corr(weight_col, col)).item()
        if corr is not None:
            measured[col] = corr
    if not measured:
        return []

    problems = []
    adrift = sorted(
        ((c, v) for c, v in measured.items() if v < _MIN_REPLICATE_CORRELATION),
        key=lambda pair: pair[1],
    )
    if adrift:
        worst = ", ".join(f"{c} ({v:.2f})" for c, v in adrift[:5])
        problems.append(
            f"{len(adrift)} of {len(present)} {weight_col} replicate weight(s) do not track "
            f"{weight_col} (correlation below {_MIN_REPLICATE_CORRELATION}): {worst}"
        )

    spread = max(measured.values()) - min(measured.values())
    if spread > _MAX_REPLICATE_CORRELATION_SPREAD:
        low = min(measured, key=lambda c: measured[c])
        high = max(measured, key=lambda c: measured[c])
        problems.append(
            f"{weight_col} replicate weights disagree with each other: correlation with "
            f"{weight_col} ranges {measured[low]:.2f} ({low}) to {measured[high]:.2f} "
            f"({high}), a spread of {spread:.2f} where sound data agrees to within "
            f"{_MAX_REPLICATE_CORRELATION_SPREAD}. Replicates are built alike, so a split "
            "like this means some are on the wrong records"
        )
    return problems


def check_pums_integrity(hh_df: pl.DataFrame, person_df: pl.DataFrame) -> None:
    """Verify PUMS records are internally consistent, or raise.

    Fetching assembles each table from several API requests, so a record can end
    up carrying another record's values while still looking well formed -- right
    keys, plausible numbers, correct row count. These checks look for the
    disagreements that misattribution produces and that nothing downstream can
    see:

    * ``NP`` is the household's own count of its members, so it must equal the
      number of person records sharing that ``SERIALNO``. This is the sharpest
      check available: the two facts come from different tables, so they only
      agree when both are attached to the right household. It found 965
      disagreements in a corrupted fetch and none in a sound one.
    * ``TYPEHUGQ`` distinguishes housing units from the two kinds of group
      quarters and admits nothing else. A foreign value there is proof of
      misattribution -- it is how this class of corruption was first noticed.
    * Keys identify rows, or the tables cannot be joined at all.

    Args:
        hh_df: PUMS household records, before group-quarters filtering.
        person_df: PUMS person records.

    Raises:
        ValueError: If any check fails, naming the count and what it means.
    """
    problems: list[str] = []

    if hh_df["SERIALNO"].n_unique() != hh_df.height:
        problems.append(
            f"SERIALNO repeats in households: {hh_df['SERIALNO'].n_unique():,} distinct "
            f"of {hh_df.height:,} rows"
        )
    if {"SERIALNO", "SPORDER"} <= set(person_df.columns):
        n_unique = person_df.select(["SERIALNO", "SPORDER"]).n_unique()
        if n_unique != person_df.height:
            problems.append(
                f"(SERIALNO, SPORDER) repeats in persons: {n_unique:,} distinct "
                f"of {person_df.height:,} rows"
            )

    if "TYPEHUGQ" in hh_df.columns:
        foreign = hh_df.filter(
            pl.col("TYPEHUGQ").is_not_null() & ~pl.col("TYPEHUGQ").is_in(_TYPEHUGQ_DOMAIN)
        )
        if foreign.height:
            seen = sorted(foreign["TYPEHUGQ"].unique().to_list())[:8]
            problems.append(
                f"{foreign.height:,} household(s) have a TYPEHUGQ outside "
                f"{sorted(_TYPEHUGQ_DOMAIN)}: saw {seen}"
            )

    if "NP" in hh_df.columns and "SERIALNO" in person_df.columns:
        counted = person_df.group_by("SERIALNO").agg(pl.len().alias("_n_persons"))
        joined = (
            hh_df.select("SERIALNO", "NP")
            .join(counted, on="SERIALNO", how="left")
            .with_columns(pl.col("_n_persons").fill_null(0))
        )
        # NP == 0 marks a vacant unit, which correctly has no person records.
        disagree = joined.filter((pl.col("NP") > 0) & (pl.col("NP") != pl.col("_n_persons")))
        if disagree.height:
            problems.append(
                f"{disagree.height:,} household(s) report an NP that disagrees with their "
                "own number of person records"
            )

    problems += _replicate_weight_problems(hh_df, "WGTP", _HH_REPLICATE_WEIGHTS)
    problems += _replicate_weight_problems(person_df, "PWGTP", _PERSON_REPLICATE_WEIGHTS)

    if problems:
        msg = (
            "PUMS records are not internally consistent, so values are attached to "
            "the wrong records:\n  - "
            + "\n  - ".join(problems)
            + "\n\nThis is a fetch or cache defect, not a data-quality question: "
            "these facts are true of the Census extract by construction. If a cache "
            "is in play, delete it and re-fetch."
        )
        raise ValueError(msg)


def _drop_gq(hh_df: pl.DataFrame, person_df: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Drop group-quarters households (TYPEHUGQ != 1) and their persons."""
    if "TYPEHUGQ" not in hh_df.columns:
        return hh_df, person_df
    n_before = len(hh_df)
    hh_df = hh_df.filter(pl.col("TYPEHUGQ") == 1)
    n_dropped = n_before - len(hh_df)
    if n_dropped > 0:
        logger.info(
            "Dropped %d / %d group-quarters HH records (TYPEHUGQ != 1)", n_dropped, n_before
        )

    # Always filter persons to surviving HH serials (handles stale caches
    # where GQ households were already removed but GQ persons were not).
    hh_serials = hh_df["SERIALNO"]
    n_per_before = len(person_df)
    person_df = person_df.filter(pl.col("SERIALNO").is_in(hh_serials))
    n_per_dropped = n_per_before - len(person_df)
    if n_per_dropped > 0:
        logger.info(
            "Dropped %d / %d group-quarters person records",
            n_per_dropped,
            n_per_before,
        )
    return hh_df, person_df


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------
def _cache_paths(
    cache_dir: Path | None,
    source: "PUMSSource",
    hh_vars: list[str],
    person_vars: list[str],
) -> tuple[Path | None, Path | None]:
    """Cache file paths for this variable set, or ``(None, None)`` if uncached.

    The filename carries a hash of the requested variables so a query with
    replicate weights cannot be served the cache of one without them.
    """
    if cache_dir is None:
        return None, None
    var_key = f"{','.join(hh_vars)}|{','.join(person_vars)}"
    digest = hashlib.sha256(var_key.encode()).hexdigest()[:10]
    tag = f"{source.state_fips}_{source.pums_year}_{digest}"
    pums_dir = cache_dir / "pums"
    return pums_dir / f"{tag}_hh.parquet", pums_dir / f"{tag}_person.parquet"


def _load_cached_pums(
    hh_cache: Path | None, per_cache: Path | None
) -> tuple[pl.DataFrame, pl.DataFrame] | None:
    """Load and verify a cached PUMS pair, or ``None`` if there is no cache.

    The cache is verified on the way out, not just on the way in. It outlives
    the run that wrote it, so an unchecked one lets a single bad fetch be
    believed indefinitely -- which is exactly how misattributed replicate
    weights persisted here across months of runs.
    """
    if hh_cache is None or per_cache is None:
        return None
    if not (hh_cache.exists() and per_cache.exists()):
        return None
    hh_df = pl.read_parquet(hh_cache)
    person_df = pl.read_parquet(per_cache)
    logger.info("Loaded PUMS from cache (%d HH, %d persons)", len(hh_df), len(person_df))
    check_pums_integrity(hh_df, person_df)
    return hh_df, person_df


def _fetch_verified_pums(
    base_url: str,
    hh_vars: list[str],
    person_vars: list[str],
    source: "PUMSSource",
    puma_geo: str,
    *,
    hh_rename: dict[str, str],
    per_rename: dict[str, str],
    hh_expected: set[str],
    person_expected: set[str],
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Fetch both tables and return them only once they verify, retrying if not.

    The keyed join guarantees the chunks agree with each other, but it cannot
    see a chunk whose own response is internally wrong -- correct SERIALNOs with
    values shuffled beneath them. Joining on the key then propagates that
    faithfully, and the tables are self-consistent right up until
    :func:`check_pums_integrity` compares facts across them.

    So integrity is a retry condition, not just a failure. The fault is
    transient: the same request returns sound data on a later attempt. Without
    this the check turns an intermittent corruption into an intermittent
    outage, which is what it did the first time it fired in a real run.

    Raises:
        ValueError: If the data is still inconsistent after
            :data:`MAX_FETCH_ATTEMPTS` whole-fetch attempts, which means the
            fault is not the transient one a retry clears.
    """
    last_error: ValueError | None = None
    for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
        hh_df = fetch_table(
            base_url, hh_vars, source.state_fips, puma_geo, label="households", keys=_HH_KEYS
        )
        person_df = fetch_table(
            base_url, person_vars, source.state_fips, puma_geo, label="persons", keys=_PERSON_KEYS
        )
        # Canonical names first, so the checks below can name real columns.
        if hh_rename:
            hh_df = hh_df.rename(hh_rename)
        if per_rename:
            person_df = person_df.rename(per_rename)
        hh_df = _cast_pums_types(hh_df, hh_expected)
        person_df = _cast_pums_types(person_df, person_expected)

        try:
            check_pums_integrity(hh_df, person_df)
        except ValueError as err:
            last_error = err
            logger.warning(
                "PUMS failed its integrity check on attempt %d/%d, re-fetching. %s",
                attempt,
                MAX_FETCH_ATTEMPTS,
                err,
            )
            continue
        if attempt > 1:
            logger.info("PUMS verified on attempt %d", attempt)
        return hh_df, person_df

    msg = (
        f"PUMS failed its integrity check on {MAX_FETCH_ATTEMPTS} independent fetches, "
        f"so this is not the transient fault a retry clears.\n\n{last_error}"
    )
    raise ValueError(msg) from last_error


def fetch_pums_data(
    source: PUMSSource,
    extra_hh_vars: set[str] | None = None,
    extra_person_vars: set[str] | None = None,
    load_replicate_weights: bool = False,
    cache_dir: Path | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Download PUMS household and person microdata from the Census API.

    Args:
        source: State, year, and optional PUMA filter.
        extra_hh_vars: Additional household PUMS variable names to fetch
            beyond the defaults.
        extra_person_vars: Additional person PUMS variable names to fetch
            beyond the defaults.
        load_replicate_weights: If ``True``, also fetch the 80 replicate
            weight columns per table (``WGTP1`` to ``WGTP80`` and
            ``PWGTP1`` to ``PWGTP80``).  Required for MOE-based importance
            calculation.
        cache_dir: If set, raw PUMS data is cached as Parquet files under
            ``cache_dir/pums/``.  Subsequent calls with the same state/year
            load from cache instead of hitting the API.

    Returns:
        Tuple of ``(households, persons)`` Polars DataFrames with PUMS
        data typed to appropriate dtypes.
    """
    hh_extra = extra_hh_vars or set()
    person_extra = extra_person_vars or set()
    if load_replicate_weights:
        hh_extra = hh_extra | _HH_REPLICATE_WEIGHTS
        person_extra = person_extra | _PERSON_REPLICATE_WEIGHTS
    hh_vars = sorted(_HH_VARS | hh_extra)
    person_vars = sorted(_PERSON_VARS | person_extra)

    # Check cache first — filename includes a hash of the requested
    # variable set so different queries (e.g. with/without replicate
    # weights) get independent cache entries.
    hh_cache, per_cache = _cache_paths(cache_dir, source, hh_vars, person_vars)
    cached = _load_cached_pums(hh_cache, per_cache)
    if cached is not None:
        return _drop_gq(*cached)

    dataset_name = f"ACSPUMS1Y{source.pums_year}"
    base_url = f"{CENSUS_BASE}/{source.pums_year}/acs/acs1/pums"
    puma_geo = ",".join(source.puma_ids) if source.puma_ids else "*"
    if source.puma_ids and len(puma_geo) >= MAX_COLS_PER_REQUEST:
        puma_label = f"{len(source.puma_ids)} PUMAs"
    else:
        puma_label = puma_geo
    logger.info(
        "Fetching PUMS from Census API: %s (PUMAs: %s)",
        dataset_name,
        puma_label,
    )

    # Pre-flight: check which variables the API actually exposes for
    # this year.  Raises ValueError for missing control variables (likely
    # renamed across Census vintages) with similar-name suggestions.
    # Replicate weights are optional — silently dropped if absent.
    api_vars = available_variables(base_url)
    optional_vars = _HH_REPLICATE_WEIGHTS | _PERSON_REPLICATE_WEIGHTS
    aliases = _PRE2020_ALIASES if source.pums_year < 2020 else {}  # noqa: PLR2004
    hh_vars = _validate_variables(
        hh_vars,
        api_vars,
        "households",
        optional=optional_vars,
        aliases=aliases,
    )
    person_vars = _validate_variables(
        person_vars,
        api_vars,
        "persons",
        optional=optional_vars,
        aliases=aliases,
    )

    # Swap canonical 2020+ names for their pre-2020 equivalents when needed.
    hh_vars, hh_rename = _apply_vintage_aliases(hh_vars, api_vars, source.pums_year)
    person_vars, per_rename = _apply_vintage_aliases(person_vars, api_vars, source.pums_year)

    hh_df, person_df = _fetch_verified_pums(
        base_url,
        hh_vars,
        person_vars,
        source,
        puma_geo,
        hh_rename=hh_rename,
        per_rename=per_rename,
        hh_expected=_HH_VARS | hh_extra,
        person_expected=_PERSON_VARS | person_extra,
    )

    # Save raw data to cache (before any filtering)
    if cache_dir is not None:
        hh_cache.parent.mkdir(parents=True, exist_ok=True)
        hh_df.write_parquet(hh_cache)
        person_df.write_parquet(per_cache)
        logger.info("Cached PUMS data to %s", hh_cache.parent)

    hh_df, person_df = _drop_gq(hh_df, person_df)

    logger.info(
        "Fetched %d household records, %d person records",
        len(hh_df),
        len(person_df),
    )
    return hh_df, person_df


def load_pums_from_files(
    hh_path: str,
    person_path: str,
    state_fips: str | None = None,
    puma_ids: list[str] | None = None,
    load_replicate_weights: bool = False,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Load PUMS data from local CSV/Parquet files.

    Args:
        hh_path: Path to household PUMS file (CSV or Parquet).
        person_path: Path to person PUMS file (CSV or Parquet).
        state_fips: Optional filter to a specific state FIPS code.
        puma_ids: Optional filter to specific PUMAs.
        load_replicate_weights: If ``True``, retain ``WGTP1`` to ``WGTP80``
            and ``PWGTP1`` to ``PWGTP80`` replicate weight columns.

    Returns:
        Tuple of ``(households, persons)`` Polars DataFrames.
    """
    hh_ext = hh_path.rsplit(".", 1)[-1].lower()
    person_ext = person_path.rsplit(".", 1)[-1].lower()

    if hh_ext == "parquet":
        hh_df = pl.read_parquet(hh_path)
    else:
        hh_df = pl.read_csv(hh_path, infer_schema_length=10_000)

    if person_ext == "parquet":
        person_df = pl.read_parquet(person_path)
    else:
        person_df = pl.read_csv(person_path, infer_schema_length=10_000)

    # Determine expected vars for type casting
    hh_known = set(hh_df.columns) & _HH_VARS
    person_known = set(person_df.columns) & _PERSON_VARS
    if load_replicate_weights:
        hh_known |= set(hh_df.columns) & _HH_REPLICATE_WEIGHTS
        person_known |= set(person_df.columns) & _PERSON_REPLICATE_WEIGHTS

    # Cast types
    hh_df = _cast_pums_types(hh_df, hh_known)
    person_df = _cast_pums_types(person_df, person_known)

    hh_df, person_df = _drop_gq(hh_df, person_df)

    if state_fips is not None and "ST" in hh_df.columns:
        hh_df = hh_df.filter(pl.col("ST") == state_fips)
        person_df = person_df.filter(pl.col("ST") == state_fips)

    if puma_ids is not None and "PUMA" in hh_df.columns:
        hh_df = hh_df.filter(pl.col("PUMA").is_in(puma_ids))
        person_df = person_df.filter(pl.col("PUMA").is_in(puma_ids))

    logger.info(
        "Loaded %d household records, %d person records from files",
        len(hh_df),
        len(person_df),
    )
    return hh_df, person_df


def _cast_pums_types(df: pl.DataFrame, expected_vars: set[str]) -> pl.DataFrame:
    """Cast PUMS columns from string (Census API) to numeric types."""
    # String ID columns that should stay as strings
    string_cols = {"SERIALNO", "PUMA", "ST"}

    numeric_casts = []
    for col_name in df.columns:
        if col_name in string_cols:
            numeric_casts.append(pl.col(col_name).cast(pl.Utf8))
        elif col_name in expected_vars:
            # Try int first, fall back to float for income-like fields
            if col_name in ("HINCP", "PWGTP", "WGTP", "WKHP"):
                numeric_casts.append(
                    pl.col(col_name).cast(pl.Utf8).str.strip_chars().cast(pl.Float64, strict=False)
                )
            else:
                numeric_casts.append(
                    pl.col(col_name).cast(pl.Utf8).str.strip_chars().cast(pl.Int32, strict=False)
                )

    if numeric_casts:
        df = df.with_columns(numeric_casts)
    return df
