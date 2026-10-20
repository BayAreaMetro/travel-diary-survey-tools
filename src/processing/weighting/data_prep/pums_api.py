"""The Census API client for PUMS microdata.

* All PUMAs batched in a single API request.
* Column chunking when >48 columns (API limit ~50), parallel via
  ``ThreadPoolExecutor``; the chunks are joined on their record keys rather
  than stacked.
* A response that fails its checks is retried.
* JSON → Polars directly (no pandas intermediate).
* Streaming download with ``tqdm`` progress bars.
"""

import json
import logging
import math
from concurrent.futures import ThreadPoolExecutor, as_completed

import polars as pl
import requests
from tqdm import tqdm

from processing.weighting.data_prep.census_geo import _KEY_SIGNUP_URL, _census_api_key

logger = logging.getLogger(__name__)


# Census API settings
CENSUS_BASE = "https://api.census.gov/data"


MAX_COLS_PER_REQUEST = 48  # Census API caps ~50 variables per GET


_MAX_API_WORKERS = 4


def available_variables(base_url: str) -> set[str]:
    """Fetch the set of valid variable names from the Census API.

    Hits ``{base_url}/variables.json`` (typically <1 MB) and returns
    the set of variable names.  Falls back to an empty set on error
    (skipping validation rather than blocking the pipeline).
    """
    url = f"{base_url}/variables.json"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        return set(data.get("variables", {}).keys())
    except Exception:  # noqa: BLE001
        logger.warning("Could not fetch variable list from %s — skipping validation", url)
        return set()


#: A response that fails its own integrity checks is retried this many times
#: before giving up. The corruption seen here is intermittent -- the same request
#: returns sound data on a later attempt -- so a retry turns a failed run into a
#: transparent recovery. It is deliberately small: repeated failure means
#: something systematic, and looping would only delay saying so.
MAX_FETCH_ATTEMPTS = 4


def _census_get(
    base_url: str,
    cols: list[str],
    state_fips: str,
    puma_geo: str,
    *,
    label: str = "",
    keys: tuple[str, ...] = (),
) -> list[list[str]]:
    """Fetch one Census API request, verify it, and retry it if it is unsound.

    A response is only returned once it is rectangular, carries every requested
    column, and -- when *keys* is given -- identifies each row uniquely. The API
    intermittently returns responses that fail those checks, and the failure is
    not visible in the data afterwards: values are shifted between columns or
    rows are duplicated, all of them plausible. Retrying gets sound data.

    Args:
        base_url: Census API endpoint for the dataset and vintage.
        cols: Variables to request.
        state_fips: State to fetch, e.g. ``"06"``.
        puma_geo: PUMA filter, or ``"*"`` for all.
        label: Progress-bar and log label.
        keys: Columns that must uniquely identify a row of this response. Empty
            skips that check, for requests that do not carry a full key.

    Raises:
        RuntimeError: On HTTP or API errors, or if the response is still unsound
            after :data:`MAX_FETCH_ATTEMPTS` attempts.
    """
    last_error: RuntimeError | None = None
    for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
        rows = _census_get_once(base_url, cols, state_fips, puma_geo, label=label)
        try:
            _check_response_shape(rows, cols, base_url)
            _check_response_keys(rows, keys, base_url)
        except RuntimeError as err:
            last_error = err
            logger.warning(
                "%s: response failed its integrity check on attempt %d/%d, retrying. %s",
                label or "Census API",
                attempt,
                MAX_FETCH_ATTEMPTS,
                err,
            )
            continue
        return rows

    msg = (
        f"Census API returned an unsound response {MAX_FETCH_ATTEMPTS} times running, "
        f"so this is not the intermittent fault a retry clears.\n\n{last_error}"
    )
    raise RuntimeError(msg) from last_error


def _check_response_keys(rows: list[list[str]], keys: tuple[str, ...], url: str) -> None:
    """Verify *keys* identify each row of the response uniquely.

    A duplicated key is proof the response is wrong -- PUMS assigns one row per
    household, and one per person within a household -- and it would also make
    the chunk unjoinable. Seen live: a person chunk came back with 6 duplicate
    (SERIALNO, SPORDER) pairs where a narrow request for the same columns had
    none.

    Raises:
        RuntimeError: If any key value repeats.
    """
    if not keys or not rows:
        return
    header = rows[0]
    try:
        positions = [header.index(k) for k in keys]
    except ValueError:
        return
    seen: set[tuple[str, ...]] = set()
    duplicates: list[tuple[str, ...]] = []
    for row in rows[1:]:
        value = tuple(row[i] for i in positions)
        if value in seen:
            duplicates.append(value)
        seen.add(value)
    if duplicates:
        shown = ", ".join("/".join(d) for d in duplicates[:5])
        msg = (
            f"Census API returned {len(duplicates):,} duplicate "
            f"{', '.join(keys)} value(s), which PUMS does not contain: {shown}.\n"
            f"  URL: {url}"
        )
        raise RuntimeError(msg)


def _census_get_once(
    base_url: str,
    cols: list[str],
    state_fips: str,
    puma_geo: str,
    *,
    label: str = "",
) -> list[list[str]]:
    """Execute a single Census API GET and return the JSON rows.

    Streams the response with a ``tqdm`` progress bar when *label* is
    provided.  Raises ``RuntimeError`` on HTTP or API errors.
    """
    params = {
        "get": ",".join(cols),
        "for": f"public use microdata area:{puma_geo}",
        "in": f"state:{state_fips}",
        "key": _census_api_key(),
    }
    resp = requests.get(base_url, params=params, timeout=120, stream=True)
    resp.raise_for_status()

    # A missing/invalid key redirects (HTTP 200) to an HTML "Missing Key" page,
    # so raise_for_status() passes but the body is not JSON.  Fail clearly.
    if "missing_key" in resp.url:
        msg = (
            "Census API request failed: invalid or missing API key.\n"
            f"  URL: {resp.url}\n"
            f"  Ensure CENSUS_KEY is set to a valid key (sign up: {_KEY_SIGNUP_URL})."
        )
        raise RuntimeError(msg)

    total = int(resp.headers.get("content-length", 0))
    chunks: list[bytes] = []
    with tqdm(
        total=total or None,
        unit="B",
        unit_scale=True,
        desc=label or "Census API",
        leave=False,
    ) as bar:
        for chunk in resp.iter_content(chunk_size=64 * 1024):
            chunks.append(chunk)
            bar.update(len(chunk))

    body = b"".join(chunks)
    if total and len(body) != total:
        msg = (
            f"Census API response was truncated: received {len(body):,} of {total:,} "
            f"bytes.\n  URL: {resp.url}\n"
            "A short body can still parse as JSON if it ends on a row boundary, so "
            "this is checked rather than inferred."
        )
        raise RuntimeError(msg)
    try:
        data = json.loads(body)
    except json.JSONDecodeError as e:
        msg = (
            "Census API returned a body that could not be parsed as JSON "
            f"(likely an error page).\n"
            f"  URL: {resp.url}\n"
            f"  First bytes: {body[:200]!r}\n"
            f"  Ensure CENSUS_KEY is set to a valid key (sign up: {_KEY_SIGNUP_URL})."
        )
        raise RuntimeError(msg) from e
    if isinstance(data, dict) and "error" in data:
        msg = f"Census API error: {data['error']}"
        raise RuntimeError(msg)
    return data  # pyright: ignore[reportReturnType]


def _check_response_shape(rows: list[list[str]], requested: list[str], url: str) -> None:
    """Verify the response is rectangular and carries what was asked for.

    Two failures this catches, both of which produce wrong values rather than an
    error. A row with the wrong number of fields shifts every value after the
    defect into its neighbour's column, so a code lands where a count belongs --
    the shape of corruption seen here as TYPEHUGQ holding 12 and 3213 where the
    API itself returns 2 and 3. And a silently omitted column makes the frame
    narrower than requested, which later positional work would misread.

    Raises:
        RuntimeError: If the header is missing, a row is not the header's width,
            or a requested column is absent.
    """
    if not rows:
        msg = f"Census API returned no rows at all.\n  URL: {url}"
        raise RuntimeError(msg)

    header = rows[0]
    width = len(header)
    ragged = [(i, len(row)) for i, row in enumerate(rows[1:], start=1) if len(row) != width]
    if ragged:
        shown = ", ".join(f"row {i} has {n}" for i, n in ragged[:5])
        msg = (
            f"Census API returned {len(ragged):,} row(s) that are not {width} fields "
            f"wide, so values are shifted between columns: {shown}.\n"
            f"  URL: {url}"
        )
        raise RuntimeError(msg)

    missing = [c for c in requested if c not in header]
    if missing:
        msg = f"Census API response omits requested column(s): {', '.join(missing)}.\n  URL: {url}"
        raise RuntimeError(msg)


def _json_to_polars(rows: list[list[str]]) -> pl.DataFrame:
    """Convert Census API JSON (header + data rows) to a Polars DataFrame.

    Shape is verified by :func:`_check_response_shape` before this runs, so the
    positional read below is safe.
    """
    header = rows[0]
    return pl.DataFrame(
        {col: [row[i] for row in rows[1:]] for i, col in enumerate(header)},
        schema=dict.fromkeys(header, pl.Utf8),
    )


def fetch_table(
    base_url: str,
    all_cols: list[str],
    state_fips: str,
    puma_geo: str,
    *,
    label: str = "table",
    keys: tuple[str, ...] = ("SERIALNO",),
) -> pl.DataFrame:
    """Fetch a full PUMS table, chunking columns if needed.

    The Census API limits ~50 variables per request. When *all_cols* exceeds
    that, the request is split into chunks and fetched in parallel, then joined
    back together **on *keys***.

    The join is what makes this safe, and it has to be a join. The API does not
    return a stable row order across requests: two 48-column requests for the
    same 167,075 California households were measured returning the same rows in
    orders differing at 110,255 positions. Stacking the chunks side by side by
    position -- which this did, while discarding each chunk's own copy of the
    key -- therefore attached most values to the wrong record, and left every
    row carrying exactly one key inherited from the first chunk, so the result
    looked well formed. It silently scrambled 65% of the replicate weights on
    every fetch that chunked.

    *keys* must identify a row uniquely and is added to every chunk request:
    ``SERIALNO`` for households, ``(SERIALNO, SPORDER)`` for persons, where
    ``SERIALNO`` alone repeats once per household member.

    Raises:
        ValueError: If a chunk does not carry exactly the rows of the first, or
            if *keys* does not uniquely identify a row -- either would mean the
            join silently dropped or multiplied records.
    """
    if len(all_cols) <= MAX_COLS_PER_REQUEST:
        rows = _census_get(base_url, all_cols, state_fips, puma_geo, label=label, keys=keys)
        df = _json_to_polars(rows)
        logger.info("  %s: %d rows x %d cols", label, len(df), len(df.columns))
        return df

    # Split into chunks, each including every join key.
    # Use smaller chunks to spread work across _MAX_API_WORKERS threads,
    # while still respecting the Census API column limit.
    missing_keys = [k for k in keys if k not in all_cols]
    if missing_keys:
        msg = (
            f"{label}: cannot chunk without {', '.join(missing_keys)} in the requested "
            f"columns -- the chunks are joined on {', '.join(keys)}, so every chunk has "
            "to carry it."
        )
        raise ValueError(msg)

    non_key = [c for c in all_cols if c not in keys]
    cols_per_chunk = min(
        math.ceil(len(non_key) / _MAX_API_WORKERS),
        MAX_COLS_PER_REQUEST - len(keys),  # leave room for the join keys
    )
    chunks: list[list[str]] = []
    for i in range(0, len(non_key), cols_per_chunk):
        chunk = [*keys, *non_key[i : i + cols_per_chunk]]
        chunks.append(chunk)

    n_chunks = len(chunks)
    logger.info(
        "  %s: %d cols across %d requests",
        label,
        len(all_cols),
        n_chunks,
    )

    # Fetch chunks in parallel
    parts: list[pl.DataFrame] = [None] * n_chunks  # type: ignore[list-item]
    with ThreadPoolExecutor(max_workers=min(_MAX_API_WORKERS, n_chunks)) as pool:
        futures = {
            pool.submit(
                _census_get,
                base_url,
                chunk,
                state_fips,
                puma_geo,
                label=f"{label} [{i + 1}/{n_chunks}]",
                keys=keys,
            ): i
            for i, chunk in enumerate(chunks)
        }
        for future in as_completed(futures):
            idx = futures[future]
            parts[idx] = _json_to_polars(future.result())

    # Join chunks on their keys (first chunk is the base, others add columns).
    result = parts[0]
    n_rows = result.height
    key_list = list(keys)
    if result.select(key_list).n_unique() != n_rows:
        msg = (
            f"{label}: {', '.join(keys)} does not uniquely identify a row "
            f"({result.select(key_list).n_unique():,} distinct of {n_rows:,}), so joining "
            "the column chunks on it would multiply records. Add the missing key column."
        )
        raise ValueError(msg)

    for i, part in enumerate(parts[1:], start=1):
        new_cols = [c for c in part.columns if c not in result.columns]
        result = result.join(part.select([*key_list, *new_cols]), on=key_list, how="left")
        if result.height != n_rows:
            msg = (
                f"{label}: joining chunk {i} changed the row count "
                f"({n_rows:,} -> {result.height:,}). The chunk did not carry the same "
                "records as the first one."
            )
            raise ValueError(msg)
        unmatched = result.select(pl.col(new_cols[0]).null_count()).item() if new_cols else 0
        if unmatched:
            msg = (
                f"{label}: {unmatched:,} of {n_rows:,} records found no match in chunk {i}. "
                "The chunks disagree about which records exist."
            )
            raise ValueError(msg)

    logger.info("  %s: %d rows x %d cols", label, len(result), len(result.columns))
    return result
