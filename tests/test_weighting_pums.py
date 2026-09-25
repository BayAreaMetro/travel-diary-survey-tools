"""PUMS records must belong to the record they are attached to.

The Census API caps a request at ~50 variables, so a PUMS table is assembled
from several requests and stitched back together. That stitch used to be
positional -- ``hstack`` -- while the API does not return a stable row order
across requests. The result was a table of real values and real keys with most
values on the wrong record, which nothing downstream could detect: every row
carried exactly one key, inherited from the first chunk, and every number was
plausible.

It went unnoticed for months. 65% of the replicate weights in the working cache
were misattributed, and they feed ``moe_based_importance``, so the balancer's
per-control importance was derived from other households' weights.

Two things are tested here. The join is keyed, so row order cannot matter. And
``check_pums_integrity`` catches misattribution independently of the join, using
facts that are true of any correct Census extract -- because the join is not the
only way records can end up mismatched, and the cache outlives the run that
wrote it.
"""

from typing import ClassVar

import polars as pl
import pytest

from processing.weighting.data_prep import pums_data
from processing.weighting.data_prep.pums_data import (
    check_pums_integrity,
    load_pums_from_files,
)

rng = __import__("random").Random(20260831)


def _households(n: int = 400) -> pl.DataFrame:
    """Households whose NP agrees with the person table built beside them."""
    sizes = [rng.randint(1, 5) for _ in range(n)]
    weights = [float(rng.randint(5, 300)) for _ in range(n)]
    return pl.DataFrame(
        {
            "SERIALNO": [f"2023HU{i:07d}" for i in range(n)],
            "NP": sizes,
            "TYPEHUGQ": [1] * n,
            "WGTP": weights,
            # A replicate is the full weight perturbed, so it tracks it closely.
            **{
                f"WGTP{r}": [w * (1 + rng.uniform(-0.15, 0.15)) for w in weights]
                for r in range(1, 6)
            },
        }
    )


def _persons(hh: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for serial, size in zip(hh["SERIALNO"], hh["NP"], strict=True):
        rows.extend((serial, order) for order in range(1, size + 1))
    weights = [float(rng.randint(5, 300)) for _ in rows]
    return pl.DataFrame(
        {
            "SERIALNO": [r[0] for r in rows],
            "SPORDER": [r[1] for r in rows],
            "PWGTP": weights,
            **{
                f"PWGTP{r}": [w * (1 + rng.uniform(-0.15, 0.15)) for w in weights]
                for r in range(1, 6)
            },
        }
    )


@pytest.fixture
def sound() -> tuple[pl.DataFrame, pl.DataFrame]:
    """A household table and the person table that agrees with it."""
    hh = _households()
    return hh, _persons(hh)


class TestSoundRecordsPass:
    """The gate has to be quiet on correct data or it will be switched off."""

    def test_consistent_pums_is_accepted(self, sound):
        """Nothing to report on records that belong together."""
        check_pums_integrity(*sound)

    def test_a_vacant_unit_is_not_a_mismatch(self, sound):
        """NP == 0 marks a vacant unit, which correctly has no person records."""
        hh, persons = sound
        hh = hh.with_columns(
            pl.when(pl.col("SERIALNO") == hh["SERIALNO"][0])
            .then(0)
            .otherwise(pl.col("NP"))
            .alias("NP")
        )
        persons = persons.filter(pl.col("SERIALNO") != hh["SERIALNO"][0])

        check_pums_integrity(hh, persons)

    def test_absent_columns_are_not_failures(self, sound):
        """A query without replicate weights is legitimate, not suspicious."""
        hh, persons = sound
        reps = [c for c in hh.columns if c.startswith("WGTP") and c != "WGTP"]

        check_pums_integrity(hh.drop(reps), persons)


class TestMisattributionIsCaught:
    """Each check corresponds to a way the real corruption showed itself."""

    def test_household_size_disagreeing_with_its_persons(self, sound):
        """The sharpest signal: two tables that only agree when both are right."""
        hh, persons = sound
        hh = hh.with_columns((pl.col("NP") + 1).alias("NP"))

        with pytest.raises(ValueError, match="disagrees with their own number"):
            check_pums_integrity(hh, persons)

    def test_a_type_code_outside_its_domain(self, sound):
        """How this was first noticed -- TYPEHUGQ holding 502 and 3213."""
        hh, persons = sound
        hh = hh.with_columns(
            pl.when(pl.col("SERIALNO") == hh["SERIALNO"][0])
            .then(3213)
            .otherwise(pl.col("TYPEHUGQ"))
            .alias("TYPEHUGQ")
        )

        with pytest.raises(ValueError, match="TYPEHUGQ outside"):
            check_pums_integrity(hh, persons)

    @pytest.mark.parametrize(
        ("table", "column", "seed", "match"),
        [
            pytest.param("households", "WGTP3", 7, "do not track WGTP", id="household"),
            pytest.param("persons", "PWGTP2", 11, "do not track PWGTP", id="person"),
        ],
    )
    def test_shuffled_replicate_weights(self, sound, table, column, seed, match):
        """The corruption that survived months of runs, because it looks normal.

        Every value is a real weight and every row has one; they are simply on
        the wrong records. Only their relationship to the full weight tells, and
        the person table chunks the same way and fails the same way.
        """
        hh, persons = sound
        if table == "households":
            hh = hh.with_columns(hh[column].shuffle(seed=seed).alias(column))
        else:
            persons = persons.with_columns(persons[column].shuffle(seed=seed).alias(column))

        with pytest.raises(ValueError, match=match):
            check_pums_integrity(hh, persons)

    @pytest.mark.parametrize(
        ("table", "match"),
        [
            pytest.param("households", "SERIALNO repeats in households", id="household"),
            pytest.param("persons", r"\(SERIALNO, SPORDER\) repeats", id="person"),
        ],
    )
    def test_a_repeated_key(self, sound, table, match):
        """A key that repeats cannot join the chunks back together.

        SERIALNO alone repeats per member, so persons need SPORDER as well.
        """
        hh, persons = sound
        if table == "households":
            hh = pl.concat([hh, hh.head(1)])
        else:
            persons = pl.concat([persons, persons.head(1)])

        with pytest.raises(ValueError, match=match):
            check_pums_integrity(hh, persons)

    def test_the_error_names_every_problem_at_once(self, sound):
        """One fetch produces several symptoms; fixing them one run at a time is slow."""
        hh, persons = sound
        hh = hh.with_columns(
            (pl.col("NP") + 1).alias("NP"),
            pl.lit(99).alias("TYPEHUGQ"),
            hh["WGTP3"].shuffle(seed=3).alias("WGTP3"),
        )

        with pytest.raises(ValueError, match="not internally consistent") as excinfo:
            check_pums_integrity(hh, persons)

        message = str(excinfo.value)
        assert "TYPEHUGQ outside" in message
        assert "disagrees with their own number" in message
        assert "do not track WGTP" in message


class TestReplicatesMustAgreeWithEachOther:
    """The sharp check is relative, because the sound value is not universal.

    A replicate correlates with its full weight to a degree that is a property
    of the sample -- 0.77 for 2023 California, not necessarily elsewhere. What
    does hold everywhere is that replicates are built alike, so they must agree
    with *each other*. Sound data agrees to within 0.009; a partly misattributed
    file split into populations 0.605 apart.
    """

    def test_a_split_population_is_caught_even_above_the_absolute_bound(self, sound):
        """The case an absolute threshold alone would wave through.

        Both groups here track their weight well enough to clear 0.5; only their
        disagreement with each other gives them away.
        """
        hh, persons = sound
        # Degrade one replicate part-way: still correlated, but visibly less so.
        weakened = hh["WGTP4"] * 0.5 + hh["WGTP4"].shuffle(seed=5) * 0.5
        hh = hh.with_columns(weakened.alias("WGTP4"))

        with pytest.raises(ValueError, match="disagree with each other"):
            check_pums_integrity(hh, persons)


class TestChunksAreJoinedNotStacked:
    """The fix itself: assembling a table from several requests.

    The Census API returns the same rows in different orders for different
    column sets -- measured at 110,255 of 167,075 positions differing between
    two 48-column requests. These drive the real assembly with chunks
    deliberately ordered differently, which is the condition the old positional
    stack could not survive.
    """

    ROWS = 60

    def _fake_api(self, monkeypatch, *, shuffle_after_first: bool) -> None:
        """Serve each requested column set as its own 'response'."""
        serials = [f"2023HU{i:07d}" for i in range(self.ROWS)]
        truth = {
            s: {"A": i, "B": i * 10, "C": i * 100, "D": i * 1000} for i, s in enumerate(serials)
        }
        calls = {"n": 0}

        def fake_get(_base, cols, _state, _puma, *, label="", keys=()):  # noqa: ARG001
            order = list(serials)
            if shuffle_after_first and calls["n"] > 0:
                order.reverse()
            calls["n"] += 1
            header = list(cols)
            rows = [[s if c == "SERIALNO" else str(truth[s][c]) for c in header] for s in order]
            return [header, *rows]

        monkeypatch.setattr(pums_data, "_census_get", fake_get)
        monkeypatch.setattr(pums_data, "_MAX_COLS_PER_REQUEST", 3)
        monkeypatch.setattr(pums_data, "_MAX_API_WORKERS", 2)
        return truth

    def test_values_follow_their_key_when_chunks_are_reordered(self, monkeypatch):
        """The regression. Positional stacking put these on the wrong records."""
        truth = self._fake_api(monkeypatch, shuffle_after_first=True)

        out = pums_data._fetch_table(
            "u", ["SERIALNO", "A", "B", "C", "D"], "06", "*", label="t", keys=("SERIALNO",)
        )

        assert out.height == self.ROWS
        for row in out.iter_rows(named=True):
            expected = truth[row["SERIALNO"]]
            for col in ("A", "B", "C", "D"):
                assert int(row[col]) == expected[col], f"{col} on {row['SERIALNO']}"

    def test_a_non_unique_key_is_refused(self, monkeypatch):
        """Joining persons on SERIALNO alone would multiply rows, not misalign them."""
        serials = ["2023HU0000001", "2023HU0000001", "2023HU0000002"]

        def fake_get(_base, cols, _state, _puma, *, label="", keys=()):  # noqa: ARG001
            return [list(cols), *[[s, "1", "2", "3", "4"][: len(cols)] for s in serials]]

        monkeypatch.setattr(pums_data, "_census_get", fake_get)
        monkeypatch.setattr(pums_data, "_MAX_COLS_PER_REQUEST", 3)
        monkeypatch.setattr(pums_data, "_MAX_API_WORKERS", 2)

        with pytest.raises(ValueError, match="does not uniquely identify a row"):
            pums_data._fetch_table(
                "u", ["SERIALNO", "A", "B", "C"], "06", "*", label="t", keys=("SERIALNO",)
            )

    def test_a_key_missing_from_the_request_is_refused(self, monkeypatch):
        """Every chunk must carry the key, so the key has to be requested."""
        monkeypatch.setattr(pums_data, "_MAX_COLS_PER_REQUEST", 3)

        with pytest.raises(ValueError, match="cannot chunk without SPORDER"):
            pums_data._fetch_table(
                "u",
                ["SERIALNO", "A", "B", "C"],
                "06",
                "*",
                label="t",
                keys=("SERIALNO", "SPORDER"),
            )


class TestTheResponseIsCheckedBeforeItIsTrusted:
    """Guards at the point of parsing, where a bad response is still legible.

    The join protects against chunks disagreeing with each other. These protect
    against a single response being wrong on its own -- which happened here, in
    one chunk, past a sharp row boundary, and which no join can prevent.
    """

    HEADER: ClassVar[list[str]] = ["SERIALNO", "NP", "TYPEHUGQ"]

    def test_a_sound_response_passes(self):
        """Rectangular and complete: nothing to say."""
        rows = [self.HEADER, ["2023HU0000001", "2", "1"], ["2023HU0000002", "3", "1"]]

        pums_data._check_response_shape(rows, self.HEADER, "u")

    @pytest.mark.parametrize(
        ("bad_row", "match"),
        [
            # One missing field shifts every value after it into the next
            # column.  This is the shape of the corruption that put 12 and 3213
            # into TYPEHUGQ where the API returns 2 and 3.
            pytest.param(["2023HU0000002", "3"], "row 2 has 2", id="short"),
            # An extra field is equally disqualifying, and would be dropped.
            pytest.param(
                ["2023HU0000002", "3", "1", "surplus"],
                "row 2 has 4",
                id="long",
            ),
        ],
    )
    def test_a_row_of_the_wrong_width_is_refused(self, bad_row, match):
        """And the message names the row, which is what makes it investigable."""
        rows = [self.HEADER, ["2023HU0000001", "2", "1"], bad_row]

        with pytest.raises(RuntimeError, match=match):
            pums_data._check_response_shape(rows, self.HEADER, "u")

    def test_a_silently_omitted_column_is_refused(self):
        """Asking for a variable and not getting it must not pass unnoticed."""
        rows = [["SERIALNO", "NP"], ["2023HU0000001", "2"]]

        with pytest.raises(RuntimeError, match="omits requested column"):
            pums_data._check_response_shape(rows, self.HEADER, "u")

    def test_an_empty_response_is_refused(self):
        """No header means nothing can be positioned at all."""
        with pytest.raises(RuntimeError, match="no rows at all"):
            pums_data._check_response_shape([], self.HEADER, "u")


class TestAnUnsoundResponseIsRetried:
    """Detection alone would turn an intermittent fault into an intermittent outage.

    The corruption is not reproducible: the same request returns sound data on a
    later attempt. Observed live, a person chunk came back with 6 duplicate
    (SERIALNO, SPORDER) pairs where a narrow request for the same columns had
    none. Retrying is what makes the guard usable rather than merely correct.
    """

    HEADER: ClassVar[list[str]] = ["SERIALNO", "SPORDER", "AGEP"]
    SOUND: ClassVar[list[list[str]]] = [
        HEADER,
        ["2023HU0000001", "1", "40"],
        ["2023HU0000001", "2", "38"],
    ]
    DUPLICATED: ClassVar[list[list[str]]] = [
        HEADER,
        ["2023HU0000001", "1", "40"],
        ["2023HU0000001", "1", "38"],
    ]

    def _serve(self, monkeypatch, responses):
        served = iter(responses)
        calls = {"n": 0}

        def once(*_args, **_kwargs):
            calls["n"] += 1
            return next(served)

        monkeypatch.setattr(pums_data, "_census_get_once", once)
        return calls

    def test_a_transient_fault_is_retried_and_recovers(self, monkeypatch):
        """One bad response then a good one: the caller never sees the fault."""
        calls = self._serve(monkeypatch, [self.DUPLICATED, self.SOUND])

        rows = pums_data._census_get("u", self.HEADER, "06", "*", keys=("SERIALNO", "SPORDER"))

        assert rows == self.SOUND
        assert calls["n"] == 2

    def test_a_sound_response_is_not_retried(self, monkeypatch):
        """Retrying good data would double every fetch."""
        calls = self._serve(monkeypatch, [self.SOUND])

        pums_data._census_get("u", self.HEADER, "06", "*", keys=("SERIALNO", "SPORDER"))

        assert calls["n"] == 1

    def test_persistent_corruption_gives_up_and_says_so(self, monkeypatch):
        """Repeated failure is systematic, and looping only delays reporting it.

        The last failure's own message has to survive the give-up, or the report
        says only that it failed.
        """
        self._serve(monkeypatch, [self.DUPLICATED] * pums_data._MAX_FETCH_ATTEMPTS)

        with pytest.raises(RuntimeError, match="times running") as excinfo:
            pums_data._census_get("u", self.HEADER, "06", "*", keys=("SERIALNO", "SPORDER"))
        assert "duplicate SERIALNO, SPORDER" in str(excinfo.value)

    def test_no_keys_means_no_uniqueness_requirement(self, monkeypatch):
        """Some requests legitimately carry no full key; they must not be rejected."""
        calls = self._serve(monkeypatch, [self.DUPLICATED])

        pums_data._census_get("u", self.HEADER, "06", "*")

        assert calls["n"] == 1


# ===========================================================================
# Reading PUMS off disk instead of the API
# ===========================================================================


# ---------------------------------------------------------------------------
# load_pums_from_files
# ---------------------------------------------------------------------------
class TestLoadPumsFromFiles:
    """Tests for loading PUMS data from files."""

    @pytest.mark.parametrize("suffix", ["csv", "parquet"])
    def test_load_round_trip(self, tmp_path, pums_households, pums_persons, suffix):
        """Both on-disk formats come back with every row and the ids intact."""
        hh_path = tmp_path / f"hh.{suffix}"
        per_path = tmp_path / f"per.{suffix}"
        if suffix == "csv":
            pums_households.write_csv(hh_path)
            pums_persons.write_csv(per_path)
        else:
            pums_households.write_parquet(hh_path)
            pums_persons.write_parquet(per_path)

        hh, per = load_pums_from_files(str(hh_path), str(per_path))
        assert hh.sort("SERIALNO")["SERIALNO"].to_list() == ["HH1", "HH2", "HH3"]
        assert per.height == 7

    def test_puma_filter(self, tmp_path, pums_households, pums_persons):
        """Filtering by PUMA should return only matching records."""
        hh_path = tmp_path / "hh.csv"
        per_path = tmp_path / "per.csv"
        pums_households.write_csv(hh_path)
        pums_persons.write_csv(per_path)

        hh, _ = load_pums_from_files(str(hh_path), str(per_path), puma_ids=["00100"])
        assert all(hh["PUMA"].cast(pl.Utf8) == "00100")
