"""Shared assertion helpers for E2E integration tests."""

import polars as pl

from data_canon.core.dataclass import CanonicalData

# ---------------------------------------------------------------------------
# Table presence
# ---------------------------------------------------------------------------


def assert_tables_non_empty(data: CanonicalData, table_names: list[str]):
    """Assert that each named table exists and has at least one row."""
    for name in table_names:
        df = getattr(data, name, None)
        assert df is not None, f"Table '{name}' is None"
        assert isinstance(df, pl.DataFrame), f"Table '{name}' is not a DataFrame"
        assert df.height > 0, f"Table '{name}' is empty"


# ---------------------------------------------------------------------------
# Referential integrity
# ---------------------------------------------------------------------------

_FK_CHAINS = [
    ("persons", "hh_id", "households", "hh_id"),
    ("days", "person_id", "persons", "person_id"),
    ("unlinked_trips", "day_id", "days", "day_id"),
    ("linked_trips", "day_id", "days", "day_id"),
    ("linked_trips", "person_id", "persons", "person_id"),
    ("tours", "person_id", "persons", "person_id"),
    ("tours", "hh_id", "households", "hh_id"),
    ("tours", "day_id", "days", "day_id"),
]


def assert_referential_integrity(data: CanonicalData):
    """Check that all foreign key references resolve to existing parent rows."""
    for child_table, child_col, parent_table, parent_col in _FK_CHAINS:
        child = getattr(data, child_table, None)
        parent = getattr(data, parent_table, None)
        if child is None or parent is None:
            continue
        if child.height == 0 or parent.height == 0:
            continue
        if child_col not in child.columns or parent_col not in parent.columns:
            continue

        parent_ids = set(parent[parent_col].drop_nulls().to_list())
        child_ids = set(child[child_col].drop_nulls().to_list())
        orphans = child_ids - parent_ids
        assert not orphans, (
            f"{child_table}.{child_col} has {len(orphans)} orphaned FK(s) "
            f"not in {parent_table}.{parent_col}: {list(orphans)[:10]}"
        )


# ---------------------------------------------------------------------------
# Weights
# ---------------------------------------------------------------------------


def assert_weights_populated(data: CanonicalData):
    """Assert all tables with weight columns have them filled (non-null for complete records)."""
    weight_map = {
        "households": "hh_weight",
        "persons": "person_weight",
        "days": "day_weight",
        "tours": "tour_weight",
        "linked_trips": "linked_trip_weight",
    }
    for table_name, weight_col in weight_map.items():
        df = getattr(data, table_name, None)
        if df is None or df.height == 0:
            continue
        assert weight_col in df.columns, (
            f"Table '{table_name}' missing weight column '{weight_col}'"
        )
        # Complete records should have non-null weights
        if "complete" in df.columns:
            complete = df.filter(pl.col("complete") == True)  # noqa: E712
            null_weights = complete.filter(pl.col(weight_col).is_null()).height
            assert null_weights == 0, (
                f"{table_name}: {null_weights} complete records have null {weight_col}"
            )


def assert_weights_propagated(data: CanonicalData):
    """Check that the weight hierarchy flows correctly: hh → person → day → trip → tour."""
    hh = data.households
    per = data.persons
    if hh is not None and per is not None and hh.height > 0 and per.height > 0:
        if "hh_weight" in hh.columns and "person_weight" in per.columns:
            # Every person's weight should be derived from their household
            merged = per.select(["person_id", "hh_id", "person_weight"]).join(
                hh.select(["hh_id", "hh_weight"]), on="hh_id", how="left"
            )
            # person_weight for complete records should match hh_weight
            complete = merged.filter(merged["person_weight"].is_not_null())
            if complete.height > 0:
                mismatches = complete.filter(
                    (pl.col("person_weight") - pl.col("hh_weight")).abs() > 0.01
                )
                assert mismatches.height == 0, (
                    f"person_weight != hh_weight for {mismatches.height} records"
                )


def assert_weights_positive_for_complete(data: CanonicalData):
    """Complete records should have weight > 0."""
    weight_map = {
        "households": "hh_weight",
        "persons": "person_weight",
    }
    for table_name, weight_col in weight_map.items():
        df = getattr(data, table_name, None)
        if df is None or df.height == 0:
            continue
        if weight_col not in df.columns or "complete" not in df.columns:
            continue
        complete = df.filter(pl.col("complete") == True)  # noqa: E712
        zero_weights = complete.filter(pl.col(weight_col) <= 0).height
        assert zero_weights == 0, (
            f"{table_name}: {zero_weights} complete records have {weight_col} <= 0"
        )
