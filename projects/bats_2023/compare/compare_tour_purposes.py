"""Compare tour purpose distributions between legacy and new pipelines.

Shows aggregate statistics that don't require tour-by-tour matching.
"""

import sys
from pathlib import Path

import polars as pl
from helpers import load_legacy_data, load_new_pipeline_data

# Load data
legacy_dir = Path(
    "M:/Data/HomeInterview/Bay Area Travel Study 2023/"
    "Data/Processed/test/03b-assign_day/wt-wkday_3day"
)
config_path = Path(__file__).parent.parent / "config.yaml"
cache_dir = Path(__file__).parent.parent.parent.parent / ".cache"
legacy = load_legacy_data(legacy_dir)
new = load_new_pipeline_data(config_path, cache_dir)

# Get common households for fair comparison
common_hhnos = set(legacy["hh"]["hhno"]) & set(new["hh"]["hhno"])
sys.stderr.write(f"Comparing {len(common_hhnos):,} common households\n\n")

# IMPORTANT: Legacy uses 3-day weekday (Tue, Wed, Thu = days 2, 3, 4)
# See: wt-wkday_3day directory name and
# num_days_complete_3dayweekday in 02a-reformat.py
DAYS_TO_COMPARE = [2, 3, 4]  # Tuesday, Wednesday, Thursday
sys.stderr.write(
    f"Filtering to 3-day weekday: {DAYS_TO_COMPARE} (Tue, Wed, Thu)\n\n"
)

# Filter to common households AND common days
legacy_tours = legacy["tour"].filter(
    pl.col("hhno").is_in(common_hhnos) & pl.col("day").is_in(DAYS_TO_COMPARE)
)
new_tours = new["tour"].filter(
    pl.col("hhno").is_in(common_hhnos) & pl.col("day").is_in(DAYS_TO_COMPARE)
)

sys.stderr.write(f"Legacy tours (Tue-Thu): {len(legacy_tours):,}\n")
sys.stderr.write(f"New tours (Tue-Thu):    {len(new_tours):,}\n")
tour_diff = len(new_tours) - len(legacy_tours)
tour_diff_pct = tour_diff / len(legacy_tours) * 100
sys.stderr.write(
    f"Difference:             {tour_diff:,} ({tour_diff_pct:+.2f}%)\n\n"
)

# Purpose distribution
sys.stderr.write("=" * 70 + "\n")
sys.stderr.write("TOUR PURPOSE DISTRIBUTION\n")
sys.stderr.write("=" * 70 + "\n")

# Get distributions
legacy_dist = (
    legacy_tours.group_by("pdpurp")
    .agg(pl.len().alias("legacy_count"))
    .with_columns(
        (pl.col("legacy_count") / pl.col("legacy_count").sum() * 100).alias(
            "legacy_pct"
        )
    )
    .sort("pdpurp")
)

new_dist = (
    new_tours.group_by("pdpurp")
    .agg(pl.len().alias("new_count"))
    .with_columns(
        (pl.col("new_count") / pl.col("new_count").sum() * 100).alias("new_pct")
    )
    .sort("pdpurp")
)

# Join distributions
comparison = (
    legacy_dist.join(new_dist, on="pdpurp", how="full", coalesce=True)
    .with_columns(
        [
            pl.col("legacy_count").fill_null(0),
            pl.col("new_count").fill_null(0),
            pl.col("legacy_pct").fill_null(0.0),
            pl.col("new_pct").fill_null(0.0),
        ]
    )
    .with_columns(
        [
            (pl.col("new_count") - pl.col("legacy_count")).alias("diff_count"),
            (pl.col("new_pct") - pl.col("legacy_pct")).alias("diff_pct"),
        ]
    )
)

# Purpose names (DaySim codes)
purpose_names = {
    0: "Home",
    1: "Work",
    2: "School",
    3: "Escort",
    4: "Personal Business",
    5: "Shopping",
    6: "Meal",
    7: "Social/Recreation",
    8: "Work-Based",
    9: "Change Mode",
    10: "At Work Subtour",
    11: "Business",
}

comparison = comparison.with_columns(
    pl.col("pdpurp")
    .map_elements(
        lambda x: purpose_names.get(x, f"Unknown({x})"), return_dtype=pl.Utf8
    )
    .alias("purpose_name")
)

# Display
sys.stderr.write(
    f"\n{'Purpose':<25} {'Legacy':<12} {'New':<12} {'Difference':<20}\n"
)
sys.stderr.write(f"{'=' * 25} {'=' * 12} {'=' * 12} {'=' * 20}\n")

for row in comparison.sort("pdpurp").iter_rows(named=True):
    purpose = row["purpose_name"]
    legacy_count = int(row["legacy_count"] or 0)
    legacy_pct = float(row["legacy_pct"] or 0.0)
    new_count = int(row["new_count"] or 0)
    new_pct = float(row["new_pct"] or 0.0)
    diff_count = int(new_count - legacy_count)
    diff_pct = float(new_pct - legacy_pct)

    legacy_str = f"{legacy_count:,} ({legacy_pct:.1f}%)"
    new_str = f"{new_count:,} ({new_pct:.1f}%)"
    diff_str = f"{diff_count:+,} ({diff_pct:+.2f}pp)"

    sys.stderr.write(
        f"{purpose:<25} {legacy_str:<12} {new_str:<12} {diff_str:<20}\n"
    )

sys.stderr.write(f"{'=' * 25} {'=' * 12} {'=' * 12} {'=' * 20}\n")
total_diff = len(new_tours) - len(legacy_tours)
sys.stderr.write(
    f"{'TOTAL':<25} {len(legacy_tours):,} (100%) "
    f"{len(new_tours):,} (100%) {total_diff:+,}\n"
)

sys.stderr.write("\n" + "=" * 70 + "\n")
sys.stderr.write("KEY DIFFERENCES (>1 percentage point)\n")
sys.stderr.write("=" * 70 + "\n")
large_diffs = comparison.filter(pl.col("diff_pct").abs() > 1.0).sort(
    pl.col("diff_pct").abs(), descending=True
)
for row in large_diffs.iter_rows(named=True):
    purpose = row["purpose_name"]
    legacy_count = int(row["legacy_count"] or 0)
    new_count = int(row["new_count"] or 0)
    diff_count = int(new_count - legacy_count)
    diff_pct = float(row["diff_pct"] or 0.0)
    direction = "MORE" if diff_count > 0 else "FEWER"
    sys.stderr.write(
        f"  {purpose:<25} {direction:>6}: {abs(diff_count):>5,} tours "
        f"({diff_pct:+.2f}pp)\n"
    )

sys.stderr.write("\n" + "=" * 70 + "\n")
sys.stderr.write("INTERPRETATION\n")
sys.stderr.write("=" * 70 + "\n")
sys.stderr.write(
    "* Positive differences indicate more tours of that type in new pipeline\n"
)
sys.stderr.write(
    "* 'pp' = percentage points (absolute difference in percentages)\n"
)
sys.stderr.write(
    "* Large shifts suggest different tour purpose classification logic\n"
)
sys.stderr.write(
    "* Business -> Work reclassification likely (-3.07pp + part of -4.46pp)\n"
)
sys.stderr.write(
    "* New pipeline extracts 'Change Mode' tours (0 -> 929 tours, +2.32pp)\n"
)
sys.stderr.write(
    "* Social/Recreation increased (+3.58pp), Shopping increased (+2.20pp)\n"
)
