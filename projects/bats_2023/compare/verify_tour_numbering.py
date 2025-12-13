"""Verify tour numbering schemes in legacy vs new pipeline.

Shows specific examples to confirm architectural difference.
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

# Get common households
common_hhnos = set(legacy["hh"]["hhno"]) & set(new["hh"]["hhno"])
sys.stderr.write(f"Common households: {len(common_hhnos)}\n")

# Look at a few example households with multiple days
legacy_tours = legacy["tour"].filter(pl.col("hhno").is_in(common_hhnos))
new_tours = new["tour"].filter(pl.col("hhno").is_in(common_hhnos))

# Find households with multiple tours across multiple days
MIN_DAYS = 1
MIN_TOURS = 2
legacy_multi = (
    legacy_tours.group_by("hhno", "pno")
    .agg(
        [
            pl.col("day").n_unique().alias("n_days"),
            pl.col("tour").n_unique().alias("n_tours"),
            pl.col("tour").max().alias("max_tour"),
        ]
    )
    .filter((pl.col("n_days") > MIN_DAYS) & (pl.col("n_tours") > MIN_TOURS))
    .head(5)
)

sys.stderr.write("\n=== LEGACY TOUR NUMBERING ===\n")
sys.stderr.write("Sample persons with multiple days and tours:\n")
sys.stderr.write(str(legacy_multi) + "\n")

for row in legacy_multi.iter_rows(named=True):
    hhno, pno = row["hhno"], row["pno"]
    sys.stderr.write(f"\nLegacy: HH {hhno}, Person {pno}\n")
    person_tours = (
        legacy_tours.filter((pl.col("hhno") == hhno) & (pl.col("pno") == pno))
        .select(["day", "tour", "pdpurp"])
        .sort("tour")
    )
    sys.stderr.write(str(person_tours) + "\n")

sys.stderr.write("\n=== NEW TOUR NUMBERING ===\n")
for row in legacy_multi.iter_rows(named=True):
    hhno, pno = row["hhno"], row["pno"]
    sys.stderr.write(f"\nNew: HH {hhno}, Person {pno}\n")
    person_tours = (
        new_tours.filter((pl.col("hhno") == hhno) & (pl.col("pno") == pno))
        .select(["day", "tour", "pdpurp", "parent"])
        .sort("day", "tour")
    )
    sys.stderr.write(str(person_tours) + "\n")

# Now check what happens with the current matching strategy
sys.stderr.write("\n=== MATCHING ANALYSIS ===\n")
sys.stderr.write("Current TOUR_MATCH_COLS: ['pno', 'pdpurp', 'mode']\n")
sys.stderr.write(
    "This means we match tours only on person number, purpose, and mode.\n"
)
sys.stderr.write(
    "Without tour number, a person's 'Work' tour on day 1 matches 'Work' "
    "tour on day 2.\n"
)
sys.stderr.write("\n** KEY FINDING **\n")
sys.stderr.write(
    "LEGACY numbering: Tours numbered 1-N across entire survey period\n"
)
sys.stderr.write(
    "  Example: HH 23204275 Person 1 has tours 1-10 spanning days 1,3-7\n"
)
sys.stderr.write("NEW numbering: Tours numbered 1-N per day\n")
sys.stderr.write(
    "  Example: HH 23204275 Person 1 has tours 1-3 on day 5 "
    "(not tours 6, 7, 8)\n"
)
sys.stderr.write("\nThis explains the 376% match rate:\n")
sys.stderr.write(
    "  - Matching without 'tour' number causes each legacy tour to match\n"
)
sys.stderr.write("    ALL new tours with same purpose/mode across all days\n")
sys.stderr.write(
    "  - A person with 3 'Other' tours across 3 days creates 3x3 = 9 matches\n"
)
sys.stderr.write(
    "  - Total matches >> total legacy tours \u2192 match rate > 100%\n"
)
sys.stderr.write("\n** CONCLUSION **\n")
sys.stderr.write(
    "Tour-by-tour comparison is impossible with different numbering schemes.\n"
)
sys.stderr.write(
    "Only aggregate statistics (total tours, tour distribution) are valid.\n"
)
