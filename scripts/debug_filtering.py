"""Debug script to investigate filtering differences between old and new pipeline.

This script loads data from both old pipeline outputs and raw data,
then compares counts at each processing stage to identify where trips
are being filtered out.
"""
import polars as pl
from pathlib import Path

# Paths
RAW_DIR = Path(r"\\models.ad.mtc.ca.gov\data\models\Data\HomeInterview\Bay Area Travel Study 2023\Data\Full Weighted 2023 Dataset\WeightedDataset_02212025")
OLD_01_DIR = Path(r"\\models.ad.mtc.ca.gov\data\models\Data\HomeInterview\Bay Area Travel Study 2023\Data\Processed\TripLinking_20250728\01-taz_spatial_join")
OLD_02A_DIR = Path(r"\\models.ad.mtc.ca.gov\data\models\Data\HomeInterview\Bay Area Travel Study 2023\Data\Processed\TripLinking_20250728\02a-reformat")
OLD_02B_DIR = Path(r"\\models.ad.mtc.ca.gov\data\models\Data\HomeInterview\Bay Area Travel Study 2023\Data\Processed\TripLinking_20250728\02b-link_trips_week")

print("=" * 80)
print("TRIP COUNT COMPARISON")
print("=" * 80)

# 1. Raw data (before any processing)
print("\n1. RAW DATA (WeightedDataset_02212025)")
try:
    raw_trips = pl.read_csv(RAW_DIR / "trip.csv")
    print(f"   Total trips: {len(raw_trips):,}")
    
    # Check day_is_complete distribution
    if "day_is_complete" in raw_trips.columns:
        complete_counts = raw_trips.group_by("day_is_complete").len()
        print(f"   Day completeness distribution:")
        for row in complete_counts.iter_rows():
            print(f"      day_is_complete={row[0]}: {row[1]:,} trips")
        
        complete_trips = raw_trips.filter(pl.col("day_is_complete") == 1)
        print(f"   Complete day trips (day_is_complete=1): {len(complete_trips):,}")
except Exception as e:
    print(f"   ERROR: {e}")

# 2. Old pipeline: 01-taz_spatial_join output
print("\n2. OLD PIPELINE: 01-taz_spatial_join output")
try:
    old_01_trips = pl.read_csv(OLD_01_DIR / "trip.csv")
    print(f"   Total trips: {len(old_01_trips):,}")
    
    if "day_is_complete" in old_01_trips.columns:
        complete_01 = old_01_trips.filter(pl.col("day_is_complete") == 1)
        print(f"   Complete day trips: {len(complete_01):,}")
        
        # Check for null TAZ values
        if "o_taz" in old_01_trips.columns:
            null_o_taz = old_01_trips.filter(pl.col("o_taz").is_null()).height
            null_d_taz = old_01_trips.filter(pl.col("d_taz").is_null()).height
            print(f"   Trips with null o_taz: {null_o_taz:,}")
            print(f"   Trips with null d_taz: {null_d_taz:,}")
except Exception as e:
    print(f"   ERROR: {e}")

# 3. Old pipeline: 02a-reformat output
print("\n3. OLD PIPELINE: 02a-reformat output")
try:
    old_02a_trips = pl.read_csv(OLD_02A_DIR / "trip.csv")
    print(f"   Total trips: {len(old_02a_trips):,}")
    
    # Check for -1 TAZ values (out of region)
    if "otaz" in old_02a_trips.columns:
        out_of_region_o = old_02a_trips.filter(pl.col("otaz") == -1).height
        out_of_region_d = old_02a_trips.filter(pl.col("dtaz") == -1).height
        print(f"   Trips with otaz=-1 (out of region): {out_of_region_o:,}")
        print(f"   Trips with dtaz=-1 (out of region): {out_of_region_d:,}")
        
        in_region = old_02a_trips.filter(
            (pl.col("otaz") != -1) & (pl.col("dtaz") != -1)
        )
        print(f"   In-region trips (both o/d in region): {len(in_region):,}")
except Exception as e:
    print(f"   ERROR: {e}")

# 4. Old pipeline: 02b-link_trips_week output
print("\n4. OLD PIPELINE: 02b-link_trips_week output")
try:
    old_02b_trips = pl.read_csv(OLD_02B_DIR / "trip.csv")
    print(f"   Total trip segments: {len(old_02b_trips):,}")
    print(f"   NOTE: This excludes trip segments that were merged during linking")
    
    # Count unique linked trips
    if "lintripno" in old_02b_trips.columns:
        unique_linked = old_02b_trips.select([
            pl.col("hhno"), 
            pl.col("pno"), 
            pl.col("lintripno")
        ]).unique().height
        print(f"   Unique linked trips: {unique_linked:,}")
        
    # Try to load the detailed output that includes deleted trips
    detail_path = OLD_02B_DIR / "trip_linked_detail_week.csv"
    if detail_path.exists():
        old_02b_detail = pl.read_csv(detail_path)
        print(f"\n   02b DETAILED output (with deleted segments):")
        print(f"      Total segments: {len(old_02b_detail):,}")
        if "del_flag" in old_02b_detail.columns:
            deleted = old_02b_detail.filter(pl.col("del_flag") == 1)
            kept = old_02b_detail.filter(pl.col("del_flag") == 0)
            print(f"      Segments deleted (merged): {len(deleted):,}")
            print(f"      Segments kept: {len(kept):,}")
except Exception as e:
    print(f"   ERROR: {e}")

# 5. Compare raw vs old_01 to see if spatial join filters
print("\n" + "=" * 80)
print("ANALYSIS: Where are trips being lost?")
print("=" * 80)

try:
    raw_count = len(raw_trips)
    raw_complete = len(raw_trips.filter(pl.col("day_is_complete") == 1))
    old_01_count = len(old_01_trips)
    old_02a_count = len(old_02a_trips)
    old_02b_count = len(old_02b_trips)
    
    print(f"\nRaw → 01-spatial_join:")
    print(f"   {raw_count:,} → {old_01_count:,} = {old_01_count - raw_count:+,} trips")
    
    print(f"\n01-spatial_join → 02a-reformat (filtering complete days):")
    print(f"   {old_01_count:,} → {old_02a_count:,} = {old_02a_count - old_01_count:+,} trips")
    print(f"   Expected loss from incomplete days: {raw_count - raw_complete:,}")
    
    print(f"\n02a-reformat → 02b-link_trips:")
    print(f"   {old_02a_count:,} → {old_02b_count:,} = {old_02b_count - old_02a_count:+,} trips")
    
    print(f"\nRaw complete days vs our pipeline:")
    print(f"   Raw complete days: {raw_complete:,}")
    print(f"   Old 02b output: {old_02b_count:,}")
    print(f"   Difference: {raw_complete - old_02b_count:+,} trips")
    
except Exception as e:
    print(f"ERROR in analysis: {e}")

# 6. Check for differences in trip_id matching
print("\n" + "=" * 80)
print("DETAILED COMPARISON: Trip IDs")
print("=" * 80)

try:
    # Get trip IDs from each stage
    raw_trip_ids = set(raw_trips.select("trip_id").to_series().to_list())
    old_01_trip_ids = set(old_01_trips.select("trip_id").to_series().to_list())
    
    print(f"\nRaw trip IDs: {len(raw_trip_ids):,}")
    print(f"Old 01 trip IDs: {len(old_01_trip_ids):,}")
    
    lost_in_spatial_join = raw_trip_ids - old_01_trip_ids
    if lost_in_spatial_join:
        print(f"\nTrips lost in spatial join: {len(lost_in_spatial_join):,}")
        print(f"First 10 lost trip_ids: {sorted(list(lost_in_spatial_join))[:10]}")
        
        # Check if these trips have missing coordinates
        lost_trips = raw_trips.filter(pl.col("trip_id").is_in(lost_in_spatial_join))
        null_coords = lost_trips.filter(
            pl.col("o_lon").is_null() | pl.col("o_lat").is_null() |
            pl.col("d_lon").is_null() | pl.col("d_lat").is_null()
        )
        print(f"Lost trips with null coordinates: {len(null_coords):,}")
    
except Exception as e:
    print(f"ERROR: {e}")

print("\n" + "=" * 80)
