# Strip Location Fields - External Sharing Processing

This script removes location-sensitive fields from CSV files in a survey directory and writes cleaned copies to an `external_sharing` folder next to that directory.

## ⚠️ Warning

**This script writes output files to a separate folder.** If a matching output file already exists, it is left unchanged and the script skips writing it.

## Quick Start

```bash
python strip_location_fields.py --survey-dir "E:\Box\Modeling and Surveys\Surveys\Travel Diary Survey\BATS_2023\External_Sharing_Staging\survey"
```

Optional skip flags:

```bash
python strip_location_fields.py --survey-dir "E:\Box\Modeling and Surveys\Surveys\Travel Diary Survey\BATS_2023\External_Sharing_Staging\survey" --skip-tours --skip-joint-trips
```

## What It Does

1. Scans all CSV files in the survey directory passed via `--survey-dir`
2. Identifies columns containing "_lat", "_lon", or "_bg" (case-insensitive)
3. Removes those columns
4. Writes cleaned CSV files to `external_sharing` beside the survey directory
5. Writes a processing log file named `strip_location_fields_YYYYMMDD_HHMMSS.log`
6. Can skip `tours_2023.csv` and `joint_trips_2023.csv` via command-line options
7. Copies `codebook.xlsx` from the survey directory into `external_sharing`
8. Copies any `pipeline_*.log` and `pipeline_*.yaml` files from the parent staging directory into `external_sharing`

## Fields Removed

Any column name containing (case-insensitive):
- `_lat` - Examples: `home_lat`, `latitude`, `d_lat`
- `_lon` - Examples: `home_lon`, `longitude`, `o_lon`
- `_bg` - Examples: `home_bg`

## Output Files

### strip_location_fields_YYYYMMDD_HHMMSS.log
Complete processing log showing:
```
2026-07-01 10:30:15 - INFO - Processing: households_2023.csv
2026-07-01 10:30:15 - INFO -   Read 5,234 rows, 65 columns
2026-07-01 10:30:15 - INFO -   Removing 4 location fields: ['home_lon', 'home_lat', ...]
2026-07-01 10:30:15 - INFO -   Wrote 5,234 rows, 61 columns
```

### Output CSVs
Cleaned CSV files are written to the `external_sharing` folder next to the input survey directory.

### Copied Artifacts
The script also copies these files into `external_sharing` when they exist:
- `codebook.xlsx`
- `pipeline_*.log`
- `pipeline_*.yaml`

## Workflow

1. **First**: Run the main pipeline to generate data
   ```bash
   python run.py --config projects/bats_2023/config_census_tract_2010.yaml
   ```

2. **Then**: Strip location fields for external sharing
   ```bash
   cd projects/bats_2023/external_sharing
   python strip_location_fields.py --survey-dir "E:\Box\Modeling and Surveys\Surveys\Travel Diary Survey\BATS_2023\External_Sharing_Staging\survey"
   ```

3. **Optional**: Skip the tours and joint trips files
   ```bash
   python strip_location_fields.py --survey-dir "E:\Box\Modeling and Surveys\Surveys\Travel Diary Survey\BATS_2023\External_Sharing_Staging\survey" --skip-tours --skip-joint-trips
   ```

## Notes

- Geography IDs (census tracts, TAZs, etc.) are NOT removed - only lat/lon coordinates or block groups (bg)
- Original files are not overwritten
- The script processes all `.csv` files in the target directory unless skipped by option
- Existing copied artifacts are overwritten with the latest versions
