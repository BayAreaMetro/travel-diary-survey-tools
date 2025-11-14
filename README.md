# travel-diary-survey-tools
For collaborating on Travel Diary Survey Tools

## Features

- **Trip Linking**: Link sequential trips by mode changes to create linked trip chains
- **Tour Building**: Identify home-based and work-based tours from linked trips
- **Relational Structure**: Maintains foreign key relationships for database integration
- **Step-Aware Validation**: Validates data requirements specific to each pipeline step

## Documentation

- [Validation Framework](VALIDATION_README.md) - Comprehensive data validation system
- [Column Requirements](COLUMN_REQUIREMENTS.md) - Field requirements per pipeline step

## Setup Instructions for Windows Users

### Installing UV

1. Open PowerShell and run:
```powershell
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

2. Restart your terminal to ensure UV is in your PATH

3. Verify the installation:
```powershell
uv --version
```

### Using UV

Create a virtual environment:
```powershell
uv sync
```

In VSCode you may need to restart terminal or select the interpreter manually.

## Usage

### Complete Pipeline

```python
import polars as pl
from travel_diary_survey_tools import link_trips, build_tours

# Step 1: Link trips by mode changes
raw_trips = pl.read_csv("raw_trips.csv")
trip_tours_raw, linked_trips = link_trips(raw_trips, change_mode_code=9)

# Step 2: Build tours from linked trips
persons = pl.read_csv("persons.csv")
linked_trips_with_ids, tours = build_tours(linked_trips, persons)

# Now linked_trips_with_ids contains:
# - All original fields from linked_trips
# - tour_id, subtour_id (tour assignments only)

# Join tour attributes as needed
enriched = linked_trips_with_ids.join(
    tours.select(["tour_id", "tour_purpose", "tour_mode"]),
    on="tour_id",
    how="left"
)
```

### Relational Structure

The modules maintain foreign key relationships for database integration:

```
raw_trips (trip_id)
    ↓ link_trips() → (trip_tours_raw, linked_trips)
    ↓ adds: linked_trip_id
linked_trips (linked_trip_id)
    ↓ build_tours() → (linked_trips_with_ids, tours)
    ↓ adds: tour_id, subtour_id
linked_trips_with_ids (linked_trip_id, tour_id)
    ↓ tours table has aggregated attributes →
tours (tour_id, tour_purpose, tour_mode, ...)
```

Key relationships:
- `linked_trip_id`: Links raw trips to linked trip chains (from link_trips)
- `tour_id`: Links trips to their parent tour (from build_tours)
- `subtour_id`: Links trips to work-based subtours if applicable

Both functions follow the same pattern: enrich input with IDs, return aggregated table with attributes.
