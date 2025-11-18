# travel-diary-survey-tools
For collaborating on Travel Diary Survey Tools

## Features

- **Pipeline Architecture**: Modular pipeline system with configurable YAML-based execution
- **Trip Linking**: Link sequential trips by mode changes to create linked trip chains
- **Tour Extraction**: Identify home-based and work-based tours from linked trips with hierarchical subtours
- **Person Type Classification**: Automatically derive person types from demographics and employment
- **Relational Structure**: Maintains foreign key relationships for database integration
- **Step-Aware Validation**: Validates data requirements specific to each pipeline step
- **Codebook Integration**: Enum-based validation with labeled categorical values

## Documentation

- [Validation Framework](VALIDATION_README.md) - Comprehensive data validation system
- [Column Requirements](COLUMN_REQUIREMENTS.md) - Field requirements per pipeline step
- [Codebook](CODEBOOK_README.md) - Enumeration definitions and labeled values

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

### Pipeline Configuration

The pipeline is configured using YAML files that specify input data, steps, and parameters:

```yaml
# config.yaml
input_paths:
  households: "data/households.csv"
  persons: "data/persons.csv"
  unlinked_trips: "data/trips.csv"

steps:
  - name: load_data
    enabled: true

  - name: link_trips
    enabled: true
    params:
      change_mode_code: 9
      transit_mode_codes: [6, 7, 8]
      max_dwell_time: 120
      dwell_buffer_distance: 100

  - name: extract_tours
    enabled: true
    params:
      distance_thresholds:
        home: 200
        work: 200
        school: 200
```

### Running the Pipeline

```python
from processing.pipeline import Pipeline

# Initialize and run pipeline
pipeline = Pipeline("config.yaml")
pipeline.run()

# Access processed data
linked_trips = pipeline.data.linked_trips
tours = pipeline.data.tours
persons = pipeline.data.persons
```

### Using Individual Steps

You can also use individual processing steps directly:

```python
from processing.steps import load_data, link_trips, extract_tours
import polars as pl

# Load data
result = load_data(input_paths={
    "households": "data/households.csv",
    "persons": "data/persons.csv",
    "unlinked_trips": "data/trips.csv",
})

# Link trips
link_result = link_trips(
    unlinked_trips=result["unlinked_trips"],
    change_mode_code=9,
    transit_mode_codes=[6, 7, 8],
    max_dwell_time=120,
    dwell_buffer_distance=100,
)
linked_trips = link_result["linked_trips"]
trips_with_ids = link_result["trips_with_ids"]

# Extract tours
tour_result = extract_tours(
    persons=result["persons"],
    households=result["households"],
    linked_trips=linked_trips,
    distance_thresholds={"home": 200, "work": 200, "school": 200},
)
linked_trips_with_tours = tour_result["linked_trips"]
tours = tour_result["tours"]
```

### Data Flow

The pipeline maintains relational structure across steps:

```
unlinked_trips (trip_id)
    ↓
  link_trips
    ↓ adds: linked_trip_id
linked_trips (linked_trip_id)
    ↓
  extract_tours
    ↓ adds: tour_id, subtour_id, tour_category
linked_trips_with_tour_ids (linked_trip_id, tour_id)
    ↓
tours (tour_id, tour_purpose, tour_mode, ...)
```

### Validation

Steps automatically validate inputs and outputs when decorated with `@step()`:

```python
from processing.decoration import step

@step(
    validate_input=True,   # Validate before processing
    validate_output=True,  # Validate after processing
)
def my_custom_step(
    persons: pl.DataFrame,
) -> dict[str, pl.DataFrame]:
    # Your processing logic
    return {"persons": persons}
```

## Project Structure

```
travel-diary-survey-tools/
├── src/
│   ├── data_canon/              # Data models and validation
│   │   ├── codebook/            # Enumeration definitions
│   │   │   ├── days.py
│   │   │   ├── generic.py
│   │   │   ├── households.py
│   │   │   ├── persons.py
│   │   │   ├── tours.py
│   │   │   ├── trips.py
│   │   │   └── vehicles.py
│   │   └── core/                # Core validation logic
│   │       ├── dataclass.py     # CanonicalData container
│   │       ├── validators.py    # Validation framework
│   │       └── step_field.py    # Step-aware field definitions
│   └── processing/              # Pipeline processing
│       ├── pipeline.py          # Pipeline orchestration
│       ├── decoration.py        # Step decorator
│       └── steps/               # Processing steps
│           ├── load.py          # Data loading
│           ├── link.py          # Trip linking
│           ├── extract_tours/   # Tour extraction
│           │   ├── extraction.py
│           │   ├── person_type.py
│           │   ├── priority_utils.py
│           │   └── tour_configs.py
│           └── format_daysim.py # DaySim output formatting
├── tests/                       # Test suite
├── scripts/                     # Analysis and utilities
└── docs/
    ├── README.md
    ├── VALIDATION_README.md
    ├── CODEBOOK_README.md
    └── COLUMN_REQUIREMENTS.md
```

## Development

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src --cov-report=html

# Run specific test file
uv run pytest tests/test_validation.py -v
```

### Code Quality

The project uses `ruff` for linting and formatting:

```bash
# Run linter
uv run ruff check src/ tests/

# Auto-fix issues
uv run ruff check --fix src/ tests/

# Format code
uv run ruff format src/ tests/
```

### Pre-commit Hooks

Install pre-commit hooks to ensure code quality:

```bash
uv run pre-commit install
```

## Contributing

1. Create a new branch for your feature
2. Make your changes with appropriate tests
3. Ensure all tests pass and code is formatted
4. Submit a pull request

## License

[Add license information here]
