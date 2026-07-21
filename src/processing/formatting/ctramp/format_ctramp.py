"""CT-RAMP Formatting Step.

Transforms canonical survey data (persons, households, tours, trips) into
CT-RAMP (Coordinated Travel - Regional Activity Modeling Platform) format for
use with activity-based travel demand models. See [CTRAMP Data Models](../../models/ctramp.md).

This module orchestrates the transformation of households, persons, tours, and
trips from canonical format into seven CT-RAMP tables, intelligently handling
missing data and providing configurable income thresholds and filtering options.

# Components

* [`format_households`][processing.formatting.ctramp.format_households]: Produces households
  table consistent with [`HouseholdCTRAMPModel`][data_canon.models.ctramp.HouseholdCTRAMPModel].
* [`format_persons`][processing.formatting.ctramp.format_persons]: Produces persons
  table consistent with [`PersonCTRAMPModel`][data_canon.models.ctramp.PersonCTRAMPModel].
* [`format_mandatory_location`][processing.formatting.ctramp.format_mandatory_location]:
  Produces mandatory locations ctramp table with work and school location records for persons
  with usual work/school locations, linking person characteristics with destination TAZ.  This
  table is consistent with
  [`MandatoryLocationCTRAMPModel`][data_canon.models.ctramp.MandatoryLocationCTRAMPModel].
* [`format_tours`][processing.formatting.ctramp.format_tours]: Produces individual and joint
  tours tables, consistent with
  [`IndividualTourCTRAMPModel`][data_canon.models.ctramp.IndividualTourCTRAMPModel] and
  [`JointTourCTRAMPModel`][data_canon.models.ctramp.JointTourCTRAMPModel].
* [`format_trips`][processing.formatting.ctramp.format_trips]: Produces individual and joint trips
  tables consistent with
  [`IndividualTripCTRAMPModel`][data_canon.models.ctramp.IndividualTripCTRAMPModel] and
  [`JointTripCTRAMPModel`][data_canon.models.ctramp.JointTripCTRAMPModel].

# Data Flow

## Key Dependencies

1. households_ctramp must be created first (needed by all downstream formatters)
2. individual_tours_ctramp must be created before persons_ctramp (provides tour
   statistics)
3. persons_ctramp and trips use already-formatted tour/household data
4. Joint tables can be processed independently from individual tables

## Mermaid Diagram

```mermaid
flowchart TD
    %% Canonical inputs
    subgraph inputs["Canonical Inputs"]
        direction TB
        hh_canon[("households")]
        per_canon[("persons")]
        tours_canon[("tours")]
        trips_canon[("linked_trips")]
        joint_trips_canon[("joint_trips")]
    end

    %% Stage 1
    subgraph s1["Stage 1: Households"]
        direction LR
        fmt_hh["format_households"]
        hh_ctramp{{"households_ctramp"}}
        fmt_hh --> hh_ctramp
    end

    %% Stage 2
    subgraph s2["Stage 2: Tours"]
        direction TB
        fmt_ind_tour["format_individual_tour"]
        ind_tour_ctramp{{"individual_tours_ctramp"}}
        fmt_ind_tour --> ind_tour_ctramp

        fmt_joint_tour["format_joint_tour"]
        joint_tour_ctramp{{"joint_tours_ctramp"}}
        fmt_joint_tour --> joint_tour_ctramp
    end

    %% Stage 3
    subgraph s3["Stage 3: Persons"]
        direction LR
        fmt_persons["format_persons"]
        per_ctramp{{"persons_ctramp"}}
        fmt_persons --> per_ctramp
    end

    %% Stage 4
    subgraph s4["Stage 4: Locations & Trips"]
        direction TB
        fmt_mand_loc["format_mandatory_location"]
        mand_loc_ctramp{{"mandatory_locations_ctramp"}}
        fmt_mand_loc --> mand_loc_ctramp

        fmt_ind_trip["format_individual_trip"]
        ind_trip_ctramp{{"individual_trips_ctramp"}}
        fmt_ind_trip --> ind_trip_ctramp

        fmt_joint_trip["format_joint_trip"]
        joint_trip_ctramp{{"joint_trips_ctramp"}}
        fmt_joint_trip --> joint_trip_ctramp
    end

    %% Canonical inputs to stages (consolidated)
    inputs -.-> s1
    inputs -.-> s2
    inputs -.-> s3
    inputs -.-> s4

    %% Stage ordering (vertical layout)
    s1 ~~~ s2
    s2 ~~~ s3
    s3 ~~~ s4

    %% Formatted table dependencies (thick solid)
    hh_ctramp ==> fmt_ind_tour
    hh_ctramp ==> fmt_joint_tour
    ind_tour_ctramp ==> fmt_persons
    hh_ctramp ==> fmt_mand_loc
    hh_ctramp ==> fmt_ind_trip
    ind_tour_ctramp ==> fmt_ind_trip
    hh_ctramp ==> fmt_joint_trip

    %% Styling
    classDef canonClass fill:#e1f5ff,stroke:#0277bd,stroke-width:2px
    classDef formatterClass fill:#fff3e0,stroke:#ef6c00,stroke-width:2px
    classDef outputClass fill:#e8f5e9,stroke:#2e7d32,stroke-width:3px

    class hh_canon,per_canon,tours_canon,trips_canon,joint_trips_canon canonClass
    class fmt_hh,fmt_ind_tour,fmt_persons,fmt_mand_loc,fmt_ind_trip formatterClass
    class fmt_joint_tour,fmt_joint_trip formatterClass
    class hh_ctramp,per_ctramp,ind_tour_ctramp,ind_trip_ctramp outputClass
    class mand_loc_ctramp,joint_tour_ctramp,joint_trip_ctramp outputClass
```

**Legend:**

- 🔵 **Blue cylinders**: Canonical input tables
- 🟠 **Orange boxes**: Formatter functions
- 🟢 **Green hexagons**: CT-RAMP output tables (all formatted)
- **Dashed arrows (⋯→)**: Canonical inputs available to formatters
  (see component details above for specifics)
- **Thick solid arrows (⟹)**: Formatted table dependencies
  (execution order)

**Note:** Canonical input details for each formatter are listed in the
[Components](#components) section above.

## Execution Stages

1. TAZ Filtering (optional): Households without valid home_taz are removed,
   cascading to persons, tours, and trips
2. Households: Formatted first to establish income brackets needed downstream
3. Tours: Formatted before persons to provide tour frequency statistics
4. Persons: Combines tour-derived fields (activity patterns, frequencies) with
   demographic characteristics
5. Mandatory Locations: Uses both canonical (TAZ) and formatted (income)
   household data
6. Trips: Formatted last, linking to already-formatted tours

## Configuration

All thresholds and defaults are managed via
[`CTRAMPConfig`][processing.formatting.ctramp.ctramp_config.CTRAMPConfig]

## Implementation Notes

### Excluded Fields

- Random Number Fields: Excluded because they are simulation-specific and not
  derivable from survey data (household: `ao_rn`, `fp_rn`, `cdap_rn`; tour: `imtf_rn`,
  `imtod_rn`, `immc_rn`, `jtf_rn`, `jtl_rn`, `jtod_rn`, etc.)
- Model Output Fields: `auto_suff` (auto sufficiency), `walk_subzone`
  (walk-to-transit accessibility), wait times and logsums

### Placeholder Values

When tour data is unavailable or incomplete, person-level tour frequency fields
use these defaults:

- `activity_pattern`: 'H' (home all day)
- `imf_choice`: 0 (no mandatory tours)
- `inmf_choice`: 1 (minimum valid code)
- `wfh_choice`: 0 (no work from home)
- `jtf_choice`: Derived from joint tour data if available; otherwise NONE_NONE (-4)

### Empty Data Handling

The module gracefully handles missing data:

- Empty tour DataFrames result in placeholder person fields
- Missing joint tour IDs default to individual tour treatment
- Missing or null values use sensible defaults per field type

"""

import logging

import polars as pl

from data_canon.codebook.ctramp import CTRAMPEmploymentCategory
from data_canon.codebook.tours import TourCategory, TourDataQuality
from data_canon.models.ctramp import (
    AOResultsCTRAMPModel,
    CDAPResultsCTRAMPModel,
    HouseholdCTRAMPModel,
    IndividualTourCTRAMPModel,
    IndividualTripCTRAMPModel,
    JointTourCTRAMPModel,
    JointTripCTRAMPModel,
    MandatoryLocationCTRAMPModel,
    PersonCTRAMPModel,
)
from pipeline.decoration import step

from .ctramp_config import CTRAMPConfig
from .format_ao import format_ao_results
from .format_cdap import format_cdap_results
from .format_households import format_households
from .format_mandatory_location import format_mandatory_location
from .format_persons import enrich_persons_with_person_type, format_persons
from .format_tours import format_individual_tour, format_joint_tour
from .format_trips import format_individual_trip, format_joint_trip
from .mappings import EMPLOYMENT_TO_CTRAMP, ctramp_student_category_expression

logger = logging.getLogger(__name__)


MODEL_MAP = {
    "households_ctramp": HouseholdCTRAMPModel,
    "persons_ctramp": PersonCTRAMPModel,
    "mandatory_locations_ctramp": MandatoryLocationCTRAMPModel,
    "individual_trips_ctramp": IndividualTripCTRAMPModel,
    "individual_tours_ctramp": IndividualTourCTRAMPModel,
    "joint_trips_ctramp": JointTripCTRAMPModel,
    "joint_tours_ctramp": JointTourCTRAMPModel,
    "cdap_results_ctramp": CDAPResultsCTRAMPModel,
    "ao_results_ctramp": AOResultsCTRAMPModel,
}


def _drop_by_tour_ids(
    tours: pl.DataFrame,
    linked_trips: pl.DataFrame,
    joint_trips: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Cascade a tour filter down to linked trips and joint trips.

    Keeps only linked trips whose ``tour_id`` still exists in ``tours``, then
    keeps only joint trips whose ``joint_trip_id`` still appears in the surviving
    linked trips. This preserves referential integrity after tours are removed.

    Args:
        tours: Tours that survived a filter (defines the surviving tour_ids)
        linked_trips: Linked trips to prune to the surviving tours
        joint_trips: Joint trips to prune to the surviving linked trips

    Returns:
        Tuple of (tours, linked_trips, joint_trips) with integrity maintained
    """
    linked_trips = linked_trips.filter(pl.col("tour_id").is_in(tours["tour_id"].implode()))

    if len(joint_trips) > 0 and "joint_trip_id" in linked_trips.columns:
        valid_joint_trip_ids = linked_trips.filter(pl.col("joint_trip_id").is_not_null())[
            "joint_trip_id"
        ].unique()
        joint_trips = joint_trips.filter(
            pl.col("joint_trip_id").is_in(valid_joint_trip_ids.implode())
        )

    return tours, linked_trips, joint_trips


def _drop_invalid_tours(
    tours: pl.DataFrame,
    linked_trips: pl.DataFrame,
    joint_trips: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Remove tours unusable for the model, cascading to linked and joint trips.

    Drops tours that are not VALID (single-trip, loop, missing-anchor,
    change-mode, indeterminate) or not COMPLETE (do not start and end at home).
    Mirrors the DaySim formatter (``format_daysim``), which drops both, so the
    CT-RAMP and DaySim outputs contain the same set of tours. Without this,
    single-trip/loop tours (which carry a null ``tour_purpose``) survive into
    CT-RAMP output and are silently coerced to the ``OTHDISCR`` catch-all purpose.

    Args:
        tours: Canonical tour data with ``tour_data_quality`` and ``tour_category``
        linked_trips: Canonical linked trip data
        joint_trips: Aggregated joint trip data

    Returns:
        Tuple of (tours, linked_trips, joint_trips) with unusable tours and their
        orphaned trips removed.
    """
    # tour_data_quality is produced during tour extraction; guard for callers
    # (e.g. schema-only empty frames) that do not carry every column.
    has_quality = "tour_data_quality" in tours.columns
    has_category = "tour_category" in tours.columns
    if not (has_quality or has_category):
        return tours, linked_trips, joint_trips

    is_valid = pl.col("tour_data_quality") == TourDataQuality.VALID.value
    is_complete = pl.col("tour_category") == TourCategory.COMPLETE.value

    # Tag each tour's drop reason so the log can distinguish structurally invalid
    # tours from partial/open ones (valid but not home-based).
    if has_quality and has_category:
        keep = is_valid & is_complete
        reason = pl.when(~is_valid).then(pl.lit("invalid")).otherwise(pl.lit("partial/open"))
    elif has_quality:
        keep = is_valid
        reason = pl.lit("invalid")
    else:
        keep = is_complete
        reason = pl.lit("partial/open")

    n_og_tours = len(tours)
    dropped = tours.filter(~keep).with_columns(reason.alias("_reason"))
    _log_drop_summary(dropped, linked_trips, n_og_tours)

    tours = tours.filter(keep)
    tours, linked_trips, joint_trips = _drop_by_tour_ids(tours, linked_trips, joint_trips)
    return tours, linked_trips, joint_trips


def _completeness_split(df: pl.DataFrame) -> tuple[int, int]:
    """Return (already_incomplete, net_new) counts from the ``complete`` flag.

    ``already_incomplete`` are records flagged incomplete (household / person /
    day cascade) — they were already excluded from the weighted model. ``net_new``
    are otherwise-complete records now being dropped. When the ``complete`` column
    is absent (e.g. tests), everything is treated as net-new.
    """
    total = df.height
    if "complete" not in df.columns:
        return 0, total
    incomplete = df.filter(~pl.col("complete").fill_null(value=False)).height
    return incomplete, total - incomplete


def _drop_grid(title: str, df: pl.DataFrame, denom: int) -> list[str]:
    """Build the lines of one drop grid (tours or member trips).

    ``net %`` is the net-new share of all records — the genuinely new data loss.
    """
    header = f"  {title:<14}{'dropped':>9}{'incomplete':>13}{'net-new':>10}{'net %':>8}"

    def row(name: str, incomplete: int, net: int) -> str:
        total = incomplete + net
        pct = net * 100 / denom if denom > 0 else 0
        return f"  {name:<14}{total:>9,}{incomplete:>13,}{net:>10,}{pct:>7.1f}%"

    lines = [header]
    for label in ("invalid", "partial/open"):
        incomplete, net = _completeness_split(df.filter(pl.col("_reason") == label))
        if incomplete + net == 0:
            continue
        lines.append(row(label, incomplete, net))
    incomplete, net = _completeness_split(df)
    lines.append(row("total", incomplete, net))
    return lines


def _log_drop_summary(dropped: pl.DataFrame, linked_trips: pl.DataFrame, n_og_tours: int) -> None:
    """Log a two-grid table of dropped tours and member trips.

    Splits each count into records that were already incomplete (household /
    person / day flagged incomplete — already out of the weighted model) versus
    net-new removal of otherwise-complete records, so the log makes clear how
    much is genuinely new loss.
    """
    if dropped.height == 0:
        return

    dropped_trips = linked_trips.join(
        dropped.select("tour_id", "_reason"), on="tour_id", how="inner"
    )
    lines = [
        "Dropped tours/member-trips to match DaySim (DaySim already drops these).",
        '  "incomplete" = flagged incomplete via the household>person>day>trip cascade; '
        '"net-new" = otherwise-complete records.',
        "",
        *_drop_grid("TOURS", dropped, n_og_tours),
        "",
        *_drop_grid("MEMBER TRIPS", dropped_trips, len(linked_trips)),
    ]
    logger.info("\n".join(lines))


def _drop_missing_taz(
    households: pl.DataFrame,
    persons: pl.DataFrame,
    tours: pl.DataFrame,
    linked_trips: pl.DataFrame,
    joint_trips: pl.DataFrame,
    config: CTRAMPConfig,
) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """Remove records with missing TAZ fields and ensure referential integrity.

    Performs cascading deletions to maintain data consistency:
    1. Remove households without valid home TAZ
    2. Remove persons from dropped households
    3. Remove tours/trips with missing origin or destination TAZ
    4. Remove tours that have no trips remaining
    5. Remove joint trips that have no linked trips remaining

    Args:
        households: Canonical household data
        persons: Canonical person data
        tours: Canonical tour data
        linked_trips: Canonical linked trip data
        joint_trips: Canonical joint trip data
        config: CTRAMP configuration with taz_field name

    Returns:
        Tuple of filtered DataFrames maintaining referential integrity
    """
    # Track original counts for logging
    counts = {
        "households": len(households),
        "persons": len(persons),
        "tours": max(0, len(tours)),
        "linked_trips": max(0, len(linked_trips)),
        "joint_trips": max(0, len(joint_trips)),
    }

    # Step 1: Filter households by home TAZ
    households = households.filter(
        pl.col(f"home_{config.taz_field}").is_not_null()
        & (pl.col(f"home_{config.taz_field}") != -1)
    )
    valid_hh_ids = households["hh_id"]

    # Step 2: Remove orphaned persons
    persons = persons.filter(pl.col("hh_id").is_in(valid_hh_ids.implode()))

    logger.info(
        "Dropped %d households without valid home TAZ (keeping %d households, %d persons)",
        counts["households"] - len(households),
        len(households),
        len(persons),
    )

    # Step 3: Filter tours by household and TAZ fields
    if len(tours) > 0:
        tours = tours.filter(
            pl.col("hh_id").is_in(valid_hh_ids.implode())
            & pl.col(f"o_{config.taz_field}").is_not_null()
            & (pl.col(f"o_{config.taz_field}") != -1)
            & pl.col(f"d_{config.taz_field}").is_not_null()
            & (pl.col(f"d_{config.taz_field}") != -1)
        )
        valid_tour_ids = tours["tour_id"]
    else:
        valid_tour_ids = pl.Series("tour_id", [], dtype=pl.Int64)

    # Step 4: Filter linked trips by tour and TAZ fields
    if len(linked_trips) > 0:
        linked_trips = linked_trips.filter(
            pl.col("tour_id").is_in(valid_tour_ids.implode())
            & pl.col(f"o_{config.taz_field}").is_not_null()
            & (pl.col(f"o_{config.taz_field}") != -1)
            & pl.col(f"d_{config.taz_field}").is_not_null()
            & (pl.col(f"d_{config.taz_field}") != -1)
        )
        # Get tours that still have trips
        tours_with_trips = linked_trips["tour_id"].unique()

        # Remove tours that lost all their trips
        if len(tours) > 0:
            tours_before = len(tours)
            tours = tours.filter(pl.col("tour_id").is_in(tours_with_trips.implode()))
            if tours_before != len(tours):
                logger.info(
                    "Removed %d tours that had no valid trips remaining",
                    tours_before - len(tours),
                )
    # No trips means no tours should remain
    elif len(tours) > 0:
        logger.info("Removed all %d tours because no valid trips exist", len(tours))
        tours = tours.head(0)

    # Step 5: Filter joint trips by linked trips and TAZ fields
    if len(joint_trips) > 0:
        # First filter by household
        joint_trips = joint_trips.filter(pl.col("hh_id").is_in(valid_hh_ids))

        # Filter by TAZ fields if they exist in joint_trips
        taz_cols = [f"o_{config.taz_field}", f"d_{config.taz_field}"]
        has_taz = all(col in joint_trips.columns for col in taz_cols)
        if has_taz:
            joint_trips = joint_trips.filter(
                pl.col(f"o_{config.taz_field}").is_not_null()
                & (pl.col(f"o_{config.taz_field}") != -1)
                & pl.col(f"d_{config.taz_field}").is_not_null()
                & (pl.col(f"d_{config.taz_field}") != -1)
            )

        # Keep only joint trips that have corresponding linked trips
        if len(linked_trips) > 0 and "joint_trip_id" in linked_trips.columns:
            valid_joint_trip_ids = linked_trips.filter(pl.col("joint_trip_id").is_not_null())[
                "joint_trip_id"
            ].unique()
            joint_trips = joint_trips.filter(pl.col("joint_trip_id").is_in(valid_joint_trip_ids))
        else:
            # No linked trips with joint_trip_id means no joint trips should remain
            logger.info(
                "Removed all %d joint trips because no valid linked trips exist", len(joint_trips)
            )
            joint_trips = joint_trips.head(0)

    # Final logging
    logger.info(
        "TAZ filtering complete:\n"
        "  Tours: %d → %d (dropped %d)\n"
        "  Linked trips: %d → %d (dropped %d)\n"
        "  Joint trips: %d → %d (dropped %d)",
        counts["tours"],
        len(tours),
        counts["tours"] - len(tours),
        counts["linked_trips"],
        len(linked_trips),
        counts["linked_trips"] - len(linked_trips),
        counts["joint_trips"],
        len(joint_trips),
        counts["joint_trips"] - len(joint_trips),
    )

    return households, persons, tours, linked_trips, joint_trips


def _drop_excess_fields(
    df: pl.DataFrame,
    model_cls: type,
) -> pl.DataFrame:
    """Drop columns from df that are not in the data model class.

    Args:
        df: Input DataFrame
        model_cls: Data model class with defined fields
    Returns:
        DataFrame with only columns defined in the model class
    valid_fields = set(model_cls.model_fields.keys())
    cols_to_drop = [col for col in df.columns if col not in valid_fields]
    return df.drop(cols_to_drop)
    """
    valid_fields = set(model_cls.model_fields.keys())
    cols_to_drop = set(df.columns) - valid_fields
    return df.drop(cols_to_drop)


@step(
    requires={
        "persons": {
            "person_num",
            "gender",
            "job_type",
            "commute_subsidy_use_3",
            "commute_subsidy_use_4",
        },
        "linked_trips": {
            "o_purpose",
            "d_purpose",
            "access_mode",
            "egress_mode",
        },
        "tours": {"num_travelers"},
    },
)
def format_ctramp(
    persons: pl.DataFrame,
    households: pl.DataFrame,
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
    joint_trips: pl.DataFrame,
    income_low_threshold: int,
    income_med_threshold: int,
    income_high_threshold: int,
    income_base_year_dollars: int,
    taz_field: str = "taz",
    drop_missing_taz: bool = True,
    drop_invalid_tours: bool = True,
) -> dict[str, pl.DataFrame]:
    """Format canonical survey data to CT-RAMP model specification.

    Transforms person, household, tour, and trip data from canonical format to
    CT-RAMP format required by the activity-based travel demand model. See module
    docstring for complete component descriptions and data flow.

    Args:
        persons: Canonical person data with demographic fields. Required columns:
            person_id, hh_id, person_num, age, gender, employment, student,
            school_type, commute subsidies.
        households: Canonical household data with income and dwelling fields.
            Required columns: hh_id, home_taz, income_detailed, income_followup,
            num_vehicles.
        linked_trips: Canonical linked trip data. Required columns: linked_trip_id,
            tour_id, o_purpose_category, d_purpose_category, mode_type, o_taz,
            d_taz, tour_direction, times, person_id, hh_id.
        tours: Canonical tour data. Required columns: tour_id, hh_id, person_id,
            person_num, tour_category, tour_purpose, o_taz, d_taz, times,
            tour_mode, joint_tour_id, parent_tour_id.
        joint_trips: Aggregated joint trip data. Required columns: joint_trip_id,
            hh_id, num_joint_travelers.
        income_low_threshold: Dollar value dividing low from medium income bracket.
            Must be less than income_med_threshold.
        income_med_threshold: Dollar value dividing medium from high income bracket.
            Must be between income_low_threshold and income_high_threshold.
        income_high_threshold: Dollar value dividing high from very high income
            bracket. Must be greater than income_med_threshold.
        income_base_year_dollars: Target year for income conversion (e.g., 2000,
            2010). Categorical income values are converted to midpoint dollars in
            this base year.
        taz_field: Field name containing the TAZ ID for CTRAMP formatting
            (default: "taz").
        drop_missing_taz: If True, remove households without valid TAZ IDs. This
            cascades to persons, tours, and trips (default: True).
        drop_invalid_tours: If True, remove tours that are not VALID (single-trip,
            loop, missing-anchor, change-mode, indeterminate) or not COMPLETE (do
            not start and end at home), mirroring the DaySim formatter (which drops
            both). Cascades to linked and joint trips (default: True).

    Returns:
        Dictionary with keys:
            - households_ctramp: Formatted household data (7 core fields including
              income, TAZ, size)
            - persons_ctramp: Formatted person data (20+ fields including person
              type, activity patterns, tour frequencies)
            - mandatory_locations_ctramp: Mandatory location data (work/school
              location records)
            - individual_tours_ctramp: Individual tour data (tour-level attributes:
              purpose, mode, time, stops)
            - individual_trips_ctramp: Individual trip data (trip-level attributes
              for individual tours)
            - joint_tours_ctramp: Joint tour data (joint tours with composition and
              participants)
            - joint_trips_ctramp: Joint trip data (trip-level attributes for joint
              tours)

    !!! Example

        ```python
        from processing.formatting.ctramp import format_ctramp

        result = format_ctramp(
            persons=canonical_persons,
            households=canonical_households,
            linked_trips=canonical_linked_trips,
            tours=canonical_tours,
            joint_trips=canonical_joint_trips,
            income_low_threshold=60000,          # $60k divides low from medium
            income_med_threshold=150000,         # $150k divides medium from high
            income_high_threshold=250000,        # $250k divides high from very high
            income_base_year_dollars=2000,       # Convert income to $2000
            drop_missing_taz=True                # Remove households without TAZ
        )

        # Access formatted tables
        households_ctramp = result["households_ctramp"]
        persons_ctramp = result["persons_ctramp"]
        mandatory_locations_ctramp = result["mandatory_locations_ctramp"]
        individual_tours_ctramp = result["individual_tours_ctramp"]
        joint_tours_ctramp = result["joint_tours_ctramp"]
        individual_trips_ctramp = result["individual_trips_ctramp"]
        joint_trips_ctramp = result["joint_trips_ctramp"]
        ```
    """
    # Validate configuration parameters
    config = CTRAMPConfig(
        income_low_threshold=income_low_threshold,
        income_med_threshold=income_med_threshold,
        income_high_threshold=income_high_threshold,
        income_base_year_dollars=income_base_year_dollars,
        drop_missing_taz=drop_missing_taz,
        drop_invalid_tours=drop_invalid_tours,
        taz_field=taz_field,
    )
    logger.info("Starting CT-RAMP formatting")

    # Drop invalid/partial tours before anything else so CT-RAMP and DaySim
    # outputs contain the same set of tours (and no null-purpose leakage).
    if config.drop_invalid_tours:
        tours, linked_trips, joint_trips = _drop_invalid_tours(tours, linked_trips, joint_trips)

    # Ensure TAZ columns are Int64 for filtering
    households = households.with_columns(pl.col(f"home_{config.taz_field}").cast(pl.Int64))

    # Drop any households that do not have a TAZ assigned
    if config.drop_missing_taz:
        (
            households,
            persons,
            tours,
            linked_trips,
            joint_trips,
        ) = _drop_missing_taz(households, persons, tours, linked_trips, joint_trips, config)

    # Format each table ----------------------------------------------------
    # Format households first since it has no derived field dependencies
    households_ctramp = format_households(households, persons, tours, config)

    # Derive/validate person_type and type for use in tour/trip formatting
    # Pre-compute student_category and employment_category so person_type
    # expression can use them for consistent classification
    if "student_category" not in persons.columns:
        persons = persons.with_columns(
            ctramp_student_category_expression(school_taz_col=f"school_{config.taz_field}").alias(
                "student_category"
            )
        )
    if "employment_category" not in persons.columns:
        persons = persons.with_columns(
            pl.col("employment")
            .replace_strict(
                EMPLOYMENT_TO_CTRAMP,
                default=CTRAMPEmploymentCategory.NOT_EMPLOYED.value,
            )
            .alias("employment_category")
        )
    persons_with_type = enrich_persons_with_person_type(persons)

    # Format tours - use empty DataFrame with proper schema if no tours exist
    if len(tours) == 0:
        individual_tours_ctramp = pl.DataFrame(
            schema={
                "person_id": pl.Int64,
                "tour_purpose": pl.String,
            }
        )
    else:
        individual_tours_ctramp = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=linked_trips,
            persons_canonical=persons_with_type,
            households_ctramp=households_ctramp,
            config=config,
        )

    # Format persons with tour statistics (works with empty or populated tours)
    persons_ctramp = format_persons(
        persons_canonical=persons_with_type,
        tours_ctramp=individual_tours_ctramp,
        config=config,
    )

    # Format mandatory locations - uses formatted persons and households
    mandatory_location_ctramp = format_mandatory_location(
        persons_ctramp=persons_ctramp,
        households_ctramp=households_ctramp,
        linked_trips_canonical=linked_trips,
        config=config,
    )

    # Add formatted tours to results
    joint_tours_ctramp = format_joint_tour(
        tours_canonical=tours,
        linked_trips_canonical=linked_trips,
        persons_canonical=persons,
        households_ctramp=households_ctramp,
        config=config,
    )

    individual_trips_ctramp = format_individual_trip(
        linked_trips_canonical=linked_trips,
        tours_ctramp=individual_tours_ctramp,
        persons_canonical=persons,
        households_ctramp=households_ctramp,
        config=config,
    )

    joint_trips_ctramp = format_joint_trip(
        joint_trips_canonical=joint_trips,
        linked_trips_canonical=linked_trips,
        tours_canonical=tours,
        households_ctramp=households_ctramp,
        config=config,
    )

    logger.info("CT-RAMP formatting complete")

    # Prepare result dictionary and clean up temporary columns
    tables = {
        "households_ctramp": households_ctramp,
        "persons_ctramp": persons_ctramp,
        "individual_trips_ctramp": individual_trips_ctramp,
        "individual_tours_ctramp": individual_tours_ctramp,
        "joint_trips_ctramp": joint_trips_ctramp,
        "joint_tours_ctramp": joint_tours_ctramp,
        "mandatory_locations_ctramp": mandatory_location_ctramp,
    }

    # Derive cdapResults / aoResults from the formatted persons/households before
    # the cleanup loop trims each table to its own model.
    tables["cdap_results_ctramp"] = format_cdap_results(tables["persons_ctramp"])
    tables["ao_results_ctramp"] = format_ao_results(tables["households_ctramp"])

    # Cleanup tables
    for table_name, df in tables.items():
        model_cls = MODEL_MAP[table_name]
        tables[table_name] = _drop_excess_fields(df, model_cls)

    return tables
