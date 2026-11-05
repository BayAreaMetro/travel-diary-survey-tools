"""CT-RAMP Formatting Step.

Transforms canonical survey data (persons, households, tours, trips) into
CT-RAMP (Coordinated Travel - Regional Activity Modeling Platform) format for
use with activity-based travel demand models. See [CTRAMP Data Models][ctramp-output-models].

This module orchestrates the transformation of households, persons, tours, and
trips from canonical format into eleven CT-RAMP tables, handling missing data
and providing configurable income thresholds. Which records it writes is the
``usability_profile``'s choice, not this step's.

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

1. Record selection: keeps the records the ``usability_profile`` admits
   (``usable_<profile>``); a profile's ``zone_coverage`` leaves out records with no TAZ
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
- `inmf_choice`: 0 (no non-mandatory tours)
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

from data_canon.codebook.ctramp import CTRAMPEmploymentCategory, CTRAMPPersonType
from data_canon.models.ctramp import (
    AllTourCTRAMPModel,
    AllTripCTRAMPModel,
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
from processing.formatting.usable_records import keep_usable
from processing.formatting.usual_workplace import fill_usual_workplace

from .ctramp_config import CTRAMPConfig
from .day_ids import incorporate_day_into_ids
from .format_ao import format_ao_results
from .format_cdap import format_cdap_results
from .format_households import format_households
from .format_joint_trips import format_joint_trip
from .format_mandatory_location import format_mandatory_location
from .format_persons import enrich_persons_with_person_type, format_persons
from .format_tours import format_individual_tour, format_joint_tour
from .format_trips import format_individual_trip
from .person_mappings import EMPLOYMENT_TO_CTRAMP
from .student_mappings import ctramp_student_category_expression

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
    "all_tours_ctramp": AllTourCTRAMPModel,
    "all_trips_ctramp": AllTripCTRAMPModel,
}


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
    """
    valid_fields = set(model_cls.model_fields.keys())
    cols_to_drop = set(df.columns) - valid_fields
    return df.drop(cols_to_drop)


@step(
    # Derived by ablation against the 2023 canonical output: every column below was
    # dropped in turn and the step re-run, keeping those whose absence raised. Columns
    # whose absence merely degrades the output (weights, `industry`, `tnc_type`,
    # the usability flag, the `person_day_id`s) are deliberately excluded - see module note.
    # Zone columns are omitted because their names come from the `taz_field` param
    # (e.g. `o_TAZ1454`) and so cannot be expressed statically.
    requires={
        "households": {
            "hh_id",
            "home_lat",
            "home_lon",
            "income_bin",
            "num_vehicles",
        },
        "persons": {
            "person_id",
            "hh_id",
            "person_num",
            "age",
            "gender",
            "employment",
            "student",
            "school_type",
            "school_lat",
            "school_lon",
            "work_lat",
            "work_lon",
            "commute_subsidy_provide_free_parking",
            "commute_subsidy_provide_discounted_parking",
            "commute_subsidy_use_free_parking",
            "commute_subsidy_use_discounted_parking",
        },
        "linked_trips": {
            "linked_trip_id",
            "tour_id",
            "o_purpose_category",
            "d_purpose_category",
            "o_lat",
            "o_lon",
            "d_lat",
            "d_lon",
            "depart_time",
            "arrive_time",
            "distance_meters",
            "mode_type",
            "access_mode",
            "egress_mode",
            "tour_direction",
            "num_travelers",
        },
        "tours": {
            "tour_id",
            "parent_tour_id",
            "subtour_num",
            "tour_purpose",
            "tour_category",
            "tour_mode",
            "origin_depart_time",
            "origin_arrive_time",
        },
        "joint_trips": {"joint_trip_id", "hh_day_id"},
        "person_days": {"person_id", "hh_id", "person_day_id", "hh_day_id"},
    },
)
def format_ctramp(
    persons: pl.DataFrame,
    households: pl.DataFrame,
    unlinked_trips: pl.DataFrame,
    linked_trips: pl.DataFrame,
    tours: pl.DataFrame,
    joint_trips: pl.DataFrame,
    joint_tours: pl.DataFrame,
    person_days: pl.DataFrame,
    ctramp: CTRAMPConfig,
    habitual_locations: pl.DataFrame | None = None,
    habitual_location_days: pl.DataFrame | None = None,
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
            Required columns: hh_id, home_lat, home_lon, income_bin, num_vehicles.
        linked_trips: Canonical linked trip data. Required columns: linked_trip_id,
            tour_id, o_purpose_category, d_purpose_category, mode_type, o_taz,
            d_taz, tour_direction, times, person_id, hh_id.
        tours: Canonical tour data. Required columns: tour_id, hh_id, subtour_num,
            parent_tour_id, joint_tour_id, tour_purpose, tour_category, tour_mode,
            origin_depart_time, origin_arrive_time, and the zone columns named by
            `taz_field`.
        joint_tours: Aggregated joint tour data carrying ``joint_tour_weight``.
            May be empty but must be provided.
        unlinked_trips: Canonical unlinked trips used to derive detailed transit
            submodes for formatted tours and trips. May be empty but must be provided.
        joint_trips: Aggregated joint trip data. Required columns: joint_trip_id,
            hh_id, num_joint_travelers.
        person_days: Canonical person-days data. Required columns: person_id, hh_id,
            person_day_id. Used to expand persons and households to the person-day
            level and encode the survey day into CTRAMP hh_id / person_id so
            that multi-day respondents produce one distinct CTRAMP record per
            day rather than one record that conflates all survey days.
        ctramp: Formatting settings: the usability profile deciding the
            record universe, the income brackets and dollar-year factor, the
            TAZ field, the adult age and the gender for missing values. See
            ``CTRAMPConfig``.
        habitual_locations: Habitual locations, zoned by ``add_zone_ids``. An
            employed person who reported no workplace takes their most-visited
            observed workplace as ``WorkLocation`` (see
            [`usual_workplace`][processing.formatting.usual_workplace]). None
            leaves the reported workplaces alone.
        habitual_location_days: Time spent at each habitual location per day,
            which picks the most-visited workplace.

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
            - all_tours_ctramp, all_trips_ctramp: individual and joint, together
            - cdap_results_ctramp, ao_results_ctramp: CDAP and auto ownership results

    !!! Example

        ```python
        from processing.formatting.ctramp import format_ctramp

        result = format_ctramp(
            persons=canonical_persons,
            households=canonical_households,
            unlinked_trips=canonical_unlinked_trips,
            linked_trips=canonical_linked_trips,
            tours=canonical_tours,
            joint_trips=canonical_joint_trips,
            joint_tours=canonical_joint_tours,
            days=canonical_days,
            ctramp=CTRAMPConfig(
                usability_profile="ctramp",  # the profile deciding the record universe
                income_low_threshold=30000,  # $30k divides low from medium ($2000)
                income_med_threshold=60000,  # $60k divides medium from high ($2000)
                income_high_threshold=100000,  # $100k divides high from very high ($2000)
                income_survey_year_to_ctramp_year=0.5319148936,  # 1/1.88: 2023 to $2000
                taz_field="taz",
                age_adult=4,
                gender_default_for_missing="f",
            ),
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
    logger.info("Starting CT-RAMP formatting")

    # One gate, read from the profile the config names. The formatter selects
    # nothing of its own: see the filters module for why deciding here rather
    # than in cascade_completeness put the weighting out of step with the output.
    gated = keep_usable(
        {
            "households": households,
            "persons": persons,
            "person_days": person_days,
            "tours": tours,
            "linked_trips": linked_trips,
            "unlinked_trips": unlinked_trips,
            "joint_trips": joint_trips,
            "joint_tours": joint_tours,
        },
        ctramp.usability_profile,
    )
    households = gated["households"]
    persons = gated["persons"]
    person_days = gated["person_days"]
    tours = gated["tours"]
    linked_trips = gated["linked_trips"]
    unlinked_trips = gated["unlinked_trips"]
    joint_trips = gated["joint_trips"]
    joint_tours = gated["joint_tours"]

    # The survey asks for a workplace only when the job has one fixed location;
    # for anyone else employed, their trips may show one.
    persons = fill_usual_workplace(
        persons,
        habitual_locations,
        habitual_location_days,
        zone_names=[ctramp.taz_field],
    )

    # CT-RAMP writes the TAZ as an integer.
    households = households.with_columns(pl.col(f"home_{ctramp.taz_field}").cast(pl.Int64))

    # Expand to person-day level: each survey day becomes a distinct CTRAMP
    # household/person so that tour and trip counts are correctly attributed
    # to a single day rather than aggregated across all days.
    (
        households,
        persons,
        tours,
        linked_trips,
        joint_trips,
    ) = incorporate_day_into_ids(households, persons, tours, linked_trips, joint_trips, person_days)

    # Format each table ----------------------------------------------------
    # Format households first since it has no derived field dependencies
    households_ctramp = format_households(households, persons, tours, ctramp)

    # Derive/validate person_type and type for use in tour/trip formatting
    # Pre-compute student_category and employment_category so person_type
    # expression can use them for consistent classification
    if "student_category" not in persons.columns:
        persons = persons.with_columns(
            ctramp_student_category_expression(school_taz_col=f"school_{ctramp.taz_field}").alias(
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

    # Children under 16 get UNDER_16 employment category regardless of reported employment
    persons_with_type = persons_with_type.with_columns(
        pl.when(
            pl.col("person_type").is_in(
                [
                    CTRAMPPersonType.CHILD_UNDER_5.value,
                    CTRAMPPersonType.STUDENT_NON_DRIVING_AGE.value,
                ]
            )
        )
        .then(pl.lit(CTRAMPEmploymentCategory.UNDER_16.value))
        .otherwise(pl.col("employment_category"))
        .alias("employment_category")
    )

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
            unlinked_trips_canonical=unlinked_trips,
            persons_canonical=persons_with_type,
            households_ctramp=households_ctramp,
            config=ctramp,
        )

    # Format persons with tour statistics (works with empty or populated tours)
    persons_ctramp = format_persons(
        persons_canonical=persons_with_type,
        tours_ctramp=individual_tours_ctramp,
        config=ctramp,
    )

    # Format mandatory locations - uses formatted persons and households
    mandatory_location_ctramp = format_mandatory_location(
        persons_ctramp=persons_ctramp,
        households_ctramp=households_ctramp,
        linked_trips_canonical=linked_trips,
        config=ctramp,
    )

    # Add formatted tours to results
    joint_tours_ctramp = format_joint_tour(
        tours_canonical=tours,
        linked_trips_canonical=linked_trips,
        unlinked_trips_canonical=unlinked_trips,
        joint_tours_canonical=joint_tours,
        persons_canonical=persons,
        households_ctramp=households_ctramp,
        config=ctramp,
    )

    individual_trips_ctramp = format_individual_trip(
        linked_trips_canonical=linked_trips,
        unlinked_trips_canonical=unlinked_trips,
        tours_ctramp=individual_tours_ctramp,
        persons_canonical=persons,
        households_ctramp=households_ctramp,
        config=ctramp,
    )

    joint_trips_ctramp = format_joint_trip(
        joint_trips_canonical=joint_trips,
        linked_trips_canonical=linked_trips,
        unlinked_trips_canonical=unlinked_trips,
        tours_canonical=tours,
        households_ctramp=households_ctramp,
        config=ctramp,
    )

    # The unified tables: the same formatting over the whole tour set, so a joint
    # tour appears as its participants' own per-person tours. Nothing is exploded
    # and no weight is rescaled -- those rows already exist in the canonical data.
    if len(tours) == 0:
        all_tours_ctramp = individual_tours_ctramp.with_columns(
            pl.lit(1).cast(pl.Int64).alias("num_participants")
        )
        all_trips_ctramp = individual_trips_ctramp
    else:
        all_tours_ctramp = format_individual_tour(
            tours_canonical=tours,
            linked_trips_canonical=linked_trips,
            unlinked_trips_canonical=unlinked_trips,
            persons_canonical=persons_with_type,
            households_ctramp=households_ctramp,
            config=ctramp,
            include_joint=True,
        )
        all_trips_ctramp = format_individual_trip(
            linked_trips_canonical=linked_trips,
            unlinked_trips_canonical=unlinked_trips,
            tours_ctramp=all_tours_ctramp,
            persons_canonical=persons,
            households_ctramp=households_ctramp,
            config=ctramp,
        )

    # Prepare result dictionary and clean up temporary columns
    # Two tables come out of group_bys in no fixed order; sort them so runs match.
    tables = {
        "households_ctramp": households_ctramp.sort("hh_id"),
        "persons_ctramp": persons_ctramp,
        "individual_trips_ctramp": individual_trips_ctramp,
        "individual_tours_ctramp": individual_tours_ctramp,
        "joint_trips_ctramp": joint_trips_ctramp,
        # With no joint tours the table has no columns to sort on.
        "joint_tours_ctramp": (
            joint_tours_ctramp.sort("hh_id", "tour_id")
            if joint_tours_ctramp.width
            else joint_tours_ctramp
        ),
        "mandatory_locations_ctramp": mandatory_location_ctramp,
        "all_tours_ctramp": all_tours_ctramp,
        "all_trips_ctramp": all_trips_ctramp,
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
