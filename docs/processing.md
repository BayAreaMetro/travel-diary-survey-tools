# Processing Functions

Data processing functions for survey data transformation and enrichment.

## Overview

Processing functions are organized by topic and can be composed into pipelines. Each function is decorated with `@step()` to enable pipeline integration, caching, and logging.

## Read/Write

::: processing.read_write.read_write.load_data

::: processing.read_write.read_write.write_data

## Trip Linking

::: processing.link_trips.link.link_trips

## Joint Trip Detection

::: processing.joint_trips.detect_joint_trips
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - detect_joint_trips

## Tour Extraction

::: processing.tours.extraction
    options:
      show_root_heading: true
      show_root_toc_entry: false
      members:
        - extract_tours

## Zone Assignment

::: processing.add_zone_ids.add_zone_ids.add_zone_ids

## Weighting

::: processing.weighting.existing_weights.add_existing_weights

## Formatting - DaySim

::: processing.formatting.daysim.format_daysim.format_daysim

## Formatting - CTRAMP

::: processing.formatting.ctramp.format_ctramp.format_ctramp
