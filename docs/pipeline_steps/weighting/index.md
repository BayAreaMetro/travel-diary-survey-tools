# Weighting

::: processing.weighting
    options:
      show_root_heading: true
      show_docstring_description: true
      members: false

## Overview

Weighting has two mutually exclusive paths:

- **`compute_weights`** computes new weights from PUMS controls and survey seed data.
- **`add_existing_weights`** attaches weights that were already computed elsewhere.

Only the `compute_weights` path needs the full weighting pipeline machinery such as PUMS fetching, crosswalk construction, incidence preparation, control aggregation, balancing, diagnostics, and control validation. The `add_existing_weights` path is much lighter: it joins external weight files onto canonical tables and can optionally derive missing downstream weights through the survey hierarchy.

## Choose a Path

| Path | Use when | Key inputs | Main output |
|---|---|---|---|
| **Compute Weights** | You need to create expansion weights from controls | PUMS, geography, control definitions, survey tables | New household weights propagated to all tables |
| **Existing Weights** | You already have weight files from another system or prior run | Weight CSVs keyed to canonical IDs | Existing weights attached and optionally propagated |

## Branches

```mermaid
flowchart TD
  A["Weighting"] --> B["Compute Weights\nDerive new weights from controls"]
  A --> C["Existing Weights\nAttach pre-computed weights"]
```

The `compute_weights` branch is the full control-based weighting pipeline. The `add_existing_weights` branch bypasses that machinery and simply joins external weight files to the canonical tables, with optional downstream propagation.

For implementation details, see the dedicated pages for [compute_weights](compute_weights.md) and [existing_weights](existing_weights.md).
