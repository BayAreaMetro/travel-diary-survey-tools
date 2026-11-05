# Controls

The `controls:` list of [Compute Weights](compute_weights.md#settings) names the
targets the weights are balanced to. Each entry is a name from the table below,
with an optional `importance` and an optional `merge` that joins categories into
one, using the category names shown:

```yaml
controls:
  - name: h_total
    importance: 200
  - name: h_size
    merge:
      size_4_plus: [size_4, size_5, size_6, size_7, size_8, size_9, size_10_plus]
  - name: p_gender
```

- `h_total` is required: the balancer matches every zone's household total first.
- A control without its own `importance` gets one from its PUMS margin of error
  when `balancing.moe_based_importance` is true, and `balancing.default_importance`
  otherwise.
- Group and category names are read in lower case, so `AAPI:` names the group `aapi`.

## Merging in some zones only

`zone_merges` joins categories in the zones it names and leaves the others as they
are. Each key is a balancing zone: a zone group's name, or a zone ID where the zone
is in no group. A zone merge can join groups the global `merge` already made:

```yaml
controls:
  - name: h_workers
    merge:
      workers_3_plus: [workers_3, workers_4, workers_5_plus]
    zone_merges:
      "Marin/Napa/Solano/Sonoma":
        workers_2_plus: [workers_2, workers_3_plus]
```

The merged group is a target only in the zones listed. Elsewhere the separate
categories stay targets.

## Households per PUMA

`h_puma` is listed like any other control, but it is not in the table below: its
categories are the crosswalk's PUMAs, found when the run starts, named
`puma_<PUMA ID>` (such as `puma_01308`), plus `outside_region` for PUMS households
outside every zone.

- Each home's PUMA comes from the same PUMA boundaries the PUMS is allocated by, and
  is written to `household_weights` as `home_puma`.
- It is never imputed: a household with no PUMA stops the run.

A survey rarely reaches enough homes in every PUMA to target each one. Grouping a
zone's PUMAs by county keeps each county's households right where counties share a
zone, without a zone per county:

```yaml
controls:
  - name: h_puma
    zone_merges:
      "Alameda/Contra Costa/San Mateo":  # PUMA lists shortened
        alameda: [puma_00101, puma_00111, puma_00112, ...]
        contra_costa: [puma_01301, puma_01305, puma_01308, ...]
        san_mateo: [puma_08101, puma_08102, puma_08103, ...]
```

Merging all of a zone's PUMAs into one group turns the target off in that zone.
A category that merges replace in every zone is left out of the report's tables.

## Available controls

<!-- weighting-controls -->

Code: [processing.weighting.controls](../../reference/processing/weighting/controls.md)
