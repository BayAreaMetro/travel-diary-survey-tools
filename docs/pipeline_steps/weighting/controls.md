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

## Available controls

<!-- weighting-controls -->

Code: [processing.weighting.controls](../../reference/processing/weighting/controls.md)
