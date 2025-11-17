# Column Requirement Matrix

This matrix shows which columns are required in which pipeline steps.
Generated automatically from Pydantic model field metadata.


- ✓ = required in step
- \+ = created in step

| Table | Field | Type | Constraints | extract_tours | final_check | link_trip |
| --- | --- | --- | --- | --- | --- | --- |
| **households** | `hh_id` | int | ≥ 1 | ✓ | ✓ | ✓ |
|  | `home_lat` | float | ≥ -90, ≤ 90 |  | ✓ |  |
|  | `home_lon` | float | ≥ -180, ≤ 180 |  | ✓ |  |
| **persons** | `person_id` | int | ≥ 1 | ✓ | ✓ | ✓ |
|  | `hh_id` | int | ≥ 1 | ✓ | ✓ | ✓ |
|  | `age` | int or None | ≥ 0 |  | ✓ |  |
|  | `work_lat` | float or None | ≥ -90, ≤ 90 |  | ✓ |  |
|  | `work_lon` | float or None | ≥ -180, ≤ 180 |  | ✓ |  |
|  | `school_lat` | float or None | ≥ -90, ≤ 90 |  | ✓ |  |
|  | `school_lon` | float or None | ≥ -180, ≤ 180 |  | ✓ |  |
|  | `person_type` | int | ≥ 1 |  | ✓ |  |
| **days** | `person_id` | int | ≥ 1 | ✓ | ✓ | ✓ |
|  | `day_id` | int | ≥ 1 | ✓ | ✓ | ✓ |
|  | `hh_id` | int | ≥ 1 | ✓ | ✓ | ✓ |
|  | `travel_dow` | int | ≥ 1, ≤ 7 |  | ✓ |  |
| **unlinked_trips** | `trip_id` | int | ≥ 1 | ✓ | ✓ | ✓ |
|  | `day_id` | int | ≥ 1 | ✓ | ✓ | ✓ |
|  | `person_id` | int | ≥ 1 | ✓ | ✓ | ✓ |
|  | `hh_id` | int | ≥ 1 | ✓ | ✓ | ✓ |
|  | `linked_trip_id` | int or None | ≥ 1 | ✓ | ✓ |  |
|  | `tour_id` | int or None | ≥ 1 | ✓ | ✓ |  |
|  | `depart_date` | str |  |  |  |  |
|  | `depart_hour` | int | ≥ 0, ≤ 23 |  | ✓ |  |
|  | `depart_minute` | int | ≥ 0, ≤ 59 |  | ✓ |  |
|  | `depart_seconds` | int | ≥ 0, ≤ 59 |  | ✓ |  |
|  | `arrive_date` | str |  |  |  |  |
|  | `arrive_hour` | int | ≥ 0, ≤ 23 |  | ✓ |  |
|  | `arrive_minute` | int | ≥ 0, ≤ 59 |  | ✓ |  |
|  | `arrive_seconds` | int | ≥ 0, ≤ 59 |  | ✓ |  |
|  | `o_purpose_category` | int |  |  |  |  |
|  | `d_purpose_category` | int |  |  |  |  |
|  | `mode_type` | int |  |  |  |  |
|  | `duration_minutes` | float | ≥ 0 |  | ✓ |  |
|  | `distance_miles` | float | ≥ 0 |  | ✓ |  |
|  | `depart_time` | datetime.datetime or None |  |  | ✓ | ✓ |
|  | `arrive_time` | datetime.datetime or None |  |  | ✓ | ✓ |
| **linked_trips** | `day_id` | int | ≥ 1 |  |  | + |
|  | `person_id` | int | ≥ 1 |  |  | + |
|  | `hh_id` | int | ≥ 1 |  |  | + |
|  | `linked_trip_id` | int or None | ≥ 1 |  |  |  |
|  | `tour_id` | int or None | ≥ 1 |  |  |  |
|  | `depart_date` | str |  |  |  |  |
|  | `depart_hour` | int | ≥ 0, ≤ 23 |  |  | + |
|  | `depart_minute` | int | ≥ 0, ≤ 59 |  |  | + |
|  | `depart_seconds` | int | ≥ 0, ≤ 59 |  |  | + |
|  | `arrive_date` | str |  |  |  | + |
|  | `arrive_hour` | int | ≥ 0, ≤ 23 |  |  | + |
|  | `arrive_minute` | int | ≥ 0, ≤ 59 |  |  | + |
|  | `arrive_seconds` | int | ≥ 0, ≤ 59 |  |  | + |
|  | `o_purpose_category` | int |  |  |  | + |
|  | `d_purpose_category` | int |  |  |  | + |
|  | `mode_type` | int |  |  |  | + |
|  | `duration_minutes` | float | ≥ 0 |  |  | + |
|  | `distance_miles` | float | ≥ 0 |  |  | + |
|  | `depart_time` | datetime.datetime or None |  |  |  |  |
|  | `arrive_time` | datetime.datetime or None |  |  |  |  |
|  | `is_primary_dest_trip` | bool or None |  |  |  |  |
| **tours** | `tour_id` | int | ≥ 1 | + |  |  |
|  | `person_id` | int | ≥ 1 | + |  |  |
|  | `day_id` | int | ≥ 1 | + |  |  |
|  | `tour_sequence_num` | int | ≥ 1 |  | ✓ |  |
|  | `tour_category` | str |  |  |  |  |
|  | `parent_tour_id` | int or None | ≥ 1 | + |  |  |
|  | `primary_purpose` | int | ≥ 1 | + |  |  |
|  | `primary_dest_purpose` | int | ≥ 1 | + |  |  |
|  | `purpose_priority` | int | ≥ 1 | + |  |  |
|  | `origin_depart_time` | datetime |  | + |  |  |
|  | `dest_arrive_time` | datetime |  | + |  |  |
|  | `dest_depart_time` | datetime |  | + |  |  |
|  | `origin_arrive_time` | datetime |  | + |  |  |
|  | `o_lat` | float | ≥ -90, ≤ 90 | + |  |  |
|  | `o_lon` | float | ≥ -180, ≤ 180 | + |  |  |
|  | `d_lat` | float | ≥ -90, ≤ 90 | + |  |  |
|  | `d_lon` | float | ≥ -180, ≤ 180 | + |  |  |
|  | `o_location_type` | str |  | + |  |  |
|  | `d_location_type` | str |  | + |  |  |
|  | `tour_mode` | int | ≥ 1 | + |  |  |
|  | `outbound_mode` | int | ≥ 1 | + |  |  |
|  | `inbound_mode` | int | ≥ 1 | + |  |  |
|  | `num_outbound_stops` | int | ≥ 0 | + |  |  |
|  | `num_inbound_stops` | int | ≥ 0 | + |  |  |
|  | `is_primary_tour` | bool |  | + |  |  |
|  | `tour_starts_at_origin` | bool |  | + |  |  |
|  | `tour_ends_at_origin` | bool |  | + |  |  |
