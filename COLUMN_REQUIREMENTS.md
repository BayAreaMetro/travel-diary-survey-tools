# Column Requirement Matrix

This matrix shows which columns are created or required in which pipeline steps.
Generated automatically from Pydantic model field metadata.


- ✓ = required in step
- \+ = created in step

| Table | Field | extract_tours | link_trip |
| --- | --- | --- | --- |
| **households** | `hh_id` | ✓ | ✓ |
|  | `home_lat` |  |  |
|  | `home_lon` |  |  |
| **persons** | `person_id` | ✓ | ✓ |
|  | `hh_id` | ✓ | ✓ |
|  | `age` |  |  |
|  | `work_lat` |  |  |
|  | `work_lon` |  |  |
|  | `school_lat` |  |  |
|  | `school_lon` |  |  |
|  | `person_type` |  |  |
| **days** | `person_id` | ✓ | ✓ |
|  | `day_id` | ✓ | ✓ |
|  | `hh_id` | ✓ | ✓ |
|  | `travel_dow` |  |  |
| **unlinked_trips** | `trip_id` | ✓ | ✓ |
|  | `day_id` | ✓ | ✓ |
|  | `person_id` | ✓ | ✓ |
|  | `hh_id` | ✓ | ✓ |
|  | `linked_trip_id` | ✓ |  |
|  | `tour_id` | ✓ |  |
|  | `depart_date` |  |  |
|  | `depart_hour` |  |  |
|  | `depart_minute` |  |  |
|  | `depart_seconds` |  |  |
|  | `arrive_date` |  |  |
|  | `arrive_hour` |  |  |
|  | `arrive_minute` |  |  |
|  | `arrive_seconds` |  |  |
|  | `o_purpose_category` |  |  |
|  | `d_purpose_category` |  |  |
|  | `mode_type` |  |  |
|  | `duration_minutes` |  |  |
|  | `distance_miles` |  |  |
|  | `depart_time` |  | ✓ |
|  | `arrive_time` |  | ✓ |
| **linked_trips** | `day_id` |  | + |
|  | `person_id` |  | + |
|  | `hh_id` |  | + |
|  | `linked_trip_id` |  |  |
|  | `tour_id` |  |  |
|  | `depart_date` |  |  |
|  | `depart_hour` |  | + |
|  | `depart_minute` |  | + |
|  | `depart_seconds` |  | + |
|  | `arrive_date` |  | + |
|  | `arrive_hour` |  | + |
|  | `arrive_minute` |  | + |
|  | `arrive_seconds` |  | + |
|  | `o_purpose_category` |  | + |
|  | `d_purpose_category` |  | + |
|  | `mode_type` |  | + |
|  | `duration_minutes` |  | + |
|  | `distance_miles` |  | + |
|  | `depart_time` |  |  |
|  | `arrive_time` |  |  |
|  | `is_primary_dest_trip` |  |  |
| **tours** | `tour_id` | + |  |
|  | `person_id` | + |  |
|  | `day_id` | + |  |
|  | `tour_sequence_num` |  |  |
|  | `tour_category` |  |  |
|  | `parent_tour_id` | + |  |
|  | `primary_purpose` | + |  |
|  | `primary_dest_purpose` | + |  |
|  | `purpose_priority` | + |  |
|  | `origin_depart_time` | + |  |
|  | `dest_arrive_time` | + |  |
|  | `dest_depart_time` | + |  |
|  | `origin_arrive_time` | + |  |
|  | `o_lat` | + |  |
|  | `o_lon` | + |  |
|  | `d_lat` | + |  |
|  | `d_lon` | + |  |
|  | `o_location_type` | + |  |
|  | `d_location_type` | + |  |
|  | `tour_mode` | + |  |
|  | `outbound_mode` | + |  |
|  | `inbound_mode` | + |  |
|  | `num_outbound_stops` | + |  |
|  | `num_inbound_stops` | + |  |
|  | `is_primary_tour` | + |  |
|  | `tour_starts_at_origin` | + |  |
|  | `tour_ends_at_origin` | + |  |
