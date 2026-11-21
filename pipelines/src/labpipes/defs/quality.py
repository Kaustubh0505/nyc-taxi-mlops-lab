"""Collect S1 quality evidence without imposing thresholds or gate severity yet.

The student chooses which findings block and the thresholds after these metrics are measured in
Redshift. This profile is informational: it does not pass/fail or stop downstream assets.
"""

import dagster as dg

from ..partitions import check_month, trip_months
from ..resources import RedshiftData
from .staging import stg_trips


@dg.asset(
    partitions_def=trip_months,
    deps=[stg_trips],
    group_name="quality",
    kinds={"redshift"},
    pool="redshift",
    description="Observed row-quality metrics for one month; no thresholds are enforced yet.",
)
def stg_trips_quality_profile(
    context: dg.AssetExecutionContext, redshift: RedshiftData
) -> dg.MaterializeResult:
    month = check_month(context.partition_key)
    row = redshift.query(
        "select count(*) as rows, "
        "sum(case when pickup_at is null then 1 else 0 end) as null_pickup_at, "
        "sum(case when dropoff_at is null then 1 else 0 end) as null_dropoff_at, "
        "sum(case when pickup_zone_id is null then 1 else 0 end) as null_pickup_zone, "
        "sum(case when dropoff_zone_id is null then 1 else 0 end) as null_dropoff_zone, "
        "sum(case when passenger_count is null then 1 else 0 end) as null_passengers, "
        "sum(case when trip_distance is null then 1 else 0 end) as null_trip_distance, "
        "sum(case when duration_min <= 0 then 1 else 0 end) as nonpositive_duration, "
        "sum(case when duration_min > 180 then 1 else 0 end) as over_3h, "
        "sum(case when pickup_zone_id is not null and pickup_zone_id not between 1 and 263 then 1 else 0 end) "
        "as out_of_range_pickup_zone, "
        "sum(case when dropoff_zone_id is not null and dropoff_zone_id not between 1 and 263 then 1 else 0 end) "
        "as out_of_range_dropoff_zone, "
        "sum(case when pickup_zone_id is not null and z.location_id is null then 1 else 0 end) "
        "as unknown_zone, "
        "min(passenger_count) as min_passengers, max(passenger_count) as max_passengers, "
        "min(trip_distance) as min_trip_distance, max(trip_distance) as max_trip_distance, "
        "min(duration_min) as min_duration_min, max(duration_min) as max_duration_min "
        "from staging.stg_trips s left join (select distinct location_id from ingest.taxi_zones) z "
        "on z.location_id = s.pickup_zone_id "
        f"where trip_month = '{month}'"
    )[0]
    duplicate_rows = redshift.scalar(
        "select coalesce(sum(dupe_count), 0) from ("
        "select count(*) - 1 as dupe_count from staging.stg_trips "
        f"where trip_month = '{month}' "
        "group by source_file, vendor_id, pickup_at, dropoff_at, duration_min, pickup_hour, pickup_dow, "
        "pickup_zone_id, dropoff_zone_id, passenger_count, trip_distance, ratecode_id, payment_type, fare_amount, total_amount "
        "having count(*) > 1) duplicates"
    )

    metadata = {
        key: dg.MetadataValue.int(int(value or 0))
        for key, value in row.items()
        if key not in {"min_passengers", "max_passengers", "min_trip_distance", "max_trip_distance", "min_duration_min", "max_duration_min"}
    }
    metadata["duplicate_rows"] = dg.MetadataValue.int(int(duplicate_rows or 0))
    for key in (
        "min_passengers",
        "max_passengers",
        "min_trip_distance",
        "max_trip_distance",
        "min_duration_min",
        "max_duration_min",
    ):
        value = row.get(key)
        if value is not None:
            metadata[key] = dg.MetadataValue.float(float(value))

    return dg.MaterializeResult(metadata=metadata)
