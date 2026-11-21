"""Add changed trip files as monthly partitions and request the ingest-to-mart asset job."""

import json

import dagster as dg

from ..partitions import trip_months
from ..resources import DataBucket
from .features import trip_features
from .marts import zone_hour_stats
from .quality import stg_trips_quality_profile
from .raw_trips import raw_trips
from .staging import stg_trips
from .trip_files import trip_files
from .zones import taxi_zones


trip_ingest_job = dg.define_asset_job(
    "trip_ingest_job",
    selection=dg.AssetSelection.assets(
        taxi_zones,
        trip_files,
        raw_trips,
        stg_trips,
        stg_trips_quality_profile,
        zone_hour_stats,
        trip_features,
    ),
)


@dg.sensor(
    job=trip_ingest_job,
    minimum_interval_seconds=60,
    default_status=dg.DefaultSensorStatus.RUNNING,
    description="Watch trips/ for new or changed monthly Parquet files.",
)
def trip_files_sensor(context: dg.SensorEvaluationContext, data_bucket: DataBucket) -> dg.SensorResult | dg.SkipReason:
    """Request one partition run per new file or changed ETag; repeated polls are no-ops."""
    previous = json.loads(context.cursor or "{}")
    if not isinstance(previous, dict):
        previous = {}

    files = data_bucket.trip_files()
    changed = [f for f in files if previous.get(f.key) != {"etag": f.etag, "size": f.size}]
    if not changed:
        return dg.SkipReason("no new or changed trip files")

    updated = dict(previous)
    for file in changed:
        updated[file.key] = {"etag": file.etag, "size": file.size}

    months = sorted({file.month for file in changed})
    return dg.SensorResult(
        run_requests=[
            dg.RunRequest(
                partition_key=file.month,
                run_key=f"{file.key}:{file.etag}:{file.size}",
            )
            for file in changed
        ],
        dynamic_partitions_requests=[trip_months.build_add_request(months)],
        cursor=json.dumps(updated, sort_keys=True),
    )
