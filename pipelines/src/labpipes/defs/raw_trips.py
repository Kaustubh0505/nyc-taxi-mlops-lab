"""COPY one validated month into Redshift, replacing that month's rows on every rerun."""

import dagster as dg

from ..contracts import redshift_columns, trips_schema
from ..partitions import check_month, source_file, trip_months
from ..resources import DataBucket, RedshiftData
from .trip_files import trip_files


@dg.asset(
    partitions_def=trip_months,
    deps=[trip_files],
    group_name="ingest",
    kinds={"redshift"},
    pool="redshift",
    description="Idempotently load one Parquet month from S3 into ingest.raw_trips.",
)
def raw_trips(
    context: dg.AssetExecutionContext,
    redshift: RedshiftData,
    data_bucket: DataBucket,
) -> dg.MaterializeResult:
    month = check_month(context.partition_key)
    filename = source_file(month)
    columns = redshift_columns(trips_schema())
    names = ", ".join(column.name.lower() for column in trips_schema())
    uri = data_bucket.trip_uri(month)

    redshift.execute(
        "create schema if not exists ingest",
        f"create table if not exists ingest.raw_trips ({columns}, "
        "trip_month char(7) not null, source_file varchar(64) not null) diststyle auto sortkey auto",
        f"create temp table raw_trips_load ({columns})",
        f"copy raw_trips_load from '{uri}' iam_role default format as parquet",
        f"delete from ingest.raw_trips where trip_month = '{month}'",
        f"insert into ingest.raw_trips ({names}, trip_month, source_file) "
        f"select {names}, '{month}', '{filename}' from raw_trips_load",
    )
    rows = int(redshift.scalar(f"select count(*) from ingest.raw_trips where trip_month = '{month}'") or 0)
    return dg.MaterializeResult(
        metadata={
            "month": month,
            "source_file": filename,
            "source_uri": uri,
            "rows": dg.MetadataValue.int(rows),
        }
    )
