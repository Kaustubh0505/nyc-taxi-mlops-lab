"""Block the trip-file load unless the Parquet footer matches the frozen schema contract."""

import dagster as dg

from ..contracts import trips_schema
from ..partitions import trip_months
from ..resources import DataBucket
from .trip_files import trip_files


@dg.asset_check(
    asset=trip_files,
    partitions_def=trip_months,
    blocking=True,
    description="The landed Parquet footer matches contracts/trips.schema.yaml before COPY.",
)
def schema_contract(context: dg.AssetCheckExecutionContext, data_bucket: DataBucket) -> dg.AssetCheckResult:
    month = context.partition_key
    expected = trips_schema()
    actual = data_bucket.footer(month).columns

    mismatches = []
    for index in range(max(len(expected), len(actual))):
        want = expected[index] if index < len(expected) else None
        found = actual[index] if index < len(actual) else None
        if want is None or found is None:
            mismatches.append(
                {
                    "index": index,
                    "expected": None if want is None else {"name": want.name, "type": want.type},
                    "actual": None if found is None else {"name": found.name, "type": found.type},
                }
            )
        elif want.name.casefold() != found.name.casefold() or want.type != found.type:
            mismatches.append(
                {
                    "index": index,
                    "expected": {"name": want.name, "type": want.type},
                    "actual": {"name": found.name, "type": found.type},
                }
            )

    passed = not mismatches
    return dg.AssetCheckResult(
        passed=passed,
        severity=dg.AssetCheckSeverity.ERROR,
        description=(
            f"{month}: Parquet footer matches the {len(expected)}-column contract"
            if passed
            else f"{month}: Parquet footer differs from the contract at {len(mismatches)} position(s)"
        ),
        metadata={
            "month": month,
            "expected_column_count": dg.MetadataValue.int(len(expected)),
            "actual_column_count": dg.MetadataValue.int(len(actual)),
            "mismatches": dg.MetadataValue.json(mismatches),
        },
    )
