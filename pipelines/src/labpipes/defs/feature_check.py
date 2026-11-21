"""Block downstream work when the physical feature table violates its contract."""
import dagster as dg

from ..contracts import features_contract
from ..partitions import trip_months
from ..resources import RedshiftData
from .features import trip_features


def feature_schema_errors(redshift: RedshiftData) -> list[str]:
    contract = features_contract()
    expected = contract['keys'] + contract['inputs'] + [contract['label']]
    rows = redshift.query(
        "select column_name, data_type, character_maximum_length from information_schema.columns "
        "where table_schema = 'staging' and table_name = 'trip_features' order by ordinal_position"
    )
    actual = {}
    for row in rows:
        typ = row['data_type'].lower()
        if typ in ('character', 'char', 'bpchar'):
            typ = f"char({row['character_maximum_length']})"
        typ = {'int4': 'integer', 'int2': 'smallint', 'float8': 'double precision'}.get(typ, typ)
        actual[row['column_name'].lower()] = typ
    wanted = {c['name']: c['type'].lower() for c in expected}
    return [f"{name}: expected {wanted.get(name, '<absent>')}, found {actual.get(name, '<absent>')}"
            for name in sorted(wanted.keys() | actual.keys()) if wanted.get(name) != actual.get(name)]


@dg.asset_check(asset=trip_features, partitions_def=trip_months, blocking=True)
def feature_contract(redshift: RedshiftData) -> dg.AssetCheckResult:
    errors = feature_schema_errors(redshift)
    return dg.AssetCheckResult(passed=not errors, metadata={'mismatches': dg.MetadataValue.json(errors)})
