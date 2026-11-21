import pytest

from labpipes.defs.feature_check import feature_schema_errors
from labpipes.defs.training import gate_result


class Schema:
    def __init__(self, rows):
        self.rows = rows

    def query(self, sql):
        return self.rows


def schema_rows():
    return [{'column_name': name, 'data_type': typ, 'character_maximum_length': length} for name, typ, length in [
        ('trip_month', 'character', 7), ('pickup_zone_id', 'integer', None),
        ('dropoff_zone_id', 'integer', None), ('pickup_hour', 'smallint', None),
        ('pickup_dow', 'smallint', None), ('duration_min', 'double precision', None),
    ]]


def test_feature_contract_detects_extra_and_wrong_type():
    rows = schema_rows()
    assert feature_schema_errors(Schema(rows)) == []
    rows[1]['data_type'] = 'bigint'
    rows.append({'column_name': 'fare_amount', 'data_type': 'double precision', 'character_maximum_length': None})
    errors = feature_schema_errors(Schema(rows))
    assert any('pickup_zone_id' in error for error in errors)
    assert any('fare_amount' in error for error in errors)


def test_gate_requires_budget_and_both_conditions(monkeypatch):
    report = {'reference': 'v1 baseline', 'metrics': {'mae_min': 9.5, 'champion_mae_min': 10., 'p95_latency_ms': 4.}}
    monkeypatch.delenv('LAB_MAX_P95_LATENCY_MS', raising=False)
    with pytest.raises(Exception, match='LAB_MAX_P95_LATENCY_MS'):
        gate_result(report)
    monkeypatch.setenv('LAB_MAX_P95_LATENCY_MS', '4')
    assert gate_result(report).passed
    report['metrics']['mae_min'] = 9.6
    assert not gate_result(report).passed
    report['metrics']['mae_min'] = 9.
    report['metrics']['p95_latency_ms'] = 4.1
    assert not gate_result(report).passed


def test_mlflow_registration_and_baseline_evaluation(tmp_path, monkeypatch):
    import pandas as pd

    from labpipes.defs import training
    from labpipes.resources import MlflowTracking
    from tripmodel.model import LightGBMModel

    class LocalFrames:
        def read_parquet(self, path):
            return pd.read_parquet(tmp_path / (path.split('/')[-2] + '.parquet'))

    class Zones:
        def query(self, sql):
            return [{'location_id': n, 'borough': 'Manhattan'} for n in range(1, 5)]

    rows = [{'pickup_zone_id': i % 4 + 1, 'dropoff_zone_id': (i // 4) % 4 + 1,
             'pickup_hour': i % 24, 'pickup_dow': i % 7, 'duration_min': float(i % 20 + 4)}
            for i in range(300)]
    for name in ('train', 'holdout'):
        pd.DataFrame(rows).to_parquet(tmp_path / f'{name}.parquet')
    pd.DataFrame([{'pickup_zone_id': i, 'dropoff_zone_id': j, 'est_distance_miles': float(i + j)}
                  for i in range(1, 5) for j in range(1, 5)]).to_parquet(tmp_path / 'distance.parquet')
    original_fit = LightGBMModel.fit

    def small_fit(self, train, distances):
        return original_fit(self, train, distances, num_rounds=2, params={'num_threads': 1})

    monkeypatch.setattr(LightGBMModel, 'fit', small_fit)
    tracking = MlflowTracking(tracking_uri=f'sqlite:///{tmp_path}/mlflow.db')
    model = training.trained_model.op.compute_fn.decorated_fn({'prefix': 'x'}, LocalFrames(), tracking)
    report = training.eval_report.op.compute_fn.decorated_fn(model, LocalFrames(), tracking, Zones())
    version = tracking.client().get_model_version('trip-duration', model['version'])
    assert report['reference'] == 'v1 baseline'
    assert {'training_window', 'holdout_month', 'features_version', 'git_sha',
            'mae_min', 'p95_latency_ms', 'champion_mae_min'} <= version.tags.keys()
