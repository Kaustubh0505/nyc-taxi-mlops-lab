"""Repeatable time-split training, evaluation, and gated challenger registration."""
import math
import os
import re
from pathlib import Path

import dagster as dg
import mlflow
from mlflow.tracking import MlflowClient

from tripmodel.model import BaselineModel, LightGBMModel, evaluate, model_signature

from ..contracts import features_contract, mlflow_contract
from ..resources import ArtifactBucket, MlflowTracking, RedshiftData
from .feature_check import feature_schema_errors

TRAIN_MONTHS = ('2024-01', '2024-02', '2024-03')
HOLDOUT = '2024-04'


def _unload(redshift, artifacts, query, key):
    uri = artifacts.uri(key)
    if not re.fullmatch(r's3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/[a-zA-Z0-9_/-]+', uri):
        raise ValueError('invalid artifact bucket or unload path')
    redshift.execute(f"unload ('{query.replace(chr(39), chr(39) * 2)}') to '{uri}' iam_role default format as parquet")


@dg.asset(deps=['trip_features'], group_name='model', pool='redshift')
def training_set(context: dg.AssetExecutionContext, redshift: RedshiftData, artifacts: ArtifactBucket) -> dict:
    errors = feature_schema_errors(redshift)
    if errors:
        raise dg.Failure(description='feature contract failed: ' + '; '.join(errors))
    rows = redshift.query('select trip_month, count(*) as n from staging.trip_features '
                          "where trip_month in ('2024-01','2024-02','2024-03','2024-04') group by trip_month")
    present = {r['trip_month'].strip(): int(r['n']) for r in rows}
    if any(present.get(m, 0) < 1 for m in (*TRAIN_MONTHS, HOLDOUT)):
        raise dg.Failure(description=f'All four feature months must contain rows: {present}')
    prefix = f'training/{context.run_id}'
    cols = 'pickup_zone_id, dropoff_zone_id, pickup_hour, pickup_dow, duration_min'
    months = "'2024-01','2024-02','2024-03'"
    queries = {
        'train': f'select {cols} from staging.trip_features where trip_month in ({months})',
        'holdout': f"select {cols} from staging.trip_features where trip_month = '{HOLDOUT}'",
        'distance': 'select pickup_zone_id, dropoff_zone_id, median(trip_distance) as est_distance_miles '
                    f'from staging.stg_trips where trip_month in ({months}) and duration_min > 0 and duration_min <= 180 '
                    'and pickup_zone_id between 1 and 263 and dropoff_zone_id between 1 and 263 '
                    'and trip_distance > 0 group by pickup_zone_id, dropoff_zone_id',
    }
    for key, query in queries.items():
        _unload(redshift, artifacts, query, f'{prefix}/{key}/')
    return {'prefix': prefix}


@dg.asset(group_name='model')
def trained_model(training_set: dict, artifacts: ArtifactBucket, tracking: MlflowTracking) -> dict:
    tracking.client()
    prefix = training_set['prefix']
    train = artifacts.read_parquet(f'{prefix}/train/')
    distances = artifacts.read_parquet(f'{prefix}/distance/')
    model = LightGBMModel().fit(train, distances)
    contract = mlflow_contract()
    tags = {'training_window': '2024-01..2024-03', 'holdout_month': HOLDOUT,
            'features_version': str(features_contract()['version']), 'git_sha': os.environ.get('GIT_SHA', 'unknown')}
    with mlflow.start_run(tags=tags) as run:
        info = mlflow.pyfunc.log_model(name='model', python_model=model, signature=model_signature(),
                                       code_paths=[str(Path(__import__('tripmodel').__file__).parent)])
        version = mlflow.register_model(info.model_uri, contract['registered_model'])
    return {'prefix': prefix, 'run_id': run.info.run_id, 'version': str(version.version), 'tags': tags}


@dg.asset(deps=['taxi_zones'], group_name='model')
def eval_report(trained_model: dict, artifacts: ArtifactBucket, tracking: MlflowTracking, redshift: RedshiftData) -> dict:
    client = tracking.client()
    name = mlflow_contract()['registered_model']
    holdout = artifacts.read_parquet(f"{trained_model['prefix']}/holdout/")
    model = mlflow.pyfunc.load_model(f"models:/{name}/{trained_model['version']}")
    boroughs = {int(r['location_id']): r['borough'] for r in redshift.query('select location_id, borough from ingest.taxi_zones')}
    metrics = evaluate(model.predict, holdout, boroughs)
    champion_version = client.get_registered_model(name).aliases.get('champion')
    if champion_version is None:
        train = artifacts.read_parquet(f"{trained_model['prefix']}/train/")
        distances = artifacts.read_parquet(f"{trained_model['prefix']}/distance/")
        baseline = BaselineModel().fit(train, distances)
        reference = evaluate(lambda frame: baseline.predict(None, frame), holdout, boroughs)
        reference_id = 'v1 baseline'
    else:
        reference_model = mlflow.pyfunc.load_model(f'models:/{name}/{champion_version}')
        reference = evaluate(reference_model.predict, holdout, boroughs)
        reference_id = str(champion_version)
    metrics['champion_mae_min'] = reference['mae_min']
    metrics['reference_p95_latency_ms'] = reference['p95_latency_ms']
    for key, value in metrics.items():
        if math.isfinite(value):
            client.log_metric(trained_model['run_id'], key, value)
    tags = trained_model['tags'] | {k: str(metrics[k]) for k in ('mae_min', 'p95_latency_ms', 'champion_mae_min')}
    for key, value in tags.items():
        client.set_tag(trained_model['run_id'], key, value)
        client.set_model_version_tag(name, trained_model['version'], key, value)
    return trained_model | {'metrics': metrics, 'reference': reference_id}


def gate_result(report: dict) -> dg.AssetCheckResult:
    m = report['metrics']
    try:
        budget = float(os.environ['LAB_MAX_P95_LATENCY_MS'])
    except (KeyError, ValueError) as exc:
        raise dg.Failure(description='Set LAB_MAX_P95_LATENCY_MS from the measured reference p95 before promotion.') from exc
    passed = (math.isfinite(budget) and budget > 0 and math.isfinite(m['mae_min'])
              and math.isfinite(m['champion_mae_min']) and m['champion_mae_min'] > 0
              and math.isfinite(m['p95_latency_ms']) and m['mae_min'] <= .95 * m['champion_mae_min']
              and m['p95_latency_ms'] <= budget)
    return dg.AssetCheckResult(passed=passed, metadata={'metrics': dg.MetadataValue.json(m),
                                                       'max_p95_latency_ms': budget, 'reference': report['reference']})


@dg.asset_check(asset=eval_report, blocking=True)
def beats_champion(eval_report: dict) -> dg.AssetCheckResult:
    return gate_result(eval_report)


@dg.asset(group_name='model')
def challenger(eval_report: dict, tracking: MlflowTracking) -> dg.MaterializeResult:
    if not gate_result(eval_report).passed:
        raise dg.Failure(description='Promotion gate failed; challenger alias was not moved.')
    client: MlflowClient = tracking.client()
    name = mlflow_contract()['registered_model']
    # A concurrent champion promotion invalidates the comparison; rerun the evaluation.
    current = str(client.get_registered_model(name).aliases.get('champion') or 'v1 baseline')
    if current != eval_report['reference']:
        raise dg.Failure(description='Champion changed after evaluation; rerun training.')
    client.set_registered_model_alias(name, 'challenger', eval_report['version'])
    return dg.MaterializeResult(metadata={'version': eval_report['version']})


train_model = dg.define_asset_job('train_model', selection=dg.AssetSelection.assets(training_set, trained_model, eval_report, challenger))
