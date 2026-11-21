# pipelines/

Dagster 1.13.25 (pinned in `pyproject.toml`). One code location, `labpipes.definitions`, which loads every module in `src/labpipes/defs/`.

```
trips/ in the bucket --sensor--> trip_files[month] --(schema_contract, blocking)--> raw_trips[month]
                                                                                       |
                                 stg_trips[month] <------------------------------------+
                                      |--(quality checks)--> zone_hour_stats <-- taxi_zones
```

| Piece | Where | Given or built |
| --- | --- | --- |
| Resources: `RedshiftData` (Data API, `--db-user`, no passwords), `DataBucket` (lists `trips/`, reads Parquet footers) | `src/labpipes/resources.py` | given |
| Partitions `trip_months` (one per landed file), contract readers, SQL loader | `partitions.py`, `contracts.py`, `sql.py` | given |
| `taxi_zones`, `trip_files`, `stg_trips`, `zone_hour_stats` | `defs/zones.py`, `trip_files.py`, `staging.py`, `marts.py` | given |
| `schema_contract` check, `raw_trips`, data-quality checks, sensor and job | `defs/schema_check.py`, `raw_trips.py`, `quality.py`, `sensor.py` | built in S1 |
| One asset, one blocking check, one sensor to copy from | `reference/patterns.py` | given |
| Container entrypoint (RDS secret at start-up, database creation, MLflow server), status for the grader | `runtime.py`, `status.py` | given |
| Models: v1 baseline, v2 LightGBM, the zone-pair distance lookup, evaluation | `src/tripmodel/` | given (S2) |
| `trip_features`; resources `ArtifactBucket`, `MlflowTracking` | `defs/features.py`, `resources.py` | given (S2) |
| `feature_contract` check; `training_set`, `trained_model`, `eval_report`, the `beats_champion` gate, `challenger` | `defs/feature_check.py`, `training.py` | built in S2 |

S6 adds the `wiki_*` assets. Jobs: `ingest_month` (the sensor's, one month to the mart) and `train_model` (window to challenger); start either from the UI or `scripts/dagster-run.sh`.

**Tests:** `make test-pipelines` (fake Redshift and S3, no AWS). **Image:** `docker build -f pipelines/Dockerfile .` from the repo root (it reads `contracts/`). **Runtime:** `deploy/compose.platform.yaml` on the platform host; `dagster.yaml` keeps run storage in the `dagster` database on RDS, runs two at a time, and lets one Redshift-writing step run at a time (pool `redshift`).

**Local UI without AWS resources:** `cd pipelines && uv run dagster dev -m labpipes.definitions` shows the asset graph. Materializing needs Redshift and the bucket, so set `REDSHIFT_CLUSTER_ID=lab REDSHIFT_DATABASE=lab REDSHIFT_DB_USER=labadmin LAB_DATA_BUCKET=<bucket>` with Learner Lab credentials exported.

### S2 training and the latency budget

The `trip_ingest_job` materializes `trip_features` for each month and runs its blocking schema check. Materialize January through April 2024 before running `train_model`. The job exports the January–March training rows, April holdout rows and a distance lookup derived only from the training months. It compares the candidate to the current champion, or the v1 baseline if there is no champion, on the same holdout. An improvement of at least 5% in MAE and a p95 below the configured budget are both required.

The latency budget has no default: run the evaluation to obtain `reference_p95_latency_ms` in MLflow and choose a budget based on that measurement. Set the repository variable `LAB_MAX_P95_LATENCY_MS` to the chosen budget and redeploy the platform workflow, then rerun the job. For a manual Compose deployment, set the same environment variable before recreating the services. Until then the gate fails closed; a failed run can still have logged an evaluation and registered a model version. The pipeline alone assigns `challenger`, never `champion`. Setting the budget or deploying requires the appropriate lab authorization.
