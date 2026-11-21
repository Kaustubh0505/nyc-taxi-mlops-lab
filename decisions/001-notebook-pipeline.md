# 001: Notebook and pipeline responsibilities

Stage: S2 · Date: 2026-11-21 · Author: student · Drafted by agent: yes

## Choice

Keep exploration, plots and comparison of ideas in the notebook. Put feature validation, dataset export, training, evaluation, registration and the promotion gate in the repeatable pipeline.

## Rejected alternative

Manually train or assign an alias from the notebook.

## Why

Repeatable execution preserves the holdout and contract checks and records model provenance. Notebook findings are pending the live notebook run; no notebook results are claimed here.

## What would show this was wrong

The pipeline cannot reproduce a notebook finding or manual work is repeatedly needed for a production run.
