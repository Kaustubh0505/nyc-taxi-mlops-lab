# 002: Promotion metric and margin

Stage: S2 · Date: 2026-11-21 · Author: student · Drafted by agent: yes

## Choice

Require holdout MAE in minutes to improve by at least 5% against the current champion, or the v1 baseline when there is no champion. Require single-row p95 prediction latency to meet a separately configured budget chosen from the measured reference p95.

## Rejected alternative

Promote any candidate with a small MAE improvement regardless of prediction latency.

## Why

The margin makes minor differences less likely to drive promotion, while a latency limit protects responsiveness. The budget is intentionally unresolved until the baseline is measured; no numerical measurement is available yet.

## What would show this was wrong

The gate rejects meaningful improvements repeatedly, or promoted models are too slow or perform worse on important slices.
