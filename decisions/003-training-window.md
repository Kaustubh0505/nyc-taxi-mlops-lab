# 003: Training and holdout months

Stage: S2 · Date: 2026-11-21 · Author: student · Drafted by agent: yes

## Choice

Train on January–March 2024 and evaluate on April 2024. Build the zone-pair distance lookup from training months only.

## Rejected alternative

Use a random split across January–April or a shorter training window.

## Why

A later holdout mimics future predictions, while three months provide more history. The choice reflects the student's stated preference; live exploration results remain unverified.

## What would show this was wrong

April or subsequent months show drift, or a shorter or longer window consistently improves future-month performance.
