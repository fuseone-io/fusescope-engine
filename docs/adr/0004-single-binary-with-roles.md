# 0004 · Single binary with roles

- Status: accepted
- Date: 2026-10-24

## Context

Ingester, querier and compactor have different scaling and state profiles, but shipping three binaries makes development and small installs painful. Quickwit and Tempo ship one binary with roles.

## Decision

One binary, fs-server, with --role ingester|querier|compactor|all. Kubernetes deploys three workloads from the same image.

## Consequences

One build, one image, one dev loop. Role separation is enforced by configuration and IAM identity, not by binary boundaries.
