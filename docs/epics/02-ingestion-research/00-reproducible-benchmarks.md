# Reproducible Ingestion Benchmarks

Status: **Complete**
Last updated: 2026-09-07
Epic: 02 Ingestion Research
Phase: 00
Related docs: [Benchmark guide](../../../benchmarks/ingestion/README.md)

## Intent

Make the synthetic experiments available to developers as reproducible evidence
for a storage decision. Packaging the experiments does not approve a new
Assquack architecture or implement its materialization lifecycle.

## Scope

Preserve the changing-schema fixtures, final-table routes, actual MAD.Prefect
lifecycle comparison, native/adapted local reader control and stored-query probes.
Pin DuckDB 1.5.5 and the exact DuckDB 2 alpha, reuse the existing MAD.Prefect
submodule, and isolate research dependencies from Assquack core.

## Architecture Notes

Each route runs in a fresh subprocess. Shared fixtures define inconsistent API
responses; experiment modules use ordinary imports and explicit main entry points.
The runner compares complete output rows and classifies known limitations
separately from unexpected failures. Local outputs are ignored; maintained
instructions and a compact validation record are reviewable in Git.

## Implementation Checklist

- [x] Package portable experiments and schema fixtures.
- [x] Pin research dependencies and reference submodule identity.
- [x] Add validation for failure reporting and result correctness.
- [x] Validate both engines from a clean checkout before the Slack handoff.

## Validation

Run the commands in the benchmark guide. Completion requires both full engine
matrices, harness tests, lint, type checks, documentation links and Git whitespace
checks. Expected OOM/schema limitations must remain explicitly reported.

Completed on 7 September 2026: both matrices passed their expected outcomes
(56 quick runs and 56 full runs), with harness and core checks also verified.
The [validation record](../../../benchmarks/ingestion/VALIDATION.md) retains the
exact source revisions, output checksum, observed timings and limitations.
