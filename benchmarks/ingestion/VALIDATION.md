# Clean-checkout validation: 7 September 2026

The published GitHub branch was cloned into a separate directory. Setup
initialized the pinned MAD.Prefect submodule and installed both locked
environments. No earlier experiment files or virtual environments were copied.

- Full matrix revision: `f49455722c4d7d5db6c676243560d40022769308`.
- Quick matrix revision: `cc6759fa37324162757a1897ff4a78e0193ca74d`.
- The revision between these runs adds only benchmark Git line-ending attributes.
  A further fresh GitHub clone confirmed formatting without local normalization.
- MAD.Prefect: `59a811ff4003eb68ba77f8fdeb9cbdabf09b2f19`; unmodified submodule.
- Platform: `Windows-11-10.0.26200-SP0`, CPython 3.11.9. Linux/macOS were not exercised.
- Full run started: `2026-09-07T01:03:59Z`.
- Engine identities: `v1.5.5` and `v2.0.0-alpha39998`.

## Results

Both commands completed successfully: **56 quick runs and 56 full runs**.
The quick matrix uses 1,000 records. Full asset builds use 100,000 records
in 100 pages; stored-query probes use 300,000 records. Engine settings are
one thread and a 512 MB memory budget. Fixture generation, input decoding and
output endpoints are explained in the [guide](README.md).

```bash
python benchmarks/ingestion/setup_envs.py
python benchmarks/ingestion/run_suite.py --output benchmarks/ingestion/results/quick
python benchmarks/ingestion/run_suite.py --full --output benchmarks/ingestion/results/full
```

Success means the observations matched the documented expectations; it does
not mean every prototype is safe for every API schema. In particular:

- Actual MAD.Prefect matched native JSON on all 14 populated schema fixtures
  on both engines. Empty extraction produced no fabricated Parquet output.
- The full-size whole-dataset VARIANT/JSON aggregation reproduced its expected
  inference OOM on both engines. It completed at quick size. Raw staging
  succeeded before the full-size inference failure.
- Known native VARIANT projection, per-page Parquet, oversized-integer,
  first-page Arrow and default-sampling limitations remain exercised. See
  [schema expectations](expected-schema-outcomes.json) and the fidelity probes.
- All successful full-size asset routes agreed on every output row, across
  both engines, with Parquet readback checked where that endpoint applies.

Complete-row SHA-256 for the full dataset:

`6e96589fdeb8bb39c252d438d10cc9dad64ec590b234503e12fefd5a4583c0ca`

## Complete table plus Parquet timings

Individual local measurements, in seconds. These are observations, not
pass/fail performance thresholds or production guarantees. Every row below
finishes with both a physical persisted asset table and final Parquet.

| Route | DuckDB 1.5.5 | DuckDB 2 alpha |
| --- | ---: | ---: |
| `mad_prefect` | 11.80 | 9.92 |
| `json_pages` | 1.53 | 1.46 |
| `json_gzip` | 1.59 | 1.50 |
| `json_reader` | 2.03 | 3.51 |
| `variant_reader` | 2.82 | 4.34 |
| `variant_native` | 7.21 | 4.09 |

The same-fragment control used the actual framework-written local JSON with
the same inference options and matching output checksums:

| Engine | Native paths, two reads | `mad://`, two reads |
| --- | ---: | ---: |
| `v1.5.5` | 0.23 s, 0.24 s | 6.95 s, 6.55 s |
| `v2.0.0-alpha39998` | 0.22 s, 0.24 s | 5.62 s, 5.61 s |

This supports evaluating native local JSON ingestion first. Both sides used
local storage; this does not measure Azure-versus-local gains. A stateful
asset lifecycle, incremental updates, atomic publication, recovery and actual
worker storage still need implementation and representative testing.

## Harness and repository checks

- Harness tests: 4 passed; existing core tests: 3 passed.
- Ruff lint: whole repository passed; benchmark formatting: all 9 Python files passed.
- Pyright: benchmark and core configurations both passed with zero errors.
- Documentation link checker and Git whitespace checks passed.
- The clean validation checkout remained clean after the tests.

The pre-existing core files `src/assquack/__init__.py`, `src/assquack/_api.py`
and `tests/test_imports.py` fail the LF formatting check after this Windows
checkout, including on unchanged main. The benchmark folder now pins LF
through its own `.gitattributes`; unrelated core files were not reformatted.

The full run's JSON and logs are reproducible under the chosen results directory
and intentionally ignored by Git. This record retains the tested revisions,
output identity, observed timings and limitations without machine-specific paths.
