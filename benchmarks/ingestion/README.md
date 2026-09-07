# Ingestion experiments: API responses to a finished asset

These are reproducible research tests, separate from the Assquack library. They
compare the actual MAD.Prefect data-asset lifecycle with alternative ingestion
paths on **DuckDB 1.5.5 and a pinned DuckDB 2 alpha**. They do not choose or
implement Assquack's production storage architecture.

See the [clean-checkout validation record](VALIDATION.md) for the tested revisions,
observed results and remaining limitations.

## Run from a clean checkout

Install Python 3.11, Git and [uv](https://docs.astral.sh/uv/getting-started/installation/).
From the repository root:

```bash
git submodule update --init .submodules/MAD.Prefect
python benchmarks/ingestion/setup_envs.py
python benchmarks/ingestion/run_suite.py
python benchmarks/ingestion/run_suite.py --full
```

The setup creates two independent environments under `.venvs/` in this directory.
No Poetry install, local source edits, credentials, Prefect server, Azure account
or Assquack installation is required. The submodule is reference/test material;
Prefect remains outside Assquack's core dependencies.

`run_suite.py` runs both versions sequentially. `--engine 155` or `--engine 2`
selects one. The default uses 1,000 records for quick checks but still runs every
schema fixture. `--full` uses 100,000 records for asset builds, 300,000 for queries
over stored data, and the same-fragment native-versus-adapter control. Allow several
minutes. Individual subprocesses have a ten-minute timeout. Do not run versions
in parallel when comparing timings.

Results and logs go to `results/`; temporary datasets go to `scratch/` and are
removed after each process. Both locations and `.venvs/` are narrowly gitignored.
`--output <directory>` changes the results destination. Each run writes
`summary.json` and exits nonzero on an unexpected failure or compatibility change.
Do not use Python's `-O` option: the probe scripts deliberately use assertions.

## What each experiment means

All API inputs are synthetic. The shared [fixtures](schema_fixtures.py) include
late fields, fields appearing on later pages, numeric widening, strings mixed
with numbers, objects mixed with scalars, mixed arrays, missing/null values,
empty objects later gaining fields, unusual/case-colliding keys, dates, oversized
integers and wide nested records. There are 14 populated cases plus empty input.
The source responses define the schema; the tests do not hand-code API columns.

| Route | Work performed |
| --- | --- |
| `json_pages` | Write local JSON pages; native `read_json` over all pages; create asset table. |
| `json_stream` | Append pages to one local NDJSON file; infer across the complete file; create asset table. |
| `json_gzip` | Write gzip JSON pages; native reader decompresses and infers; create asset table. |
| `parquet_pages` | Infer each page separately and write Parquet; union the Parquet pages into the asset table. |
| `json_reader` | Insert raw JSON into a database; expose bounded virtual pages to the native JSON reader; create asset table. |
| `variant_reader` | Insert VARIANT; convert stored batches back to JSON virtual pages; native reader creates the typed asset. |
| `variant_native` | Insert/checkpoint VARIANT; discover schema through bounded pages; generate native field casts into a physical asset table. |
| `variant_aggregate` | Insert VARIANT; aggregate all payloads with `json_group_structure`; transform to typed asset columns. This reproduces the full-size inference OOM. |
| `mad_prefect` | Execute the real asset decorator, fragment collector, `mad://` reader, Parquet persistence and manifest; load its output into an asset table. |

The scripts answer different questions. Keep their timing endpoints separate:

- [final_asset_benchmark.py](final_asset_benchmark.py): all eight prototype routes
  finish at a persisted table, checkpointed and reopened. Failed experiments are
  captured in JSON so their limitations can be compared; use the suite runner to
  evaluate expected versus unexpected outcomes.
- [actual_prefect_comparison.py](actual_prefect_comparison.py): the real framework
  and five selected prototypes finish with **both a persisted table and a Parquet file**.
  Every row is compared in both outputs. Candidate Parquet-export time is added
  to table-build time; verification between those operations is excluded.
- [actual_prefect_schema_checks.py](actual_prefect_schema_checks.py): run all
  changing-schema fixtures through the real decorator and its Parquet output.
- [schema_experiments.py](schema_experiments.py): additional fidelity, first-page
  Arrow typing, default-sampling and MERGE/view-drift assertions. These are probes
  of specific limitations, not a finished incremental asset implementation.
- [duckdb_version_comparison.py](duckdb_version_comparison.py): query already-stored
  JSON/VARIANT, JSON files or a typed table. Includes first execution and five warm
  repeats, query plans, storage details and discovered keys. File/query-only modes
  are explicitly not complete asset-build comparisons.

For a single experiment, use its environment's Python. For example on Windows:

```powershell
benchmarks/ingestion/.venvs/155/Scripts/python.exe benchmarks/ingestion/actual_prefect_comparison.py --mode mad_prefect --reader-control --output benchmarks/ingestion/results/manual-mad.json
benchmarks/ingestion/.venvs/2/Scripts/python.exe benchmarks/ingestion/final_asset_benchmark.py --mode variant_native --semantics --output benchmarks/ingestion/results/manual-variant-schema.json
```

On Linux/macOS the executable is `.venvs/<engine>/bin/python` instead. Run the
default suite before changing individual probes; the suite also removes inherited
Prefect and storage settings from child processes.

## Pins and limits

The existing MAD.Prefect submodule is pinned to
`59a811ff4003eb68ba77f8fdeb9cbdabf09b2f19`. Setup rejects a different revision.
The DuckDB packages are `1.5.5` and `1.6.0.dev379`; the latter contains engine
`v2.0.0-alpha39998`. Setup and the runner check the engine identity rather than
assuming a Python package called 1.6 is DuckDB 1.6.

The `.in` files pin the direct experimental dependencies, and the `.txt` files
lock their transitive dependencies for Python 3.11. Recompile only when deliberately
changing the comparison, for example:

```bash
uv pip compile benchmarks/ingestion/requirements-155.in --python-version 3.11 --universal -o benchmarks/ingestion/requirements-155.txt
```

The original experiments used Prefect 3.8.4. MAD.Prefect's pinned source declares
DuckDB `<1.5` and Prefect `<3.5`; we intentionally import its source with the pinned
research dependencies instead of installing its package and pretending those
versions satisfy its declared constraints. These are compatibility experiments,
not certification of a supported MAD.Prefect deployment. DuckDB 2 is an alpha.

The complete-row checksum is compared across successful asset-build routes and
both engines. The runner also checks row counts, aggregates, the last-row field,
physical table existence and Parquet readback. Known schema differences and
failures are recorded in [expected-schema-outcomes.json](expected-schema-outcomes.json).
Only those specific outcomes are accepted; other exceptions fail the suite.
Changing an expectation requires reviewing the data and the reason, not merely
refreshing a snapshot to make a failed run pass.

At full size, the whole-dataset aggregate is expected to raise
`OutOfMemoryException` during inference with a 512 MB engine budget. Direct VARIANT
storage/copy can succeed; this test is not evidence that VARIANT inherently OOMs.
The quick run uses fewer rows and expects that same route to complete.

The generated fixture is repetitive and relatively small. Native local JSON
won the original asset-build comparison, but no Azure-versus-local benchmark or
production worker certification is implied. `mad://` versus native paths compares
access to the same local fragments. File size is retained storage, not cumulative
I/O, and engine memory limits do not cap total Python process memory. Peak RSS is
sampled only in the prototype kernels; it is not an equal-endpoint framework RAM
comparison. The wrapper's extra serialization makes its absolute timings different
from the earlier table-only experiment.

## Validate changes to this harness

Use the environment's Python (Windows executable shown):

```powershell
benchmarks/ingestion/.venvs/155/Scripts/python.exe -m pytest -c benchmarks/ingestion/pyproject.toml benchmarks/ingestion/test_harness.py
benchmarks/ingestion/.venvs/155/Scripts/python.exe -m ruff check benchmarks/ingestion
benchmarks/ingestion/.venvs/155/Scripts/python.exe -m ruff format --check benchmarks/ingestion
benchmarks/ingestion/.venvs/155/Scripts/python.exe -m pyright --project benchmarks/ingestion
python .agents/skills/assquack-documentation/scripts/check_doc_links.py
git diff --check
```

The lightweight harness tests ensure changed rows, missing fixtures and unexpected
error types fail validation, and production connection settings are not inherited.
The SQL in the experiments is intentionally kept close to the original probes;
only this directory exempts long SQL strings from the line-length lint rule.

## Related docs

- [Documentation index](../../docs/README.md)
- [Schema inference](../../docs/schema-inference.md): the intended library contract.
- [MAD.Prefect reference](../../docs/mad-prefect-reference.md): migration context.
- [Research handoff](../../docs/epics/02-ingestion-research/00-reproducible-benchmarks.md)
- [DuckDB 2 alpha announcement](https://duckdb.org/2026/09/02/try-duckdb-20-alpha): client/engine version naming.
