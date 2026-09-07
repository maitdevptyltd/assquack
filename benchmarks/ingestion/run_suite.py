"""Run the synthetic research matrix and reject unexpected failures or data changes."""

import argparse
import json
import os
import platform
import subprocess
import time
from pathlib import Path

from setup_envs import ENGINES, MAD_COMMIT, ROOT, python_path

MODES = [
    "json_pages",
    "json_stream",
    "json_gzip",
    "parquet_pages",
    "variant_aggregate",
    "variant_reader",
    "variant_native",
    "json_reader",
]
FINAL_MODES = [
    "mad_prefect",
    "json_pages",
    "json_gzip",
    "json_reader",
    "variant_reader",
    "variant_native",
]


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def validate_schema(cases, baseline, expected):
    require(set(cases) == set(expected), "Missing or additional schema fixtures")
    for name, outcome in expected.items():
        case = cases[name]
        require(
            case["status"] == outcome["status"], f"{name}: unexpected status: {case}"
        )
        if case["status"] == "passed":
            same_schema = dict(case["asset"]["schema"]) == dict(
                baseline[name]["asset"]["schema"]
            )
            same_rows = case["asset"]["rows"] == baseline[name]["asset"]["rows"]
            require(
                same_schema == outcome["same_schema"],
                f"{name}: schema compatibility changed",
            )
            require(
                same_rows == outcome["same_rows"], f"{name}: row compatibility changed"
            )
        elif case["status"] == "failed":
            require(
                case["error_type"] == outcome["error_type"],
                f"{name}: different error: {case}",
            )


def child_environment():
    # These experiments must not inherit a production Prefect connection or store.
    return {
        key: value
        for key, value in os.environ.items()
        if key
        not in {
            "PYTHONPATH",
            "PYTHONHOME",
            "PYTHONOPTIMIZE",
            "FILESYSTEM_URL",
            "FILESYSTEM_BLOCK_NAME",
            "ASSET_METADATA_LOCATION",
        }
        and not key.startswith("PREFECT_")
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=["155", "2", "both"], default="both")
    parser.add_argument(
        "--full",
        action="store_true",
        help="100k asset builds, 300k stored-query tests, and same-fragment adapter controls",
    )
    parser.add_argument("--output", type=Path, default=ROOT / "results")
    args = parser.parse_args()
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    revision = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT.parents[1], text=True
    ).strip()
    submodule = ROOT.parents[1] / ".submodules/MAD.Prefect"
    actual_mad = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=submodule, text=True
    ).strip()
    require(
        actual_mad == MAD_COMMIT,
        "MAD.Prefect revision changed; run setup or review the pin",
    )
    dirty_mad = subprocess.check_output(
        ["git", "status", "--porcelain"], cwd=submodule, text=True
    ).strip()
    require(
        not dirty_mad,
        "MAD.Prefect submodule has local changes; benchmark requires unmodified source",
    )
    expectations = json.loads((ROOT / "expected-schema-outcomes.json").read_text())
    summary = {
        "status": "running",
        "benchmark_revision": revision,
        "full": args.full,
        "platform": platform.platform(),
        "mad_prefect_commit": MAD_COMMIT,
        "results": {},
        "started": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    rows = 100000 if args.full else 1000
    checksums = set()
    engines = ENGINES if args.engine == "both" else [args.engine]
    try:
        for engine in engines:
            executable = python_path(engine)
            require(executable.exists(), "Run setup_envs.py first")
            version = subprocess.check_output(
                [
                    str(executable),
                    "-c",
                    "import duckdb; print(duckdb.sql('SELECT version()').fetchall()[0][0])",
                ],
                text=True,
                env=child_environment(),
            ).strip()
            require(version == ENGINES[engine], f"Unexpected engine {version}")
            suite = summary["results"][engine] = {"engine": version, "runs": {}}
            folder = args.output / engine
            folder.mkdir(exist_ok=True)

            def run(
                label,
                script,
                *options,
                folder=folder,
                executable=executable,
                engine=engine,
                suite=suite,
            ):
                output = folder / f"{label}.json"
                command = [
                    str(executable),
                    str(ROOT / script),
                    *map(str, options),
                    "--output",
                    str(output),
                ]
                print(f"{engine}: {label}", flush=True)
                with (folder / f"{label}.log").open("w", encoding="utf-8") as log:
                    completed = subprocess.run(
                        command,
                        cwd=ROOT,
                        env=child_environment(),
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        timeout=600,
                    )
                require(
                    completed.returncode == 0,
                    f"{label}: process failed; see {folder / (label + '.log')}",
                )
                result = json.loads(output.read_text())
                suite["runs"][label] = {
                    "file": str(output.relative_to(args.output)),
                    "status": result.get(
                        "status", result.get("validation", "recorded")
                    ),
                }
                return result

            baseline = run(
                "schema-json_pages",
                "final_asset_benchmark.py",
                "--mode",
                "json_pages",
                "--semantics",
            )["cases"]
            validate_schema(baseline, baseline, expectations[engine]["json_pages"])
            for mode in MODES[1:]:
                cases = run(
                    "schema-" + mode,
                    "final_asset_benchmark.py",
                    "--mode",
                    mode,
                    "--semantics",
                )["cases"]
                validate_schema(cases, baseline, expectations[engine][mode])
            actual = run("schema-mad-prefect", "actual_prefect_schema_checks.py")[
                "cases"
            ]
            validate_schema(actual, baseline, expectations[engine]["json_pages"])
            require(
                not actual["empty_extraction"]["file_exists"],
                "Empty extraction invented a file",
            )

            # These additional probes assert fidelity, first-page inference and MERGE/view drift.
            run("fidelity", "schema_experiments.py")
            for mode in MODES:
                result = run(
                    "asset-" + mode,
                    "final_asset_benchmark.py",
                    "--mode",
                    mode,
                    "--rows",
                    rows,
                )
                if mode == "variant_aggregate" and args.full:
                    require(
                        result["status"] == "failed"
                        and result["error_type"] == "OutOfMemoryException"
                        and result["failed_phase"] == "infer_and_build",
                        f"Expected whole-dataset inference OOM changed: {result}",
                    )
                    suite["runs"]["asset-" + mode]["status"] = "expected_oom"
                else:
                    require(result["status"] == "passed", f"{mode}: {result}")
                    require(
                        result["asset_type"] == "BASE TABLE"
                        and result["readback_rows"] == rows,
                        f"{mode}: missing final asset",
                    )
                    checksums.add(result["complete_row_checksum"])
            for mode in FINAL_MODES:
                options = ["--mode", mode, "--rows", rows]
                if args.full and mode == "mad_prefect":
                    options.append("--reader-control")
                result = run(
                    "parquet-and-table-" + mode,
                    "actual_prefect_comparison.py",
                    *options,
                )
                require(result["status"] == "passed", f"{mode}: final outputs failed")
                require(
                    result["table_checksum"] == result["parquet_checksum"],
                    f"{mode}: Parquet differs",
                )
                checksums.add(result["table_checksum"])
            for mode in ["files", "typed", "json", "variant"]:
                result = run(
                    "stored-query-" + mode,
                    "duckdb_version_comparison.py",
                    "--mode",
                    mode,
                    "--rows",
                    300000 if args.full else 1000,
                )
                require(
                    result["validation"] == "passed", f"{mode}: query results differ"
                )
        require(len(checksums) == 1, f"Final output checksums differ: {checksums}")
        summary.update(
            status="passed", rows=rows, complete_row_checksum=next(iter(checksums))
        )
    except Exception as error:
        summary.update(status="failed", error=str(error))
        raise
    finally:
        (args.output / "summary.json").write_text(
            json.dumps(summary, indent=2) + "\n", encoding="utf-8"
        )
    print(
        f"PASS: {sum(len(s['runs']) for s in summary['results'].values())} runs; {args.output / 'summary.json'}",
        flush=True,
    )


if __name__ == "__main__":
    main()
