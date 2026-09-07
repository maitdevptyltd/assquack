"""Compare native storage/query paths without a JSON round trip for VARIANT queries."""

import argparse
import json
import statistics
import tempfile
import time
from pathlib import Path

import duckdb
import pyarrow as pa
from schema_experiments import (
    OPTIONS,
    attempt,
    fixtures,
    incremental_probe,
    run_case,
    sampling_probe,
)


def versions(con):
    return {
        "python_package": duckdb.__version__,
        "engine": con.execute("SELECT version()").fetchall()[0][0],
        "build": con.execute("SELECT * FROM pragma_version()").fetchall(),
    }


def semantics(root):
    con = duckdb.connect()
    result = {"versions": versions(con), "cases": {}}
    result["json_group_structure_implementation"] = con.execute(
        "SELECT macro_definition FROM duckdb_functions() WHERE function_name='json_group_structure'"
    ).fetchall()[0][0]
    result["variant_functions"] = con.execute(
        "SELECT function_name FROM duckdb_functions() WHERE function_name LIKE 'variant%' ORDER BY function_name"
    ).fetchall()
    con.close()
    for name, batches in fixtures().items():
        result["cases"][name] = run_case(root, name, batches)
    result["incremental"] = attempt(incremental_probe)
    result["sampling"] = sampling_probe(root)
    result["populated_cases"] = sum(
        bool(case["input_rows"]) for case in result["cases"].values()
    )
    return result


def benchmark(root, args):
    # All routes begin with identical JSON strings in memory, representing API responses.
    pages = []
    for first in range(0, args.rows, 10000):
        records = []
        for i in range(first, min(first + 10000, args.rows)):
            row = {
                "id": i,
                "amount": i / 4,
                "details": {"name": f"item-{i}", "active": i % 2 == 0},
                "items": [{"code": i % 20}],
                "memo": (f"record-{i}-" * 25)[:256],
            }
            if i == args.rows - 1:
                row["late_field"] = "found"
            records.append(json.dumps(row, separators=(",", ":")))
        pages.append("\n".join(records))

    database = root / "assets.duckdb"
    config = {"threads": 1, "memory_limit": "512MB"}
    if duckdb.__version__.startswith("1.5."):
        config["storage_compatibility_version"] = "v1.5.0"
    con = duckdb.connect(str(database), config=config)
    result = {
        "mode": args.mode,
        "rows": args.rows,
        "input_bytes": sum(len(page.encode()) for page in pages),
        "versions": versions(con),
        "config": config,
    }
    result["effective_settings"] = con.execute(
        "SELECT name,value FROM duckdb_settings() WHERE name LIKE '%shredding%' OR name='storage_compatibility_version'"
    ).fetchall()
    start = time.perf_counter()
    if args.mode in ("files", "typed"):
        for index, page in enumerate(pages):
            (root / f"page_{index:03d}.json").write_bytes(page.encode())
    else:
        con.execute(f"CREATE TABLE raw(payload {args.mode.upper()})")
        con.execute("BEGIN")
        for page in pages:
            con.register("batch", pa.table({"body": page.splitlines()}))
            cast = "body::JSON::VARIANT" if args.mode == "variant" else "body::JSON"
            con.execute(f"INSERT INTO raw SELECT {cast} FROM batch")
            con.unregister("batch")
        con.execute("COMMIT")
    result["capture_seconds"] = time.perf_counter() - start
    paths = repr([path.as_posix() for path in sorted(root.glob("page_*.json"))])
    reader_query = f"SELECT * FROM read_json({paths}, {OPTIONS})"
    start = time.perf_counter()
    if args.mode == "typed":
        con.execute("CREATE TABLE shaped AS " + reader_query)
    result["typed_materialization_seconds"] = time.perf_counter() - start
    start = time.perf_counter()
    con.execute("CHECKPOINT")
    con.close()
    result["checkpoint_close_seconds"] = time.perf_counter() - start
    result["database_bytes"] = database.stat().st_size
    result["fragment_bytes"] = sum(
        path.stat().st_size for path in root.glob("page_*.json")
    )

    # Reopen to exercise persisted storage, then report first and five warm query timings.
    con = duckdb.connect(str(database), config=config)
    if args.mode in ("files", "typed"):
        source = "shaped" if args.mode == "typed" else "(" + reader_query + ")"
        queries = {
            "nested_aggregate": f"SELECT count(*), sum(amount), sum(id) FROM {source} WHERE details.active",
            "selective_lookup": f"SELECT id, amount, details.name, memo FROM {source} WHERE id={args.rows - 2}",
            "late_field": f"SELECT count(*) FROM {source} WHERE late_field IS NOT NULL",
        }
    else:
        queries = {
            "nested_aggregate": "SELECT count(*), sum(payload.amount::DOUBLE), sum(payload.id::BIGINT) FROM raw WHERE payload.details.active::BOOLEAN",
            "selective_lookup": f"SELECT payload.id::BIGINT, payload.amount::DOUBLE, payload.details.name::VARCHAR, payload.memo::VARCHAR FROM raw WHERE payload.id::BIGINT={args.rows - 2}",
            "late_field": "SELECT count(*) FROM raw WHERE payload.late_field IS NOT NULL",
        }
        if args.mode == "json":
            # JSON string casts include quotes; ->> provides the equivalent text result.
            queries["selective_lookup"] = (
                f"SELECT payload.id::BIGINT, payload.amount::DOUBLE, payload->>'$.details.name', payload->>'$.memo' FROM raw WHERE payload.id::BIGINT={args.rows - 2}"
            )
        if args.mode == "variant" and duckdb.sql("SELECT version()").fetchall()[0][
            0
        ].startswith("v2."):
            queries["late_field_cast"] = (
                "SELECT count(*) FROM raw WHERE payload.late_field::VARCHAR IS NOT NULL"
            )
    result["queries"] = {}
    for name, query in queries.items():
        timings = []
        for _repeat in range(6):
            start = time.perf_counter()
            rows = con.execute(query).fetchall()
            timings.append(time.perf_counter() - start)
        result["queries"][name] = {
            "sql": query,
            "rows": rows,
            "first_seconds": timings[0],
            "warm_median_seconds": statistics.median(timings[1:]),
            "all_seconds": timings,
        }
        if name == "nested_aggregate":
            expected_sum = sum(range(0, args.rows, 2))
            assert rows == [
                (len(range(0, args.rows, 2)), expected_sum / 4, expected_sum)
            ], rows
        elif name in ("late_field", "late_field_cast"):
            assert rows == [(1,)], rows
        else:
            target = args.rows - 2
            assert rows == [
                (target, target / 4, f"item-{target}", (f"record-{target}-" * 25)[:256])
            ], rows

    if args.mode in ("json", "variant"):
        keys = (
            "variant_keys(payload)"
            if args.mode == "variant"
            and duckdb.sql("SELECT version()").fetchall()[0][0].startswith("v2.")
            else "json_keys(payload::JSON)"
        )
        start = time.perf_counter()
        discovered = con.execute(
            f"SELECT DISTINCT unnest({keys}) AS key FROM raw ORDER BY key"
        ).fetchall()
        result["automatic_key_discovery"] = {
            "seconds": time.perf_counter() - start,
            "keys": discovered,
            "expression": keys,
        }
        assert ("late_field",) in discovered
        result["storage_info"] = con.execute("PRAGMA storage_info('raw')").fetchall()
        result["aggregate_plan"] = con.execute(
            "EXPLAIN " + queries["nested_aggregate"]
        ).fetchall()
        result["lookup_plan"] = con.execute(
            "EXPLAIN " + queries["selective_lookup"]
        ).fetchall()
        if args.mode == "variant" and duckdb.sql("SELECT version()").fetchall()[0][
            0
        ].startswith("v2."):
            result["rare_field_plans"] = {
                name: con.execute("EXPLAIN " + queries[name]).fetchall()
                for name in ["late_field", "late_field_cast"]
            }
            start = time.perf_counter()
            try:
                structure = con.execute(
                    "SELECT json_group_structure(payload::JSON) FROM (SELECT payload FROM raw LIMIT 100000)"
                ).fetchall()
                result["whole_table_inference_100k"] = {
                    "ok": True,
                    "structure": structure,
                }
            except Exception as error:
                result["whole_table_inference_100k"] = {
                    "ok": False,
                    "error_type": type(error).__name__,
                    "error": str(error),
                }
            result["whole_table_inference_100k"]["seconds"] = (
                time.perf_counter() - start
            )
    con.close()
    result["validation"] = "passed"
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["semantics", "files", "typed", "json", "variant"],
        required=True,
    )
    parser.add_argument("--rows", type=int, default=300000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    (Path(__file__).parent / "scratch").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=Path(__file__).parent / "scratch") as temp:
        result = (
            semantics(Path(temp))
            if args.mode == "semantics"
            else benchmark(Path(temp), args)
        )
    args.output.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    print(
        json.dumps(
            {
                key: value
                for key, value in result.items()
                if key
                in [
                    "versions",
                    "mode",
                    "capture_seconds",
                    "typed_materialization_seconds",
                    "validation",
                    "populated_cases",
                ]
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
