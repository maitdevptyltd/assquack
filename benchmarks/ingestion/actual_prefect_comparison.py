"""Compare unmodified MAD.Prefect with ingestion prototypes at the same outputs.

Run one route per process. Runtime imports and Prefect storage are process-local.
The companion final_asset_benchmark.py supplies the existing candidate kernels.
"""

import argparse
import asyncio
import hashlib
import json
import os
import sys
import tempfile
import time
from pathlib import Path

import duckdb
from final_asset_benchmark import make_pages, materialize
from schema_fixtures import OPTIONS, quote


def verify(con, query, size):
    assert con.execute(f"SELECT count(*) FROM ({query})").fetchall()[0][0] == size
    digest = hashlib.sha256()
    cursor = con.execute(
        f"SELECT to_json(t) FROM (SELECT id,amount,details,items,memo,late_field FROM ({query}) ORDER BY id) t"
    )
    while rows := cursor.fetchmany(1000):
        for row in rows:
            canonical = json.dumps(
                json.loads(row[0]), sort_keys=True, separators=(",", ":")
            )
            digest.update(canonical.encode() + b"\n")
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--repo",
        type=Path,
        default=Path(__file__).resolve().parents[2] / ".submodules/MAD.Prefect",
    )
    parser.add_argument("--mode", required=True)
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--reader-control", action="store_true")
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    (Path(__file__).parent / "scratch").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="actual-prefect-", dir=Path(__file__).parent / "scratch"
    ) as temp:
        root = Path(temp).resolve()
        database = root / "asset.duckdb"
        parquet = root / "final.parquet"
        config = {"threads": 1, "memory_limit": "512MB"}
        if duckdb.__version__.startswith("1.5."):
            config["storage_compatibility_version"] = "v1.5.0"
        result = {
            "mode": args.mode,
            "package": duckdb.__version__,
            "engine": duckdb.sql("SELECT version()").fetchall()[0][0],
            "rows": args.rows,
        }
        try:
            if args.mode == "mad_prefect":
                os.environ["FILESYSTEM_URL"] = root.as_uri()
                os.environ.pop("FILESYSTEM_BLOCK_NAME", None)
                os.environ["ASSET_METADATA_LOCATION"] = "_asset_metadata"
                os.environ["PREFECT_SERVER_ANALYTICS_ENABLED"] = "false"
                sys.path.insert(0, str(args.repo.resolve()))
                from mad_prefect.data_assets import asset
                from mad_prefect.data_assets.asset_metadata import load_asset_manifest

                duckdb.sql("SET threads=1")
                duckdb.sql("SET memory_limit='512MB'")
                duckdb.sql("SET preserve_insertion_order=false")

                # Input JSON decoding is timed for every route below, including candidates.
                @asset(
                    path="final.parquet",
                    artifacts_dir="raw",
                    name="synthetic_ingestion",
                )
                async def source_asset():
                    for page in make_pages(args.rows, 1000):
                        yield [json.loads(line) for line in page.splitlines()]

                async def run():
                    started = time.perf_counter()
                    artifact = await source_asset()
                    result["framework_parquet_ready_seconds"] = (
                        time.perf_counter() - started
                    )
                    assert artifact.persisted and parquet.exists()
                    manifest = await load_asset_manifest(
                        source_asset.name, source_asset.id
                    )
                    assert manifest is not None
                    result["manifest"] = manifest.model_dump(mode="json")
                    return started

                if args.profile:
                    import cProfile

                    profiler = cProfile.Profile()
                    profiler.enable()
                start = asyncio.run(run())
                if args.profile:
                    profiler.disable()
                    profiler.dump_stats(str(args.profile))
                duckdb.close()
                con = duckdb.connect(str(database), config=config)
                con.execute(
                    f"CREATE TABLE asset AS SELECT * FROM read_parquet({quote(parquet.as_posix())})"
                )
                con.execute("CHECKPOINT")
                con.close()
                result["both_outputs_ready_seconds"] = time.perf_counter() - start
                result["files"] = [
                    str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()
                ]
                assert result["manifest"]["last_status"] == "success", result[
                    "manifest"
                ]
            else:

                def source_pages():
                    for page in make_pages(args.rows, 1000):
                        decoded = [json.loads(line) for line in page.splitlines()]
                        yield "\n".join(
                            json.dumps(row, separators=(",", ":")) for row in decoded
                        )

                result.update(materialize(root, source_pages(), args.mode))
                assert result["status"] == "passed", result.get("error")
                start_export = time.perf_counter()
                con = duckdb.connect(str(database), config=config)
                con.execute(
                    f"COPY asset TO {quote(parquet.as_posix())} (FORMAT PARQUET)"
                )
                con.close()
                result["parquet_export_seconds"] = time.perf_counter() - start_export
                result["both_outputs_ready_seconds"] = (
                    result["ready_seconds"] + result["parquet_export_seconds"]
                )

            con = duckdb.connect(str(database), config=config)
            assert con.execute(
                "SELECT table_type FROM information_schema.tables WHERE table_name='asset'"
            ).fetchall() == [("BASE TABLE",)]
            result["table_checksum"] = verify(con, "SELECT * FROM asset", args.rows)
            result["parquet_checksum"] = verify(
                con,
                f"SELECT * FROM read_parquet({quote(parquet.as_posix())})",
                args.rows,
            )
            assert result["table_checksum"] == result["parquet_checksum"]
            con.close()
            result["status"] = "passed"
            result["parquet_bytes"] = parquet.stat().st_size
            result["all_retained_bytes"] = sum(
                p.stat().st_size for p in root.rglob("*") if p.is_file()
            )
            if args.reader_control and args.mode == "mad_prefect":
                from mad_prefect.duckdb import register_mad_protocol

                result["same_fragment_reader_control"] = []
                for route in ["native", "mad", "mad", "native"]:
                    con = duckdb.connect(config={"threads": 1, "memory_limit": "512MB"})
                    asyncio.run(register_mad_protocol(con))
                    paths = sorted((root / "raw").glob("*.json"))
                    paths = [
                        p.as_posix() if route == "native" else "mad://raw/" + p.name
                        for p in paths
                    ]
                    started = time.perf_counter()
                    con.execute(
                        f"CREATE TABLE asset AS SELECT * FROM read_json({paths!r}, {OPTIONS})"
                    )
                    elapsed = time.perf_counter() - started
                    checksum = verify(con, "SELECT * FROM asset", args.rows)
                    assert checksum == result["table_checksum"]
                    result["same_fragment_reader_control"].append(
                        {"route": route, "seconds": elapsed, "checksum": checksum}
                    )
                    con.close()
        except Exception as error:
            import traceback

            result.update(
                status="failed", error=str(error), traceback=traceback.format_exc()
            )
        args.output.write_text(
            json.dumps(result, indent=2, default=str), encoding="utf-8"
        )
        print(
            json.dumps(
                {
                    k: result.get(k)
                    for k in [
                        "mode",
                        "engine",
                        "status",
                        "framework_parquet_ready_seconds",
                        "both_outputs_ready_seconds",
                        "error",
                    ]
                }
            ),
            flush=True,
        )
    if result["status"] != "passed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
