"""Exercise changing API responses through the unmodified asset decorator."""

import argparse
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

import duckdb
from schema_fixtures import describe, fixtures, quote


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--repo",
        type=Path,
        default=Path(__file__).resolve().parents[2] / ".submodules/MAD.Prefect",
    )
    args = parser.parse_args()
    sys.path.insert(0, str(args.repo.resolve()))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    (Path(__file__).parent / "scratch").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(
        prefix="prefect-schema-", dir=Path(__file__).parent / "scratch"
    ) as temp:
        root = Path(temp).resolve()
        os.environ["FILESYSTEM_URL"] = root.as_uri()
        os.environ.pop("FILESYSTEM_BLOCK_NAME", None)
        os.environ["ASSET_METADATA_LOCATION"] = "_asset_metadata"
        from mad_prefect.data_assets import asset

        duckdb.sql("SET memory_limit='512MB'")
        duckdb.sql("SET threads=1")
        output = {
            "engine": duckdb.sql("SELECT version()").fetchall()[0][0],
            "cases": {},
        }

        async def run():
            for name, batches in fixtures().items():

                @asset(
                    path=f"{name}/final.parquet", artifacts_dir=f"{name}/raw", name=name
                )
                async def source(batches=batches):
                    for batch in batches:
                        yield batch

                try:
                    result = await source()
                    path = root / name / "final.parquet"
                    if not any(batches):
                        output["cases"][name] = {
                            "status": "no_data",
                            "file_exists": path.exists(),
                        }
                        continue
                    assert result.persisted and path.exists()
                    database = root / name / "asset.duckdb"
                    con = duckdb.connect(
                        str(database), config={"threads": 1, "memory_limit": "512MB"}
                    )
                    con.execute(
                        f"CREATE TABLE asset AS SELECT * FROM read_parquet({quote(path.as_posix())})"
                    )
                    con.execute("CHECKPOINT")
                    con.close()
                    con = duckdb.connect(str(database))
                    output["cases"][name] = {
                        "status": "passed",
                        "asset": describe(con, "SELECT * FROM asset"),
                    }
                    con.close()
                except Exception as error:
                    output["cases"][name] = {"status": "failed", "error": str(error)}

        asyncio.run(run())
        duckdb.close()
        args.output.write_text(
            json.dumps(output, indent=2, default=str), encoding="utf-8"
        )
        print(
            json.dumps({name: case["status"] for name, case in output["cases"].items()})
        )


if __name__ == "__main__":
    main()
