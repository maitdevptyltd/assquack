"""All routes finish at a persisted, automatically shaped asset table."""

import argparse
import gzip
import hashlib
import io
import json
import tempfile
import threading
import time
from pathlib import Path

import duckdb
import fsspec
import psutil
import pyarrow as pa
from schema_fixtures import OPTIONS, describe, fixtures, identifier, quote


class StagedPages(fsspec.AbstractFileSystem):
    """Expose bounded batches to the native reader without creating JSON files."""

    protocol = "stagedpages"
    cachable = False

    def __init__(self, con):
        super().__init__()
        self.con = con
        self.bytes_returned = 0
        sizes = con.execute("""SELECT batch_id,
            sum(octet_length(encode(payload::JSON::VARCHAR))) + count(*) - 1
            FROM stage GROUP BY batch_id ORDER BY batch_id""").fetchall()
        self.entries = {
            f"page_{batch:06d}.json": {
                "name": f"page_{batch:06d}.json",
                "size": size,
                "type": "file",
                "batch": batch,
            }
            for batch, size in sizes
        }

    def info(self, path, **kwargs):
        name = self._strip_protocol(path)
        assert isinstance(name, str)
        return self.entries[name]

    def ls(self, path, detail=True, **kwargs):
        return list(self.entries.values()) if detail else list(self.entries)

    def _open(self, path, mode="rb", **kwargs):
        if mode != "rb":
            raise ValueError("Read only")
        entry = self.info(path)
        rows = self.con.execute(
            "SELECT payload::JSON::VARCHAR FROM stage WHERE batch_id=? ORDER BY sequence",
            [entry["batch"]],
        ).fetchall()
        body = "\n".join(row[0] for row in rows).encode()
        assert len(body) == entry["size"]
        self.bytes_returned += len(body)
        return io.BytesIO(body)


def make_pages(size, page_size):
    for first in range(0, size, page_size):
        rows = []
        for i in range(first, min(first + page_size, size)):
            row = {
                "id": i,
                "amount": i / 4,
                "details": {"name": f"item-{i}", "active": i % 2 == 0},
                "items": [{"code": i % 20}],
                "memo": (f"record-{i}-" * 25)[:256],
            }
            if i == size - 1:
                row["late_field"] = "found"
            rows.append(json.dumps(row, separators=(",", ":")))
        yield "\n".join(rows)


def materialize(root, pages, mode, semantic=False):
    database = root / "asset.duckdb"
    config = {"threads": 1, "memory_limit": "512MB"}
    if duckdb.__version__.startswith("1.5."):
        config["storage_compatibility_version"] = "v1.5.0"
    con = duckdb.connect(str(database), config=config)
    con.execute("SET preserve_insertion_order=false")
    result = {
        "mode": mode,
        "python_package": duckdb.__version__,
        "engine": con.execute("SELECT version()").fetchall()[0][0],
        "config": config,
    }
    process = psutil.Process()
    peak = [process.memory_info().rss]
    result["rss_before_capture"] = peak[0]
    stop = threading.Event()

    def monitor():
        while not stop.wait(0.01):
            peak[0] = max(peak[0], process.memory_info().rss)

    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    reader = None
    stream = None
    memory = None
    start = time.perf_counter()
    phase = "capture"
    try:
        if mode in (
            "variant_aggregate",
            "variant_reader",
            "variant_native",
            "json_reader",
        ):
            dtype = "JSON" if mode == "json_reader" else "VARIANT"
            con.execute(
                f"CREATE TABLE stage(batch_id INTEGER, sequence BIGINT, payload {dtype})"
            )
            con.execute("BEGIN")
        elif mode == "json_stream":
            stream = (root / "all.json").open("wb")
        elif mode == "parquet_pages":
            memory = fsspec.filesystem("memory")
            con.register_filesystem(memory)
        encoded_bytes = 0
        count = 0
        batch_count = 0
        for batch_id, page in enumerate(pages):
            if not page:
                continue
            body = page.encode()
            encoded_bytes += len(body)
            page_rows = page.splitlines()
            if mode == "json_pages":
                (root / f"page_{batch_id:06d}.json").write_bytes(body)
            elif mode == "json_gzip":
                (root / f"page_{batch_id:06d}.json.gz").write_bytes(
                    gzip.compress(body, compresslevel=1)
                )
            elif mode == "json_stream":
                assert stream is not None
                stream.write(body + b"\n")
            elif mode == "parquet_pages":
                assert memory is not None
                memory.pipe("memory://asset-page.json", body)
                destination = root / f"page_{batch_id:06d}.parquet"
                con.execute(
                    f"COPY (SELECT * FROM read_json('memory://asset-page.json', {OPTIONS})) TO {quote(destination.as_posix())} (FORMAT PARQUET)"
                )
                memory.rm("memory://asset-page.json")
            else:
                batch = pa.table(
                    {
                        "batch_id": [batch_id] * len(page_rows),
                        "sequence": list(range(count, count + len(page_rows))),
                        "body": page_rows,
                    }
                )
                con.register("input_batch", batch)
                cast = "body::JSON" if mode == "json_reader" else "body::JSON::VARIANT"
                con.execute(
                    f"INSERT INTO stage SELECT batch_id, sequence, {cast} FROM input_batch"
                )
                con.unregister("input_batch")
            count += len(page_rows)
            batch_count += 1
        if stream:
            stream.close()
            stream = None
        if mode in (
            "variant_aggregate",
            "variant_reader",
            "variant_native",
            "json_reader",
        ):
            con.execute("COMMIT")
            if mode == "variant_native":
                con.execute("CHECKPOINT")
        result.update(
            {
                "capture_seconds": time.perf_counter() - start,
                "input_bytes": encoded_bytes,
                "input_rows": count,
                "page_count": batch_count,
            }
        )
        if not count:
            result.update({"status": "no_data", "asset_created": False})
            return result

        phase = "infer_and_build"
        build_start = time.perf_counter()
        if mode in ("json_pages", "json_stream", "json_gzip"):
            pattern = "/*.json.gz" if mode == "json_gzip" else "/*.json"
            query = f"SELECT * FROM read_json({quote(root.as_posix() + pattern)}, {OPTIONS})"
        elif mode == "parquet_pages":
            query = f"SELECT * FROM read_parquet({quote(root.as_posix() + '/*.parquet')}, union_by_name=true)"
        elif mode == "variant_aggregate":
            structure = con.execute(
                "SELECT json_group_structure(payload::JSON) FROM stage"
            ).fetchall()[0][0]
            query = f"SELECT data.* FROM (SELECT from_json(payload::JSON, {quote(structure)}) AS data FROM stage)"
        else:
            reader = duckdb.connect(str(database), config=config)
            adapter = StagedPages(reader)
            con.register_filesystem(adapter)
            paths = ["stagedpages://" + name for name in adapter.entries]
            query = f"SELECT * FROM read_json({paths!r}, {OPTIONS})"
            if mode == "variant_native":
                schema = [
                    (row[0], row[1])
                    for row in con.execute("DESCRIBE " + query).fetchall()
                ]
                keys_function = (
                    "variant_keys(payload)"
                    if result["engine"].startswith("v2.")
                    else "json_keys(payload::JSON)"
                )
                keys = [
                    row[0]
                    for row in con.execute(
                        f"SELECT DISTINCT unnest({keys_function}) FROM stage"
                    ).fetchall()
                ]
                if len({key.casefold() for key in keys}) == len(keys):
                    fields = [
                        f"CAST(variant_extract(payload, {quote(name)}) AS {dtype}) AS {identifier(name)}"
                        for name, dtype in schema
                    ]
                    query = "SELECT " + ", ".join(fields) + " FROM stage"
                    result["native_projection"] = True
                else:
                    # Keep the reader's name disambiguation when source keys differ only by case.
                    result["native_projection"] = False
                    result["projection_fallback"] = (
                        "case-sensitive source-key collision"
                    )
        con.execute("CREATE TABLE asset AS " + query)
        result["infer_and_build_seconds"] = time.perf_counter() - build_start
        if reader:
            result["adapter_bytes_returned"] = adapter.bytes_returned
            reader.close()
            reader = None

        phase = "checkpoint_and_close"
        checkpoint_start = time.perf_counter()
        con.execute("CHECKPOINT")
        con.close()
        con = None
        result["checkpoint_close_seconds"] = time.perf_counter() - checkpoint_start
        result["ready_seconds"] = time.perf_counter() - start
        result["database_bytes"] = database.stat().st_size
        result["fragment_bytes"] = sum(
            path.stat().st_size
            for path in root.iterdir()
            if path.suffix in (".json", ".parquet", ".gz")
        )

        phase = "readback"
        con = duckdb.connect(str(database), config=config)
        table_type = con.execute(
            "SELECT table_type FROM information_schema.tables WHERE table_name='asset'"
        ).fetchall()
        assert table_type == [("BASE TABLE",)], table_type
        actual_count = con.execute("SELECT count(*) FROM asset").fetchall()[0][0]
        assert actual_count == count, (actual_count, count)
        result["asset_created"] = True
        result["asset_type"] = "BASE TABLE"
        result["readback_rows"] = actual_count
        if semantic:
            result["asset"] = describe(con, "SELECT * FROM asset")
        else:
            expected_sum = sum(range(0, count, 2))
            rows = con.execute(
                "SELECT count(*),sum(amount),sum(id) FROM asset WHERE details.active"
            ).fetchall()
            assert rows == [
                (len(range(0, count, 2)), expected_sum / 4, expected_sum)
            ], rows
            assert con.execute(
                "SELECT count(*) FROM asset WHERE late_field IS NOT NULL"
            ).fetchall() == [(1,)]
            result["asset_schema"] = [
                (row[0], row[1]) for row in con.execute("DESCRIBE asset").fetchall()
            ]
            result["expected_aggregate"] = rows
            # Compare complete rows independently of physical row order and object-key order.
            digest = hashlib.sha256()
            cursor = con.execute(
                "SELECT to_json(t) FROM (SELECT id,amount,details,items,memo,late_field FROM asset ORDER BY id) t"
            )
            while rows := cursor.fetchmany(1000):
                for row in rows:
                    canonical = json.dumps(
                        json.loads(row[0]), sort_keys=True, separators=(",", ":")
                    )
                    digest.update(canonical.encode() + b"\n")
            result["complete_row_checksum"] = digest.hexdigest()
        result["status"] = "passed"
    except Exception as error:
        result.update(
            {
                "status": "failed",
                "failed_phase": phase,
                "error_type": type(error).__name__,
                "error": str(error)[:1500],
                "elapsed_before_failure_seconds": time.perf_counter() - start,
            }
        )
    finally:
        if stream:
            stream.close()
        if reader:
            reader.close()
        if con:
            con.close()
        stop.set()
        thread.join()
        result["peak_process_rss_bytes"] = max(peak[0], process.memory_info().rss)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        required=True,
        choices=[
            "json_pages",
            "json_stream",
            "json_gzip",
            "parquet_pages",
            "variant_aggregate",
            "variant_reader",
            "variant_native",
            "json_reader",
        ],
    )
    parser.add_argument("--semantics", action="store_true")
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    (Path(__file__).parent / "scratch").mkdir(exist_ok=True)
    with tempfile.TemporaryDirectory(dir=Path(__file__).parent / "scratch") as temp:
        root = Path(temp)
        if args.semantics:
            cases = {}
            for name, batches in fixtures().items():
                case_root = root / name
                case_root.mkdir()
                pages = [
                    "\n".join(json.dumps(row, separators=(",", ":")) for row in batch)
                    for batch in batches
                ]
                cases[name] = materialize(case_root, pages, args.mode, semantic=True)
            output = {"mode": args.mode, "cases": cases}
        else:
            # Only one response page is generated at a time, to bound Python memory.
            output = materialize(root, make_pages(args.rows, args.page_size), args.mode)
            output["requested_rows"] = args.rows
            output["requested_page_size"] = args.page_size
    args.output.write_text(json.dumps(output, indent=2, default=str), encoding="utf-8")
    print(
        json.dumps(
            {
                key: output.get(key)
                for key in [
                    "mode",
                    "engine",
                    "status",
                    "capture_seconds",
                    "infer_and_build_seconds",
                    "ready_seconds",
                    "failed_phase",
                ]
            }
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
