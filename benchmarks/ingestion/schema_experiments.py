"""Compare API-defined schema paths with synthetic, deliberately inconsistent records."""

import argparse
import io
import json
import tempfile
import time
from pathlib import Path
from typing import Any

import duckdb
import fsspec
import pyarrow as pa
from schema_fixtures import OPTIONS, describe, fixtures, identifier, quote


def attempt(action):
    start = time.perf_counter()
    try:
        result = action()
        return {"ok": True, **result, "seconds": round(time.perf_counter() - start, 6)}
    except Exception as error:
        return {
            "ok": False,
            "error_type": type(error).__name__,
            "error": str(error)[:1800],
            "seconds": round(time.perf_counter() - start, 6),
        }


def infer_transform(con, expression, target="shaped"):
    structure = con.execute(
        f"SELECT json_group_structure({expression}) FROM raw"
    ).fetchall()[0][0]
    if structure is None:
        return {"empty": True, "schema": [], "rows": []}
    sql = f"SELECT data.* FROM (SELECT from_json({expression}, {quote(structure)}) AS data FROM raw)"
    con.execute(f"CREATE OR REPLACE VIEW {target} AS {sql}")
    return {
        "inferred_structure": json.loads(structure),
        **describe(con, f"SELECT * FROM {target}"),
    }


def variant_projection(con):
    # Generate column names from every record, retaining VARIANT values for conflicts.
    keys = [
        r[0]
        for r in con.execute(
            "SELECT DISTINCT unnest(json_keys(payload::JSON)) AS key FROM raw ORDER BY key"
        ).fetchall()
    ]
    if not keys:
        return {"empty": True, "schema": [], "rows": []}
    fields = [
        f"variant_extract(payload, {quote(key)}) AS {identifier(key)}" for key in keys
    ]
    sql = "SELECT " + ", ".join(fields) + " FROM raw"
    return {"discovered_keys": keys, **describe(con, sql)}


class DatabaseFragments(fsspec.AbstractFileSystem):
    """A read-only experiment: let the native reader open database-held fragments."""

    protocol = "dbfragments"
    cachable = False

    def __init__(self, connection):
        super().__init__()
        self.connection = connection
        self.bytes_opened = 0

    def info(self, path, **kwargs):
        name = self._strip_protocol(path)
        rows = self.connection.execute(
            "SELECT octet_length(body) FROM fragments WHERE name=?", [name]
        ).fetchall()
        if not rows:
            raise FileNotFoundError(name)
        return {"name": name, "size": rows[0][0], "type": "file"}

    def ls(self, path, detail=True, **kwargs):
        rows = self.connection.execute(
            "SELECT name, octet_length(body) FROM fragments ORDER BY name"
        ).fetchall()
        entries = [{"name": name, "size": size, "type": "file"} for name, size in rows]
        return entries if detail else [entry["name"] for entry in entries]

    def _open(self, path, mode="rb", **kwargs):
        if mode != "rb":
            raise ValueError("Read only")
        rows = self.connection.execute(
            "SELECT body FROM fragments WHERE name=?", [self._strip_protocol(path)]
        ).fetchall()
        if not rows:
            raise FileNotFoundError(path)
        self.bytes_opened += len(rows[0][0])
        return io.BytesIO(rows[0][0])


def database_fragment_probe(root, name, texts):
    database = root / f"{name}.duckdb"
    con = duckdb.connect(str(database), config={"threads": 1})
    con.execute("CREATE TABLE fragments(name VARCHAR PRIMARY KEY, body BLOB)")
    batch = pa.table(
        {
            "name": [f"page_{i:03d}.json" for i in range(len(texts))],
            "body": [t.encode() for t in texts],
        }
    )
    con.register("batch", batch)
    con.execute("INSERT INTO fragments SELECT * FROM batch")
    con.unregister("batch")
    reader = duckdb.connect(str(database), config={"threads": 1})
    filesystem = DatabaseFragments(reader)
    con.register_filesystem(filesystem)
    paths = [f"dbfragments://page_{i:03d}.json" for i in range(len(texts))]
    try:
        query = f"SELECT * FROM read_json({paths!r}, {OPTIONS})"
        result = describe(con, query)
        result["bytes_opened"] = filesystem.bytes_opened
        return result
    finally:
        con.close()
        reader.close()


def run_case(root, name, batches):
    folder = root / name
    folder.mkdir()
    texts = [
        "\n".join(json.dumps(record, separators=(",", ":")) for record in batch)
        for batch in batches
    ]
    paths = []
    for index, text in enumerate(texts):
        path = folder / f"page_{index:03d}.json"
        path.write_text(text, encoding="utf-8")
        paths.append(path.as_posix())
    records = [row for batch in batches for row in batch]
    result: dict[str, Any] = {"input_rows": records}
    con = duckdb.connect()

    result["files"] = attempt(
        lambda: describe(
            con,
            f"SELECT * FROM read_json({quote(folder.as_posix() + '/*.json')}, {OPTIONS})",
        )
    )

    # This keeps the same reader and inference but moves all fragments into RAM.
    memory = fsspec.filesystem("memory")
    con.register_filesystem(memory)
    memory_paths = []
    for index, text in enumerate(texts):
        path = f"memory://schema-probe/{name}/{index:03d}.json"
        memory.pipe(path, text.encode())
        memory_paths.append(path)
    result["memory_files"] = attempt(
        lambda: describe(
            con,
            f"SELECT * FROM read_json({quote(f'memory://schema-probe/{name}/*.json')}, {OPTIONS})",
        )
    )
    for path in memory_paths:
        memory.rm(path)
    result["database_fragments"] = attempt(
        lambda: database_fragment_probe(root, name, texts)
    )

    # The only Arrow schema is one string per raw response record, not API columns.
    con.execute("CREATE TABLE raw(body JSON, payload VARIANT)")
    for text, batch in zip(texts, batches, strict=True):
        if not batch:
            continue
        con.register("batch", pa.table({"body": text.splitlines()}))
        con.execute("INSERT INTO raw SELECT body::JSON, body::JSON::VARIANT FROM batch")
        con.unregister("batch")

    result["json_auto"] = attempt(lambda: infer_transform(con, "body"))
    result["variant_auto"] = attempt(lambda: infer_transform(con, "payload::JSON"))
    result["variant_columns"] = attempt(lambda: variant_projection(con))
    raw_json = [
        json.loads(r[0]) for r in con.execute("SELECT body FROM raw").fetchall()
    ]
    raw_variant = [
        json.loads(r[0])
        for r in con.execute("SELECT payload::JSON FROM raw").fetchall()
    ]
    result["json_raw_equal_input"] = raw_json == records
    result["variant_raw_equal_input"] = raw_variant == records
    if raw_variant != records:
        result["variant_raw_rows"] = raw_variant

    def arrow_direct():
        for index, batch in enumerate(batches):
            if not batch:
                continue
            con.register("batch", pa.Table.from_pylist(batch))
            if index == 0:
                con.execute("CREATE TABLE typed AS SELECT * FROM batch")
            else:
                con.execute("INSERT INTO typed BY NAME SELECT * FROM batch")
            con.unregister("batch")
        return describe(con, "SELECT * FROM typed")

    result["arrow_first_schema"] = attempt(arrow_direct)
    con.close()
    baseline = result["files"]
    for path in [
        "memory_files",
        "database_fragments",
        "json_auto",
        "variant_auto",
        "variant_columns",
        "arrow_first_schema",
    ]:
        other = result[path]
        if baseline["ok"] and other["ok"]:
            other["same_schema_as_files"] = dict(baseline["schema"]) == dict(
                other["schema"]
            )
            other["same_values_as_files"] = baseline["rows"] == other["rows"]
    return result


def incremental_probe():
    con = duckdb.connect()
    con.execute("CREATE TABLE raw(id BIGINT PRIMARY KEY, payload VARIANT)")
    con.execute(
        "INSERT INTO raw VALUES (1, ?::JSON::VARIANT)",
        [json.dumps({"id": 1, "value": 1})],
    )
    before = infer_transform(con, "payload::JSON")
    con.register(
        "changes",
        pa.table(
            {
                "id": [1, 2],
                "body": [
                    json.dumps({"id": 1, "value": "unknown", "new_field": "x"}),
                    json.dumps({"id": 2, "value": 2, "new_field": "y"}),
                ],
            }
        ),
    )
    con.execute("""MERGE INTO raw AS t USING changes AS s ON t.id=s.id
        WHEN MATCHED THEN UPDATE SET payload=s.body::JSON::VARIANT
        WHEN NOT MATCHED THEN INSERT VALUES(s.id, s.body::JSON::VARIANT)""")
    stale = attempt(lambda: describe(con, "SELECT * FROM shaped"))
    rebuilt = attempt(lambda: infer_transform(con, "payload::JSON"))
    assert con.execute("SELECT count(*) FROM raw").fetchall()[0][0] == 2
    assert rebuilt["ok"] and "new_field" in dict(rebuilt["schema"])
    con.close()
    return {
        "before": before,
        "existing_view_after_merge": stale,
        "regenerated_view": rebuilt,
    }


def sampling_probe(root):
    path = root / "late_row.json"
    with path.open("w", encoding="utf-8") as file:
        for i in range(25000):
            row: dict[str, Any] = {"id": i}
            if i == 24999:
                row["late_field"] = "found"
            file.write(json.dumps(row) + "\n")
    con = duckdb.connect()

    def read(options):
        query = f"SELECT * FROM read_json({quote(path.as_posix())}, {options})"
        schema = [(r[0], r[1]) for r in con.execute("DESCRIBE " + query).fetchall()]
        # Fetch the full data so sampling-induced conversion errors are observed.
        rows = con.execute(query).fetchall()
        return {"schema": schema, "row_count": len(rows), "last_row": rows[-1]}

    result = {
        "default": attempt(lambda: read("format='auto'")),
        "mad_prefect_options": attempt(lambda: read(OPTIONS)),
    }
    con.close()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    (Path(__file__).parent / "scratch").mkdir(exist_ok=True)
    con = duckdb.connect()
    result = {
        "versions": {
            "duckdb": duckdb.__version__,
            "pyarrow": pa.__version__,
            "fsspec": fsspec.__version__,
        },
        "baseline_options": OPTIONS,
        "json_group_structure_implementation": con.execute(
            "SELECT macro_definition FROM duckdb_functions() WHERE function_name='json_group_structure'"
        ).fetchall()[0][0],
        "cases": {},
    }
    con.close()
    with tempfile.TemporaryDirectory(dir=Path(__file__).parent / "scratch") as temp:
        root = Path(temp)
        for name, batches in fixtures().items():
            result["cases"][name] = run_case(root, name, batches)
            status = {
                path: ("ok" if detail["ok"] else detail["error_type"])
                for path, detail in result["cases"][name].items()
                if isinstance(detail, dict) and "ok" in detail
            }
            print(name, status, flush=True)
        result["incremental"] = incremental_probe()
        result["sampling"] = sampling_probe(root)
    # Assert the behaviours that determine whether each route is a viable substitute.
    populated = [case for case in result["cases"].values() if case["input_rows"]]
    assert len(populated) == 14
    assert all(case["files"]["ok"] for case in populated)
    assert all(
        case["database_fragments"]["same_schema_as_files"]
        and case["database_fragments"]["same_values_as_files"]
        for case in populated
    )
    assert all(case["json_raw_equal_input"] for case in populated)
    assert result["cases"]["large_numbers"]["variant_raw_equal_input"] is False
    assert (
        result["cases"]["numeric_widening"]["arrow_first_schema"]["rows"][1]["value"]
        == 1
    )
    assert "late" not in dict(
        result["cases"]["same_batch_late_field"]["arrow_first_schema"]["schema"]
    )
    assert result["sampling"]["default"]["ok"] is False
    assert result["sampling"]["mad_prefect_options"]["last_row"] == (24999, "found")
    result["regression_assertions"] = "passed"
    args.output.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")


if __name__ == "__main__":
    main()
