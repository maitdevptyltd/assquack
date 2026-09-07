"""Shared inconsistent API responses and native-reader compatibility settings."""

import json

OPTIONS = "format='auto', union_by_name=true, sample_size=-1, field_appearance_threshold=0, map_inference_threshold=-1, maximum_object_size=33554432"


def quote(value):
    return "'" + value.replace("'", "''") + "'"


def identifier(value):
    return '"' + value.replace('"', '""') + '"'


def fixtures():
    return {
        "late_fields": [
            [{"id": 1, "details": {"name": "a"}, "later": None}],
            [
                {
                    "id": 2,
                    "details": {"name": "b", "code": 7},
                    "later": 42,
                    "new_field": True,
                }
            ],
        ],
        "same_batch_late_field": [[{"id": 1}, {"id": 2, "late": "keep me"}]],
        "numeric_widening": [[{"id": 1, "value": 1}], [{"id": 2, "value": 1.25}]],
        "number_string": [
            [{"id": 1, "value": 1}],
            [{"id": 2, "value": "2"}, {"id": 3, "value": "unknown"}],
        ],
        "object_scalar": [
            [{"id": 1, "value": {"code": 7}}],
            [{"id": 2, "value": "unavailable"}],
        ],
        "mixed_arrays": [
            [{"id": 1, "items": [1, 2]}],
            [{"id": 2, "items": ["x", {"name": "y"}, None]}],
        ],
        "dates": [
            [{"id": 1, "day": "2026-09-01", "at": "2026-09-01T09:30:00Z"}],
            [{"id": 2, "day": "2026-09-02", "at": "2026-09-02T09:30:00Z"}],
        ],
        "large_numbers": [
            [{"id": 1, "value": 9007199254740993}],
            [{"id": 2, "value": 0.5}, {"id": 3, "value": 18446744073709551617}],
        ],
        "awkward_keys": [
            [{"id": 1, "a.b": {"x/y": 1}, 'quote"key': 2}],
            [{"id": 2, "a.b": {"x/y": 3}, 'quote"key': 4}],
        ],
        "case_collision": [[{"id": 1, "Code": 1}], [{"id": 2, "code": "other"}]],
        "empty_then_populated": [
            [{"id": 1, "items": [], "detail": {}, "value": None}],
            [{"id": 2, "items": [{"n": 3}], "detail": {"code": "x"}, "value": True}],
        ],
        "missing_null": [
            [{"id": 1}, {"id": 2, "value": None}],
            [{"id": 3, "value": "x"}],
        ],
        "wide_nested": [
            [{"id": 1, "attributes": {f"key_{i}": i for i in range(220)}}],
            [{"id": 2, "attributes": {"late": "x"}}],
        ],
        "fortieth_fragment": [
            [{"id": i}] if i < 39 else [{"id": i, "late": "found"}] for i in range(40)
        ],
        "empty_extraction": [[], []],
    }


def describe(con, query):
    query = "SELECT * FROM (" + query + ") named_columns"
    schema = [(r[0], r[1]) for r in con.execute("DESCRIBE " + query).fetchall()]
    columns = [
        identifier(name)
        + ("::JSON" if dtype == "VARIANT" else "")
        + " AS "
        + identifier(name)
        for name, dtype in schema
    ]
    serializable = "SELECT " + ", ".join(columns) + " FROM (" + query + ") source"
    rows = [
        json.loads(r[0])
        for r in con.execute(
            "SELECT to_json(t) FROM ("
            + serializable
            + ") t ORDER BY try_cast(id AS BIGINT)"
        ).fetchall()
    ]
    return {"schema": schema, "rows": rows}
