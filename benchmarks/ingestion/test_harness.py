"""The runner must report failures rather than turn every experiment green."""

import copy

import pytest
from run_suite import child_environment, validate_schema
from schema_fixtures import identifier, quote


def test_changed_rows_and_missing_cases_fail_validation():
    baseline = {
        "late_field": {
            "status": "passed",
            "asset": {"schema": [["late", "VARCHAR"]], "rows": [{"late": "keep"}]},
        }
    }
    expected = {
        "late_field": {"status": "passed", "same_schema": True, "same_rows": True}
    }
    actual = copy.deepcopy(baseline)
    actual["late_field"]["asset"]["rows"] = [{"late": None}]
    with pytest.raises(AssertionError, match="row compatibility changed"):
        validate_schema(actual, baseline, expected)
    with pytest.raises(AssertionError, match="Missing"):
        validate_schema({}, baseline, expected)


def test_known_failure_does_not_accept_a_different_error():
    expected = {
        "empty_object": {"status": "failed", "error_type": "ConversionException"}
    }
    actual = {"empty_object": {"status": "failed", "error_type": "IOException"}}
    with pytest.raises(AssertionError, match="different error"):
        validate_schema(actual, {}, expected)


def test_child_does_not_inherit_prefect_or_storage_configuration(monkeypatch):
    for name in [
        "PREFECT_API_URL",
        "PREFECT_API_KEY",
        "FILESYSTEM_URL",
        "FILESYSTEM_BLOCK_NAME",
        "ASSET_METADATA_LOCATION",
        "PYTHONPATH",
    ]:
        monkeypatch.setenv(name, "test-only-placeholder")
    env = child_environment()
    assert not any(key.startswith("PREFECT_") for key in env)
    assert "FILESYSTEM_URL" not in env
    assert "FILESYSTEM_BLOCK_NAME" not in env
    assert "ASSET_METADATA_LOCATION" not in env
    assert "PYTHONPATH" not in env


def test_generated_sql_quotes_unusual_api_keys():
    assert identifier('quote"key') == '"quote""key"'
    assert quote("customer's field") == "'customer''s field'"
