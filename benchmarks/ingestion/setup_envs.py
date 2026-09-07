"""Create isolated, pinned research environments without installing Assquack."""

import argparse
import os
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MAD_COMMIT = "59a811ff4003eb68ba77f8fdeb9cbdabf09b2f19"
ENGINES = {"155": "v1.5.5", "2": "v2.0.0-alpha39998"}


def python_path(engine: str) -> Path:
    environment = ROOT / ".venvs" / engine
    return environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", choices=["155", "2", "both"], default="both")
    args = parser.parse_args()
    if not shutil.which("uv"):
        raise SystemExit(
            "Install uv first: https://docs.astral.sh/uv/getting-started/installation/"
        )
    repository = ROOT.parents[1]
    subprocess.run(
        ["git", "submodule", "update", "--init", ".submodules/MAD.Prefect"],
        cwd=repository,
        check=True,
    )
    submodule = repository / ".submodules/MAD.Prefect"
    revision = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=submodule, text=True
    ).strip()
    if revision != MAD_COMMIT:
        raise SystemExit(
            f"Expected MAD.Prefect {MAD_COMMIT}; found {revision}. Review the benchmark pin."
        )
    for engine in ENGINES if args.engine == "both" else [args.engine]:
        environment = ROOT / ".venvs" / engine
        if not python_path(engine).exists():
            subprocess.run(
                ["uv", "venv", "--python", "3.11", str(environment)], check=True
            )
        subprocess.run(
            [
                "uv",
                "pip",
                "sync",
                "--python",
                str(python_path(engine)),
                str(ROOT / f"requirements-{engine}.txt"),
            ],
            check=True,
        )
        actual = subprocess.check_output(
            [
                str(python_path(engine)),
                "-c",
                "import duckdb; print(duckdb.sql('SELECT version()').fetchall()[0][0])",
            ],
            text=True,
        ).strip()
        if actual != ENGINES[engine]:
            raise SystemExit(
                f"Wrong engine: expected {ENGINES[engine]}, received {actual}"
            )
        print(f"Ready: {engine} = {actual}", flush=True)


if __name__ == "__main__":
    main()
