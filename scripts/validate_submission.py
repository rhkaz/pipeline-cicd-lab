"""Fail when generated or inconsistent evidence enters the submission."""

from __future__ import annotations

import json
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FORBIDDEN_NAMES = {
    ".venv",
    ".ipynb_checkpoints",
    ".pytest_cache",
    "__pycache__",
    "htmlcov",
    "metastore_db",
    "output",
    "spark-warehouse",
}


def main() -> None:
    failures: list[str] = []
    for path in PROJECT_ROOT.rglob("*"):
        relative = path.relative_to(PROJECT_ROOT)
        if ".git" in relative.parts:
            continue
        if any(part in FORBIDDEN_NAMES for part in relative.parts):
            failures.append(f"generated path present: {relative}")
        if (
            path.suffix in {".pyc", ".crc"}
            or path.name.endswith(".egg-info")
            or path.name == "derby.log"
        ):
            failures.append(f"generated file present: {relative}")

    notebook_path = PROJECT_ROOT / "notebooks" / "01_end_to_end_demo.ipynb"
    notebook = json.loads(notebook_path.read_text(encoding="utf-8"))
    for index, cell in enumerate(notebook.get("cells", [])):
        if cell.get("cell_type") != "code":
            continue
        if any(
            output.get("output_type") == "error"
            for output in cell.get("outputs", [])
        ):
            failures.append(f"notebook cell {index} contains saved error output")

    if failures:
        raise SystemExit("Submission validation failed:\n- " + "\n- ".join(failures))
    print("Submission validation: PASS")


if __name__ == "__main__":
    main()
