# CSV Cleaner

> Simple, fast, auditable CSV/TSV cleaning tools — CLI and GUI wrappers around the same core cleaning logic.

This repository contains a command-line CSV cleaning engine (`main.py`), a PySide6 GUI (`gui_app.py`), a stress-test generator (`generate_max_payload.py`) and a set of sample files in `sample_data/`.

## Quick status for GitHub

- Code: `main.py`, `gui_app.py`, `generate_max_payload.py` — ready to publish. These are plain Python sources with helpful docstrings and CLI options.
- Sample data: `sample_data/` contains many small example CSVs and one ZIP (`csv_cleaner_test_suite.zip`). Review large files (e.g. `99_mega_stress_test.csv`) before pushing — large binaries should be kept out of Git or moved to releases or Git LFS.
- Build artifacts: `build/`, `dist/`, `CSV Cleaner.spec`, `build_exe.ps1` appear to be build outputs / build scripts (pyinstaller). These should typically not be pushed; add them to `.gitignore`.
- Bytecode caches: `__pycache__/` should be ignored.
- No `requirements.txt` or `pyproject.toml` detected — add one to list runtime dependencies (e.g., `pandas`, `PySide6`).
- No `LICENSE` file — choose a license before publishing (MIT/Apache/BSD/etc.).

## What I checked

- Scanned `main.py`, `gui_app.py`, `generate_max_payload.py`, `README_GUI.md` for secrets and obvious hard-coded credentials.
- Searched repository for likely secret tokens — no credentials found in source files. Matches in `build/` are references to standard library/module names (token/secrets) inside pyinstaller outputs, not secrets.

NOTE: If you ever had real credentials in older commits, treat them as leaked and rotate them; scanning only checks the working tree.

## Recommendations before pushing to GitHub

1. Add a `LICENSE` file (pick a license). 2. Add a `requirements.txt` or `pyproject.toml` listing runtime deps. Example minimal `requirements.txt`:

```
PySide6>=6.5
pandas>=2.0    # optional: only needed for pandas engine

```

3. Add a `.gitignore`. Minimal recommended contents:

```
# Python
__pycache__/
*.py[cod]
*.pyo
*.pyd

# Environments
env/
venv/
.env

# Packaging / build outputs
build/
dist/
*.egg-info/

# PyInstaller output
CSV Cleaner.spec
"CSV Cleaner"/

# OS files
.DS_Store
Thumbs.db

# Large test outputs (if generated locally)
*.csv

```

4. Remove or move large files out of git history (if `99_mega_stress_test.csv` or others exceed a few MB). Use Git LFS or attach to GitHub release assets.

5. Add `README.md` (this file). Optionally add `README_GUI.md` as supplemental documentation (already present).

6. Add a short `CONTRIBUTING.md` and `CODE_OF_CONDUCT.md` later if this will be collaborative.

7. Add a `GitHub Actions` workflow for CI if you plan to run tests or lint on push (I can add a lightweight workflow if you want).

## How to run

CLI usage (simple):

```
python main.py -i data.csv

# overwrite original
python main.py -i data.csv --inplace

# interactive GUI
python gui_app.py
```

Notes:
- For the GUI you need PySide6 installed.
- The `pandas` engine is optional but recommended for richer semantics and often better performance; install `pandas` if you plan to use it.

## Packaging for distribution

- The project includes a PyInstaller spec and a `build_exe.ps1` script. Typical steps:
  1. Create a virtualenv and install requirements.
  2. Run the PowerShell script `build_exe.ps1` or `pyinstaller "CSV Cleaner.spec"`.

## Suggested next PRs / improvements I can do for you

- Add a `requirements.txt` automatically inferred from imports.
- Add a `.gitignore` file and move `build/` and `dist/` into gitignore.
- Add a `LICENSE` (MIT) and a lightweight GitHub Actions CI workflow.
- Optional: add a minimal GitHub Actions workflow to run flake8/black/pytest.

## Files and purpose (quick map)

- `main.py` — CLI core cleaning logic and streaming engine.
- `gui_app.py` — PySide6 GUI wrapper re-using core functions.
- `generate_max_payload.py` — stress test CSV generator (produces large datasets).
- `README_GUI.md` — detailed GUI README (kept as supplemental).
- `build/`, `dist/` — build outputs from PyInstaller (should be ignored before pushing).
- `sample_data/` — example CSVs and test suite; keep small examples, remove very large test files from the repo if present.

---

If you want, I can now:

1. Create a `.gitignore` file in the repo with the suggested entries.
2. Add a `requirements.txt` (I can infer common deps from the code: `PySide6`, `pandas`, `numpy` for the generator script).
3. Add a `LICENSE` (MIT) and a lightweight GitHub Actions CI workflow.

Tell me which of the above you'd like me to perform next and I'll carry it out.
