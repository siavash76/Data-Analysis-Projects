# CSV Cleaner

> Simple, fast, auditable CSV/TSV cleaning tools — available as both a CLI and a GUI.

## Overview
CSV Cleaner is a Python-based tool for cleaning messy CSV/TSV files.  
It provides:
- A **command-line interface** (`main.py`) for automation and scripting
- A **PySide6 GUI** (`gui_app.py`) for interactive, user-friendly cleaning
- A **stress-test generator** (`generate_max_payload.py`) for benchmarking
- A library of **sample datasets** (`sample_data/`) for testing

## Features
- Trim whitespace from text fields
- Drop duplicate rows (with optional key selection)
- Standardize column names (snake_case)
- Remove empty rows/columns
- Infer types (int, float, bool, date) with date format override
- Handle missing values (empty, constant, zero, mean, median, mode)
- Live progress display in GUI
- Consistent cleaning logic shared between CLI and GUI

## Installation
```bash
git clone https://github.com/siavash76/Data-Analysis-Projects.git
cd Data-Analysis-Projects/csv_cleaner_project
pip install -r requirements.txt

Notes
Review your cleaned files—this tool standardizes structure, but data context always requires human oversight.

For stress testing, see generate_max_payload.py which generates huge messy CSVs.

Both CLI (main.py) and GUI (gui_app.py) share the same cleaning core.
