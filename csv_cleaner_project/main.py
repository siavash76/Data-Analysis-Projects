"""
CSV Cleaner - simple, fast, and safe CLI

What it does
- Accepts one or more CSV/TSV files via --input or a file picker.
- Detects the delimiter (comma, tab, etc.) unless you set --delimiter.
- Standardizes column names to lower_snake_case; de-duplicates with suffixes.
- Trims extra spaces in headers and cells (disable with --no-trim).
- Handles uneven rows: pad (default), truncate, or error (--pad-rows).
- Drops rows that are completely empty by default (--keep-empty-rows to keep).
- Optional: remove duplicate rows (whole row or by --dedup-keys; names matched after sanitization).
- Optional: remove columns that are empty in all rows.
- Optional: infer integers/floats/booleans and parse dates uniformly (--parse-dates).
- Optional: fill missing values (empty, constant, zero, mean, median, mode); add custom NA tokens via --na.
- Writes a per-file log next to each output unless --no-log.
- Lets you choose where to save with --ask-output or when picking inputs interactively (also see --output-dir and --suffix).

Two processing engines
- csv: streaming, no extra dependencies.
- pandas: requires pandas; richer type handling and often faster operations(default).

How to use (examples)
- Clean a file and save alongside it: python main.py -i data.csv
- Overwrite the original: python main.py -i data.csv --inplace
- Clean a folder or glob: python main.py -i "./data/*.csv" -o ./cleaned
- Ask where to save: python main.py --ask-output
- With type inference and date parsing: python main.py -i data.csv --engine pandas --infer-types --parse-dates --fill-missing mean --date-format %Y-%m-%d

Out of scope (human review recommended)
- Understanding your data's meaning; verify sanitized headers and dedup keys.
- Correcting typos or merging similar values (e.g., Jon vs John, USA vs U.S.A.).
- Enforcing business rules, units, or ranges (e.g., valid IDs, units, date ranges).
- Fixing severely broken CSVs (bad quoting, mixed delimiters within a row).
- Detecting/removing sensitive data before sharing outputs.
- Preserving spreadsheet formulas, formatting, or comments.
- Choosing a fill strategy; filling changes data.
- Time zones and ambiguous date formats; confirm final format and zone.

Tip: Work on copies, run without --inplace, read the *_log.txt, then iterate with options that fit your dataset.
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, date
from typing import Iterable, List, Tuple, Optional, Dict, Any, Callable
from collections import Counter
import math


# -------------------------
# Utilities
# -------------------------
def _normalize_whitespace(s: str) -> str:
    s = s.replace("\u00A0", " ")  # non-breaking space to regular space
    s = s.replace("\t", " ")
    s = s.strip()
    # collapse internal whitespace
    parts = s.split()
    return " ".join(parts)


def sanitize_header_name(name: str) -> str:
    s = _normalize_whitespace(str(name))
    # lower_snake_case: replace non-alphanumerics with underscore
    out = []
    prev_underscore = False
    for ch in s.lower():
        if ch.isalnum():
            out.append(ch)
            prev_underscore = False
        else:
            if not prev_underscore:
                out.append("_")
            prev_underscore = True
    sanitized = "".join(out).strip("_")
    return sanitized or "column"


def sanitize_headers(headers: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    result: List[str] = []
    for h in headers:
        base = sanitize_header_name(h)
        count = seen.get(base, 0)
        if count:
            new = f"{base}_{count+1}"
            seen[base] = count + 1
            result.append(new)
        else:
            seen[base] = 1
            result.append(base)
    return result


def try_sniff_dialect(sample: str, delimiter: Optional[str]) -> csv.Dialect:
    # If user specified a delimiter, honor it (with tab alias)
    if delimiter:
        delim = delimiter
        if delim.lower() == 'tab' or delim == r'\t':
            delim = "\t"
        Simple = type(
            "_SimpleDialect",
            (csv.Dialect,),
            dict(
                delimiter=delim,
                quotechar='"',
                doublequote=True,
                skipinitialspace=False,
                lineterminator="\n",
                quoting=csv.QUOTE_MINIMAL,
            ),
        )
        return Simple()

    allowed = ',;\t|'
    sniffer = csv.Sniffer()
    try:
        sniffed = sniffer.sniff(sample, delimiters=allowed)
        delim = getattr(sniffed, 'delimiter', ',')
        if delim not in allowed:
            delim = ','
    except Exception:
        delim = ','

    Robust = type(
        "_RobustDialect",
        (csv.Dialect,),
        dict(
            delimiter=delim,
            quotechar='"',
            doublequote=True,
            skipinitialspace=False,
            lineterminator="\n",
            quoting=csv.QUOTE_MINIMAL,
        ),
    )
    return Robust()


def read_text(path: str, max_bytes: int = 8192, encoding_candidates: Tuple[str, ...] = ("utf-8-sig", "utf-8", "cp1252", "latin-1")) -> Tuple[str, str]:
    """Read up to max_bytes from file trying several encodings. Returns (text, encoding)."""
    for enc in encoding_candidates:
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                return f.read(max_bytes), enc
        except Exception:
            continue
    # last resort, replace errors to avoid crashing
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read(max_bytes), "utf-8-replace"


# -------------------------
# Type parsing helpers
# -------------------------
BOOL_TRUE = {"true", "t", "yes", "y", "1"}
BOOL_FALSE = {"false", "f", "no", "n", "0"}


def parse_bool(s: str) -> Optional[bool]:
    t = s.strip().lower()
    if t in BOOL_TRUE:
        return True
    if t in BOOL_FALSE:
        return False
    return None


def _strip_numeric_decorations(s: str) -> str:
    x = s.strip()
    # handle accounting negatives (e.g., (1234))
    negative = x.startswith("(") and x.endswith(")")
    if negative:
        x = x[1:-1]
    # remove thousands separators common variants
    x = x.replace(",", "").replace(" ", "")
    # handle percentage
    pct = x.endswith("%")
    if pct:
        x = x[:-1]
    if negative:
        x = "-" + x
    return x


def parse_numeric(s: str) -> Tuple[Optional[Any], Optional[str]]:
    """Return (value, kind) where kind in {"int","float"}. None if not numeric."""
    x = _strip_numeric_decorations(s)
    if x == "":
        return None, None
    try:
        if "." not in x and "e" not in x.lower():
            val = int(x)
            return val, "int"
    except Exception:
        pass
    try:
        valf = float(x)
        if math.isfinite(valf):
            if valf.is_integer():
                return int(valf), "int"
            return valf, "float"
    except Exception:
        pass
    return None, None


DATE_FORMATS = [
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%d-%m-%Y",
    "%m-%d-%Y",
    "%Y.%m.%d",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
]


def parse_date_str(s: str) -> Optional[datetime]:
    t = s.strip()
    if not t:
        return None
    # Try ISO fast path
    try:
        # fromisoformat supports YYYY-MM-DD[ HH:MM[:SS[.mmm]]]
        return datetime.fromisoformat(t.replace("Z", "+00:00"))
    except Exception:
        pass
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(t, fmt)
        except Exception:
            continue
    return None


@dataclass
class CleanStats:
    input_path: str
    output_path: str
    rows_in: int = 0
    rows_out: int = 0
    empty_rows_dropped: int = 0
    duplicate_rows_dropped: int = 0
    header_in: List[str] = None  # type: ignore
    header_out: List[str] = None  # type: ignore
    delimiter: str = ","
    encoding_read: str = ""
    pad_mode: str = "pad"
    # New reporting fields
    infer_types: bool = False
    parse_dates: bool = False
    date_format: str = "%Y-%m-%d"
    type_threshold: float = 0.9
    fill_missing: str = "none"
    column_types: Dict[str, str] = None  # type: ignore


def write_log(stats: CleanStats, log_path: str) -> None:
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"CSV Cleaner Log - {datetime.now().isoformat()}\n")
        f.write("-" * 60 + "\n")
        data = asdict(stats)
        # Ensure lists are represented nicely
        data["header_in"] = ", ".join(stats.header_in or [])
        data["header_out"] = ", ".join(stats.header_out or [])
        for k, v in data.items():
            f.write(f"{k}: {v}\n")


def format_log_text(stats: CleanStats) -> str:
    lines = [
        f"CSV Cleaner Log - {datetime.now().isoformat()}",
        "-" * 60,
    ]
    data = asdict(stats)
    data["header_in"] = ", ".join(stats.header_in or [])
    data["header_out"] = ", ".join(stats.header_out or [])
    for k, v in data.items():
        lines.append(f"{k}: {v}")
    return "\n".join(lines)


def pass_one_scan(path: str, dialect: csv.Dialect, encoding: str) -> Tuple[List[str], int]:
    """Return (first_row_as_header, max_columns_encountered)."""
    max_cols = 0
    header: List[str] = []
    with open(path, "r", encoding=encoding, errors="replace", newline="") as f:
        reader = csv.reader(f, dialect)
        for i, row in enumerate(reader):
            max_cols = max(max_cols, len(row))
            if i == 0:
                header = row
            # scan entire file to ensure width covers all rows
    return header, max_cols


def clean_file(
    in_path: str,
    out_path: str,
    *,
    delimiter: Optional[str] = None,
    trim_cells: bool = True,
    drop_empty_rows: bool = True,
    drop_duplicates: bool = False,
    dedup_keys: Optional[List[str]] = None,
    pad_rows: str = "pad",  # pad|truncate|error
    remove_empty_columns: bool = False,
    # new options
    infer_types: bool = False,
    parse_dates: bool = False,
    date_format: str = "%Y-%m-%d",
    type_threshold: float = 0.9,
    fill_missing: str = "none",
    fill_constant: str = "",
    na_tokens: Optional[List[str]] = None,
    progress: Optional[Callable[[str, Optional[float]], None]] = None,
) -> CleanStats:
    if progress:
        progress("Detecting encoding and delimiter", 0.02)
    sample, encoding = read_text(in_path)
    dialect = try_sniff_dialect(sample, delimiter)

    # First pass: header + max columns
    raw_header, max_cols_seen = pass_one_scan(in_path, dialect, encoding)
    if progress:
        progress("Scanned file and detected header/width", 0.12)
    header_in = raw_header or []
    if not header_in:
        # Empty file or no header: synthesize columns
        header_in = [f"column_{i+1}" for i in range(max(1, max_cols_seen))]
        max_cols_seen = len(header_in)

    header_out = sanitize_headers(header_in)
    if progress:
        progress("Standardized headers", 0.2)

    # Ensure header length equals max width we'll write
    width = max(max_cols_seen, len(header_out))
    if width > len(header_out):
        extra = [f"extra_{i}" for i in range(1, width - len(header_out) + 1)]
        header_out = header_out + extra

    stats = CleanStats(
        input_path=in_path,
        output_path=out_path,
        header_in=header_in,
        header_out=header_out,
        delimiter=getattr(dialect, "delimiter", ","),
        encoding_read=encoding,
        pad_mode=pad_rows,
        infer_types=infer_types,
        parse_dates=parse_dates,
        date_format=date_format,
        type_threshold=type_threshold,
        fill_missing=fill_missing,
    )

    # Analyze for types and fill values if requested
    def analyze_file(path: str,
                     dialect: csv.Dialect,
                     encoding: str,
                     width: int,
                     type_threshold: float,
                     infer_types: bool,
                     parse_dates: bool,
                     fill_missing: str,
                     na_set: set) -> Tuple[Dict[int, str], Dict[int, Any]]:
        # Per-column stats
        nonempty: Dict[int, int] = {}
        num_count: Dict[int, int] = {}
        int_count: Dict[int, int] = {}
        float_count: Dict[int, int] = {}
        bool_count: Dict[int, int] = {}
        date_count: Dict[int, int] = {}
        num_values: Dict[int, List[float]] = {}
        mode_counter: Dict[int, Counter] = {}

        with open(path, "r", encoding=encoding, errors="replace", newline="") as f:
            reader = csv.reader(f, dialect)
            for i, row in enumerate(reader):
                if i == 0:
                    continue  # header
                for j in range(min(len(row), width)):
                    raw = row[j]
                    val = _normalize_whitespace(raw) if isinstance(raw, str) else str(raw)
                    if val == "" or val.strip().lower() in na_set:
                        continue
                    nonempty[j] = nonempty.get(j, 0) + 1
                    # mode for fill=mode
                    if fill_missing == "mode":
                        mode_counter.setdefault(j, Counter())[val] += 1
                    # numeric
                    v, kind = parse_numeric(val)
                    if v is not None:
                        num_count[j] = num_count.get(j, 0) + 1
                        if isinstance(v, int):
                            int_count[j] = int_count.get(j, 0) + 1
                            num_values.setdefault(j, []).append(float(v))
                        else:
                            float_count[j] = float_count.get(j, 0) + 1
                            num_values.setdefault(j, []).append(float(v))
                    # boolean
                    b = parse_bool(val)
                    if b is not None:
                        bool_count[j] = bool_count.get(j, 0) + 1
                    # date
                    if parse_dates:
                        dt = parse_date_str(val)
                        if dt is not None:
                            date_count[j] = date_count.get(j, 0) + 1

        # Decide types
        types_by_idx: Dict[int, str] = {}
        for j in range(width):
            ne = nonempty.get(j, 0)
            t = "string"
            if ne > 0:
                num_ratio = num_count.get(j, 0) / ne
                bool_ratio = bool_count.get(j, 0) / ne
                date_ratio = date_count.get(j, 0) / ne if parse_dates else 0.0
                if infer_types and num_ratio >= type_threshold:
                    # integer vs float
                    if float_count.get(j, 0) > 0:
                        t = "float"
                    else:
                        t = "integer"
                elif infer_types and bool_ratio >= type_threshold:
                    t = "boolean"
                elif parse_dates and date_ratio >= type_threshold:
                    t = "date"
                else:
                    t = "string"
            types_by_idx[j] = t

        # Compute fill values per column
        fill_by_idx: Dict[int, Any] = {}
        if fill_missing == "empty":
            fill_by_idx = {j: "" for j in range(width)}
        elif fill_missing == "constant":
            fill_by_idx = {j: fill_constant for j in range(width)}
        elif fill_missing == "zero":
            for j, t in types_by_idx.items():
                if t in ("integer", "float"):
                    fill_by_idx[j] = 0
        elif fill_missing in ("mean", "median"):
            for j, t in types_by_idx.items():
                if t in ("integer", "float") and j in num_values and num_values[j]:
                    vals = num_values[j]
                    if fill_missing == "mean":
                        m = sum(vals) / len(vals)
                        fill_by_idx[j] = int(round(m)) if t == "integer" and float(m).is_integer() else m
                    else:  # median
                        vs = sorted(vals)
                        n = len(vs)
                        if n % 2 == 1:
                            med = vs[n//2]
                        else:
                            med = (vs[n//2 - 1] + vs[n//2]) / 2.0
                        fill_by_idx[j] = int(round(med)) if t == "integer" and float(med).is_integer() else med
        elif fill_missing == "mode":
            for j in range(width):
                if j in mode_counter and mode_counter[j]:
                    # pick most common value
                    fill_by_idx[j] = mode_counter[j].most_common(1)[0][0]

        return types_by_idx, fill_by_idx

    need_analysis = infer_types or parse_dates or (fill_missing not in ("none",))
    types_by_idx: Dict[int, str] = {i: "string" for i in range(width)}
    fill_by_idx: Dict[int, Any] = {}
    na_set = set(["", "na", "n/a", "null", "none", "#n/a", "-", "?", "nan"])
    if na_tokens:
        for tkn in na_tokens:
            if tkn is not None:
                na_set.add(str(tkn).strip().lower())
    if need_analysis:
        if progress:
            progress("Analyzing column types and computing fill values", 0.35)
        types_by_idx, fill_by_idx = analyze_file(
            in_path, dialect, encoding, width, type_threshold, infer_types, parse_dates, fill_missing, na_set
        )
    # Map column types by name for stats
    stats.column_types = {header_out[i]: types_by_idx.get(i, "string") for i in range(len(header_out))}

    # Prepare dedup tracking
    seen: set = set()
    dedup_indexes: Optional[List[int]] = None
    if drop_duplicates:
        if dedup_keys:
            key_to_index = {h: i for i, h in enumerate(header_out)}
            try:
                dedup_indexes = [key_to_index[sanitize_header_name(k)] for k in dedup_keys]
            except KeyError as e:
                raise SystemExit(f"Dedup key not found after sanitization: {e.args[0]}")
        else:
            dedup_indexes = None  # use entire row

    # Second pass: read, clean, write
    os.makedirs(os.path.dirname(out_path) or '.', exist_ok=True)
    if progress:
        progress("Reading rows and applying cleaning rules", 0.45)
    with open(in_path, "r", encoding=encoding, errors="replace", newline="") as rf, \
         open(out_path, "w", encoding="utf-8", newline="") as wf:
        reader = csv.reader(rf, dialect)
        writer = csv.writer(wf, dialect)

        # write header
        writer.writerow(header_out)

        for i, row in enumerate(reader):
            if i == 0:
                stats.rows_in += 1
                continue  # header already handled

            stats.rows_in += 1

            # Normalize row length
            if len(row) < width:
                if pad_rows == "pad":
                    row = row + [""] * (width - len(row))
                elif pad_rows == "truncate":
                    # Keep as-is (short); write available cells only
                    row = row[:width]
                elif pad_rows == "error":
                    raise SystemExit(f"Row {i} shorter than header width {width}")
            elif len(row) > width:
                if pad_rows == "truncate":
                    row = row[:width]
                elif pad_rows == "pad":
                    # expand header on the fly if needed (rare after first pass)
                    extra_n = len(row) - width
                    header_out.extend([f"extra_{len(header_out)+j+1}" for j in range(extra_n)])
                    width = len(row)
                elif pad_rows == "error":
                    raise SystemExit(f"Row {i} longer than header width {width}")

            # Trim cells
            if trim_cells:
                row = [_normalize_whitespace(str(x)) if isinstance(x, str) else str(x) for x in row]

            # Apply NA normalization, conversion, and fill
            for j in range(width):
                val = row[j]
                sval = str(val)
                sval_norm = sval.strip()
                is_missing = (sval_norm == "" or sval_norm.lower() in na_set)

                if is_missing:
                    if j in fill_by_idx:
                        row[j] = fill_by_idx[j]
                    else:
                        row[j] = ""
                    continue

                t = types_by_idx.get(j, "string")
                if t == "integer":
                    v, kind = parse_numeric(sval_norm)
                    if isinstance(v, int):
                        row[j] = v
                    elif isinstance(v, float) and v.is_integer():
                        row[j] = int(v)
                    else:
                        row[j] = sval_norm
                elif t == "float":
                    v, kind = parse_numeric(sval_norm)
                    if isinstance(v, (int, float)):
                        row[j] = float(v)
                    else:
                        row[j] = sval_norm
                elif t == "boolean":
                    b = parse_bool(sval_norm)
                    row[j] = "true" if b is True else ("false" if b is False else sval_norm)
                elif t == "date" and parse_dates:
                    dt = parse_date_str(sval_norm)
                    row[j] = dt.strftime(date_format) if dt else sval_norm
                else:
                    row[j] = sval_norm

            # Drop fully-empty rows
            if drop_empty_rows:
                if all((c == "" for c in row)):
                    stats.empty_rows_dropped += 1
                    continue

            # Duplicate handling
            if drop_duplicates:
                if dedup_indexes is None:
                    key_tuple = tuple(row)
                else:
                    key_tuple = tuple(row[idx] if idx < len(row) else "" for idx in dedup_indexes)
                if key_tuple in seen:
                    stats.duplicate_rows_dropped += 1
                    continue
                seen.add(key_tuple)

            writer.writerow(row)
            stats.rows_out += 1

    # Optionally remove empty columns by rewriting file (third pass)
    if remove_empty_columns:
        if progress:
            progress("Removing empty columns", 0.9)
        # Identify non-empty columns
        non_empty_cols: List[bool] = [False] * width
        with open(out_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.reader(f, dialect)
            for i, row in enumerate(reader):
                if i == 0:
                    continue
                for j, val in enumerate(row):
                    if val != "":
                        non_empty_cols[j] = True
        # If any empty-only columns, rewrite
        if not all(non_empty_cols):
            keep_idx = [j for j, keep in enumerate(non_empty_cols) if keep]
            tmp_path = out_path + ".tmp"
            with open(out_path, "r", encoding="utf-8", newline="") as rf, \
                 open(tmp_path, "w", encoding="utf-8", newline="") as wf:
                reader = csv.reader(rf, dialect)
                writer = csv.writer(wf, dialect)
                for i, row in enumerate(reader):
                    kept = [row[j] for j in keep_idx if j < len(row)]
                    writer.writerow(kept)
            os.replace(tmp_path, out_path)
            stats.header_out = [stats.header_out[j] for j in keep_idx]

    if progress:
        progress("Done", 1.0)
    return stats


def clean_file_pandas(
    in_path: str,
    out_path: str,
    *,
    delimiter: Optional[str] = None,
    trim_cells: bool = True,
    drop_empty_rows: bool = True,
    drop_duplicates: bool = False,
    dedup_keys: Optional[List[str]] = None,
    remove_empty_columns: bool = False,
    infer_types: bool = False,
    parse_dates: bool = False,
    date_format: str = "%Y-%m-%d",
    type_threshold: float = 0.9,
    fill_missing: str = "none",
    fill_constant: str = "",
    na_tokens: Optional[List[str]] = None,
    progress: Optional[Callable[[str, Optional[float]], None]] = None,
) -> CleanStats:
    try:
        import pandas as pd  # type: ignore
    except Exception:
        raise SystemExit("Pandas engine requested but pandas is not installed. Try: pip install pandas")

    # Encoding + delimiter detection
    if progress:
        progress("Detecting encoding and delimiter", 0.02)
    sample, encoding = read_text(in_path)
    dialect = try_sniff_dialect(sample, delimiter)
    sep = delimiter if delimiter is not None else getattr(dialect, "delimiter", ",")

    # Read as strings, let us control NA
    if progress:
        progress("Reading file", 0.12)
    try:
        df = pd.read_csv(in_path, sep=sep, dtype=str, keep_default_na=False, na_values=[], encoding=encoding)
    except Exception:
        if progress:
            progress("Parser failed; retrying with robust parser", 0.15)
        # Fallback to Python engine which is more tolerant of odd quoting
        df = pd.read_csv(
            in_path,
            sep=sep,
            dtype=str,
            keep_default_na=False,
            na_values=[],
            encoding=encoding,
            engine="python",
        )
    orig_len = len(df)

    header_in = list(df.columns)
    header_out = sanitize_headers(header_in)
    rename_map = {old: new for old, new in zip(header_in, header_out)}
    df = df.rename(columns=rename_map)
    if progress:
        progress("Standardized headers", 0.2)

    # Normalize whitespace
    if trim_cells:
        df = df.applymap(lambda x: _normalize_whitespace(x) if isinstance(x, str) else x)

    # Build NA set
    na_set = set(["na", "n/a", "null", "none", "#n/a", "-", "?", "nan", ""])  # include empty
    if na_tokens:
        for tkn in na_tokens:
            if tkn is not None:
                na_set.add(str(tkn).strip().lower())

    # Helper: is missing
    def is_missing_val(x: Any) -> bool:
        if x is None:
            return True
        s = str(x).strip().lower()
        return s in na_set

    # Type analysis
    types_by_col: Dict[str, str] = {c: "string" for c in df.columns}
    if infer_types or parse_dates:
        if progress:
            progress("Analyzing column types and computing fill values", 0.35)
        for c in df.columns:
            nonempty = 0
            num = 0
            intval = 0
            floatval = 0
            boolv = 0
            datev = 0
            num_vals: List[float] = []
            for v in df[c].tolist():
                if is_missing_val(v):
                    continue
                nonempty += 1
                s = str(v)
                vb, vn = parse_bool(s), parse_numeric(s)[0]
                if vn is not None:
                    num += 1
                    if isinstance(vn, int):
                        intval += 1
                        num_vals.append(float(vn))
                    else:
                        floatval += 1
                        num_vals.append(float(vn))
                if vb is not None:
                    boolv += 1
                if parse_dates and parse_date_str(s) is not None:
                    datev += 1
            if nonempty > 0:
                num_ratio = num / nonempty
                bool_ratio = boolv / nonempty
                date_ratio = datev / nonempty
                if infer_types and num_ratio >= type_threshold:
                    types_by_col[c] = "float" if floatval > 0 else "integer"
                elif infer_types and bool_ratio >= type_threshold:
                    types_by_col[c] = "boolean"
                elif parse_dates and date_ratio >= type_threshold:
                    types_by_col[c] = "date"
                else:
                    types_by_col[c] = "string"

    # Compute fill values
    fill_by_col: Dict[str, Any] = {}
    if fill_missing == "empty":
        fill_by_col = {c: "" for c in df.columns}
    elif fill_missing == "constant":
        fill_by_col = {c: fill_constant for c in df.columns}
    elif fill_missing == "zero":
        for c, t in types_by_col.items():
            if t in ("integer", "float"):
                fill_by_col[c] = 0
    elif fill_missing in ("mean", "median"):
        for c, t in types_by_col.items():
            if t in ("integer", "float"):
                vals: List[float] = []
                for v in df[c].tolist():
                    if is_missing_val(v):
                        continue
                    numv, _k = parse_numeric(str(v))
                    if isinstance(numv, (int, float)):
                        vals.append(float(numv))
                if vals:
                    if fill_missing == "mean":
                        m = sum(vals) / len(vals)
                        fill_by_col[c] = int(round(m)) if t == "integer" and float(m).is_integer() else m
                    else:
                        vs = sorted(vals)
                        n = len(vs)
                        med = vs[n//2] if n % 2 == 1 else (vs[n//2 - 1] + vs[n//2]) / 2.0
                        fill_by_col[c] = int(round(med)) if t == "integer" and float(med).is_integer() else med
    elif fill_missing == "mode":
        for c in df.columns:
            counts: Counter = Counter()
            for v in df[c].tolist():
                if is_missing_val(v):
                    continue
                counts[str(v)] += 1
            if counts:
                fill_by_col[c] = counts.most_common(1)[0][0]

    # Convert and fill
    def convert_cell(col: str, val: Any) -> Any:
        if is_missing_val(val):
            return fill_by_col.get(col, "")
        t = types_by_col.get(col, "string")
        s = str(val)
        if t == "integer":
            v, _k = parse_numeric(s)
            if isinstance(v, int):
                return v
            if isinstance(v, float) and v.is_integer():
                return int(v)
            return s
        if t == "float":
            v, _k = parse_numeric(s)
            if isinstance(v, (int, float)):
                return float(v)
            return s
        if t == "boolean":
            b = parse_bool(s)
            return "true" if b is True else ("false" if b is False else s)
        if t == "date" and parse_dates:
            dt = parse_date_str(s)
            return dt.strftime(date_format) if dt else s
        return s

    if progress:
        progress("Converting and filling cells", 0.45)
    for c in df.columns:
        df[c] = [convert_cell(c, v) for v in df[c].tolist()]

    # Drop fully-empty rows
    empty_rows_dropped = 0
    if drop_empty_rows:
        before = len(df)
        mask_all_empty = df.apply(lambda r: all((str(x) == "" for x in r)), axis=1)
        df = df.loc[~mask_all_empty].copy()
        empty_rows_dropped = before - len(df)

    # Remove empty columns
    if remove_empty_columns:
        if progress:
            progress("Removing empty columns", 0.9)
        non_empty_cols = [c for c in df.columns if any(str(x) != "" for x in df[c].tolist())]
        df = df[non_empty_cols]
        header_out = non_empty_cols

    # Deduplicate
    duplicate_rows_dropped = 0
    if drop_duplicates:
        if dedup_keys:
            keys = [sanitize_header_name(k) for k in dedup_keys]
            for k in keys:
                if k not in df.columns:
                    raise SystemExit(f"Dedup key not found after sanitization: {k}")
            before = len(df)
            df = df.drop_duplicates(subset=keys, keep="first")
            duplicate_rows_dropped = before - len(df)
        else:
            before = len(df)
            df = df.drop_duplicates(keep="first")
            duplicate_rows_dropped = before - len(df)

    # Save
    os.makedirs(os.path.dirname(out_path) or '.', exist_ok=True)
    if progress:
        progress("Saving output", 0.95)
    df.to_csv(out_path, index=False, sep=sep, encoding="utf-8")

    stats = CleanStats(
        input_path=in_path,
        output_path=out_path,
        rows_in=orig_len + 1,  # include header for consistency with csv engine
        rows_out=len(df),
        empty_rows_dropped=empty_rows_dropped,
        duplicate_rows_dropped=duplicate_rows_dropped,
        header_in=header_in,
        header_out=header_out,
        delimiter=sep,
        encoding_read=encoding,
        pad_mode="n/a",
        infer_types=infer_types,
        parse_dates=parse_dates,
        date_format=date_format,
        type_threshold=type_threshold,
        fill_missing=fill_missing,
        column_types={c: types_by_col.get(c, "string") for c in header_out},
    )

    if progress:
        progress("Done", 1.0)
    return stats


def resolve_input_paths(patterns: List[str]) -> List[str]:
    files: List[str] = []
    for p in patterns:
        # If it's a directory, include *.csv inside it
        if os.path.isdir(p):
            files.extend(glob.glob(os.path.join(p, "*.csv")))
        else:
            # Glob pattern or direct file
            matches = glob.glob(p)
            if matches:
                files.extend(matches)
            else:
                files.append(p)
    # Deduplicate while preserving order
    seen = set()
    unique_files = []
    for f in files:
        ab = os.path.abspath(f)
        if ab not in seen and os.path.isfile(ab):
            seen.add(ab)
            unique_files.append(ab)
    return unique_files


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Clean CSV files with sensible defaults.")
    p.add_argument("--input", "-i", nargs="+", required=False, default=None, help="Input file(s), folder(s), or glob(s). If omitted, opens a file picker.")
    p.add_argument("--output-dir", "-o", default=None, help="Directory to write cleaned files")
    p.add_argument("--inplace", action="store_true", help="Overwrite files in place")
    p.add_argument("--suffix", default="_cleaned", help="Suffix for cleaned files (when not inplace)")
    p.add_argument("--delimiter", default=None, help="Force delimiter (e.g. , ; \t)")
    p.add_argument("--no-trim", dest="trim", action="store_false", help="Disable cell trimming")
    p.add_argument("--keep-empty-rows", dest="drop_empty", action="store_false", help="Do not drop fully-empty rows")
    p.add_argument("--drop-duplicates", action="store_true", help="Remove duplicate rows")
    p.add_argument("--dedup-keys", nargs="*", help="Column names to define duplicates (after sanitization)")
    p.add_argument("--pad-rows", choices=["pad", "truncate", "error"], default="pad", help="How to handle uneven rows")
    p.add_argument("--remove-empty-columns", action="store_true", help="Remove columns that are empty for all rows")
    p.add_argument("--no-log", dest="write_log", action="store_false", help="Do not write per-file log")
    p.add_argument("--engine", choices=["csv", "pandas"], default="csv", help="Processing engine (default csv)")
    p.add_argument("--ask-output", action="store_true", help="Ask where to save the cleaned file(s)")
    # new features
    p.add_argument("--infer-types", action="store_true", help="Infer integers/floats/booleans and normalize values")
    p.add_argument("--parse-dates", action="store_true", help="Parse date-like columns and format them uniformly")
    p.add_argument("--date-format", default="%Y-%m-%d", help="Output format for parsed dates (default %%Y-%%m-%%d)")
    p.add_argument("--type-threshold", type=float, default=0.9, help="Confidence threshold for type inference (0-1)")
    p.add_argument(
        "--fill-missing",
        choices=["none", "empty", "constant", "zero", "mean", "median", "mode"],
        default="none",
        help="Strategy to fill missing values",
    )
    p.add_argument("--fill-constant", default="", help="Constant value when --fill-missing=constant")
    p.add_argument("--na", dest="na_tokens", action="append", help="Additional tokens to treat as missing")
    return p.parse_args()


def interactive_select_inputs() -> List[str]:
    # Try a GUI file picker first
    try:
        from tkinter import Tk, filedialog  # type: ignore
        root = Tk()
        root.withdraw()
        paths = filedialog.askopenfilenames(
            title="Select CSV file(s)",
            filetypes=[
                ("CSV/TSV files", ("*.csv", "*.tsv")),
                ("Text files", ("*.txt",)),
                ("All files", ("*.*",)),
            ],
        )
        root.update()
        root.destroy()
        if paths:
            return list(paths)
    except Exception:
        pass

    # Fallback to console prompt
    try:
        if sys.stdin and sys.stdin.isatty():
            raw = input("Enter file paths or globs (space-separated): ").strip()
            return raw.split() if raw else []
    except Exception:
        pass
    return []


def interactive_select_output_single(suggested_path: str) -> Optional[str]:
    # GUI save-as dialog first
    try:
        from tkinter import Tk, filedialog  # type: ignore
        root = Tk()
        root.withdraw()
        base, ext = os.path.splitext(os.path.basename(suggested_path))
        initialdir = os.path.dirname(suggested_path) or os.getcwd()
        picked = filedialog.asksaveasfilename(
            title="Save cleaned file as...",
            initialdir=initialdir,
            initialfile=os.path.basename(suggested_path),
            defaultextension=ext or ".csv",
            filetypes=[
                ("CSV files", ("*.csv",)),
                ("TSV files", ("*.tsv",)),
                ("All files", ("*.*",)),
            ],
        )
        root.update(); root.destroy()
        if picked:
            # Ensure extension if user removed it
            if not os.path.splitext(picked)[1] and ext:
                picked = picked + ext
            return picked
    except Exception:
        pass
    # Console fallback
    try:
        if sys.stdin and sys.stdin.isatty():
            outp = input(f"Output path [default: {suggested_path}]: ").strip()
            return outp or suggested_path
    except Exception:
        pass
    return None


def interactive_select_output_dir(suggested_dir: str) -> Optional[str]:
    try:
        from tkinter import Tk, filedialog  # type: ignore
        root = Tk(); root.withdraw()
        initialdir = suggested_dir or os.getcwd()
        picked = filedialog.askdirectory(title="Select output folder for cleaned files", initialdir=initialdir)
        root.update(); root.destroy()
        if picked:
            return picked
    except Exception:
        pass
    try:
        if sys.stdin and sys.stdin.isatty():
            outd = input(f"Output folder [default: {suggested_dir or os.getcwd()}]: ").strip()
            return outd or suggested_dir or os.getcwd()
    except Exception:
        pass
    return None


def build_output_path(in_path: str, output_dir: Optional[str], suffix: str, inplace: bool) -> str:
    if inplace:
        return in_path
    base = os.path.basename(in_path)
    stem, ext = os.path.splitext(base)
    out_name = f"{stem}{suffix}{ext or '.csv'}"
    directory = output_dir or os.path.dirname(in_path)
    return os.path.join(directory, out_name)


def main() -> None:
    args = parse_args()
    raw_inputs = args.input
    if not raw_inputs:
        # Interactive fallback for double-click usage
        picked = interactive_select_inputs()
        raw_inputs = picked if picked else None
    inputs = resolve_input_paths(raw_inputs or [])
    if not inputs:
        print("No input files provided or selected. Exiting.")
        return

    # Interactive output selection when requested or when inputs were selected interactively
    used_interactive_picker = args.input is None
    out_dir_override: Optional[str] = None
    out_path_override: Optional[str] = None
    if not args.inplace:
        if len(inputs) == 1:
            # If --ask-output or we picked inputs interactively without an explicit output-dir, ask for a save-as path
            if args.ask_output or (used_interactive_picker and not args.output_dir):
                # Build a suggested path from current defaults
                suggested = build_output_path(inputs[0], args.output_dir, args.suffix, inplace=False)
                chosen = interactive_select_output_single(suggested)
                if not chosen:
                    print("No output path chosen. Exiting.")
                    return
                out_path_override = chosen
        else:
            # Multiple inputs: choose an output directory if asked or interactive with no output-dir
            if args.ask_output or (used_interactive_picker and not args.output_dir):
                suggested_dir = args.output_dir or os.path.dirname(inputs[0])
                outd = interactive_select_output_dir(suggested_dir)
                if not outd:
                    print("No output folder chosen. Exiting.")
                    return
                out_dir_override = outd

    results: List[CleanStats] = []
    for idx, in_path in enumerate(inputs):
        # Determine output path honoring overrides
        if args.inplace:
            out_path = in_path
        elif out_path_override and len(inputs) == 1:
            out_path = out_path_override
        else:
            dest_dir = out_dir_override or args.output_dir
            out_path = build_output_path(in_path, dest_dir, args.suffix, inplace=False)
        if args.engine == "csv":
            stats = clean_file(
                in_path,
                out_path,
                delimiter=args.delimiter,
                trim_cells=args.trim,
                drop_empty_rows=args.drop_empty,
                drop_duplicates=args.drop_duplicates,
                dedup_keys=args.dedup_keys,
                pad_rows=args.pad_rows,
                remove_empty_columns=args.remove_empty_columns,
                infer_types=args.infer_types,
                parse_dates=args.parse_dates,
                date_format=args.date_format,
                type_threshold=args.type_threshold,
                fill_missing=args.fill_missing,
                fill_constant=args.fill_constant,
                na_tokens=args.na_tokens,
            )
        else:
            stats = clean_file_pandas(
                in_path,
                out_path,
                delimiter=args.delimiter,
                trim_cells=args.trim,
                drop_empty_rows=args.drop_empty,
                drop_duplicates=args.drop_duplicates,
                dedup_keys=args.dedup_keys,
                remove_empty_columns=args.remove_empty_columns,
                infer_types=args.infer_types,
                parse_dates=args.parse_dates,
                date_format=args.date_format,
                type_threshold=args.type_threshold,
                fill_missing=args.fill_missing,
                fill_constant=args.fill_constant,
                na_tokens=args.na_tokens,
            )
        results.append(stats)

        if args.write_log:
            base, _ = os.path.splitext(out_path)
            log_path = base + "_log.txt"
            write_log(stats, log_path)

        print(f"Cleaned: {in_path} -> {out_path} (rows: {stats.rows_in-1} -> {stats.rows_out})")


if __name__ == "__main__":
    main()
