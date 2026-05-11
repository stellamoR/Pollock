#!/usr/bin/env python3
"""Find files where a given SUT performed poorly (errors or low F1 scores)."""

import argparse
import csv
import json
import os
import re
import sys
from collections import Counter

import pandas as pd


def _counter_subtract(a: Counter, b: Counter) -> Counter:
    """Return elements in a that are not covered by b (multiset difference)."""
    result = Counter()
    for k, v in a.items():
        diff = v - b.get(k, 0)
        if diff > 0:
            result[k] = diff
    return result


POLLUTION_PATTERNS = [
    (r"file_no_payload",                "Empty file (0 bytes)"),
    (r"file_no_trailing_newline",        "Missing trailing newline"),
    (r"file_double_trailing_newline",    "Double trailing newline"),
    (r"file_no_header",                  "No header row"),
    (r"file_header_multirow_(\d+)",      "Multi-row header ({0} rows)"),
    (r"file_header_only",                "Header row only, no data"),
    (r"file_one_data_row",               "Single data row"),
    (r"file_preamble",                   "Preamble rows before header"),
    (r"file_multitable_less",            "Two tables, first has fewer columns"),
    (r"file_multitable_more",            "Two tables, first has more columns"),
    (r"file_multitable_same",            "Two tables with the same number of columns"),
    (r"file_field_delimiter_(0x\w+)",    "Non-standard field delimiter ({0})"),
    (r"file_quotation_char_(0x\w+)",     "Non-standard quotation character ({0})"),
    (r"file_escape_char_(0x\w+)",        "Non-standard escape character ({0})"),
    (r"file_record_delimiter_(0x\w+)",   "Non-standard record delimiter ({0})"),
    (r"row_extra_quote(\d+)_col(\d+)",   "Extra unescaped quote in row {0}, column {1}"),
    (r"row_field_delimiter_(\d+)_",      "Row {0} uses space as field delimiter (opposed to the correct delimiter defined by the grammar)"),
    (r"row_less_sep_row(\d+)_col(\d+)",  "Missing delimiter in row {0} at column {1}"),
    (r"row_more_sep_row(\d+)_col(\d+)",  "Extra delimiter in row {0} at column {1}"),
]


def pollution_type(filename):
    stem = filename.removesuffix(".csv")
    for pattern, description in POLLUTION_PATTERNS:
        m = re.match(pattern, stem)
        if m:
            return description.format(*m.groups())
    return "Unknown"


def read_csv_rows(path):
    """Read a CSV (clean/converted format: comma-delimited, double-quote) into list of rows."""
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f, delimiter=",", quotechar='"', doublequote=True)
            return [row for row in reader]
    except Exception:
        with open(path, "rb") as f:
            raw = f.read()
        import chardet
        enc = chardet.detect(raw)["encoding"] or "utf-8"
        with open(path, "r", encoding=enc) as f:
            reader = csv.reader(f, delimiter=",", quotechar='"', doublequote=True)
            return [row for row in reader]


def read_polluted_lines(path, n=5):
    """Read first n raw lines of a polluted input file."""
    try:
        with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
            return [f.readline().rstrip("\r\n") for _ in range(n)]
    except Exception as e:
        return [f"(could not read: {e})"]


def load_params(params_path):
    try:
        with open(params_path) as f:
            return json.load(f)
    except Exception:
        return {}


def format_params(params):
    keys = ["delimiter", "quotechar", "escapechar", "row_delimiter",
            "encoding", "header_lines", "preamble_lines", "n_columns"]
    parts = []
    for k in keys:
        if k in params:
            v = params[k]
            parts.append(f"{k}={repr(v)}")
    return ", ".join(parts)


def diff_rows(clean_rows, loaded_rows, max_examples=3):
    """
    Return a diagnostic dict comparing clean vs loaded row lists.
    Both lists include the header as row 0.
    """
    diag = {}

    # Header comparison
    clean_header = clean_rows[0] if clean_rows else []
    loaded_header = loaded_rows[0] if loaded_rows else []
    if clean_header != loaded_header:
        diag["header_expected"] = clean_header
        diag["header_got"] = loaded_header

    # Row / column counts
    clean_data = clean_rows[1:] if len(clean_rows) > 1 else []
    loaded_data = loaded_rows[1:] if len(loaded_rows) > 1 else []
    diag["expected_rows"] = len(clean_data)
    diag["loaded_rows"] = len(loaded_data)

    if clean_data:
        diag["expected_cols"] = len(clean_data[0])
    if loaded_data:
        diag["loaded_cols"] = len(loaded_data[0])

    # Multiset diff on records (same logic as metrics.py)
    clean_records = Counter("||".join(r) for r in clean_data)
    loaded_records = Counter("||".join(r) for r in loaded_data)

    missing = _counter_subtract(clean_records, loaded_records)
    extra   = _counter_subtract(loaded_records, clean_records)

    if missing:
        diag["missing_count"] = sum(missing.values())
        diag["missing_examples"] = [r.split("||") for r in list(missing)[:max_examples]]
    if extra:
        diag["extra_count"] = sum(extra.values())
        diag["extra_examples"] = [r.split("||") for r in list(extra)[:max_examples]]

    return diag


def write_file_section(f, filename, scores, diag, polluted_lines, params, poll_type):
    sep = "-" * 70
    f.write(f"\n{sep}\n")
    f.write(f"FILE: {filename}\n")
    f.write(f"POLLUTION: {poll_type}\n")
    if params:
        f.write(f"DIALECT: {format_params(params)}\n")

    success = scores.get("success", "?")
    f.write(f"SUCCESS: {success}  |  "
            f"header_f1={scores.get('header_f1', 0):.4f}  "
            f"record_f1={scores.get('record_f1', 0):.4f}  "
            f"cell_f1={scores.get('cell_f1', 0):.4f}\n")

    if success == 0:
        f.write("\n  SUT failed to load the file.\n")
        if polluted_lines:
            f.write("  First lines of polluted input:\n")
            for line in polluted_lines:
                f.write(f"    {line}\n")
        return

    if diag is None:
        return

    # Header mismatch
    if "header_expected" in diag:
        f.write("\n  HEADER MISMATCH:\n")
        f.write(f"    Expected: {diag['header_expected']}\n")
        f.write(f"    Got:      {diag['header_got']}\n")

    # Row / column counts
    er, lr = diag.get("expected_rows"), diag.get("loaded_rows")
    ec, lc = diag.get("expected_cols"), diag.get("loaded_cols")
    if er is not None and lr is not None:
        row_note = "" if er == lr else f"  ← expected {er}"
        f.write(f"\n  ROWS: loaded {lr}{row_note}\n")
    if ec is not None and lc is not None and ec != lc:
        f.write(f"  COLS: expected {ec}, got {lc} (first data row)\n")

    # Missing records
    if "missing_count" in diag:
        cnt = diag["missing_count"]
        f.write(f"\n  MISSING RECORDS ({cnt} record(s) present in clean but absent in loaded output):\n")
        for ex in diag["missing_examples"]:
            f.write(f"    {ex}\n")
        if cnt > len(diag["missing_examples"]):
            f.write(f"    ... and {cnt - len(diag['missing_examples'])} more\n")

    # Extra records
    if "extra_count" in diag:
        cnt = diag["extra_count"]
        f.write(f"\n  EXTRA RECORDS ({cnt} record(s) in loaded output not in clean file):\n")
        for ex in diag["extra_examples"]:
            f.write(f"    {ex}\n")
        if cnt > len(diag["extra_examples"]):
            f.write(f"    ... and {cnt - len(diag['extra_examples'])} more\n")


def main():
    parser = argparse.ArgumentParser(
        description="Find files where a SUT did not perform well."
    )
    parser.add_argument("sut", help="Name of the SUT (e.g. pandas, duckdbparse)")
    parser.add_argument(
        "--dataset", default="polluted_files",
        help="Dataset to examine (default: polluted_files)"
    )
    parser.add_argument(
        "--threshold", type=float, default=0.99,
        help="Cell F1 score below which a file is considered a poor result (default: 0.99)"
    )
    parser.add_argument(
        "--results-dir", default="results",
        help="Root results directory (default: results)"
    )
    parser.add_argument(
        "--polluted-dir", default="data/polluted_files",
        help="Root polluted files directory (default: data/polluted_files)"
    )
    parser.add_argument(
        "--output", default=None,
        help="Output file path (default: {sut}_errors.txt)"
    )
    args = parser.parse_args()

    sut = args.sut
    results_csv = os.path.join(args.results_dir, sut, args.dataset, f"{sut}_results.csv")
    loading_dir = os.path.join(args.results_dir, sut, args.dataset, "loading")
    clean_dir   = os.path.join(args.polluted_dir, "clean")
    csv_dir     = os.path.join(args.polluted_dir, "csv")
    params_dir  = os.path.join(args.polluted_dir, "parameters")

    if not os.path.exists(results_csv):
        sys.exit(f"Error: results file not found at {results_csv}")

    df = pd.read_csv(results_csv)

    success_col   = f"{sut}_success"
    cell_f1_col   = f"{sut}_cell_f1"
    record_f1_col = f"{sut}_record_f1"
    header_f1_col = f"{sut}_header_f1"

    missing_cols = [c for c in [success_col, cell_f1_col] if c not in df.columns]
    if missing_cols:
        sys.exit(f"Error: expected columns not found: {missing_cols}")

    failed = df[df[success_col] == 0]
    poor   = df[(df[success_col] == 1) & (df[cell_f1_col] < args.threshold)]

    output_path = args.output or f"{sut}_errors.txt"

    with open(output_path, "w") as out:
        out.write(f"SUT: {sut}\n")
        out.write(f"Dataset: {args.dataset}\n")
        out.write(f"Results file: {results_csv}\n")
        out.write(f"Cell F1 threshold: {args.threshold}\n")
        out.write(f"Total files evaluated: {len(df)}\n")
        out.write(f"Failed to load: {len(failed)}\n")
        out.write(f"Poor results (cell F1 < {args.threshold}): {len(poor)}\n")

        # --- Failed to load ---
        out.write(f"\n{'='*70}\n")
        out.write(f"FAILED TO LOAD ({len(failed)} files)\n")
        out.write(f"{'='*70}\n")

        for _, row in failed.iterrows():
            fname = row["file"]
            scores = {
                "success": int(row[success_col]),
                "header_f1": row.get(header_f1_col, 0),
                "record_f1": row.get(record_f1_col, 0),
                "cell_f1": row.get(cell_f1_col, 0),
            }
            params = load_params(os.path.join(params_dir, fname + "_parameters.json"))
            polluted_lines = read_polluted_lines(os.path.join(csv_dir, fname))
            write_file_section(out, fname, scores, None, polluted_lines, params, pollution_type(fname))

        # --- Poor results ---
        out.write(f"\n{'='*70}\n")
        out.write(f"POOR RESULTS — cell F1 < {args.threshold} ({len(poor)} files)\n")
        out.write(f"{'='*70}\n")

        for _, row in poor.sort_values(cell_f1_col).iterrows():
            fname = row["file"]
            scores = {
                "success": int(row[success_col]),
                "header_f1": row.get(header_f1_col, 0),
                "record_f1": row.get(record_f1_col, 0),
                "cell_f1": row.get(cell_f1_col, 0),
            }
            params = load_params(os.path.join(params_dir, fname + "_parameters.json"))

            clean_path   = os.path.join(clean_dir, fname)
            loaded_path  = os.path.join(loading_dir, fname + "_converted.csv")

            diag = None
            if os.path.exists(clean_path) and os.path.exists(loaded_path):
                clean_rows  = read_csv_rows(clean_path)
                loaded_rows = read_csv_rows(loaded_path)
                diag = diff_rows(clean_rows, loaded_rows)

            write_file_section(out, fname, scores, diag, None, params, pollution_type(fname))

    print(f"Results written to {output_path}")
    print(f"  Failed to load:           {len(failed)}")
    print(f"  Poor (cell F1 < {args.threshold}): {len(poor)}")


if __name__ == "__main__":
    main()