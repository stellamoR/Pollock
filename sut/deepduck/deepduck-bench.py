import sys
import os
from os.path import join, abspath, dirname

# make sure this script can be invoked from anywhere by finding repo root
REPO_ROOT = abspath(join(dirname(__file__), '..', '..'))
sys.path.insert(0, join(REPO_ROOT, 'sut'))

import time
import duckdb
import pandas as pd
from utils import print, save_time_df
from solution import repair_rows

sut = 'deepduck'
DATASET = os.environ.get('DATASET', 'polluted_files')
IN_DIR = join(REPO_ROOT, DATASET, 'csv')
OUT_DIR = join(REPO_ROOT, 'results', sut, DATASET, 'loading')
TIME_DIR = join(REPO_ROOT, 'results', sut, DATASET)

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(TIME_DIR, exist_ok=True)

# Number of successfully parsed rows passed to the LLM as schema context.
N_CONTEXT_ROWS = 5

N_REPETITIONS = 1


times_dict = {}
benchmark_files = os.listdir(IN_DIR)
for idx, file in enumerate(benchmark_files):
    f = os.path.basename(file)
    in_filepath = join(IN_DIR, f)
    out_filename = f'{f}_converted.csv'
    out_filepath = join(OUT_DIR, out_filename)
    if os.path.exists(out_filepath):
        continue
    print(f"({idx}/{len(benchmark_files)}) {f}")

    for time_rep in range(N_REPETITIONS):
        con = duckdb.connect()
        start = time.time()
        try:
            # Sniff schema from a small sample so outlier rows don't widen the schema.
            cols_list = con.execute(
                "SELECT Columns FROM sniff_csv(?, sample_size=8)", [in_filepath]
            ).fetchone()[0]
            sniffed_columns = {col["name"]: col["type"] for col in cols_list}

            # Force the sniffed schema; rows that don't fit go to reject_errors.
            # No null_padding so short rows are rejected rather than silently padded.
            rel = con.read_csv(in_filepath, columns=sniffed_columns, store_rejects=True)
            df = rel.df()

            # Each rejected row may produce multiple entries (one per bad column),
            # so we deduplicate on line number and keep the raw csv_line text.
            rejected = con.execute(
                "SELECT DISTINCT line, csv_line FROM reject_errors ORDER BY line"
            ).df()

            if not rejected.empty:
                print(f"  {len(rejected)} rejected row(s)")
                print(rejected)
                pass
                failed_lines = rejected["csv_line"].tolist()
                context_rows = df.head(N_CONTEXT_ROWS)
                repaired_df = repair_rows(failed_lines, context_rows)
                df = pd.concat([df, repaired_df], ignore_index=True)

            end = time.time()
            df.to_csv(out_filepath, index=False)
        except Exception as e:
            end = time.time()
            print("\t", e)
            with open(out_filepath, "w") as text_file:
                text_file.write("Application Error\n")
                text_file.write(str(e))
            print(str(e))

        times_dict[f] = times_dict.get(f, []) + [(end - start)]

        try:
            del start, end, df, text_file
        except:
            pass

save_time_df(TIME_DIR, sut, times_dict)
