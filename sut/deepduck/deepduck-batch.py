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
from solution import repair_batch

sut = 'deepduck'
DATASET = os.environ.get('DATASET', 'polluted_files')
IN_DIR = join(REPO_ROOT, DATASET, 'csv')
OUT_DIR = join(REPO_ROOT, 'results', sut, DATASET, 'loading')
TIME_DIR = join(REPO_ROOT, 'results', sut, DATASET)

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(TIME_DIR, exist_ok=True)

# Accumulate this many failed lines before making one LLM call.
BATCH_SIZE = int(os.environ.get('BATCH_SIZE', '50'))

N_CONTEXT_ROWS = 5

times_dict = {}
# Each entry: (filename, out_filepath, successful_df, failed_lines, context_rows, duck_time)
pending = []
pending_line_count = 0


def flush_pending():
    global pending, pending_line_count
    if not pending:
        return

    batch_input = [(f, failed_lines, ctx) for f, _, _, failed_lines, ctx, _ in pending]
    repaired_map = repair_batch(batch_input)

    for f, out_filepath, successful_df, _, ctx, duck_time in pending:
        repaired_df = repaired_map.get(f, pd.DataFrame(columns=list(ctx.columns)))
        pd.concat([successful_df, repaired_df], ignore_index=True).to_csv(out_filepath, index=False)
        times_dict[f] = times_dict.get(f, []) + [duck_time]

    pending = []
    pending_line_count = 0


benchmark_files = os.listdir(IN_DIR)
for idx, file in enumerate(benchmark_files):
    f = os.path.basename(file)
    in_filepath = join(IN_DIR, f)
    out_filename = f'{f}_converted.csv'
    out_filepath = join(OUT_DIR, out_filename)
    if os.path.exists(out_filepath):
        continue
    print(f"({idx}/{len(benchmark_files)}) {f}")

    con = duckdb.connect()
    start = time.time()
    try:
        kw = {}
        kw["strict_mode"] = False
        kw["null_padding"] = True
        kw["store_rejects"] = True

        rel = con.read_csv(in_filepath, **kw)
        df = rel.df()

        rejected = con.execute(
            "SELECT DISTINCT line, csv_line FROM reject_errors ORDER BY line"
        ).df()
        duck_time = time.time() - start

        if rejected.empty:
            df.to_csv(out_filepath, index=False)
            times_dict[f] = times_dict.get(f, []) + [duck_time]
        else:
            failed_lines = rejected["csv_line"].tolist()
            context_rows = df.head(N_CONTEXT_ROWS)
            pending.append((f, out_filepath, df, failed_lines, context_rows, duck_time))
            pending_line_count += len(failed_lines)

            if pending_line_count >= BATCH_SIZE:
                flush_pending()

    except Exception as e:
        duck_time = time.time() - start
        print("\t", e)
        with open(out_filepath, "w") as text_file:
            text_file.write("Application Error\n")
            text_file.write(str(e))
        times_dict[f] = times_dict.get(f, []) + [duck_time]

# Flush any remaining files that didn't fill a full batch
flush_pending()

save_time_df(TIME_DIR, sut, times_dict)
