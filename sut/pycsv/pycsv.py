import csv
from os import listdir
from os.path import join, dirname
import os
import sys
import time

sys.path.insert(0, join(dirname(__file__), '..'))
from utils import print, save_time_df, load_parameters

sut = 'pycsv'
DATASET = os.environ.get('DATASET', 'polluted_files')
IN_DIR = f'data/{DATASET}/csv/'
PARAM_DIR = f'data/{DATASET}/parameters'
OUT_DIR = f'results/{sut}/{DATASET}/loading/'
TIME_DIR = f'results/{sut}/{DATASET}/'

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(TIME_DIR, exist_ok=True)
N_REPETITIONS = int(os.environ.get("N_REPETITIONS", 3))

TO_SKIP = []

times_dict = {}
benchmark_files = listdir(IN_DIR)

for idx, f in enumerate(benchmark_files):
    in_filepath = join(IN_DIR, f)
    out_filename = f'{f}_converted.csv'
    out_filepath = join(OUT_DIR, out_filename)
    if os.path.exists(out_filepath):
        continue
    print(f'Processing file ({idx + 1}/{len(benchmark_files)}) {f}')

    sut_params = load_parameters(join(PARAM_DIR, f'{f}_parameters.json'))
    for time_rep in range(N_REPETITIONS):
        start = time.time()
        try:
            with open(in_filepath, newline='', encoding=sut_params["encoding"]) as in_csvfile:
                dialect = csv.Sniffer().sniff(in_csvfile.read())
                in_csvfile.seek(0)
                reader = csv.reader(in_csvfile, dialect)
                rows = list(reader)
            end = time.time()
            with open(out_filepath, 'w', newline='') as out_csvfile:
                csv.writer(out_csvfile).writerows(rows)

        except Exception as e:
            end = time.time()
            print("Application error on file", f)
            print("\t", e)
            with open(out_filepath, "w") as out_csvfile:
                out_csvfile.write("Application Error\n")
                out_csvfile.write(str(e))

        times_dict[f] = times_dict.get(f, []) + [(end - start)]

        try:
            del start, end, in_csvfile, dialect, reader, rows, out_csvfile
        except:
            pass

save_time_df(TIME_DIR, sut, times_dict)
