# Running Pollock on a custom file

## 1. Pollute your file

Use the helper script from the repository root:

```bash
scripts/pollute.sh myfile.csv output/
```

This mounts your file into the pollution container and runs all pollution variants. When it finishes you'll find:

| Directory | Contents |
|---|---|
| `output/csv/` | Polluted CSV variants |
| `output/clean/` | Clean reference versions |
| `output/parameters/` | Dialect parameters per file (JSON) |

> **Note (manual alternative):** If you prefer to run the Docker command yourself, or need more control:
> ```bash
> docker-compose run --rm \
>   -v $(pwd)/myfile.csv:/app/myfile.csv \
>   -v $(pwd)/output:/app/output \
>   pollution \
>   python3 /app/pollute_main.py --source /app/myfile.csv --output /app/output
> ```
> Omitting `--source` and `--output` falls back to `./results/source.csv` and `./polluted_files`.

## 2. Run a benchmark SUT (example DuckDBParse)

Each SUT reads from `/polluted_files` inside the container. Mount your output directory there:

```bash
docker-compose run --rm \
  -v $(pwd)/output:/polluted_files \
  duckdbparse-client
```

Results land in `./results/duckdbparse/` on the host (already wired in docker-compose.yml).

If a previous benchmark run exists for this SUT, clear it first or the script will skip all files:

```bash
rm -rf results/duckdbparse/polluted_files/
```

To run against the default `./polluted_files` folder, just use:

```bash
docker-compose up duckdbparse-client
```

## 3. Run all benchmark SUTs

(on the standard source file used by the authors)
To run all SUTs at once, use `benchmark.sh`:

```bash
chmod +x benchmark.sh && ./benchmark.sh
```

Note that not all SUTs are currently working due to dependency issues — expect some containers to fail. The ones confirmed working are `duckdbauto` and `duckdbparse`.

## 4. Evaluate

To score a specific SUT against your custom file, mount your output directory as the dataset:

```bash
docker-compose run --rm \
  -v $(pwd)/output:/app/polluted_files \
  evaluate python3 /app/evaluate.py --sut duckdbparse
```

To score all SUTs with results folders present:

```bash
docker-compose run --rm \
  -v $(pwd)/output:/app/polluted_files \
  evaluate python3 /app/evaluate.py
```

Aggregate scores are saved to `results/aggregate_results_polluted_files.csv`.