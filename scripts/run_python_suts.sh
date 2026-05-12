#!/usr/bin/env bash
# Run all pure-Python SUTs against a dataset from the repo root.
# Usage: scripts/run_python_suts.sh <dataset_name> [sut1 sut2 ...]
#
# <dataset_name>  Name of the folder under data/ (e.g. polluted_files)
# [sut ...]       Optional list of SUTs to run. Defaults to all Python SUTs.
#
# Examples:
#   scripts/run_python_suts.sh polluted_files
#   scripts/run_python_suts.sh my_data duckdbauto duckdbparse

set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <dataset_name> [sut1 sut2 ...]"
    echo "  dataset_name  Name of the folder under data/ (e.g. polluted_files)"
    echo "  sut ...       Optional subset of SUTs (default: all Python SUTs)"
    echo ""
    echo "Python SUTs: duckdbauto duckdbparse pandas pycsv clevercs"
    exit 1
fi

DATASET="$1"
shift

ALL_SUTS=(duckdbauto duckdbparse pandas pycsv clevercs)
if [[ $# -gt 0 ]]; then
    SUTS=("$@")
else
    SUTS=("${ALL_SUTS[@]}")
fi

# Must be run from repo root
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [[ "$(pwd)" != "$REPO_ROOT" ]]; then
    echo "Error: run this script from the repo root: cd $REPO_ROOT"
    exit 1
fi

if [[ ! -d "data/$DATASET/csv" ]]; then
    echo "Error: data/$DATASET/csv not found. Run pollution first."
    exit 1
fi

export DATASET

declare -A SUT_SCRIPT=(
    [duckdbauto]="sut/duckdbauto/duck-bench.py"
    [duckdbparse]="sut/duckdbparse/duck-bench.py"
    [pandas]="sut/pandas/panda.py"
    [pycsv]="sut/pycsv/pycsv.py"
    [clevercs]="sut/clevercs/clevercs.py"
)

for sut in "${SUTS[@]}"; do
    if [[ -z "${SUT_SCRIPT[$sut]+_}" ]]; then
        echo "Unknown SUT '$sut'. Valid options: ${!SUT_SCRIPT[*]}"
        exit 1
    fi
    echo ""
    echo "=== Running $sut on dataset '$DATASET' ==="
    python3 "${SUT_SCRIPT[$sut]}"
done

echo ""
echo "Done. Results are in results/*/$DATASET/"
echo "Run evaluation with:"
echo "  python3 evaluate.py --dataset $DATASET"
