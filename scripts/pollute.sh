#!/usr/bin/env bash
# Pollute a CSV file using the Pollock benchmark pollution container.
# Usage: scripts/pollute.sh <source.csv> <output_dir>
# Run from the repository root.

set -euo pipefail

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <source_csv> <output_dir>"
    echo "  source_csv  Path to the CSV file to pollute"
    echo "  output_dir  Directory where polluted files will be written"
    exit 1
fi

SOURCE="$(realpath "$1")"
OUTPUT="$(realpath "$2")"

if [[ ! -f "$SOURCE" ]]; then
    echo "Error: file not found: $SOURCE"
    exit 1
fi

mkdir -p "$OUTPUT"

FILENAME="$(basename "$SOURCE")"

docker-compose run --rm \
    -v "$SOURCE":/app/input/"$FILENAME" \
    -v "$OUTPUT":/app/output \
    pollution \
    python3 /app/pollute_main.py --source /app/input/"$FILENAME" --output /app/output

echo ""
echo "Done. Polluted files written to: $OUTPUT"
echo "  csv/        — polluted variants"
echo "  clean/      — clean reference versions"
echo "  parameters/ — dialect parameters (JSON)"
