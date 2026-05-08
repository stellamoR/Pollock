import io
import os
import pandas as pd
from typing import Dict, List, Tuple
from openai import OpenAI

client = OpenAI(
    base_url="https://integrate.api.nvidia.com/v1",
    api_key=os.environ["NVIDIA_API_KEY_CSV"],
)

SYSTEM_PROMPT = """\
You are a CSV repair assistant. You receive raw CSV lines that a parser \
failed to read, along with a sample of correctly parsed rows that show \
the expected schema.

Your job: interpret each failed line and return it as a correctly \
formatted CSV row that matches the schema.

Rules:
- Output ONLY the repaired rows as comma-separated values, one row per line, no header
- Preserve the number and order of columns shown in the schema
- If a line is genuinely uninterpretable (e.g. a comment or stray metadata), skip it
- Do not include any explanation, markdown formatting, or code fences\
"""


def repair_rows(failed_lines: List[str], context_rows: pd.DataFrame) -> pd.DataFrame:
    """
    Use an LLM to interpret raw CSV lines that DuckDB failed to parse.

    Args:
        failed_lines: Raw CSV lines that DuckDB rejected (one string per line).
        context_rows: A small sample of successfully parsed rows from the same
                      file, so the LLM can infer column names, types, and
                      formatting conventions.

    Returns:
        A DataFrame with the same columns as context_rows, one row per repaired
        line. Returns an empty DataFrame with the correct columns if nothing can
        be repaired.
    """
    columns = list(context_rows.columns)
    context_csv = context_rows.to_csv(index=False)
    failed_block = "\n".join(f"{i + 1}. {line}" for i, line in enumerate(failed_lines))

    user_message = (
        f"Schema (header + sample rows):\n{context_csv}\n"
        f"Failed lines to repair:\n{failed_block}\n\n"
        "Return the repaired rows as CSV (no header, one row per line, "
        "same column order as above)."
    )

    response = client.chat.completions.create(
        model="minimaxai/minimax-m2.7",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        max_tokens=2048,
    )

    repaired_text = response.choices[0].message.content.strip()

    if not repaired_text:
        return pd.DataFrame(columns=columns)

    try:
        return pd.read_csv(io.StringIO(repaired_text), header=None, names=columns)
    except Exception:
        return pd.DataFrame(columns=columns)


BATCH_SYSTEM_PROMPT = """\
You are a CSV repair assistant. You receive failed rows from multiple CSV files. \
For each file the schema is shown as a header + sample rows.

Your job: interpret each failed line and return it as a correctly formatted CSV row \
matching that file's schema.

Rules:
- For each file, output repaired rows preceded by the exact marker line: === FILE: <filename> ===
- Output bare comma-separated values, one row per line, no header
- Preserve the number and order of columns shown in that file's schema
- If a line is genuinely uninterpretable (e.g. a comment or stray metadata), skip it
- Omit the marker entirely for a file if all its rows are uninterpretable
- Do not include any explanation, markdown formatting, or code fences\
"""


def repair_batch(
    batch: List[Tuple[str, List[str], pd.DataFrame]],
) -> Dict[str, pd.DataFrame]:
    """
    Repair failed rows from multiple files in a single LLM call.

    Args:
        batch: List of (filename, failed_lines, context_rows) tuples.

    Returns:
        Dict mapping filename -> DataFrame of repaired rows (same columns as
        context_rows for that file). Files with nothing repairable are absent.
    """
    sections = []
    for filename, failed_lines, context_rows in batch:
        context_csv = context_rows.to_csv(index=False)
        failed_block = "\n".join(f"{i + 1}. {line}" for i, line in enumerate(failed_lines))
        sections.append(
            f"=== FILE: {filename} ===\n"
            f"Schema (header + sample rows):\n{context_csv}\n"
            f"Failed rows:\n{failed_block}"
        )

    user_message = "\n\n".join(sections)

    response = client.chat.completions.create(
        model="minimaxai/minimax-m2.7",
        messages=[
            {"role": "system", "content": BATCH_SYSTEM_PROMPT},
            {"role": "user", "content": user_message},
        ],
        max_tokens=4096,
    )

    repaired_text = response.choices[0].message.content.strip()

    # Parse response into per-file DataFrames
    column_map = {filename: list(ctx.columns) for filename, _, ctx in batch}
    result: Dict[str, pd.DataFrame] = {}

    current_file = None
    current_lines: List[str] = []

    def flush(filename, lines):
        if filename and lines:
            cols = column_map.get(filename, [])
            text = "\n".join(lines)
            try:
                result[filename] = pd.read_csv(
                    io.StringIO(text), header=None, names=cols
                )
            except Exception:
                pass

    for line in repaired_text.splitlines():
        if line.startswith("=== FILE:") and line.endswith("==="):
            flush(current_file, current_lines)
            current_file = line[len("=== FILE:"):][:-3].strip()
            current_lines = []
        elif current_file is not None:
            current_lines.append(line)

    flush(current_file, current_lines)
    return result
