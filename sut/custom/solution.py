import os
import pandas as pd


def parse_csv(csv_input: str) -> pd.DataFrame:
    """
    Parse a CSV and return a pandas DataFrame 

    Args:
        csv_input: Either a filesystem path to a CSV file, or raw CSV content
                   as a string.

    Returns:
        A pandas DataFrame.
    """
    raise NotImplementedError("Implement parse_csv in this file")
