from __future__ import annotations

import pandas as pd


def count_rows(dataframe: pd.DataFrame) -> int:
    """Return the number of rows in the dataframe."""
    return int(dataframe.shape[0])


def count_columns(dataframe: pd.DataFrame) -> int:
    """Return the number of columns in the dataframe."""
    return int(dataframe.shape[1])


def count_null_values(dataframe: pd.DataFrame) -> dict[str, object]:
    """Return null value counts for the dataframe.

    Returns a dictionary with a total null count and null counts by column.
    """
    null_counts = dataframe.isna().sum()
    return {
        "total_nulls": int(null_counts.sum()),
        "nulls_by_column": null_counts.astype(int).to_dict(),
    }


def count_duplicates(dataframe: pd.DataFrame, subset=None, keep: str = "first") -> int:
    """Return the number of duplicate rows in the dataframe."""
    return int(dataframe.duplicated(subset=subset, keep=keep).sum())
