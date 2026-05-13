from __future__ import annotations

import math
import re

import pandas as pd

# ── helpers ───────────────────────────────────────────────────────────────────

_DATE_PATTERNS = [
    r"^\d{4}-\d{2}-\d{2}$",          # YYYY-MM-DD
    r"^\d{2}/\d{2}/\d{4}$",          # DD/MM/YYYY or MM/DD/YYYY
    r"^\d{2}-\d{2}-\d{4}$",          # DD-MM-YYYY
    r"^\d{4}/\d{2}/\d{2}$",          # YYYY/MM/DD
]

_ID_KEYWORDS = {"id", "key", "code", "age", "year", "count", "qty", "quantity"}


def _looks_like_id_column(col_name: str) -> bool:
    lower = col_name.lower()
    return any(kw in lower for kw in _ID_KEYWORDS)


def _classify_date_format(value: str) -> str | None:
    for pat in _DATE_PATTERNS:
        if re.match(pat, value.strip()):
            return pat
    return None


# ── public API ────────────────────────────────────────────────────────────────

def detect_empty_columns(dataframe: pd.DataFrame) -> dict[str, object]:
    """Detect columns where ALL values are null/NaN."""
    null_counts = dataframe.isna().sum()
    empty_cols = [col for col, count in null_counts.items() if count == len(dataframe)]
    return {
        "empty_column_count": len(empty_cols),
        "empty_columns": empty_cols,
    }


def detect_invalid_values(dataframe: pd.DataFrame) -> dict[str, object]:
    """Detect invalid values per column.

    Numeric  : non-finite (inf/-inf) + statistical outliers (mean ± 3 std).
    String   : blank strings (empty / whitespace-only).
    """
    invalid: dict[str, dict[str, object]] = {}
    total_invalid = 0

    for col in dataframe.columns:
        series = dataframe[col]

        if pd.api.types.is_numeric_dtype(series):
            non_finite_mask = series.apply(
                lambda x: isinstance(x, float) and not math.isfinite(x)
            )
            non_finite_count = int(non_finite_mask.sum())
            non_finite_indices = series[non_finite_mask].index.tolist()

            finite_series = series.dropna()[series.dropna().apply(
                lambda x: not (isinstance(x, float) and not math.isfinite(x))
            )]
            outlier_count = 0
            outlier_indices: list = []
            if len(finite_series) >= 2:
                mean = finite_series.mean()
                std = finite_series.std()
                if std > 0:
                    outlier_mask = (
                        ((series < mean - 3 * std) | (series > mean + 3 * std))
                        & series.notna()
                        & ~non_finite_mask
                    )
                    outlier_count = int(outlier_mask.sum())
                    outlier_indices = series[outlier_mask].index.tolist()

            col_total = non_finite_count + outlier_count
            if col_total > 0:
                invalid[col] = {
                    "type": "numeric",
                    "non_finite_count": non_finite_count,
                    "non_finite_indices": non_finite_indices,
                    "outlier_count": outlier_count,
                    "outlier_indices": outlier_indices,
                    "total_invalid": col_total,
                }
                total_invalid += col_total

        elif pd.api.types.is_string_dtype(series) or series.dtype == object:
            blank_mask = series.apply(lambda x: isinstance(x, str) and x.strip() == "")
            blank_count = int(blank_mask.sum())
            if blank_count > 0:
                invalid[col] = {
                    "type": "string",
                    "blank_string_count": blank_count,
                    "blank_string_indices": series[blank_mask].index.tolist(),
                    "total_invalid": blank_count,
                }
                total_invalid += blank_count

    return {
        "total_invalid_values": total_invalid,
        "invalid_by_column": invalid,
    }


def detect_mixed_types(dataframe: pd.DataFrame) -> dict[str, object]:
    """Detect object columns that contain more than one Python type (excl. NaN).

    Example: a column with both int and str values is flagged.
    """
    mixed: dict[str, object] = {}
    for col in dataframe.columns:
        series = dataframe[col].dropna()
        if series.empty:
            continue
        types_found = set(type(v).__name__ for v in series)
        if len(types_found) > 1:
            mixed[col] = {
                "types_found": sorted(types_found),
                "example_values": series.head(5).tolist(),
            }
    return {
        "mixed_type_column_count": len(mixed),
        "mixed_type_columns": mixed,
    }


def detect_negative_values(dataframe: pd.DataFrame) -> dict[str, object]:
    """Flag negative values in numeric columns whose name suggests non-negative data
    (id, age, count, qty, year, key, code).
    """
    flagged: dict[str, object] = {}
    for col in dataframe.columns:
        if not pd.api.types.is_numeric_dtype(dataframe[col]):
            continue
        if not _looks_like_id_column(col):
            continue
        neg_mask = dataframe[col] < 0
        neg_count = int(neg_mask.sum())
        if neg_count > 0:
            flagged[col] = {
                "negative_count": neg_count,
                "negative_indices": dataframe[col][neg_mask].index.tolist(),
            }
    return {
        "columns_with_unexpected_negatives": len(flagged),
        "negative_value_details": flagged,
    }


def detect_constant_columns(dataframe: pd.DataFrame) -> dict[str, object]:
    """Detect columns with only one unique non-null value (no variance)."""
    constant_cols = []
    for col in dataframe.columns:
        series = dataframe[col].dropna()
        if series.nunique() == 1:
            constant_cols.append({"column": col, "constant_value": series.iloc[0]})
    return {
        "constant_column_count": len(constant_cols),
        "constant_columns": constant_cols,
    }


def detect_high_cardinality(
    dataframe: pd.DataFrame, threshold: float = 0.9
) -> dict[str, object]:
    """Detect string/object columns where unique-value ratio exceeds `threshold`.

    High cardinality may indicate free-text or ID fields that are hard to analyse.
    Default threshold: 90 % unique values.
    """
    high: dict[str, object] = {}
    n_rows = len(dataframe)
    if n_rows == 0:
        return {"high_cardinality_column_count": 0, "high_cardinality_columns": high}
    for col in dataframe.columns:
        series = dataframe[col]
        if not (pd.api.types.is_string_dtype(series) or series.dtype == object):
            continue
        ratio = series.nunique() / n_rows
        if ratio >= threshold:
            high[col] = {
                "unique_count": int(series.nunique()),
                "total_rows": n_rows,
                "unique_ratio": round(ratio, 4),
            }
    return {
        "high_cardinality_column_count": len(high),
        "high_cardinality_columns": high,
    }


def detect_date_format_inconsistency(dataframe: pd.DataFrame) -> dict[str, object]:
    """Detect string columns that look like dates but use inconsistent formats."""
    inconsistent: dict[str, object] = {}
    for col in dataframe.columns:
        series = dataframe[col].dropna()
        if not (pd.api.types.is_string_dtype(series) or series.dtype == object):
            continue
        formats: dict[str, int] = {}
        non_date_count = 0
        for val in series:
            if not isinstance(val, str):
                continue
            fmt = _classify_date_format(val)
            if fmt:
                formats[fmt] = formats.get(fmt, 0) + 1
            else:
                non_date_count += 1
        if len(formats) > 1:
            inconsistent[col] = {
                "formats_detected": formats,
                "non_date_values": non_date_count,
            }
    return {
        "date_inconsistency_column_count": len(inconsistent),
        "date_inconsistency_columns": inconsistent,
    }


def advanced_validation(dataframe: pd.DataFrame) -> dict[str, object]:
    """Run all validation checks and return a consolidated advanced validation report.

    Checks included:
    - Empty columns
    - Invalid values (non-finite, outliers, blank strings)
    - Mixed data types
    - Unexpected negative values
    - Constant (zero-variance) columns
    - High-cardinality string columns
    - Inconsistent date formats

    The report also includes a severity summary:
      - critical : empty columns, mixed types
      - warning  : invalid values, unexpected negatives, date inconsistencies
      - info     : constant columns, high cardinality
    """
    empty = detect_empty_columns(dataframe)
    invalid = detect_invalid_values(dataframe)
    mixed = detect_mixed_types(dataframe)
    negatives = detect_negative_values(dataframe)
    constant = detect_constant_columns(dataframe)
    cardinality = detect_high_cardinality(dataframe)
    date_fmt = detect_date_format_inconsistency(dataframe)

    severity_summary = {
        "critical": {
            "empty_columns": empty["empty_column_count"],
            "mixed_type_columns": mixed["mixed_type_column_count"],
        },
        "warning": {
            "columns_with_invalid_values": len(invalid["invalid_by_column"]),
            "columns_with_unexpected_negatives": negatives["columns_with_unexpected_negatives"],
            "columns_with_date_inconsistency": date_fmt["date_inconsistency_column_count"],
        },
        "info": {
            "constant_columns": constant["constant_column_count"],
            "high_cardinality_columns": cardinality["high_cardinality_column_count"],
        },
    }

    return {
        "severity_summary": severity_summary,
        "empty_columns": empty,
        "invalid_values": invalid,
        "mixed_types": mixed,
        "unexpected_negatives": negatives,
        "constant_columns": constant,
        "high_cardinality": cardinality,
        "date_format_inconsistency": date_fmt,
    }
