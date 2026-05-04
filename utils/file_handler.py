import pandas as pd
from fastapi import UploadFile


async def read_csv_dataframe(file: UploadFile) -> pd.DataFrame:
    file.file.seek(0)
    try:
        dataframe = pd.read_csv(file.file)
    except Exception as exc:
        raise ValueError("Unable to parse CSV file") from exc

    return dataframe


async def read_csv_columns(file: UploadFile) -> list[str]:
    dataframe = await read_csv_dataframe(file)
    return dataframe.columns.tolist()
