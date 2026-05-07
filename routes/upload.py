from fastapi import APIRouter, UploadFile, File, HTTPException
from services.data_quality import (
    count_columns,
    count_duplicates,
    count_null_values,
    count_rows,
)
from utils.file_handler import read_csv_dataframe
from app.storage import store_last_result

router = APIRouter(prefix="/upload", tags=["upload"])


@router.post("/csv")
async def upload_csv(file: UploadFile = File(...)):
    if file.content_type not in ["text/csv", "application/vnd.ms-excel", "application/csv"]:
        raise HTTPException(status_code=400, detail="Uploaded file must be a CSV")

    try:
        dataframe = await read_csv_dataframe(file)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    result = {
        "rows": count_rows(dataframe),
        "columns": count_columns(dataframe),
        "column_names": dataframe.columns.tolist(),
        "null_values": count_null_values(dataframe),
        "duplicate_rows": count_duplicates(dataframe),
    }
    store_last_result(result)
    return result
