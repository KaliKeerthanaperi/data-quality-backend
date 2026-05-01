from fastapi import APIRouter, UploadFile, File, HTTPException
from utils.file_handler import read_csv_columns

router = APIRouter(prefix="/upload", tags=["upload"])


@router.post("/csv")
async def upload_csv(file: UploadFile = File(...)):
    if file.content_type not in ["text/csv", "application/vnd.ms-excel", "application/csv"]:
        raise HTTPException(status_code=400, detail="Uploaded file must be a CSV")

    try:
        columns = await read_csv_columns(file)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    return {"columns": columns}
