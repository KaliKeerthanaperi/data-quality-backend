from fastapi import APIRouter, HTTPException
from app.storage import get_last_result

router = APIRouter(prefix="/summary", tags=["summary"])


@router.get("")
async def get_summary():
    """Retrieve the last analysis result."""
    result = get_last_result()
    if result is None:
        raise HTTPException(
            status_code=404, detail="No analysis result available. Please upload a CSV first."
        )
    return result
