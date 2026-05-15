from fastapi import APIRouter, HTTPException
from app.storage import get_last_result

router = APIRouter(prefix="/summary", tags=["summary"])


@router.get("")
async def get_summary():
    """Retrieve the last analysis result."""
    try:
        result = get_last_result()
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to retrieve the analysis result")

    if result is None:
        raise HTTPException(
            status_code=404, detail="No analysis result available. Please upload a CSV first."
        )

    try:
        return result
    except Exception:
        raise HTTPException(status_code=500, detail="Failed to build summary response")
