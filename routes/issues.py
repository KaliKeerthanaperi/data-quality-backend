from fastapi import APIRouter, HTTPException
from app.storage import get_last_result

router = APIRouter(prefix="/issues", tags=["issues"])


@router.get("")
async def get_issues():
    """Return null values and duplicate rows from the last analysis result."""
    result = get_last_result()
    if result is None:
        raise HTTPException(
            status_code=404, detail="No analysis result available. Please upload a CSV first."
        )
    return {
        "null_values": result.get("null_values"),
        "duplicate_rows": result.get("duplicate_rows"),
        "advanced_validation": result.get("advanced_validation"),
    }
