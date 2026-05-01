from fastapi import FastAPI
from routes.upload import router as upload_router
from app.config import EMAIL, USERNAME

app = FastAPI()
app.include_router(upload_router)


@app.get("/")
def read_root():
    return {
        "message": "Data Quality Backend API",
        "email": EMAIL,
        "username": USERNAME,
    }