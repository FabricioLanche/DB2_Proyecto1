from fastapi import APIRouter, UploadFile, File
from pydantic import BaseModel
import os
import shutil

router = APIRouter()

class QueryRequest(BaseModel):
    query: str

@router.post("/query")
def ejecutar_query(data: QueryRequest):
    return {"result": data.query}

UPLOAD_DIR = "Backend/DBMS/datasets"  

@router.post("/dataset")
async def create_dataset(file: UploadFile = File(...)):
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    file_path = os.path.join(UPLOAD_DIR, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return {"message": "Dataset guardado", "path": file_path}

@router.get("/dataset/list")
async def list_datasets():
    if not os.path.exists(UPLOAD_DIR):
        return {"datasets": []}
    archivos = [
        file
        for file in os.listdir(UPLOAD_DIR)
        if file.endswith(".csv") and os.path.isfile(os.path.join(UPLOAD_DIR, file))
    ]

    return {"datasets": archivos}

@router.post("/restart")
async def restart():
    if os.path.exists(UPLOAD_DIR):
        shutil.rmtree(UPLOAD_DIR)

    return {"message": "Datasets eliminados"}
