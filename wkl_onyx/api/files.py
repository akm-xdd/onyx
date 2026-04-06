# api/files.py
from fastapi import APIRouter, UploadFile, File, HTTPException
from storage import get_storage

router = APIRouter(prefix="/files", tags=["files"])

ALLOWED_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".txt", ".md", ".csv",
    ".xls", ".xlsx", ".pptx", ".json", ".html", ".rtf",
}


def _safe_filename(name: str) -> str:
    for char in ['/', '\\', ':', '*', '?', '"', '<', '>', '|', '#']:
        name = name.replace(char, "_")
    return name


@router.post("/upload")
async def upload_files(files: list[UploadFile] = File(...)):
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    storage = get_storage()
    uploaded = []
    errors = []

    for file in files:
        filename = file.filename or "unknown"
        ext = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""

        if ext not in ALLOWED_EXTENSIONS:
            errors.append({"file": filename, "error": f"Extension {ext} not allowed"})
            continue

        try:
            content = await file.read()
            safe_name = _safe_filename(filename)
            storage_key = f"uploads/{safe_name}"

            storage.upload(storage_key, content)
            uploaded.append({"file": filename, "storage_key": storage_key})
        except Exception as e:
            errors.append({"file": filename, "error": str(e)})

    return {
        "uploaded": uploaded,
        "errors": errors,
        "total_uploaded": len(uploaded),
        "total_errors": len(errors),
    }