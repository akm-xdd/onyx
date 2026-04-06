from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.oauth import router as oauth_router
from api.files import router as files_router
from api.connectors import router as connectors_router

app = FastAPI(title="Wokelo Onyx")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(oauth_router)
app.include_router(files_router)
app.include_router(connectors_router)

@app.get("/health")
def health():
    return {"status": "ok"}