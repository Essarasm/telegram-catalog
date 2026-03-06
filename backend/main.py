import os
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

load_dotenv()

from backend.database import init_db
from backend.routers import categories, products, export

app = FastAPI(title="Katalog API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/api/health")
def health():
    return {"status": "ok"}


app.include_router(categories.router)
app.include_router(products.router)
app.include_router(export.router)

# Serve product images
images_dir = Path(os.getenv("IMAGES_DIR", "./images"))
images_dir.mkdir(exist_ok=True)
app.mount("/images", StaticFiles(directory=str(images_dir)), name="images")

# Serve frontend (built files) — must be last since it catches all routes
frontend_dist = Path("./frontend/dist")
if frontend_dist.exists():
    app.mount("/", StaticFiles(directory=str(frontend_dist), html=True), name="frontend")


@app.on_event("startup")
def startup():
    init_db()
