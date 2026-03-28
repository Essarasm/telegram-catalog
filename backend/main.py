import os
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response


class CacheControlMiddleware(BaseHTTPMiddleware):
    """Smart caching: aggressive for images/assets, no-cache for HTML."""
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        path = request.url.path

        # HTML — no cache (Telegram WebView caches aggressively)
        if path == "/" or path.endswith(".html"):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
            response.headers["Expires"] = "0"

        # Product images — cache 7 days (they rarely change)
        elif path.startswith("/images/"):
            response.headers["Cache-Control"] = "public, max-age=604800, stale-while-revalidate=86400"

        # Hashed frontend assets (JS/CSS) — cache 1 year (filename changes on rebuild)
        elif "/assets/" in path and (path.endswith(".js") or path.endswith(".css")):
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"

        return response

load_dotenv()

from backend.database import init_db
from backend.routers import categories, products, export, cart, users, reports, cabinet, search

app = FastAPI(title="Katalog API", version="1.0.0")

# GZip compression — compresses API JSON responses and text assets
# min_size=500 avoids compressing tiny responses where overhead > savings
app.add_middleware(GZipMiddleware, minimum_size=500)
app.add_middleware(CacheControlMiddleware)
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


@app.get("/api/debug/volume")
def debug_volume():
    """Diagnostic: check what exists on /data volume."""
    import glob
    data_files = []
    try:
        for f in glob.glob("/data/**", recursive=True):
            try:
                size = os.path.getsize(f) if os.path.isfile(f) else -1
                data_files.append({"path": f, "size": size})
            except Exception:
                data_files.append({"path": f, "size": "error"})
    except Exception as e:
        data_files = [{"error": str(e)}]

    backup_exists = os.path.exists("/data/users_backup.json")
    db_exists = os.path.exists("/data/catalog.db")

    # Check users in DB
    users_count = 0
    approved_count = 0
    try:
        from backend.database import get_db
        conn = get_db()
        users_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        approved_count = conn.execute("SELECT COUNT(*) FROM users WHERE is_approved = 1").fetchone()[0]
        conn.close()
    except Exception:
        pass

    # Check override sources
    from backend.routers.users import _ALWAYS_APPROVED
    env_ids = os.getenv("ALWAYS_APPROVED_IDS", "(not set)")

    return {
        "db_exists": db_exists,
        "backup_exists": backup_exists,
        "users_in_db": users_count,
        "approved_in_db": approved_count,
        "always_approved_ids": sorted(_ALWAYS_APPROVED),
        "env_ALWAYS_APPROVED_IDS": env_ids,
        "data_files": data_files,
    }


app.include_router(categories.router)
app.include_router(products.router)
app.include_router(export.router)
app.include_router(cart.router)
app.include_router(users.router)
app.include_router(reports.router)
app.include_router(cabinet.router)
app.include_router(search.router)

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
