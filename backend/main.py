import os
from pathlib import Path
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.staticfiles import StaticFiles


class CacheControlMiddleware:
    """Pure ASGI middleware for smart cache headers. Avoids BaseHTTPMiddleware streaming bugs."""
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")

        async def send_with_cache(message):
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                if path == "/" or path.endswith(".html"):
                    headers.append((b"cache-control", b"no-cache, no-store, must-revalidate"))
                    headers.append((b"pragma", b"no-cache"))
                    headers.append((b"expires", b"0"))
                elif path.startswith("/images/"):
                    headers.append((b"cache-control", b"public, max-age=604800, stale-while-revalidate=86400"))
                elif "/assets/" in path and (path.endswith(".js") or path.endswith(".css")):
                    headers.append((b"cache-control", b"public, max-age=31536000, immutable"))
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_cache)

load_dotenv()

from backend.database import init_db
from backend.routers import categories, products, export, cart, users, reports, cabinet, search, finance, admin, feedback, locations

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
app.include_router(finance.router)
app.include_router(admin.router)
app.include_router(feedback.router)
app.include_router(locations.router)
from backend.routers import agent
app.include_router(agent.router)
app.include_router(locations.client_router)

# Serve admin dashboard (static HTML)
admin_dir = Path("./admin")
admin_dir.mkdir(exist_ok=True)
if admin_dir.exists():
    app.mount("/admin", StaticFiles(directory=str(admin_dir), html=True), name="admin")

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
