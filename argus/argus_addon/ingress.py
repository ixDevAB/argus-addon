from pathlib import Path

from aiohttp import web

STATIC_DIR = Path(__file__).parent / "static"


def build_app(token_path: Path) -> web.Application:
    app = web.Application()
    app["token_path"] = token_path

    async def index(_request: web.Request) -> web.Response:
        if token_path.exists() and token_path.read_text().strip():
            return web.Response(
                text="<!doctype html><html><body><h1>Argus is paired</h1>"
                "<p>The add-on is connecting to the Argus cloud.</p></body></html>",
                content_type="text/html",
            )
        return web.FileResponse(STATIC_DIR / "pair.html")

    async def save_token(request: web.Request) -> web.Response:
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"error": "invalid json"}, status=400)
        token = (data.get("token") or "").strip() if isinstance(data, dict) else ""
        if not token:
            return web.json_response({"error": "empty token"}, status=400)
        if len(token) > 256:
            return web.json_response({"error": "token too long"}, status=400)
        token_path.parent.mkdir(parents=True, exist_ok=True)
        token_path.write_text(token)
        return web.json_response({"ok": True})

    app.router.add_get("/", index)
    app.router.add_post("/token", save_token)
    app.router.add_static("/static", STATIC_DIR)
    return app
