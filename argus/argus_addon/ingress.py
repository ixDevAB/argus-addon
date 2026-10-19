import asyncio
import html
from datetime import UTC, datetime
from pathlib import Path

import segno
import structlog
from aiohttp import web

from argus_addon.pair_client import CodeHolder

log = structlog.get_logger(__name__)


def _qr_svg(code: str) -> str:
    """Render the bare claim-code as an inline SVG QR for the app's scanner.

    The payload is the code verbatim (the iOS scanner feeds the decoded string
    straight into its 6-digit field), so nothing but the digits is encoded.
    """
    return segno.make(code, error="m").svg_inline(scale=6, border=2)


PAIRED_PAGE = (
    "<!doctype html><html><body>"
    "<h1>Argus is paired</h1>"
    "<p>The add-on is connecting to the Argus cloud.</p>"
    '<form method="post" action="/unpair" '
    "onsubmit=\"return confirm('Unpair this add-on? "
    "You will need to pair this Home again from the Argus app.');\">"
    '<button type="submit" style="margin-top:1em;">Unpair</button>'
    "</form>"
    "</body></html>"
)


COPY_SCRIPT = (
    "var code=document.getElementById('code'),button=document.getElementById('copy');"
    "function copied(){button.textContent='Copied';}"
    "function fallback(){var r=document.createRange();r.selectNodeContents(code);"
    "var s=getSelection();s.removeAllRanges();s.addRange(r);"
    "if(document.execCommand('copy'))copied();}"
    "button.addEventListener('click',function(){"
    "var text=code.textContent.trim();"
    "if(navigator.clipboard)navigator.clipboard.writeText(text).then(copied,fallback);"
    "else fallback();});"
    "var t=document.querySelector('time');"
    "t.textContent=new Date(t.getAttribute('datetime'))"
    ".toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit',hour12:false});"
)


def _pairing_page(code_holder: CodeHolder) -> str:
    code = code_holder.code
    expires_at = code_holder.expires_at
    expired = expires_at is not None and expires_at <= datetime.now(UTC)
    if not code or expired:
        body = (
            '<p id="status">Waiting for a pairing code from Argus…</p>'
            if not code
            else '<p id="status">This code has expired. A fresh code is on its way…</p>'
        )
        return _page(body)
    safe_code = html.escape(code)
    try:
        qr_block = f'<div id="qr" style="text-align:center;padding:1rem 0;max-width:240px;margin:0 auto;">{_qr_svg(code)}</div>'
        intro = "Scan this QR code with the Argus app, or enter the code below for the Home you are setting up:"
    except Exception as exc:
        log.warning("qr render failed, showing code only", error=str(exc))
        qr_block = ""
        intro = "Enter this code in the Argus app for the Home you are setting up:"
    body = (
        f"<p>{intro}</p>"
        f"{qr_block}"
        f'<div id="code" style="font-size:3rem;font-weight:700;letter-spacing:0.3rem;'
        "text-align:center;font-variant-numeric:tabular-nums;padding:1rem 0;"
        f'user-select:all;cursor:pointer;">{safe_code}</div>'
        '<p style="text-align:center;"><button type="button" id="copy" '
        'style="font-size:1rem;padding:0.5rem 1.25rem;border-radius:0.5rem;'
        'border:1px solid #999;background:#fff;cursor:pointer;">Copy code</button></p>'
        f'<p id="expires">Expires at <time datetime="{html.escape(expires_at.isoformat())}">'
        f"{expires_at.strftime('%H:%M:%S')} UTC</time>.</p>"
        f"<script>{COPY_SCRIPT}</script>"
    )
    return _page(body)


def _page(body: str) -> str:
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        '<meta http-equiv="refresh" content="5">'
        "<title>Pair Argus</title><style>"
        "body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;padding:1.5rem;"
        "max-width:480px;margin:auto;color:#222;}h1{font-size:1.25rem;}</style></head>"
        "<body><h1>Pair this Home with Argus</h1>"
        f"{body}</body></html>"
    )


def build_app(
    token_path: Path,
    code_holder: CodeHolder,
    unpair_event: asyncio.Event | None = None,
) -> web.Application:
    app = web.Application()
    app["token_path"] = token_path
    app["code_holder"] = code_holder

    async def index(_request: web.Request) -> web.Response:
        if token_path.exists() and token_path.read_text().strip():
            return web.Response(text=PAIRED_PAGE, content_type="text/html")
        return web.Response(text=_pairing_page(code_holder), content_type="text/html")

    async def unpair(_request: web.Request) -> web.Response:
        if token_path.exists():
            token_path.unlink()
            if unpair_event is not None:
                unpair_event.set()
        raise web.HTTPFound(location="/")

    app.router.add_get("/", index)
    app.router.add_post("/unpair", unpair)
    return app
