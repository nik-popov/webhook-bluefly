import os
import hmac
import hashlib
import base64
import json
import secrets
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.parse import urlencode

from flask import Flask, request, abort, redirect
from dotenv import load_dotenv, set_key

from logger import TransactionLogger

load_dotenv()

app = Flask(__name__)

WEBHOOK_SECRET = os.environ.get("SHOPIFY_WEBHOOK_SECRET", "")
LOG_DIR = os.environ.get("LOG_DIR", "./logs")
PORT = int(os.environ.get("PORT", "5000"))

SHOPIFY_API_KEY = os.environ.get("SHOPIFY_API_KEY", "")
SHOPIFY_API_SECRET = os.environ.get("SHOPIFY_API_SECRET", "")
SHOPIFY_SCOPES = os.environ.get("SHOPIFY_SCOPES", "read_orders,read_products")
SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE", "")

_oauth_nonce = ""

tx_logger = TransactionLogger(LOG_DIR)

ALLOWED_TOPICS = {
    "orders/create",
    "orders/updated",
    "orders/paid",
    "orders/fulfilled",
    "orders/cancelled",
    "products/create",
    "products/update",
    "products/delete",
    "inventory_levels/update",
    "inventory_levels/connect",
}


def verify_shopify_hmac(data: bytes, hmac_header: str, secret: str) -> bool:
    computed = base64.b64encode(
        hmac.new(
            secret.encode("utf-8"),
            data,
            hashlib.sha256,
        ).digest()
    ).decode("utf-8")
    return hmac.compare_digest(computed, hmac_header)


@app.route("/webhooks/shopify", methods=["POST"])
def handle_webhook():
    raw_body = request.get_data()

    hmac_header = request.headers.get("X-Shopify-Hmac-SHA256", "")
    if not verify_shopify_hmac(raw_body, hmac_header, WEBHOOK_SECRET):
        abort(401)

    topic = request.headers.get("X-Shopify-Topic", "unknown")
    shop_domain = request.headers.get("X-Shopify-Shop-Domain", "unknown")
    event_id = request.headers.get("X-Shopify-Event-Id", "")

    if topic not in ALLOWED_TOPICS:
        app.logger.warning("Unexpected topic: %s from %s", topic, shop_domain)

    try:
        payload = json.loads(raw_body)
    except json.JSONDecodeError:
        abort(400)

    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_id": event_id,
        "topic": topic,
        "shop_domain": shop_domain,
        "payload": payload,
    }

    file_path = tx_logger.log(record)
    app.logger.info("Logged %s -> %s", topic, file_path)

    return "", 200


@app.route("/health", methods=["GET"])
def health():
    return {"status": "ok"}, 200


# --- OAuth flow to get access token from Dev Dashboard app ---

@app.route("/auth/install", methods=["GET"])
def auth_install():
    """Visit this URL to start the OAuth install flow."""
    global _oauth_nonce
    _oauth_nonce = secrets.token_hex(16)

    redirect_uri = "https://tunnel-shopify-vendize.ngrok.app/auth/callback"
    params = urlencode({
        "client_id": SHOPIFY_API_KEY,
        "scope": SHOPIFY_SCOPES,
        "redirect_uri": redirect_uri,
        "state": _oauth_nonce,
    })
    install_url = f"https://{SHOPIFY_STORE}/admin/oauth/authorize?{params}"
    return redirect(install_url)


@app.route("/auth/callback", methods=["GET"])
def auth_callback():
    """Shopify redirects here after merchant approves. Exchanges code for access token."""
    global _oauth_nonce

    # Verify state/nonce
    state = request.args.get("state", "")
    if not _oauth_nonce or state != _oauth_nonce:
        return "Invalid state parameter", 403
    _oauth_nonce = ""

    code = request.args.get("code", "")
    if not code:
        return "Missing code parameter", 400

    # Exchange code for permanent access token
    token_url = f"https://{SHOPIFY_STORE}/admin/oauth/access_token"
    payload = json.dumps({
        "client_id": SHOPIFY_API_KEY,
        "client_secret": SHOPIFY_API_SECRET,
        "code": code,
    }).encode("utf-8")

    req = Request(token_url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")

    try:
        resp = urlopen(req)
        data = json.loads(resp.read())
        access_token = data.get("access_token", "")
    except Exception as e:
        return f"Token exchange failed: {e}", 500

    if not access_token:
        return f"No access token in response: {data}", 500

    # Save token to .env
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    set_key(env_path, "SHOPIFY_ACCESS_TOKEN", access_token)

    return (
        f"<h2>App installed successfully!</h2>"
        f"<p>Access token saved to .env: <code>{access_token[:12]}...</code></p>"
        f"<p>Now restart the app and run: <code>python register_webhooks.py</code></p>"
    )


if __name__ == "__main__":
    if not WEBHOOK_SECRET:
        print("WARNING: SHOPIFY_WEBHOOK_SECRET is not set. HMAC verification will fail.")
    print(f"Starting webhook listener on port {PORT}...")
    app.run(host="0.0.0.0", port=PORT)
