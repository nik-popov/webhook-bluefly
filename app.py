import os
import hmac
import hashlib
import base64
import json
import secrets
import threading
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.parse import urlencode

from flask import Flask, request, abort, redirect
from dotenv import load_dotenv, set_key

from logger import TransactionLogger
from pipeline_logger import PipelineLogger
from shopify_client import ShopifyClient
from bluefly_client import BlueflyClient
from sql_lookup import BlueflyDBLookup
from field_mapper import (
    should_sync_product,
    get_metafield,
    build_bluefly_payload,
    build_quantity_price_payload,
)

load_dotenv()

app = Flask(__name__)

# Register the dashboard UI Blueprint
from dashboard.routes import dashboard_bp
app.register_blueprint(dashboard_bp)


# App-level error handlers — force JSON for /api/ routes, never return HTML
@app.errorhandler(404)
def app_404(e):
    from flask import request as req, jsonify as jf
    if req.path.startswith('/api/'):
        return jf({"error": "Not found: " + req.path}), 404
    return e  # let Flask handle non-API 404s normally


@app.errorhandler(500)
def app_500(e):
    from flask import request as req, jsonify as jf
    if req.path.startswith('/api/'):
        return jf({"error": "Internal server error"}), 500
    return e


@app.errorhandler(Exception)
def app_exception(e):
    from flask import request as req, jsonify as jf
    import traceback
    traceback.print_exc()
    if req.path.startswith('/api/'):
        return jf({"error": str(e)}), 500
    return e

WEBHOOK_SECRET = os.environ.get("SHOPIFY_WEBHOOK_SECRET", "")
LOG_DIR = os.environ.get("LOG_DIR", "./logs")
PIPELINE_LOG_DIR = os.environ.get("PIPELINE_LOG_DIR", "./pipeline_logs")
PORT = int(os.environ.get("PORT", "5000"))

SHOPIFY_API_KEY = os.environ.get("SHOPIFY_API_KEY", "")
SHOPIFY_API_SECRET = os.environ.get("SHOPIFY_API_SECRET", "")
SHOPIFY_SCOPES = os.environ.get("SHOPIFY_SCOPES", "read_orders,read_products")
SHOPIFY_STORE = os.environ.get("SHOPIFY_STORE", "")
SHOPIFY_ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN", "").strip("'\"")

BLUEFLY_API_URL = os.environ.get(
    "BLUEFLY_API_URL",
    "https://webhook.mindcloud.co/v1/webhook/bluefly/rithum/v2/products",
)
BLUEFLY_SELLER_ID = os.environ.get("BLUEFLY_SELLER_ID", "")
BLUEFLY_SELLER_TOKEN = os.environ.get("BLUEFLY_SELLER_TOKEN", "")

SQL_SERVER = os.environ.get("SQL_SERVER", "")
SQL_DATABASE = os.environ.get("SQL_DATABASE", "")
SQL_USER = os.environ.get("SQL_USER", "")
SQL_PASSWORD = os.environ.get("SQL_PASSWORD", "")

_oauth_nonce = ""

tx_logger = TransactionLogger(LOG_DIR)
pipeline = PipelineLogger(PIPELINE_LOG_DIR)
shopify_client = ShopifyClient(SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN) if SHOPIFY_STORE and SHOPIFY_ACCESS_TOKEN else None
bluefly_client = BlueflyClient(BLUEFLY_SELLER_ID, BLUEFLY_SELLER_TOKEN, BLUEFLY_API_URL) if BLUEFLY_SELLER_ID else None

# Topics that trigger the Bluefly sync pipeline
PRODUCT_TOPICS = {"products/create", "products/update"}
INVENTORY_TOPICS = {"inventory_levels/update"}

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


# -----------------------------------------------------------------------
# Background pipeline processing
# -----------------------------------------------------------------------

def _sync_product_to_bluefly(file_path: str, record: dict):
    """Background: enrich, map, and push a product event to Bluefly."""
    topic = record["topic"]
    product_id = record["payload"].get("id")

    print(f"\n[Pipeline] Processing {topic} for product {product_id}")
    tx_logger.update_status(file_path, "processing")

    job_path = pipeline.create_job(
        source_file=file_path,
        topic=topic,
        product_id=product_id,
        event_id=record.get("event_id", ""),
    )

    try:
        if topic == "products/delete":
            print(f"[Pipeline] Product deleted -- skipping")
            pipeline.update_stage(job_path, "skipped", {"reason": "product deleted"})
            tx_logger.update_status(file_path, "processed")
            return

        # Enrich
        pipeline.update_stage(job_path, "enriching")
        enriched = shopify_client.get_product_full(product_id)
        if not enriched:
            raise ValueError(f"Product {product_id} not found in Shopify")

        metafields = enriched.get("metafields", [])
        pipeline.update_stage(job_path, "enriched", {
            "title": enriched.get("title"),
            "variant_count": len(enriched.get("variants", [])),
        })
        print(f"[Pipeline] Enriched: '{enriched.get('title')}'")

        category_id = get_metafield(metafields, "custom", "bluefly_category")

        if not should_sync_product(enriched):
            # Auto-draft: if product was on Bluefly (has category), set NotLive
            if category_id and bluefly_client:
                print(f"[Pipeline] Product not ACTIVE -- auto-drafting on Bluefly")
                try:
                    qp = build_quantity_price_payload(enriched, metafields)
                    for bp in qp.get("BuyableProducts", []):
                        bp["ListingStatus"] = "NotLive"
                    bluefly_client.update_quantity_price([qp])
                    pipeline.update_stage(job_path, "pushed", {"action": "auto-draft", "status": "NotLive"})
                except Exception as draft_err:
                    print(f"[Pipeline] Auto-draft failed: {draft_err}")
            else:
                pipeline.update_stage(job_path, "skipped", {"reason": f"status: {enriched.get('status')}"})
            tx_logger.update_status(file_path, "processed")
            return

        if not category_id:
            print(f"[Pipeline] No bluefly_category -- skipping")
            pipeline.update_stage(job_path, "skipped", {"reason": "no bluefly_category"})
            tx_logger.update_status(file_path, "processed")
            return

        # Map (SQL lookup with graceful fallback)
        pipeline.update_stage(job_path, "mapping")
        sql_field_map = {}
        try:
            db = BlueflyDBLookup(SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD)
            with db:
                for variant in enriched.get("variants", []):
                    vt = variant.get("title", "")
                    if vt:
                        sql_field_map[vt] = db.lookup_category_fields(category_id, vt)
        except Exception as e:
            print(f"[Pipeline] SQL lookup skipped: {e}")

        # Read price adjustment from config.json
        _cfg = {}
        try:
            import dashboard.routes as _dr
            _cfg = _dr.load_config()
        except Exception:
            pass
        _adj = _cfg.get("price_adjustment_pct", 0)

        # All product events (create + update) use /v2/products with full payload.
        # This endpoint is an upsert — creates new products or updates all fields
        # (color, size, images, options, etc.) on existing products.
        bluefly_payload = build_bluefly_payload(enriched, metafields, sql_field_map, price_adjustment_pct=_adj)

        pipeline.update_stage(job_path, "mapped", {
            "seller_sku": bluefly_payload.get("SellerSKU"),
            "buyable_count": len(bluefly_payload.get("BuyableProducts", [])),
            "endpoint": "products",
        })

        # Push
        pipeline.update_stage(job_path, "pushing")
        result = bluefly_client.push_products([bluefly_payload])

        if result["success"]:
            print(f"[Pipeline] OK Pushed (products): HTTP {result['status_code']}")
            pipeline.update_stage(job_path, "pushed", {
                "response_status": result["status_code"],
                "endpoint": "products",
            })
            tx_logger.update_status(file_path, "processed")
        else:
            raise RuntimeError(f"Bluefly API error (products): {result['error']}")

    except Exception as e:
        print(f"[Pipeline] ERROR: {e}")
        pipeline.update_stage(job_path, "error", error=str(e))
        tx_logger.update_status(file_path, "error")


def _sync_inventory_to_bluefly(file_path: str, record: dict):
    """Background: resolve inventory update, enrich, map, push to Bluefly."""
    payload = record["payload"]
    inventory_item_id = payload.get("inventory_item_id")
    new_available = payload.get("available", 0)

    print(f"\n[Pipeline] Inventory update for item {inventory_item_id}, qty={new_available}")
    tx_logger.update_status(file_path, "processing")

    job_path = pipeline.create_job(
        source_file=file_path,
        topic="inventory_levels/update",
        product_id=inventory_item_id,
        event_id=record.get("event_id", ""),
    )

    try:
        pipeline.update_stage(job_path, "enriching")
        lookup = shopify_client.find_product_by_inventory_item(inventory_item_id)
        if not lookup:
            pipeline.update_stage(job_path, "skipped", {"reason": "unresolvable inventory item"})
            tx_logger.update_status(file_path, "processed")
            return

        product_id = lookup["product_id"]
        variant_sku = lookup["variant_sku"]

        enriched = shopify_client.get_product_full(product_id)
        if not enriched:
            raise ValueError(f"Product {product_id} not found")

        metafields = enriched.get("metafields", [])
        pipeline.update_stage(job_path, "enriched", {
            "product_id": product_id,
            "variant_sku": variant_sku,
        })

        if not should_sync_product(enriched):
            pipeline.update_stage(job_path, "skipped", {"reason": f"status: {enriched.get('status')}"})
            tx_logger.update_status(file_path, "processed")
            return

        category_id = get_metafield(metafields, "custom", "bluefly_category")
        if not category_id:
            pipeline.update_stage(job_path, "skipped", {"reason": "no bluefly_category"})
            tx_logger.update_status(file_path, "processed")
            return

        pipeline.update_stage(job_path, "mapping")
        sql_field_map = {}
        try:
            db = BlueflyDBLookup(SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD)
            with db:
                for variant in enriched.get("variants", []):
                    vt = variant.get("title", "")
                    if vt:
                        sql_field_map[vt] = db.lookup_category_fields(category_id, vt)
        except Exception as e:
            print(f"[Pipeline] SQL lookup skipped: {e}")

        # Build lightweight quantityprice payload for inventory updates
        bluefly_payload = build_quantity_price_payload(enriched, metafields)

        # Override specific variant's quantity with webhook value
        for bp in bluefly_payload.get("BuyableProducts", []):
            if bp.get("SellerSKU") == variant_sku:
                bp["Quantity"] = new_available

        pipeline.update_stage(job_path, "mapped", {
            "variant_sku": variant_sku,
            "quantity": new_available,
            "endpoint": "quantityprice",
        })

        pipeline.update_stage(job_path, "pushing")
        result = bluefly_client.update_quantity_price([bluefly_payload])

        if result["success"]:
            print(f"[Pipeline] OK Inventory pushed (quantityprice): HTTP {result['status_code']}")
            pipeline.update_stage(job_path, "pushed", {
                "response_status": result["status_code"],
                "endpoint": "quantityprice",
            })
            tx_logger.update_status(file_path, "processed")
        else:
            raise RuntimeError(f"Bluefly API error (quantityprice): {result['error']}")

    except Exception as e:
        print(f"[Pipeline] ERROR: {e}")
        pipeline.update_stage(job_path, "error", error=str(e))
        tx_logger.update_status(file_path, "error")


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

    # Trigger Bluefly sync pipeline in background thread
    if shopify_client and bluefly_client:
        if topic in PRODUCT_TOPICS:
            t = threading.Thread(
                target=_sync_product_to_bluefly,
                args=(file_path, record),
                daemon=True,
            )
            t.start()
        elif topic in INVENTORY_TOPICS:
            t = threading.Thread(
                target=_sync_inventory_to_bluefly,
                args=(file_path, record),
                daemon=True,
            )
            t.start()

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
