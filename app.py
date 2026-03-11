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
from d1_client import get_d1_client
from field_mapper import (
    should_sync_product,
    is_default_only_product,
    get_metafield,
    build_bluefly_payload,
    build_quantity_price_payload,
    extract_qty_price_payload,
)

load_dotenv()

# Per-product lock to serialize concurrent webhook processing
_product_locks: dict[int, threading.Lock] = {}
_product_locks_guard = threading.Lock()


def _get_product_lock(product_id: int) -> threading.Lock:
    with _product_locks_guard:
        if product_id not in _product_locks:
            _product_locks[product_id] = threading.Lock()
        return _product_locks[product_id]


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
    return e.get_response()


@app.errorhandler(500)
def app_500(e):
    from flask import request as req, jsonify as jf
    if req.path.startswith('/api/'):
        return jf({"error": "Internal server error"}), 500
    return e.get_response()


@app.errorhandler(Exception)
def app_exception(e):
    from flask import request as req, jsonify as jf
    import traceback
    traceback.print_exc()
    if req.path.startswith('/api/'):
        return jf({"error": str(e)}), 500
    return f"Internal Server Error: {e}", 500

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

def _is_published_to_bluefly(product_id) -> bool:
    """Return True if this product has ever been successfully pushed to Bluefly."""
    import glob as _glob
    pattern = os.path.join(PIPELINE_LOG_DIR, "*", "*.json")
    pid_str = str(product_id)
    for fpath in _glob.glob(pattern):
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                rec = json.load(f)
            pid = str(rec.get("product_id", ""))
            for stage in rec.get("stages", []):
                if stage.get("stage") == "enriched":
                    ep = str((stage.get("data") or {}).get("product_id", ""))
                    if ep:
                        pid = ep
                    break
            if pid == pid_str and rec.get("status") == "pushed":
                return True
        except (OSError, json.JSONDecodeError):
            continue
    return False


def _sync_product_to_bluefly(file_path: str, record: dict):
    """Background: enrich, map, and push a product event to Bluefly."""
    topic = record["topic"]
    product_id = record["payload"].get("id")

    # Serialize processing per product so concurrent webhooks don't race
    with _get_product_lock(product_id):
        _sync_product_to_bluefly_inner(file_path, record, topic, product_id)


def _sync_product_to_bluefly_inner(file_path, record, topic, product_id):
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

        # Load config once — used for eligibility checks, price adjustment, and field defaults
        _cfg = {}
        try:
            import dashboard.routes as _dr
            _cfg = _dr.load_config()
        except Exception:
            pass
        _adj = _cfg.get("price_adjustment_pct", 0)
        _field_defaults = _cfg.get("field_defaults", {})
        _elig = _cfg.get("eligibility", {})

        if not should_sync_product(enriched):
            # Auto-draft: if product was on Bluefly (has category), set NotLive
            if category_id and bluefly_client:
                print(f"[Pipeline] Product not ACTIVE -- auto-drafting on Bluefly")
                try:
                    qp = build_quantity_price_payload(enriched, metafields, seller_id=BLUEFLY_SELLER_ID)
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

        if _elig.get("require_images", True) and not enriched.get("images"):
            print(f"[Pipeline] No images -- skipping")
            pipeline.update_stage(job_path, "skipped", {"reason": "no images"})
            tx_logger.update_status(file_path, "processed")
            return

        if _elig.get("require_quantity", True):
            total_qty = sum((v.get("inventoryQuantity") or 0) for v in enriched.get("variants", []))
            if total_qty <= 0:
                print(f"[Pipeline] No inventory -- skipping")
                pipeline.update_stage(job_path, "skipped", {"reason": "no inventory"})
                tx_logger.update_status(file_path, "processed")
                return

        if is_default_only_product(enriched.get("variants", [])):
            print(f"[Pipeline] Default Title only -- skipping")
            pipeline.update_stage(job_path, "skipped", {"reason": "default_title_only"})
            tx_logger.update_status(file_path, "processed")
            return

        # Only update products already published to Bluefly via the dashboard.
        # Prevents webhook events from auto-publishing products that were never
        # manually pushed, or that were intentionally reset/removed.
        if not _is_published_to_bluefly(product_id):
            print(f"[Pipeline] Not yet published to Bluefly — skipping (push manually from dashboard)")
            pipeline.update_stage(job_path, "skipped", {"reason": "not yet published — push manually"})
            tx_logger.update_status(file_path, "processed")
            return

        # Apply per-product overrides (same as dashboard push)
        try:
            from product_store import get_product_store
            from dashboard.routes import _patch_metafield, _apply_image_overrides, _resolve_bpid_template
            _store = get_product_store()
            _ovr = _store.get_all_overrides_for_product(str(product_id))

            if _ovr["category"]:
                category_id = _ovr["category"]
                _patch_metafield(metafields, "custom", "bluefly_category", _ovr["category"])
            if _ovr["size_field"]:
                _patch_metafield(metafields, "custom", "size_field_override", _ovr["size_field"])
            if _ovr["title"]:
                enriched["title"] = _ovr["title"]
            if _ovr["vendor"]:
                enriched["vendor"] = _ovr["vendor"]
            if _ovr["brand_product_id"]:
                _variants = enriched.get("variants", [])
                bpid_product = {
                    "id": enriched.get("id", product_id),
                    "title": enriched.get("title", ""),
                    "vendor": enriched.get("vendor", ""),
                    "product_type": enriched.get("productType", ""),
                    "first_sku": (_variants[0].get("sku", "") if _variants else ""),
                    "first_variant_sku": (_variants[0].get("sku", "") if _variants else ""),
                    "brand_product_id": get_metafield(metafields, "custom", "brand_product_id") or "",
                    "gender": get_metafield(metafields, "custom", "gender") or "",
                    "color": get_metafield(metafields, "custom", "color") or "",
                    "variant_ids": [str(v.get("id", "")).split("/")[-1] for v in _variants if v.get("id")],
                    "variant_skus": [v.get("sku", "") for v in _variants if v.get("sku")],
                }
                resolved = _resolve_bpid_template(_ovr["brand_product_id"], bpid_product)
                if resolved:
                    _patch_metafield(metafields, "custom", "brand_product_id", resolved)
            # Price override
            price_ov_raw = _ovr["price"]
            if price_ov_raw:
                try:
                    price_ov = json.loads(price_ov_raw) if isinstance(price_ov_raw, str) else price_ov_raw
                except (json.JSONDecodeError, TypeError):
                    price_ov = None
                if price_ov:
                    if price_ov.get("type") == "fixed":
                        for v in enriched.get("variants", []):
                            v["price"] = str(price_ov["value"])
                        _adj = 0
                    elif price_ov.get("type") == "pct":
                        _adj = float(price_ov["value"])
            # Image overrides
            _apply_image_overrides(enriched, product_id, image_overrides=_ovr["image"])
            ovr_applied = [k for k in ("category", "size_field", "title", "vendor", "brand_product_id", "price") if _ovr.get(k)]
            if ovr_applied or _ovr.get("image"):
                print(f"[Pipeline] Overrides applied: {', '.join(ovr_applied)}" + (f", images: {len(_ovr['image'])}" if _ovr.get("image") else ""))
        except Exception as ovr_err:
            print(f"[Pipeline] Override loading skipped: {ovr_err}")

        # Map (SQL lookup with graceful fallback)
        pipeline.update_stage(job_path, "mapping")
        sql_field_map = {}
        variants = enriched.get("variants", [])
        variant_titles = [v.get("title", "") for v in variants]
        try:
            db = BlueflyDBLookup(get_d1_client())
            vt_list = [v.get("title", "") for v in variants if v.get("title", "")]
            sql_field_map = db.lookup_category_fields_batch(category_id, vt_list)
        except Exception as e:
            print(f"[Pipeline] SQL lookup skipped: {e}")
        missed = [t for t in vt_list if t not in sql_field_map]
        if missed:
            print(f"[Pipeline] bf_mapping miss: cat={category_id} {missed}")
        tx_logger.log({
            "type": "sql_db",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "topic": "sql/lookup",
            "product_id": product_id,
            "category_id": category_id,
            "variant_count": len(sql_field_map),
            "field_count": sum(len(v) for v in sql_field_map.values()),
            "status": "unread",
        })

        # All product events (create + update) use /v2/products with full payload.
        # This endpoint is an upsert — creates new products or updates all fields
        # (color, size, images, options, etc.) on existing products.
        bluefly_payload = build_bluefly_payload(
            enriched, metafields, sql_field_map,
            price_adjustment_pct=_adj,
            field_defaults=_field_defaults,
            seller_id=BLUEFLY_SELLER_ID,
        )

        # Block push if any variants have unmappable sizes
        size_errors = bluefly_payload.pop("_size_errors", None)
        if size_errors:
            pipeline.update_stage(job_path, "size_error", {
                "size_errors": size_errors,
                "product_id": str(product_id),
                "source": "webhook",
            })
            tx_logger.update_status(file_path, "size_error")
            print(f"[Pipeline] BLOCKED: {len(size_errors)} size error(s) — fix mappings before push")
            return

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
            # Follow-up: set inventory via quantityprice (products POST may not update qty)
            try:
                qp = extract_qty_price_payload(bluefly_payload)
                bluefly_client.update_quantity_price([qp])
                print(f"[Pipeline] OK quantityprice follow-up sent")
            except Exception as qp_err:
                print(f"[Pipeline] quantityprice follow-up failed: {qp_err}")
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

        # Serialize processing per product so concurrent webhooks don't race
        with _get_product_lock(product_id):
            _sync_inventory_inner(file_path, record, job_path, product_id, variant_sku, inventory_item_id, new_available)

    except Exception as e:
        print(f"[Pipeline] ERROR: {e}")
        pipeline.update_stage(job_path, "error", error=str(e))
        tx_logger.update_status(file_path, "error")


def _sync_inventory_inner(file_path, record, job_path, product_id, variant_sku, inventory_item_id, new_available):
    # Correct the job's product_id — it was created with inventory_item_id
    pipeline.patch_product_id(job_path, product_id)

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

    # Load config once — used for eligibility checks, price adjustment, and field defaults
    _cfg_inv = {}
    try:
        import dashboard.routes as _dr_inv
        _cfg_inv = _dr_inv.load_config()
    except Exception:
        pass
    _adj_inv = _cfg_inv.get("price_adjustment_pct", 0)
    _field_defaults_inv = _cfg_inv.get("field_defaults", {})
    _elig_inv = _cfg_inv.get("eligibility", {})

    if _elig_inv.get("require_images", True) and not enriched.get("images"):
        pipeline.update_stage(job_path, "skipped", {"reason": "no images"})
        tx_logger.update_status(file_path, "processed")
        return

    if _elig_inv.get("require_quantity", True):
        total_qty = sum((v.get("inventoryQuantity") or 0) for v in enriched.get("variants", []))
        if total_qty <= 0:
            pipeline.update_stage(job_path, "skipped", {"reason": "no inventory"})
            tx_logger.update_status(file_path, "processed")
            return

    # Only update products already published to Bluefly via the dashboard.
    if not _is_published_to_bluefly(product_id):
        print(f"[Pipeline/inv] Not yet published to Bluefly — skipping")
        pipeline.update_stage(job_path, "skipped", {"reason": "not yet published — push manually"})
        tx_logger.update_status(file_path, "processed")
        return

    # Apply per-product overrides (same as dashboard push)
    try:
        from product_store import get_product_store
        from dashboard.routes import _patch_metafield, _apply_image_overrides, _resolve_bpid_template
        _store_inv = get_product_store()
        _ovr_inv = _store_inv.get_all_overrides_for_product(str(product_id))

        if _ovr_inv["category"]:
            category_id = _ovr_inv["category"]
            _patch_metafield(metafields, "custom", "bluefly_category", _ovr_inv["category"])
        if _ovr_inv["size_field"]:
            _patch_metafield(metafields, "custom", "size_field_override", _ovr_inv["size_field"])
        if _ovr_inv["title"]:
            enriched["title"] = _ovr_inv["title"]
        if _ovr_inv["vendor"]:
            enriched["vendor"] = _ovr_inv["vendor"]
        if _ovr_inv["brand_product_id"]:
            _variants_inv = enriched.get("variants", [])
            bpid_product = {
                "id": enriched.get("id", product_id),
                "title": enriched.get("title", ""),
                "vendor": enriched.get("vendor", ""),
                "product_type": enriched.get("productType", ""),
                "first_sku": (_variants_inv[0].get("sku", "") if _variants_inv else ""),
                "first_variant_sku": (_variants_inv[0].get("sku", "") if _variants_inv else ""),
                "brand_product_id": get_metafield(metafields, "custom", "brand_product_id") or "",
                "gender": get_metafield(metafields, "custom", "gender") or "",
                "color": get_metafield(metafields, "custom", "color") or "",
                "variant_ids": [str(v.get("id", "")).split("/")[-1] for v in _variants_inv if v.get("id")],
                "variant_skus": [v.get("sku", "") for v in _variants_inv if v.get("sku")],
            }
            resolved = _resolve_bpid_template(_ovr_inv["brand_product_id"], bpid_product)
            if resolved:
                _patch_metafield(metafields, "custom", "brand_product_id", resolved)
        # Price override
        price_ov_raw = _ovr_inv["price"]
        if price_ov_raw:
            try:
                price_ov = json.loads(price_ov_raw) if isinstance(price_ov_raw, str) else price_ov_raw
            except (json.JSONDecodeError, TypeError):
                price_ov = None
            if price_ov:
                if price_ov.get("type") == "fixed":
                    for v in enriched.get("variants", []):
                        v["price"] = str(price_ov["value"])
                    _adj_inv = 0
                elif price_ov.get("type") == "pct":
                    _adj_inv = float(price_ov["value"])
        # Image overrides
        _apply_image_overrides(enriched, product_id, image_overrides=_ovr_inv["image"])
        ovr_applied = [k for k in ("category", "size_field", "title", "vendor", "brand_product_id", "price") if _ovr_inv.get(k)]
        if ovr_applied or _ovr_inv.get("image"):
            print(f"[Pipeline/inv] Overrides applied: {', '.join(ovr_applied)}" + (f", images: {len(_ovr_inv['image'])}" if _ovr_inv.get("image") else ""))
    except Exception as ovr_err:
        print(f"[Pipeline/inv] Override loading skipped: {ovr_err}")

    pipeline.update_stage(job_path, "mapping")
    sql_field_map = {}
    variants = enriched.get("variants", [])
    variant_titles = [v.get("title", "") for v in variants]
    try:
        db = BlueflyDBLookup(get_d1_client())
        vt_list = [v.get("title", "") for v in variants if v.get("title", "")]
        sql_field_map = db.lookup_category_fields_batch(category_id, vt_list)
    except Exception as e:
        print(f"[Pipeline/inv] SQL lookup skipped: {e}")
    missed = [t for t in vt_list if t not in sql_field_map]
    if missed:
        print(f"[Pipeline/inv] bf_mapping miss: cat={category_id} {missed}")
    tx_logger.log({
        "type": "sql_db",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "topic": "sql/lookup",
        "product_id": product_id,
        "category_id": category_id,
        "variant_count": len(sql_field_map),
        "field_count": sum(len(v) for v in sql_field_map.values()),
        "status": "unread",
    })

    # Log Shopify quantities vs webhook quantity to detect race conditions
    for v in enriched.get("variants", []):
        print(f"[Pipeline/inv] variant={v.get('title')} shopify_qty={v.get('inventoryQuantity', '?')}")
    print(f"[Pipeline/inv] webhook says qty={new_available} for item={inventory_item_id}")

    # Full payload with merge logic so merged variants (e.g. 95B+95C+95D → XXL)
    # get summed quantities and a single SellerSKU.
    bluefly_payload = build_bluefly_payload(
        enriched, metafields, sql_field_map,
        price_adjustment_pct=_adj_inv,
        field_defaults=_field_defaults_inv,
        seller_id=BLUEFLY_SELLER_ID,
    )
    # Block push if any variants have unmappable sizes
    size_errors = bluefly_payload.pop("_size_errors", None)
    if size_errors:
        pipeline.update_stage(job_path, "size_error", {
            "size_errors": size_errors,
            "product_id": str(product_id),
            "source": "inventory-webhook",
        })
        tx_logger.update_status(file_path, "size_error")
        print(f"[Pipeline/inv] BLOCKED: {len(size_errors)} size error(s)")
        return

    pipeline.update_stage(job_path, "mapped", {
        "variant_sku": variant_sku,
        "quantity": new_available,
        "endpoint": "products",
    })

    pipeline.update_stage(job_path, "pushing")
    result = bluefly_client.push_products([bluefly_payload])

    if result["success"]:
        print(f"[Pipeline] OK Inventory pushed (products): HTTP {result['status_code']}")
        # Follow-up: set inventory via quantityprice
        try:
            qp = extract_qty_price_payload(bluefly_payload)
            bluefly_client.update_quantity_price([qp])
            print(f"[Pipeline] OK quantityprice follow-up sent")
        except Exception as qp_err:
            print(f"[Pipeline] quantityprice follow-up failed: {qp_err}")
        pipeline.update_stage(job_path, "pushed", {
            "response_status": result["status_code"],
            "endpoint": "products+quantityprice",
        })
        tx_logger.update_status(file_path, "processed")
    else:
        raise RuntimeError(f"Bluefly API error (products): {result['error']}")


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

    redirect_uri = "https://accessxbrooklyn-bf.iconluxury.today/auth/callback"
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
    from waitress import serve
    serve(app, host="0.0.0.0", port=PORT, threads=8)
