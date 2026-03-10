"""
Dashboard Blueprint — web UI + JSON API for Bluefly sync management.

Provides:
  GET  /                             Serve the dashboard HTML
  GET  /api/products                 List Bluefly-eligible products
  POST /api/products/push            Push single product to Bluefly
  POST /api/products/push-bulk       Push all unpushed eligible products (NDJSON)
  POST /api/products/set-status      Change listing status on Bluefly
  POST /api/products/sync-qty-price  Sync qty+price for a published product
  GET  /api/bluefly/catalog          Fetch live Bluefly catalog
  GET  /api/events                   List webhook events
  POST /api/events/process           Trigger processing of unread events
  GET  /api/settings                 Read config.json
  POST /api/settings                 Update config.json
  GET  /api/pipeline/jobs            List recent pipeline jobs
  GET  /api/field-mapping            Get field mapping definitions
  GET  /api/bluefly/field-catalog    Get full Bluefly field catalog (TSV-backed)
  GET  /api/orders                  Fetch orders from Bluefly/Rithum
  POST /api/orders/acknowledge      Acknowledge an order
  POST /api/orders/fulfill          Submit fulfillment/tracking data
  POST /api/orders/cancel           Cancel an unfillable order
"""

import os
import csv
import json
import glob
import threading
from datetime import datetime, timezone

from flask import Blueprint, render_template, request, jsonify, Response, stream_with_context

from field_mapper import (
    should_sync_product,
    is_default_only_product,
    get_metafield,
    build_bluefly_payload,
    build_quantity_price_payload,
)

dashboard_bp = Blueprint("dashboard", __name__)

CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.json")

# Default configuration structure
DEFAULT_CONFIG = {
    "price_adjustment_pct": 0,
    "eligibility": {
        "require_category": True,
        "require_quantity": True,
        "require_images": True,
    },
    "field_defaults": {
        "is_returnable": "Not Returnable",
        "product_condition": "New",
        "listing_status": "Live",
    },
}


@dashboard_bp.after_request
def add_no_cache(response):
    """Prevent browser caching on API responses."""
    if response.content_type and 'json' in response.content_type:
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
        response.headers['Pragma'] = 'no-cache'
    return response


@dashboard_bp.errorhandler(Exception)
def handle_exception(e):
    """Catch-all: always return JSON for API errors, never HTML."""
    import traceback
    traceback.print_exc()
    return jsonify({"error": str(e)}), 500


@dashboard_bp.errorhandler(404)
def handle_404(e):
    return jsonify({"error": "Not found"}), 404


@dashboard_bp.errorhandler(500)
def handle_500(e):
    return jsonify({"error": "Internal server error"}), 500


# -----------------------------------------------------------------------
# Config helpers
# -----------------------------------------------------------------------

def load_config() -> dict:
    try:
        with open(CONFIG_PATH, "r") as f:
            cfg = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        cfg = {}

    # Merge with defaults so new keys always exist
    merged = {**DEFAULT_CONFIG}
    merged.update(cfg)
    # Deep merge nested dicts
    for key in ("eligibility", "field_defaults"):
        merged[key] = {**DEFAULT_CONFIG.get(key, {}), **cfg.get(key, {})}
    return merged


def save_config(cfg: dict):
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)
        f.write("\n")


# -----------------------------------------------------------------------
# Lazy access to shared app objects
# -----------------------------------------------------------------------

def _get_clients():
    """Import shared clients from the app module (avoids circular imports)."""
    import app as app_module
    return {
        "shopify": app_module.shopify_client,
        "bluefly": app_module.bluefly_client,
        "tx_logger": app_module.tx_logger,
        "pipeline": app_module.pipeline,
    }


def _get_db():
    """Create a fresh DB connection for dashboard operations."""
    import app as app_module
    from sql_lookup import BlueflyDBLookup
    return BlueflyDBLookup(
        app_module.SQL_SERVER,
        app_module.SQL_DATABASE,
        app_module.SQL_USER,
        app_module.SQL_PASSWORD,
    )


# -----------------------------------------------------------------------
# Page route
# -----------------------------------------------------------------------

@dashboard_bp.route("/")
def index():
    return render_template("dashboard.html")


# -----------------------------------------------------------------------
# Image format validation
# -----------------------------------------------------------------------

_ALLOWED_IMAGE_EXTS = {'.jpg', '.jpeg', '.png', '.gif'}


def _has_valid_image_format(image_url: str) -> bool:
    """Check if an image URL has an allowed extension (jpg, jpeg, png, gif)."""
    if not image_url:
        return False
    from urllib.parse import urlparse
    path = urlparse(image_url).path.lower()
    # Strip query params that Shopify sometimes adds
    for ext in _ALLOWED_IMAGE_EXTS:
        if path.endswith(ext):
            return True
    return False


# -----------------------------------------------------------------------
# Products API
# -----------------------------------------------------------------------

def _enrich_products(products, synced_products, cfg):
    """Add sync status, eligibility flags, and adjusted price to each product."""
    adj = cfg.get("price_adjustment_pct", 0)
    elig = cfg.get("eligibility", {})
    require_quantity = elig.get("require_quantity", True)
    require_images = elig.get("require_images", True)

    for p in products:
        pid = p["id"]
        if pid in synced_products:
            p["sync_status"] = synced_products[pid]["status"]
            p["sync_time"] = synced_products[pid].get("time")
        else:
            p["sync_status"] = "never"
            p["sync_time"] = None

        if p["sync_status"] == "pushed" and p.get("has_default_variants"):
            p["invalid"] = True
            p["invalid_reason"] = "Published with Default Title/size variants — should be removed from Bluefly"
        else:
            p["invalid"] = False

        p["has_images"] = bool(p.get("image_url"))
        p["has_quantity"] = (p.get("total_quantity", 0) or 0) > 0
        p["valid_image_format"] = _has_valid_image_format(p.get("image_url", ""))

        p["push_eligible"] = True
        if not p.get("bluefly_category"):
            p["push_eligible"] = False
        if p.get("has_default_variants"):
            p["push_eligible"] = False
        if require_quantity and not p["has_quantity"]:
            p["push_eligible"] = False
        if require_images and not p["has_images"]:
            p["push_eligible"] = False
        if p["has_images"] and not p["valid_image_format"]:
            p["push_eligible"] = False
            p["ineligible_reason"] = "Image format not supported (must be jpg, png, or gif)"

        try:
            raw = float(p.get("first_price") or 0)
            p["adjusted_price"] = round(raw * (1 + adj / 100), 2) if raw else None
        except (ValueError, TypeError):
            p["adjusted_price"] = None

    return products


def _fetch_and_enrich():
    """Fetch all Shopify products, enrich, and return (products, cfg, synced)."""
    clients = _get_clients()
    shopify = clients["shopify"]
    if not shopify:
        return None, None, None, "Shopify client not configured"

    try:
        all_products = shopify.list_products(query_filter="status:active")
    except Exception as e:
        return None, None, None, str(e)

    cfg = load_config()
    pipeline_dir = clients["pipeline"].log_dir
    synced_products = _get_synced_product_ids(pipeline_dir)
    _enrich_products(all_products, synced_products, cfg)
    return all_products, cfg, synced_products, None


@dashboard_bp.route("/api/products")
def api_products():
    """List all products from Shopify with sync status."""
    all_products, cfg, synced_products, err = _fetch_and_enrich()
    if err:
        return jsonify({"error": err}), 500

    import app as _app
    return jsonify({
        "products": all_products,
        "total": len(all_products),
        "price_adjustment_pct": cfg.get("price_adjustment_pct", 0),
        "eligibility": cfg.get("eligibility", {}),
        "store": _app.SHOPIFY_STORE,
    })


@dashboard_bp.route("/api/stats")
def api_stats():
    """Lightweight dashboard stats from pipeline logs only (no Shopify API calls)."""
    clients = _get_clients()
    pipeline_dir = clients["pipeline"].log_dir
    synced_products = _get_synced_product_ids(pipeline_dir)

    synced_count = sum(1 for info in synced_products.values()
                       if info.get("status") == "pushed")
    return jsonify({
        "synced": synced_count,
    })


@dashboard_bp.route("/api/products/published")
def api_products_published():
    """Stream published products as NDJSON — fetches only pushed product IDs."""
    clients = _get_clients()
    shopify = clients["shopify"]
    if not shopify:
        return jsonify({"error": "Shopify client not configured"}), 500

    cfg = load_config()
    pipeline_dir = clients["pipeline"].log_dir
    synced_products = _get_synced_product_ids(pipeline_dir)
    pushed_ids = [pid for pid, info in synced_products.items()
                  if info.get("status") == "pushed"]

    import app as _app

    # Use nodes query — one GraphQL call per 50 IDs, no pagination needed
    print(f"[Published] Fetching {len(pushed_ids)} pushed products by ID")
    products = shopify.get_products_by_ids(pushed_ids)
    _enrich_products(products, synced_products, cfg)
    print(f"[Published] Done — {len(products)} products returned")

    return jsonify({
        "products": products,
        "total": len(products),
        "price_adjustment_pct": cfg.get("price_adjustment_pct", 0),
        "eligibility": cfg.get("eligibility", {}),
        "store": _app.SHOPIFY_STORE,
    })


@dashboard_bp.route("/api/products/unpublished")
def api_products_unpublished():
    """Stream unpublished products as NDJSON, one batch per Shopify page."""
    return _stream_products(filter_status="not_pushed")


def _stream_products(filter_status=None):
    """Stream enriched products as NDJSON batches.

    Each line is a JSON object:
      {"type":"meta", ...}      — first line with config
      {"type":"batch", "products":[...], "done":false}
      {"type":"batch", "products":[...], "done":true}  — last batch
    """
    clients = _get_clients()
    shopify = clients["shopify"]
    if not shopify:
        return jsonify({"error": "Shopify client not configured"}), 500

    cfg = load_config()
    pipeline_dir = clients["pipeline"].log_dir
    synced_products = _get_synced_product_ids(pipeline_dir)

    import app as _app

    def generate():
        yield json.dumps({
            "type": "meta",
            "price_adjustment_pct": cfg.get("price_adjustment_pct", 0),
            "eligibility": cfg.get("eligibility", {}),
            "store": _app.SHOPIFY_STORE,
        }) + "\n"

        total_sent = 0
        total_shopify = 0
        for page_products, has_more in shopify.list_products_pages(query_filter="status:active"):
            total_shopify += len(page_products)
            _enrich_products(page_products, synced_products, cfg)

            if filter_status == "pushed":
                batch = [p for p in page_products if p["sync_status"] == "pushed"]
            elif filter_status == "not_pushed":
                batch = [p for p in page_products if p["sync_status"] != "pushed"]
            else:
                batch = page_products

            total_sent += len(batch)
            print(f"[Stream] filter={filter_status} page={total_shopify} batch={len(batch)} total_sent={total_sent} has_more={has_more}")

            yield json.dumps({
                "type": "batch",
                "products": batch,
                "done": not has_more,
            }) + "\n"

        print(f"[Stream] DONE filter={filter_status} total_shopify={total_shopify} total_sent={total_sent}")

    return Response(stream_with_context(generate()),
                    content_type="application/x-ndjson")


def _get_synced_product_ids(pipeline_dir: str) -> dict:
    """Scan pipeline_logs to find the latest status per product_id.

    For inventory-triggered jobs the top-level product_id was historically
    recorded as inventory_item_id.  Fall back to the enriched-stage
    product_id (the real Shopify ID) when present.
    """
    result = {}
    pattern = os.path.join(pipeline_dir, "*", "*.json")
    for fpath in sorted(glob.glob(pattern), key=os.path.getmtime):
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                record = json.load(f)
            pid = record.get("product_id")
            # For inventory webhooks the real Shopify product_id is stored
            # in the enriched stage data — prefer that when available.
            for stage in record.get("stages", []):
                if stage.get("stage") == "enriched":
                    enriched_pid = (stage.get("data") or {}).get("product_id")
                    if enriched_pid:
                        pid = enriched_pid
                    break
            if pid:
                result[pid] = {
                    "status": record.get("status", "unknown"),
                    "time": record.get("created_at"),
                }
        except (json.JSONDecodeError, OSError):
            continue
    return result


@dashboard_bp.route("/api/products/push", methods=["POST"])
def api_push_product():
    """Push a single product to Bluefly."""
    data = request.get_json(force=True)
    product_id = data.get("product_id")
    if not product_id:
        return jsonify({"error": "product_id required"}), 400

    clients = _get_clients()
    shopify = clients["shopify"]
    bluefly = clients["bluefly"]
    pipeline = clients["pipeline"]

    if not shopify or not bluefly:
        return jsonify({"error": "Clients not configured"}), 500

    try:
        # Enrich
        enriched = shopify.get_product_full(int(product_id))
        if not enriched:
            return jsonify({"error": f"Product {product_id} not found"}), 404

        metafields = enriched.get("metafields", [])
        category_id = get_metafield(metafields, "custom", "bluefly_category")
        if not category_id:
            return jsonify({"error": "No bluefly_category metafield"}), 400

        if is_default_only_product(enriched.get("variants", [])):
            return jsonify({"error": "Product has no real variants (Default Title only)"}), 400

        if not enriched.get("images"):
            return jsonify({"error": "Product has no images"}), 400

        # Validate image formats (must be jpg, png, or gif)
        for img in enriched.get("images", []):
            img_url = img.get("url", "") if isinstance(img, dict) else str(img)
            if img_url and not _has_valid_image_format(img_url):
                return jsonify({"error": f"Image format not supported (must be jpg, png, or gif): {img_url}"}), 400

        # SQL lookup (graceful fallback)
        sql_field_map = {}
        variants = enriched.get("variants", [])
        variant_titles = [v.get("title", "") for v in variants]
        print(f"[Dashboard] SQL lookup: category={category_id}, "
              f"{len(variants)} variants, titles={variant_titles}")
        try:
            db = _get_db()
            with db:
                for variant in variants:
                    vt = variant.get("title", "")
                    if vt:
                        sql_field_map[vt] = db.lookup_category_fields(category_id, vt)
                    else:
                        print(f"[Dashboard] SQL skipped variant with empty title")
        except Exception as e:
            print(f"[Dashboard] SQL lookup skipped: {e}")
        total_fields = sum(len(v) for v in sql_field_map.values())
        print(f"[Dashboard] SQL done: {len(sql_field_map)} variants mapped, {total_fields} fields")

        # Build payload with price adjustment and field defaults
        cfg = load_config()
        adj = cfg.get("price_adjustment_pct", 0)
        field_defaults = cfg.get("field_defaults", {})
        bluefly_payload = build_bluefly_payload(
            enriched, metafields, sql_field_map,
            price_adjustment_pct=adj,
            field_defaults=field_defaults,
            seller_id=bluefly.seller_id,
        )

        # Create pipeline job
        job_path = pipeline.create_job(
            source_file="dashboard-push",
            topic="dashboard/push",
            product_id=product_id,
            event_id=f"dash-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        )

        # Push — use PUT if already published (full overwrite), POST for first push
        pipeline_dir = pipeline.log_dir
        synced = _get_synced_product_ids(pipeline_dir)
        already_published = synced.get(int(product_id), {}).get("status") == "pushed"
        if already_published:
            result = bluefly.update_products_full([bluefly_payload])
            endpoint_label = "products-put"
        else:
            result = bluefly.push_products([bluefly_payload])
            endpoint_label = "products-post"

        if result["success"]:
            pipeline.update_stage(job_path, "pushed", {
                "response_status": result["status_code"],
                "endpoint": endpoint_label,
                "sku": bluefly_payload.get("SellerSKU", ""),
                "source": "dashboard",
            })
            return jsonify({
                "success": True,
                "status_code": result["status_code"],
                "product_title": enriched.get("title", ""),
                "sku": bluefly_payload.get("SellerSKU", ""),
            })
        else:
            pipeline.update_stage(job_path, "error", error=result["error"])
            return jsonify({"error": result["error"]}), 502

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route("/api/products/push-bulk", methods=["POST"])
def api_push_bulk():
    """Push all unpushed eligible products. Streams NDJSON progress.

    Body params (JSON, all optional):
      force (bool): If true, re-sync all eligible products regardless of previous push history.
                    Use this to backfill after downtime when webhook events were missed.
    """
    body = request.get_json(silent=True) or {}
    force = bool(body.get("force", False))

    def generate():
        clients = _get_clients()
        shopify = clients["shopify"]
        bluefly = clients["bluefly"]
        pipeline = clients["pipeline"]

        if not shopify or not bluefly:
            yield json.dumps({"error": "Clients not configured"}) + "\n"
            return

        try:
            all_products = shopify.list_products(query_filter="status:active")
            eligible = [p for p in all_products if p.get("bluefly_category")]
        except Exception as e:
            yield json.dumps({"error": str(e)}) + "\n"
            return

        # Apply eligibility filters
        cfg = load_config()
        elig = cfg.get("eligibility", {})

        if elig.get("require_quantity", True):
            eligible = [p for p in eligible if (p.get("total_quantity", 0) or 0) > 0]
        if elig.get("require_images", True):
            eligible = [p for p in eligible if p.get("image_url")]

        # Check which are already synced (skipped when force=True)
        synced = _get_synced_product_ids(pipeline.log_dir)
        if force:
            to_push = eligible
        else:
            to_push = [p for p in eligible if p["id"] not in synced or synced[p["id"]]["status"] != "pushed"]

        yield json.dumps({"type": "start", "total": len(to_push)}) + "\n"

        adj = cfg.get("price_adjustment_pct", 0)
        field_defaults = cfg.get("field_defaults", {})
        success_count = 0
        error_count = 0

        for i, product in enumerate(to_push):
            pid = product["id"]
            try:
                enriched = shopify.get_product_full(pid)
                if not enriched:
                    yield json.dumps({"type": "error", "product_id": pid, "error": "Not found"}) + "\n"
                    error_count += 1
                    continue

                metafields = enriched.get("metafields", [])
                category_id = get_metafield(metafields, "custom", "bluefly_category")
                if not category_id:
                    yield json.dumps({"type": "skip", "product_id": pid, "reason": "no category"}) + "\n"
                    continue

                if is_default_only_product(enriched.get("variants", [])):
                    yield json.dumps({"type": "skip", "product_id": pid, "reason": "default_title_only"}) + "\n"
                    continue

                if elig.get("require_images", True) and not enriched.get("images"):
                    yield json.dumps({"type": "skip", "product_id": pid, "reason": "no images"}) + "\n"
                    continue

                sql_field_map = {}
                try:
                    db = _get_db()
                    with db:
                        for variant in enriched.get("variants", []):
                            vt = variant.get("title", "")
                            if vt:
                                sql_field_map[vt] = db.lookup_category_fields(category_id, vt)
                except Exception:
                    pass

                payload = build_bluefly_payload(
                    enriched, metafields, sql_field_map,
                    price_adjustment_pct=adj,
                    field_defaults=field_defaults,
                    seller_id=bluefly.seller_id,
                )

                job_path = pipeline.create_job(
                    source_file="dashboard-bulk-push",
                    topic="dashboard/push-bulk",
                    product_id=pid,
                    event_id=f"bulk-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
                )

                already_published = synced.get(pid, {}).get("status") == "pushed"
                if already_published:
                    result = bluefly.update_products_full([payload])
                    endpoint_label = "products-put"
                else:
                    result = bluefly.push_products([payload])
                    endpoint_label = "products-post"

                if result["success"]:
                    pipeline.update_stage(job_path, "pushed", {
                        "response_status": result["status_code"],
                        "endpoint": endpoint_label,
                        "sku": payload.get("SellerSKU", ""),
                        "source": "dashboard-bulk",
                    })
                    success_count += 1
                    yield json.dumps({
                        "type": "success",
                        "product_id": pid,
                        "title": enriched.get("title", ""),
                        "index": i + 1,
                        "total": len(to_push),
                    }) + "\n"
                else:
                    pipeline.update_stage(job_path, "error", error=result["error"])
                    error_count += 1
                    yield json.dumps({
                        "type": "error",
                        "product_id": pid,
                        "error": result["error"],
                    }) + "\n"

            except Exception as e:
                error_count += 1
                yield json.dumps({"type": "error", "product_id": pid, "error": str(e)}) + "\n"

        yield json.dumps({
            "type": "complete",
            "success": success_count,
            "errors": error_count,
            "total": len(to_push),
        }) + "\n"

    return Response(
        stream_with_context(generate()),
        mimetype="application/x-ndjson",
    )


@dashboard_bp.route("/api/products/reset-sync", methods=["POST"])
def api_reset_sync():
    """Delete pipeline log entries for given product IDs so they appear as 'never synced'.

    Body (JSON):
      product_ids: list of product IDs to reset  — OR —
      all: true    to clear every pipeline log entry
    """
    data = request.get_json(force=True) or {}
    reset_all = bool(data.get("all", False))
    product_ids = set(str(pid) for pid in (data.get("product_ids") or []))

    if not reset_all and not product_ids:
        return jsonify({"error": "product_ids or all required"}), 400

    clients = _get_clients()
    pipeline_dir = clients["pipeline"].log_dir
    pattern = os.path.join(pipeline_dir, "*", "*.json")

    deleted = 0
    for fpath in glob.glob(pattern):
        try:
            if reset_all:
                os.remove(fpath)
                deleted += 1
            else:
                with open(fpath, "r", encoding="utf-8") as f:
                    record = json.load(f)
                pid = str(record.get("product_id", ""))
                # Also check enriched-stage product_id (inventory jobs)
                for stage in record.get("stages", []):
                    if stage.get("stage") == "enriched":
                        ep = str((stage.get("data") or {}).get("product_id", ""))
                        if ep:
                            pid = ep
                        break
                if pid in product_ids:
                    os.remove(fpath)
                    deleted += 1
        except (OSError, json.JSONDecodeError):
            continue

    return jsonify({"reset": deleted})


@dashboard_bp.route("/api/products/set-status", methods=["POST"])
def api_set_status():
    """Change a product's listing status on Bluefly (e.g. NotLive to draft)."""
    data = request.get_json(force=True)
    product_id = data.get("product_id")
    new_status = data.get("status", "NotLive")  # "Live" or "NotLive"

    if not product_id:
        return jsonify({"error": "product_id required"}), 400

    clients = _get_clients()
    shopify = clients["shopify"]
    bluefly = clients["bluefly"]

    if not shopify or not bluefly:
        return jsonify({"error": "Clients not configured"}), 500

    try:
        enriched = shopify.get_product_full(int(product_id))
        if not enriched:
            return jsonify({"error": f"Product {product_id} not found"}), 404

        metafields = enriched.get("metafields", [])
        cfg = load_config()
        adj = cfg.get("price_adjustment_pct", 0)
        qp = build_quantity_price_payload(enriched, metafields, price_adjustment_pct=adj, seller_id=bluefly.seller_id)

        # Override all variant statuses
        for bp in qp.get("BuyableProducts", []):
            bp["ListingStatus"] = new_status

        result = bluefly.update_quantity_price([qp])

        if result["success"]:
            return jsonify({
                "success": True,
                "status_code": result["status_code"],
                "product_title": enriched.get("title", ""),
                "new_status": new_status,
            })
        else:
            return jsonify({"error": result["error"]}), 502

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route("/api/products/sync-qty-price", methods=["POST"])
def api_sync_qty_price():
    """Push current Shopify qty+price to Bluefly for an already-published product."""
    data = request.get_json(force=True)
    product_id = data.get("product_id")

    if not product_id:
        return jsonify({"error": "product_id required"}), 400

    clients = _get_clients()
    shopify = clients["shopify"]
    bluefly = clients["bluefly"]
    pipeline = clients["pipeline"]

    if not shopify or not bluefly:
        return jsonify({"error": "Clients not configured"}), 500

    try:
        enriched = shopify.get_product_full(int(product_id))
        if not enriched:
            return jsonify({"error": f"Product {product_id} not found"}), 404

        metafields = enriched.get("metafields", [])
        cfg = load_config()
        adj = cfg.get("price_adjustment_pct", 0)
        qp = build_quantity_price_payload(enriched, metafields, price_adjustment_pct=adj, seller_id=bluefly.seller_id)

        job_path = pipeline.create_job(
            source_file="dashboard-sync-qty",
            topic="dashboard/sync-qty-price",
            product_id=product_id,
            event_id=f"sync-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        )

        result = bluefly.update_quantity_price([qp])

        if result["success"]:
            pipeline.update_stage(job_path, "pushed", {
                "response_status": result["status_code"],
                "source": "dashboard-sync",
            })
            return jsonify({
                "success": True,
                "status_code": result["status_code"],
                "product_title": enriched.get("title", ""),
            })
        else:
            pipeline.update_stage(job_path, "error", error=result["error"])
            return jsonify({"error": result["error"]}), 502

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# -----------------------------------------------------------------------
# Field Mapping — describe how Shopify fields map to Bluefly/Rithum fields
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/field-mapping")
def api_field_mapping():
    """Return the complete field mapping configuration."""
    cfg = load_config()
    field_defaults = cfg.get("field_defaults", {})
    adj = cfg.get("price_adjustment_pct", 0)

    product_fields = [
        {"bluefly_field": "category",              "source_type": "metafield", "shopify_source": "custom.bluefly_category",                                    "description": "Rithum category ID",          "required": True,  "editable": False},
        {"bluefly_field": "brand",                 "source_type": "product",   "shopify_source": "vendor",                                                     "description": "Product brand/vendor name",    "required": True,  "editable": False},
        {"bluefly_field": "name",                  "source_type": "product",   "shopify_source": "title",                                                      "description": "Product title/name",           "required": True,  "editable": False},
        {"bluefly_field": "description",           "source_type": "product",   "shopify_source": "descriptionHtml",                                            "description": "Product description (HTML)",   "required": True,  "editable": False},
        {"bluefly_field": "type_frames",           "source_type": "product",   "shopify_source": "productType",                                                "description": "Product type classification",  "required": False, "editable": False},
        {"bluefly_field": "material_clothing",     "source_type": "tag_parse", "shopify_source": "tags (leather, silk, cotton, wool, polyester, metal, plastic, acetate)", "description": "Material extracted from tags", "required": False, "editable": False},
        {"bluefly_field": "pattern",               "source_type": "tag_parse", "shopify_source": "tags (stripe, plaid, check, floral, solid, print, geometric)", "description": "Pattern from tags",          "required": False, "editable": False},
        {"bluefly_field": "gender",                "source_type": "metafield", "shopify_source": "custom.gender",                                              "description": "Target gender",                "required": False, "editable": False},
        {"bluefly_field": "sub_category",          "source_type": "metafield", "shopify_source": "custom.sub_category",                                        "description": "Product sub-category",         "required": False, "editable": False},
        {"bluefly_field": "care_instructions",     "source_type": "metafield", "shopify_source": "custom.care_instructions",                                   "description": "Care instructions",            "required": False, "editable": False},
        {"bluefly_field": "country_of_manufacture","source_type": "metafield", "shopify_source": "custom.country_of_origin",                                   "description": "Country of manufacture",       "required": False, "editable": False},
        {"bluefly_field": "size_notes",            "source_type": "metafield", "shopify_source": "custom.size_notes",                                          "description": "Size notes / fit guide",       "required": False, "editable": False},
    ]

    variant_fields = [
        {"bluefly_field": "color",            "source_type": "option/metafield", "shopify_source": "selectedOptions.Color / custom.color",                                                                 "description": "Display color value",                                                           "required": False, "editable": False},
        {"bluefly_field": "color_standard",   "source_type": "auto/default",     "shopify_source": "keyword match on color value → fallback default",                                        "description": "Standard color bucket. Auto-matched from color; falls back to configured default.", "required": True,  "editable": True,  "default_value": field_defaults.get("color_standard", "No color"), "options": ["Beige","Black","Blue","Brown","Gold","Green","Grey","Multi","No color","Off White","Orange","Pink","Purple","Red","Silver","White","Yellow"]},
        {"bluefly_field": "size",             "source_type": "option",           "shopify_source": "selectedOptions.Size",                                                             "description": "Size value",                "required": False, "editable": False},
        {"bluefly_field": "is_returnable",    "source_type": "default",          "shopify_source": "N/A",                                                                              "description": "Return policy",             "required": True,  "editable": True,  "default_value": field_defaults.get("is_returnable", "Not Returnable"), "options": ["Returnable", "Not Returnable"]},
        {"bluefly_field": "product_condition","source_type": "default",          "shopify_source": "N/A",                                                                              "description": "Condition of product",      "required": True,  "editable": True,  "default_value": field_defaults.get("product_condition", "New"),         "options": ["New", "Used", "Refurbished"]},
        {"bluefly_field": "upc",              "source_type": "variant",          "shopify_source": "barcode",                                                                          "description": "UPC / barcode",             "required": False, "editable": False},
        {"bluefly_field": "price",            "source_type": "variant",          "shopify_source": f"compareAtPrice as-is; falls back to price + {adj}% adj",                        "description": "MSRP / retail 'was' price", "required": True,  "editable": False},
        {"bluefly_field": "special_price",    "source_type": "variant",          "shopify_source": f"price + {adj}% adjustment (when compareAtPrice set)",                           "description": "Actual selling price",      "required": False, "editable": False},
        {"bluefly_field": "image_1 to image_5","source_type": "product",        "shopify_source": "images[0..4]",                                                                     "description": "Product images (up to 5)", "required": False, "editable": False},
        {"bluefly_field": "weight",           "source_type": "variant",          "shopify_source": "inventoryItem.measurement.weight",                                                 "description": "Product weight",            "required": False, "editable": False},
        {"bluefly_field": "ListingStatus",    "source_type": "default",          "shopify_source": "Derived from product.status",                                                      "description": "Bluefly listing status",    "required": True,  "editable": True,  "default_value": field_defaults.get("listing_status", "Live"),           "options": ["Live", "NotLive"]},
    ]

    return jsonify({
        "product_fields": product_fields,
        "variant_fields": variant_fields,
        "field_defaults": field_defaults,
        "eligibility": cfg.get("eligibility", {}),
    })


# -----------------------------------------------------------------------
# Bluefly Field Catalog — static TSV-backed reference for all Bluefly fields
# -----------------------------------------------------------------------

_field_catalog_cache = None


def _load_field_catalog():
    global _field_catalog_cache
    if _field_catalog_cache is not None:
        return _field_catalog_cache
    tsv_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'bluefly_field_catalog.tsv')
    fields = []
    with open(tsv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            fields.append({
                "category":       row["FieldCategory"],
                "group":          row["FieldGroup"],
                "display_name":   row["FieldDisplayName"],
                "field_name":     row["FieldName"],
                "field_level":    row["FieldLevel"],
                "importance":     row["Importance"],
                "data_type":      row["DataType"],
                "target_ids":     [t for t in row["Target"].split("|^|") if t],
                "allowed_values": [v for v in row["AllowedValues"].split("|^|") if v],
                "description":    row["Description"],
                "min_length":     row["MinLength"],
                "max_length":     row["MaxLength"],
            })
    _field_catalog_cache = fields
    return fields


@dashboard_bp.route("/api/bluefly/field-catalog")
def api_field_catalog():
    """Return the Bluefly field catalog, optionally filtered by category ID."""
    category_id = request.args.get("category_id", "").strip()
    fields = _load_field_catalog()
    if category_id:
        fields = [f for f in fields if category_id in f["target_ids"]]
    return jsonify({"fields": fields, "total": len(fields)})


# -----------------------------------------------------------------------
# Bluefly Catalog API — GET current catalog from Rithum
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/bluefly/catalog")
def api_bluefly_catalog():
    """Fetch the live Bluefly catalog via GET /v2/products."""
    clients = _get_clients()
    bluefly = clients["bluefly"]

    if not bluefly:
        return jsonify({"error": "Bluefly client not configured"}), 500

    try:
        result = bluefly.get_catalog()

        if result["success"]:
            catalog_data = result["data"]

            # Normalize: ensure we always return a list
            products = []
            if isinstance(catalog_data, list):
                products = catalog_data
            elif isinstance(catalog_data, dict):
                # Common Rithum response wrappers
                products = (
                    catalog_data.get("Products")
                    or catalog_data.get("products")
                    or catalog_data.get("Items")
                    or catalog_data.get("items")
                    or catalog_data.get("Results")
                    or catalog_data.get("results")
                    or ([catalog_data] if catalog_data.get("SellerSKU") else [])
                )
                if not isinstance(products, list):
                    products = [catalog_data]

            # Extract key fields for dashboard display
            simplified = []
            for p in products:
                if not isinstance(p, dict):
                    continue

                # Extract product-level fields
                fields_dict = {}
                for f in p.get("Fields", []):
                    if isinstance(f, dict):
                        fields_dict[f.get("Name", "")] = f.get("Value", "")

                buyables = p.get("BuyableProducts", [])
                total_qty = sum(b.get("Quantity", 0) for b in buyables if isinstance(b, dict))

                # Collect listing statuses and variant-level fields
                listing_statuses = set()
                variant_skus = []
                shopify_skus = []
                for b in buyables:
                    if not isinstance(b, dict):
                        continue
                    listing_statuses.add(b.get("ListingStatus", "Unknown"))
                    variant_skus.append(b.get("SellerSKU", ""))
                    # Check variant-level Fields too
                    for f in b.get("Fields", []):
                        if isinstance(f, dict) and f.get("Name") and f.get("Value"):
                            if f["Name"] == "shopify_sku":
                                shopify_skus.append(f["Value"])
                            # Only add to product-level if not already there
                            elif f["Name"] not in fields_dict:
                                fields_dict[f["Name"]] = f["Value"]

                # Determine overall listing status
                if len(listing_statuses) == 1:
                    listing_status = listing_statuses.pop()
                elif "Live" in listing_statuses:
                    listing_status = "Mixed"
                else:
                    listing_status = listing_statuses.pop() if listing_statuses else "Unknown"

                # Extract selling price from first BuyableProduct (special_price preferred)
                catalog_price = None
                for b in buyables[:1]:
                    if not isinstance(b, dict):
                        continue
                    for f in b.get("Fields", []):
                        if isinstance(f, dict) and f.get("Name") in ("special_price", "price"):
                            try:
                                catalog_price = float(f["Value"])
                                break
                            except (TypeError, ValueError):
                                pass

                simplified.append({
                    "seller_sku": p.get("SellerSKU", ""),
                    "name": fields_dict.get("name", fields_dict.get("Name", "")),
                    "brand": fields_dict.get("brand", fields_dict.get("Brand", "")),
                    "category": fields_dict.get("category", ""),
                    "buyable_count": len(buyables),
                    "variant_skus": variant_skus,
                    "shopify_skus": shopify_skus,
                    "total_quantity": total_qty,
                    "listing_status": listing_status,
                    "catalog_price": catalog_price,
                })

            return jsonify({
                "products": simplified,
                "total": len(simplified),
                "raw_response_type": type(catalog_data).__name__,
                # Include raw body when empty to help diagnose response format issues
                "raw_body": result.get("body", "")[:2000] if not simplified else None,
            })
        else:
            return jsonify({"error": result["error"]}), 502

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route("/api/bluefly/catalog/delist-all", methods=["POST"])
def api_catalog_delist_all():
    """Set every product in the Bluefly catalog to ListingStatus=NotLive. Streams NDJSON."""
    clients = _get_clients()
    bluefly = clients["bluefly"]
    if not bluefly:
        return jsonify({"error": "Bluefly client not configured"}), 500

    def generate():
        # Fetch current catalog
        try:
            result = bluefly.get_catalog()
        except Exception as e:
            yield json.dumps({"type": "error", "error": str(e)}) + "\n"
            return

        if not result["success"]:
            yield json.dumps({"type": "error", "error": result.get("error", "Catalog fetch failed")}) + "\n"
            return

        catalog_data = result["data"]
        products = []
        if isinstance(catalog_data, list):
            products = catalog_data
        elif isinstance(catalog_data, dict):
            products = (
                catalog_data.get("Products") or catalog_data.get("products")
                or catalog_data.get("Items") or catalog_data.get("items")
                or catalog_data.get("Results") or catalog_data.get("results")
                or ([catalog_data] if catalog_data.get("SellerSKU") else [])
            )
            if not isinstance(products, list):
                products = [catalog_data]

        yield json.dumps({"type": "start", "total": len(products)}) + "\n"

        success_count = 0
        error_count = 0
        for i, p in enumerate(products):
            if not isinstance(p, dict) or not p.get("SellerSKU"):
                continue
            buyables = p.get("BuyableProducts", [])
            payload = {
                "SellerSKU": p["SellerSKU"],
                "Fields": [],
                "BuyableProducts": [
                    {
                        "SellerSKU": b["SellerSKU"],
                        "Fields": [],
                        "Quantity": 0,
                        "ListingStatus": "NotLive",
                    }
                    for b in buyables if isinstance(b, dict) and b.get("SellerSKU")
                ],
            }
            try:
                res = bluefly.update_quantity_price([payload])
                if res["success"]:
                    success_count += 1
                    yield json.dumps({"type": "success", "index": i + 1, "total": len(products), "sku": p["SellerSKU"]}) + "\n"
                else:
                    error_count += 1
                    yield json.dumps({"type": "error", "sku": p["SellerSKU"], "error": res.get("error", "")}) + "\n"
            except Exception as e:
                error_count += 1
                yield json.dumps({"type": "error", "sku": p["SellerSKU"], "error": str(e)}) + "\n"

        yield json.dumps({"type": "complete", "success": success_count, "errors": error_count}) + "\n"

    return Response(stream_with_context(generate()), mimetype="application/x-ndjson")


# -----------------------------------------------------------------------
# Events API
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/events")
def api_events():
    """List webhook and SQL events with optional filters."""
    status = request.args.get("status")
    date = request.args.get("date")
    event_type = request.args.get("type")  # "webhook_shopify" | "sql_db" | None

    clients = _get_clients()
    tx_logger = clients["tx_logger"]

    try:
        if status:
            events = tx_logger.get_by_status(status, date=date)
        else:
            # Get all events for the date (or all dates)
            all_events = []
            log_dir = tx_logger.log_dir
            if date:
                pattern = os.path.join(log_dir, date, "*.json")
            else:
                pattern = os.path.join(log_dir, "*", "*.json")

            for fpath in sorted(glob.glob(pattern), reverse=True)[:200]:
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        record = json.load(f)
                    all_events.append({"file": fpath, "record": record})
                except (json.JSONDecodeError, OSError):
                    continue
            events = all_events

        # Simplify for API response
        simplified = []
        for ev in events:
            rec = ev["record"]
            rec_type = rec.get("type", "webhook_shopify")
            simplified.append({
                "file": ev["file"],
                "type": rec_type,
                "timestamp": rec.get("timestamp", ""),
                "topic": rec.get("topic", ""),
                "status": rec.get("status", ""),
                "event_id": rec.get("event_id", ""),
                "shop_domain": rec.get("shop_domain", ""),
                "product_id": rec.get("product_id") or rec.get("payload", {}).get("id"),
                "category_id": rec.get("category_id"),
                "field_count": rec.get("field_count"),
            })

        if event_type:
            simplified = [e for e in simplified if e["type"] == event_type]

        return jsonify({"events": simplified, "total": len(simplified)})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route("/api/events/process", methods=["POST"])
def api_process_events():
    """Trigger processing of unread events (mirrors process_products.py)."""
    clients = _get_clients()
    tx_logger = clients["tx_logger"]
    shopify = clients["shopify"]
    bluefly = clients["bluefly"]
    pipeline = clients["pipeline"]

    if not shopify or not bluefly:
        return jsonify({"error": "Clients not configured"}), 500

    try:
        unread = tx_logger.get_by_status("unread")

        product_topics = {"products/create", "products/update", "products/delete"}
        inventory_topics = {"inventory_levels/update"}

        product_events = [e for e in unread if e["record"]["topic"] in product_topics]
        inventory_events = [e for e in unread if e["record"]["topic"] in inventory_topics]

        total = len(product_events) + len(inventory_events)
        if total == 0:
            return jsonify({"message": "No unread events to process", "processed": 0})

        # Process in background thread
        def _process():
            import app as app_module
            cfg = load_config()
            adj = cfg.get("price_adjustment_pct", 0)

            for ev in product_events:
                app_module._sync_product_to_bluefly(ev["file"], ev["record"])

            for ev in inventory_events:
                app_module._sync_inventory_to_bluefly(ev["file"], ev["record"])

        t = threading.Thread(target=_process, daemon=True)
        t.start()

        return jsonify({
            "message": f"Processing {total} events in background",
            "products": len(product_events),
            "inventory": len(inventory_events),
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# -----------------------------------------------------------------------
# Pipeline API
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/pipeline/jobs")
def api_pipeline_jobs():
    """List recent pipeline jobs."""
    status = request.args.get("status")
    date = request.args.get("date")

    clients = _get_clients()
    pipeline = clients["pipeline"]

    try:
        if status:
            jobs = pipeline.get_jobs_by_status(status, date=date)
        else:
            pipeline_dir = pipeline.log_dir
            if date:
                pattern = os.path.join(pipeline_dir, date, "*.json")
            else:
                pattern = os.path.join(pipeline_dir, "*", "*.json")

            jobs = []
            for fpath in sorted(glob.glob(pattern), reverse=True)[:200]:
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        record = json.load(f)
                    jobs.append({"file": fpath, "record": record})
                except (json.JSONDecodeError, OSError):
                    continue

        simplified = []
        for j in jobs:
            rec = j["record"]
            simplified.append({
                "job_id": rec.get("job_id", ""),
                "product_id": rec.get("product_id"),
                "topic": rec.get("topic", ""),
                "status": rec.get("status", ""),
                "created_at": rec.get("created_at", ""),
                "error": rec.get("error"),
            })

        return jsonify({"jobs": simplified, "total": len(simplified)})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# -----------------------------------------------------------------------
# Settings API
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/settings")
def api_get_settings():
    """Read current configuration."""
    cfg = load_config()

    # Include read-only info
    import app as app_module
    cfg["store"] = app_module.SHOPIFY_STORE
    cfg["seller_id"] = app_module.BLUEFLY_SELLER_ID
    cfg["log_dir"] = app_module.LOG_DIR
    cfg["pipeline_log_dir"] = app_module.PIPELINE_LOG_DIR

    return jsonify(cfg)


@dashboard_bp.route("/api/settings", methods=["POST"])
def api_update_settings():
    """Update configuration (price_adjustment_pct, eligibility, field_defaults)."""
    data = request.get_json(force=True)

    cfg = load_config()

    if "price_adjustment_pct" in data:
        try:
            cfg["price_adjustment_pct"] = float(data["price_adjustment_pct"])
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid price_adjustment_pct value"}), 400

    # Update eligibility settings
    if "eligibility" in data and isinstance(data["eligibility"], dict):
        elig = cfg.get("eligibility", {})
        for key in ("require_category", "require_quantity", "require_images"):
            if key in data["eligibility"]:
                elig[key] = bool(data["eligibility"][key])
        cfg["eligibility"] = elig

    # Update field defaults
    if "field_defaults" in data and isinstance(data["field_defaults"], dict):
        defaults = cfg.get("field_defaults", {})
        for key in ("is_returnable", "product_condition", "listing_status", "color_standard"):
            if key in data["field_defaults"]:
                defaults[key] = str(data["field_defaults"][key])
        cfg["field_defaults"] = defaults

    save_config(cfg)
    return jsonify({"success": True, "config": cfg})


# -----------------------------------------------------------------------
# Mapping proxy — kept for sub-resource proxy only
# -----------------------------------------------------------------------

MAPPING_URL = "http://3.150.206.227/bluefly/conversion"


# -----------------------------------------------------------------------
# Orders API
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/orders")
def api_orders():
    """Fetch orders from Bluefly/Rithum."""
    clients = _get_clients()
    bf = clients["bluefly"]
    if not bf:
        return jsonify({"error": "Bluefly client not configured"}), 500

    status = request.args.get("status", "ReleasedForShipment")
    try:
        result = bf.get_orders(status=status)
        if not result["success"]:
            return jsonify({"error": result["error"]}), 502

        orders = result["data"]

        # Handle double-encoded responses: list of JSON strings
        if isinstance(orders, list) and len(orders) == 1 and isinstance(orders[0], str):
            try:
                parsed = json.loads(orders[0])
                if isinstance(parsed, dict) and "ResponseBody" in parsed:
                    orders = parsed["ResponseBody"]
                elif isinstance(parsed, list):
                    orders = parsed
                else:
                    orders = [parsed]
            except (json.JSONDecodeError, TypeError):
                pass
        elif isinstance(orders, str):
            try:
                orders = json.loads(orders)
            except (json.JSONDecodeError, TypeError):
                orders = []

        if not isinstance(orders, list):
            orders = []

        return jsonify({"orders": orders})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route("/api/orders/acknowledge", methods=["POST"])
def api_orders_acknowledge():
    """Acknowledge receipt of an order."""
    clients = _get_clients()
    bf = clients["bluefly"]
    if not bf:
        return jsonify({"error": "Bluefly client not configured"}), 500

    data = request.get_json(force=True)
    order_id = data.get("order_id")
    if not order_id:
        return jsonify({"error": "order_id is required"}), 400

    try:
        result = bf.acknowledge_order(order_id)
        if result["success"]:
            return jsonify({"ok": True, "order_id": order_id})
        return jsonify({"error": result["error"]}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route("/api/orders/fulfill", methods=["POST"])
def api_orders_fulfill():
    """Submit fulfillment/tracking data for an order."""
    clients = _get_clients()
    bf = clients["bluefly"]
    if not bf:
        return jsonify({"error": "Bluefly client not configured"}), 500

    data = request.get_json(force=True)
    order_id = data.get("order_id")
    items = data.get("items", [])
    if not order_id or not items:
        return jsonify({"error": "order_id and items are required"}), 400

    fulfillment = {
        "ID": order_id,
        "Items": items,
    }

    try:
        result = bf.fulfill_orders([fulfillment])
        if result["success"]:
            return jsonify({"ok": True, "order_id": order_id})
        return jsonify({"error": result["error"]}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route("/api/orders/cancel", methods=["POST"])
def api_orders_cancel():
    """Cancel an unfillable order."""
    clients = _get_clients()
    bf = clients["bluefly"]
    if not bf:
        return jsonify({"error": "Bluefly client not configured"}), 500

    data = request.get_json(force=True)
    order_id = data.get("order_id")
    items = data.get("items", [])
    if not order_id or not items:
        return jsonify({"error": "order_id and items are required"}), 400

    try:
        result = bf.cancel_order(order_id, items)
        if result["success"]:
            return jsonify({"ok": True, "order_id": order_id})
        return jsonify({"error": result["error"]}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route("/proxy/mapping")
def proxy_mapping():
    """Proxy the external Bluefly mapping tool page."""
    from urllib.request import Request, urlopen
    from urllib.error import URLError
    try:
        req = Request(MAPPING_URL)
        resp = urlopen(req, timeout=15)
        content = resp.read()
        content_type = resp.headers.get("Content-Type", "text/html")

        if "text/html" in content_type:
            html = content.decode("utf-8", errors="replace")
            base_tag = '<base href="http://3.150.206.227/bluefly/">'
            if "<head>" in html:
                html = html.replace("<head>", "<head>" + base_tag, 1)
            elif "<HEAD>" in html:
                html = html.replace("<HEAD>", "<HEAD>" + base_tag, 1)
            else:
                html = base_tag + html
            return Response(html, content_type=content_type)

        return Response(content, content_type=content_type)
    except (URLError, TimeoutError, OSError) as e:
        return Response(
            f"<html><body><h3>Could not load mapping tool</h3><p>{e}</p>"
            f"<p>Try directly: <a href='{MAPPING_URL}' target='_blank'>{MAPPING_URL}</a></p></body></html>",
            content_type="text/html",
        )


@dashboard_bp.route("/api/logs/reset", methods=["POST"])
def api_logs_reset():
    """Delete all logs and pipeline_logs for a clean republish.

    Body params (JSON, all optional — default true):
      logs (bool):          Clear webhook event logs (logs/)
      pipeline_logs (bool): Clear pipeline job logs (pipeline_logs/)
    """
    import shutil
    import app as app_module
    body = request.get_json(silent=True) or {}
    cleared = []
    dirs = {
        "logs": app_module.LOG_DIR,
        "pipeline_logs": app_module.PIPELINE_LOG_DIR,
    }
    for key, path in dirs.items():
        if body.get(key, True) and os.path.isdir(path):
            shutil.rmtree(path)
            os.makedirs(path, exist_ok=True)
            cleared.append(key)
    return jsonify({"cleared": cleared})


@dashboard_bp.route("/proxy/mapping/<path:subpath>")
def proxy_mapping_subpath(subpath):
    """Proxy sub-resources for the mapping tool."""
    from urllib.request import Request, urlopen
    from urllib.error import URLError
    try:
        url = f"http://3.150.206.227/bluefly/{subpath}"
        qs = request.query_string.decode()
        if qs:
            url += "?" + qs
        req = Request(url)
        resp = urlopen(req, timeout=15)
        content = resp.read()
        content_type = resp.headers.get("Content-Type", "application/octet-stream")
        return Response(content, content_type=content_type)
    except (URLError, TimeoutError, OSError) as e:
        return Response(str(e), status=502)
