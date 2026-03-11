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
import time
import logging
import threading
from datetime import datetime, timezone

from flask import Blueprint, render_template, request, jsonify, Response, stream_with_context

from field_mapper import (
    should_sync_product,
    is_default_only_product,
    get_metafield,
    build_bluefly_payload,
    build_quantity_price_payload,
    extract_qty_price_payload,
    parse_seller_sku,
)

dashboard_bp = Blueprint("dashboard", __name__)


def _patch_metafield(metafields: list, namespace: str, key: str, value: str):
    """Update or insert a metafield value in the metafields list (in-place)."""
    for mf in metafields:
        if mf.get("namespace") == namespace and mf.get("key") == key:
            mf["value"] = value
            return
    metafields.append({"namespace": namespace, "key": key, "value": value})
logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.json")

# Default configuration structure
DEFAULT_CONFIG = {
    "price_adjustment_pct": 0,
    "eligibility": {
        "require_category": True,
        "require_quantity": True,
        "require_images": True,
        "require_brand_product_id": True,
    },
    "field_defaults": {
        "is_returnable": "NotReturnable",
        "product_condition": "New",
        "listing_status": "Live",
    },
    "portal_cookie": "",
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
    require_bpid = elig.get("require_brand_product_id", True)

    for p in products:
        pid = p["id"]
        if pid in synced_products:
            p["sync_status"] = synced_products[pid]["status"]
            p["sync_time"] = synced_products[pid].get("time")
            p["size_errors"] = synced_products[pid].get("size_errors")
        else:
            p["sync_status"] = "never"
            p["sync_time"] = None
            p["size_errors"] = None

        if p["sync_status"] == "pushed" and p.get("has_default_variants"):
            p["invalid"] = True
            p["invalid_reason"] = "Published with Default Title/size variants — should be removed from Bluefly"
        else:
            p["invalid"] = False

        p["has_images"] = bool(p.get("image_url"))
        p["has_quantity"] = (p.get("total_quantity", 0) or 0) > 0
        p["valid_image_format"] = _has_valid_image_format(p.get("image_url", ""))

        # Apply per-product category override from config
        cat_overrides = cfg.get("category_overrides", {})
        override_cat = cat_overrides.get(str(pid))
        if override_cat:
            p["bluefly_category"] = override_cat
            p["category_overridden"] = True

        # Derive size field from category (after any category override)
        from field_catalog import get_catalog
        catalog = get_catalog()
        cat = p.get("bluefly_category", "")
        sf = catalog.get_size_field_for_category(cat) if cat else None
        # Apply per-product size field override
        sf_overrides = cfg.get("size_field_overrides", {})
        sf_override = sf_overrides.get(str(pid))
        if sf_override:
            sf = catalog.get_size_field_by_name(sf_override)
            p["size_field_overridden"] = True
        p["size_field"] = sf["field_name"] if sf else None
        p["size_field_display"] = sf["display_name"] if sf else None

        # Apply title override
        t_override = cfg.get("title_overrides", {}).get(str(pid))
        if t_override:
            p["title"] = t_override
            p["title_overridden"] = True

        # Apply vendor override
        v_override = cfg.get("vendor_overrides", {}).get(str(pid))
        if v_override:
            p["vendor"] = v_override
            p["vendor_overridden"] = True

        # Apply price override
        pr_override = cfg.get("price_overrides", {}).get(str(pid))
        if pr_override:
            p["price_overridden"] = True
            p["price_override"] = pr_override

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
        if require_bpid and not p.get("brand_product_id"):
            p["push_eligible"] = False
            p["ineligible_reason"] = "Missing brand_product_id metafield"

        try:
            raw = float(p.get("first_price") or 0)
            if pr_override:
                if pr_override.get("type") == "fixed":
                    p["adjusted_price"] = round(float(pr_override["value"]), 2)
                elif pr_override.get("type") == "pct":
                    p["adjusted_price"] = round(raw * (1 + float(pr_override["value"]) / 100), 2) if raw else None
                else:
                    p["adjusted_price"] = round(raw * (1 + adj / 100), 2) if raw else None
            else:
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
                  if info.get("status") in ("pushed", "size_error")]

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
                entry = {
                    "status": record.get("status", "unknown"),
                    "time": record.get("created_at"),
                }
                # Extract size errors from pushed or size_error stage data
                for stage in record.get("stages", []):
                    if stage.get("stage") in ("pushed", "size_error"):
                        se = (stage.get("data") or {}).get("size_errors")
                        if se:
                            entry["size_errors"] = se
                        break
                result[pid] = entry
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

        cfg = load_config()

        # Apply per-product category override
        override_cat = cfg.get("category_overrides", {}).get(str(product_id))
        if override_cat:
            category_id = override_cat
            # Patch metafields so build_bluefly_payload also uses the override
            _patch_metafield(metafields, "custom", "bluefly_category", override_cat)

        # Apply per-product size field override
        sf_override = cfg.get("size_field_overrides", {}).get(str(product_id))
        if sf_override:
            _patch_metafield(metafields, "custom", "size_field_override", sf_override)

        # Apply per-product title/vendor/price overrides
        title_override = cfg.get("title_overrides", {}).get(str(product_id))
        if title_override:
            enriched["title"] = title_override

        vendor_override = cfg.get("vendor_overrides", {}).get(str(product_id))
        if vendor_override:
            enriched["vendor"] = vendor_override

        if not category_id:
            return jsonify({"error": "No bluefly_category metafield"}), 400

        if is_default_only_product(enriched.get("variants", [])):
            return jsonify({"error": "Product has no real variants (Default Title only)"}), 400
        elig = cfg.get("eligibility", {})
        if elig.get("require_brand_product_id", True):
            bpid = get_metafield(metafields, "custom", "brand_product_id")
            if not bpid:
                return jsonify({"error": "Missing brand_product_id metafield (required for unique SKU)"}), 400

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

        # Apply per-product price override
        price_ov = cfg.get("price_overrides", {}).get(str(product_id))
        if price_ov:
            if price_ov.get("type") == "fixed":
                for v in enriched.get("variants", []):
                    v["price"] = str(price_ov["value"])
                adj = 0
            elif price_ov.get("type") == "pct":
                adj = float(price_ov["value"])

        bluefly_payload = build_bluefly_payload(
            enriched, metafields, sql_field_map,
            price_adjustment_pct=adj,
            field_defaults=field_defaults,
            seller_id=bluefly.seller_id,
        )

        # Extract size errors — block push if any variants have size issues
        size_errors = bluefly_payload.pop("_size_errors", None)

        # Create pipeline job
        job_path = pipeline.create_job(
            source_file="dashboard-push",
            topic="dashboard/push",
            product_id=product_id,
            event_id=f"dash-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        )

        if size_errors:
            pipeline.update_stage(job_path, "size_error", {
                "size_errors": size_errors,
                "source": "dashboard",
            })
            return jsonify({
                "error": "Size mapping errors — fix size field or add conversion before pushing",
                "size_errors": size_errors,
                "product_title": enriched.get("title", ""),
            }), 422

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
            # Follow-up: set inventory via quantityprice
            try:
                qp_payload = extract_qty_price_payload(bluefly_payload)
                bluefly.update_quantity_price([qp_payload])
            except Exception as qp_err:
                print(f"[Dashboard] quantityprice follow-up failed: {qp_err}")

            stage_data = {
                "response_status": result["status_code"],
                "endpoint": endpoint_label,
                "sku": bluefly_payload.get("SellerSKU", ""),
                "source": "dashboard",
            }
            pipeline.update_stage(job_path, "pushed", stage_data)
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
    """Push all unpushed eligible products. Streams NDJSON progress."""

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

        eligible = [p for p in eligible if not p.get("has_default_variants")]
        if elig.get("require_quantity", True):
            eligible = [p for p in eligible if (p.get("total_quantity", 0) or 0) > 0]
        if elig.get("require_images", True):
            eligible = [p for p in eligible if p.get("image_url")]
        if elig.get("require_brand_product_id", True):
            eligible = [p for p in eligible if p.get("brand_product_id")]

        # Check which are already synced
        synced = _get_synced_product_ids(pipeline.log_dir)
        to_push = [p for p in eligible if p["id"] not in synced or synced[p["id"]]["status"] != "pushed"]

        yield json.dumps({"type": "start", "total": len(to_push)}) + "\n"

        adj = cfg.get("price_adjustment_pct", 0)
        field_defaults = cfg.get("field_defaults", {})
        cat_overrides = cfg.get("category_overrides", {})
        sf_overrides = cfg.get("size_field_overrides", {})
        title_overrides = cfg.get("title_overrides", {})
        vendor_overrides = cfg.get("vendor_overrides", {})
        price_overrides = cfg.get("price_overrides", {})
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
                # Apply per-product category override
                override_cat = cat_overrides.get(str(pid))
                if override_cat:
                    category_id = override_cat
                    _patch_metafield(metafields, "custom", "bluefly_category", override_cat)
                # Apply per-product size field override
                sf_override = sf_overrides.get(str(pid))
                if sf_override:
                    _patch_metafield(metafields, "custom", "size_field_override", sf_override)

                # Apply title/vendor overrides
                t_ov = title_overrides.get(str(pid))
                if t_ov:
                    enriched["title"] = t_ov
                v_ov = vendor_overrides.get(str(pid))
                if v_ov:
                    enriched["vendor"] = v_ov

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

                # Apply per-product price override
                prod_adj = adj
                pr_ov = price_overrides.get(str(pid))
                if pr_ov:
                    if pr_ov.get("type") == "fixed":
                        for v in enriched.get("variants", []):
                            v["price"] = str(pr_ov["value"])
                        prod_adj = 0
                    elif pr_ov.get("type") == "pct":
                        prod_adj = float(pr_ov["value"])

                payload = build_bluefly_payload(
                    enriched, metafields, sql_field_map,
                    price_adjustment_pct=prod_adj,
                    field_defaults=field_defaults,
                    seller_id=bluefly.seller_id,
                )

                # Extract size errors — block push if any variants have size issues
                size_errors = payload.pop("_size_errors", None)

                job_path = pipeline.create_job(
                    source_file="dashboard-bulk-push",
                    topic="dashboard/push-bulk",
                    product_id=pid,
                    event_id=f"bulk-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
                )

                if size_errors:
                    pipeline.update_stage(job_path, "size_error", {
                        "size_errors": size_errors,
                        "source": "dashboard-bulk",
                    })
                    error_count += 1
                    yield json.dumps({
                        "type": "size_error",
                        "product_id": pid,
                        "title": enriched.get("title", ""),
                        "size_errors": size_errors,
                        "index": i + 1,
                        "total": len(to_push),
                    }) + "\n"
                    continue

                already_published = synced.get(pid, {}).get("status") == "pushed"
                if already_published:
                    result = bluefly.update_products_full([payload])
                    endpoint_label = "products-put"
                else:
                    result = bluefly.push_products([payload])
                    endpoint_label = "products-post"

                if result["success"]:
                    stage_data = {
                        "response_status": result["status_code"],
                        "endpoint": endpoint_label,
                        "sku": payload.get("SellerSKU", ""),
                        "source": "dashboard-bulk",
                    }
                    pipeline.update_stage(job_path, "pushed", stage_data)
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


@dashboard_bp.route("/api/products/size-errors", methods=["GET"])
def api_size_errors():
    """List all products that failed due to unmappable sizes."""
    clients = _get_clients()
    pipeline = clients["pipeline"]
    errors = pipeline.get_error_jobs()
    return jsonify({"errors": errors, "count": len(errors)})


@dashboard_bp.route("/api/products/retry-errors", methods=["POST"])
def api_retry_errors():
    """Retry pushing products that previously failed size mapping.

    Re-runs the full pipeline for each error job. If sizes now resolve,
    the product pushes and the error job is deleted. If still failing,
    the error job stays.
    """
    clients = _get_clients()
    shopify = clients["shopify"]
    bluefly = clients["bluefly"]
    pipeline = clients["pipeline"]

    if not shopify or not bluefly:
        return jsonify({"error": "Clients not configured"}), 500

    error_jobs = pipeline.get_error_jobs()
    if not error_jobs:
        return jsonify({"message": "No error jobs to retry", "retried": 0})

    cfg = load_config()
    adj = cfg.get("price_adjustment_pct", 0)
    field_defaults = cfg.get("field_defaults", {})
    synced = _get_synced_product_ids(pipeline.log_dir)

    resolved = 0
    still_failing = 0

    for job in error_jobs:
        pid = job.get("product_id")
        file_path = job.get("_file")
        try:
            enriched = shopify.get_product_full(int(pid))
            if not enriched:
                continue
            metafields = enriched.get("metafields", [])
            category_id = get_metafield(metafields, "custom", "bluefly_category")
            if not category_id:
                continue

            sql_field_map = {}
            try:
                db = _get_db()
                with db:
                    for v in enriched.get("variants", []):
                        vt = v.get("title", "")
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

            new_errors = payload.pop("_size_errors", None)

            # Push with good variants (error variants already excluded)
            if not payload.get("BuyableProducts"):
                # ALL variants failed — can't push anything
                still_failing += 1
                continue

            job_path = pipeline.create_job(
                source_file="retry-size-error",
                topic="dashboard/retry-error",
                product_id=pid,
                event_id=f"retry-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
            )

            already_published = synced.get(int(pid), {}).get("status") == "pushed"
            if already_published:
                result = bluefly.update_products_full([payload])
            else:
                result = bluefly.push_products([payload])

            if result["success"]:
                stage_data = {
                    "response_status": result["status_code"],
                    "sku": payload.get("SellerSKU", ""),
                    "source": "retry-error",
                }
                if new_errors:
                    stage_data["size_errors"] = new_errors
                pipeline.update_stage(job_path, "pushed", stage_data)
                pipeline.delete_error_job(file_path)
                resolved += 1
            else:
                pipeline.update_stage(job_path, "error", error=result["error"])
                still_failing += 1
        except Exception:
            still_failing += 1

    return jsonify({
        "retried": len(error_jobs),
        "resolved": resolved,
        "still_failing": still_failing,
    })


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


@dashboard_bp.route("/api/products/delete-selected", methods=["POST"])
def api_delete_selected():
    """Delete selected products: reset pipeline logs + delete from portal.

    Body: { product_ids: [int, ...] }
    SKUs are extracted from pipeline logs before deletion and used to match portal products.
    """
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError, URLError

    data = request.get_json(force=True) or {}
    product_ids = set(str(pid) for pid in (data.get("product_ids") or []))

    if not product_ids:
        return jsonify({"error": "product_ids required"}), 400

    def generate():
        # Step 1: Reset pipeline logs AND collect pushed SKUs for portal matching
        clients = _get_clients()
        pipeline_dir = clients["pipeline"].log_dir
        pattern = os.path.join(pipeline_dir, "*", "*.json")
        reset_count = 0
        pushed_skus = set()
        for fpath in glob.glob(pattern):
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    record = json.load(f)
                pid = str(record.get("product_id", ""))
                for stage in record.get("stages", []):
                    if stage.get("stage") == "enriched":
                        ep = str((stage.get("data") or {}).get("product_id", ""))
                        if ep:
                            pid = ep
                        break
                if pid in product_ids:
                    # Extract SKU from pushed/size_error stage before deleting
                    for stage in record.get("stages", []):
                        if stage.get("stage") in ("pushed", "size_error"):
                            sku = (stage.get("data") or {}).get("sku", "")
                            if sku:
                                pushed_skus.add(sku)
                            break
                    os.remove(fpath)
                    reset_count += 1
            except (OSError, json.JSONDecodeError):
                continue

        yield json.dumps({"type": "reset", "count": reset_count}) + "\n"

        # Step 2: Delete from Puppet Vendors portal
        cfg = load_config()
        cookie = cfg.get("portal_cookie", "")
        if not cookie or not pushed_skus:
            yield json.dumps({"type": "complete", "reset": reset_count, "deleted": 0, "errors": 0, "skipped": "no cookie or no pushed SKUs found"}) + "\n"
            return

        portal_base = "https://app.puppetvendors.com"
        headers = {
            "Cookie": f"connect.sid={cookie}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        # Fetch portal products and find matching IDs
        portal_ids_to_delete = []
        offset = 0
        limit = 50
        while True:
            url = f"{portal_base}/api/portal/v2/products?limit={limit}&offset={offset}"
            req = Request(url, headers=headers)
            try:
                resp = urlopen(req, timeout=30)
                resp_data = json.loads(resp.read().decode())
                products = resp_data.get("products", resp_data.get("data", []))
                if not isinstance(products, list) or not products:
                    break
                for p in products:
                    # Check if product's SellerSKU or any variant SellerSKU matches
                    skus = {p.get("SellerSKU", "")}
                    for b in p.get("BuyableProducts", []):
                        if isinstance(b, dict):
                            skus.add(b.get("SellerSKU", ""))
                    skus.discard("")
                    if skus & pushed_skus:
                        pid = p.get("_id", p.get("id", ""))
                        if pid:
                            portal_ids_to_delete.append(pid)
                if len(products) < limit:
                    break
                offset += limit
            except (HTTPError, URLError, Exception) as e:
                yield json.dumps({"type": "error", "error": f"Portal fetch failed: {str(e)}"}) + "\n"
                yield json.dumps({"type": "complete", "reset": reset_count, "deleted": 0, "errors": 1}) + "\n"
                return

        if not portal_ids_to_delete:
            yield json.dumps({"type": "complete", "reset": reset_count, "deleted": 0, "errors": 0, "note": "No matching portal products found"}) + "\n"
            return

        yield json.dumps({"type": "deleting", "portal_count": len(portal_ids_to_delete)}) + "\n"

        # Delete in batches
        success_count = 0
        error_count = 0
        batch_size = 50
        for i in range(0, len(portal_ids_to_delete), batch_size):
            batch = portal_ids_to_delete[i:i + batch_size]
            payload = json.dumps({"request": "productDelete", "products": batch}).encode("utf-8")
            req = Request(f"{portal_base}/api/portal/bulk-action", data=payload, headers=headers, method="POST")
            try:
                urlopen(req, timeout=30)
                success_count += len(batch)
            except (HTTPError, URLError, Exception) as e:
                error_count += len(batch)
                yield json.dumps({"type": "error", "error": f"Delete batch failed: {str(e)}"}) + "\n"
            time.sleep(0.5)

        yield json.dumps({"type": "complete", "reset": reset_count, "deleted": success_count, "errors": error_count}) + "\n"

    return Response(stream_with_context(generate()), mimetype="application/x-ndjson")


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


@dashboard_bp.route("/api/bluefly/catalog/set-sku-status", methods=["POST"])
def api_catalog_set_sku_status():
    """Set all variants of a catalog product (by SellerSKU) to a given ListingStatus."""
    data = request.get_json(force=True)
    seller_sku = data.get("seller_sku")
    new_status = data.get("status", "Live")  # "Live" or "NotLive"
    variant_skus = data.get("variant_skus")  # optional list of variant SKUs

    if not seller_sku:
        return jsonify({"error": "seller_sku required"}), 400
    if new_status not in ("Live", "NotLive"):
        return jsonify({"error": "status must be Live or NotLive"}), 400

    clients = _get_clients()
    bluefly = clients["bluefly"]
    if not bluefly:
        return jsonify({"error": "Bluefly client not configured"}), 500

    try:
        # If frontend passed variant SKUs, use them directly (skip catalog fetch)
        if variant_skus and isinstance(variant_skus, list):
            buyable_products = [
                {
                    "SellerSKU": vsku,
                    "Fields": [],
                    "ListingStatus": new_status,
                }
                for vsku in variant_skus if vsku
            ]
        else:
            # Fallback: fetch catalog to find variant SKUs
            result = bluefly.get_catalog()
            if not result["success"]:
                return jsonify({"error": result.get("error", "Catalog fetch failed")}), 502

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

            product = None
            for p in products:
                if isinstance(p, dict) and p.get("SellerSKU") == seller_sku:
                    product = p
                    break

            if not product:
                return jsonify({"error": f"SKU {seller_sku} not found in catalog"}), 404

            buyables = product.get("BuyableProducts", [])
            buyable_products = [
                {
                    "SellerSKU": b["SellerSKU"],
                    "Fields": [],
                    "Quantity": b.get("Quantity", 0),
                    "ListingStatus": new_status,
                }
                for b in buyables if isinstance(b, dict) and b.get("SellerSKU")
            ]

        payload = {
            "SellerSKU": seller_sku,
            "Fields": [],
            "BuyableProducts": buyable_products,
        }

        res = bluefly.update_quantity_price([payload])
        if res["success"]:
            return jsonify({"success": True, "seller_sku": seller_sku, "new_status": new_status})
        else:
            return jsonify({"error": res.get("error", "Update failed")}), 502

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route("/api/bluefly/catalog/claim", methods=["POST"])
def api_catalog_claim():
    """Create a pipeline log entry for a product already on Bluefly, marking it as published."""
    data = request.get_json(force=True)
    product_id = data.get("product_id")

    if not product_id:
        return jsonify({"error": "product_id required"}), 400

    clients = _get_clients()
    pipeline = clients["pipeline"]
    if not pipeline:
        return jsonify({"error": "Pipeline not configured"}), 500

    try:
        product_id = int(product_id)
        # Check if already claimed
        synced = _get_synced_product_ids(pipeline.log_dir)
        if synced.get(product_id, {}).get("status") == "pushed":
            return jsonify({"success": True, "product_id": product_id, "already": True})

        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        event_id = f"claim-{now.strftime('%Y%m%dT%H%M%SZ')}"
        job_path = pipeline.create_job("catalog-claim", "catalog/claim", product_id, event_id)
        pipeline.update_stage(job_path, "pushed", {"reason": "Claimed from catalog — product already exists on Bluefly"})

        return jsonify({"success": True, "product_id": product_id})

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
        field_defaults = cfg.get("field_defaults", {})

        # Apply per-product category/size overrides (same as push route)
        override_cat = cfg.get("category_overrides", {}).get(str(product_id))
        if override_cat:
            _patch_metafield(metafields, "custom", "bluefly_category", override_cat)
        sf_override = cfg.get("size_field_overrides", {}).get(str(product_id))
        if sf_override:
            _patch_metafield(metafields, "custom", "size_field_override", sf_override)

        # Apply title/vendor/price overrides
        title_override = cfg.get("title_overrides", {}).get(str(product_id))
        if title_override:
            enriched["title"] = title_override
        vendor_override = cfg.get("vendor_overrides", {}).get(str(product_id))
        if vendor_override:
            enriched["vendor"] = vendor_override
        price_ov = cfg.get("price_overrides", {}).get(str(product_id))
        if price_ov:
            if price_ov.get("type") == "fixed":
                for v in enriched.get("variants", []):
                    v["price"] = str(price_ov["value"])
                adj = 0
            elif price_ov.get("type") == "pct":
                adj = float(price_ov["value"])

        # Full re-push (PUT) with merged quantities — most reliable way
        # to ensure Bluefly gets correct inventory + price.
        bluefly_payload = build_bluefly_payload(
            enriched, metafields, {},
            price_adjustment_pct=adj,
            field_defaults=field_defaults,
            seller_id=bluefly.seller_id,
        )
        bluefly_payload.pop("_size_errors", None)

        job_path = pipeline.create_job(
            source_file="dashboard-sync-qty",
            topic="dashboard/sync-qty-price",
            product_id=product_id,
            event_id=f"sync-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        )

        # Log what we're sending so we can diagnose quantity issues
        bp_summary = [
            {"sku": bp["SellerSKU"], "qty": bp["Quantity"], "size": next((f["Value"] for f in bp["Fields"] if f["Name"].startswith("size_")), None)}
            for bp in bluefly_payload.get("BuyableProducts", [])
        ]
        print(f"[Sync] product={product_id} SellerSKU={bluefly_payload.get('SellerSKU')} buyables={bp_summary}")

        result = bluefly.push_products([bluefly_payload])
        print(f"[Sync] result: success={result['success']} status={result.get('status_code')} body={result.get('body', '')[:500]}")

        if result["success"]:
            # Follow-up: set inventory via quantityprice
            try:
                qp = extract_qty_price_payload(bluefly_payload)
                qp_result = bluefly.update_quantity_price([qp])
                print(f"[Sync] quantityprice follow-up: success={qp_result['success']} status={qp_result.get('status_code')}")
            except Exception as qp_err:
                print(f"[Sync] quantityprice follow-up failed: {qp_err}")

            pipeline.update_stage(job_path, "pushed", {
                "response_status": result["status_code"],
                "source": "dashboard-sync",
            })
            return jsonify({
                "success": True,
                "status_code": result["status_code"],
                "product_title": enriched.get("title", ""),
                "buyables": bp_summary,
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
        {"bluefly_field": "is_returnable",    "source_type": "default",          "shopify_source": "N/A",                                                                              "description": "Return policy",             "required": True,  "editable": True,  "default_value": field_defaults.get("is_returnable", "NotReturnable"), "options": ["Returnable", "NotReturnable"]},
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
# Size Conversion Mappings — editable EU/international → US tables
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/size-mappings")
def api_size_mappings_get():
    """Return all size conversion mapping tables from config."""
    cfg = load_config()
    return jsonify(cfg.get("size_mappings", {}))


@dashboard_bp.route("/api/size-mappings", methods=["PUT"])
def api_size_mappings_put():
    """Save updated size conversion mappings to config and reload catalog."""
    data = request.get_json(force=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Expected a JSON object"}), 400

    cfg = load_config()
    cfg["size_mappings"] = data
    save_config(cfg)

    # Reload the singleton catalog so new mappings take effect immediately
    from field_catalog import get_catalog
    get_catalog().reload_mappings(data)

    return jsonify({"ok": True})


# -----------------------------------------------------------------------
# Brand Size Conversions API — per-brand IT/UK/EU → US tables
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/brand-size-conversions")
def api_brand_conversions_get():
    """Return brand size conversion tables from config."""
    cfg = load_config()
    return jsonify(cfg.get("brand_size_conversions", {}))


@dashboard_bp.route("/api/brand-size-conversions", methods=["PUT"])
def api_brand_conversions_put():
    """Save brand size conversion tables to config and reload catalog."""
    data = request.get_json(force=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Expected a JSON object"}), 400
    cfg = load_config()
    cfg["brand_size_conversions"] = data
    save_config(cfg)

    # Reload brand conversions in the singleton catalog
    from field_catalog import get_catalog
    get_catalog()._brand_conversions = get_catalog()._load_brand_conversions()

    return jsonify({"ok": True})


# -----------------------------------------------------------------------
# Category Overrides API — per-product category override
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/category-overrides")
def api_category_overrides_get():
    """Return per-product category overrides from config."""
    cfg = load_config()
    return jsonify(cfg.get("category_overrides", {}))


@dashboard_bp.route("/api/category-overrides", methods=["PUT"])
def api_category_overrides_put():
    """Save per-product category overrides to config."""
    data = request.get_json(force=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Expected a JSON object"}), 400
    cfg = load_config()
    cfg["category_overrides"] = data
    save_config(cfg)
    return jsonify({"ok": True})


@dashboard_bp.route("/api/favorites")
def api_favorites_get():
    """Return list of favorited product IDs from config."""
    cfg = load_config()
    return jsonify(cfg.get("favorites", []))


@dashboard_bp.route("/api/favorites", methods=["PUT"])
def api_favorites_put():
    """Save favorited product IDs to config."""
    data = request.get_json(force=True)
    if not isinstance(data, list):
        return jsonify({"error": "Expected a JSON array"}), 400
    cfg = load_config()
    cfg["favorites"] = data
    save_config(cfg)
    return jsonify({"ok": True})


@dashboard_bp.route("/api/vendor-favorites")
def api_vendor_favorites_get():
    cfg = load_config()
    return jsonify(cfg.get("vendor_favorites", []))


@dashboard_bp.route("/api/vendor-favorites", methods=["PUT"])
def api_vendor_favorites_put():
    data = request.get_json(force=True)
    if not isinstance(data, list):
        return jsonify({"error": "Expected a JSON array"}), 400
    cfg = load_config()
    cfg["vendor_favorites"] = data
    save_config(cfg)
    return jsonify({"ok": True})


@dashboard_bp.route("/api/category-list")
def api_category_list():
    """Return category ID → path mapping from Product Categories.xlsx."""
    import openpyxl
    xlsx_path = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                             "data", "Product Categories.xlsx")
    if not os.path.exists(xlsx_path):
        return jsonify({"error": "Product Categories.xlsx not found"}), 404
    wb = openpyxl.load_workbook(xlsx_path, read_only=True)
    ws = wb.active
    cat_map = {}
    for row in ws.iter_rows(min_row=2, values_only=True):
        cat_id, path = row[0], row[1]
        if cat_id is not None:
            cat_map[str(int(cat_id))] = path
    wb.close()
    return jsonify(cat_map)


# -----------------------------------------------------------------------
# Size Field Overrides API — per-product size field override
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/size-field-overrides")
def api_size_field_overrides_get():
    """Return per-product size field overrides from config."""
    cfg = load_config()
    return jsonify(cfg.get("size_field_overrides", {}))


@dashboard_bp.route("/api/size-field-overrides", methods=["PUT"])
def api_size_field_overrides_put():
    """Save per-product size field overrides to config."""
    data = request.get_json(force=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Expected a JSON object"}), 400
    cfg = load_config()
    cfg["size_field_overrides"] = data
    save_config(cfg)
    return jsonify({"ok": True})


@dashboard_bp.route("/api/size-field-list")
def api_size_field_list():
    """Return all distinct size field definitions for the picker UI."""
    from field_catalog import get_catalog
    return jsonify(get_catalog().list_size_fields())


# -----------------------------------------------------------------------
# Title Overrides API — per-product title override
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/title-overrides")
def api_title_overrides_get():
    cfg = load_config()
    return jsonify(cfg.get("title_overrides", {}))


@dashboard_bp.route("/api/title-overrides", methods=["PUT"])
def api_title_overrides_put():
    data = request.get_json(force=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Expected a JSON object"}), 400
    cfg = load_config()
    cfg["title_overrides"] = data
    save_config(cfg)
    return jsonify({"ok": True})


# -----------------------------------------------------------------------
# Vendor Overrides API — per-product vendor override
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/vendor-overrides")
def api_vendor_overrides_get():
    cfg = load_config()
    return jsonify(cfg.get("vendor_overrides", {}))


@dashboard_bp.route("/api/vendor-overrides", methods=["PUT"])
def api_vendor_overrides_put():
    data = request.get_json(force=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Expected a JSON object"}), 400
    cfg = load_config()
    cfg["vendor_overrides"] = data
    save_config(cfg)
    return jsonify({"ok": True})


# -----------------------------------------------------------------------
# Price Overrides API — per-product price override (fixed $ or %)
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/price-overrides")
def api_price_overrides_get():
    cfg = load_config()
    return jsonify(cfg.get("price_overrides", {}))


@dashboard_bp.route("/api/price-overrides", methods=["PUT"])
def api_price_overrides_put():
    data = request.get_json(force=True)
    if not isinstance(data, dict):
        return jsonify({"error": "Expected a JSON object"}), 400
    cfg = load_config()
    cfg["price_overrides"] = data
    save_config(cfg)
    return jsonify({"ok": True})


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
                            if f["Name"] in ("shopify_sku", "shopify_product_id"):
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

                # Use Fields if available, otherwise parse from seller SKU
                sku_val = p.get("SellerSKU", "")
                name_val = fields_dict.get("name", fields_dict.get("Name", ""))
                brand_val = fields_dict.get("brand", fields_dict.get("Brand", ""))
                category_val = fields_dict.get("category", "")

                if not name_val or not brand_val:
                    parsed = parse_seller_sku(sku_val)
                    if not brand_val:
                        brand_val = parsed["brand"]
                    if not category_val:
                        category_val = parsed["category"]
                    if not name_val:
                        # Build display name from parsed parts
                        name_parts = [parsed["brand"], parsed["gender"], parsed["category"]]
                        name_val = " ".join(part for part in name_parts if part)

                # Build per-variant detail
                variants = []
                for b in buyables:
                    if not isinstance(b, dict) or not b.get("SellerSKU"):
                        continue
                    v_fields = {}
                    for f in b.get("Fields", []):
                        if isinstance(f, dict) and f.get("Name"):
                            v_fields[f["Name"]] = f.get("Value", "")
                    variants.append({
                        "sku": b["SellerSKU"],
                        "quantity": b.get("Quantity", 0),
                        "listing_status": b.get("ListingStatus", "Unknown"),
                        "price": v_fields.get("special_price") or v_fields.get("price", ""),
                    })

                simplified.append({
                    "seller_sku": sku_val,
                    "name": name_val,
                    "brand": brand_val,
                    "category": category_val,
                    "buyable_count": len(buyables),
                    "variant_skus": variant_skus,
                    "shopify_skus": shopify_skus,
                    "total_quantity": total_qty,
                    "listing_status": listing_status,
                    "catalog_price": catalog_price,
                    "variants": variants,
                })

            return jsonify({
                "products": simplified,
                "total": len(simplified),
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
                        "Quantity": b.get("Quantity", 0),
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


@dashboard_bp.route("/api/bluefly/catalog/set-all-live", methods=["POST"])
def api_catalog_set_all_live():
    """Set every product in the Bluefly catalog to ListingStatus=Live. Streams NDJSON."""
    clients = _get_clients()
    bluefly = clients["bluefly"]
    if not bluefly:
        return jsonify({"error": "Bluefly client not configured"}), 500

    def generate():
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
                        "Quantity": b.get("Quantity", 0),
                        "ListingStatus": "Live",
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


@dashboard_bp.route("/api/bluefly/catalog/delete-all", methods=["POST"])
def api_catalog_delete_all():
    """Delete all products from Puppet Vendors portal. Streams NDJSON."""
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError, URLError

    cfg = load_config()
    cookie = cfg.get("portal_cookie", "")
    if not cookie:
        return jsonify({"error": "Portal cookie not configured — set it in Settings tab"}), 400

    portal_base = "https://app.puppetvendors.com"

    def generate():
        headers = {
            "Cookie": f"connect.sid={cookie}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        # Fetch all product IDs
        all_ids = []
        offset = 0
        limit = 50
        while True:
            url = f"{portal_base}/api/portal/v2/products?limit={limit}&offset={offset}"
            req = Request(url, headers=headers)
            try:
                resp = urlopen(req, timeout=30)
                data = json.loads(resp.read().decode())
                products = data.get("products", data.get("data", []))
                if not isinstance(products, list) or not products:
                    break
                for p in products:
                    pid = p.get("_id", p.get("id", ""))
                    if pid:
                        all_ids.append(pid)
                yield json.dumps({"type": "fetch", "fetched": len(all_ids)}) + "\n"
                if len(products) < limit:
                    break
                offset += limit
            except (HTTPError, URLError, Exception) as e:
                yield json.dumps({"type": "error", "error": f"Fetch failed at offset {offset}: {str(e)}"}) + "\n"
                return

        if not all_ids:
            yield json.dumps({"type": "complete", "success": 0, "errors": 0}) + "\n"
            return

        yield json.dumps({"type": "start", "total": len(all_ids)}) + "\n"

        # Delete in batches of 50
        success_count = 0
        error_count = 0
        batch_size = 50
        for i in range(0, len(all_ids), batch_size):
            batch = all_ids[i:i + batch_size]
            payload = json.dumps({"request": "productDelete", "products": batch}).encode("utf-8")
            req = Request(f"{portal_base}/api/portal/bulk-action", data=payload, headers=headers, method="POST")
            try:
                resp = urlopen(req, timeout=30)
                success_count += len(batch)
                yield json.dumps({
                    "type": "success",
                    "index": i + len(batch),
                    "total": len(all_ids),
                    "batch_size": len(batch),
                }) + "\n"
            except (HTTPError, URLError, Exception) as e:
                error_count += len(batch)
                yield json.dumps({"type": "error", "error": str(e)}) + "\n"
            time.sleep(1)

        yield json.dumps({"type": "complete", "success": success_count, "errors": error_count}) + "\n"

    return Response(stream_with_context(generate()), mimetype="application/x-ndjson")


@dashboard_bp.route("/api/products/delete-all", methods=["POST"])
def api_delete_all_published():
    """Delete ALL published products: reset all pipeline logs + delete all from portal. Streams NDJSON."""
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError, URLError

    cfg = load_config()
    cookie = cfg.get("portal_cookie", "")

    def generate():
        # Step 1: Reset ALL pipeline logs
        clients = _get_clients()
        pipeline_dir = clients["pipeline"].log_dir
        pattern = os.path.join(pipeline_dir, "*", "*.json")
        reset_count = 0
        for fpath in glob.glob(pattern):
            try:
                os.remove(fpath)
                reset_count += 1
            except OSError:
                continue
        yield json.dumps({"type": "reset", "count": reset_count}) + "\n"

        # Step 2: Delete all from portal
        if not cookie:
            yield json.dumps({"type": "complete", "reset": reset_count, "deleted": 0, "errors": 0, "skipped": "no portal cookie"}) + "\n"
            return

        portal_base = "https://app.puppetvendors.com"
        headers = {
            "Cookie": f"connect.sid={cookie}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        all_ids = []
        offset = 0
        limit = 50
        while True:
            url = f"{portal_base}/api/portal/v2/products?limit={limit}&offset={offset}"
            req = Request(url, headers=headers)
            try:
                resp = urlopen(req, timeout=30)
                data = json.loads(resp.read().decode())
                products = data.get("products", data.get("data", []))
                if not isinstance(products, list) or not products:
                    break
                for p in products:
                    pid = p.get("_id", p.get("id", ""))
                    if pid:
                        all_ids.append(pid)
                yield json.dumps({"type": "fetch", "fetched": len(all_ids)}) + "\n"
                if len(products) < limit:
                    break
                offset += limit
            except (HTTPError, URLError, Exception) as e:
                yield json.dumps({"type": "error", "error": f"Fetch failed: {str(e)}"}) + "\n"
                yield json.dumps({"type": "complete", "reset": reset_count, "deleted": 0, "errors": 1}) + "\n"
                return

        if not all_ids:
            yield json.dumps({"type": "complete", "reset": reset_count, "deleted": 0, "errors": 0}) + "\n"
            return

        yield json.dumps({"type": "start", "total": len(all_ids)}) + "\n"

        success_count = 0
        error_count = 0
        batch_size = 50
        for i in range(0, len(all_ids), batch_size):
            batch = all_ids[i:i + batch_size]
            payload = json.dumps({"request": "productDelete", "products": batch}).encode("utf-8")
            req = Request(f"{portal_base}/api/portal/bulk-action", data=payload, headers=headers, method="POST")
            try:
                urlopen(req, timeout=30)
                success_count += len(batch)
                yield json.dumps({"type": "success", "index": i + len(batch), "total": len(all_ids), "batch_size": len(batch)}) + "\n"
            except (HTTPError, URLError, Exception) as e:
                error_count += len(batch)
                yield json.dumps({"type": "error", "error": str(e)}) + "\n"
            time.sleep(0.5)

        yield json.dumps({"type": "complete", "reset": reset_count, "deleted": success_count, "errors": error_count}) + "\n"

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

    # Portal cookie
    if "portal_cookie" in data:
        cfg["portal_cookie"] = str(data["portal_cookie"])

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
            logger.warning("api_orders: failed for status=%s — %s", status, result.get("error"))
            return jsonify({"error": result["error"]}), 502

        orders = result["data"]

        # Fallback: if data is a bare string, try to parse it
        if isinstance(orders, str):
            try:
                orders = json.loads(orders)
            except (json.JSONDecodeError, TypeError):
                orders = []

        if not isinstance(orders, list):
            logger.warning("api_orders: orders is not a list (type=%s), defaulting to []", type(orders).__name__)
            orders = []

        logger.info("api_orders: returning %d orders for status=%s", len(orders), status)
        return jsonify({"orders": orders})
    except Exception as e:
        logger.exception("api_orders: unexpected error for status=%s", status)
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
