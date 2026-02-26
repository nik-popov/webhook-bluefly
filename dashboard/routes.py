"""
Dashboard Blueprint — web UI + JSON API for Bluefly sync management.

Provides:
  GET  /                        Serve the dashboard HTML
  GET  /api/products            List Bluefly-eligible products
  POST /api/products/push       Push single product to Bluefly
  POST /api/products/push-bulk  Push all unpushed eligible products (NDJSON)
  POST /api/products/set-status Change listing status on Bluefly
  GET  /api/events              List webhook events
  POST /api/events/process      Trigger processing of unread events
  GET  /api/settings            Read config.json
  POST /api/settings            Update config.json
  GET  /api/pipeline/jobs       List recent pipeline jobs
"""

import os
import json
import glob
import threading
from datetime import datetime, timezone

from flask import Blueprint, render_template, request, jsonify, Response, stream_with_context

from field_mapper import (
    should_sync_product,
    get_metafield,
    build_bluefly_payload,
    build_quantity_price_payload,
)

dashboard_bp = Blueprint("dashboard", __name__)

CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.json")


# -----------------------------------------------------------------------
# Config helpers
# -----------------------------------------------------------------------

def load_config() -> dict:
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"price_adjustment_pct": 0}


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
# Products API
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/products")
def api_products():
    """List Bluefly-eligible products from Shopify with sync status."""
    clients = _get_clients()
    shopify = clients["shopify"]
    if not shopify:
        return jsonify({"error": "Shopify client not configured"}), 500

    try:
        all_products = shopify.list_products(query_filter="status:active")
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Filter to only Bluefly-eligible (have bluefly_category)
    eligible = [p for p in all_products if p.get("bluefly_category")]

    # Cross-reference with pipeline logs for sync status
    pipeline_dir = clients["pipeline"].log_dir
    synced_products = _get_synced_product_ids(pipeline_dir)

    for p in eligible:
        pid = p["id"]
        if pid in synced_products:
            p["sync_status"] = synced_products[pid]["status"]
            p["sync_time"] = synced_products[pid].get("time")
        else:
            p["sync_status"] = "never"
            p["sync_time"] = None

    return jsonify({"products": eligible, "total": len(eligible)})


def _get_synced_product_ids(pipeline_dir: str) -> dict:
    """Scan pipeline_logs to find the latest status per product_id."""
    result = {}
    pattern = os.path.join(pipeline_dir, "*", "*.json")
    for fpath in sorted(glob.glob(pattern)):
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                record = json.load(f)
            pid = record.get("product_id")
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

        # SQL lookup (graceful fallback)
        sql_field_map = {}
        try:
            db = _get_db()
            with db:
                for variant in enriched.get("variants", []):
                    vt = variant.get("title", "")
                    if vt:
                        sql_field_map[vt] = db.lookup_category_fields(category_id, vt)
        except Exception as e:
            print(f"[Dashboard] SQL lookup skipped: {e}")

        # Build payload with price adjustment
        cfg = load_config()
        adj = cfg.get("price_adjustment_pct", 0)
        bluefly_payload = build_bluefly_payload(enriched, metafields, sql_field_map, price_adjustment_pct=adj)

        # Create pipeline job
        job_path = pipeline.create_job(
            source_file="dashboard-push",
            topic="dashboard/push",
            product_id=product_id,
            event_id=f"dash-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
        )

        # Push
        result = bluefly.push_products([bluefly_payload])

        if result["success"]:
            pipeline.update_stage(job_path, "pushed", {
                "response_status": result["status_code"],
                "endpoint": "products",
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

        # Check which are already synced
        synced = _get_synced_product_ids(pipeline.log_dir)
        to_push = [p for p in eligible if p["id"] not in synced or synced[p["id"]]["status"] != "pushed"]

        yield json.dumps({"type": "start", "total": len(to_push)}) + "\n"

        cfg = load_config()
        adj = cfg.get("price_adjustment_pct", 0)
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

                payload = build_bluefly_payload(enriched, metafields, sql_field_map, price_adjustment_pct=adj)

                job_path = pipeline.create_job(
                    source_file="dashboard-bulk-push",
                    topic="dashboard/push-bulk",
                    product_id=pid,
                    event_id=f"bulk-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}",
                )

                result = bluefly.push_products([payload])

                if result["success"]:
                    pipeline.update_stage(job_path, "pushed", {
                        "response_status": result["status_code"],
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
        qp = build_quantity_price_payload(enriched, metafields, price_adjustment_pct=adj)

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


# -----------------------------------------------------------------------
# Events API
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/events")
def api_events():
    """List webhook events with optional filters."""
    status = request.args.get("status")
    date = request.args.get("date")

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
            simplified.append({
                "file": ev["file"],
                "timestamp": rec.get("timestamp", ""),
                "topic": rec.get("topic", ""),
                "status": rec.get("status", ""),
                "event_id": rec.get("event_id", ""),
                "shop_domain": rec.get("shop_domain", ""),
                "product_id": rec.get("payload", {}).get("id"),
            })

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
    """Update configuration (price_adjustment_pct)."""
    data = request.get_json(force=True)

    cfg = load_config()

    if "price_adjustment_pct" in data:
        try:
            cfg["price_adjustment_pct"] = float(data["price_adjustment_pct"])
        except (ValueError, TypeError):
            return jsonify({"error": "Invalid price_adjustment_pct value"}), 400

    save_config(cfg)
    return jsonify({"success": True, "config": cfg})
