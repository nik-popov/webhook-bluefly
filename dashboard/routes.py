"""
Dashboard Blueprint — web UI + JSON API for Shopify image management.

Provides:
  GET  /                             Serve the dashboard HTML
  GET  /api/products                 List all Shopify products
  GET  /api/stats                    Dashboard stats
  GET  /api/products/<id>/images     List all images for a product
  POST /api/products/<id>/images/override   Save image URL override
  DELETE /api/products/<id>/images/override  Remove image URL override
  POST /api/products/<id>/images/reorder    Reorder product images
  POST /api/products/<id>/images/convert    Convert image format
  POST /api/products/<id>/images/upload     Upload edited image
  POST /api/products/<id>/images/generate   AI image generation via Gemini
  GET  /api/image-proxy              Proxy Shopify CDN image (CORS bypass)
  GET  /api/events                   List webhook events
  GET  /api/pipeline/jobs            List recent pipeline jobs
  GET  /api/settings                 Read config
  POST /api/settings                 Update config
  POST /api/logs/reset               Clear logs
"""

import os
import json
import time
import logging
import base64
from datetime import datetime, timezone

from flask import Blueprint, render_template, request, jsonify, Response, stream_with_context

dashboard_bp = Blueprint("dashboard", __name__)

logger = logging.getLogger(__name__)

from d1_client import get_config_store

# Default configuration structure
DEFAULT_CONFIG = {
    "gemini_api_key": "",
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
        cfg = get_config_store().load()
    except Exception:
        cfg = {}
    merged = {**DEFAULT_CONFIG}
    merged.update(cfg)
    return merged


def save_config(cfg: dict):
    get_config_store().save(cfg)


# -----------------------------------------------------------------------
# Lazy access to shared app objects
# -----------------------------------------------------------------------

def _get_clients():
    """Import shared clients from the app module (avoids circular imports)."""
    import app as app_module
    return {
        "shopify": app_module.shopify_client,
        "tx_logger": app_module.tx_logger,
        "pipeline": app_module.pipeline,
    }


def _log_activity(event_type, topic, product_id=None, status="processed", detail=""):
    """Log a dashboard activity event to the events system."""
    try:
        clients = _get_clients()
        tx = clients["tx_logger"]
        tx.log({
            "type": event_type,
            "topic": topic,
            "product_id": product_id,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "event_id": f"{topic}_{int(time.time())}",
            "detail": detail,
        })
    except Exception as e:
        print(f"[Activity] Failed to log event: {e}")


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
    for ext in _ALLOWED_IMAGE_EXTS:
        if path.endswith(ext):
            return True
    return False


# -----------------------------------------------------------------------
# Product images API
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/products/<int:product_id>/images")
def api_product_images(product_id):
    """Return all images for a product, with format validity flags."""
    clients = _get_clients()
    shopify = clients["shopify"]
    if not shopify:
        return jsonify({"error": "Shopify client not configured"}), 500

    product = shopify.get_product_full(product_id)
    if not product:
        return jsonify({"error": "Product not found"}), 404

    from product_store import get_product_store
    image_overrides = get_product_store().get_image_overrides(str(product_id))

    images = []
    shopify_images = product.get("images", [])
    for i, img in enumerate(shopify_images):
        url = img.get("url", "") if isinstance(img, dict) else str(img)
        override_url = image_overrides.get(str(i))
        effective_url = override_url or url
        images.append({
            "index": i,
            "original_url": url,
            "effective_url": effective_url,
            "alt_text": img.get("altText", "") if isinstance(img, dict) else "",
            "valid_format": _has_valid_image_format(effective_url),
            "has_override": bool(override_url),
        })

    # Include override-only images (generated/added beyond Shopify count)
    for idx_str, url in sorted(image_overrides.items(), key=lambda x: int(x[0])):
        idx = int(idx_str)
        if idx >= len(shopify_images):
            images.append({
                "index": idx,
                "original_url": url,
                "effective_url": url,
                "alt_text": "",
                "valid_format": _has_valid_image_format(url),
                "has_override": True,
            })

    return jsonify({"product_id": product_id, "images": images})


@dashboard_bp.route("/api/products/<int:product_id>/images/override", methods=["POST"])
def api_image_override(product_id):
    """Save an image URL override for a product image."""
    data = request.get_json(force=True)
    image_index = data.get("image_index")
    new_url = data.get("url")

    if image_index is None or not new_url:
        return jsonify({"error": "image_index and url required"}), 400

    from product_store import get_product_store
    get_product_store().set_image_override(str(product_id), int(image_index), new_url)

    return jsonify({"ok": True, "product_id": product_id,
                     "image_index": image_index, "url": new_url})


@dashboard_bp.route("/api/products/<int:product_id>/images/override", methods=["DELETE"])
def api_image_override_delete(product_id):
    """Delete an image at a given index."""
    data = request.get_json(force=True)
    image_index = data.get("image_index")

    if image_index is None:
        return jsonify({"error": "image_index required"}), 400

    idx = int(image_index)
    pid = str(product_id)
    from product_store import get_product_store
    store = get_product_store()

    # Build the full effective image list (Shopify + overrides)
    clients = _get_clients()
    shopify = clients["shopify"]
    shopify_images = []
    if shopify:
        try:
            product = shopify.get_product_full(product_id)
            shopify_images = product.get("images", []) if product else []
        except Exception:
            pass

    overrides = store.get_image_overrides(pid)

    # Build effective URL list
    effective = []
    shopify_count = len(shopify_images)
    for i in range(shopify_count):
        url = overrides.get(str(i)) or (
            shopify_images[i].get("url", "") if isinstance(shopify_images[i], dict)
            else str(shopify_images[i])
        )
        effective.append(url)
    # Add extra override-only images
    for idx_str in sorted(overrides.keys(), key=int):
        if int(idx_str) >= shopify_count:
            effective.append(overrides[idx_str])

    if idx < 0 or idx >= len(effective):
        return jsonify({"error": "Invalid image index"}), 400

    # Remove the image at idx
    effective.pop(idx)

    # Clear all existing overrides
    for key in list(overrides.keys()):
        store.delete_image_override(pid, int(key))

    # Save all remaining positions as overrides
    for i, url in enumerate(effective):
        store.set_image_override(pid, i, url)

    return jsonify({"ok": True})


@dashboard_bp.route("/api/products/<int:product_id>/images/reorder", methods=["POST"])
def api_image_reorder(product_id):
    """Reorder images by providing new index order.

    Body: { "order": [2, 0, 1, 3] }  — array of old indices in new order.
    """
    data = request.get_json(force=True)
    new_order = data.get("order", [])

    if not new_order or not isinstance(new_order, list):
        return jsonify({"error": "order array required"}), 400

    pid = str(product_id)
    clients = _get_clients()
    shopify = clients["shopify"]

    if not shopify:
        return jsonify({"error": "Shopify client not configured"}), 500

    product = shopify.get_product_full(product_id)
    if not product:
        return jsonify({"error": "Product not found"}), 404

    from product_store import get_product_store
    store = get_product_store()
    overrides = store.get_image_overrides(pid)

    # Build current effective URL list
    shopify_images = product.get("images", [])
    effective = []
    for i in range(max(len(shopify_images), max((int(k) for k in overrides), default=-1) + 1)):
        override_url = overrides.get(str(i))
        if override_url:
            effective.append(override_url)
        elif i < len(shopify_images):
            img = shopify_images[i]
            effective.append(img.get("url", "") if isinstance(img, dict) else str(img))
        else:
            effective.append("")

    # Validate order array
    if len(new_order) != len(effective):
        return jsonify({"error": f"order length ({len(new_order)}) != image count ({len(effective)})"}), 400

    # Build reordered URL list
    reordered = [effective[i] for i in new_order]

    # Save all as overrides (clear existing first)
    for old_idx_str in list(overrides.keys()):
        store.delete_image_override(pid, int(old_idx_str))

    for new_idx, url in enumerate(reordered):
        shopify_url = ""
        if new_idx < len(shopify_images):
            img = shopify_images[new_idx]
            shopify_url = img.get("url", "") if isinstance(img, dict) else str(img)
        if url != shopify_url or new_idx >= len(shopify_images):
            store.set_image_override(pid, new_idx, url)

    return jsonify({"ok": True})


@dashboard_bp.route("/api/image-proxy")
def api_image_proxy():
    """Proxy a Shopify CDN image to avoid CORS issues with Canvas."""
    from urllib.request import urlopen, Request
    url = request.args.get("url", "")
    allowed = ("cdn.shopify.com", ".r2.dev", "vendizeinc.com", "bk.iconluxury.shop")
    if not url or not any(host in url for host in allowed):
        return jsonify({"error": "Invalid URL"}), 400
    try:
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urlopen(req, timeout=15)
        ct = resp.headers.get("Content-Type", "image/jpeg")
        ttl = 60 if ".r2.dev" in url else 3600
        return Response(resp.read(), content_type=ct,
                        headers={"Cache-Control": f"public, max-age={ttl}"})
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@dashboard_bp.route("/api/products/<int:product_id>/images/convert", methods=["POST"])
def api_image_convert(product_id):
    """Download a Shopify image, convert to target format, upload back, and save as override."""
    from urllib.request import urlopen, Request
    from io import BytesIO
    from PIL import Image

    data = request.get_json(force=True)
    image_index = data.get("image_index")
    source_url = data.get("url")
    target_format = data.get("format", "jpg").lower()

    if image_index is None or not source_url:
        return jsonify({"error": "image_index and url required"}), 400
    if target_format not in ("jpg", "jpeg", "png"):
        return jsonify({"error": "Unsupported format"}), 400

    try:
        # Download original image
        req = Request(source_url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urlopen(req, timeout=30)
        img_bytes = resp.read()

        # Convert with Pillow
        img = Image.open(BytesIO(img_bytes))
        if img.mode in ("RGBA", "P") and target_format in ("jpg", "jpeg"):
            img = img.convert("RGB")
        out = BytesIO()
        pil_format = "JPEG" if target_format in ("jpg", "jpeg") else "PNG"
        img.save(out, format=pil_format, quality=92)
        file_bytes = out.getvalue()

        ext = "jpg" if target_format in ("jpg", "jpeg") else "png"
        mime = "image/jpeg" if ext == "jpg" else "image/png"
        key = f"images/{product_id}/{image_index}.{ext}"

        # Upload to R2
        import r2_client
        resource_url = r2_client.upload(file_bytes, key, mime)

        # Save override to D1 with cache-busting timestamp
        cache_bust_url = f"{resource_url}?t={int(time.time())}"
        from product_store import get_product_store
        get_product_store().set_image_override(str(product_id), int(image_index), cache_bust_url)

        return jsonify({"ok": True, "url": cache_bust_url,
                         "product_id": product_id, "image_index": image_index})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route("/api/products/<int:product_id>/images/upload", methods=["POST"])
def api_image_upload(product_id):
    """Upload an edited image to R2 and save as override."""
    data = request.get_json(force=True)
    image_data_url = data.get("image_data")  # data:image/jpeg;base64,...
    image_index = data.get("image_index")
    filename = data.get("filename", f"edited_{product_id}_{image_index}.jpg")

    if not image_data_url or image_index is None:
        return jsonify({"error": "image_data and image_index required"}), 400

    # Decode base64 data URL
    try:
        header, b64data = image_data_url.split(",", 1)
        mime_type = header.split(":")[1].split(";")[0] if ":" in header else "image/jpeg"
        file_bytes = base64.b64decode(b64data)
    except Exception as e:
        return jsonify({"error": f"Invalid image data: {e}"}), 400

    try:
        ext = "png" if "png" in mime_type else "jpg"
        key = f"images/{product_id}/{image_index}_edited.{ext}"

        # Upload to R2
        import r2_client
        resource_url = r2_client.upload(file_bytes, key, mime_type)

        # Save override to D1 with cache-busting timestamp
        cache_bust_url = f"{resource_url}?t={int(time.time())}"
        from product_store import get_product_store
        get_product_store().set_image_override(str(product_id), int(image_index), cache_bust_url)

        return jsonify({"ok": True, "url": cache_bust_url,
                         "product_id": product_id, "image_index": image_index})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@dashboard_bp.route("/api/products/<int:product_id>/images/generate", methods=["POST"])
def api_generate_image(product_id):
    """Generate an image using Gemini API, upload to R2, return URL."""
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError

    data = request.get_json(force=True)
    prompt = (data.get("prompt") or "").strip()
    image_url = (data.get("image_url") or "").strip()
    image_urls = data.get("image_urls") or []
    if not prompt:
        return jsonify({"error": "Prompt required"}), 400

    cfg = load_config()
    api_key = cfg.get("gemini_api_key", "")
    if not api_key:
        return jsonify({"error": "Gemini API key not configured. Add it in Settings."}), 400

    import r2_client

    # Stream the response so Cloudflare doesn't timeout
    def generate():
        yield " "

        # Build parts: text + reference image(s)
        effective_prompt = prompt
        urls_to_load = image_urls if image_urls else ([image_url] if image_url else [])
        if len(urls_to_load) > 1:
            effective_prompt = (
                "Here are the existing product images for reference. "
                "Generate a NEW angle that is distinctly different from all of these. "
                + prompt
            )
        parts = [{"text": effective_prompt}]
        for ref_url in urls_to_load:
            if not ref_url:
                continue
            try:
                print(f"[GenImage] Downloading ref image: {ref_url[:80]}")
                img_req = Request(ref_url, headers={"User-Agent": "Mozilla/5.0"})
                img_resp = urlopen(img_req, timeout=15)
                img_bytes = img_resp.read()
                img_mime = img_resp.headers.get("Content-Type", "image/jpeg")
                parts.insert(len(parts) - 1, {
                    "inlineData": {
                        "mimeType": img_mime,
                        "data": base64.b64encode(img_bytes).decode("ascii"),
                    }
                })
                print(f"[GenImage] Ref image downloaded: {len(img_bytes)} bytes")
            except Exception as ex:
                print(f"[GenImage] Ref image download failed: {ex}")

        yield " "

        # Call Gemini image generation API
        payload = json.dumps({
            "contents": [{"parts": parts}],
            "generationConfig": {
                "responseModalities": ["TEXT", "IMAGE"],
            },
        }).encode("utf-8")

        models_to_try = [
            "gemini-3.1-flash-image-preview",
            "gemini-3-pro-image-preview",
            "gemini-2.5-flash-image",
        ]
        result = None
        last_error = None

        for gemini_model in models_to_try:
            gemini_url = f"https://generativelanguage.googleapis.com/v1beta/models/{gemini_model}:generateContent"
            print(f"[GenImage] Trying {gemini_model} prompt={prompt[:60]}")
            yield " "

            req = Request(
                gemini_url,
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "x-goog-api-key": api_key,
                },
                method="POST",
            )

            try:
                resp = urlopen(req, timeout=120)
                result = json.loads(resp.read().decode("utf-8"))
                print(f"[GenImage] {gemini_model} responded OK")
                break
            except HTTPError as e:
                body = e.read().decode("utf-8", errors="replace")
                last_error = f"Gemini API error ({e.code}): {body[:300]}"
                print(f"[GenImage] {gemini_model} error ({e.code}): {body[:200]}")
                continue
            except Exception as e:
                last_error = f"Gemini request failed: {e}"
                print(f"[GenImage] {gemini_model} failed: {e}")
                continue

        if result is None:
            _log_activity("gemini", "gemini/generate", product_id=product_id, status="error", detail=last_error or "All models failed")
            yield json.dumps({"error": last_error or "All Gemini models failed"})
            return

        # Extract base64 image from response
        image_data = None
        mime_type = "image/png"
        for candidate in result.get("candidates", []):
            for part in candidate.get("content", {}).get("parts", []):
                inline = part.get("inlineData")
                if inline and inline.get("data"):
                    image_data = inline["data"]
                    mime_type = inline.get("mimeType", "image/png")
                    break
            if image_data:
                break

        if not image_data:
            print(f"[GenImage] No image in response: {json.dumps(result)[:200]}")
            _log_activity("gemini", "gemini/generate", product_id=product_id, status="error", detail="No image in Gemini response")
            yield json.dumps({"error": "Gemini returned no image. Try a different prompt."})
            return

        # Upload to R2
        try:
            file_bytes = base64.b64decode(image_data)
            ext = "png" if "png" in mime_type else "jpg"
            timestamp = int(time.time())
            key = f"images/{product_id}/gen_{timestamp}.{ext}"
            resource_url = r2_client.upload(file_bytes, key, mime_type)
            print(f"[GenImage] Uploaded to R2: {resource_url}")
            _log_activity("gemini", "gemini/generate", product_id=product_id, status="processed", detail=f"Model: {gemini_model}")
            yield json.dumps({"ok": True, "url": resource_url})
        except Exception as e:
            print(f"[GenImage] R2 upload failed: {e}")
            _log_activity("gemini", "gemini/generate", product_id=product_id, status="error", detail=f"R2 upload failed: {e}")
            yield json.dumps({"error": f"R2 upload failed: {e}"})

    return Response(stream_with_context(generate()), mimetype="application/json")


# -----------------------------------------------------------------------
# Products API
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/products")
def api_products():
    """List all active products from Shopify."""
    clients = _get_clients()
    shopify = clients["shopify"]
    if not shopify:
        return jsonify({"error": "Shopify client not configured"}), 500

    try:
        products = shopify.list_products(query_filter="status:active")
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    # Enrich with image override info
    from product_store import get_product_store
    store = get_product_store()
    all_image_overrides = store.load_all_overrides_batch().get("image", {})

    for p in products:
        pid = str(p["id"])
        p["has_images"] = bool(p.get("image_url"))
        p["valid_image_format"] = _has_valid_image_format(p.get("image_url", ""))

        img_overrides = all_image_overrides.get(pid, {})
        if img_overrides:
            override_0 = img_overrides.get("0")
            if override_0:
                if not p["valid_image_format"] and _has_valid_image_format(override_0):
                    p["valid_image_format"] = True
                p["image_url"] = override_0
            p["has_image_overrides"] = True
        else:
            p["has_image_overrides"] = False

    import app as _app
    return jsonify({
        "products": products,
        "total": len(products),
        "store": _app.SHOPIFY_STORE,
    })


@dashboard_bp.route("/api/stats")
def api_stats():
    """Lightweight dashboard stats."""
    return jsonify({"status": "ok"})


# -----------------------------------------------------------------------
# Events API
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/events")
def api_events():
    """List webhook and activity events with optional filters."""
    status = request.args.get("status")
    date = request.args.get("date")
    event_type = request.args.get("type")

    clients = _get_clients()
    tx_logger = clients["tx_logger"]

    try:
        if status:
            events = tx_logger.get_by_status(status, date=date)
        else:
            events = tx_logger.get_recent(limit=200, date=date)

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
                "detail": rec.get("detail", ""),
            })

        if event_type:
            simplified = [e for e in simplified if e["type"] == event_type]

        return jsonify({"events": simplified, "total": len(simplified)})

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
            jobs = pipeline.get_recent_jobs(limit=200, date=date)

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

    import app as app_module
    cfg["store"] = app_module.SHOPIFY_STORE
    cfg["log_dir"] = app_module.LOG_DIR
    cfg["pipeline_log_dir"] = app_module.PIPELINE_LOG_DIR

    # Mask sensitive keys for frontend
    if cfg.get("gemini_api_key"):
        key = cfg["gemini_api_key"]
        cfg["gemini_api_key"] = key[:4] + "..." + key[-4:] if len(key) > 8 else "***"

    return jsonify(cfg)


@dashboard_bp.route("/api/settings", methods=["POST"])
def api_update_settings():
    """Update configuration."""
    data = request.get_json(force=True)

    cfg = load_config()

    # Gemini API key
    if "gemini_api_key" in data:
        cfg["gemini_api_key"] = str(data["gemini_api_key"])

    save_config(cfg)
    return jsonify({"success": True, "config": cfg})


# -----------------------------------------------------------------------
# Logs reset
# -----------------------------------------------------------------------

@dashboard_bp.route("/api/logs/reset", methods=["POST"])
def api_logs_reset():
    """Delete all logs and pipeline_logs for a clean start."""
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
