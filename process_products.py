"""
Shopify -> Bluefly product sync pipeline.

Reads unread product and inventory webhook events, enriches them via
the Shopify GraphQL API, transforms to Bluefly/Rithum format using
the field mapping + SQL lookups, and pushes to the Bluefly API.

Usage:
    python process_products.py
"""

import os
from dotenv import load_dotenv

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

# --- Configuration ---
LOG_DIR = os.environ.get("LOG_DIR", "./logs")
PIPELINE_LOG_DIR = os.environ.get("PIPELINE_LOG_DIR", "./pipeline_logs")

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

# --- Topic filters ---
PRODUCT_TOPICS = {"products/create", "products/update", "products/delete"}
INVENTORY_TOPICS = {"inventory_levels/update"}


# -----------------------------------------------------------------------
# Deduplication
# -----------------------------------------------------------------------

def deduplicate_product_events(events: list) -> tuple[list, list]:
    """
    Keep only the latest event per product_id.

    Returns:
        (latest_events, skipped_events)
    """
    by_product: dict = {}
    for ev in events:
        pid = ev["record"]["payload"].get("id")
        if pid is None:
            continue
        existing = by_product.get(pid)
        if existing is None or ev["record"]["timestamp"] > existing["record"]["timestamp"]:
            by_product[pid] = ev

    latest_set = set(id(v) for v in by_product.values())
    latest = [ev for ev in events if id(ev) in latest_set]
    skipped = [ev for ev in events if id(ev) not in latest_set]
    return latest, skipped


# -----------------------------------------------------------------------
# Product event processor
# -----------------------------------------------------------------------

def process_product_event(
    event_info: dict,
    tx_logger: TransactionLogger,
    pipeline: PipelineLogger,
    shopify: ShopifyClient,
    bluefly: BlueflyClient,
    db: BlueflyDBLookup,
):
    """Full product sync pipeline for a single product event."""
    file_path = event_info["file"]
    record = event_info["record"]
    product_id = record["payload"].get("id")
    topic = record["topic"]

    print(f"\n{'='*60}")
    print(f"Processing {topic} for product {product_id}")
    print(f"Source: {file_path}")

    # Mark webhook as processing
    tx_logger.update_status(file_path, "processing")

    # Create pipeline job
    job_path = pipeline.create_job(
        source_file=file_path,
        topic=topic,
        product_id=product_id,
        event_id=record.get("event_id", ""),
    )

    try:
        # Handle products/delete -- skip (no Bluefly delete API)
        if topic == "products/delete":
            print("  Product deleted -- skipping (no Bluefly delete endpoint)")
            pipeline.update_stage(job_path, "skipped", {"reason": "product deleted"})
            tx_logger.update_status(file_path, "processed")
            return

        # Stage: Enrich
        pipeline.update_stage(job_path, "enriching")
        print("  Enriching via Shopify GraphQL...")
        enriched = shopify.get_product_full(product_id)
        if not enriched:
            raise ValueError(f"Product {product_id} not found in Shopify")

        metafields = enriched.get("metafields", [])
        pipeline.update_stage(job_path, "enriched", {
            "title": enriched.get("title"),
            "variant_count": len(enriched.get("variants", [])),
            "metafield_count": len(metafields),
        })
        print(f"  Enriched: '{enriched.get('title')}' -- "
              f"{len(enriched.get('variants', []))} variants, "
              f"{len(metafields)} metafields")

        # Check if product should be synced
        if not should_sync_product(enriched):
            reason = f"status: {enriched.get('status')}"
            print(f"  Product not ACTIVE ({reason}) -- skipping")
            pipeline.update_stage(job_path, "skipped", {"reason": reason})
            tx_logger.update_status(file_path, "processed")
            return

        # Extract category from metafield -- required for Bluefly
        category_id = get_metafield(metafields, "custom", "bluefly_category")
        if not category_id:
            print("  No custom.bluefly_category metafield -- skipping")
            pipeline.update_stage(job_path, "skipped", {"reason": "no bluefly_category"})
            tx_logger.update_status(file_path, "processed")
            return
        print(f"  Category ID: {category_id}")

        # Stage: Map
        pipeline.update_stage(job_path, "mapping")
        print("  Mapping fields...")

        # SQL lookup for each variant
        sql_field_map = {}
        if category_id:
            for variant in enriched.get("variants", []):
                variant_title = variant.get("title", "")
                if variant_title:
                    sql_fields = db.lookup_category_fields(category_id, variant_title)
                    sql_field_map[variant_title] = sql_fields
                    if sql_fields:
                        print(f"    SQL fields for '{variant_title}': {list(sql_fields.keys())}")

        # All product events (create + update) use /v2/products with full payload.
        # This endpoint is an upsert — creates or updates all fields (color, size,
        # images, options, etc.) on existing products.
        bluefly_payload = build_bluefly_payload(enriched, metafields, sql_field_map, seller_id=BLUEFLY_SELLER_ID)

        bp_count = len(bluefly_payload.get("BuyableProducts", []))
        pipeline.update_stage(job_path, "mapped", {
            "seller_sku": bluefly_payload.get("SellerSKU"),
            "buyable_count": bp_count,
            "endpoint": "products",
        })
        print(f"  Mapped: SKU={bluefly_payload.get('SellerSKU')}, "
              f"{bp_count} buyable products → products")

        # Stage: Push
        pipeline.update_stage(job_path, "pushing")
        print(f"  Pushing to Bluefly API (products)...")
        result = bluefly.push_products([bluefly_payload])

        if result["success"]:
            print(f"  OK Pushed (products): HTTP {result['status_code']}")
            pipeline.update_stage(job_path, "pushed", {
                "response_status": result["status_code"],
                "endpoint": "products",
            })
            tx_logger.update_status(file_path, "processed")
        else:
            raise RuntimeError(f"Bluefly API error (products): {result['error']}")

    except Exception as e:
        print(f"  FAIL ERROR: {e}")
        pipeline.update_stage(job_path, "error", error=str(e))
        tx_logger.update_status(file_path, "error")


# -----------------------------------------------------------------------
# Inventory event processor
# -----------------------------------------------------------------------

def process_inventory_event(
    event_info: dict,
    tx_logger: TransactionLogger,
    pipeline: PipelineLogger,
    shopify: ShopifyClient,
    bluefly: BlueflyClient,
    db: BlueflyDBLookup,
):
    """Process an inventory_levels/update event."""
    file_path = event_info["file"]
    record = event_info["record"]
    payload = record["payload"]
    inventory_item_id = payload.get("inventory_item_id")
    new_available = payload.get("available", 0)

    print(f"\n{'='*60}")
    print(f"Processing inventory update for item {inventory_item_id}")
    print(f"  New available: {new_available}")

    tx_logger.update_status(file_path, "processing")

    job_path = pipeline.create_job(
        source_file=file_path,
        topic="inventory_levels/update",
        product_id=inventory_item_id,
        event_id=record.get("event_id", ""),
    )

    try:
        # Resolve inventory_item_id -> product/variant
        pipeline.update_stage(job_path, "enriching")
        print("  Resolving inventory item to product...")
        lookup = shopify.find_product_by_inventory_item(inventory_item_id)

        if not lookup:
            print(f"  Could not resolve inventory_item_id {inventory_item_id}")
            pipeline.update_stage(
                job_path, "skipped",
                {"reason": "unresolvable inventory item"},
            )
            tx_logger.update_status(file_path, "processed")
            return

        product_id = lookup["product_id"]
        variant_sku = lookup["variant_sku"]
        print(f"  Resolved to product {product_id}, variant SKU: {variant_sku}")

        # Full enrich (Rithum needs the full product structure)
        enriched = shopify.get_product_full(product_id)
        if not enriched:
            raise ValueError(f"Product {product_id} not found in Shopify")

        metafields = enriched.get("metafields", [])
        pipeline.update_stage(job_path, "enriched", {
            "product_id": product_id,
            "variant_sku": variant_sku,
            "title": enriched.get("title"),
        })

        if not should_sync_product(enriched):
            reason = f"product status: {enriched.get('status')}"
            print(f"  Product not ACTIVE ({reason}) -- skipping")
            pipeline.update_stage(job_path, "skipped", {"reason": reason})
            tx_logger.update_status(file_path, "processed")
            return

        # Map with SQL lookup -- require bluefly_category
        pipeline.update_stage(job_path, "mapping")
        category_id = get_metafield(metafields, "custom", "bluefly_category")

        if not category_id:
            print("  No custom.bluefly_category metafield -- skipping inventory update")
            pipeline.update_stage(job_path, "skipped", {"reason": "no bluefly_category"})
            tx_logger.update_status(file_path, "processed")
            return

        # Build lightweight quantityprice payload for inventory updates
        bluefly_payload = build_quantity_price_payload(enriched, metafields, seller_id=BLUEFLY_SELLER_ID)

        # Override the specific variant's quantity with webhook value
        updated_sku = None
        for bp in bluefly_payload.get("BuyableProducts", []):
            if bp.get("SellerSKU") == variant_sku:
                bp["Quantity"] = new_available
                updated_sku = variant_sku

        pipeline.update_stage(job_path, "mapped", {
            "variant_sku": updated_sku or variant_sku,
            "quantity": new_available,
            "endpoint": "quantityprice",
        })
        print(f"  Mapped: updated {variant_sku} quantity -> {new_available}")

        # Push to quantityprice endpoint
        pipeline.update_stage(job_path, "pushing")
        print("  Pushing to Bluefly API (quantityprice)...")
        result = bluefly.update_quantity_price([bluefly_payload])

        if result["success"]:
            print(f"  OK Pushed inventory update (quantityprice): HTTP {result['status_code']}")
            pipeline.update_stage(job_path, "pushed", {
                "response_status": result["status_code"],
                "endpoint": "quantityprice",
            })
            tx_logger.update_status(file_path, "processed")
        else:
            raise RuntimeError(f"Bluefly API error (quantityprice): {result['error']}")

    except Exception as e:
        print(f"  FAIL ERROR: {e}")
        pipeline.update_stage(job_path, "error", error=str(e))
        tx_logger.update_status(file_path, "error")


# -----------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------

def main():
    print("=" * 60)
    print("Shopify -> Bluefly Product Sync Pipeline")
    print("=" * 60)

    # Validate config
    missing = []
    if not SHOPIFY_STORE:
        missing.append("SHOPIFY_STORE")
    if not SHOPIFY_ACCESS_TOKEN:
        missing.append("SHOPIFY_ACCESS_TOKEN")
    if not BLUEFLY_SELLER_ID:
        missing.append("BLUEFLY_SELLER_ID")
    if not BLUEFLY_SELLER_TOKEN:
        missing.append("BLUEFLY_SELLER_TOKEN")
    if not SQL_SERVER:
        missing.append("SQL_SERVER")
    if missing:
        print(f"ERROR: Missing .env variables: {', '.join(missing)}")
        return

    # Initialize clients
    tx_logger = TransactionLogger(LOG_DIR)
    pipeline = PipelineLogger(PIPELINE_LOG_DIR)
    shopify = ShopifyClient(SHOPIFY_STORE, SHOPIFY_ACCESS_TOKEN)
    bluefly = BlueflyClient(BLUEFLY_SELLER_ID, BLUEFLY_SELLER_TOKEN, BLUEFLY_API_URL)
    db = BlueflyDBLookup(SQL_SERVER, SQL_DATABASE, SQL_USER, SQL_PASSWORD)

    # Scan for unread events
    print(f"\nScanning {LOG_DIR} for unread events...")
    unread = tx_logger.get_by_status("unread")

    product_events = [
        e for e in unread if e["record"]["topic"] in PRODUCT_TOPICS
    ]
    inventory_events = [
        e for e in unread if e["record"]["topic"] in INVENTORY_TOPICS
    ]
    other_events = [
        e for e in unread
        if e["record"]["topic"] not in PRODUCT_TOPICS
        and e["record"]["topic"] not in INVENTORY_TOPICS
    ]

    print(f"  Product events:   {len(product_events)}")
    print(f"  Inventory events: {len(inventory_events)}")
    print(f"  Other events:     {len(other_events)} (skipped)")

    if not product_events and not inventory_events:
        print("\nNo product or inventory events to process.")
        return

    # Deduplicate product events
    latest_products, skipped_products = deduplicate_product_events(product_events)
    if skipped_products:
        print(f"\n  Deduplicated: processing {len(latest_products)} products "
              f"(skipping {len(skipped_products)} older duplicates)")
        # Mark skipped duplicates as processed
        for ev in skipped_products:
            tx_logger.update_status(ev["file"], "processed")

    # Process all events
    with db:
        # Product events first
        for event_info in latest_products:
            process_product_event(
                event_info, tx_logger, pipeline, shopify, bluefly, db
            )

        # Then inventory events
        for event_info in inventory_events:
            process_inventory_event(
                event_info, tx_logger, pipeline, shopify, bluefly, db
            )

    # Summary
    print(f"\n{'='*60}")
    print("Pipeline complete.")
    print(f"  Processed: {len(latest_products)} products, "
          f"{len(inventory_events)} inventory updates")
    print(f"  Check pipeline_logs/ for detailed job records")


if __name__ == "__main__":
    main()
