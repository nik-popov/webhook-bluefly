"""
Shopify GraphQL Admin API client for product enrichment.

Fetches full product data including metafields, images, and variants
to support the Shopify → Bluefly sync pipeline.
"""

import json
import time
import uuid
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from urllib.parse import urlencode


class ShopifyClient:
    def __init__(self, store: str, access_token: str, api_version: str = "2025-01"):
        self.base_url = f"https://{store}/admin/api/{api_version}/graphql.json"
        self.access_token = access_token

    def graphql(self, query: str, variables: dict = None) -> dict:
        """Execute a GraphQL query against Shopify Admin API."""
        body_dict = {"query": query}
        if variables:
            body_dict["variables"] = variables

        body = json.dumps(body_dict).encode("utf-8")
        req = Request(self.base_url, data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("X-Shopify-Access-Token", self.access_token)

        for attempt in range(1, 4):
            try:
                resp = urlopen(req, timeout=30)
                result = json.loads(resp.read())

                # Proactive throttle management: pause if running low on budget
                extensions = result.get("extensions", {})
                cost = extensions.get("cost", {})
                if cost:
                    available = cost.get("currentlyAvailable", 1000)
                    requested = cost.get("requestedQueryCost", 0)
                    restore = cost.get("restoreRate", 50) or 50
                    if available < requested * 1.5:
                        wait = max(1, (requested - available) / restore)
                        print(f"  Shopify budget low ({available} available), pacing {wait:.1f}s...")
                        time.sleep(wait)

                return result
            except HTTPError as e:
                if e.code == 429:
                    retry_after = int(e.headers.get("Retry-After", 2))
                    print(f"  Shopify rate limited, waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue
                if e.code >= 500:
                    wait = 2 ** attempt
                    print(f"  Shopify 5xx error, retry {attempt}/3 after {wait}s")
                    time.sleep(wait)
                    continue
                raise
        raise RuntimeError("Shopify API: all retry attempts exhausted")

    def get_product_full(self, product_id: int) -> dict | None:
        """
        Fetch a full product by numeric ID including metafields, images,
        and variants with selected options.

        Returns the product dict or None if not found.
        """
        gid = f"gid://shopify/Product/{product_id}"
        query = """
        query getProduct($id: ID!) {
          product(id: $id) {
            id
            title
            vendor
            descriptionHtml
            productType
            status
            tags
            metafields(first: 20) {
              edges {
                node {
                  namespace
                  key
                  value
                  type
                }
              }
            }
            bluefly_category: metafield(namespace: "custom", key: "bluefly_category") {
              namespace
              key
              value
              type
            }
            sub_category: metafield(namespace: "custom", key: "sub_category") {
              namespace
              key
              value
              type
            }
            gender: metafield(namespace: "custom", key: "gender") {
              namespace
              key
              value
              type
            }
            country_of_origin: metafield(namespace: "custom", key: "country_of_origin") {
              namespace
              key
              value
              type
            }
            care_instructions: metafield(namespace: "custom", key: "care_instructions") {
              namespace
              key
              value
              type
            }
            color: metafield(namespace: "custom", key: "color") {
              namespace
              key
              value
              type
            }
            size_notes: metafield(namespace: "custom", key: "size_notes") {
              namespace
              key
              value
              type
            }
            ff_size_scale: metafield(namespace: "custom", key: "ff_size_scale") {
              namespace
              key
              value
              type
            }
            brand_product_id: metafield(namespace: "custom", key: "brand_product_id") {
              namespace
              key
              value
              type
            }
            images(first: 10) {
              edges {
                node {
                  url
                  altText
                }
              }
            }
            variants(first: 100) {
              edges {
                node {
                  id
                  sku
                  price
                  compareAtPrice
                  barcode
                  title
                  inventoryQuantity
                  selectedOptions {
                    name
                    value
                  }
                  image {
                    url
                    altText
                  }
                  inventoryItem {
                    id
                    measurement {
                      weight {
                        value
                        unit
                      }
                    }
                  }
                }
              }
            }
          }
        }
        """
        result = self.graphql(query, {"id": gid})

        product = result.get("data", {}).get("product")
        if not product:
            errors = result.get("errors", [])
            if errors:
                print(f"  Shopify GraphQL errors: {errors}")
            return None

        # Flatten edges for convenience
        return self._flatten_product(product)

    def find_product_by_inventory_item(self, inventory_item_id: int) -> dict | None:
        """
        Resolve an inventory_item_id to a product ID and variant SKU.

        Returns:
            {"product_id": int, "variant_id": str, "variant_sku": str}
            or None if not found.
        """
        gid = f"gid://shopify/InventoryItem/{inventory_item_id}"
        query = """
        query findByInventoryItem($id: ID!) {
          inventoryItem(id: $id) {
            id
            variant {
              id
              sku
              product {
                id
              }
            }
          }
        }
        """
        result = self.graphql(query, {"id": gid})
        item = result.get("data", {}).get("inventoryItem")
        if not item or not item.get("variant"):
            return None

        variant = item["variant"]
        product_gid = variant.get("product", {}).get("id", "")
        # Extract numeric ID from gid://shopify/Product/12345
        product_id = int(product_gid.split("/")[-1]) if product_gid else None

        if not product_id:
            return None

        return {
            "product_id": product_id,
            "variant_id": variant.get("id", ""),
            "variant_sku": variant.get("sku", ""),
        }

    def get_products_by_ids(self, product_ids: list) -> list[dict]:
        """Fetch multiple products by numeric IDs using the GraphQL nodes query.

        Returns the same simplified product dict format as list_products.
        Uses batches of 50 IDs per GraphQL call (Shopify nodes limit).
        """
        query = """
        query getNodes($ids: [ID!]!) {
          nodes(ids: $ids) {
            ... on Product {
              id
              title
              vendor
              productType
              status
              tags
              featuredImage { url }
              bluefly_category: metafield(namespace: "custom", key: "bluefly_category") { value }
              color: metafield(namespace: "custom", key: "color") { value }
              sub_category: metafield(namespace: "custom", key: "sub_category") { value }
              gender: metafield(namespace: "custom", key: "gender") { value }
              brand_product_id: metafield(namespace: "custom", key: "brand_product_id") { value }
              ff_size_scale: metafield(namespace: "custom", key: "ff_size_scale") { value }
              variants(first: 100) {
                edges {
                  node {
                    id
                    sku
                    title
                    price
                    compareAtPrice
                    inventoryQuantity
                    selectedOptions { name value }
                  }
                }
              }
            }
          }
        }
        """
        all_products = []
        BATCH = 250
        for i in range(0, len(product_ids), BATCH):
            chunk = product_ids[i:i + BATCH]
            gids = [f"gid://shopify/Product/{pid}" for pid in chunk]
            result = self.graphql(query, {"ids": gids})
            nodes = result.get("data", {}).get("nodes", [])
            for node in nodes:
                if not node or not node.get("id"):
                    continue
                gid = node["id"]
                numeric_id = int(gid.split("/")[-1]) if gid else None

                category_mf = node.get("bluefly_category")
                category = category_mf.get("value") if category_mf else None
                color_mf = node.get("color")
                color = color_mf.get("value") if color_mf else None
                sub_cat_mf = node.get("sub_category")
                sub_category = sub_cat_mf.get("value") if sub_cat_mf else None
                gender_mf = node.get("gender")
                gender = gender_mf.get("value") if gender_mf else None
                bpid_mf = node.get("brand_product_id")
                brand_product_id = bpid_mf.get("value") if bpid_mf else None
                fss_mf = node.get("ff_size_scale")
                ff_size_scale = fss_mf.get("value") if fss_mf else None

                variant_edges = node.get("variants", {}).get("edges", [])
                variants = [e["node"] for e in variant_edges]
                total_qty = sum(v.get("inventoryQuantity", 0) for v in variants)
                first_price = variants[0].get("price") if variants else None
                first_sku = numeric_id

                def _is_default_v(v):
                    if v.get("title", "").lower() == "default title":
                        return True
                    return any(
                        o.get("value", "").lower() in ("default title", "default")
                        for o in v.get("selectedOptions", [])
                    )
                def _has_default_size_v(vs):
                    for v in vs:
                        for o in v.get("selectedOptions", []):
                            if o.get("name", "").lower() == "size" and o.get("value", "").lower() in ("default", "default title", ""):
                                return True
                    return False
                has_default_variants = bool(variants) and (
                    all(_is_default_v(v) for v in variants) or _has_default_size_v(variants)
                )

                img = node.get("featuredImage")
                image_url = img.get("url") if img else None
                variant_ids = [v.get("id", "").split("/")[-1] for v in variants if "/" in str(v.get("id", ""))]
                variant_skus = [v.get("sku", "") for v in variants if v.get("sku")]
                first_variant_sku = variants[0].get("sku", "") if variants else ""
                variants_detail = [{"sku": v.get("sku", ""), "title": v.get("title", ""), "id": v.get("id", "").split("/")[-1] if v.get("id") else ""} for v in variants]

                all_products.append({
                    "id": numeric_id,
                    "title": node.get("title", ""),
                    "vendor": node.get("vendor", ""),
                    "product_type": node.get("productType", ""),
                    "status": node.get("status", ""),
                    "tags": node.get("tags", []),
                    "image_url": image_url,
                    "bluefly_category": category,
                    "color": color,
                    "sub_category": sub_category,
                    "gender": gender,
                    "brand_product_id": brand_product_id,
                    "ff_size_scale": ff_size_scale,
                    "first_sku": first_sku,
                    "first_price": first_price,
                    "variant_count": len(variants),
                    "total_quantity": total_qty,
                    "has_default_variants": has_default_variants,
                    "variant_ids": variant_ids,
                    "variant_skus": variant_skus,
                    "first_variant_sku": first_variant_sku,
                    "variants_detail": variants_detail,
                })
        return all_products

    def list_products(self, query_filter: str = "status:active") -> list[dict]:
        """
        List products via cursor-based pagination through Shopify GraphQL.

        Args:
            query_filter: Shopify search query (default: "status:active")

        Returns:
            List of simplified product dicts with id, title, vendor, status,
            category, image_url, variants summary, etc.
        """
        query = """
        query listProducts($cursor: String, $query: String) {
          products(first: 250, after: $cursor, query: $query, sortKey: ID) {
            edges {
              node {
                id
                title
                vendor
                productType
                status
                tags
                featuredImage { url }
                bluefly_category: metafield(namespace: "custom", key: "bluefly_category") { value }
                color: metafield(namespace: "custom", key: "color") { value }
                sub_category: metafield(namespace: "custom", key: "sub_category") { value }
                gender: metafield(namespace: "custom", key: "gender") { value }
                brand_product_id: metafield(namespace: "custom", key: "brand_product_id") { value }
                ff_size_scale: metafield(namespace: "custom", key: "ff_size_scale") { value }
                variants(first: 100) {
                  edges {
                    node {
                      sku
                      title
                      price
                      compareAtPrice
                      inventoryQuantity
                      selectedOptions { name value }
                    }
                  }
                }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        all_products = []
        cursor = None
        page_retries = 0
        max_page_retries = 3

        while True:
            variables = {"query": query_filter}
            if cursor:
                variables["cursor"] = cursor

            result = self.graphql(query, variables)

            if result.get("errors"):
                err_codes = [e.get("extensions", {}).get("code", "") for e in result["errors"]]
                if "THROTTLED" in err_codes:
                    cost = result.get("extensions", {}).get("cost", {})
                    restore = cost.get("restoreRate", 50) or 50
                    requested = cost.get("requestedQueryCost", 0)
                    available = cost.get("currentlyAvailable", 0)
                    wait = max(2, (requested - available) / restore)
                    print(f"  Shopify throttled, waiting {wait:.1f}s...")
                    time.sleep(wait)
                    page_retries += 1
                    if page_retries <= max_page_retries:
                        continue
                print(f"  GraphQL errors: {result['errors']}")

            data = result.get("data", {}).get("products", {})

            if not data and cursor:
                page_retries += 1
                if page_retries <= max_page_retries:
                    print(f"  Empty products response mid-pagination, retry {page_retries}/{max_page_retries}...")
                    time.sleep(2)
                    continue
                print("  Giving up on page after max retries")
                break

            page_retries = 0
            edges = data.get("edges", [])

            for edge in edges:
                node = edge["node"]

                # Extract numeric ID from GID
                gid = node.get("id", "")
                numeric_id = int(gid.split("/")[-1]) if gid else None

                # Get metafield values (may be None)
                category_mf = node.get("bluefly_category")
                category = category_mf.get("value") if category_mf else None
                color_mf = node.get("color")
                color = color_mf.get("value") if color_mf else None
                sub_cat_mf = node.get("sub_category")
                sub_category = sub_cat_mf.get("value") if sub_cat_mf else None
                gender_mf = node.get("gender")
                gender = gender_mf.get("value") if gender_mf else None
                bpid_mf = node.get("brand_product_id")
                brand_product_id = bpid_mf.get("value") if bpid_mf else None
                fss_mf = node.get("ff_size_scale")
                ff_size_scale = fss_mf.get("value") if fss_mf else None

                # Flatten variants
                variant_edges = node.get("variants", {}).get("edges", [])
                variants = [e["node"] for e in variant_edges]
                total_qty = sum(v.get("inventoryQuantity", 0) for v in variants)
                first_price = variants[0].get("price") if variants else None
                first_sku = numeric_id

                # Detect default-only products (no real size/color options)
                def _is_default_v(v):
                    if v.get("title", "").lower() == "default title":
                        return True
                    return any(
                        o.get("value", "").lower() in ("default title", "default")
                        for o in v.get("selectedOptions", [])
                    )
                def _has_default_size_v(vs):
                    for v in vs:
                        for o in v.get("selectedOptions", []):
                            if o.get("name", "").lower() == "size" and o.get("value", "").lower() in ("default", "default title", ""):
                                return True
                    return False
                has_default_variants = bool(variants) and (
                    all(_is_default_v(v) for v in variants) or _has_default_size_v(variants)
                )

                # Featured image
                img = node.get("featuredImage")
                image_url = img.get("url") if img else None
                variant_ids = [v.get("id", "").split("/")[-1] for v in variants if "/" in str(v.get("id", ""))]
                variant_skus = [v.get("sku", "") for v in variants if v.get("sku")]
                first_variant_sku = variants[0].get("sku", "") if variants else ""
                variants_detail = [{"sku": v.get("sku", ""), "title": v.get("title", ""), "id": v.get("id", "").split("/")[-1] if v.get("id") else ""} for v in variants]

                all_products.append({
                    "id": numeric_id,
                    "title": node.get("title", ""),
                    "vendor": node.get("vendor", ""),
                    "product_type": node.get("productType", ""),
                    "status": node.get("status", ""),
                    "tags": node.get("tags", []),
                    "image_url": image_url,
                    "bluefly_category": category,
                    "color": color,
                    "sub_category": sub_category,
                    "gender": gender,
                    "brand_product_id": brand_product_id,
                    "ff_size_scale": ff_size_scale,
                    "first_sku": first_sku,
                    "first_price": first_price,
                    "variant_count": len(variants),
                    "total_quantity": total_qty,
                    "has_default_variants": has_default_variants,
                    "variant_ids": variant_ids,
                    "variant_skus": variant_skus,
                    "first_variant_sku": first_variant_sku,
                    "variants_detail": variants_detail,
                })

            page_info = data.get("pageInfo", {})
            if page_info.get("hasNextPage"):
                cursor = page_info.get("endCursor")
            else:
                break

        return all_products

    def list_products_pages(self, query_filter: str = "status:active"):
        """Yield pages of products as they arrive from Shopify GraphQL pagination."""
        query = """
        query listProducts($cursor: String, $query: String) {
          products(first: 250, after: $cursor, query: $query, sortKey: ID) {
            edges {
              node {
                id
                title
                vendor
                productType
                status
                tags
                featuredImage { url }
                bluefly_category: metafield(namespace: "custom", key: "bluefly_category") { value }
                color: metafield(namespace: "custom", key: "color") { value }
                sub_category: metafield(namespace: "custom", key: "sub_category") { value }
                gender: metafield(namespace: "custom", key: "gender") { value }
                brand_product_id: metafield(namespace: "custom", key: "brand_product_id") { value }
                ff_size_scale: metafield(namespace: "custom", key: "ff_size_scale") { value }
                variants(first: 100) {
                  edges {
                    node {
                      sku
                      title
                      price
                      compareAtPrice
                      inventoryQuantity
                      selectedOptions { name value }
                    }
                  }
                }
              }
            }
            pageInfo { hasNextPage endCursor }
          }
        }
        """
        cursor = None
        page_retries = 0
        max_page_retries = 3

        while True:
            variables = {"query": query_filter}
            if cursor:
                variables["cursor"] = cursor

            result = self.graphql(query, variables)

            # Handle GraphQL-level errors (e.g. THROTTLED)
            if result.get("errors"):
                err_codes = [e.get("extensions", {}).get("code", "") for e in result["errors"]]
                if "THROTTLED" in err_codes:
                    cost = result.get("extensions", {}).get("cost", {})
                    restore = cost.get("restoreRate", 50) or 50
                    requested = cost.get("requestedQueryCost", 0)
                    available = cost.get("currentlyAvailable", 0)
                    wait = max(2, (requested - available) / restore)
                    print(f"  Shopify throttled, waiting {wait:.1f}s...")
                    time.sleep(wait)
                    page_retries += 1
                    if page_retries <= max_page_retries:
                        continue  # retry same page
                print(f"  GraphQL errors: {result['errors']}")

            data = result.get("data", {}).get("products", {})

            # Empty data mid-pagination — retry
            if not data and cursor:
                page_retries += 1
                if page_retries <= max_page_retries:
                    print(f"  Empty products response mid-pagination, retry {page_retries}/{max_page_retries}...")
                    time.sleep(2)
                    continue
                print("  Giving up on page after max retries")
                break

            edges = data.get("edges", [])

            # Got 0 edges mid-pagination — likely throttled, retry
            if not edges and cursor:
                page_retries += 1
                if page_retries <= max_page_retries:
                    print(f"  0 edges mid-pagination (cursor exists), retry {page_retries}/{max_page_retries}...")
                    time.sleep(3)
                    continue
                print("  Giving up on page after max retries (0 edges)")
                break

            page_retries = 0  # reset on success
            page_products = []

            for edge in edges:
                node = edge["node"]
                gid = node.get("id", "")
                numeric_id = int(gid.split("/")[-1]) if gid else None

                category_mf = node.get("bluefly_category")
                category = category_mf.get("value") if category_mf else None
                color_mf = node.get("color")
                color = color_mf.get("value") if color_mf else None
                sub_cat_mf = node.get("sub_category")
                sub_category = sub_cat_mf.get("value") if sub_cat_mf else None
                gender_mf = node.get("gender")
                gender = gender_mf.get("value") if gender_mf else None
                bpid_mf = node.get("brand_product_id")
                brand_product_id = bpid_mf.get("value") if bpid_mf else None
                fss_mf = node.get("ff_size_scale")
                ff_size_scale = fss_mf.get("value") if fss_mf else None

                variant_edges = node.get("variants", {}).get("edges", [])
                variants = [e["node"] for e in variant_edges]
                total_qty = sum(v.get("inventoryQuantity", 0) for v in variants)
                first_price = variants[0].get("price") if variants else None
                first_sku = numeric_id

                def _is_default_v(v):
                    if v.get("title", "").lower() == "default title":
                        return True
                    return any(
                        o.get("value", "").lower() in ("default title", "default")
                        for o in v.get("selectedOptions", [])
                    )
                def _has_default_size_v(vs):
                    for v in vs:
                        for o in v.get("selectedOptions", []):
                            if o.get("name", "").lower() == "size" and o.get("value", "").lower() in ("default", "default title", ""):
                                return True
                    return False
                has_default_variants = bool(variants) and (
                    all(_is_default_v(v) for v in variants) or _has_default_size_v(variants)
                )

                img = node.get("featuredImage")
                image_url = img.get("url") if img else None
                variant_ids = [v.get("id", "").split("/")[-1] for v in variants if "/" in str(v.get("id", ""))]
                variant_skus = [v.get("sku", "") for v in variants if v.get("sku")]
                first_variant_sku = variants[0].get("sku", "") if variants else ""
                variants_detail = [{"sku": v.get("sku", ""), "title": v.get("title", ""), "id": v.get("id", "").split("/")[-1] if v.get("id") else ""} for v in variants]

                page_products.append({
                    "id": numeric_id,
                    "title": node.get("title", ""),
                    "vendor": node.get("vendor", ""),
                    "product_type": node.get("productType", ""),
                    "status": node.get("status", ""),
                    "tags": node.get("tags", []),
                    "image_url": image_url,
                    "bluefly_category": category,
                    "color": color,
                    "sub_category": sub_category,
                    "gender": gender,
                    "brand_product_id": brand_product_id,
                    "ff_size_scale": ff_size_scale,
                    "first_sku": first_sku,
                    "first_price": first_price,
                    "variant_count": len(variants),
                    "total_quantity": total_qty,
                    "has_default_variants": has_default_variants,
                    "variant_ids": variant_ids,
                    "variant_skus": variant_skus,
                    "first_variant_sku": first_variant_sku,
                    "variants_detail": variants_detail,
                })

            page_info = data.get("pageInfo", {})
            has_more = page_info.get("hasNextPage", False)
            yield page_products, has_more

            if has_more:
                cursor = page_info.get("endCursor")
            else:
                break

    # Metafield aliases used in the GraphQL query for direct lookups.
    # These may not appear in metafields(first:20) if they lack a
    # formal metafield definition on the store.
    DIRECT_METAFIELD_ALIASES = [
        "bluefly_category",
        "sub_category",
        "gender",
        "country_of_origin",
        "care_instructions",
        "color",
        "size_notes",
        "ff_size_scale",
        "brand_product_id",
    ]

    @staticmethod
    def _flatten_product(product: dict) -> dict:
        """Flatten GraphQL edges/nodes into simple lists."""
        # Flatten metafields from the connection (metafields(first:20))
        metafield_edges = product.get("metafields", {}).get("edges", [])
        metafields = [e["node"] for e in metafield_edges]

        # Merge explicitly fetched metafields (aliased direct lookups)
        # These cover metafields that exist but have no formal definition.
        seen = {(m["namespace"], m["key"]) for m in metafields}
        for alias in ShopifyClient.DIRECT_METAFIELD_ALIASES:
            mf = product.pop(alias, None)
            if mf and mf.get("value") is not None:
                ns_key = (mf["namespace"], mf["key"])
                if ns_key not in seen:
                    metafields.append(mf)
                    seen.add(ns_key)

        product["metafields"] = metafields

        # Flatten images
        image_edges = product.get("images", {}).get("edges", [])
        product["images"] = [e["node"] for e in image_edges]

        # Flatten variants + extract weight from inventoryItem.measurement
        variant_edges = product.get("variants", {}).get("edges", [])
        variants = []
        for e in variant_edges:
            v = e["node"]
            # Pull weight from inventoryItem.measurement.weight
            inv_item = v.get("inventoryItem") or {}
            measurement = inv_item.get("measurement") or {}
            weight_data = measurement.get("weight") or {}
            v["weight"] = weight_data.get("value")
            v["weight_unit"] = weight_data.get("unit", "POUNDS")
            variants.append(v)
        product["variants"] = variants

        # Extract numeric product ID from GID
        gid = product.get("id", "")
        product["numeric_id"] = int(gid.split("/")[-1]) if gid else None

        return product

    # ------------------------------------------------------------------
    # Staged uploads (for image editing/upload)
    # ------------------------------------------------------------------

    def staged_upload_create(self, filename: str, mime_type: str, file_size: int) -> dict | None:
        """Create a staged upload target for a file."""
        query = """
        mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
          stagedUploadsCreate(input: $input) {
            stagedTargets {
              url
              resourceUrl
              parameters { name value }
            }
            userErrors { field message }
          }
        }
        """
        variables = {
            "input": [{
                "resource": "IMAGE",
                "filename": filename,
                "mimeType": mime_type,
                "fileSize": str(file_size),
                "httpMethod": "POST",
            }]
        }
        result = self.graphql(query, variables)
        data = result.get("data", {}).get("stagedUploadsCreate", {})
        errors = data.get("userErrors", [])
        if errors:
            raise Exception(f"Staged upload error: {errors}")
        targets = data.get("stagedTargets", [])
        return targets[0] if targets else None

    def upload_staged_file(self, target: dict, file_bytes: bytes,
                           filename: str, mime_type: str) -> str | None:
        """Upload file bytes to a staged upload target. Returns the resourceUrl."""
        upload_url = target["url"]
        params = target.get("parameters", [])
        resource_url = target.get("resourceUrl", "")

        # Build multipart form data
        boundary = uuid.uuid4().hex
        body_parts = []
        for p in params:
            body_parts.append(
                f'--{boundary}\r\n'
                f'Content-Disposition: form-data; name="{p["name"]}"\r\n\r\n'
                f'{p["value"]}\r\n'
            )
        body_parts.append(
            f'--{boundary}\r\n'
            f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'
            f'Content-Type: {mime_type}\r\n\r\n'
        )
        # Assemble: text parts + file bytes + closing boundary
        body_prefix = "".join(body_parts).encode("utf-8")
        body_suffix = f"\r\n--{boundary}--\r\n".encode("utf-8")
        body = body_prefix + file_bytes + body_suffix

        req = Request(upload_url, data=body, method="POST")
        req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
        req.add_header("Content-Length", str(len(body)))

        try:
            resp = urlopen(req, timeout=30)
            if resp.status in (200, 201, 204):
                return resource_url
            return None
        except HTTPError as e:
            print(f"[Shopify] Staged upload failed: {e.code} {e.read().decode()}")
            return None
