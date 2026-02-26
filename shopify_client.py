"""
Shopify GraphQL Admin API client for product enrichment.

Fetches full product data including metafields, images, and variants
to support the Shopify → Bluefly sync pipeline.
"""

import json
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError


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
                return json.loads(resp.read())
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
