"""
Register Shopify webhook subscriptions via the GraphQL Admin API.

Usage:
    python register_webhooks.py

Requires SHOPIFY_STORE and SHOPIFY_ACCESS_TOKEN in .env
"""

import os
import json
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from dotenv import load_dotenv

load_dotenv()

STORE = os.environ.get("SHOPIFY_STORE", "")  # e.g. my-store.myshopify.com
ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN", "")
WEBHOOK_URL = os.environ.get("WEBHOOK_URL", "")

TOPICS = [
    "ORDERS_CREATE",
    "ORDERS_UPDATED",
    "ORDERS_PAID",
    "ORDERS_FULFILLED",
    "ORDERS_CANCELLED",
    "PRODUCTS_CREATE",
    "PRODUCTS_UPDATE",
    "PRODUCTS_DELETE",
    "INVENTORY_LEVELS_UPDATE",
    "INVENTORY_LEVELS_CONNECT",
]


def graphql(query: str) -> dict:
    url = f"https://{STORE}/admin/api/2025-01/graphql.json"
    body = json.dumps({"query": query}).encode("utf-8")
    req = Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("X-Shopify-Access-Token", ACCESS_TOKEN)
    try:
        resp = urlopen(req)
        return json.loads(resp.read())
    except HTTPError as e:
        print(f"HTTP {e.code}: {e.read().decode()}")
        raise


def register_webhook(topic: str, callback_url: str):
    query = f"""
    mutation {{
      webhookSubscriptionCreate(
        topic: {topic}
        webhookSubscription: {{
          callbackUrl: "{callback_url}"
          format: JSON
        }}
      ) {{
        webhookSubscription {{
          id
          topic
          endpoint {{
            ... on WebhookHttpEndpoint {{
              callbackUrl
            }}
          }}
        }}
        userErrors {{
          field
          message
        }}
      }}
    }}
    """
    result = graphql(query)
    data = result.get("data", {}).get("webhookSubscriptionCreate", {})
    errors = data.get("userErrors", [])
    sub = data.get("webhookSubscription")

    if errors:
        print(f"  {topic}: ERROR - {errors[0]['message']}")
    elif sub:
        print(f"  {topic}: OK -> {sub['id']}")
    else:
        print(f"  {topic}: UNEXPECTED -> {json.dumps(result, indent=2)}")


def list_webhooks():
    query = """
    {
      webhookSubscriptions(first: 25) {
        edges {
          node {
            id
            topic
            endpoint {
              ... on WebhookHttpEndpoint {
                callbackUrl
              }
            }
          }
        }
      }
    }
    """
    result = graphql(query)
    subs = result.get("data", {}).get("webhookSubscriptions", {}).get("edges", [])
    return subs


if __name__ == "__main__":
    if not STORE or not ACCESS_TOKEN or not WEBHOOK_URL:
        print("ERROR: Set these in .env:")
        print("  SHOPIFY_STORE=my-store.myshopify.com")
        print("  SHOPIFY_ACCESS_TOKEN=shpat_xxxxx")
        print("  WEBHOOK_URL=https://tunnel-shopify-vendize.ngrok.app/webhooks/shopify")
        exit(1)

    print(f"Store: {STORE}")
    print(f"Webhook URL: {WEBHOOK_URL}")
    print()

    # Check existing webhooks
    print("=== Existing Webhooks ===")
    existing = list_webhooks()
    if existing:
        for edge in existing:
            node = edge["node"]
            endpoint = node.get("endpoint", {})
            url = endpoint.get("callbackUrl", "N/A")
            print(f"  {node['topic']} -> {url}")
    else:
        print("  (none)")

    print()
    print("=== Registering Webhooks ===")
    for topic in TOPICS:
        register_webhook(topic, WEBHOOK_URL)

    print()
    print("Done. Test by updating a product in Shopify Admin.")
