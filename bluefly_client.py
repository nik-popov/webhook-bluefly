"""
Bluefly/Rithum API client.

Pushes product data to the Rithum webhook endpoint at webhook.mindcloud.co.

Two endpoints:
  /v2/products              — Create new products (full fields, options, images)
  /v2/products/quantityprice — Update existing products (price, quantity, status)
"""

import json
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError


BLUEFLY_API_URL = (
    "https://webhook.mindcloud.co/v1/webhook/bluefly/rithum/v2/products"
)


class BlueflyClient:
    def __init__(
        self,
        seller_id: str,
        seller_token: str,
        api_url: str = BLUEFLY_API_URL,
    ):
        self.api_url = api_url
        self.quantity_price_url = api_url.rstrip("/") + "/quantityprice"
        self.seller_id = seller_id
        self.seller_token = seller_token

    def _post(
        self, url: str, payload: list[dict], max_retries: int = 3
    ) -> dict:
        """
        POST JSON payload to a Bluefly/Rithum endpoint with retry logic.

        Returns:
            {
                "status_code": int,
                "body": str,
                "success": bool,
                "error": str | None,
            }
        """
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        last_error = None

        for attempt in range(1, max_retries + 1):
            try:
                req = Request(url, data=body, method="POST")
                req.add_header("Content-Type", "application/json")
                req.add_header("sellerid", self.seller_id)
                req.add_header("sellertoken", self.seller_token)

                resp = urlopen(req, timeout=30)
                resp_body = resp.read().decode("utf-8")
                return {
                    "status_code": resp.status,
                    "body": resp_body,
                    "success": True,
                    "error": None,
                }
            except HTTPError as e:
                resp_body = e.read().decode("utf-8", errors="replace")
                last_error = f"HTTP {e.code}: {resp_body[:500]}"

                # Retry on 5xx or 429 only
                if e.code >= 500 or e.code == 429:
                    wait = 2 ** attempt
                    print(
                        f"  Bluefly retry {attempt}/{max_retries} "
                        f"after {wait}s: {last_error}"
                    )
                    time.sleep(wait)
                    continue
                else:
                    return {
                        "status_code": e.code,
                        "body": resp_body,
                        "success": False,
                        "error": last_error,
                    }
            except (URLError, TimeoutError, OSError) as e:
                last_error = str(e)
                wait = 2 ** attempt
                print(
                    f"  Bluefly retry {attempt}/{max_retries} "
                    f"after {wait}s: {last_error}"
                )
                time.sleep(wait)

        return {
            "status_code": 0,
            "body": "",
            "success": False,
            "error": f"All {max_retries} attempts failed. Last: {last_error}",
        }

    def push_products(
        self, products: list[dict], max_retries: int = 3
    ) -> dict:
        """
        POST to /v2/products — create new products with full fields.

        Use for products/create events (new listings).
        """
        return self._post(self.api_url, products, max_retries)

    def update_quantity_price(
        self, products: list[dict], max_retries: int = 3
    ) -> dict:
        """
        POST to /v2/products/quantityprice — update existing products.

        Use for products/update and inventory_levels/update events.
        Updates: price, special_price, quantity, is_returnable, ListingStatus.
        """
        return self._post(self.quantity_price_url, products, max_retries)
