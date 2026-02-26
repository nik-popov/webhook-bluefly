"""
Bluefly/Rithum API client.

Pushes product data to the Rithum webhook endpoint at webhook.mindcloud.co.

Three endpoints:
  GET  /v2/products              — Download current catalog
  POST /v2/products              — Create new products (full fields, options, images)
  POST /v2/products/quantityprice — Update existing products (price, quantity, status)
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

    # ------------------------------------------------------------------
    # GET helper
    # ------------------------------------------------------------------

    def _get(self, url: str, max_retries: int = 3) -> dict:
        """
        GET request to a Bluefly/Rithum endpoint with retry logic.

        Returns:
            {
                "status_code": int,
                "body": str,
                "data": any,       # parsed JSON (list or dict)
                "success": bool,
                "error": str | None,
            }
        """
        last_error = None

        for attempt in range(1, max_retries + 1):
            try:
                req = Request(url, method="GET")
                req.add_header("sellerid", self.seller_id)
                req.add_header("sellertoken", self.seller_token)
                req.add_header("Accept", "application/json")

                resp = urlopen(req, timeout=60)
                resp_body = resp.read().decode("utf-8")

                # Try to parse JSON
                try:
                    data = json.loads(resp_body)
                except json.JSONDecodeError:
                    data = resp_body

                return {
                    "status_code": resp.status,
                    "body": resp_body,
                    "data": data,
                    "success": True,
                    "error": None,
                }
            except HTTPError as e:
                resp_body = e.read().decode("utf-8", errors="replace")
                last_error = f"HTTP {e.code}: {resp_body[:500]}"

                if e.code >= 500 or e.code == 429:
                    wait = 2 ** attempt
                    print(
                        f"  Bluefly GET retry {attempt}/{max_retries} "
                        f"after {wait}s: {last_error}"
                    )
                    time.sleep(wait)
                    continue
                else:
                    return {
                        "status_code": e.code,
                        "body": resp_body,
                        "data": None,
                        "success": False,
                        "error": last_error,
                    }
            except (URLError, TimeoutError, OSError) as e:
                last_error = str(e)
                wait = 2 ** attempt
                print(
                    f"  Bluefly GET retry {attempt}/{max_retries} "
                    f"after {wait}s: {last_error}"
                )
                time.sleep(wait)

        return {
            "status_code": 0,
            "body": "",
            "data": None,
            "success": False,
            "error": f"All {max_retries} attempts failed. Last: {last_error}",
        }

    # ------------------------------------------------------------------
    # POST helper
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    def get_catalog(self, max_poll: int = 10, poll_interval: int = 3) -> dict:
        """
        GET /v2/products — Download current product catalog from Bluefly.

        The Rithum API is async: the first response returns a PendingUri
        which must be polled until Status becomes 'Complete'.

        Returns the raw result dict with 'data' containing the catalog.
        """
        result = self._get(self.api_url)
        if not result["success"]:
            return result

        data = result["data"]
        if not isinstance(data, dict):
            return result

        # Handle async response
        status = data.get("Status", "")
        pending_uri = data.get("PendingUri")

        if status == "Complete":
            # Already complete — return the response body
            result["data"] = data.get("ResponseBody", data)
            return result

        if status == "AsyncResponsePending" and pending_uri:
            print(f"[Bluefly] Catalog async — polling PendingUri...")
            for poll in range(max_poll):
                time.sleep(poll_interval)
                poll_result = self._get(pending_uri)
                if not poll_result["success"]:
                    return poll_result

                poll_data = poll_result["data"]
                if isinstance(poll_data, dict):
                    poll_status = poll_data.get("Status", "")
                    print(f"[Bluefly] Poll {poll+1}/{max_poll}: Status={poll_status}")

                    if poll_status == "Complete":
                        poll_result["data"] = poll_data.get("ResponseBody", poll_data)
                        return poll_result
                    elif poll_status == "AsyncResponsePending":
                        # Update pending URI if changed
                        new_uri = poll_data.get("PendingUri")
                        if new_uri:
                            pending_uri = new_uri
                        continue
                    elif poll_data.get("Errors"):
                        return {
                            "status_code": poll_result["status_code"],
                            "body": poll_result["body"],
                            "data": None,
                            "success": False,
                            "error": f"Rithum errors: {poll_data['Errors']}",
                        }
                else:
                    # Got a list or something else — treat as final data
                    return poll_result

            return {
                "status_code": 0,
                "body": "",
                "data": None,
                "success": False,
                "error": f"Catalog poll timed out after {max_poll} attempts",
            }

        # Unknown status — return as-is
        result["data"] = data.get("ResponseBody", data)
        return result

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
