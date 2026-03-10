"""
Bluefly/Rithum API client.

Communicates with the Rithum webhook endpoint at webhook.mindcloud.co.

Product endpoints:
  GET  /v2/products              — Download current catalog
  POST /v2/products              — Create new products (full fields, options, images)
  POST /v2/products/quantityprice — Update existing products (price, quantity, status)

Order endpoints:
  GET  /v2/orders                — Download orders (by status)
  POST /orders/:orderId/acknowledge — Acknowledge an order
  POST /orders/fulfill           — Submit fulfillment/tracking data
  POST /orders/:orderId/cancel   — Cancel an unfillable order
"""

import json
import logging
import time

logger = logging.getLogger(__name__)
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
        # Derive base URL for order endpoints
        # e.g. https://webhook.mindcloud.co/v1/webhook/bluefly/rithum
        self.base_url = api_url.rsplit("/v2/products", 1)[0]
        self.orders_v2_url = self.base_url + "/v2/orders"
        self.orders_base_url = self.base_url + "/orders"
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

    def _put(
        self, url: str, payload: list[dict], max_retries: int = 3
    ) -> dict:
        """
        PUT JSON payload to a Bluefly/Rithum endpoint with retry logic.

        Returns same structure as _post().
        """
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        last_error = None

        for attempt in range(1, max_retries + 1):
            try:
                req = Request(url, data=body, method="PUT")
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

                if e.code >= 500 or e.code == 429:
                    wait = 2 ** attempt
                    print(
                        f"  Bluefly PUT retry {attempt}/{max_retries} "
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
                    f"  Bluefly PUT retry {attempt}/{max_retries} "
                    f"after {wait}s: {last_error}"
                )
                time.sleep(wait)

        return {
            "status_code": 0,
            "body": "",
            "success": False,
            "error": f"All {max_retries} attempts failed. Last: {last_error}",
        }

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
        catalog_url = self.api_url 
        result = self._get(catalog_url)
        if not result["success"]:
            return result

        data = result["data"]

        # Mindcloud responses may be double-encoded: ["{\"Status\":...}"]
        if isinstance(data, list) and len(data) == 1 and isinstance(data[0], str):
            try:
                data = json.loads(data[0])
                result["data"] = data
            except (json.JSONDecodeError, TypeError):
                pass

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

        Use for first-time listings.
        """
        return self._post(self.api_url, products, max_retries)

    def update_products_full(
        self, products: list[dict], max_retries: int = 3
    ) -> dict:
        """
        PUT to /v2/products — full overwrite of existing products.

        Use for repushes where the product already exists on Bluefly.
        Updates all fields including images, options, and variant data.
        """
        return self._put(self.api_url, products, max_retries)

    def update_quantity_price(
        self, products: list[dict], max_retries: int = 3
    ) -> dict:
        """
        POST to /v2/products/quantityprice — update existing products.

        Use for products/update and inventory_levels/update events.
        Updates: price, special_price, quantity, is_returnable, ListingStatus.
        """
        for p in products:
            skus = [b.get("SellerSKU", "?") for b in p.get("BuyableProducts", [])]
            statuses = [b.get("ListingStatus", "?") for b in p.get("BuyableProducts", [])]
            logger.info("update_quantity_price: SKU=%s variants=%s statuses=%s", p.get("SellerSKU"), skus, statuses)
        result = self._post(self.quantity_price_url, products, max_retries)
        logger.info("update_quantity_price result: success=%s status_code=%s body=%.500s", result.get("success"), result.get("status_code"), result.get("body", ""))
        return result

    # ------------------------------------------------------------------
    # Order API methods
    # ------------------------------------------------------------------

    def get_orders(
        self,
        status: str = "releasedforshipment",
        max_retries: int = 3,
        max_poll: int = 10,
        poll_interval: int = 3,
    ) -> dict:
        """
        GET /v2/orders — Download orders from Bluefly.

        Like the catalog endpoint, the Rithum API may be async: the first
        response can return a PendingUri which must be polled until Status
        becomes 'Complete'.

        Returns the parsed result dict with 'data' containing the order list.
        """
        from urllib.parse import quote
        url = f"{self.orders_v2_url}?status={quote(status)}&limit=2147483647"
        logger.info("get_orders: fetching status=%s url=%s", status, url)
        result = self._get(url, max_retries)
        logger.info(
            "get_orders: success=%s status_code=%s body_len=%s",
            result.get("success"), result.get("status_code"),
            len(result.get("body", "")),
        )
        if not result["success"]:
            logger.warning("get_orders: failed — %s", result.get("error"))
            return result

        data = result["data"]

        # Mindcloud responses may be double-encoded: a list containing a
        # JSON string like ["{\"Status\":\"Complete\",\"ResponseBody\":[...]}"]
        if isinstance(data, list) and len(data) == 1 and isinstance(data[0], str):
            try:
                data = json.loads(data[0])
                result["data"] = data
            except (json.JSONDecodeError, TypeError):
                pass

        if isinstance(data, dict):
            status_val = data.get("Status", "")
            pending_uri = data.get("PendingUri")

            if status_val == "Complete":
                result["data"] = data.get("ResponseBody", data)
            elif status_val == "AsyncResponsePending" and pending_uri:
                logger.info("get_orders: async pending — polling PendingUri for status=%s", status)
                for poll in range(max_poll):
                    time.sleep(poll_interval)
                    poll_result = self._get(pending_uri)
                    if not poll_result["success"]:
                        logger.warning("get_orders: poll %d failed — %s", poll + 1, poll_result.get("error"))
                        return poll_result

                    poll_data = poll_result["data"]
                    # Double-encoded check on poll response
                    if isinstance(poll_data, list) and len(poll_data) == 1 and isinstance(poll_data[0], str):
                        try:
                            poll_data = json.loads(poll_data[0])
                        except (json.JSONDecodeError, TypeError):
                            pass

                    if isinstance(poll_data, dict):
                        poll_status = poll_data.get("Status", "")
                        logger.info("get_orders: poll %d/%d Status=%s", poll + 1, max_poll, poll_status)

                        if poll_status == "Complete":
                            result["data"] = poll_data.get("ResponseBody", poll_data)
                            result["body"] = poll_result["body"]
                            break
                        elif poll_status == "AsyncResponsePending":
                            new_uri = poll_data.get("PendingUri")
                            if new_uri:
                                pending_uri = new_uri
                            continue
                        elif poll_data.get("Errors"):
                            return {
                                "status_code": poll_result["status_code"],
                                "body": poll_result["body"],
                                "data": poll_data,
                                "success": False,
                                "error": f"Rithum errors: {poll_data['Errors']}",
                            }
                else:
                    logger.warning("get_orders: polling timed out after %d attempts for status=%s", max_poll, status)
                    result["success"] = False
                    result["error"] = f"Async polling timed out after {max_poll} attempts"
                    return result
            elif data.get("Errors") and data["Errors"]:
                result["success"] = False
                result["error"] = f"Rithum errors: {data['Errors']}"

        final = result["data"]
        if isinstance(final, list):
            logger.info("get_orders: returning %d orders for status=%s", len(final), status)
        else:
            logger.info("get_orders: returning data type=%s for status=%s", type(final).__name__, status)
        return result

    def acknowledge_order(
        self, order_id: str, max_retries: int = 3
    ) -> dict:
        """
        POST /orders/:orderId/acknowledge — Acknowledge receipt of an order.
        """
        url = f"{self.orders_base_url}/{order_id}/acknowledge"
        body = b""
        last_error = None

        for attempt in range(1, max_retries + 1):
            try:
                req = Request(url, data=body, method="POST")
                req.add_header("sellerid", self.seller_id)
                req.add_header("sellertoken", self.seller_token)
                req.add_header("Content-Length", "0")

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
                if e.code >= 500 or e.code == 429:
                    time.sleep(2 ** attempt)
                    continue
                return {
                    "status_code": e.code,
                    "body": resp_body,
                    "success": False,
                    "error": last_error,
                }
            except (URLError, TimeoutError, OSError) as e:
                last_error = str(e)
                time.sleep(2 ** attempt)

        return {
            "status_code": 0,
            "body": "",
            "success": False,
            "error": f"All {max_retries} attempts failed. Last: {last_error}",
        }

    def fulfill_orders(
        self, fulfillments: list[dict], max_retries: int = 3
    ) -> dict:
        """
        POST /orders/fulfill — Submit fulfillment/tracking data.

        fulfillments: list of dicts, each with:
            {
                "ID": "BFW-...",
                "Items": [{
                    "Quantity": int,
                    "SellerSku": str,
                    "OrderItemID": str,
                    "ShippingClass": str,
                    "ShippedDateUtc": str (ISO),
                    "TrackingNumber": str,
                    "ShippingCarrier": str,
                }]
            }
        """
        url = f"{self.orders_base_url}/fulfill"
        return self._post(url, fulfillments, max_retries)

    def cancel_order(
        self, order_id: str, items: list[dict], max_retries: int = 3
    ) -> dict:
        """
        POST /orders/:orderId/cancel — Cancel an unfillable order.

        items: list of dicts, each with:
            {"ID": str, "Reason": str, "Quantity": int, "SellerSku": str}
        """
        url = f"{self.orders_base_url}/{order_id}/cancel"
        payload = {"OrderID": order_id, "Items": items}
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
                if e.code >= 500 or e.code == 429:
                    time.sleep(2 ** attempt)
                    continue
                return {
                    "status_code": e.code,
                    "body": resp_body,
                    "success": False,
                    "error": last_error,
                }
            except (URLError, TimeoutError, OSError) as e:
                last_error = str(e)
                time.sleep(2 ** attempt)

        return {
            "status_code": 0,
            "body": "",
            "success": False,
            "error": f"All {max_retries} attempts failed. Last: {last_error}",
        }
