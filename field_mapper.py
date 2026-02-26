"""
Shopify → Bluefly/Rithum field transformation engine.

Maps Shopify product data to the Rithum API payload format based on:
- Excel mapping file (4 sheets: Product Fields, Variant Fields, Images, Color Mapping)
- SQL lookup results for category-specific fields
- Color standardization rules

Output matches the Rithum POST /products body structure exactly.
"""


# ---------------------------------------------------------------------------
# Color standardization: Shopify display color → Bluefly standard color
# ---------------------------------------------------------------------------

COLOR_STANDARD_MAP = {
    # Black family
    "black": "Black",
    "noir": "Black",
    "ebony": "Black",
    "onyx": "Black",
    "jet": "Black",
    "matte black": "Black",
    "shiny black": "Black",
    # Brown family
    "brown": "Brown",
    "tan": "Brown",
    "chocolate": "Brown",
    "cognac": "Brown",
    "camel": "Brown",
    "nude": "Brown",
    "tortoise": "Brown",
    "havana": "Brown",
    "dark havana": "Brown",
    "porcini tortoise": "Brown",
    "milky hazelnut": "Brown",
    "tortoise pink fade": "Brown",
    # Gold family
    "gold": "Gold",
    "golden": "Gold",
    "champagne": "Gold",
    "shiny light gold": "Gold",
    "light gold": "Gold",
    # Pink family
    "pink": "Pink",
    "rose": "Pink",
    "blush": "Pink",
    "fuchsia": "Pink",
    "coral": "Pink",
    "matte pink": "Pink",
    # Silver family
    "silver": "No color",
    "grey": "No color",
    "gray": "No color",
    # White / transparent / crystal
    "white": "No color",
    "clear": "No color",
    "crystal": "No color",
    "transparent": "No color",
    "clear transparent": "No color",
}


def standardize_color(color_display: str) -> str:
    """Map a display color name to a Bluefly standard color value."""
    if not color_display:
        return "No color"
    key = color_display.strip().lower()
    # Exact match first
    if key in COLOR_STANDARD_MAP:
        return COLOR_STANDARD_MAP[key]
    # Partial match: check if any known keyword appears
    for term, standard in COLOR_STANDARD_MAP.items():
        if term in key:
            return standard
    return "No color"


# ---------------------------------------------------------------------------
# Metafield helpers
# ---------------------------------------------------------------------------

def get_metafield(metafields: list, namespace: str, key: str) -> str | None:
    """Extract a metafield value from the flattened metafields list."""
    for mf in metafields:
        if mf.get("namespace") == namespace and mf.get("key") == key:
            return mf.get("value")
    return None


# ---------------------------------------------------------------------------
# Product-level field mapping
# ---------------------------------------------------------------------------

def should_sync_product(product: dict) -> bool:
    """Return True if product should be synced to Bluefly (status is ACTIVE)."""
    status = product.get("status", "").upper()
    return status == "ACTIVE"


def _parse_tags_for_field(tags: list[str], keywords: list[str]) -> str | None:
    """Search tags for any keyword match. Returns first match or None."""
    for tag in tags:
        tag_lower = tag.strip().lower()
        for kw in keywords:
            if kw in tag_lower:
                return tag.strip()
    return None


def map_product_fields(product: dict, metafields: list) -> list[dict]:
    """
    Build the product-level Fields array for Rithum API.

    Returns: [{"Name": "category", "Value": "194"}, ...]
    """
    fields = []

    # category — from metafield custom.bluefly_category
    category_id = get_metafield(metafields, "custom", "bluefly_category")
    fields.append({"Name": "category", "Value": category_id or ""})

    # brand — from vendor
    fields.append({"Name": "brand", "Value": product.get("vendor", "")})

    # name — from title
    fields.append({"Name": "name", "Value": product.get("title", "")})

    # description — from descriptionHtml (HTML allowed)
    fields.append(
        {"Name": "description", "Value": product.get("descriptionHtml", "")}
    )

    # type_frames — from productType (when applicable)
    product_type = product.get("productType", "")
    fields.append({"Name": "type_frames", "Value": product_type or None})

    # material_clothing — parse from tags
    tags = product.get("tags", [])
    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(",")]
    material_keywords = ["leather", "silk", "cotton", "wool", "polyester", "metal", "plastic", "acetate"]
    material = _parse_tags_for_field(tags, material_keywords)
    fields.append({"Name": "material_clothing", "Value": material})

    # pattern — parse from tags
    pattern_keywords = ["stripe", "plaid", "check", "floral", "solid", "print", "geometric"]
    pattern = _parse_tags_for_field(tags, pattern_keywords)
    fields.append({"Name": "pattern", "Value": pattern})

    # gender — from metafield custom.gender
    gender = get_metafield(metafields, "custom", "gender")
    fields.append({"Name": "gender", "Value": gender})

    # sub_category — from metafield custom.sub_category
    sub_category = get_metafield(metafields, "custom", "sub_category")
    fields.append({"Name": "sub_category", "Value": sub_category})

    # care_instructions — no native Shopify field, use metafield if available
    care = get_metafield(metafields, "custom", "care_instructions")
    fields.append({"Name": "care_instructions", "Value": care})

    # country_of_manufacture — use metafield if available
    country = get_metafield(metafields, "custom", "country_of_origin")
    fields.append({"Name": "country_of_manufacture", "Value": country})

    # size_notes — use metafield if available
    size_notes = get_metafield(metafields, "custom", "size_notes")
    fields.append({"Name": "size_notes", "Value": size_notes})

    return fields


# ---------------------------------------------------------------------------
# Variant / BuyableProduct mapping
# ---------------------------------------------------------------------------

def _extract_option(variant: dict, option_name: str) -> str:
    """Pull an option value from variant.selectedOptions by name."""
    for opt in variant.get("selectedOptions", []):
        if opt.get("name", "").lower() == option_name.lower():
            return opt.get("value", "")
    return ""


def _adjust_price(price, adjustment_pct: float = 0) -> float | None:
    """Apply a percentage adjustment to a price value.

    Args:
        price: Original price (str, float, or None)
        adjustment_pct: Percentage to adjust (e.g. 20 = +20%, -10 = -10%)

    Returns:
        Adjusted price as float, or None if input is None.
    """
    if price is None:
        return None
    try:
        p = float(price)
    except (ValueError, TypeError):
        return None
    if adjustment_pct:
        p = p * (1 + adjustment_pct / 100)
    return round(p, 2)


def _format_price(price) -> str:
    """Ensure price is formatted as a string with 2 decimal places."""
    if price is None:
        return None
    try:
        return f"{float(price):.2f}"
    except (ValueError, TypeError):
        return str(price)


def _format_weight(weight) -> str:
    """Format weight as a string with 4 decimal places."""
    if weight is None:
        return None
    try:
        return f"{float(weight):.4f}"
    except (ValueError, TypeError):
        return str(weight)


def map_variant_to_buyable(
    variant: dict,
    product_images: list,
    sql_fields: dict,
    product_status: str,
    metafields: list = None,
    price_adjustment_pct: float = 0,
) -> dict:
    """
    Transform a single Shopify variant into a Rithum BuyableProduct entry.

    Args:
        variant: Shopify variant dict (flattened from GraphQL)
        product_images: List of image dicts [{url, altText}, ...]
        sql_fields: Dict of {FieldName: BFValue} from SQL lookup for this variant
        product_status: Shopify product status (ACTIVE/DRAFT/ARCHIVED)
        metafields: Product metafields list (fallback for color, etc.)

    Returns:
        BuyableProduct dict matching Rithum API structure.
    """
    metafields = metafields or []
    fields = []

    # Color — try selectedOptions first, then custom.color metafield
    color_display = _extract_option(variant, "color")
    if not color_display:
        color_display = get_metafield(metafields, "custom", "color") or ""
    fields.append({"Name": "color", "Value": color_display or None})
    fields.append({"Name": "color_standard", "Value": color_display or None})

    # Size — from selectedOptions
    size_value = _extract_option(variant, "size")
    fields.append({"Name": "size", "Value": size_value or None})

    # Defaults
    fields.append({"Name": "is_returnable", "Value": "Returnable"})
    fields.append({"Name": "product_condition", "Value": "New"})

    # UPC / barcode
    fields.append({"Name": "upc", "Value": variant.get("barcode") or None})

    # Price (apply adjustment if configured)
    price = _adjust_price(variant.get("price"), price_adjustment_pct)
    compare_at = _adjust_price(variant.get("compareAtPrice"), price_adjustment_pct)
    fields.append({"Name": "price", "Value": _format_price(price)})
    fields.append({"Name": "special_price", "Value": _format_price(compare_at)})

    # Images (up to 5 from product-level images)
    for i in range(5):
        img_name = f"image_{i + 1}"
        img_url = None
        if i < len(product_images):
            img_url = product_images[i].get("url")
        fields.append({"Name": img_name, "Value": img_url})

    # Weight
    fields.append({"Name": "weight", "Value": _format_weight(variant.get("weight"))})

    # SQL-looked-up fields (glasses_magnification, size fields, etc.)
    for field_name, bf_value in sql_fields.items():
        # Check if we already have this field; if so, override
        existing = next(
            (f for f in fields if f["Name"] == field_name), None
        )
        if existing:
            existing["Value"] = bf_value
        else:
            fields.append({"Name": field_name, "Value": bf_value})

    # ListingStatus
    listing_status = "Live" if product_status.upper() == "ACTIVE" else "NotLive"

    return {
        "Fields": fields,
        "Quantity": variant.get("inventoryQuantity", 0),
        "SellerSKU": variant.get("sku", ""),
        "ListingStatus": listing_status,
    }


# ---------------------------------------------------------------------------
# Full payload builder
# ---------------------------------------------------------------------------

def build_quantity_price_payload(
    product: dict,
    metafields: list,
    price_adjustment_pct: float = 0,
) -> dict:
    """
    Build a lightweight payload for the /quantityprice endpoint.

    Used for products/update and inventory_levels/update events
    on products that already exist on Bluefly.

    Only sends: price, special_price, is_returnable, quantity, listing status.
    Product-level Fields is always [].
    """
    variants = product.get("variants", [])
    product_status = product.get("status", "ACTIVE")
    listing_status = "Live" if product_status.upper() == "ACTIVE" else "NotLive"

    buyable_products = []
    for variant in variants:
        fields = []
        fields.append({"Name": "is_returnable", "Value": "Returnable"})

        price = _adjust_price(variant.get("price"), price_adjustment_pct)
        if price is not None:
            fields.append({"Name": "price", "Value": _format_price(price)})

        compare_at = _adjust_price(variant.get("compareAtPrice"), price_adjustment_pct)
        if compare_at is not None:
            fields.append({"Name": "special_price", "Value": _format_price(compare_at)})

        buyable_products.append({
            "Fields": fields,
            "Quantity": variant.get("inventoryQuantity", 0),
            "SellerSKU": variant.get("sku", ""),
            "ListingStatus": listing_status,
        })

    seller_sku = variants[0].get("sku", "") if variants else ""

    return {
        "Fields": [],
        "SellerSKU": seller_sku,
        "BuyableProducts": buyable_products,
    }


def _build_options(variants: list, metafields: list) -> list[dict]:
    """
    Build the product-level Options array for Rithum.

    Rithum needs Options to know which fields are variant differentiators
    (e.g. Color, Size). Without this, the dashboard shows empty options.

    Returns: [{"Name": "color"}, {"Name": "size"}]
    """
    option_names = []
    seen = set()

    for variant in variants:
        for opt in variant.get("selectedOptions", []):
            opt_name = opt.get("name", "")
            # Skip "Title" — Shopify default when there are no real options
            if opt_name and opt_name.lower() != "title" and opt_name.lower() not in seen:
                option_names.append(opt_name.lower())
                seen.add(opt_name.lower())

    # If no selectedOptions had color but we have it from metafield, add it
    if "color" not in seen:
        color_val = get_metafield(metafields, "custom", "color")
        if color_val:
            option_names.insert(0, "color")

    return [{"Name": name} for name in option_names]


def build_bluefly_payload(
    product: dict,
    metafields: list,
    sql_field_map: dict,
    price_adjustment_pct: float = 0,
) -> dict:
    """
    Build the complete Rithum API body for one product.

    Args:
        product: Enriched Shopify product (flattened from GraphQL)
        metafields: List of metafield dicts [{namespace, key, value, type}, ...]
        sql_field_map: {variant_title: {FieldName: BFValue}} from SQL lookups

    Returns:
        Product dict matching Rithum POST /products body structure.
    """
    # Product-level fields -- strip nulls so Rithum doesn't choke
    product_fields = [
        f for f in map_product_fields(product, metafields)
        if f.get("Value") is not None
    ]

    # Variants
    variants = product.get("variants", [])
    images = product.get("images", [])
    product_status = product.get("status", "ACTIVE")

    buyable_products = []
    for variant in variants:
        variant_title = variant.get("title", "")
        sql_fields = sql_field_map.get(variant_title, {})

        buyable = map_variant_to_buyable(
            variant=variant,
            product_images=images,
            sql_fields=sql_fields,
            product_status=product_status,
            metafields=metafields,
            price_adjustment_pct=price_adjustment_pct,
        )
        # Strip null-valued fields from variant too
        buyable["Fields"] = [
            f for f in buyable["Fields"]
            if f.get("Value") is not None
        ]
        buyable_products.append(buyable)

    # SellerSKU at product level = first variant's SKU
    seller_sku = variants[0].get("sku", "") if variants else ""

    # Build Options (variant differentiators like Color, Size)
    options = _build_options(variants, metafields)

    payload = {
        "Fields": product_fields,
        "SellerSKU": seller_sku,
        "BuyableProducts": buyable_products,
    }
    if options:
        payload["Options"] = options

    return payload
