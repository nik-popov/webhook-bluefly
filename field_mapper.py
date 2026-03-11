"""
Shopify → Bluefly/Rithum field transformation engine.

Maps Shopify product data to the Rithum API payload format based on:
- Excel mapping file (4 sheets: Product Fields, Variant Fields, Images, Color Mapping)
- SQL lookup results for category-specific fields

Output matches the Rithum POST /products body structure exactly.
"""
import logging
import re

from field_catalog import get_catalog

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Color standard mapping — free-form color → 17 Bluefly standard values
# ---------------------------------------------------------------------------

_COLOR_STANDARD_KEYWORDS = [
    ("off white", "Off White"), ("ivory", "Off White"), ("cream", "Off White"),
    ("black", "Black"),
    ("white", "White"),
    ("beige", "Beige"), ("tan", "Beige"), ("camel", "Beige"), ("khaki", "Beige"), ("taupe", "Beige"), ("sand", "Beige"),
    ("grey", "Grey"), ("gray", "Grey"), ("charcoal", "Grey"), ("slate", "Grey"),
    ("blue", "Blue"), ("navy", "Blue"), ("cobalt", "Blue"), ("teal", "Blue"), ("denim", "Blue"), ("indigo", "Blue"),
    ("red", "Red"), ("burgundy", "Red"), ("wine", "Red"), ("maroon", "Red"), ("crimson", "Red"),
    ("green", "Green"), ("olive", "Green"), ("army", "Green"), ("sage", "Green"), ("forest", "Green"),
    ("brown", "Brown"), ("chocolate", "Brown"), ("cognac", "Brown"),
    ("gold", "Gold"),
    ("silver", "Silver"), ("metallic", "Silver"),
    ("pink", "Pink"), ("blush", "Pink"), ("rose", "Pink"), ("mauve", "Pink"), ("fuchsia", "Pink"),
    ("purple", "Purple"), ("violet", "Purple"), ("lavender", "Purple"), ("plum", "Purple"),
    ("orange", "Orange"), ("coral", "Orange"), ("rust", "Orange"), ("peach", "Orange"),
    ("yellow", "Yellow"), ("mustard", "Yellow"),
    ("multi", "Multi"), ("multicolor", "Multi"), ("pattern", "Multi"),
]


def _map_color_standard(color_str: str, default: str = "No color") -> str:
    """Map a free-form color string to one of the 17 Bluefly standard colors."""
    if not color_str:
        return default
    color_lower = color_str.lower()
    for keyword, standard in _COLOR_STANDARD_KEYWORDS:
        if keyword in color_lower:
            return standard
    return default


def _slugify(text: str) -> str:
    """Convert text to a lowercase hyphenated URL slug."""
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    return text.strip("-")


def _gender_slug(gender_str: str) -> str:
    """Normalize a gender string to a compact slug (mens/womens/unisex)."""
    if not gender_str:
        return ""
    g = gender_str.lower().strip()
    if any(w in g for w in ("woman", "women", "female")):
        return "womens"
    if any(w in g for w in ("man", "men", "male")):
        return "mens"
    if any(w in g for w in ("unisex", "neutral")):
        return "unisex"
    return _slugify(gender_str)


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


def _is_default_variant(variant: dict) -> bool:
    """Return True if this variant has no real options (Shopify single-variant placeholder)."""
    if variant.get("title", "").lower() == "default title":
        return True
    return any(
        opt.get("value", "").lower() in ("default title", "default")
        for opt in variant.get("selectedOptions", [])
    )


def _has_default_size(variants: list) -> bool:
    """Return True if any variant has a Size option with a placeholder value."""
    for v in variants:
        for opt in v.get("selectedOptions", []):
            if "size" in opt.get("name", "").lower() and opt.get("value", "").lower() in ("default", "default title", ""):
                return True
    return False


def is_default_only_product(variants: list) -> bool:
    """Return True if ALL variants are Shopify default placeholders, OR if any
    variant has a Size option set to a placeholder value like 'Default'."""
    if not variants:
        return False
    return all(_is_default_variant(v) for v in variants) or _has_default_size(variants)


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

    # name — from title, with duplicate vendor prefix removed
    raw_title = product.get("title", "")
    vendor = product.get("vendor", "")
    if vendor:
        doubled = f"{vendor} {vendor}"
        if raw_title.lower().startswith(doubled.lower()):
            raw_title = raw_title[len(vendor):].lstrip()
    fields.append({"Name": "name", "Value": raw_title})

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


def _extract_size_option(variant: dict) -> str:
    """Pull a size value from any option whose name contains 'size' (e.g. 'Shoe size', 'US Size')."""
    for opt in variant.get("selectedOptions", []):
        if "size" in opt.get("name", "").lower():
            return opt.get("value", "")
    return ""


def _derive_sku(
    variant: dict,
    seller_id: str = "",
    product: dict = None,
    metafields: list = None,
) -> str:
    """Return the variant's SellerSKU.

    When seller_id and product context are provided, generates the full
    structured SKU (all hyphens, no underscores):

        {brand}-{gender}-{type}-in-{color}-{variant_sku}-{color_code}

    Example: brunello-cucinelli-mens-shoes-in-brown-bc123-c8456

    seller_id has any non-numeric prefix (e.g. "vpid-") stripped automatically.
    Falls back to the bare variant SKU (or SHOPIFY-{id}) when context is missing.
    """
    if not seller_id or not product:
        variant_sku = (variant.get("sku") or "").strip()
        if not variant_sku:
            gid = variant.get("id", "")
            numeric = gid.split("/")[-1] if gid else ""
            variant_sku = f"SHOPIFY-{numeric}" if numeric else ""
        return variant_sku

    # Gender from metafield → tag fallback
    gender_raw = ""
    if metafields:
        gender_raw = get_metafield(metafields, "custom", "gender") or ""
    if not gender_raw:
        tags = product.get("tags", [])
        if isinstance(tags, str):
            tags = [t.strip() for t in tags.split(",")]
        for tag in tags:
            if any(w in tag.lower() for w in ("mens", "womens", "women", "men", "unisex")):
                gender_raw = tag
                break
    gender = _gender_slug(gender_raw)

    # Build handle: brand-gender-type-productId
    # Skip vendor if it matches the numeric seller_id (data quality guard)
    vendor = _slugify(product.get("vendor", ""))
    if vendor == seller_id:
        vendor = ""
    raw_type = product.get("productType", "")
    if "/" in raw_type:
        raw_type = raw_type.split("/")[-1].strip()
    product_type = _slugify(raw_type)

    # Use brand_product_id metafield as the unique product identifier in the handle
    brand_product_id = ""
    if metafields:
        brand_product_id = get_metafield(metafields, "custom", "brand_product_id") or ""
    brand_product_id = _slugify(brand_product_id)

    handle_parts = [p for p in [vendor, gender, product_type, brand_product_id] if p]
    derived = "-".join(handle_parts)
    # Do NOT prepend seller_id — Mindcloud adds it automatically
    return derived


def parse_seller_sku(seller_sku: str) -> dict:
    """Reverse-parse a structured seller SKU into its component parts.

    Input format:  {seller_id}_{brand}-{gender}-{type}-{variant_sku}-{color_code}
    Example:       10009_brunello-cucinelli-mens-shoes-mzusofy752-c4407
    Legacy format: 10009_brunello-cucinelli-mens-shoes-in-multi-mzusofy752-c4407

    Returns dict with keys: seller_id, brand, gender, category, color.
    Values are title-cased for display.  Missing parts default to "".
    """
    result = {"seller_id": "", "brand": "", "gender": "", "category": "", "color": ""}

    if "_" not in seller_sku:
        return result

    seller_id, _, body = seller_sku.partition("_")
    result["seller_id"] = seller_id

    # Split on "-in-" to separate the handle from the color+sku tail
    in_split = body.split("-in-", 1)
    handle = in_split[0]  # brand-gender-type

    if len(in_split) > 1:
        tail = in_split[1]  # color-variant_sku-color_code
        # Color is everything before the last model/sku chunk
        # The color_code is always the last segment matching c\d+
        # Walk backwards to find where the color ends
        tail_parts = tail.split("-")
        # Find where color ends: first part that looks like a model number
        # (contains digits mixed with letters and is long, or starts with known patterns)
        color_parts = []
        for i, part in enumerate(tail_parts):
            # Stop at color_code (cNNNN) or model-number-like segments
            if re.match(r"^c\d{3,5}$", part):
                break
            # Stop at segments that look like model numbers (mix of letters+digits, 6+ chars)
            if len(part) >= 6 and re.search(r"\d", part) and re.search(r"[a-z]", part):
                break
            # Stop at "shopify" prefix (synthetic SKU)
            if part == "shopify":
                break
            color_parts.append(part)
        result["color"] = " ".join(color_parts).title() if color_parts else ""

    # Parse handle: brand tokens, then gender, then category
    # Stop collecting category when we hit model-number-like segments
    handle_parts = handle.split("-")
    gender_keywords = {"mens", "womens", "unisex"}

    brand_parts = []
    gender = ""
    category_parts = []
    found_gender = False

    for part in handle_parts:
        if not found_gender and part in gender_keywords:
            gender = part
            found_gender = True
        elif found_gender:
            # Stop at color_code (cNNNN), model numbers, or "shopify"
            if re.match(r"^c\d{3,5}$", part):
                break
            if len(part) >= 6 and re.search(r"\d", part) and re.search(r"[a-z]", part):
                break
            if part == "shopify":
                break
            category_parts.append(part)
        else:
            brand_parts.append(part)

    result["brand"] = " ".join(brand_parts).title() if brand_parts else ""
    result["gender"] = gender.title() if gender else ""
    result["category"] = " ".join(category_parts).title() if category_parts else ""

    return result


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
    field_defaults: dict = None,
    seller_id: str = "",
    product: dict = None,
    category_id: str = "",
    size_scale: str = "",
    size_field_override: str = "",
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
    field_defaults = field_defaults or {}
    fields = []

    # Color — try selectedOptions first, then custom.color metafield
    color_display = _extract_option(variant, "color")
    if not color_display:
        color_display = get_metafield(metafields, "custom", "color") or ""
    fields.append({"Name": "color", "Value": color_display or None})

    # Color Standard — auto-map from color string to one of 17 Bluefly standard values
    color_std_default = field_defaults.get("color_standard", "No color")
    fields.append({"Name": "color_standard", "Value": _map_color_standard(color_display, default=color_std_default)})

    # Size — resolve category-specific size field via local catalog
    size_error = None
    size_value = _extract_size_option(variant)
    if size_value and category_id:
        catalog = get_catalog()
        size_result = catalog.resolve_size(category_id, size_value, size_scale=size_scale, size_field_override=size_field_override)
        if size_result["field_name"]:
            if size_result.get("value"):
                fields.append({"Name": size_result["field_name"], "Value": size_result["value"]})
            elif sql_fields.get(size_result["field_name"]):
                # bf_mapping has a manual override for this size field (original Shopify size)
                fields.append({"Name": size_result["field_name"], "Value": sql_fields[size_result["field_name"]]})
            else:
                # Try bf_mapping with the brand-converted value (e.g., 90C → 36C via brand table)
                brand_conv_raw = size_result.get("brand_converted_raw")
                bf_conv_val = None
                if brand_conv_raw and brand_conv_raw != size_value:
                    from sql_lookup import BlueflyDBLookup
                    from d1_client import get_d1_client
                    conv_fields = BlueflyDBLookup(get_d1_client()).lookup_category_fields(category_id, brand_conv_raw)
                    bf_conv_val = conv_fields.get(size_result["field_name"])

                if bf_conv_val:
                    fields.append({"Name": size_result["field_name"], "Value": bf_conv_val})
                else:
                    _gender = get_metafield(metafields, "custom", "gender") or "?"
                    logger.warning(
                        "G:%s Size mapping failed: variant=%s size='%s' -> %s",
                        _gender, variant.get("id", "?"), size_value, size_result.get("error", "unknown"),
                    )
                    size_error = {
                        "variant_id": variant.get("id", ""),
                        "shopify_size": size_value,
                        "field_name": size_result["field_name"],
                        "error": size_result.get("error", "unknown"),
                    }
                    if size_result.get("brand_converted_raw"):
                        size_error["brand_converted"] = size_result["brand_converted_raw"]
                    if size_scale:
                        size_error["size_scale"] = size_scale
                    if size_result.get("suggestion"):
                        size_error["suggestion"] = size_result["suggestion"]
        else:
            # Category has no size field — send nothing
            pass
    elif size_value:
        # No category_id — fall back to generic size field
        fields.append({"Name": "size", "Value": size_value})

    # Defaults — use config-driven values if provided
    is_returnable = field_defaults.get("is_returnable", "NotReturnable")
    product_condition = field_defaults.get("product_condition", "New")
    fields.append({"Name": "is_returnable", "Value": is_returnable})
    fields.append({"Name": "product_condition", "Value": product_condition})
    raw_sku = (variant.get("sku") or "").strip()
    # UPC / barcode — use Shopify variant ID as unique barcode per variant
    variant_gid_for_upc = variant.get("id", "")
    upc_value = variant_gid_for_upc.split("/")[-1] if "/" in str(variant_gid_for_upc) else str(variant_gid_for_upc)
    if not upc_value:
        upc_value = variant.get("barcode") or raw_sku
    fields.append({"Name": "upc", "Value": upc_value})

    # Price mapping:
    #   Bluefly price         = compareAtPrice (MSRP/retail, the "was" price) — no adjustment
    #   Bluefly special_price = price (actual selling/discounted price) — adjustment applied
    # If compareAtPrice is not set, fall back to price (with adjustment) for the list price
    # and omit special_price (no discount to show).
    selling_price = _adjust_price(variant.get("price"), price_adjustment_pct)
    compare_at = _adjust_price(variant.get("compareAtPrice"), 0)
    if compare_at is not None:
        fields.append({"Name": "price", "Value": _format_price(compare_at)})
        fields.append({"Name": "special_price", "Value": _format_price(selling_price)})
    else:
        fields.append({"Name": "price", "Value": _format_price(selling_price)})
        fields.append({"Name": "special_price", "Value": None})

    # Images: variant image first (most relevant for color variants), then product images.
    # De-duplicate so the variant image doesn't appear twice if it's also in product images.
    variant_img = (variant.get("image") or {}).get("url")
    image_sources = []
    if variant_img:
        image_sources.append(variant_img)
    for img in product_images:
        url = img.get("url")
        if url and url not in image_sources:
            image_sources.append(url)
    for i, img_url in enumerate(image_sources[:5], 1):
        fields.append({"Name": f"image_{i}", "Value": img_url})

    # Weight
    fields.append({"Name": "weight", "Value": _format_weight(variant.get("weight"))})

    # Raw Shopify SKU — stored so the dashboard can match catalog entries back to Shopify products

    if raw_sku:
        fields.append({"Name": "shopify_sku", "Value": raw_sku})

    # Shopify product ID — primary key for matching catalog entries to Shopify products
    if product:
        product_gid = product.get("id", "")
        product_id = product_gid.split("/")[-1] if "/" in str(product_gid) else str(product_gid)
        if product_id:
            fields.append({"Name": "shopify_product_id", "Value": product_id})

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

    # Variant SellerSKU = Shopify variant numeric ID for reliable catalog matching
    variant_gid = variant.get("id", "")
    variant_seller_sku = variant_gid.split("/")[-1] if "/" in str(variant_gid) else str(variant_gid)

    buyable = {
        "Fields": fields,
        "Quantity": variant.get("inventoryQuantity", 0),
        "SellerSKU": variant_seller_sku,
        "ListingStatus": listing_status,
    }
    if size_error:
        buyable["_size_error"] = size_error
    return buyable


# ---------------------------------------------------------------------------
# Full payload builder
# ---------------------------------------------------------------------------

def build_quantity_price_payload(
    product: dict,
    metafields: list,
    price_adjustment_pct: float = 0,
    field_defaults: dict = None,
    seller_id: str = "",
) -> dict:
    """
    Build a lightweight payload for the /quantityprice endpoint.

    Used for products/update and inventory_levels/update events
    on products that already exist on Bluefly.

    Only sends: price, special_price, is_returnable, quantity, listing status.
    Product-level Fields is always [].
    """
    field_defaults = field_defaults or {}
    variants = product.get("variants", [])
    product_status = product.get("status", "ACTIVE")
    listing_status = "Live" if product_status.upper() == "ACTIVE" else "NotLive"

    buyable_products = []
    for variant in variants:
        fields = []
        fields.append({"Name": "is_returnable", "Value": field_defaults.get("is_returnable", "NotReturnable")})

        selling_price = _adjust_price(variant.get("price"), price_adjustment_pct)
        compare_at = _adjust_price(variant.get("compareAtPrice"), 0)
        if compare_at is not None:
            fields.append({"Name": "price", "Value": _format_price(compare_at)})
            fields.append({"Name": "special_price", "Value": _format_price(selling_price)})
        elif selling_price is not None:
            fields.append({"Name": "price", "Value": _format_price(selling_price)})

        variant_gid = variant.get("id", "")
        variant_seller_sku = variant_gid.split("/")[-1] if "/" in str(variant_gid) else str(variant_gid)

        buyable_products.append({
            "Fields": fields,
            "Quantity": variant.get("inventoryQuantity", 0),
            "SellerSKU": variant_seller_sku,
            "ListingStatus": listing_status,
        })

    seller_sku = _derive_sku(variants[0], seller_id=seller_id, product=product, metafields=metafields) if variants else ""

    return {
        "Fields": [],
        "SellerSKU": seller_sku,
        "BuyableProducts": buyable_products,
    }


def extract_qty_price_payload(full_payload: dict) -> dict:
    """Extract a quantityprice payload from a full bluefly product payload.

    Reuses the merged BuyableProducts so variant-collapsed sizes
    (e.g. 95B+95C+95D → XXL) keep their summed quantities.
    """
    return {
        "Fields": [],
        "SellerSKU": full_payload["SellerSKU"],
        "BuyableProducts": [
            {
                "Fields": [f for f in bp["Fields"]
                           if f["Name"] in ("price", "special_price", "is_returnable")],
                "Quantity": bp["Quantity"],
                "SellerSKU": bp["SellerSKU"],
                "ListingStatus": bp["ListingStatus"],
            }
            for bp in full_payload.get("BuyableProducts", [])
        ],
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
    field_defaults: dict = None,
    seller_id: str = "",
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

    # Variants — strip Shopify default placeholder variants (no real options)
    variants = [v for v in product.get("variants", []) if not _is_default_variant(v)]
    images = product.get("images", [])
    product_status = product.get("status", "ACTIVE")

    # Extract category_id and size scale hint for size field resolution
    category_id = get_metafield(metafields, "custom", "bluefly_category") or ""
    size_scale = get_metafield(metafields, "custom", "ff_size_scale") or ""
    size_field_override = get_metafield(metafields, "custom", "size_field_override") or ""

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
            field_defaults=field_defaults,
            seller_id=seller_id,
            product=product,
            category_id=category_id,
            size_scale=size_scale,
            size_field_override=size_field_override,
        )
        # Strip null-valued fields from variant too
        buyable["Fields"] = [
            f for f in buyable["Fields"]
            if f.get("Value") is not None
        ]
        buyable_products.append(buyable)

    # Collect size errors and exclude error variants from BuyableProducts
    size_errors = []
    good_buyables = []
    for bp in buyable_products:
        err = bp.pop("_size_error", None)
        if err:
            size_errors.append(err)
        else:
            good_buyables.append(bp)

    # Merge buyables that resolved to the same size (e.g. 95B,95C,95D → XXL)
    # Sum quantities and keep the first entry's fields/SKU.
    merged = {}
    for bp in good_buyables:
        size_val = None
        for f in bp["Fields"]:
            if f["Name"].startswith("size_"):
                size_val = (f["Name"], f["Value"])
                break
        key = size_val or bp["SellerSKU"]
        if key in merged:
            merged[key]["Quantity"] += bp.get("Quantity", 0)
        else:
            merged[key] = bp
    buyable_products = list(merged.values())

    # SellerSKU at product level = first variant's derived SKU
    seller_sku = _derive_sku(variants[0], seller_id=seller_id, product=product, metafields=metafields) if variants else ""

    # Build Options (variant differentiators like Color, Size)
    options = _build_options(variants, metafields)

    payload = {
        "Fields": product_fields,
        "SellerSKU": seller_sku,
        "BuyableProducts": buyable_products,
    }
    if options:
        payload["Options"] = options
    if size_errors:
        payload["_size_errors"] = size_errors

    return payload
