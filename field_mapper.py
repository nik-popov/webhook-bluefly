"""
Shopify → Bluefly/Rithum field transformation engine.

Maps Shopify product data to the Rithum API payload format based on:
- Excel mapping file (4 sheets: Product Fields, Variant Fields, Images, Color Mapping)
- SQL lookup results for category-specific fields

Output matches the Rithum POST /products body structure exactly.
"""
import re


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
    variant_sku = (variant.get("sku") or "").strip()
    if not variant_sku:
        gid = variant.get("id", "")
        numeric = gid.split("/")[-1] if gid else ""
        variant_sku = f"SHOPIFY-{numeric}" if numeric else ""

    if not seller_id or not product:
        return variant_sku

    # Strip any non-numeric prefix (e.g. "vpid-") from seller_id
    seller_id = re.sub(r"^[^\d]+", "", seller_id) or seller_id

    # Color from selectedOptions → metafield fallback
    color = _extract_option(variant, "color")
    if not color and metafields:
        color = get_metafield(metafields, "custom", "color") or ""
    color_slug = _slugify(color)

    # Color code: "c" + last 4 digits of variant numeric GID
    gid = variant.get("id", "")
    numeric_id = gid.split("/")[-1] if gid else ""
    color_code = f"c{numeric_id[-4:]}" if len(numeric_id) >= 4 else f"c{numeric_id}"

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

    # Build handle: brand-gender-type-in-color
    # Skip vendor if it matches the numeric seller_id (data quality guard)
    vendor = _slugify(product.get("vendor", ""))
    if vendor == seller_id:
        vendor = ""
    raw_type = product.get("productType", "")
    if "/" in raw_type:
        raw_type = raw_type.split("/")[-1].strip()
    product_type = _slugify(raw_type)
    handle_parts = [p for p in [vendor, gender, product_type] if p]
    if color_slug:
        handle_parts.append(f"in-{color_slug}")
    handle = "-".join(handle_parts)

    sku_slug = _slugify(variant_sku)
    parts = [p for p in [handle, sku_slug, color_code] if p]
    return "-".join(parts)


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

    # Size — from selectedOptions
    size_value = _extract_option(variant, "size")
    fields.append({"Name": "size", "Value": size_value or None})

    # Defaults — use config-driven values if provided
    is_returnable = field_defaults.get("is_returnable", "Not Returnable")
    product_condition = field_defaults.get("product_condition", "New")
    fields.append({"Name": "is_returnable", "Value": is_returnable})
    fields.append({"Name": "product_condition", "Value": product_condition})

    # UPC / barcode
    fields.append({"Name": "upc", "Value": variant.get("barcode") or None})

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
    for i in range(5):
        img_name = f"image_{i + 1}"
        img_url = image_sources[i] if i < len(image_sources) else None
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
        "SellerSKU": _derive_sku(variant, seller_id=seller_id, product=product, metafields=metafields),
        "ListingStatus": listing_status,
    }


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
        fields.append({"Name": "is_returnable", "Value": field_defaults.get("is_returnable", "Not Returnable")})

        selling_price = _adjust_price(variant.get("price"), price_adjustment_pct)
        compare_at = _adjust_price(variant.get("compareAtPrice"), 0)
        if compare_at is not None:
            fields.append({"Name": "price", "Value": _format_price(compare_at)})
            fields.append({"Name": "special_price", "Value": _format_price(selling_price)})
        elif selling_price is not None:
            fields.append({"Name": "price", "Value": _format_price(selling_price)})

        buyable_products.append({
            "Fields": fields,
            "Quantity": variant.get("inventoryQuantity", 0),
            "SellerSKU": _derive_sku(variant, seller_id=seller_id, product=product, metafields=metafields),
            "ListingStatus": listing_status,
        })

    seller_sku = _derive_sku(variants[0], seller_id=seller_id, product=product, metafields=metafields) if variants else ""

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
            field_defaults=field_defaults,
            seller_id=seller_id,
            product=product,
        )
        # Strip null-valued fields from variant too
        buyable["Fields"] = [
            f for f in buyable["Fields"]
            if f.get("Value") is not None
        ]
        buyable_products.append(buyable)

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

    return payload
