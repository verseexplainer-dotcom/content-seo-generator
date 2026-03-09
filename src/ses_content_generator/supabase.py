from __future__ import annotations


def build_supabase_payload(record: dict[str, object]) -> dict[str, object]:
    return {
        "title": record["title"],
        "slug": record["slug"],
        "category": record["category"],
        "brand": record["brand"],
        "price_kes": record["price_kes"],
        "compare_at_price": record["compare_at_price"],
        "short_specs": record["short_specs"],
        "short_description": record["short_description"],
        "description_html": record["description_html"],
        "meta_title": record["meta_title"],
        "meta_description": record["meta_description"],
        "focus_keyword": record["focus_keyword"],
        "search_keywords": record["search_keywords"],
        "condition": record["condition"],
        "warranty": record["warranty"],
        "stock_status": record["stock_status"],
    }

