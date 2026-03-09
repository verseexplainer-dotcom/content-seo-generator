from __future__ import annotations

import csv
import json
import re
from datetime import datetime, timezone
from html import escape
from pathlib import Path

from src.ses_content_generator.config import render_template
from src.ses_content_generator.models import NormalizedInput, ParsedSpecs, RunSummary
from src.ses_content_generator.utils import (
    clean_text,
    dedupe_preserve_order,
    dump_json,
    format_kes,
    human_join,
    normalize_header,
    parse_int,
    round_up,
    slugify,
    smart_trim,
    split_fragments,
    unique_slug,
)
from src.ses_content_generator.xlsx_reader import read_xlsx

OUTPUT_FIELDS = [
    "title",
    "slug",
    "category",
    "brand",
    "price_kes",
    "compare_at_price",
    "short_specs",
    "short_description",
    "description_html",
    "meta_title",
    "meta_description",
    "focus_keyword",
    "search_keywords",
    "condition",
    "warranty",
    "stock_status",
]

REQUIRED_OUTPUT_FIELDS = [
    "title",
    "slug",
    "category",
    "brand",
    "price_kes",
    "compare_at_price",
    "short_description",
    "description_html",
    "meta_title",
    "meta_description",
    "focus_keyword",
    "condition",
    "warranty",
    "stock_status",
]

REJECTION_FIELDS = [
    "source_row_number",
    "source_kind",
    "source_sheet",
    "name",
    "key_specs",
    "price",
    "rejection_reason",
    "raw_payload_json",
]

CPU_PATTERN = re.compile(
    r"\b(core\s*i[3579]|i[3579]|ryzen\s*\d|celeron|pentium|ultra\s*[3579]|m[1234])\b",
    re.IGNORECASE,
)
CAPACITY_PATTERN = re.compile(r"^\d+\s?(gb|tb)$", re.IGNORECASE)
GEN_PATTERN = re.compile(r"\b\d{1,2}(st|nd|rd|th)\s+gen\b", re.IGNORECASE)
GRAPHICS_PATTERN = re.compile(r"\bgraphics\b|\bquadro\b|\brtx\b|\bgtx\b|\bradeon\b", re.IGNORECASE)
TOUCH_PATTERN = re.compile(r"\btouch\b", re.IGNORECASE)
ACCESSORY_HINTS = {"Accessory", "Monitor", "Desktop Computer"}

BRAND_CASE_MAP = {
    "hp": "HP",
    "dell": "Dell",
    "lenovo": "Lenovo",
    "microsoft": "Microsoft",
    "apple": "Apple",
    "acer": "Acer",
    "asus": "ASUS",
    "msi": "MSI",
    "samsung": "Samsung",
    "fujitsu": "Fujitsu",
}

MODEL_CASE_MAP = {
    "zbook": "ZBook",
    "elitebook": "EliteBook",
    "probook": "ProBook",
    "thinkpad": "ThinkPad",
    "macbook": "MacBook",
    "surface": "Surface",
}


def process_input_file(
    input_path: Path,
    output_path: Path,
    rejected_path: Path,
    log_path: Path,
    config: dict,
    prompts: dict[str, str],
    llm_refiner=None,
    sheet_name: str | None = None,
) -> RunSummary:
    rows, source_kind, active_sheet = read_input_rows(input_path, sheet_name=sheet_name)
    used_slugs: set[str] = set()
    cleaned_rows: list[dict[str, object]] = []
    rejected_rows: list[dict[str, str]] = []
    row_logs: list[dict[str, object]] = []

    started_at = timestamp_now()
    for raw_row in rows:
        try:
            normalized = normalize_source_row(raw_row, source_kind=source_kind, source_sheet=active_sheet)
        except ValueError as exc:
            rejected_rows.append(build_rejection_record(raw_row, source_kind, active_sheet, str(exc)))
            row_logs.append(
                {
                    "source_row_number": raw_row.get("__row_number__", ""),
                    "status": "rejected",
                    "reason": str(exc),
                }
            )
            continue

        input_errors = validate_normalized_input(normalized)
        if input_errors:
            reason = "; ".join(input_errors)
            rejected_rows.append(build_rejection_record(normalized.raw_row, source_kind, active_sheet, reason, normalized))
            row_logs.append(
                {
                    "source_row_number": normalized.source_row_number,
                    "status": "rejected",
                    "reason": reason,
                }
            )
            continue

        generated_record, warnings = build_generated_record(normalized, config, used_slugs)
        llm_status = "skipped"
        llm_warning = ""
        if llm_refiner is not None:
            generated_record, llm_status, llm_warning = refine_with_llm(
                generated_record=generated_record,
                normalized=normalized,
                config=config,
                prompts=prompts,
                llm_refiner=llm_refiner,
            )
            if llm_warning:
                warnings.append(llm_warning)

        output_errors = validate_output_record(generated_record, config)
        if output_errors:
            reason = "; ".join(output_errors)
            rejected_rows.append(build_rejection_record(normalized.raw_row, source_kind, active_sheet, reason, normalized))
            row_logs.append(
                {
                    "source_row_number": normalized.source_row_number,
                    "status": "rejected",
                    "reason": reason,
                    "warnings": warnings,
                    "llm_status": llm_status,
                }
            )
            continue

        cleaned_rows.append(generated_record)
        row_logs.append(
            {
                "source_row_number": normalized.source_row_number,
                "status": "cleaned",
                "slug": generated_record["slug"],
                "brand": generated_record["brand"],
                "category": generated_record["category"],
                "warnings": warnings,
                "llm_status": llm_status,
            }
        )

    write_cleaned_csv(output_path, cleaned_rows)
    write_rejected_csv(rejected_path, rejected_rows)

    log_payload = {
        "started_at": started_at,
        "finished_at": timestamp_now(),
        "input_file": str(input_path),
        "input_kind": source_kind,
        "sheet_name": active_sheet,
        "used_llm": llm_refiner is not None,
        "summary": {
            "rows_read": len(rows),
            "cleaned_rows": len(cleaned_rows),
            "rejected_rows": len(rejected_rows),
        },
        "outputs": {
            "cleaned_output": str(output_path),
            "rejected_rows": str(rejected_path),
            "generation_log": str(log_path),
        },
        "config_snapshot": {
            "store_name": config.get("store_name", ""),
            "location": config.get("location", ""),
            "currency": config.get("currency", ""),
            "default_condition": config.get("condition_labels", {}).get("default", ""),
            "default_warranty_text": config.get("default_warranty_text", ""),
        },
        "rows": row_logs,
    }
    write_log_json(log_path, log_payload)

    return RunSummary(
        rows_read=len(rows),
        cleaned_rows=len(cleaned_rows),
        rejected_rows=len(rejected_rows),
        used_llm=llm_refiner is not None,
        output_file=str(output_path),
        rejected_file=str(rejected_path),
        log_file=str(log_path),
    )


def read_input_rows(input_path: Path, sheet_name: str | None = None) -> tuple[list[dict[str, str]], str, str | None]:
    suffix = input_path.suffix.lower()
    if suffix == ".xlsx":
        rows, active_sheet = read_xlsx(input_path, sheet_name=sheet_name)
        return rows, "xlsx", active_sheet
    if suffix == ".csv":
        rows: list[dict[str, str]] = []
        with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            for row_number, row in enumerate(reader, start=2):
                rows.append({"__row_number__": str(row_number), **{key: clean_text(value) for key, value in row.items()}})
        return rows, "csv", None
    raise ValueError(f"Unsupported input file type: {input_path.suffix}")


def normalize_source_row(raw_row: dict[str, str], source_kind: str, source_sheet: str | None) -> NormalizedInput:
    normalized_keys = {normalize_header(key): clean_text(value) for key, value in raw_row.items() if key != "__row_number__"}
    row_number = parse_int(raw_row.get("__row_number__", "")) or 0
    has_simple_shape = {"name", "key_specs", "price"} <= set(normalized_keys)
    has_workbook_shape = "model" in normalized_keys and ("price_kes" in normalized_keys or "price" in normalized_keys)

    if has_simple_shape:
        name = normalized_keys.get("name", "")
        key_specs = normalized_keys.get("key_specs", "")
        price = normalized_keys.get("price", "")
    elif has_workbook_shape:
        name = normalized_keys.get("model", "") or normalized_keys.get("name", "")
        key_specs_parts = []
        if normalized_keys.get("cpu"):
            key_specs_parts.append(f"CPU: {normalized_keys['cpu']}")
        if normalized_keys.get("ram"):
            key_specs_parts.append(f"RAM: {normalized_keys['ram']}")
        if normalized_keys.get("storage"):
            key_specs_parts.append(f"Storage: {normalized_keys['storage']}")
        key_specs_parts.extend(split_fragments(normalized_keys.get("notes", "")))
        key_specs = " | ".join(key_specs_parts)
        price = normalized_keys.get("price_kes", "") or normalized_keys.get("price", "")
    else:
        raise ValueError("Unsupported columns. Expected name/key_specs/price or the SES workbook layout.")

    return NormalizedInput(
        source_row_number=row_number,
        source_kind=source_kind,
        source_sheet=source_sheet,
        raw_row={key: clean_text(value) for key, value in raw_row.items() if key != "__row_number__"},
        name=clean_text(name),
        key_specs=clean_text(key_specs),
        price=clean_text(price),
    )


def validate_normalized_input(record: NormalizedInput) -> list[str]:
    errors: list[str] = []
    if not record.name:
        errors.append("name is required")
    if not record.key_specs:
        errors.append("key_specs is required")
    price_value = parse_int(record.price)
    if price_value is None or price_value <= 0:
        errors.append("price must be a positive number")
    return errors


def build_generated_record(
    normalized: NormalizedInput, config: dict, used_slugs: set[str]
) -> tuple[dict[str, object], list[str]]:
    warnings: list[str] = []
    specs = parse_specs(normalized)
    price_value = parse_int(normalized.price) or 0
    brand = infer_brand(normalized.name, normalized.raw_row, config)
    category = infer_category(normalized.name, normalized.key_specs, normalized.raw_row, specs, config)
    if not brand:
        warnings.append("brand fallback could not be confidently inferred")
    if not category:
        warnings.append("category fallback could not be confidently inferred")

    condition = config.get("condition_labels", {}).get("default", "Refurbished")
    warranty = config.get("default_warranty_text", "")
    stock_status = infer_stock_status(normalized.raw_row, config)
    model_name = prettify_product_name(normalized.name)
    short_specs = build_short_specs(specs)
    title = build_title(model_name, category, specs)
    focus_keyword = build_focus_keyword(model_name)
    search_keywords = build_search_keywords(
        model_name=model_name,
        brand=brand,
        category=category,
        focus_keyword=focus_keyword,
        specs=specs,
        config=config,
    )
    slug_source = " ".join(
        value
        for value in [
            model_name,
            category if category and category.lower() not in model_name.lower() else "",
            specs.cpu,
            specs.ram,
            specs.storage,
        ]
        if clean_text(value)
    )
    slug = unique_slug(slugify(slug_source), used_slugs)
    compare_at = build_compare_at_price(price_value, config)
    short_description = build_short_description(
        model_name=model_name,
        category=category,
        specs=specs,
        condition=condition,
        config=config,
    )
    meta_title = build_meta_title(model_name, specs, config)
    meta_description = build_meta_description(
        model_name=model_name,
        specs=specs,
        price_value=price_value,
        condition=condition,
        warranty=warranty,
        config=config,
    )
    description_html = build_description_html(
        model_name=model_name,
        category=category,
        specs=specs,
        condition=condition,
        warranty=warranty,
        price_value=price_value,
        raw_row=normalized.raw_row,
        config=config,
    )

    record: dict[str, object] = {
        "title": title,
        "slug": slug,
        "category": category or "Electronics",
        "brand": brand or "Generic",
        "price_kes": price_value,
        "compare_at_price": compare_at,
        "short_specs": short_specs,
        "short_description": short_description,
        "description_html": description_html,
        "meta_title": meta_title,
        "meta_description": meta_description,
        "focus_keyword": focus_keyword,
        "search_keywords": search_keywords,
        "condition": condition,
        "warranty": warranty,
        "stock_status": stock_status,
    }
    return record, warnings


def parse_specs(normalized: NormalizedInput) -> ParsedSpecs:
    lower_row = {normalize_header(key): clean_text(value) for key, value in normalized.raw_row.items()}
    specs = ParsedSpecs(
        cpu=clean_text(lower_row.get("cpu", "")),
        ram=clean_text(lower_row.get("ram", "")),
        storage=clean_text(lower_row.get("storage", "")),
    )
    notes_fragments = split_fragments(lower_row.get("notes", ""))
    loose_capacities: list[str] = []

    for fragment in split_fragments(normalized.key_specs):
        lowered = fragment.lower()
        if lowered.startswith("cpu:"):
            candidate = clean_text(fragment.split(":", 1)[1])
            if candidate and not specs.cpu:
                specs.cpu = candidate
            continue
        if lowered.startswith("ram:"):
            candidate = clean_text(fragment.split(":", 1)[1])
            if candidate and not specs.ram:
                specs.ram = candidate
            continue
        if lowered.startswith("storage:"):
            candidate = clean_text(fragment.split(":", 1)[1])
            if candidate and not specs.storage:
                specs.storage = candidate
            continue
        if CPU_PATTERN.search(lowered):
            candidate = clean_text(fragment)
            if not specs.cpu:
                specs.cpu = candidate
            continue
        if GEN_PATTERN.search(lowered):
            if not specs.generation:
                specs.generation = clean_text(fragment)
            continue
        if GRAPHICS_PATTERN.search(lowered):
            if not specs.graphics:
                specs.graphics = clean_text(fragment)
            continue
        if TOUCH_PATTERN.search(lowered):
            if not specs.touch:
                specs.touch = clean_text(fragment)
            continue
        if CAPACITY_PATTERN.match(lowered):
            capacity_value = clean_text(fragment)
            if capacity_value not in {specs.ram, specs.storage}:
                loose_capacities.append(capacity_value)
            continue
        notes_fragments.append(fragment)

    if loose_capacities and not specs.ram:
        specs.ram = loose_capacities.pop(0)
    if loose_capacities and not specs.storage:
        specs.storage = loose_capacities.pop(0)
    notes_fragments.extend(loose_capacities)

    remaining_notes: list[str] = []
    for fragment in dedupe_preserve_order(notes_fragments):
        lowered = fragment.lower()
        if not specs.generation and GEN_PATTERN.search(lowered):
            specs.generation = fragment
        elif not specs.graphics and GRAPHICS_PATTERN.search(lowered):
            specs.graphics = fragment
        elif not specs.touch and TOUCH_PATTERN.search(lowered):
            specs.touch = fragment
        elif not is_structured_duplicate(fragment, specs):
            remaining_notes.append(fragment)
    specs.additional_notes = remaining_notes
    return specs


def is_structured_duplicate(fragment: str, specs: ParsedSpecs) -> bool:
    lowered = clean_text(fragment).lower()
    comparisons = {
        clean_text(specs.cpu).lower(),
        clean_text(specs.ram).lower(),
        clean_text(specs.storage).lower(),
        clean_text(specs.generation).lower(),
        clean_text(specs.graphics).lower(),
        clean_text(specs.touch).lower(),
    }
    comparisons.update(
        {
            f"cpu: {clean_text(specs.cpu).lower()}" if specs.cpu else "",
            f"ram: {clean_text(specs.ram).lower()}" if specs.ram else "",
            f"storage: {clean_text(specs.storage).lower()}" if specs.storage else "",
        }
    )
    return lowered in comparisons


def infer_brand(name: str, raw_row: dict[str, str], config: dict) -> str:
    search_text = " ".join(
        [
            clean_text(name),
            clean_text(raw_row.get("Series", "")),
            clean_text(raw_row.get("series", "")),
        ]
    ).lower()
    for brand in config.get("known_brands", []):
        if re.search(rf"\b{re.escape(brand.lower())}\b", search_text):
            return BRAND_CASE_MAP.get(brand.lower(), brand.title())

    for source in [name, raw_row.get("Series", ""), raw_row.get("series", "")]:
        token = clean_text(source).split(" ")[0] if clean_text(source) else ""
        if token and re.fullmatch(r"[A-Za-z]{2,}", token):
            lowered = token.lower()
            if lowered not in {"intel", "core", "refurbished"}:
                return BRAND_CASE_MAP.get(lowered, token.upper() if len(token) <= 3 else token.title())
    return ""


def infer_category(
    name: str, key_specs: str, raw_row: dict[str, str], specs: ParsedSpecs, config: dict
) -> str:
    search_text = " ".join(
        [
            clean_text(name),
            clean_text(key_specs),
            clean_text(raw_row.get("Series", "")),
            clean_text(raw_row.get("Notes", "")),
            clean_text(raw_row.get("notes", "")),
        ]
    ).lower()
    for category, keywords in config.get("category_keywords", {}).items():
        for keyword in keywords:
            if keyword.lower() in search_text:
                return category
    if specs.cpu or specs.ram or specs.storage:
        return "Laptop"
    return "Electronics"


def infer_stock_status(raw_row: dict[str, str], config: dict) -> str:
    stock_labels = config.get("stock_labels", {})
    stock_value = parse_int(raw_row.get("Stock") or raw_row.get("stock"))
    if stock_value is None:
        return stock_labels.get("default_if_missing", "in_stock")
    if stock_value <= 0:
        return stock_labels.get("out_of_stock", "out_of_stock")
    if stock_value <= int(config.get("low_stock_threshold", 3)):
        return stock_labels.get("low_stock", "low_stock")
    return stock_labels.get("in_stock", "in_stock")


def prettify_product_name(name: str) -> str:
    tokens: list[str] = []
    for token in clean_text(name).split():
        raw = re.sub(r"[^\w+-]", "", token)
        lowered = raw.lower()
        if lowered in BRAND_CASE_MAP:
            tokens.append(BRAND_CASE_MAP[lowered])
        elif lowered in MODEL_CASE_MAP:
            tokens.append(MODEL_CASE_MAP[lowered])
        elif raw.isupper() and raw.isalpha() and len(raw) > 4:
            tokens.append(raw.title())
        else:
            tokens.append(token)
    return " ".join(tokens)


def display_cpu(cpu: str) -> str:
    value = clean_text(cpu)
    lowered = value.lower()
    if re.fullmatch(r"i[3579]", lowered):
        return f"Intel Core {lowered}"
    if lowered.startswith("core i"):
        return f"Intel {value.title()}"
    return value


def display_ram(ram: str) -> str:
    value = clean_text(ram)
    if value and "ram" not in value.lower():
        return f"{value} RAM"
    return value


def display_storage(storage: str) -> str:
    value = clean_text(storage)
    if value and not any(keyword in value.lower() for keyword in ["ssd", "hdd", "nvme", "emmc", "storage"]):
        return f"{value} Storage"
    return value


def build_short_specs(specs: ParsedSpecs) -> str:
    parts = dedupe_preserve_order(
        [
            display_cpu(specs.cpu),
            display_ram(specs.ram),
            display_storage(specs.storage),
            specs.generation,
            specs.graphics,
            specs.touch,
            *specs.additional_notes,
        ]
    )
    return "; ".join(parts)


def build_title(model_name: str, category: str, specs: ParsedSpecs) -> str:
    title = model_name
    category_suffix = category if category not in ACCESSORY_HINTS else ""
    if category_suffix and category_suffix.lower() not in model_name.lower():
        title = f"{title} {category_suffix}"
    summary_specs = [
        display_cpu(specs.cpu),
        display_ram(specs.ram),
        display_storage(specs.storage),
    ]
    summary_specs = [value for value in summary_specs if value]
    if summary_specs:
        title = f"{title} - {', '.join(summary_specs[:3])}"
    return clean_text(title)


def build_focus_keyword(model_name: str) -> str:
    return smart_trim(f"{model_name} price in Kenya", 80)


def build_search_keywords(
    model_name: str,
    brand: str,
    category: str,
    focus_keyword: str,
    specs: ParsedSpecs,
    config: dict,
) -> str:
    location = clean_text(config.get("location", "Kenya"))
    compact_specs = " ".join(
        value for value in [clean_text(specs.ram), clean_text(specs.storage)] if value
    )
    keywords = dedupe_preserve_order(
        [
            focus_keyword,
            model_name,
            f"{model_name} Nairobi",
            f"{model_name} {compact_specs}".strip(),
            f"{brand} {category} Kenya".strip(),
            f"refurbished {brand} {category} Kenya".strip(),
            f"{model_name} {location}",
        ]
    )
    return ", ".join(keywords)


def build_compare_at_price(price_value: int, config: dict) -> int:
    markup = float(config.get("compare_at_markup_pct", 0.0))
    increment = int(config.get("compare_at_round_to", 100))
    compare_at = round_up(price_value * (1 + markup), increment)
    return compare_at if compare_at >= price_value else price_value


def build_short_description(
    model_name: str, category: str, specs: ParsedSpecs, condition: str, config: dict
) -> str:
    spec_phrase = human_join(
        [
            display_cpu(specs.cpu),
            display_ram(specs.ram),
            display_storage(specs.storage),
            specs.generation,
            specs.graphics,
            specs.touch,
        ]
    )
    base = f"{condition} {model_name}"
    if category and category.lower() not in model_name.lower():
        base = f"{base} {category.lower()}"
    if spec_phrase:
        base = f"{base} with {spec_phrase}"
    base = f"{base}. Available from {config.get('store_name', 'SES ICT HUB')} in {config.get('location', 'Kenya')}."
    return clean_text(base)


def build_meta_title(model_name: str, specs: ParsedSpecs, config: dict) -> str:
    limit = int(config.get("meta_limits", {}).get("title", 60))
    store_name = clean_text(config.get("store_name", "SES ICT HUB"))
    candidates = [
        f"{model_name} {clean_text(specs.ram)} {clean_text(specs.storage)} Price in Kenya | {store_name}".replace("  ", " "),
        f"{model_name} Price in Kenya | {store_name}",
        f"{model_name} | {store_name}",
    ]
    for candidate in candidates:
        candidate = clean_text(candidate)
        if len(candidate) <= limit:
            return candidate
    return smart_trim(candidates[0], limit)


def build_meta_description(
    model_name: str,
    specs: ParsedSpecs,
    price_value: int,
    condition: str,
    warranty: str,
    config: dict,
) -> str:
    limit = int(config.get("meta_limits", {}).get("description", 160))
    spec_phrase = human_join(
        [
            display_cpu(specs.cpu),
            display_ram(specs.ram),
            display_storage(specs.storage),
            specs.generation,
        ]
    )
    description = (
        f"Buy {model_name} with {spec_phrase} at {format_kes(price_value)} from "
        f"{config.get('store_name', 'SES ICT HUB')} in {config.get('location', 'Kenya')}. "
        f"{condition}. {warranty}"
    )
    return smart_trim(description, limit)


def build_description_html(
    model_name: str,
    category: str,
    specs: ParsedSpecs,
    condition: str,
    warranty: str,
    price_value: int,
    raw_row: dict[str, str],
    config: dict,
) -> str:
    intro_specs = human_join(
        [
            display_cpu(specs.cpu),
            display_ram(specs.ram),
            display_storage(specs.storage),
            specs.generation,
        ]
    )
    intro = (
        f"{model_name} is a {condition.lower()} {category.lower()} available from "
        f"{config.get('store_name', 'SES ICT HUB')} in {config.get('location', 'Kenya')}."
    )
    if intro_specs:
        intro = f"{intro} This unit is listed with {intro_specs} at {format_kes(price_value)}."

    features = dedupe_preserve_order(
        [
            f"Processor: {display_cpu(specs.cpu)}" if specs.cpu else "",
            f"Memory: {display_ram(specs.ram)}" if specs.ram else "",
            f"Storage: {display_storage(specs.storage)}" if specs.storage else "",
            f"Platform note: {specs.generation}" if specs.generation else "",
            f"Graphics note: {specs.graphics}" if specs.graphics else "",
            f"Input note: {specs.touch}" if specs.touch else "",
            *(f"Additional note: {note}" for note in specs.additional_notes),
        ]
    )
    bullet_html = "\n".join(f"<li>{escape(item)}</li>" for item in features)
    ideal_use = build_ideal_use_text(category, specs)
    condition_text = (
        f"Condition: {condition}. Warranty: {warranty} Please confirm the exact unit details, included accessories, "
        f"and cosmetic state before purchase."
    )
    stock_note = infer_stock_status(raw_row, config)
    if stock_note == config.get("stock_labels", {}).get("low_stock", "low_stock"):
        condition_text += " Stock is currently limited."
    return (
        f"<p>{escape(intro)}</p>\n"
        f"<ul>\n{bullet_html}\n</ul>\n"
        f"<h3>Ideal Use</h3>\n"
        f"<p>{escape(ideal_use)}</p>\n"
        f"<h3>Condition &amp; Warranty</h3>\n"
        f"<p>{escape(condition_text)}</p>"
    )


def build_ideal_use_text(category: str, specs: ParsedSpecs) -> str:
    if category == "Workstation Laptop":
        return (
            "Ideal for office workloads, coding, advanced spreadsheets, design-related tasks, and other professional "
            "workflows that benefit from a mobile workstation layout."
        )
    if category == "2-in-1 Laptop":
        return (
            "Ideal for presentations, note-taking, office work, online meetings, and day-to-day productivity where "
            "touch input is useful."
        )
    ram_value = parse_int(specs.ram)
    if ram_value is not None and ram_value >= 16:
        return (
            "Ideal for business use, multitasking, research, remote work, coding, and daily productivity tasks."
        )
    return "Ideal for everyday office work, browsing, email, online classes, and general productivity."


def validate_output_record(record: dict[str, object], config: dict) -> list[str]:
    errors: list[str] = []
    for field in REQUIRED_OUTPUT_FIELDS:
        if not clean_text(record.get(field, "")):
            errors.append(f"{field} cannot be blank")

    slug = clean_text(record.get("slug", ""))
    if slug != slug.lower():
        errors.append("slug must be lowercase")

    meta_title = clean_text(record.get("meta_title", ""))
    meta_description = clean_text(record.get("meta_description", ""))
    title_limit = int(config.get("meta_limits", {}).get("title", 60))
    description_limit = int(config.get("meta_limits", {}).get("description", 160))
    if len(meta_title) > title_limit:
        errors.append("meta_title exceeds the configured limit")
    if len(meta_description) > description_limit:
        errors.append("meta_description exceeds the configured limit")
    return errors


def refine_with_llm(
    generated_record: dict[str, object],
    normalized: NormalizedInput,
    config: dict,
    prompts: dict[str, str],
    llm_refiner,
) -> tuple[dict[str, object], str, str]:
    try:
        system_prompt = render_template(
            prompts["system_prompt.txt"],
            {
                "meta_title_limit": config.get("meta_limits", {}).get("title", 60),
                "meta_description_limit": config.get("meta_limits", {}).get("description", 160),
            },
        )
        user_prompt = render_template(
            prompts["user_prompt.txt"],
            {
                "store_name": config.get("store_name", ""),
                "location": config.get("location", ""),
                "currency": config.get("currency", ""),
                "default_condition": config.get("condition_labels", {}).get("default", ""),
                "default_warranty": config.get("default_warranty_text", ""),
                "deterministic_payload_json": dump_json(
                    {
                        "input_name": normalized.name,
                        "input_key_specs": normalized.key_specs,
                        "input_price": normalized.price,
                        "generated_record": generated_record,
                    }
                ),
            },
        )
        refined = llm_refiner.refine(system_prompt=system_prompt, user_prompt=user_prompt)
    except Exception as exc:  # noqa: BLE001
        return generated_record, "failed", f"llm_refinement_failed: {exc}"

    updated_record = dict(generated_record)
    for field in [
        "title",
        "short_description",
        "description_html",
        "meta_title",
        "meta_description",
        "focus_keyword",
        "search_keywords",
    ]:
        if field not in refined:
            continue
        if field == "description_html":
            candidate = str(refined[field]).strip()
        elif field == "search_keywords" and isinstance(refined[field], list):
            candidate = ", ".join(clean_text(value) for value in refined[field] if clean_text(value))
        else:
            candidate = clean_text(refined[field])
        if candidate:
            updated_record[field] = candidate

    updated_record["meta_title"] = smart_trim(
        str(updated_record["meta_title"]), int(config.get("meta_limits", {}).get("title", 60))
    )
    updated_record["meta_description"] = smart_trim(
        str(updated_record["meta_description"]),
        int(config.get("meta_limits", {}).get("description", 160)),
    )
    return updated_record, "applied", ""


def build_rejection_record(
    raw_row: dict[str, str],
    source_kind: str,
    source_sheet: str | None,
    reason: str,
    normalized: NormalizedInput | None = None,
) -> dict[str, str]:
    return {
        "source_row_number": str((normalized.source_row_number if normalized else raw_row.get("__row_number__", ""))),
        "source_kind": source_kind,
        "source_sheet": source_sheet or "",
        "name": normalized.name if normalized else clean_text(raw_row.get("name") or raw_row.get("Model")),
        "key_specs": normalized.key_specs if normalized else clean_text(raw_row.get("key_specs") or raw_row.get("Notes")),
        "price": normalized.price if normalized else clean_text(raw_row.get("price") or raw_row.get("Price_KES")),
        "rejection_reason": clean_text(reason),
        "raw_payload_json": json.dumps({key: value for key, value in raw_row.items() if key != "__row_number__"}, ensure_ascii=False),
    }


def write_cleaned_csv(output_path: Path, rows: list[dict[str, object]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=OUTPUT_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in OUTPUT_FIELDS})


def write_rejected_csv(output_path: Path, rows: list[dict[str, str]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=REJECTION_FIELDS)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in REJECTION_FIELDS})


def write_log_json(output_path: Path, payload: dict[str, object]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def timestamp_now() -> str:
    return datetime.now(timezone.utc).isoformat()
