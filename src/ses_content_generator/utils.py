from __future__ import annotations

import json
import math
import os
import re
import unicodedata
from pathlib import Path
from typing import Iterable


def clean_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).replace("\u00a0", " ")
    return re.sub(r"\s+", " ", text).strip()


def normalize_header(header: str) -> str:
    text = clean_text(header).lower()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def split_fragments(value: str) -> list[str]:
    return [fragment for fragment in (clean_text(part) for part in re.split(r"[|;/\n]+", value)) if fragment]


def dedupe_preserve_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        cleaned = clean_text(value)
        key = cleaned.lower()
        if not cleaned or key in seen:
            continue
        seen.add(key)
        result.append(cleaned)
    return result


def smart_trim(text: str, limit: int) -> str:
    value = clean_text(text)
    if len(value) <= limit:
        return value
    truncated = value[: limit + 1]
    if " " in truncated:
        truncated = truncated.rsplit(" ", 1)[0]
    truncated = truncated.rstrip(",;:- ")
    return truncated[:limit]


def slugify(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", clean_text(value))
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii")
    lowered = ascii_text.lower().replace("&", " and ")
    slug = re.sub(r"[^a-z0-9]+", "-", lowered)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug or "item"


def unique_slug(base_slug: str, used_slugs: set[str]) -> str:
    slug = clean_text(base_slug).lower()
    slug = slugify(slug)
    if slug not in used_slugs:
        used_slugs.add(slug)
        return slug
    counter = 2
    while True:
        candidate = f"{slug}-{counter}"
        if candidate not in used_slugs:
            used_slugs.add(candidate)
            return candidate
        counter += 1


def parse_int(value: object) -> int | None:
    text = clean_text(value)
    if not text:
        return None
    digits = re.sub(r"[^0-9.\-]", "", text)
    if not digits or digits in {"-", ".", "-."}:
        return None
    try:
        return int(round(float(digits)))
    except ValueError:
        return None


def format_kes(value: int | None) -> str:
    if value is None:
        return ""
    return f"KES {value:,}"


def round_up(value: float, increment: int) -> int:
    if increment <= 0:
        return int(round(value))
    return int(math.ceil(value / increment) * increment)


def human_join(parts: Iterable[str]) -> str:
    cleaned = [clean_text(part) for part in parts if clean_text(part)]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    return f"{', '.join(cleaned[:-1])}, and {cleaned[-1]}"


def load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, raw_value = stripped.split("=", 1)
        key = key.strip()
        value = raw_value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


def dump_json(data: object) -> str:
    return json.dumps(data, ensure_ascii=False, indent=2)

