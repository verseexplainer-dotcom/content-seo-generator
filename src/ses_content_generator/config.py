from __future__ import annotations

import json
from pathlib import Path


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_path(path: str | Path) -> Path:
    candidate = Path(path).expanduser()
    if candidate.is_absolute():
        return candidate
    return (project_root() / candidate).resolve()


def load_store_config(path: str | Path) -> dict:
    config_path = resolve_path(path)
    with config_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def load_prompt_templates(path: str | Path) -> dict[str, str]:
    prompts_path = resolve_path(path)
    templates: dict[str, str] = {}
    for template_name in ["system_prompt.txt", "user_prompt.txt"]:
        template_path = prompts_path / template_name
        templates[template_name] = template_path.read_text(encoding="utf-8")
    return templates


def render_template(template: str, values: dict[str, object]) -> str:
    rendered = template
    for key, value in values.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", str(value))
    return rendered

