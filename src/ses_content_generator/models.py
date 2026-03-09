from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class NormalizedInput:
    source_row_number: int
    source_kind: str
    source_sheet: str | None
    raw_row: dict[str, str]
    name: str
    key_specs: str
    price: str


@dataclass
class ParsedSpecs:
    cpu: str = ""
    ram: str = ""
    storage: str = ""
    generation: str = ""
    graphics: str = ""
    touch: str = ""
    additional_notes: list[str] = field(default_factory=list)

    def known_values(self) -> list[str]:
        return [
            value
            for value in [
                self.cpu,
                self.ram,
                self.storage,
                self.generation,
                self.graphics,
                self.touch,
                *self.additional_notes,
            ]
            if value
        ]


@dataclass
class RunSummary:
    rows_read: int
    cleaned_rows: int
    rejected_rows: int
    used_llm: bool
    output_file: str
    rejected_file: str
    log_file: str

