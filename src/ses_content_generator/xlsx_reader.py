from __future__ import annotations

import posixpath
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

from src.ses_content_generator.utils import clean_text

MAIN_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
REL_ATTR = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"
PKG_REL_NS = "{http://schemas.openxmlformats.org/package/2006/relationships}"


def read_xlsx(path: str | Path, sheet_name: str | None = None) -> tuple[list[dict[str, str]], str]:
    workbook_path = Path(path)
    with zipfile.ZipFile(workbook_path) as archive:
        shared_strings = _read_shared_strings(archive)
        sheets = _read_sheet_targets(archive)
        if not sheets:
            return [], ""
        selected_name, target = _select_sheet(sheets, sheet_name)
        rows = _read_sheet_rows(archive, target, shared_strings)
        return rows, selected_name


def _read_shared_strings(archive: zipfile.ZipFile) -> list[str]:
    if "xl/sharedStrings.xml" not in archive.namelist():
        return []
    root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
    values: list[str] = []
    for node in root.findall("a:si", MAIN_NS):
        text = "".join(part.text or "" for part in node.iterfind(".//a:t", MAIN_NS))
        values.append(clean_text(text))
    return values


def _read_sheet_targets(archive: zipfile.ZipFile) -> list[tuple[str, str]]:
    workbook_root = ET.fromstring(archive.read("xl/workbook.xml"))
    rels_root = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
    relationships = {
        rel.attrib["Id"]: rel.attrib["Target"]
        for rel in rels_root.findall(f"{PKG_REL_NS}Relationship")
    }
    sheets: list[tuple[str, str]] = []
    for sheet in workbook_root.findall("a:sheets/a:sheet", MAIN_NS):
        name = sheet.attrib.get("name", "Sheet1")
        rel_id = sheet.attrib.get(REL_ATTR, "")
        target = relationships.get(rel_id, "")
        if not target:
            continue
        full_target = posixpath.normpath(posixpath.join("xl", target))
        sheets.append((name, full_target))
    return sheets


def _select_sheet(sheets: list[tuple[str, str]], requested_name: str | None) -> tuple[str, str]:
    if not requested_name:
        return sheets[0]
    requested = clean_text(requested_name).lower()
    for name, target in sheets:
        if clean_text(name).lower() == requested:
            return name, target
    available = ", ".join(name for name, _ in sheets)
    raise ValueError(f"Sheet '{requested_name}' not found. Available sheets: {available}")


def _read_sheet_rows(
    archive: zipfile.ZipFile, target: str, shared_strings: list[str]
) -> list[dict[str, str]]:
    root = ET.fromstring(archive.read(target))
    parsed_rows: list[tuple[int, dict[str, str]]] = []
    for row in root.findall(".//a:sheetData/a:row", MAIN_NS):
        row_number = int(row.attrib.get("r", len(parsed_rows) + 1))
        cells: dict[str, str] = {}
        for cell in row.findall("a:c", MAIN_NS):
            ref = cell.attrib.get("r", "")
            column = _column_letters(ref)
            cells[column] = _cell_value(cell, shared_strings)
        if any(clean_text(value) for value in cells.values()):
            parsed_rows.append((row_number, cells))

    if not parsed_rows:
        return []

    header_row_number, header_cells = parsed_rows[0]
    del header_row_number
    ordered_columns = sorted(header_cells, key=_column_sort_key)
    headers = _dedupe_headers([header_cells.get(column, "") for column in ordered_columns])

    output_rows: list[dict[str, str]] = []
    for row_number, row_cells in parsed_rows[1:]:
        record: dict[str, str] = {"__row_number__": str(row_number)}
        for index, column in enumerate(ordered_columns):
            record[headers[index]] = clean_text(row_cells.get(column, ""))
        output_rows.append(record)
    return output_rows


def _cell_value(cell: ET.Element, shared_strings: list[str]) -> str:
    cell_type = cell.attrib.get("t")
    value_node = cell.find("a:v", MAIN_NS)
    if cell_type == "s":
        if value_node is None or value_node.text is None:
            return ""
        index = int(value_node.text)
        return shared_strings[index] if index < len(shared_strings) else ""
    if cell_type == "inlineStr":
        return clean_text("".join(node.text or "" for node in cell.iterfind(".//a:t", MAIN_NS)))
    if cell_type == "b":
        return "TRUE" if value_node is not None and value_node.text == "1" else "FALSE"
    if value_node is None or value_node.text is None:
        return ""
    return clean_text(value_node.text)


def _column_letters(reference: str) -> str:
    letters = "".join(character for character in reference if character.isalpha())
    return letters or "A"


def _column_sort_key(column: str) -> int:
    total = 0
    for character in column.upper():
        total = total * 26 + (ord(character) - ord("A") + 1)
    return total


def _dedupe_headers(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    deduped: list[str] = []
    for index, header in enumerate(headers, start=1):
        base = clean_text(header) or f"column_{index}"
        count = seen.get(base, 0) + 1
        seen[base] = count
        deduped.append(base if count == 1 else f"{base}_{count}")
    return deduped
