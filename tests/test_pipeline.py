from __future__ import annotations

import csv
import json
import tempfile
import unittest
from pathlib import Path

from src.ses_content_generator.config import load_prompt_templates, load_store_config, project_root
from src.ses_content_generator.pipeline import process_input_file


class PipelineTests(unittest.TestCase):
    def test_processes_csv_and_rejects_invalid_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            input_path = tmp_path / "input.csv"
            output_path = tmp_path / "cleaned_output.csv"
            rejected_path = tmp_path / "rejected_rows.csv"
            log_path = tmp_path / "generation_log.json"

            input_path.write_text(
                "\n".join(
                    [
                        "name,key_specs,price",
                        "HP ZBOOK 15 G8,CPU: i7 | RAM: 16GB | Storage: 512GB | 11th Gen | 4GB Graphics,66500",
                        "HP ZBOOK 15 G8,CPU: i7 | RAM: 32GB | Storage: 512GB | 10th Gen,69500",
                        "Broken Row,,12000",
                    ]
                ),
                encoding="utf-8",
            )

            summary = process_input_file(
                input_path=input_path,
                output_path=output_path,
                rejected_path=rejected_path,
                log_path=log_path,
                config=load_store_config(project_root() / "config/store_defaults.json"),
                prompts=load_prompt_templates(project_root() / "prompts"),
                llm_refiner=None,
            )

            self.assertEqual(summary.rows_read, 3)
            self.assertEqual(summary.cleaned_rows, 2)
            self.assertEqual(summary.rejected_rows, 1)

            with output_path.open("r", encoding="utf-8", newline="") as handle:
                cleaned = list(csv.DictReader(handle))
            self.assertEqual(len(cleaned), 2)
            self.assertNotEqual(cleaned[0]["slug"], cleaned[1]["slug"])
            self.assertEqual(cleaned[0]["brand"], "HP")
            self.assertIn("Price in Kenya", cleaned[0]["meta_title"])

            with rejected_path.open("r", encoding="utf-8", newline="") as handle:
                rejected = list(csv.DictReader(handle))
            self.assertEqual(len(rejected), 1)
            self.assertIn("key_specs is required", rejected[0]["rejection_reason"])

            log_payload = json.loads(log_path.read_text(encoding="utf-8"))
            self.assertEqual(log_payload["summary"]["cleaned_rows"], 2)
            self.assertEqual(log_payload["summary"]["rejected_rows"], 1)


if __name__ == "__main__":
    unittest.main()
