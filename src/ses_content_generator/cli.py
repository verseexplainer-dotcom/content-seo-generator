from __future__ import annotations

import argparse
import os
from pathlib import Path

from src.ses_content_generator.config import load_prompt_templates, load_store_config, project_root
from src.ses_content_generator.openai_client import OpenAIRefiner
from src.ses_content_generator.pipeline import process_input_file
from src.ses_content_generator.utils import load_dotenv


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate SEO-friendly ecommerce content from a CSV or XLSX sheet.")
    parser.add_argument("--input", required=True, help="Path to the source CSV or XLSX file.")
    parser.add_argument("--output", required=True, help="Path for cleaned_output.csv.")
    parser.add_argument(
        "--rejected-output",
        help="Optional path for rejected_rows.csv. Defaults to the output directory.",
    )
    parser.add_argument(
        "--log-output",
        help="Optional path for generation_log.json. Defaults to the output directory.",
    )
    parser.add_argument("--config", default="config/store_defaults.json", help="Store config path.")
    parser.add_argument("--prompts-dir", default="prompts", help="Prompt template directory.")
    parser.add_argument("--sheet", help="Excel sheet name to read. Defaults to the first sheet.")
    parser.add_argument(
        "--use-llm",
        action="store_true",
        help="Run an optional OpenAI refinement pass after deterministic generation.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    load_dotenv(project_root() / ".env")

    input_path = Path(args.input).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()
    rejected_path = (
        Path(args.rejected_output).expanduser().resolve()
        if args.rejected_output
        else output_path.with_name("rejected_rows.csv")
    )
    log_path = (
        Path(args.log_output).expanduser().resolve()
        if args.log_output
        else output_path.with_name("generation_log.json")
    )

    config = load_store_config(args.config)
    prompts = load_prompt_templates(args.prompts_dir)

    llm_refiner = None
    if args.use_llm:
        api_key = os.getenv("OPENAI_API_KEY", "").strip()
        model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
        base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").strip()
        if api_key:
            llm_refiner = OpenAIRefiner(api_key=api_key, model=model, base_url=base_url)
        else:
            print("LLM refinement requested, but OPENAI_API_KEY is not set. Continuing with deterministic generation.")

    summary = process_input_file(
        input_path=input_path,
        output_path=output_path,
        rejected_path=rejected_path,
        log_path=log_path,
        config=config,
        prompts=prompts,
        llm_refiner=llm_refiner,
        sheet_name=args.sheet,
    )

    print(f"Rows read: {summary.rows_read}")
    print(f"Cleaned rows: {summary.cleaned_rows}")
    print(f"Rejected rows: {summary.rejected_rows}")
    print(f"LLM used: {'yes' if summary.used_llm else 'no'}")
    print(f"Clean output: {summary.output_file}")
    print(f"Rejected rows: {summary.rejected_file}")
    print(f"Generation log: {summary.log_file}")
    return 0
