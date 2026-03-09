# SES ICT HUB Content Automation

This tool converts a raw CSV with:
- `name`
- `key_specs`
- `price`

into SEO-friendly ecommerce product content ready for import or review.

## Features

- CSV parsing
- brand/category inference
- slug generation
- SEO title and meta generation
- HTML product description generation
- validation and rejection logging
- output CSV export

## Setup

1. Create and activate a virtual environment:
   `python3 -m venv .venv`
   `source .venv/bin/activate`
2. Install dependencies:
   `pip install -r requirements.txt`
3. Copy environment file:
   `cp .env.example .env`
4. Add your OpenAI API key to `.env`

## Run

`python -m src.main --input data/input/raw_products.csv --output data/output/cleaned_output.csv`

## Input CSV

Required columns:
- `name`
- `key_specs`
- `price`

## Output files

- `cleaned_output.csv`
- `rejected_rows.csv`
- `generation_log.json`

## Notes

- This tool does not invent missing product specs.
- Review output before bulk import.
- Adjust prompts and config files for your store style.
