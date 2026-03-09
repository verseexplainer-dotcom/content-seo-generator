# SES ICT HUB Content Automation

This project turns a raw Excel or CSV product sheet into SEO-friendly ecommerce content for review or import.

It was built for SES ICT HUB with a deterministic-first workflow:
- parse the source sheet
- normalize product fields
- infer brand and category conservatively
- generate SEO content without inventing missing specs
- validate outputs
- save clean rows, rejected rows, and a JSON generation log

## What the tool accepts

The CLI supports both:
- simple input files with `name`, `key_specs`, and `price`
- workbook-style sheets like the current SES ICT HUB laptop list with `Series`, `Model`, `CPU`, `RAM`, `Storage`, `Notes`, `Price_KES`, and `Stock`

Supported file types:
- `.xlsx`
- `.csv`

## Project layout

- `src/` Python source code
- `config/store_defaults.json` store-wide defaults
- `prompts/` editable prompt templates for optional LLM refinement
- `data/input/` place your working files here
- `data/output/` generated outputs
- `samples/` sample input and output files

## Setup

1. Create a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

No third-party packages are required for the default deterministic pipeline.

3. Optional: enable OpenAI refinement:

```bash
cp .env.example .env
```

Then add your API key to `.env`.

## Run

Sample CSV run:

```bash
python3 -m src.main \
  --input data/input/raw_products.csv \
  --output data/output/cleaned_output.csv
```

Current SES ICT HUB workbook run:

```bash
python3 -m src.main \
  --input "ses_ict_hub_full_laptop_price_list.xlsx" \
  --output data/output/cleaned_output.csv
```

Optional LLM refinement after deterministic generation:

```bash
python3 -m src.main \
  --input "ses_ict_hub_full_laptop_price_list.xlsx" \
  --output data/output/cleaned_output.csv \
  --use-llm
```

## Output files

Each run writes:
- `cleaned_output.csv`
- `rejected_rows.csv`
- `generation_log.json`

The output CSV includes:
- `title`
- `slug`
- `category`
- `brand`
- `price_kes`
- `compare_at_price`
- `short_specs`
- `short_description`
- `description_html`
- `meta_title`
- `meta_description`
- `focus_keyword`
- `search_keywords`
- `condition`
- `warranty`
- `stock_status`

## Writing and validation rules

- Required fields cannot be blank.
- Slugs are lowercase and auto-deduplicated.
- Meta title and meta description are trimmed to the configured limits.
- Technical specs are never invented.
- Brand and category inference stay conservative.
- If rows cannot be normalized or validated, they go to `rejected_rows.csv`.

## Store defaults

Edit [store_defaults.json](/home/paulaflare/Desktop/ses%20content%20generator/config/store_defaults.json) to change:
- store name
- location
- currency
- warranty text
- condition labels
- stock labels
- compare-at pricing rule
- known brands and category keyword maps

## Prompt templates

Edit these files to tune the optional LLM layer:
- [system_prompt.txt](/home/paulaflare/Desktop/ses%20content%20generator/prompts/system_prompt.txt)
- [user_prompt.txt](/home/paulaflare/Desktop/ses%20content%20generator/prompts/user_prompt.txt)

If `--use-llm` is not set, the tool still works fully with deterministic generation.

## Future Supabase connection

The code keeps the generated record structure isolated so it can later be mapped into Supabase inserts without changing the parser or validation layers.

## Notes

- Review output before bulk ecommerce import.
- The current workbook in the project root is already supported.
- You can drop future demo or live files into `data/input/` and point the CLI at them.

