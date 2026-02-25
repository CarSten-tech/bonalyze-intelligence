# Bonalyze Intelligence

Enterprise scraper pipeline for weekly retailer offers:
- discovers runtime headers with Playwright (`Sentinel`)
- fetches curated offers from the Marktguru publisher API
- generates embeddings with Gemini
- upserts offers + embeddings into Supabase

## Requirements

- Python 3.11
- Playwright Chromium runtime
- Supabase project with required tables (`offers`, `product_embeddings_cache`, `retailer_configs`)
- Dependencies are version-pinned in `requirements.txt` for reproducible CI/runtime behavior.

## Environment

Use `.env` (never commit secrets):

- `SUPABASE_URL`
- `SUPABASE_KEY` (service role key recommended for reliable upsert/delete with RLS)
- `GEMINI_API_KEY`
- `GEMINI_API_VERSION` (default: `v1beta`)
- `GEMINI_EMBEDDING_MODEL` (default: `gemini-embedding-001`)
- `ALLOWED_STORES` (default: `kaufland,aldi-sued,edeka`)
- `FAIL_ON_PARTIAL_SYNC` (default: `true`)
- `MAX_FAILURE_RATE` (default: `0.35`)

See `.env.example` for a safe template.

## Local Run

```bash
python main.py
```

Dry run (no DB writes):

```bash
python main.py --dry-run
```

## Quality Gates

Run checks locally:

```bash
python -m py_compile config.py models.py main.py scraper.py data_sync.py embedder.py sentinel.py normalization.py run_policy.py runtime_utils.py
pytest -q
```

CI (`.github/workflows/sync.yml`) now executes:
1. dependency install
2. unit tests
3. Playwright install
4. scraper run

## Reliability Notes

- Embedding fallback handles model-404 scenarios and switches to `gemini-embedding-001` when available.
- Offer upsert payload enforces `product_slug` generation and fallback (`offer-<offer_id>`) to satisfy DB NOT NULL constraints.
- Scraper parser now falls back to `price` when `oldPrice` is missing.
- HTTP retries are now limited to transient conditions (`429/5xx`, timeout/connection errors) to avoid noisy retries on permanent data/schema errors.
- Run health policy fails CI if quality signals are broken (e.g. fetched data but upserted `0`, excessive failure rate, or failed retailers when partial success is not allowed).

## Runtime Flags

```bash
python main.py --allow-partial-success --max-failure-rate 0.50
```

Defaults are strict and optimized for production quality gates.

## Security Notes

- Never commit `.env` or service role keys.
- Use GitHub Actions Secrets for production runs.
- Keep Supabase RLS policies explicit; service role bypass should be limited to CI/runtime context only.
