# Pipeline — how to run it

Aggregates products from the three mock APIs into one normalized catalogue plus a run
report. Design notes are in [SPEC.md](../SPEC.md) and [PLAN.md](../PLAN.md).

## Setup

Requires [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

## Run

Start the mock in one terminal:

```bash
uv run python server.py --port 8080
```

Run the pipeline in another:

```bash
uv run pipeline run                    # report to stdout, logs to stderr
uv run pipeline run --output run.json  # report to a file
```

`--output` is the only flag; everything else is configuration:

`PIPELINE_BASE_URL` (default `http://localhost:8080`), `PIPELINE_CURRENCY`,
`PIPELINE_LOG_FORMAT` (`console`/`json`), `PIPELINE_LOG_LEVEL`,
`PIPELINE_CONNECT_TIMEOUT`, `PIPELINE_READ_TIMEOUT`, `PIPELINE_MAX_PAGES_PER_SOURCE`.

```bash
PIPELINE_BASE_URL=http://localhost:8081 uv run pipeline run
PIPELINE_LOG_FORMAT=json uv run pipeline run | jq '.status, .product_count'
```

## Test

```bash
uv run pytest        # boots the real mock on free ports; do not run in parallel
uv run ruff check .
pre-commit install   # ruff --fix on commit
```

## Expected output

`status` is `partial_success` on a healthy run, not `success`: `b-205` in `fixtures.json`
has the price `"not-a-number"`, so every run drops exactly one record. Exit code is 0
unless *no* source returned anything.

```json
{
  "run_id": "d4caad1dde4a",
  "status": "partial_success",
  "product_count": 12,
  "products": [
    {
      "id": "endpoint_a:a-101",
      "title": "Mechanical Keyboard",
      "source": "endpoint_a",
      "price": "89.99",
      "currency": "GBP",
      "category": "electronics",
      "fetched_at": "2026-09-02T20:46:19.969Z"
    }
  ],
  "sources": { "endpoint_b": { "status": "degraded", "records_dropped": 1 } },
  "warnings": [
    { "code": "malformed_record", "source": "endpoint_b", "record_id": "b-205" }
  ]
}
```

Phase 1 has no retry layer, so against the `standard` scenario source B stops at its first
503 and source C at its first 429 — both `degraded`, while source A returns all 6 products.
That isolation is the point; Phase 2 adds the retries.

## Other scenarios

```bash
MOCK_SCENARIO=source-b-down  uv run python server.py --port 8081   # B never recovers
MOCK_SCENARIO=bad-data-heavy uv run python server.py --port 8082   # more bad records
MOCK_SCENARIO=no-failures    uv run python server.py --port 8083   # no transient 5xx
```
