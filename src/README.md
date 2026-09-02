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
`PIPELINE_CONNECT_TIMEOUT`, `PIPELINE_READ_TIMEOUT`, `PIPELINE_RUN_TIMEOUT`,
`PIPELINE_RETRY_ATTEMPTS`, `PIPELINE_RETRY_BASE_DELAY`, `PIPELINE_RETRY_MAX_DELAY`,
`PIPELINE_MAX_RETRY_AFTER`, `PIPELINE_RETRY_BUDGET_PER_SOURCE`,
`PIPELINE_SOURCE_C_RATE`, `PIPELINE_MAX_PAGES_PER_SOURCE`.

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

A healthy run returns **17 products** with `status: partial_success` — not `success`, and
not 18. `b-205` in `fixtures.json` has the price `"not-a-number"`, so every run drops
exactly one record. Exit code is 0 unless *no* source returned anything.

Source B's three transient failures (one 503, two 502s) are retried away, and source C is
paced to stay inside its 2-per-second limit, so neither costs any records:

```
endpoint_a  ok        3 requests, 0 retries          p50  86ms
endpoint_b  degraded  6 requests, 3 retries          p50 128ms   (1 dropped: b-205)
endpoint_c  ok        3 requests, 0 rate-limit hits  p50  67ms

wall 1326ms vs 2621ms sequential — concurrency saved 1295ms
```

`metrics` in the report carries those totals; each source also reports p50/p95/max request
latency, measured around the HTTP call only so backoff and rate-limiter waits do not
inflate it.

```json
{
  "run_id": "d4caad1dde4a",
  "status": "partial_success",
  "product_count": 17,
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
  "sources": {
    "endpoint_b": {
      "status": "degraded",
      "records_dropped": 1,
      "requests": 6,
      "retries": 3,
      "rate_limited": 0
    }
  },
  "warnings": [
    { "code": "malformed_record", "source": "endpoint_b", "record_id": "b-205" }
  ]
}
```

A source that fails outright is isolated: it is recorded as `failed` with a warning while
every other source still reports. Try `MOCK_SCENARIO=source-b-down` below to see it.

## Other scenarios

```bash
MOCK_SCENARIO=source-b-down  uv run python server.py --port 8081   # B never recovers
MOCK_SCENARIO=bad-data-heavy uv run python server.py --port 8082   # more bad records
MOCK_SCENARIO=no-failures    uv run python server.py --port 8083   # no transient 5xx
```
