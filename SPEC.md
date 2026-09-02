# SPEC — Reliable Data Pipeline

## Goals

- Fetch every product from three upstream APIs that differ in pagination, schema, latency,
  reliability and rate limiting.
- Normalize them into one consistent representation.
- Never lose successful results because some other source failed.
- Report clearly enough that a reader can tell *what* went wrong and *where*.

## Non-goals

- No database, no deployment, no containerization of this application.
- No HTTP API of our own. The deliverable is a CLI producing a JSON report.
- No modification of the supplied mock (`server.py`, `fixtures.json`, `test_server.py`).

## Upstream behaviour (read from the supplied source, not assumed)

| Source | Pagination | Price format | Failure mode |
|---|---|---|---|
| A | `?page=N`, `total_pages` | float, major units | none |
| B | opaque `next_cursor` | integer minor units (cents) | `cursor-2` 503 ×1, `cursor-3` 502 ×2, then succeeds |
| C | `?offset=&limit=`, `max_page_size: 2` | decimal string | 429 beyond 2 requests/second |

Two properties of the fixtures drive the acceptance criteria:

- `b-205` carries `amount_cents: "not-a-number"`. It is in `fixtures.json`, not in a
  scenario, so **every** run drops exactly one record. A fully clean run is unreachable.
- There are no cross-source duplicate products. Duplicate handling is specified and unit
  tested, but never exercised by live data.

## Normalized product

```json
{
  "id": "endpoint_a:a-101",
  "title": "Mechanical Keyboard",
  "source": "endpoint_a",
  "price": "89.99",
  "currency": "GBP",
  "category": "electronics",
  "fetched_at": "2026-09-02T20:46:19Z"
}
```

- **`id` is namespaced** with its source. Upstream ids are only unique within a source.
- **`price` is `Decimal`, serialized as a string.** Source B sends cents and source C sends
  decimal strings; binary floats would make `4950` and `"49.50"` disagree on round-trip.
- **`category` is lowercased and trimmed**, so `"Office"` and `"office"` unify.
- **`currency` is assumed `GBP`.** No source states a currency; this is a documented guess,
  configurable via `PIPELINE_CURRENCY`.

## Failure behaviour

### Record level
A record failing validation is dropped, never fatal. It produces a `malformed_record`
warning naming the record and the failing field, logged at WARNING and included in the
report.

### Page level
An HTTP error ends pagination for that source only. Pages already retrieved are kept, so a
source that dies midway is `degraded` rather than lost.

### Source level
A source raising any exception is recorded with a `source_failed` warning. Sources run
under `asyncio.gather(return_exceptions=True)`; one failing never cancels another.

### Source status
- `ok` — completed, nothing dropped.
- `degraded` — completed with dropped records, or failed after yielding some records.
- `failed` — yielded nothing.

### Run status
- `success` — every source `ok` and no warnings at all.
- `partial_success` — at least one product was produced, but something went wrong.
- `failed` — no source produced any product.

Exit code is 0 for `success` and `partial_success`, 1 for `failed`. Partial data is a
usable outcome, so a degraded run is not treated as an error by default.

## Duplicates

Identity is the punctuation- and case-insensitive title plus category. On collision the
record from the higher-precedence source wins (A > B > C) and a `duplicate_conflict`
warning records what was dropped. Precedence rather than arrival order keeps results
deterministic despite concurrent fetching.

## Retry policy

- Retried: 408, 425, 429, 500, 502, 503, 504, timeouts, transport errors.
- Never retried: 400, 404, 422 — deterministic, so a retry only wastes budget.
- Exponential backoff with full jitter; `Retry-After` takes precedence, itself capped at
  `max_retry_after` so a hostile header cannot stall the run.
- Each source has its own **retry budget** (default 10). Exhausting it fails that source
  rather than letting one broken upstream consume the whole run.
- Source C is paced proactively at its documented 2 req/s, so 429s are avoided rather than
  absorbed. The reactive 429 path remains as a safety net.

## Timeouts

- Per-request connect/read timeouts on the shared client.
- A whole-run deadline (`run_timeout`, default 60s). Sources run as tasks, so a source
  cancelled by the deadline still contributes its partial results and appears in the
  report as `failed` with a deadline error — the run never returns nothing because one
  source hung.

## Acceptance criteria

| # | Criterion | Verified by |
|---|---|---|
| 1 | Source B's cents become major units (`3499` → `34.99`) | `test_normalize.py` |
| 2 | Source C's price string parses exactly (`"49.50"`) | `test_normalize.py` |
| 3 | `b-205` is dropped with a warning; its page-mates survive | `test_normalize.py`, `test_runner.py` |
| 4 | Each pagination style is walked to exhaustion | `test_pagination.py` |
| 5 | A source that never recovers does not prevent the others reporting | `test_runner.py` |
| 6 | A failed source appears in both the log and the report | `test_runner.py` |
| 7 | All-sources-down yields `failed` and exit 1 | `test_cli.py` |
| 8 | The report is JSON with exact decimal prices | `test_runner.py` |
| 9 | Transient 503/502 recover without data loss | `test_resilience.py` |
| 10 | Source C completes with zero 429s | `test_resilience.py` |
| 11 | A source that never recovers exhausts its attempts and gives up | `test_resilience.py` |
| 12 | A deterministic 4xx is attempted exactly once | `test_resilience.py` |
| 13 | `Retry-After` is honoured and capped | `test_resilience.py` |
| 14 | A run exceeding its deadline still reports partial data | `test_resilience.py` |

## Assumptions and open questions

- Currency is `GBP`; unstated upstream.
- A price of zero or below is invalid, not merely suspicious.
- `total_pages` / `next_cursor` / `next_offset` are trusted, but bounded by a page cap and
  a forward-progress check so a malformed response cannot loop forever.
- Identity by title+category is a heuristic. Two genuinely different products sharing both
  would merge incorrectly. Acceptable given no upstream shared identifier exists.

## Phasing

1. **Fetch, normalize, log** — no retry. Partial failure is visible and isolated.
2. **Resilience** — retries, backoff, rate limiting, timeouts.
3. **Metrics and performance** — per-source counters, pooling, final documentation.
