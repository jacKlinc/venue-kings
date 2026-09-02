# PLAN — Implementation Approach

A working document. Updated as phases land.

## Shape

A CLI, not a service. The task asks for aggregation and a consolidated result; an HTTP API
of our own would add scope without demonstrating anything the CLI doesn't.

```
cli.py      argument parsing, exit code, where the report goes
runner.py   concurrency, per-source isolation, report assembly
sources/    one module per pagination style, behind a shared protocol
normalize.py raw record -> NormalizedProduct, one record at a time
dedupe.py   cross-source identity
models.py   every shape, in Pydantic
```

The `Source` protocol is the main structural decision. Each adapter exposes
`paginate() -> AsyncIterator[Page]` and owns its own pagination quirk, so `runner.py`
never learns that A uses page numbers and C uses offsets. Adding a fourth source means
adding one module.

## Major decisions

**asyncio over threads.** The workload is entirely I/O-bound. Sources run concurrently;
pages within a source stay serial because pagination is inherently sequential — you cannot
request page N+1 until page N tells you it exists. This is also why a full run takes about
as long as the slowest source rather than the sum.

**Decimal, not float.** Source B sends integer cents, source C sends decimal strings. With
binary floats, `4950/100` and `float("49.50")` are not reliably equal, which would corrupt
both the output and any price comparison. Prices serialize as JSON strings to preserve this.

**Validation at the boundary, per record.** Each source has a raw Pydantic model using the
upstream's own field names. A `ValidationError` is precisely the "malformed record" case,
caught per record so one bad row cannot take down its page. This is why `b-205` is dropped
while `b-204` and `b-206` survive.

**Failures are values, not exceptions.** `collect_source` catches everything and returns a
`SourceOutcome`. With `asyncio.gather`, an escaping exception could cancel siblings, which
is exactly the data loss we are asked to prevent.

**Namespaced ids.** Upstream ids are unique only within a source, so `endpoint_a:a-101`.

## Tradeoffs

**Duplicate identity is a heuristic.** Title+category, because no upstream identifier is
shared across sources. Two genuinely distinct products with the same name and category
would merge wrongly. Accepted: the alternative is not deduplicating at all. Notably the
fixtures contain **no** cross-source duplicates, so this path is unit tested rather than
observed — worth knowing before trusting it.

**Trusting pagination metadata, but bounded.** We follow `total_pages`, `next_cursor` and
`next_offset`, guarded by a page cap, a repeated-cursor check, and a forward-progress check
on the offset. Cheap insurance against an infinite loop.

**Currency is assumed.** No source states one. `GBP` is a documented guess.

## Testing strategy

Every test runs against the **real supplied mock**, started as a subprocess. No stubbed
HTTP transports anywhere. The mock is the specification of upstream behaviour, so testing
against a hand-written fake would risk verifying our misunderstanding of it.

Mechanics that this forces:
- Scenarios are process-level env vars, so one server per scenario, each on a free port.
- The mock is globally stateful (B's failure counters, C's rate window), so fixtures call
  `POST /admin/reset` between tests and the suite must not run in parallel.
- `sys.executable` launches it, never bare `python3` — a stray `.python-version` in the
  environment should not break the suite.

Where the mock cannot express a case, we test the nearest case it can and record the gap
rather than building infrastructure around it. It has no 422, so the "deterministic 4xx is
not retried" test uses its 400. It always emits valid JSON, so malformed bodies are
untested.

## Verification performed

Beyond the suite, each phase is run end to end against a live mock and the report inspected
by hand. Phase 1's run confirms the intended degraded behaviour: A returns all 6 products,
B stops at its first 503, C stops at its first 429, and the run still reports
`partial_success` with 12 products rather than failing.

## Status

- **Phase 1 — fetch, normalize, log.** Done. No retry layer, so transient upstream failures
  degrade their source by design; this is what makes the isolation visible.
- **Phase 2 — resilience.** Retries with jittered backoff, `Retry-After`, proactive rate
  limiting for source C, per-request and whole-run timeouts.
- **Phase 3 — metrics and performance.** Per-source counters in the report, connection
  pooling, final documentation.

## With more time

- A circuit breaker, so a source known to be down is skipped rather than retried each run.
- Persist reports to compare runs and detect upstream drift.
- Property-based tests over the normalizers.
