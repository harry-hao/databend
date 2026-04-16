# Fuse Vortex predicate pushdown (Phase1+Phase2) design
Date: 2026-04-16

## Summary

This spec proposes a two-phase plan to make Databend Fuse `storage_format = 'vortex'` avoid wasted I/O and decoding, and then progressively enable predicate pushdown using Vortex scan.

- **Phase 1 (foundation)**: stop forcing full-file reads for Vortex blocks, open Vortex files via range-capable I/O (`open_read_at`), and make Databend’s existing **two-phase read** (read small set of columns → compute bitmap → read remaining columns filtered by bitmap/selection) actually effective for Vortex.
- **Phase 2 (pushdown MVP)**: translate a safe subset of Databend filter expressions into Vortex `Expression` and run them via `vortex-scan` (`ScanBuilder::with_filter`) so Vortex computes row masks internally; fallback to Phase 1 when translation is unsafe or unsupported.

**Acceptance standard for this work is “least waste”:**

- Stop the current “Vortex must read the entire file” behavior in the hot path.
- Ensure we do not decode/allocate more columns/rows than necessary when prewhere/runtime-filters/other selections exist.
- Do **not** add new metrics in this iteration. Validation relies on code-path invariants, existing profiling counters, and tests.

## Background (current behavior)

Databend already has a strong storage-layer pruning pipeline that runs before reading blocks:

- block/segment pruning via `col_stats` (range / zone-map), cluster stats (page pruning range), bloom, inverted index, etc.

However, the current Vortex read implementation wastes Vortex’s capabilities:

- **Full-file read is forced**: Vortex block format patches projected column ranges to `[0, file_size)` so merge I/O reads the entire file.
- **Full decode is forced**: Vortex deserialization uses `open_buffer(full_bytes)` and then `scan().into_array_iter(...).read_all()`, materializing a full root array/RecordBatch even when only a few columns are needed.

Databend’s compute layer already supports “two-phase read” via `ReadState`:

- preread only prewhere/runtime-filter columns to compute a bitmap
- then read remaining columns and apply `RowSelection(bitmap)`

But for Vortex this currently yields little benefit because of the full-file + full-decode behavior.

## Goals

### G1: “Least waste” in hot-path I/O

- Vortex blocks should not default to reading the full file when only a subset of columns is requested.
- Vortex file opening/reading should support **range reads** so only necessary bytes are fetched.

### G2: Two-phase read works for Vortex

When applicable, support:

- **Read few columns → compute bitmap/selection**
- **Read remaining columns → apply bitmap/selection**

Triggers include:

- prewhere filters
- runtime filters
- other pushdown-derived selections where applicable (see “Selections” below)

### G3: Phase 2 predicate pushdown MVP (no `IN`, no `LIKE`)

Allow Vortex to evaluate a safe subset of predicates on its scan pipeline to produce row masks (and skip decoding unneeded rows).

Supported in Phase 2 MVP:

- conjunction (`AND`) only
- comparisons: `=, !=, <, <=, >, >=`
- null checks: `IS NULL`, `IS NOT NULL`

Not supported in Phase 2 MVP (always fallback to Phase 1):

- `IN (...)`
- `LIKE/ILIKE` (any wildcard/escape/case variant)
- `OR`, `NOT`
- complex functions, implicit casts with uncertain semantics, Variant/JSON path, etc.

## Non-goals

- Introducing new metrics/telemetry counters for Vortex in this iteration.
- Full expression coverage parity between Databend and Vortex.
- Changing existing block/segment pruning logic in `FusePruner` beyond feeding selections into reading.

## Key concept: selections inside a block

Databend’s pruning pipeline can produce “inside-block” restrictions that should reduce work:

- **`page pruner range`**: a contiguous row `Range<usize>` in the block that may contain matches (from cluster stats).
- **`inverted index matched_rows`**: a set of explicit row indices (`Vec<usize>`) in the block that match an index search (optionally with scores).
- **bitmap selection**: computed from prewhere/runtime filters (Databend) or from Vortex scan filter (Phase 2).

Phase 1 should ensure these restrictions can be applied without forcing full-file read/decode.

## Architecture overview

### Components

1) **Range-capable Vortex I/O adaptor**

- Implement a Vortex-compatible random-access reader backed by opendal `Operator`.
- Use `vortex-file` `VortexOpenOptions::open_read_at(...)` to open the file without an in-memory full-buffer.

2) **Vortex block-format read policy**

- Stop patching each column span to the full file by default.
- Keep accurate `ColumnMeta::Vortex(offset, len)` for merge I/O, so projected reads are narrow.
- Keep a conservative fallback path for unexpected cases.

3) **Vortex decode/scan path**

- Phase 1: decode only requested columns (projection) and apply `RowSelection` if present.
- Phase 2: build and run a Vortex scan with `with_filter` + projection, so Vortex produces the row mask internally.

4) **Fallback strategy**

- If filter translation is unsafe or Vortex scan fails, fallback to Phase 1.
- If range-capable open fails unexpectedly, fallback to the current full-file buffer path (correctness first).

## Phase 1: foundation tasks (must land before Phase 2 is meaningful)

### P1-1 Stop forcing full-file reads

**Where:** `src/query/storages/fuse/src/operations/read/block_format/vortex.rs`

**Change:**

- Remove/disable the default behavior that patches projected column spans to `[0, file_size)`.
- Keep the “full-file read” behavior only as a guarded fallback path.

**Validation (no new metrics):**

- Ensure the normal code path does not widen column spans.
- Use existing scan/IO profiling to confirm reduced remote IO bytes when projecting few columns.

### P1-2 Open Vortex files via range-capable I/O (`open_read_at`)

**Where:** new module under `src/query/storages/fuse/src/io/read/block/` (proposed: `vortex_read_at.rs`).

**Change:**

- Implement the Vortex `ReadAt` interface over opendal so Vortex can request byte ranges.
- Replace `open_buffer(full_bytes)` as the primary open path with:
  - `VortexOpenOptions::open_read_at(read_at)` (preferred)
  - Optional hints: `with_file_size`, `with_initial_read_size` when safely available.

**Validation:**

- Basic open+scan on a Vortex block works without fetching whole file bytes first.

### P1-3 Decode based on projection, not `read_all()` of the full root

**Where:** `src/query/storages/fuse/src/io/read/block/vortex_deserialize.rs`

**Change:**

- Remove the unconditional “read all arrays then convert to a full RecordBatch”.
- Use `vortex-scan`’s projection-driven masks to only materialize required fields.

**Validation:**

- Queries projecting a single column should not decode unprojected columns.

### P1-4 Make Databend two-phase read effective for Vortex

**Where:** integrate via existing `ReadState` and Vortex reader behavior.

**Change:**

- Ensure preread reader for Vortex only reads prewhere/runtime-filter columns.
- Ensure remain reader for Vortex only reads remaining columns and applies `RowSelection`.

**Validation:**

- Existing prewhere/runtime-filter tests behave correctly for Vortex format.

### P1-5 Apply other inside-block restrictions when present

**Change:**

- If `page pruner range` exists, prefer passing it as row-range selection into the Vortex scan/decode path.
- If `matched_rows` exists, pass it as row-indices selection where feasible; otherwise use Databend-side filtering as a safe fallback.

**Validation:**

- Correctness parity with existing results.

### P1-6 Hard fallback rules + tests

**Change:**

- Define explicit fallback gates:
  - any unexpected Vortex open/scan error → fallback to Phase 1 or legacy full-file path depending on severity
  - never silently “partially push down” in a way that can change semantics

**Tests:**

- At least one integration test covering:
  - projection-only
  - prewhere → bitmap → remain
  - runtime-filter involvement (if test harness exists)

## Phase 2: predicate pushdown MVP tasks

Phase 2 should only be attempted once Phase 1 removes the full-file/full-decode waste. Otherwise “pushdown” becomes a semantic feature with little performance impact.

### P2-1 Define the safe predicate subset and invariants

**Supported:**

- `AND` over supported atoms
- comparisons between a column and a constant (or two columns if semantics are guaranteed; default MVP is column-vs-constant)
- `IS NULL` / `IS NOT NULL`

**Unsupported (fallback):**

- `IN`, `LIKE/ILIKE`, `OR`, `NOT`
- functions
- implicit casts where Databend and Vortex could differ

### P2-2 Implement Databend Expr → Vortex Expression translator (capability detection)

**Change:**

- Add a translator that returns `Option<Expression>`:
  - `Some(expr)` only when we can prove equivalence and type compatibility
  - `None` otherwise

**Key rule:** “If unsure, fallback.”

### P2-3 Integrate `ScanBuilder::with_filter` + projection in Vortex read path

**Change:**

- When translator returns `Some(expr)`:
  - build scan: `file.scan()?.with_filter(expr).with_projection(proj_expr)`
  - produce Arrow batches via `into_record_batch_reader(...)` (or array iterator then conversion)
  - return `DataBlock` results without doing a separate Databend-side preread bitmap

### P2-4 Avoid duplicate work with two-phase read

**Rule:**

- If Vortex filter pushdown is active for the block, do not also run preread bitmap computation in Databend for the same predicate.
- If translator returns `None`, use Phase 1 two-phase bitmap path.

### P2-5 Fallback matrix (explicit)

For each block:

- **Try pushdown** if:
  - filter exists
  - translator returns `Some`
  - Vortex scan open succeeds
- Otherwise **fallback to Phase 1**

Additionally:

- If pushdown works for some conjuncts but not others, treat the whole filter as unsupported (MVP) and fallback.

### P2-6 Tests

Add integration tests for Vortex blocks that validate:

- `AND` of comparisons
- `IS NULL`/`IS NOT NULL`
- type mismatch cases fall back (correctness preserved)

## Validation & “least waste” criteria (no new metrics)

We will consider Phase1+Phase2 acceptable when:

- **No default full-file range patching** remains in the hot path.
- **No default `open_buffer(full_bytes)`** is used in the hot path; it is only used as fallback.
- **Projection affects work**: projecting fewer columns reduces work (observable via existing scan/profiling counters and/or code-path inspection).
- **Two-phase read is effective**: presence of prewhere/runtime filters does not require reading/decoding unrelated columns.
- **Phase 2 does not regress**: unsupported predicates always fall back to Phase 1 and preserve correctness.

## Risks & mitigations

- **Semantic mismatch (types/NULL)**:
  - Mitigation: translator is conservative; fallback on any ambiguity.
- **Vortex API evolution**:
  - Mitigation: keep Phase 2 MVP small and isolated behind a translator module; avoid deep coupling.
- **Performance regressions due to fallback churn**:
  - Mitigation: fallback should reuse Phase 1 infrastructure (range I/O + projection), not legacy full-file reads.

## Open questions (to resolve during implementation)

- Exact mapping from Databend column IDs / name paths to Vortex field paths for nested types (MVP may restrict nested support).
- Best representation for `matched_rows` in Vortex scan selection (row indices vs mask).
- Where to host the translator module so it can be shared by prewhere/runtime-filter and other paths cleanly.

