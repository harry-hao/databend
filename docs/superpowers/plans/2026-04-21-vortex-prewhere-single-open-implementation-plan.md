# Vortex prewhere single-open reuse Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the double `open_read_at` + double scan/decode of the same Vortex block file in `ReadState::deserialize_and_filter_vortex`, by opening each `part.location` once per block and reusing the opened Vortex file for both pre and remain stages.

**Architecture:** Introduce a small “opened Vortex file” context (runtime/session/file) and scan helpers in the Vortex deserialization module. Refactor the existing single-shot decode to be layered on top. Update the ReadState Vortex two-stage path to reuse a single opened file handle while preserving existing bitmap selection semantics.

**Tech Stack:** Rust, Databend Fuse storage, Vortex (`vortex`/`vortex-file`), OpenDAL, Arrow.

---

## File structure (what changes where)

**Modify:**
- `src/query/storages/fuse/src/io/read/block/vortex_deserialize.rs`
  - Add an internal `OpenedVortexFile` helper
  - Add `open_vortex_file(...)` and `scan_opened_vortex_file_to_record_batch(...)`
  - Refactor decode pipeline to reuse helpers
  - Factor out `RecordBatch -> DataBlock` mapping so it can be reused by `ReadState`
- `src/query/storages/fuse/src/operations/read/read_state.rs`
  - Update `deserialize_and_filter_vortex` to open once and scan twice on the same opened file

**Optionally add (if easiest place for evidence):**
- `src/query/storages/fuse/src/io/read/block/vortex_read_at.rs`
  - Add debug/metrics counters for `read_at` calls and bytes (for evidence, not correctness)

**Docs:**
- This plan file only (already part of the plan).

## Task 1: Add “opened file” scan helpers for Vortex

**Files:**
- Modify: `src/query/storages/fuse/src/io/read/block/vortex_deserialize.rs`

- [ ] **Step 1: Introduce `OpenedVortexFile` helper type**

Implement an internal struct holding:
- `rt: SingleThreadRuntime`
- `session: VortexSession`
- `file: vortex_file::VortexFile` (or the concrete type returned by `open_read_at`)

Keep it private to the module initially.

- [ ] **Step 2: Implement `open_vortex_file(operator, location)`**

Open once using the existing logic:
- `OpendalReadAt::open(operator, location).await`
- `session.open_options().open_read_at(read_at).await`

Preserve existing error messages and `ErrorCode` mapping.

- [ ] **Step 3: Implement `scan_opened_vortex_file_to_record_batch(opened, projection, scan_filter)`**

Move the existing “scan builder → iterator → read_all → arrow conversion” into a helper that:
- calls `opened.file.scan()?`
- optionally applies `.with_filter(filter)`
- optionally applies `.with_projection(select(fields, root()))`
- calls `into_array_iter(&opened.rt)?.read_all()?`
- returns `RecordBatch` via existing `record_batch_from_vortex_root(...)`

Verify this helper takes `&OpenedVortexFile` (shared reference) so it can be called twice.

- [ ] **Step 4: Refactor `decode_vortex_file_to_record_batch(...)` to call the new helpers**

`decode_vortex_file_to_record_batch` should become a thin wrapper:
- create `OpenedVortexFile`
- call `scan_opened_vortex_file_to_record_batch(...)`

Behavior must remain unchanged for non-ReadState call sites.

- [ ] **Step 5: Run a minimal build check for the Fuse crate**

Run:

```bash
cargo check -p databend-common-storages-fuse
```

Expected: `Finished dev [unoptimized + debuginfo]` with exit code 0.

- [ ] **Step 6: Commit**

```bash
git add src/query/storages/fuse/src/io/read/block/vortex_deserialize.rs
git commit -m "$(cat <<'EOF'
refactor(fuse): factor vortex file open+scan helpers

Introduce reusable helpers to open a Vortex file once and build RecordBatches via scan, preparing reuse across prewhere stages.
EOF
)"
```

## Task 2: Reuse a single open in `ReadState::deserialize_and_filter_vortex`

**Files:**
- Modify: `src/query/storages/fuse/src/operations/read/read_state.rs`
- Modify: `src/query/storages/fuse/src/io/read/block/vortex_deserialize.rs`

- [ ] **Step 1: Factor out `RecordBatch -> DataBlock` mapping in the Vortex BlockReader path**

In `vortex_deserialize.rs`, extract a helper on `BlockReader` (or free helper) which:
- takes `record_batch: &RecordBatch`
- uses `column_name_paths(...)`, `try_column_by_name(...)`, `Value::from_arrow_rs(...)`
- emits the same `DataBlock` as today

Goal: allow pre_reader/remain_reader to map their own projections without duplicating code.

- [ ] **Step 2: Add an entry-point that decodes using an already-opened file**

Add a function like:
- `decode_opened_vortex_file(operator, opened: &OpenedVortexFile, location: &str, projection: FieldNames, scan_filter: Option<Expression>) -> Result<RecordBatch>`

Or, if you keep `OpenedVortexFile` owning operator/location, provide a method on it.

Requirement: ReadState can call “scan to RecordBatch” twice without reopening.

- [ ] **Step 3: Update `ReadState::deserialize_and_filter_vortex` to open once and scan twice**

Pseudo-flow:

- open once for `part.location`
- build pre `RecordBatch` using pre projection
- (optional) apply Arrow row selection if any (pre stage typically none)
- map to `DataBlock` using `pre_reader`’s mapping helper
- compute bitmap selection (existing code)
- build remain `RecordBatch` using remain projection
- apply Arrow row selection (bitmap-derived)
- map to `DataBlock` using `remain_reader`’s mapping helper
- merge + resort (existing code)

Important constraints to preserve:
- bitmap length mismatch check stays
- error codes remain compatible

- [ ] **Step 4: Add minimal evidence that open happens once per block (debug log or counter)**

Cheapest option:
- `debug!` in `open_vortex_file` printing a stable prefix like `"[vortex-open]"` and the `location`

This is for local validation and can later be replaced by a metric if desired.

- [ ] **Step 5: Run focused checks**

```bash
cargo check -p databend-common-storages-fuse
```

If the workspace has a targeted unit/integration test for fuse reads, run the smallest relevant one; otherwise stop at `cargo check` for this task.

- [ ] **Step 6: Commit**

```bash
git add src/query/storages/fuse/src/operations/read/read_state.rs
git add src/query/storages/fuse/src/io/read/block/vortex_deserialize.rs
git commit -m "$(cat <<'EOF'
perf(fuse): reuse opened vortex file across prewhere stages

Open each Vortex block file once per part in ReadState and reuse it for pre and remain scans to avoid duplicated open+scan+decode work.
EOF
)"
```

## Task 3: Validate with the known regression query (local NVMe)

**Files:**
- None (runtime validation)

- [ ] **Step 1: Build `databend-query` in debug mode**

Run:

```bash
make build
```

Expected: `target/debug/databend-query` exists.

- [ ] **Step 2: Run the repro query on both Parquet and Vortex tables**

Run the exact query:

```sql
SELECT * FROM hits_vortex
WHERE URL LIKE '%google%'
ORDER BY EventTime
LIMIT 10;
```

Collect:
- wall time
- whether timeouts occur
- debug logs confirming a single `[vortex-open]` per `part.location` during a prewhere read

Also run the Parquet baseline table query under the same settings for comparison.

- [ ] **Step 3: Sanity-check correctness**

Confirm result sets are consistent (same 10 rows / ordering) between Parquet and Vortex.

- [ ] **Step 4: Commit any follow-up adjustments**

Only if required (e.g., log gating changes). Otherwise skip.

## Self-review checklist (plan quality)

- Spec coverage: This plan implements A1 single-open reuse, preserves semantics, adds minimal evidence.
- Placeholder scan: No TODO/TBD steps; all steps specify exact files and commands.
- Type consistency: `OpenedVortexFile` and helper functions are introduced before ReadState modifications.

