# Vortex prewhere: single-open reuse (Design)

Date: 2026-04-21  
Status: Draft (for review)  
Audience: Databend storage / Fuse / Vortex integration

## Summary

When `ReadState` (prewhere / runtime filter) is active for `FuseStorageFormat::Vortex`, Databend currently decodes the same Vortex block file twice:

- pre stage: `pre_reader.deserialize_vortex_chunks(location, ...)`
- remain stage: `remain_reader.deserialize_vortex_chunks(location, ..., selection)`

Each call opens the Vortex file via `open_read_at(...)` and runs a full Vortex scan pipeline. On NVMe with the same dataset, query, and settings, this can cause catastrophic regressions (e.g. `SELECT * ... LIKE ... ORDER BY ... LIMIT 10` taking >1 hour vs Parquet ~3 seconds).

This design proposes **Approach A1**: keep the two-stage prewhere logic, but **open each Vortex block file once per `FuseBlockPartInfo`** and reuse the opened Vortex file handle for both the pre and remain scans.

## Problem statement

### Observed behavior

For the same dataset size, same SQL, same settings, and local NVMe storage:

- Parquet: returns in ~3 seconds
- Vortex: can time out / exceed 1 hour

The slowdown is disproportionately worse for `SELECT *` (106 columns in `hits`) compared to narrow projections, consistent with the remain stage being wide and expensive.

### Root cause hypothesis (code-based)

`ReadState::deserialize_and_filter_vortex` currently performs **two independent Vortex open+scan+decode** passes for the same `part.location`.

Parquet prewhere uses a different cost model: one merge-IO read (bytes shared), then two decode passes over the same in-memory buffers. Vortex currently does not share I/O or file-level initialization across stages.

## Goals / Non-goals

### Goals

- **G1**: For a single `FuseBlockPartInfo` (`part.location`), ensure Vortex performs **at most one `open_read_at`** within `deserialize_and_filter_vortex`.
- **G2**: Preserve correctness and existing prewhere semantics (bitmap selection, row filtering behavior).
- **G3**: Reduce catastrophic regressions for wide projections such as `SELECT *`.

### Non-goals

- **NG1**: Do not introduce global cross-block caching/pooling of open file handles (no FD/eviction management in this phase).
- **NG2**: Do not require Vortex native row-selection support; continue to apply `RowSelection` via Arrow `filter_record_batch` in Databend when needed.
- **NG3**: Do not redesign the query optimizer’s prewhere selection rules.

## Background: current Vortex read flow

Databend Vortex decoding path (simplified):

- `OpendalReadAt::open(operator, location)`
- `session.open_options().open_read_at(read_at)` → `file`
- `file.scan()` → `ScanBuilder`
  - optional `.with_filter(vortex::expr::Expression)`
  - optional `.with_projection(select(fields, root()))`
- `into_array_iter(&rt)?.read_all()?` → `vortex::ArrayRef`
- convert to Arrow → `RecordBatch`
- (optional) apply `RowSelection` via Arrow `filter_record_batch`
- map projected columns into `DataBlock`

The key inefficiency for prewhere is that the above “open + scan + read_all” work is executed twice per block.

## Proposed approach (A1): single-open reuse for prewhere

### Key idea

Within `ReadState::deserialize_and_filter_vortex(part)`:

1. Open the Vortex file once (construct runtime/session, `OpendalReadAt`, `open_read_at`) and retain the resulting `VortexFile`.
2. Run a **pre scan** against that opened file using the pre projection and (optionally) Vortex scan filter.
3. Compute bitmap selection (prewhere + runtime filters) on the resulting `DataBlock`.
4. Run a **remain scan** against the same opened file using remain projection.
5. Apply `RowSelection` after decoding (existing Arrow filter path).
6. Merge blocks and resort to output schema (existing behavior).

### API feasibility and risk

Vortex `VortexFile::scan(&self)` takes `&self` and returns a `ScanBuilder`, allowing multiple scans from the same opened file object.
Therefore, reusing a single opened `VortexFile` for multiple scans is expected to be supported and low-risk.

## Design details

### New helper type

Add a small internal helper in `vortex_deserialize.rs`:

- `OpenedVortexFile`
  - owns the `SingleThreadRuntime` used today (or equivalent runtime object)
  - owns the `VortexSession`
  - owns the opened `VortexFile` (returned by `open_read_at`)

### New helper functions

In `vortex_deserialize.rs`:

- `open_vortex_file(operator, location) -> Result<OpenedVortexFile>`
  - performs `OpendalReadAt::open` and `open_read_at`
  - returns an opened file ready for scanning

- `scan_opened_vortex_file_to_record_batch(opened, projection, scan_filter) -> Result<RecordBatch>`
  - performs `file.scan()` and `read_all()`
  - converts to Arrow `RecordBatch`

Optionally split/retain:

- keep a single-shot `decode_vortex_file_to_record_batch(...)` for non-prewhere callers by calling the above helpers.

### Reusing `RecordBatch -> DataBlock` mapping

Avoid duplicating the “map RecordBatch into DataBlock according to `BlockReader` projection” logic.
Factor out a helper in `BlockReader`’s Vortex codepath, e.g.:

- `BlockReader::vortex_record_batch_to_data_block(&self, record_batch, name_paths) -> Result<DataBlock>`

The helper should preserve:

- default value handling (`self.default_vals`)
- `DataType` conversions (`Value::from_arrow_rs`)
- row count validation against metadata

### ReadState integration changes

Modify `ReadState::deserialize_and_filter_vortex(part)` to:

1. Build one `OpenedVortexFile` (using the same `opendal::Operator` used by `BlockReader`).
2. Run pre scan via **pre_reader** projection.
3. Compute bitmap selection.
4. Run remain scan via **remain_reader** projection.
5. Apply selection post-decode (existing Arrow filter), merge, resort, return.

### Correctness constraints

- Preserve the existing restriction: `scan_filter + row_selection` combination is not expected in the current code path (as enforced in `deserialize_vortex_chunks_with_scan_filter`).
- Preserve bitmap length checks (`bitmap.len() == part.nums_rows`) and error behavior.

## Observability (minimal evidence)

Add minimal debug-level counters/metrics to validate impact:

- count how many times `open_read_at` is invoked per `part.location`
- optionally count `read_at` calls / bytes inside `OpendalReadAt` (for later follow-up if needed)

These should be low overhead and safe to keep gated behind debug logs or existing metrics infrastructure.

## Testing & validation plan

- **Unit / integration**:
  - Add a focused test (or debug-only assertion) ensuring `deserialize_and_filter_vortex` triggers a single open for a block (can be validated via a counter injected into `OpendalReadAt`).
- **Benchmark / repro**:
  - Run the known regression query:
    - `SELECT * FROM hits_vortex WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10`
  - Compare wall time before/after with identical settings on NVMe.

## Rollout & follow-ups

- If A1 reduces time substantially but still lags Parquet, consider:
  - A2 fallback for very wide projections: single-pass decode + in-memory bitmap filtering
  - B-style `ReadAt` caching/merge to reduce small reads
  - expanding `translate_expr_to_vortex` coverage to stay in single-pass `scan_filter` path more often

