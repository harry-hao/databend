---
title: Hits benchmark standalone script (Fuse vs Vortex) — design
date: 2026-04-17
status: draft
---

## Goal

Create a **single script** to run an automated, repeatable benchmark that:

- Starts `databend-meta` and `databend-query` **if not running** (and supports cold-start runs by restarting them).
- Uses **release binaries** to collect realistic performance metrics.
- Can be run from **any working directory** (no assumption that the script is executed inside the Databend repo).
- Brings along the required standalone config resources (based on `scripts/ci/deploy/databend-query-standalone.sh`) by copying them to the **current directory**.
- Loads `hits.tsv.gz` using Databend’s ability to read compressed data.
- Creates two tables with identical schema:
  - baseline: `ENGINE=FUSE` (default format)
  - vortex: `ENGINE=FUSE storage_format='vortex'`
- Runs `benchmark/hits/queries/*.sql` queries against both tables, collects **3 run times per query**, and reports:
  - performance comparison per query
  - correctness validation of vortex results vs baseline

Non-goals:

- Building a generic benchmark framework for arbitrary datasets.
- Making every parameter configurable via CLI options. Operational knobs are script variables.

## User-facing contract

### Invocation requirements

The script is run from an arbitrary directory (the “run directory”), and requires:

- `DATABEND_DIR` environment variable pointing to the Databend repo root.

All other tuning is done by editing variables near the top of the script.

### Produced artifacts (in run directory)

The script writes all artifacts under the run directory:

- `./artifacts/`
  - `logs/`
    - `databend-meta.log`
    - `databend-query.log`
    - `bendsql.log` (optional, for debugging)
  - `config/`
    - `databend-meta-node-1.toml` (copied + minimally rewritten)
    - `databend-query-node-1.toml` (copied + minimally rewritten)
  - `results/`
    - `timings.csv` (per Q, per table, per run)
    - `correctness.csv` (per Q: pass/fail + fingerprint values)
    - `report.md` (human summary)
    - `errors.jsonl` (one JSON object per error)
  - `diffs/` (only populated when correctness mismatch and fallback diff is enabled)
- `./databend-data/` (meta/query data dirs; script-owned)

Exit code:

- The run completes even if individual queries fail.
- Exit code is non-zero if:
  - services failed to start, or
  - table creation / data load failed, or
  - any query correctness check failed (configurable), or
  - any query execution failed (configurable).

## High-level flow

For each table variant (baseline, vortex):

1. Cold start services (restart meta/query) with run-directory-local configs and data dirs.
2. Ensure database exists; create and load the table variant.
3. Run all selected queries:
   - Execute query **3 times** back-to-back on the same running services.
   - Record wall time for each execution:
     - run 1 is “cold” (cold process + cold cache within query run)
     - runs 2-3 are “hot” (warm caches)
   - Record errors without aborting the entire run.

Correctness checking is done by comparing baseline vs vortex results for each query.

## Repository integration points (inputs)

- Standalone startup pattern:
  - reference: `scripts/ci/deploy/databend-query-standalone.sh`
  - configs: `scripts/ci/deploy/config/databend-meta-node-1.toml`, `scripts/ci/deploy/config/databend-query-node-1.toml`
  - readiness: `scripts/ci/wait_tcp.py`
- Hits schema reference:
  - reference: `scripts/benchmark/query/load/hits.sh`
- Queries:
  - reference directory: `benchmark/hits/queries/*.sql`
  - query short name: `Qxx` for file `xx.sql` (e.g. `23.sql` → `Q23`)

## Service lifecycle and configuration

### Release binary resolution

Given `DATABEND_DIR`, derive:

- `META_BIN = $DATABEND_DIR/target/release/databend-meta`
- `QUERY_BIN = $DATABEND_DIR/target/release/databend-query`
- `BENDSQL_BIN = $DATABEND_DIR/target/release/bendsql` (preferred), falling back to `bendsql` on `$PATH` if desired

The script checks file existence and returns a clear error if the release binaries are missing.

### Ports and endpoints

Ports are defined in the script as constants (no CLI flags). Defaults align with existing standalone config:

- meta admin port: `9191`
- query HTTP/MySQL ports as in config (commonly `8000`)

The script waits for TCP ports to open before proceeding.

### “Start if not running” vs cold-start benchmark

The script supports both:

- **Start-if-not-running**: if ports are open, reuse the running service
- **Cold-start mode (benchmark default)**: always stop and restart meta/query before running a table’s query suite

This aligns with the requirement:

- “Each run should be cold-start”
- “Within a run, a single SQL can run 3 times; first cold, second/third hot”

### Config materialization into run directory

To satisfy “don’t assume script is in repo” and “copy required resources into current directory”:

- Copy the two TOML config templates from `DATABEND_DIR/scripts/ci/deploy/config/` into `./artifacts/config/`
- Apply minimal patching:
  - set data directories to `./databend-data/...`
  - set log paths to `./artifacts/logs/...`
  - ensure bind addresses/ports match script constants

The script should prefer editing only the necessary keys so it remains resilient to upstream config changes.

## Data setup: DB + tables + load

### Database and table names

Script creates a dedicated database, e.g.:

- database: `hits_bench`
- tables:
  - `hits_fuse` (baseline)
  - `hits_vortex` (vortex)

### Table schema

Use the schema from `scripts/benchmark/query/load/hits.sh`, including `CLUSTER BY`.

DDL differences:

- baseline:
  - `ENGINE=FUSE` (default storage format)
- vortex:
  - `ENGINE=FUSE storage_format='vortex'`

### Loading `hits.tsv.gz`

Data file is provided as:

- `/Users/haoyu/src/databendlabs/hits/hits.tsv.gz`

The script uses Databend’s capability to read compressed data directly. Implementation details depend on supported COPY-from-local semantics:

- preferred: `COPY INTO ... FROM 'file:///abs/path/hits.tsv.gz' ... file_format=(type=TSV ...)`
- acceptable alternates:
  - stage-based load using a local stage abstraction
  - `bendsql` streaming if Databend supports it (still “Databend reads compressed” if it accepts gzip stream natively)

After load:

- `ANALYZE TABLE` for both tables.

## Query execution and timing

### Query selection

The script has a variable like:

- `RUN_QUERIES = ["00","01",...,"42"]`

If empty, it defaults to all SQL files under `benchmark/hits/queries/`.

### Timing method

For each query and table variant, capture 3 wall-clock durations:

- `t1_ms`, `t2_ms`, `t3_ms`

Constraints:

- Timing should be measured in Python (monotonic clock) around a single `bendsql` invocation per run.
- The output is recorded even on error (duration may be omitted / null).

### Avoid stopping on query errors

For any query run failure:

- record:
  - query name, table, run index
  - error text
  - bendsql exit code
- continue with remaining queries

## Correctness validation (layered)

### Why not EXCEPT ALL

Although the SQL parser accepts `EXCEPT ALL`, current query binding/planning only supports:

- `INTERSECT DISTINCT`
- `EXCEPT DISTINCT`

Therefore, correctness checks must not depend on `EXCEPT ALL` semantics.

### Layer 1 (default): result fingerprint (no full materialization)

For each query `Qxx.sql`, define a fingerprint query of the form:

- take original SQL and produce a derived relation `q`
- compute stable aggregates over `q` that are insensitive to row order and have strong coverage of duplicates

Example structure (conceptual):

- `row_count`
- `hash_xor` (xor of row hashes)
- `hash_sum` (sum of row hashes modulo 2^64)
- `hash_sum2` (sum of row hashes * constant modulo 2^64)

Row hash uses a deterministic hash of the serialized row values. The design assumes Databend has a stable 64-bit hash function (e.g. `xxhash64`) or an equivalent.

Acceptance:

- baseline fingerprint must equal vortex fingerprint for pass.

### Layer 2 (optional, only on mismatch): materialize and diff

If fingerprints mismatch:

- optionally materialize both result sets to files under `./artifacts/diffs/Qxx/`
- perform a deterministic diff in Python

This is a diagnostic fallback, not the default path.

### Handling “unstable queries” (LIMIT, etc.)

Instead of rewriting queries to remove `LIMIT` (which can be expensive and change semantics), the fingerprint approach provides stability:

- even if the query uses `ORDER BY ... LIMIT`, the fingerprint is computed over the query’s produced rows and remains stable for correctness comparison between two tables in the same run.

If specific queries are known to be nondeterministic without explicit ordering, the script can maintain a small allowlist that switches those queries to a different correctness mode (e.g. `count(*)` only), but the default is fingerprint.

## Reporting

### Timing report

For each query `Qxx`:

- baseline: `t1,t2,t3`, plus:
  - `hot_avg = avg(t2,t3)`
- vortex: same
- derived:
  - `speedup_hot = baseline_hot_avg / vortex_hot_avg`
  - `speedup_cold = baseline_t1 / vortex_t1`

### Correctness report

For each query:

- status: `PASS`, `FAIL_FINGERPRINT`, `FAIL_EXECUTION`
- fingerprint columns if available
- for failures: link to diff artifacts (if generated) and recorded errors

## Implementation language choice

Primary implementation: **Python driver** with subprocess control:

- Service lifecycle (start/stop/wait)
- Query execution and timing
- CSV/Markdown reporting
- JSONL error log

Shell glue is acceptable for small helpers but Python owns the flow and reporting.

## Open questions (intentionally left to implementation plan)

- Exact Databend SQL functions to compute row hash/fingerprint (selecting the best available stable hash function).
- Exact `COPY INTO` syntax for reading a local `.tsv.gz` directly.

