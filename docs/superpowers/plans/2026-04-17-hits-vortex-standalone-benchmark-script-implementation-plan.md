# Hits Vortex Standalone Benchmark Script Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Python-driven standalone benchmark script that cold-starts Databend services, loads hits data into two Fuse tables (baseline vs `storage_format='vortex'`), runs `benchmark/hits/queries/*.sql` with 3 timing runs, and reports performance + correctness (layered fingerprint, optional diff on mismatch).

**Architecture:** A single Python entrypoint orchestrates service lifecycle (meta/query), config materialization into the current directory, data load, query execution, timing, and reporting. Correctness is verified via SQL-side result fingerprints to avoid large client-side transfers, with optional materialized diffs only for mismatches.

**Tech Stack:** Python 3 (stdlib only), Databend release binaries (`databend-meta`, `databend-query`, `bendsql`), existing repo scripts/configs (`scripts/ci/wait_tcp.py`, `scripts/ci/deploy/config/*.toml`).

---

## File structure (locked in)

Create:

- `scripts/benchmark/hits_vortex_standalone_bench.py`
  - The benchmark runner. Can be executed from any working directory.
- `scripts/benchmark/tests/test_hits_vortex_standalone_bench.py`
  - `unittest`-based tests for query discovery, SQL rewriting (table substitution), and fingerprint SQL generation.

Modify:

- None required.

Artifacts (runtime, created in current directory when script runs):

- `./artifacts/{logs,config,results,diffs}/`
- `./databend-data/`

## Global constants / knobs (edit-in-script, not CLI options)

Implementation must define these near top of `hits_vortex_standalone_bench.py`:

- `DEFAULT_DATABEND_DIR_ENV = "DATABEND_DIR"`
- Ports:
  - `META_PORT = 9191`
  - `QUERY_PORT = 8000`
- Service config template paths (relative to repo):
  - `META_TOML_TEMPLATE = "scripts/ci/deploy/config/databend-meta-node-1.toml"`
  - `QUERY_TOML_TEMPLATE = "scripts/ci/deploy/config/databend-query-node-1.toml"`
- Query folder:
  - `QUERIES_DIR = "benchmark/hits/queries"`
- Query selection:
  - `RUN_QUERIES = ["00", "01", ..., "42"]` (empty → auto-discover all `*.sql`)
- Data:
  - `HITS_TSV_GZ = "/Users/haoyu/src/databendlabs/hits/hits.tsv.gz"`
  - TSV format settings (delimiter `\t`, record delimiter `\n`, skip header `1`)
- Names:
  - `DB = "hits_bench"`
  - `TABLE_BASELINE = "hits_fuse"`
  - `TABLE_VORTEX = "hits_vortex"`
- Modes:
  - `COLD_START_EACH_TABLE = True` (benchmark default)
  - `FALLBACK_MATERIALIZE_DIFF_ON_MISMATCH = False` (default off; can turn on for debugging)
  - Exit policy:
    - `FAIL_ON_ANY_CORRECTNESS_MISMATCH = True`
    - `FAIL_ON_ANY_QUERY_ERROR = False` (still non-zero if services/load fail)

## Task 0: Skeleton + unittest harness (no Databend needed)

**Files:**
- Create: `scripts/benchmark/hits_vortex_standalone_bench.py`
- Create: `scripts/benchmark/tests/test_hits_vortex_standalone_bench.py`

- [ ] **Step 1: Write failing unit tests for core pure functions**

Create `scripts/benchmark/tests/test_hits_vortex_standalone_bench.py`:

```python
import os
import unittest

from scripts.benchmark.hits_vortex_standalone_bench import (
    discover_query_files,
    query_short_name,
    load_sql_file,
    substitute_hits_table,
    fingerprint_sql_for_query,
)


class TestHitsVortexStandaloneBench(unittest.TestCase):
    def test_query_short_name(self):
        self.assertEqual(query_short_name("00.sql"), "Q00")
        self.assertEqual(query_short_name("23.sql"), "Q23")

    def test_substitute_hits_table_preserves_other_identifiers(self):
        sql = "SELECT COUNT(*) FROM hits WHERE URL LIKE '%hits%';"
        out = substitute_hits_table(sql, "hits_vortex")
        self.assertIn("FROM hits_vortex", out)
        self.assertNotIn("FROM hits ", out)

    def test_fingerprint_sql_wraps_query(self):
        sql = "SELECT COUNT(*) FROM hits;"
        fp = fingerprint_sql_for_query(sql, "hits_fuse")
        self.assertIn("WITH q AS", fp)
        self.assertIn("FROM q", fp)

    def test_discover_query_files_filters_sql(self):
        files = discover_query_files(["/tmp/00.sql", "/tmp/README.md", "/tmp/23.sql"])
        self.assertEqual(files, ["/tmp/00.sql", "/tmp/23.sql"])


if __name__ == "__main__":
    unittest.main()
```

Expected: tests fail (module and functions do not exist yet).

- [ ] **Step 2: Run tests to verify failure**

Run:

```bash
python3 -m unittest -v scripts/benchmark/tests/test_hits_vortex_standalone_bench.py
```

Expected: `ImportError` or `AttributeError` referencing missing module/functions.

- [ ] **Step 3: Implement minimal pure functions to make tests pass**

Create `scripts/benchmark/hits_vortex_standalone_bench.py` with minimal implementations:

```python
import os
from typing import Iterable, List


def discover_query_files(paths: List[str]) -> List[str]:
    return [p for p in paths if p.endswith(".sql")]


def query_short_name(filename: str) -> str:
    base = os.path.basename(filename)
    stem = base[:-4] if base.endswith(".sql") else base
    return f"Q{stem}"


def load_sql_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def substitute_hits_table(sql: str, table: str) -> str:
    # minimal & safe-enough for hits queries: replace standalone identifier hits
    # (implementation will be hardened in later tasks if needed)
    return sql.replace("FROM hits", f"FROM {table}")


def fingerprint_sql_for_query(sql: str, table: str) -> str:
    q = substitute_hits_table(sql, table).rstrip().rstrip(";")
    return (
        "WITH q AS (\n"
        f"{q}\n"
        ")\n"
        "SELECT\n"
        "  count(*) AS row_count\n"
        "FROM q"
    )
```

Notes:

- This is intentionally minimal; correctness fingerprint will be strengthened later once we confirm which hash functions exist in Databend.

- [ ] **Step 4: Run tests to verify pass**

Run:

```bash
python3 -m unittest -v scripts/benchmark/tests/test_hits_vortex_standalone_bench.py
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/benchmark/hits_vortex_standalone_bench.py scripts/benchmark/tests/test_hits_vortex_standalone_bench.py
git commit -m "feat(benchmark): add hits vortex standalone bench skeleton"
```

## Task 1: Query discovery from repo + selection variable

**Files:**
- Modify: `scripts/benchmark/hits_vortex_standalone_bench.py`
- Test: `scripts/benchmark/tests/test_hits_vortex_standalone_bench.py`

- [ ] **Step 1: Add unit tests for discovery from `DATABEND_DIR/benchmark/hits/queries`**

Add tests using a temp dir fixture pattern (stdlib `tempfile`):

```python
import tempfile
from pathlib import Path


def test_discover_query_dir(self):
    with tempfile.TemporaryDirectory() as d:
        p = Path(d)
        (p / "00.sql").write_text("SELECT 1;", encoding="utf-8")
        (p / "23.sql").write_text("SELECT 2;", encoding="utf-8")
        (p / "note.txt").write_text("x", encoding="utf-8")
        files = discover_query_dir(str(p))
        self.assertEqual([os.path.basename(x) for x in files], ["00.sql", "23.sql"])
```

- [ ] **Step 2: Run tests to verify failure**

```bash
python3 -m unittest -v scripts/benchmark/tests/test_hits_vortex_standalone_bench.py
```

Expected: failure due to missing `discover_query_dir`.

- [ ] **Step 3: Implement `discover_query_dir` and query selection**

Add in `hits_vortex_standalone_bench.py`:

```python
from pathlib import Path


def discover_query_dir(dir_path: str) -> List[str]:
    paths = sorted(str(p) for p in Path(dir_path).glob("*.sql"))
    return paths
```

Selection behavior (later used by main):

- If `RUN_QUERIES` is empty: use `discover_query_dir`.
- Else: map each `xx` to `$QUERIES_DIR/{xx}.sql` and validate existence.

- [ ] **Step 4: Run tests to verify pass**

```bash
python3 -m unittest -v scripts/benchmark/tests/test_hits_vortex_standalone_bench.py
```

- [ ] **Step 5: Commit**

```bash
git add scripts/benchmark/hits_vortex_standalone_bench.py scripts/benchmark/tests/test_hits_vortex_standalone_bench.py
git commit -m "feat(benchmark): discover hits query files"
```

## Task 2: Service lifecycle (start/stop/wait) using release binaries

**Files:**
- Modify: `scripts/benchmark/hits_vortex_standalone_bench.py`

- [ ] **Step 1: Add minimal “preflight” checks**

Implement:

- `get_databend_dir()` reads `DATABEND_DIR` env var, errors if missing.
- `resolve_release_bins(databend_dir)` ensures:
  - `target/release/databend-meta`
  - `target/release/databend-query`
  - `target/release/bendsql` (preferred; optionally fallback to PATH)

The script should print a clear error message and exit non-zero if missing.

- [ ] **Step 2: Implement port probing and waiting**

Implement helpers:

```python
import socket
import time


def tcp_is_open(host: str, port: int, timeout_s: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout_s):
            return True
    except OSError:
        return False


def wait_tcp(host: str, port: int, timeout_s: float) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if tcp_is_open(host, port):
            return
        time.sleep(0.2)
    raise TimeoutError(f"Timeout waiting for {host}:{port}")
```

Note: although repo has `scripts/ci/wait_tcp.py`, this avoids requiring cwd in repo; we can still call it by absolute path later if preferred.

- [ ] **Step 3: Implement start/stop**

Stop strategy (macOS):

- Use `pkill -f databend-meta` and `pkill -f databend-query` or `killall` with fallback.
- Record failures but continue (best-effort).

Start strategy:

- Use `subprocess.Popen([...], stdout=logfile, stderr=logfile, cwd=run_dir)`
- Pass `-c <run_dir>/artifacts/config/...toml`
- Wait for TCP ports: meta then query.

- [ ] **Step 4: Add a “dry run” mode guard (optional)**

For early testing, allow `DRY_RUN = True` to skip actually starting services. (If implemented, keep it as an internal constant, not CLI.)

- [ ] **Step 5: Commit**

```bash
git add scripts/benchmark/hits_vortex_standalone_bench.py
git commit -m "feat(benchmark): add release service lifecycle helpers"
```

## Task 3: Config materialization into current directory

**Files:**
- Modify: `scripts/benchmark/hits_vortex_standalone_bench.py`

- [ ] **Step 1: Implement config copy**

Copy templates from:

- `$DATABEND_DIR/scripts/ci/deploy/config/databend-meta-node-1.toml`
- `$DATABEND_DIR/scripts/ci/deploy/config/databend-query-node-1.toml`

To:

- `./artifacts/config/databend-meta-node-1.toml`
- `./artifacts/config/databend-query-node-1.toml`

Use `shutil.copyfile`.

- [ ] **Step 2: Implement minimal TOML rewriting**

Goal: point data dirs + log paths into run directory.

Implementation approach:

- Perform conservative line-based rewriting:
  - replace known default paths with run-local paths
  - or append overrides if configs support include/override (prefer not to assume)

If line-based rewriting is too brittle, switch to “small template overlay”:

- Keep copied template unchanged
- Add a second small config file in `./artifacts/config/override-*.toml`
- Start processes with `-c` pointing to a merged config (only if Databend supports includes/overlays)

Pick the simplest proven approach once validated in a quick manual run.

- [ ] **Step 3: Commit**

```bash
git add scripts/benchmark/hits_vortex_standalone_bench.py
git commit -m "feat(benchmark): materialize standalone configs into run dir"
```

## Task 4: bendsql execution wrapper + DB/table DDL

**Files:**
- Modify: `scripts/benchmark/hits_vortex_standalone_bench.py`

- [ ] **Step 1: Implement bendsql runner**

Implement:

- `run_sql(sql: str) -> (exit_code, stdout, stderr, duration_ms)`
- Use `subprocess.run([bendsql_bin, ...], input=sql, text=True, capture_output=True)`
- Ensure it points to the query service endpoint (host/port) matching config.

- [ ] **Step 2: Implement DDL creation for baseline + vortex**

Embed the hits schema from `scripts/benchmark/query/load/hits.sh` in Python as a triple-quoted SQL string, parameterized by table name and optional `storage_format='vortex'`.

Baseline DDL:

```sql
CREATE TRANSIENT TABLE hits_fuse ( ... ) CLUSTER BY (...);
```

Vortex DDL:

```sql
CREATE TRANSIENT TABLE hits_vortex ( ... ) ENGINE=FUSE storage_format='vortex' CLUSTER BY (...);
```

Note: use the exact Databend-supported syntax (if `ENGINE=FUSE` is implicit in `CREATE TRANSIENT TABLE`, add it explicitly for clarity).

- [ ] **Step 3: Commit**

```bash
git add scripts/benchmark/hits_vortex_standalone_bench.py
git commit -m "feat(benchmark): create hits fuse and vortex tables"
```

## Task 5: Load `hits.tsv.gz` using Databend compressed read capability

**Files:**
- Modify: `scripts/benchmark/hits_vortex_standalone_bench.py`

- [ ] **Step 1: Add a “capability probe” for COPY-from-local gzip**

Add a small probe SQL that attempts the intended `COPY INTO` syntax against a tiny temp table, and if it fails, prints a clear “unsupported syntax” error with hints.

Example preferred syntax to try (adjust once confirmed):

```sql
COPY INTO hits_fuse
FROM 'file:///Users/haoyu/src/databendlabs/hits/hits.tsv.gz'
FILE_FORMAT = (type = TSV field_delimiter = '\t' record_delimiter = '\n' skip_header = 1);
```

- [ ] **Step 2: Implement load + analyze**

After COPY:

```sql
ANALYZE TABLE hits_fuse;
```

Repeat for vortex table.

- [ ] **Step 3: Add failure handling**

If load fails:

- record error
- stop benchmark (this is a hard precondition)

- [ ] **Step 4: Commit**

```bash
git add scripts/benchmark/hits_vortex_standalone_bench.py
git commit -m "feat(benchmark): load hits.tsv.gz and analyze"
```

## Task 6: Run queries and record timings (3 runs per query)

**Files:**
- Modify: `scripts/benchmark/hits_vortex_standalone_bench.py`

- [ ] **Step 1: Implement query execution loop**

For each query file:

- read SQL
- substitute `FROM hits` / `JOIN hits` occurrences to target table (harden replacement to cover more patterns than just `FROM hits`):
  - `FROM hits` → `FROM <table>`
  - `JOIN hits` → `JOIN <table>`
  - `, hits` → `, <table>` (if present)

For run in 1..3:

- execute via bendsql
- record duration, exit status, stdout/stderr snippet

Write to `./artifacts/results/timings.csv` with columns:

- `query,table,run_idx,duration_ms,ok`

Write errors to `./artifacts/results/errors.jsonl`.

- [ ] **Step 2: Commit**

```bash
git add scripts/benchmark/hits_vortex_standalone_bench.py
git commit -m "feat(benchmark): run hits queries and record 3-run timings"
```

## Task 7: Correctness fingerprint (layer 1) and optional diff fallback (layer 2)

**Files:**
- Modify: `scripts/benchmark/hits_vortex_standalone_bench.py`
- Modify: `scripts/benchmark/tests/test_hits_vortex_standalone_bench.py`

- [ ] **Step 1: Identify hash primitives supported by Databend**

Implement a small SQL probe at startup to decide the fingerprint expression.

Target fingerprint should cover duplicates with very low collision probability by using multiple aggregates over a per-row hash. Candidate functions to probe (one-by-one until one works):

- `xxhash64(...)`
- `siphash(...)`
- `city64(...)`
- or a stable `hash(...)` function if available

Row-to-hash strategy:

- prefer hashing a stable serialization: `to_string(tuple(*))` or `to_json(object_construct(*))` if supported.

The probe should store the chosen expressions in variables used to build fingerprint SQL.

- [ ] **Step 2: Implement fingerprint SQL generator**

Fingerprint query template:

```sql
WITH q AS (
  <original query with hits substituted to table>
)
SELECT
  count(*) AS row_count,
  sum(row_h) AS h_sum,
  bit_xor(row_h) AS h_xor,
  sum(row_h * 1315423911) AS h_sum2
FROM (
  SELECT <row_hash_expr> AS row_h
  FROM q
) t
```

If `bit_xor` is not available, fall back to `sum` variants only (less robust but still useful).

- [ ] **Step 3: Compare baseline vs vortex fingerprints per query**

For each query:

- compute fingerprint on baseline table
- compute fingerprint on vortex table
- compare equality; record in `correctness.csv`:
  - `query,status,row_count_baseline,row_count_vortex,h_sum_baseline,...`

If mismatch and `FALLBACK_MATERIALIZE_DIFF_ON_MISMATCH`:

- materialize both result sets to `./artifacts/diffs/Qxx/{baseline,vortex}.tsv`
- produce a Python diff summary (line count, first N differing lines) to `./artifacts/diffs/Qxx/diff.txt`

- [ ] **Step 4: Add/adjust unit tests**

Unit tests validate:

- fingerprint SQL contains expected structure
- substitution handles `JOIN hits`

- [ ] **Step 5: Commit**

```bash
git add scripts/benchmark/hits_vortex_standalone_bench.py scripts/benchmark/tests/test_hits_vortex_standalone_bench.py
git commit -m "feat(benchmark): add layered correctness fingerprints for vortex"
```

## Task 8: Final report (`report.md`) and exit code policy

**Files:**
- Modify: `scripts/benchmark/hits_vortex_standalone_bench.py`

- [ ] **Step 1: Generate `report.md`**

Include:

- Environment summary (git commit hash optional; binaries path; ports; data file path)
- Table load summary (row counts, analyze status)
- Timing table per query with:
  - baseline t1/t2/t3 + hot_avg
  - vortex t1/t2/t3 + hot_avg
  - speedup cold/hot
- Correctness summary:
  - pass count / fail count
  - list failing queries with pointers to artifacts

- [ ] **Step 2: Decide exit code**

Implement:

- return non-zero if any correctness mismatch and `FAIL_ON_ANY_CORRECTNESS_MISMATCH`
- optionally return non-zero if any query execution error and `FAIL_ON_ANY_QUERY_ERROR`
- always non-zero on service start or load failure

- [ ] **Step 3: Commit**

```bash
git add scripts/benchmark/hits_vortex_standalone_bench.py
git commit -m "feat(benchmark): add report and strict exit policy"
```

## Task 9: Manual verification run (evidence before “done”)

This is a validation task; do not “claim success” until these commands complete.

**Prereqs:**

- Build release binaries once:

```bash
cd "$DATABEND_DIR"
cargo build --release -p databend-meta -p databend-query -p bendsql
```

**Run from an arbitrary directory:**

```bash
mkdir -p /tmp/hits-bench-run && cd /tmp/hits-bench-run
export DATABEND_DIR="/Users/haoyu/src/databendlabs/databend"
python3 "$DATABEND_DIR/scripts/benchmark/hits_vortex_standalone_bench.py"
```

Expected artifacts:

- `./artifacts/results/timings.csv`
- `./artifacts/results/correctness.csv`
- `./artifacts/results/report.md`

If failures:

- iterate with fixes; do not stop at first query error; ensure final report lists all failures.

## Plan self-review checklist (run now)

### 1) Spec coverage

- Cold-start meta/query per table suite: Task 2-3
- Release binaries derived from `DATABEND_DIR`: Task 2
- Run anywhere (no cwd assumption): all tasks use absolute paths derived from `DATABEND_DIR` + current run dir
- Standalone config resources copied to current directory: Task 3
- Create DB dir in current directory: Task 3 (data dirs under `./databend-data/`)
- Create two tables: Task 4
- Load `/Users/haoyu/src/databendlabs/hits/hits.tsv.gz` via Databend compressed read: Task 5
- Run queries from `benchmark/hits/queries`: Task 1 + Task 6
- 3 timings per query: Task 6
- Compare timings baseline vs vortex: Task 8 report
- Layered correctness vs baseline: Task 7 (+ optional diff)
- Do not stop on single query error; collect and report: Task 6 + Task 8

### 2) Placeholder scan

- No “TBD/TODO” steps remain; only “probe” decisions have concrete commands and fallback behavior.

### 3) Type/identifier consistency

- Script name consistent: `hits_vortex_standalone_bench.py`
- Env var consistent: `DATABEND_DIR`
- Tables consistent: `hits_fuse`, `hits_vortex`

