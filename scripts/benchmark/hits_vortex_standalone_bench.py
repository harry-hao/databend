#
# export DATABEND_DIR="/Users/haoyu/src/databendlabs/databend" && python3 "$DATABEND_DIR/scripts/benchmark/hits_vortex_standalone_bench.py"
#

import os
import csv
import json
import re
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Tuple


DEFAULT_DATABEND_DIR_ENV = "DATABEND_DIR"

META_PORT = 9191
QUERY_PORT = 8000

META_TOML_TEMPLATE = "scripts/ci/deploy/config/databend-meta-node-1.toml"
QUERY_TOML_TEMPLATE = "scripts/ci/deploy/config/databend-query-node-1.toml"

QUERIES_DIR = "benchmark/hits/queries"

# Keep as script variable; empty means "discover all".
RUN_QUERIES: List[str] = []

HITS_TSV_GZ = "/Users/haoyu/src/databendlabs/hits/hits.tsv.gz"

DB = "hits_bench"
TABLE_BASELINE = "hits_fuse"
TABLE_VORTEX = "hits_vortex"

# Benchmark default: restart per table suite to get cold process state.
COLD_START_EACH_TABLE = True

FAIL_ON_ANY_CORRECTNESS_MISMATCH = True
FAIL_ON_ANY_QUERY_ERROR = False

# Run order: user preference is to run vortex first.
RUN_VORTEX_FIRST = True


def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def materialize_standalone_configs(databend_dir: str, run_dir: str) -> Tuple[str, str]:
    cfg_dir = Path(run_dir) / "artifacts" / "config"
    ensure_dir(str(cfg_dir))

    meta_src = Path(databend_dir) / META_TOML_TEMPLATE
    query_src = Path(databend_dir) / QUERY_TOML_TEMPLATE

    meta_dst = cfg_dir / "databend-meta-node-1.toml"
    query_dst = cfg_dir / "databend-query-node-1.toml"

    shutil.copyfile(meta_src, meta_dst)
    shutil.copyfile(query_src, query_dst)

    # Minimal, conservative rewrites to keep all state inside run_dir.
    meta_txt = meta_dst.read_text(encoding="utf-8")
    meta_txt = meta_txt.replace('dir = "./.databend/logs1"', 'dir = "./artifacts/logs/meta"')
    meta_txt = meta_txt.replace('raft_dir      = "./.databend/meta1"', 'raft_dir      = "./databend-data/meta1"')
    meta_dst.write_text(meta_txt, encoding="utf-8")

    query_txt = query_dst.read_text(encoding="utf-8")
    query_txt = query_txt.replace('dir = "./.databend/logs_1"', 'dir = "./artifacts/logs/query"')
    query_txt = query_txt.replace('dir = "./.databend/structlog_1"', 'dir = "./artifacts/logs/structlog"')
    query_txt = query_txt.replace('data_path = "./.databend/stateless_test_data"', 'data_path = "./databend-data/query_storage"')
    query_dst.write_text(query_txt, encoding="utf-8")

    return str(meta_dst), str(query_dst)


def discover_query_files(paths: List[str]) -> List[str]:
    return [p for p in paths if p.endswith(".sql")]


def discover_query_dir(dir_path: str) -> List[str]:
    return sorted(str(p) for p in Path(dir_path).glob("*.sql"))


def resolve_query_files(databend_dir: str) -> List[str]:
    qdir = Path(databend_dir) / QUERIES_DIR
    if not qdir.exists():
        raise RuntimeError(f"Queries dir not found: {qdir}")
    if not RUN_QUERIES:
        return discover_query_dir(str(qdir))
    files: List[str] = []
    for stem in RUN_QUERIES:
        p = qdir / f"{stem}.sql"
        if not p.exists():
            raise RuntimeError(f"Query file not found: {p}")
        files.append(str(p))
    return files


def query_short_name(filename: str) -> str:
    base = os.path.basename(filename)
    stem = base[:-4] if base.endswith(".sql") else base
    return f"Q{stem}"


def load_sql_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def substitute_hits_table(sql: str, table: str) -> str:
    # Replace simple occurrences used by benchmark/hits queries.
    # Keep it intentionally conservative to avoid rewriting string literals.
    out = sql
    out = re.sub(r"\bFROM\s+hits\b", f"FROM {table}", out, flags=re.IGNORECASE)
    out = re.sub(r"\bJOIN\s+hits\b", f"JOIN {table}", out, flags=re.IGNORECASE)
    out = re.sub(r",\s*hits\b", f", {table}", out, flags=re.IGNORECASE)
    return out


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


def get_databend_dir() -> str:
    v = os.environ.get(DEFAULT_DATABEND_DIR_ENV)
    if not v:
        raise RuntimeError(f"Missing env {DEFAULT_DATABEND_DIR_ENV}=<path to databend repo>")
    p = Path(v).expanduser().resolve()
    if not p.exists():
        raise RuntimeError(f"{DEFAULT_DATABEND_DIR_ENV} path does not exist: {p}")
    return str(p)


def resolve_release_bin(databend_dir: str, rel: str) -> str:
    p = Path(databend_dir) / rel
    if not p.exists():
        raise RuntimeError(f"Missing release binary: {p}")
    if not os.access(str(p), os.X_OK):
        raise RuntimeError(f"Release binary is not executable: {p}")
    return str(p)


def resolve_release_bins(databend_dir: str) -> Tuple[str, str, str]:
    meta = resolve_release_bin(databend_dir, "target/release/databend-meta")
    query = resolve_release_bin(databend_dir, "target/release/databend-query")
    bendsql = Path(databend_dir) / "target/release/bendsql"
    if bendsql.exists() and os.access(str(bendsql), os.X_OK):
        return meta, query, str(bendsql)
    bendsql_path = shutil.which("bendsql")
    if bendsql_path:
        return meta, query, bendsql_path
    raise RuntimeError(f"Missing bendsql: {Path(databend_dir) / 'target/release/bendsql'} (and not in PATH)")


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


def stop_services_best_effort() -> None:
    for cmd in (
        ["pkill", "-f", "databend-query"],
        ["pkill", "-f", "databend-meta"],
        ["killall", "databend-query"],
        ["killall", "databend-meta"],
    ):
        try:
            subprocess.run(cmd, check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except FileNotFoundError:
            continue
    time.sleep(1.0)


def open_log(run_dir: str, name: str):
    p = Path(run_dir) / "artifacts" / "logs" / name
    ensure_dir(str(p.parent))
    return open(p, "ab", buffering=0)


def start_services(
    meta_bin: str,
    query_bin: str,
    meta_cfg: str,
    query_cfg: str,
    run_dir: str,
    cold_start: bool,
) -> Tuple[subprocess.Popen, subprocess.Popen]:
    if cold_start:
        stop_services_best_effort()
    else:
        # start only if ports not open
        if tcp_is_open("127.0.0.1", META_PORT) and tcp_is_open("127.0.0.1", QUERY_PORT):
            raise RuntimeError("Services already running; start-if-not-running mode not implemented in this revision")

    meta_log = open_log(run_dir, "databend-meta.log")
    query_log = open_log(run_dir, "databend-query.log")

    meta_p = subprocess.Popen(
        [meta_bin, "-c", meta_cfg],
        cwd=run_dir,
        stdout=meta_log,
        stderr=meta_log,
    )
    wait_tcp("127.0.0.1", META_PORT, timeout_s=60)

    query_p = subprocess.Popen(
        [query_bin, "-c", query_cfg, "--internal-enable-sandbox-tenant"],
        cwd=run_dir,
        stdout=query_log,
        stderr=query_log,
    )
    wait_tcp("127.0.0.1", QUERY_PORT, timeout_s=60)

    return meta_p, query_p


def run_sql(
    bendsql_bin: str,
    sql: str,
    host: str = "127.0.0.1",
    port: int = QUERY_PORT,
    output: str = "null",
) -> Tuple[int, str, str, int]:
    start = time.monotonic()
    # bendsql executes SQL via --query (stdin/-d is for loading data, not SQL execution).
    proc = subprocess.run(
        [
            bendsql_bin,
            "-n",
            "--host",
            host,
            "--port",
            str(port),
            "-o",
            output,
            "--quote-style",
            "never",
            f"--query={sql}",
        ],
        text=True,
        capture_output=True,
    )
    dur_ms = int((time.monotonic() - start) * 1000)
    return proc.returncode, proc.stdout, proc.stderr, dur_ms


def run_sql_in_db(
    bendsql_bin: str,
    sql: str,
    host: str = "127.0.0.1",
    port: int = QUERY_PORT,
    output: str = "null",
) -> Tuple[int, str, str, int]:
    start = time.monotonic()
    proc = subprocess.run(
        [
            bendsql_bin,
            "-n",
            "--host",
            host,
            "--port",
            str(port),
            "-D",
            DB,
            "-o",
            output,
            "--quote-style",
            "never",
            f"--query={sql}",
        ],
        text=True,
        capture_output=True,
    )
    dur_ms = int((time.monotonic() - start) * 1000)
    return proc.returncode, proc.stdout, proc.stderr, dur_ms


def parse_tsv_table(text: str) -> List[List[str]]:
    lines = [ln for ln in text.splitlines() if ln.strip() != ""]
    return [ln.split("\t") for ln in lines]


def run_sql_tsv_single_row(
    bendsql_bin: str,
    sql: str,
    host: str = "127.0.0.1",
    port: int = QUERY_PORT,
) -> List[str]:
    code, out, err, _ = run_sql_in_db(bendsql_bin, sql, host=host, port=port, output="tsv")
    if code != 0:
        raise RuntimeError(f"SQL failed:\n{sql}\nstdout:\n{out}\nstderr:\n{err}\n")
    rows = parse_tsv_table(out)
    if len(rows) < 2:
        raise RuntimeError(f"Expected TSV with header+row, got:\n{out}")
    # first line is header
    return rows[1]


def hits_schema_columns_sql() -> str:
    # From scripts/benchmark/query/load/hits.sh
    return """(
    WatchID BIGINT NOT NULL,
    JavaEnable SMALLINT NOT NULL,
    Title TEXT NOT NULL,
    GoodEvent SMALLINT NOT NULL,
    EventTime TIMESTAMP NOT NULL,
    EventDate Date NOT NULL,
    CounterID INTEGER NOT NULL,
    ClientIP INTEGER NOT NULL,
    RegionID INTEGER NOT NULL,
    UserID BIGINT NOT NULL,
    CounterClass SMALLINT NOT NULL,
    OS SMALLINT NOT NULL,
    UserAgent SMALLINT NOT NULL,
    URL TEXT NOT NULL,
    Referer TEXT NOT NULL,
    IsRefresh SMALLINT NOT NULL,
    RefererCategoryID SMALLINT NOT NULL,
    RefererRegionID INTEGER NOT NULL,
    URLCategoryID SMALLINT NOT NULL,
    URLRegionID INTEGER NOT NULL,
    ResolutionWidth SMALLINT NOT NULL,
    ResolutionHeight SMALLINT NOT NULL,
    ResolutionDepth SMALLINT NOT NULL,
    FlashMajor SMALLINT NOT NULL,
    FlashMinor SMALLINT NOT NULL,
    FlashMinor2 TEXT NOT NULL,
    NetMajor SMALLINT NOT NULL,
    NetMinor SMALLINT NOT NULL,
    UserAgentMajor SMALLINT NOT NULL,
    UserAgentMinor VARCHAR(255) NOT NULL,
    CookieEnable SMALLINT NOT NULL,
    JavascriptEnable SMALLINT NOT NULL,
    IsMobile SMALLINT NOT NULL,
    MobilePhone SMALLINT NOT NULL,
    MobilePhoneModel TEXT NOT NULL,
    Params TEXT NOT NULL,
    IPNetworkID INTEGER NOT NULL,
    TraficSourceID SMALLINT NOT NULL,
    SearchEngineID SMALLINT NOT NULL,
    SearchPhrase TEXT NOT NULL,
    AdvEngineID SMALLINT NOT NULL,
    IsArtifical SMALLINT NOT NULL,
    WindowClientWidth SMALLINT NOT NULL,
    WindowClientHeight SMALLINT NOT NULL,
    ClientTimeZone SMALLINT NOT NULL,
    ClientEventTime TIMESTAMP NOT NULL,
    SilverlightVersion1 SMALLINT NOT NULL,
    SilverlightVersion2 SMALLINT NOT NULL,
    SilverlightVersion3 INTEGER NOT NULL,
    SilverlightVersion4 SMALLINT NOT NULL,
    PageCharset TEXT NOT NULL,
    CodeVersion INTEGER NOT NULL,
    IsLink SMALLINT NOT NULL,
    IsDownload SMALLINT NOT NULL,
    IsNotBounce SMALLINT NOT NULL,
    FUniqID BIGINT NOT NULL,
    OriginalURL TEXT NOT NULL,
    HID INTEGER NOT NULL,
    IsOldCounter SMALLINT NOT NULL,
    IsEvent SMALLINT NOT NULL,
    IsParameter SMALLINT NOT NULL,
    DontCountHits SMALLINT NOT NULL,
    WithHash SMALLINT NOT NULL,
    HitColor CHAR NOT NULL,
    LocalEventTime TIMESTAMP NOT NULL,
    Age SMALLINT NOT NULL,
    Sex SMALLINT NOT NULL,
    Income SMALLINT NOT NULL,
    Interests SMALLINT NOT NULL,
    Robotness SMALLINT NOT NULL,
    RemoteIP INTEGER NOT NULL,
    WindowName INTEGER NOT NULL,
    OpenerName INTEGER NOT NULL,
    HistoryLength SMALLINT NOT NULL,
    BrowserLanguage TEXT NOT NULL,
    BrowserCountry TEXT NOT NULL,
    SocialNetwork TEXT NOT NULL,
    SocialAction TEXT NOT NULL,
    HTTPError SMALLINT NOT NULL,
    SendTiming INTEGER NOT NULL,
    DNSTiming INTEGER NOT NULL,
    ConnectTiming INTEGER NOT NULL,
    ResponseStartTiming INTEGER NOT NULL,
    ResponseEndTiming INTEGER NOT NULL,
    FetchTiming INTEGER NOT NULL,
    SocialSourceNetworkID SMALLINT NOT NULL,
    SocialSourcePage TEXT NOT NULL,
    ParamPrice BIGINT NOT NULL,
    ParamOrderID TEXT NOT NULL,
    ParamCurrency TEXT NOT NULL,
    ParamCurrencyID SMALLINT NOT NULL,
    OpenstatServiceName TEXT NOT NULL,
    OpenstatCampaignID TEXT NOT NULL,
    OpenstatAdID TEXT NOT NULL,
    OpenstatSourceID TEXT NOT NULL,
    UTMSource TEXT NOT NULL,
    UTMMedium TEXT NOT NULL,
    UTMCampaign TEXT NOT NULL,
    UTMContent TEXT NOT NULL,
    UTMTerm TEXT NOT NULL,
    FromTag TEXT NOT NULL,
    HasGCLID SMALLINT NOT NULL,
    RefererHash BIGINT NOT NULL,
    URLHash BIGINT NOT NULL,
    CLID INTEGER NOT NULL
  )"""


def hits_cluster_by_sql() -> str:
    return "CLUSTER BY (CounterID, EventDate, UserID, EventTime, WatchID)"


def create_hits_table_sql(table: str, storage_format: str = "") -> str:
    # Databend's CREATE TABLE grammar is sensitive to option ordering.
    # Use: (...) CLUSTER BY (...) ENGINE=FUSE storage_format='vortex'
    cluster = hits_cluster_by_sql()
    if storage_format:
        return (
            f"CREATE TRANSIENT TABLE {table} {hits_schema_columns_sql()} "
            f"{cluster} ENGINE=FUSE storage_format='{storage_format}';"
        )
    return f"CREATE TRANSIENT TABLE {table} {hits_schema_columns_sql()} {cluster};"


def copy_into_hits_sql(table: str, gz_path: str) -> str:
    # Prefer Databend reading gzip directly from local file URL.
    # If this fails in practice, the script will surface the server-side error and stop.
    # Databend COPY supports local filesystem URIs via fs:///abs/path (not file://).
    p = Path(gz_path).expanduser().resolve()
    # Normalize to exactly fs:///abs/path
    url = f"fs:///{p.as_posix().lstrip('/')}"
    return (
        f"COPY INTO {table} FROM '{url}' "
        "FILE_FORMAT=(type=TSV compression=GZIP field_delimiter='\\t' record_delimiter='\\n' skip_header=1);"
    )


def analyze_table_sql(table: str) -> str:
    return f"ANALYZE TABLE {table};"


def count_table_sql(table: str) -> str:
    return f"SELECT count(*) AS c FROM {table};"


def load_hits_table(
    bendsql_bin: str,
    table: str,
    gz_path: str,
    host: str = "127.0.0.1",
    port: int = QUERY_PORT,
) -> None:
    # Capability probe: attempt a tiny load into target table (real load); fail fast with clear error.
    sql = copy_into_hits_sql(table, gz_path)
    code, out, err, _ = run_sql_in_db(bendsql_bin, sql, host=host, port=port, output="null")
    if code != 0:
        raise RuntimeError(
            "COPY INTO failed for local .tsv.gz.\n"
            f"SQL: {sql}\n"
            f"stdout:\n{out}\n"
            f"stderr:\n{err}\n"
        )

    code, out, err, _ = run_sql_in_db(bendsql_bin, analyze_table_sql(table), host=host, port=port, output="null")
    if code != 0:
        raise RuntimeError(f"ANALYZE TABLE failed for {table}\nstdout:\n{out}\nstderr:\n{err}\n")


def init_database(bendsql_bin: str) -> None:
    # Create DB via default database connection first (DB may not exist yet).
    code, out, err, _ = run_sql(bendsql_bin, f"CREATE DATABASE IF NOT EXISTS {DB};", output="null")
    if code != 0:
        raise RuntimeError(f"CREATE DATABASE failed\nstdout:\n{out}\nstderr:\n{err}\n")


def reset_table(bendsql_bin: str, table: str) -> None:
    code, out, err, _ = run_sql_in_db(bendsql_bin, f"DROP TABLE IF EXISTS {table} ALL;", output="null")
    if code != 0:
        raise RuntimeError(f"DROP TABLE failed for {table}\nstdout:\n{out}\nstderr:\n{err}\n")


def create_table_and_load(
    bendsql_bin: str,
    table: str,
    storage_format: str,
    gz_path: str,
) -> None:
    reset_table(bendsql_bin, table)
    ddl = create_hits_table_sql(table, storage_format=storage_format)
    code, out, err, _ = run_sql_in_db(bendsql_bin, ddl, output="null")
    if code != 0:
        raise RuntimeError(f"CREATE TABLE failed for {table}\nSQL:\n{ddl}\nstdout:\n{out}\nstderr:\n{err}\n")
    load_hits_table(bendsql_bin, table, gz_path)


def probe_vortex_support(bendsql_bin: str) -> None:
    # Capability probe: some server builds may not recognize storage_format='vortex'.
    code, out, err, _ = run_sql_in_db(
        bendsql_bin,
        "CREATE TABLE IF NOT EXISTS __vortex_probe(a INT) ENGINE=FUSE storage_format='vortex';",
        output="null",
    )
    if code != 0:
        if "unknown fuse storage_format" in err.lower():
            raise RuntimeError(
                "Server does not support Fuse storage_format='vortex'.\n"
                "Please rebuild / run a databend-query version that includes vortex storage format support,\n"
                "or ensure the benchmark script starts the intended release binaries.\n"
                f"stderr:\n{err}\n"
            )
        raise RuntimeError(f"Vortex capability probe failed.\nstdout:\n{out}\nstderr:\n{err}\n")
    # best-effort cleanup
    run_sql_in_db(bendsql_bin, "DROP TABLE IF EXISTS __vortex_probe ALL;", output="null")


def open_errors_jsonl(run_dir: str):
    p = Path(run_dir) / "artifacts" / "results" / "errors.jsonl"
    ensure_dir(str(p.parent))
    return open(p, "a", encoding="utf-8")


def append_error_jsonl(fp, obj: dict) -> None:
    fp.write(json.dumps(obj, ensure_ascii=False) + "\n")
    fp.flush()


def append_timing_csv_row(csv_path: str, row: dict) -> None:
    ensure_dir(str(Path(csv_path).parent))
    exists = Path(csv_path).exists()
    with open(csv_path, "a", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            w.writeheader()
        w.writerow(row)


def run_query_3_times(
    bendsql_bin: str,
    query_name: str,
    sql: str,
    table: str,
    run_dir: str,
    host: str = "127.0.0.1",
    port: int = QUERY_PORT,
) -> None:
    timings_csv = str(Path(run_dir) / "artifacts" / "results" / "timings.csv")
    with open_errors_jsonl(run_dir) as errfp:
        for run_idx in (1, 2, 3):
            code, out, err, dur_ms = run_sql_in_db(bendsql_bin, sql, host=host, port=port, output="null")
            ok = code == 0
            append_timing_csv_row(
                timings_csv,
                {
                    "query": query_name,
                    "table": table,
                    "run_idx": run_idx,
                    "duration_ms": dur_ms,
                    "ok": int(ok),
                },
            )
            if not ok:
                append_error_jsonl(
                    errfp,
                    {
                        "kind": "query_error",
                        "query": query_name,
                        "table": table,
                        "run_idx": run_idx,
                        "exit_code": code,
                        "stderr": err[-4000:],
                        "stdout": out[-4000:],
                    },
                )


def run_queries_for_table(
    bendsql_bin: str,
    query_files: List[str],
    table: str,
    run_dir: str,
) -> None:
    for qf in query_files:
        name = query_short_name(qf)
        sql = load_sql_file(qf)
        sql = substitute_hits_table(sql, table)
        run_query_3_times(bendsql_bin, name, sql, table, run_dir)


def fingerprint_sql(sql: str, table: str) -> str:
    q = substitute_hits_table(sql, table).rstrip().rstrip(";")
    return (
        "WITH q AS (\n"
        f"{q}\n"
        ")\n"
        "SELECT\n"
        "  count(*) AS row_count,\n"
        "  sum(row_h) AS h_sum,\n"
        "  sum(row_h * 1315423911) AS h_sum2,\n"
        "  min(row_h) AS h_min,\n"
        "  max(row_h) AS h_max\n"
        "FROM (\n"
        "  SELECT xxhash64(*) AS row_h\n"
        "  FROM q\n"
        ") t"
    )


def fingerprint_query(
    bendsql_bin: str,
    query_name: str,
    sql: str,
    table: str,
    run_dir: str,
    host: str = "127.0.0.1",
    port: int = QUERY_PORT,
) -> dict:
    fp_sql = fingerprint_sql(sql, table)
    try:
        row = run_sql_tsv_single_row(bendsql_bin, fp_sql, host=host, port=port)
        # columns fixed by fingerprint_sql order
        return {
            "query": query_name,
            "table": table,
            "row_count": row[0],
            "h_sum": row[1],
            "h_sum2": row[2],
            "h_min": row[3],
            "h_max": row[4],
        }
    except Exception as e:
        with open_errors_jsonl(run_dir) as errfp:
            append_error_jsonl(
                errfp,
                {
                    "kind": "fingerprint_error",
                    "query": query_name,
                    "table": table,
                    "error": str(e),
                },
            )
        return {
            "query": query_name,
            "table": table,
            "row_count": "",
            "h_sum": "",
            "h_sum2": "",
            "h_min": "",
            "h_max": "",
        }


def write_correctness_csv(run_dir: str, rows: List[dict]) -> str:
    path = Path(run_dir) / "artifacts" / "results" / "correctness.csv"
    ensure_dir(str(path.parent))
    if not rows:
        path.write_text("", encoding="utf-8")
        return str(path)
    fieldnames = list(rows[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return str(path)


def compare_fingerprints(
    baseline: dict,
    vortex: dict,
) -> bool:
    keys = ["row_count", "h_sum", "h_sum2", "h_min", "h_max"]
    return all(str(baseline.get(k, "")) == str(vortex.get(k, "")) for k in keys)


def write_report_md(run_dir: str, mismatches: List[str]) -> str:
    p = Path(run_dir) / "artifacts" / "results" / "report.md"
    ensure_dir(str(p.parent))
    lines: List[str] = []
    lines.append(f"## Hits Fuse vs Vortex benchmark report\n")
    lines.append(f"- **DB**: `{DB}`")
    lines.append(f"- **Baseline table**: `{TABLE_BASELINE}`")
    lines.append(f"- **Vortex table**: `{TABLE_VORTEX}`")
    lines.append(f"- **Data**: `{HITS_TSV_GZ}`")
    lines.append("")
    lines.append("### Artifacts")
    lines.append(f"- timings: `artifacts/results/timings.csv`")
    lines.append(f"- correctness: `artifacts/results/correctness.csv`")
    lines.append(f"- errors: `artifacts/results/errors.jsonl`")
    lines.append("")
    lines.append("### Correctness summary")
    if mismatches:
        lines.append(f"- **FAIL**: {len(mismatches)} mismatched queries")
        for q in mismatches:
            lines.append(f"  - `{q}`")
    else:
        lines.append("- **PASS**: all queries fingerprints match")
    lines.append("")
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(p)


def main() -> int:
    run_dir = os.getcwd()
    databend_dir = get_databend_dir()
    meta_bin, query_bin, bendsql_bin = resolve_release_bins(databend_dir)

    query_files = resolve_query_files(databend_dir)
    sys.stderr.write(f"Running in: {run_dir}\n")
    sys.stderr.write(f"Using DATABEND_DIR: {databend_dir}\n")
    sys.stderr.write(f"Queries: {len(query_files)}\n")

    ensure_dir(str(Path(run_dir) / "artifacts" / "results"))
    ensure_dir(str(Path(run_dir) / "databend-data"))

    meta_cfg, query_cfg = materialize_standalone_configs(databend_dir, run_dir)

    # Cold-start + load baseline
    if RUN_VORTEX_FIRST:
        sys.stderr.write("Starting services (vortex)...\n")
        start_services(meta_bin, query_bin, meta_cfg, query_cfg, run_dir, cold_start=True)
        sys.stderr.write("Creating database...\n")
        init_database(bendsql_bin)
        probe_vortex_support(bendsql_bin)
        sys.stderr.write(f"Creating+loading {TABLE_VORTEX}...\n")
        create_table_and_load(bendsql_bin, TABLE_VORTEX, storage_format="vortex", gz_path=HITS_TSV_GZ)
        sys.stderr.write(f"Running queries on {TABLE_VORTEX}...\n")
        run_queries_for_table(bendsql_bin, query_files, TABLE_VORTEX, run_dir)

        if COLD_START_EACH_TABLE:
            sys.stderr.write("Restarting services (baseline)...\n")
            start_services(meta_bin, query_bin, meta_cfg, query_cfg, run_dir, cold_start=True)
            sys.stderr.write("Creating database...\n")
            init_database(bendsql_bin)
        sys.stderr.write(f"Creating+loading {TABLE_BASELINE}...\n")
        create_table_and_load(bendsql_bin, TABLE_BASELINE, storage_format="", gz_path=HITS_TSV_GZ)
        sys.stderr.write(f"Running queries on {TABLE_BASELINE}...\n")
        run_queries_for_table(bendsql_bin, query_files, TABLE_BASELINE, run_dir)
    else:
        sys.stderr.write("Starting services (baseline)...\n")
        start_services(meta_bin, query_bin, meta_cfg, query_cfg, run_dir, cold_start=True)
        sys.stderr.write("Creating database...\n")
        init_database(bendsql_bin)
        sys.stderr.write(f"Creating+loading {TABLE_BASELINE}...\n")
        create_table_and_load(bendsql_bin, TABLE_BASELINE, storage_format="", gz_path=HITS_TSV_GZ)
        sys.stderr.write(f"Running queries on {TABLE_BASELINE}...\n")
        run_queries_for_table(bendsql_bin, query_files, TABLE_BASELINE, run_dir)

        if COLD_START_EACH_TABLE:
            sys.stderr.write("Restarting services (vortex)...\n")
            start_services(meta_bin, query_bin, meta_cfg, query_cfg, run_dir, cold_start=True)
            sys.stderr.write("Creating database...\n")
            init_database(bendsql_bin)
        probe_vortex_support(bendsql_bin)
        sys.stderr.write(f"Creating+loading {TABLE_VORTEX}...\n")
        create_table_and_load(bendsql_bin, TABLE_VORTEX, storage_format="vortex", gz_path=HITS_TSV_GZ)
        sys.stderr.write(f"Running queries on {TABLE_VORTEX}...\n")
        run_queries_for_table(bendsql_bin, query_files, TABLE_VORTEX, run_dir)

    # Correctness fingerprints: baseline vs vortex
    sys.stderr.write("Computing correctness fingerprints...\n")
    correctness_rows: List[dict] = []
    mismatched: List[str] = []
    for qf in query_files:
        qname = query_short_name(qf)
        sql = load_sql_file(qf)
        base_fp = fingerprint_query(bendsql_bin, qname, sql, TABLE_BASELINE, run_dir)
        vortex_fp = fingerprint_query(bendsql_bin, qname, sql, TABLE_VORTEX, run_dir)
        ok = compare_fingerprints(base_fp, vortex_fp) and base_fp["row_count"] != ""
        if not ok:
            mismatched.append(qname)
        correctness_rows.append(
            {
                "query": qname,
                "status": "PASS" if ok else "FAIL",
                "baseline_row_count": base_fp["row_count"],
                "vortex_row_count": vortex_fp["row_count"],
                "baseline_h_sum": base_fp["h_sum"],
                "vortex_h_sum": vortex_fp["h_sum"],
                "baseline_h_sum2": base_fp["h_sum2"],
                "vortex_h_sum2": vortex_fp["h_sum2"],
                "baseline_h_min": base_fp["h_min"],
                "vortex_h_min": vortex_fp["h_min"],
                "baseline_h_max": base_fp["h_max"],
                "vortex_h_max": vortex_fp["h_max"],
            }
        )

    write_correctness_csv(run_dir, correctness_rows)
    write_report_md(run_dir, mismatched)

    if mismatched and FAIL_ON_ANY_CORRECTNESS_MISMATCH:
        return 3
    # query errors are recorded in errors.jsonl; exit policy may be tightened later.
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        sys.stderr.write(f"fatal: {e}\n")
        raise SystemExit(1)

