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


def run_sql(bendsql_bin: str, sql: str, host: str = "127.0.0.1", port: int = QUERY_PORT) -> Tuple[int, str, str, int]:
    start = time.monotonic()
    # bendsql flags may evolve; keep it minimal and rely on defaults where possible.
    proc = subprocess.run(
        [bendsql_bin, "--host", host, "--port", str(port)],
        input=sql,
        text=True,
        capture_output=True,
    )
    dur_ms = int((time.monotonic() - start) * 1000)
    return proc.returncode, proc.stdout, proc.stderr, dur_ms


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
    opts = ""
    if storage_format:
        opts = f" ENGINE=FUSE storage_format='{storage_format}'"
    return f"CREATE TRANSIENT TABLE {table} {hits_schema_columns_sql()} {opts} {hits_cluster_by_sql()};"


def copy_into_hits_sql(table: str, gz_path: str) -> str:
    # Prefer Databend reading gzip directly from local file URL.
    # If this fails in practice, the script will surface the server-side error and stop.
    url = Path(gz_path).expanduser().resolve().as_uri()
    return (
        f"COPY INTO {table} FROM '{url}' "
        "FILE_FORMAT=(type=TSV field_delimiter='\\t' record_delimiter='\\n' skip_header=1);"
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
    code, out, err, _ = run_sql(bendsql_bin, sql, host=host, port=port)
    if code != 0:
        raise RuntimeError(
            "COPY INTO failed for local .tsv.gz.\n"
            f"SQL: {sql}\n"
            f"stdout:\n{out}\n"
            f"stderr:\n{err}\n"
        )

    code, out, err, _ = run_sql(bendsql_bin, analyze_table_sql(table), host=host, port=port)
    if code != 0:
        raise RuntimeError(f"ANALYZE TABLE failed for {table}\nstdout:\n{out}\nstderr:\n{err}\n")


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
            code, out, err, dur_ms = run_sql(bendsql_bin, sql, host=host, port=port)
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


if __name__ == "__main__":
    sys.stderr.write("hits_vortex_standalone_bench.py: partial implementation (Task 2 in progress)\n")
    raise SystemExit(2)

