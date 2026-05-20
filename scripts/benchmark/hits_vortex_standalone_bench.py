#
# export DATABEND_DIR="/Users/haoyu/src/databendlabs/databend" && python3 "$DATABEND_DIR/scripts/benchmark/hits_vortex_standalone_bench.py"
#

import os
import argparse
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

META_TOML_TEXT = """# Usage:
# databend-meta -c databend-meta-node-1.toml

admin_api_address       = "0.0.0.0:28101"
grpc_api_address        = "0.0.0.0:9191"
# databend-query fetch this address to update its databend-meta endpoints list,
# in case databend-meta cluster changes.
grpc_api_advertise_host = "127.0.0.1"

[log]
[log.stderr]
  on = false
[log.file]
  on = true
  level = "INFO"
  format = "json"
  dir = "./artifacts/logs/meta"

[raft_config]
id            = 1
raft_dir      = "./databend-data/meta1"
raft_api_port = 28103

# Assign raft_{listen|advertise}_host in test config.
# This allows you to catch a bug in unit tests when something goes wrong in raft meta nodes communication.
raft_listen_host = "127.0.0.1"
raft_advertise_host = "localhost"

# Start up mode: single node cluster
single        = true
"""

QUERY_TOML_TEXT = """# Usage:
# databend-query -c databend_query_config_spec.toml

[query]
max_active_sessions = 256
shutdown_wait_timeout_ms = 5000

# For flight rpc.
flight_api_address = "0.0.0.0:9091"

# Databend Query http address.
# For admin RESET API.
admin_api_address = "0.0.0.0:8080"

# Databend Query metrics RESET API.
metric_api_address = "0.0.0.0:7070"

# Databend Query MySQL Handler.
mysql_handler_host = "0.0.0.0"
mysql_handler_port = 3307

# Databend Query ClickHouse Handler.
clickhouse_http_handler_host = "0.0.0.0"
clickhouse_http_handler_port = 8124

# Databend Query HTTP Handler.
http_handler_host = "0.0.0.0"
http_handler_port = 8000
# mainly for test/debug
# http_session_timeout_secs = 90
http_handler_result_timeout_secs = 60

# Databend Query FlightSQL Handler.
flight_sql_handler_host = "0.0.0.0"
flight_sql_handler_port = 8900

tenant_id = "test_tenant"
cluster_id = "test_cluster"
warehouse_id = "test_warehouse"

table_engine_memory_enabled = true
default_storage_format = 'parquet'
default_compression = 'zstd'

enable_udf_server = true
udf_server_allow_list = ['http://0.0.0.0:8815']
udf_server_allow_insecure = true

cloud_control_grpc_server_address = "http://0.0.0.0:50051"

# network_policy_whitelist = ['127.0.0.0/8']

[[query.users]]
name = "root"
auth_type = "no_password"

[[query.users]]
name = "default"
auth_type = "no_password"

# This for test
[[query.udfs]]
name = "ping"
definition = "CREATE FUNCTION ping(STRING) RETURNS STRING LANGUAGE python HANDLER = 'ping' ADDRESS = 'http://0.0.0.0:8815'"

[query.settings]
aggregate_spilling_memory_ratio = 60
join_spilling_memory_ratio = 60

[log]

[log.file]
level = "DEBUG"
format = "text"
dir = "./artifacts/logs/query"
limit = 12 # 12 files, 1 file per hour

[log.query]
on = true

[log.profile]
on = true

[log.structlog]
on = true
dir = "./artifacts/logs/structlog"

[meta]
# It is a list of `grpc_api_advertise_host:<grpc-api-port>` of databend-meta config
endpoints = ["0.0.0.0:9191"]
username = "root"
password = "root"
client_timeout_in_second = 60
auto_sync_interval = 60

# Storage config.
[storage]
# fs | s3 | azblob | obs | oss
type = "fs"
allow_insecure = true

# Limit OpenDAL concurrent IO requests to avoid EMFILE.
storage_max_concurrent_io_requests = 128

# Set a local folder to store your data.
# Comment out this block if you're NOT using local file system as storage.
[storage.fs]
data_path = "./databend-data/query_storage"

# Cache config.
[cache]
# Type of storage to keep the table data cache
#
# available options: [none|disk]
# default is "none", which disable table data cache
# use "disk" to enabled disk cache
data_cache_storage = "none"

[cache.disk]
# cache path
path = "./.databend/_cache"
# max bytes of cached data 20G
max_bytes = 21474836480

[spill]
spill_local_disk_path = "./.databend/temp/_query_spill"
# Cap local spill to 5GB so window spills keep ~1GB quota with default 20% ratio.
spill_local_disk_max_bytes = 1073741824
window_partition_spilling_disk_quota_ratio = 20

[settings]
vortex_remain_pushdown_max_selected_ratio = 50
"""

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
DEFAULT_START_QUERY_INDEX = 0

def ensure_dir(path: str) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)


def clean_previous_run_data(run_dir: str) -> None:
    """
    Remove data produced by previous runs under the current run_dir.
    This keeps the benchmark repeatable and avoids mixing artifacts across runs.
    """
    # Stop any leftover services that may still hold files open.
    stop_services_best_effort()

    for rel in ("artifacts"):
        p = Path(run_dir) / rel
        if p.exists():
            shutil.rmtree(p, ignore_errors=True)


def backup_previous_results(run_dir: str) -> None:
    results_dir = Path(run_dir) / "artifacts" / "results"
    if not results_dir.exists():
        return

    has_files = any(p.is_file() for p in results_dir.rglob("*"))
    if not has_files:
        return

    ts = time.strftime("%Y%m%d-%H%M%S")
    backup_dir = Path(run_dir) / "artifacts_backup" / ts / "results"
    ensure_dir(str(backup_dir.parent))
    shutil.copytree(results_dir, backup_dir)


def reset_results_files(run_dir: str) -> None:
    """Remove previous result files so current run always starts from a clean slate."""
    results_dir = Path(run_dir) / "artifacts" / "results"
    ensure_dir(str(results_dir))
    for name in ("timings.csv", "errors.jsonl", "correctness.csv", "report.md"):
        p = results_dir / name
        if p.exists():
            p.unlink()


def materialize_standalone_configs(databend_dir: str, run_dir: str) -> Tuple[str, str]:
    cfg_dir = Path(run_dir) / "artifacts" / "config"
    ensure_dir(str(cfg_dir))

    meta_dst = cfg_dir / "databend-meta-node-1.toml"
    query_dst = cfg_dir / "databend-query-node-1.toml"

    # Materialize configs from embedded templates so the benchmark is self-contained.
    meta_dst.write_text(META_TOML_TEXT, encoding="utf-8")
    query_dst.write_text(QUERY_TOML_TEXT, encoding="utf-8")

    return str(meta_dst), str(query_dst)


def discover_query_files(paths: List[str]) -> List[str]:
    return [p for p in paths if p.endswith(".sql")]


def discover_query_dir(dir_path: str) -> List[str]:
    return sorted(str(p) for p in Path(dir_path).glob("*.sql"))


def query_index_from_file(path: str) -> int:
    stem = Path(path).stem
    if not stem.isdigit():
        raise RuntimeError(f"Unexpected query filename (expected numeric stem): {path}")
    return int(stem)


def resolve_query_files(databend_dir: str, start_query_index: int) -> List[str]:
    qdir = Path(databend_dir) / QUERIES_DIR
    if not qdir.exists():
        raise RuntimeError(f"Queries dir not found: {qdir}")
    if not RUN_QUERIES:
        discovered = discover_query_dir(str(qdir))
        return [p for p in discovered if query_index_from_file(p) >= start_query_index]
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


def ensure_table_exists(bendsql_bin: str, table: str) -> None:
    code, out, err, _ = run_sql_in_db(
        bendsql_bin,
        f"SELECT 1 FROM {table} LIMIT 1;",
        output="null",
    )
    if code != 0:
        raise RuntimeError(
            f"Table `{table}` does not exist or is unreadable. "
            f"Run with --load to (re)create/load benchmark tables.\n"
            f"stdout:\n{out}\n"
            f"stderr:\n{err}\n"
        )


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


def correctness_diff_count_sql(sql: str) -> str:
    base_q = substitute_hits_table(sql, TABLE_BASELINE).rstrip().rstrip(";")
    vortex_q = substitute_hits_table(sql, TABLE_VORTEX).rstrip().rstrip(";")
    return (
        "WITH\n"
        "q_base AS (\n"
        f"{base_q}\n"
        "),\n"
        "q_vortex AS (\n"
        f"{vortex_q}\n"
        "),\n"
        "diff AS (\n"
        "  (SELECT * FROM q_base EXCEPT ALL SELECT * FROM q_vortex)\n"
        "  UNION ALL\n"
        "  (SELECT * FROM q_vortex EXCEPT ALL SELECT * FROM q_base)\n"
        ")\n"
        "SELECT count(*) AS diff_count FROM diff"
    )


def correctness_diff_count(
    bendsql_bin: str,
    query_name: str,
    sql: str,
    run_dir: str,
    host: str = "127.0.0.1",
    port: int = QUERY_PORT,
) -> Tuple[bool, int, str]:
    check_sql = correctness_diff_count_sql(sql)
    code, out, err, _ = run_sql_in_db(bendsql_bin, check_sql, host=host, port=port, output="tsv")
    if code != 0:
        with open_errors_jsonl(run_dir) as errfp:
            append_error_jsonl(
                errfp,
                {
                    "kind": "correctness_error",
                    "query": query_name,
                    "stderr": err[-4000:],
                    "stdout": out[-4000:],
                },
            )
        return False, -1, err[-1000:]

    rows = parse_tsv_table(out)
    if len(rows) < 2 or not rows[1] or not rows[1][0].strip():
        return False, -1, f"unexpected correctness output: {out[-1000:]}"

    diff_count = int(rows[1][0])
    return diff_count == 0, diff_count, ""


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hits vortex vs parquet standalone benchmark")
    parser.add_argument(
        "--load",
        action="store_true",
        help="(Re)create and load benchmark tables before running queries. Default: skip loading.",
    )
    parser.add_argument(
        "--start-query",
        type=int,
        default=DEFAULT_START_QUERY_INDEX,
        help=f"Start query index for auto-discovery (default: {DEFAULT_START_QUERY_INDEX}, i.e. Q01).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_dir = os.getcwd()
    databend_dir = get_databend_dir()
    meta_bin, query_bin, bendsql_bin = resolve_release_bins(databend_dir)

    query_files = resolve_query_files(databend_dir, args.start_query)
    sys.stderr.write(f"Running in: {run_dir}\n")
    sys.stderr.write(f"Using DATABEND_DIR: {databend_dir}\n")
    sys.stderr.write(f"Queries: {len(query_files)}\n")

    backup_previous_results(run_dir)
    clean_previous_run_data(run_dir)

    ensure_dir(str(Path(run_dir) / "artifacts" / "results"))
    ensure_dir(str(Path(run_dir) / "databend-data"))
    reset_results_files(run_dir)

    meta_cfg, query_cfg = materialize_standalone_configs(databend_dir, run_dir)

    # Cold-start + load baseline
    if RUN_VORTEX_FIRST:
        sys.stderr.write("Starting services (vortex)...\n")
        start_services(meta_bin, query_bin, meta_cfg, query_cfg, run_dir, cold_start=True)
        sys.stderr.write("Creating database...\n")
        init_database(bendsql_bin)
        probe_vortex_support(bendsql_bin)
        if args.load:
            sys.stderr.write(f"Creating+loading {TABLE_VORTEX}...\n")
            create_table_and_load(bendsql_bin, TABLE_VORTEX, storage_format="vortex", gz_path=HITS_TSV_GZ)
        else:
            sys.stderr.write(f"Skipping load; using existing {TABLE_VORTEX}.\n")
            ensure_table_exists(bendsql_bin, TABLE_VORTEX)
        sys.stderr.write(f"Running queries on {TABLE_VORTEX}...\n")
        run_queries_for_table(bendsql_bin, query_files, TABLE_VORTEX, run_dir)

        if COLD_START_EACH_TABLE:
            sys.stderr.write("Restarting services (baseline)...\n")
            start_services(meta_bin, query_bin, meta_cfg, query_cfg, run_dir, cold_start=True)
            sys.stderr.write("Creating database...\n")
            init_database(bendsql_bin)
        if args.load:
            sys.stderr.write(f"Creating+loading {TABLE_BASELINE}...\n")
            create_table_and_load(bendsql_bin, TABLE_BASELINE, storage_format="", gz_path=HITS_TSV_GZ)
        else:
            sys.stderr.write(f"Skipping load; using existing {TABLE_BASELINE}.\n")
            ensure_table_exists(bendsql_bin, TABLE_BASELINE)
        sys.stderr.write(f"Running queries on {TABLE_BASELINE}...\n")
        run_queries_for_table(bendsql_bin, query_files, TABLE_BASELINE, run_dir)
    else:
        sys.stderr.write("Starting services (baseline)...\n")
        start_services(meta_bin, query_bin, meta_cfg, query_cfg, run_dir, cold_start=True)
        sys.stderr.write("Creating database...\n")
        init_database(bendsql_bin)
        if args.load:
            sys.stderr.write(f"Creating+loading {TABLE_BASELINE}...\n")
            create_table_and_load(bendsql_bin, TABLE_BASELINE, storage_format="", gz_path=HITS_TSV_GZ)
        else:
            sys.stderr.write(f"Skipping load; using existing {TABLE_BASELINE}.\n")
            ensure_table_exists(bendsql_bin, TABLE_BASELINE)
        sys.stderr.write(f"Running queries on {TABLE_BASELINE}...\n")
        run_queries_for_table(bendsql_bin, query_files, TABLE_BASELINE, run_dir)

        if COLD_START_EACH_TABLE:
            sys.stderr.write("Restarting services (vortex)...\n")
            start_services(meta_bin, query_bin, meta_cfg, query_cfg, run_dir, cold_start=True)
            sys.stderr.write("Creating database...\n")
            init_database(bendsql_bin)
        probe_vortex_support(bendsql_bin)
        if args.load:
            sys.stderr.write(f"Creating+loading {TABLE_VORTEX}...\n")
            create_table_and_load(bendsql_bin, TABLE_VORTEX, storage_format="vortex", gz_path=HITS_TSV_GZ)
        else:
            sys.stderr.write(f"Skipping load; using existing {TABLE_VORTEX}.\n")
            ensure_table_exists(bendsql_bin, TABLE_VORTEX)
        sys.stderr.write(f"Running queries on {TABLE_VORTEX}...\n")
        run_queries_for_table(bendsql_bin, query_files, TABLE_VORTEX, run_dir)

    # Correctness check: exact multiset comparison via EXCEPT ALL (both directions).
    sys.stderr.write("Computing correctness diffs...\n")
    correctness_rows: List[dict] = []
    mismatched: List[str] = []
    for qf in query_files:
        qname = query_short_name(qf)
        sql = load_sql_file(qf)
        ok, diff_count, err_msg = correctness_diff_count(
            bendsql_bin, qname, sql, run_dir
        )
        if not ok:
            mismatched.append(qname)
        correctness_rows.append(
            {
                "query": qname,
                "status": "PASS" if ok else "FAIL",
                "diff_count": diff_count,
                "error": err_msg,
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

