import os
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


if __name__ == "__main__":
    sys.stderr.write("hits_vortex_standalone_bench.py: partial implementation (Task 2 in progress)\n")
    raise SystemExit(2)

