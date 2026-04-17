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


if __name__ == "__main__":
    sys.stderr.write("hits_vortex_standalone_bench.py: partial implementation (Task 2 in progress)\n")
    raise SystemExit(2)

