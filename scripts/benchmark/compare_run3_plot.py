import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt


def query_sort_key(q: str) -> int:
    if q.startswith("Q") and q[1:].isdigit():
        return int(q[1:])
    return 10**9


def resolve_input(path_arg: str | None) -> Path:
    if path_arg:
        p = Path(path_arg).expanduser().resolve()
        if not p.exists():
            raise FileNotFoundError(f"Input CSV not found: {p}")
        return p

    candidates = [
        Path.cwd() / "artifacts" / "results" / "timings.csv",
        Path.cwd() / "artifacts" / "results" / "timing.csv",
    ]
    for c in candidates:
        if c.exists():
            return c.resolve()

    raise FileNotFoundError(
        "Cannot find input CSV. Use --input, or run in a directory with artifacts/results/timings.csv"
    )


def format_ratio(numerator: int, denominator: int) -> str:
    if not denominator:
        return ""
    return f"{numerator / denominator:.2f}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Compare hits_vortex vs hits_fuse duration by run index (default: run 1, cold start)"
    )
    parser.add_argument("--input", type=str, default=None, help="Path to timings CSV (timings.csv or timing.csv)")
    parser.add_argument("--out-dir", type=str, default=None, help="Output directory for PNG/CSV (default: input parent)")
    parser.add_argument("--vortex-table", type=str, default="hits_vortex")
    parser.add_argument("--parquet-table", type=str, default="hits_fuse")
    parser.add_argument(
        "--run-idx",
        type=int,
        default=1,
        help="Which run index to use for comparison (1=cold start, 2, 3, or 0=min across all runs). Default: 1.",
    )
    args = parser.parse_args()
    parquet_output_name = "fuse_parquet" if args.parquet_table == "hits_fuse" else args.parquet_table

    in_file = resolve_input(args.input)
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else in_file.parent
    out_dir.mkdir(parents=True, exist_ok=True)

    run_label = f"run{args.run_idx}" if args.run_idx != 0 else "min"
    out_png = out_dir / f"timing_{run_label}_compare.png"
    out_csv = out_dir / f"timing_{run_label}_compare.csv"

    # (query, table) -> selected duration_ms
    selected: dict[tuple[str, str], int] = {}
    with in_file.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            table = row.get("table", "")
            if table not in (args.vortex_table, args.parquet_table):
                continue

            ok = row.get("ok")
            if ok is not None and ok != "1":
                continue

            query = row["query"]
            dur = int(row["duration_ms"])
            k = (query, table)

            if args.run_idx == 0:
                # min across all runs
                prev = selected.get(k)
                if prev is None or dur < prev:
                    selected[k] = dur
            else:
                # specific run index
                if str(row.get("run_idx", "")) == str(args.run_idx):
                    selected[k] = dur

    queries = sorted(
        {
            q
            for (q, t) in selected.keys()
            if (q, args.vortex_table) in selected and (q, args.parquet_table) in selected
        },
        key=query_sort_key,
    )

    vortex = {q: selected[(q, args.vortex_table)] for q in queries}
    parquet = {q: selected[(q, args.parquet_table)] for q in queries}

    col_label = run_label
    with out_csv.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "query",
            f"{args.vortex_table}_{col_label}_ms",
            f"{parquet_output_name}_{col_label}_ms",
            "delta_ms(vortex-parquet)",
            "ratio(vortex/parquet)",
        ])
        total_v = 0
        total_p = 0
        for q in queries:
            v = vortex[q]
            p = parquet[q]
            total_v += v
            total_p += p
            w.writerow([q, v, p, v - p, format_ratio(v, p)])

        w.writerow([
            "TOTAL",
            total_v,
            total_p,
            total_v - total_p,
            format_ratio(total_v, total_p),
        ])

    x = list(range(len(queries)))
    width = 0.42
    v_vals = [vortex[q] for q in queries]
    p_vals = [parquet[q] for q in queries]

    plt.figure(figsize=(22, 7))
    plt.bar([i - width / 2 for i in x], v_vals, width=width, label=args.vortex_table)
    plt.bar([i + width / 2 for i in x], p_vals, width=width, label=f"{parquet_output_name}(parquet)")
    plt.xticks(x, queries, rotation=90)
    plt.ylabel("Duration (ms)")
    plt.xlabel("Query")
    title_label = f"run {args.run_idx} (cold start)" if args.run_idx == 1 else (f"run {args.run_idx}" if args.run_idx != 0 else "min across runs")
    plt.title(f"Duration Comparison ({title_label}): hits_vortex vs fuse_parquet")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_png, dpi=180)

    faster_queries = [q for q in queries if vortex[q] < parquet[q]]
    slower_queries = [q for q in queries if vortex[q] > parquet[q]]
    equal_queries = [q for q in queries if vortex[q] == parquet[q]]

    better = len(faster_queries)
    slower = len(slower_queries)
    equal = len(equal_queries)

    print(f"input={in_file}")
    print(f"run_idx={args.run_idx} ({'cold start' if args.run_idx == 1 else 'min across runs' if args.run_idx == 0 else f'run {args.run_idx}'})")
    print(f"queries={len(queries)}")
    print(f"{args.vortex_table}_faster={better}, slower={slower}, equal={equal}")
    print(f"faster_queries={','.join(faster_queries) if faster_queries else '-'}")
    print(f"slower_queries={','.join(slower_queries) if slower_queries else '-'}")
    print(f"equal_queries={','.join(equal_queries) if equal_queries else '-'}")
    print(f"png={out_png}")
    print(f"csv={out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

