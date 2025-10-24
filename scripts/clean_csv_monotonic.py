#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path
from typing import List, Tuple, Dict, Optional

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Clean CSV by removing out-of-order rows (strictly increasing timestamps).")
    p.add_argument("--input", required=True, type=Path, help="Input CSV path")
    p.add_argument("--output", required=True, type=Path, help="Output cleaned CSV path")
    p.add_argument("--delimiter", default=",", help="CSV delimiter (default ',')")
    p.add_argument("--ts-col-index", type=int, default=0, help="Zero-based timestamp column index (default 0)")
    p.add_argument("--key-col-index", type=int, nargs='*', default=[1], help="Zero-based key column index(es), default [1] (device_id)")
    p.add_argument("--skip-header", action="store_true", help="Skip first line as header and write it to output")
    p.add_argument("--drop-equals", action="store_true", help="Drop rows with equal timestamp (strictly increasing)")
    p.add_argument("--order-scope", choices=["per-key","global"], default="per-key", help="Enforce per-key (default) or global ordering")
    return p.parse_args()


def parse_ts(val: str) -> Optional[float]:
    s = val.strip()
    if not s:
        return None
    try:
        return float(int(s))
    except ValueError:
        try:
            return float(s)
        except ValueError:
            return None


def main() -> int:
    args = parse_args()
    inp: Path = args.input
    outp: Path = args.output

    if not inp.is_file():
        print(f"Input not found: {inp}")
        return 1

    last_global = float('-inf')
    last_by_key: Optional[Dict[Tuple[str, ...], float]] = {} if args.order_scope == "per-key" else None

    total = 0
    kept = 0
    dropped_non_increasing = 0
    dropped_unparsable = 0
    dropped_index_error = 0

    outp.parent.mkdir(parents=True, exist_ok=True)

    with inp.open("r", newline="", encoding="utf-8") as fin, outp.open("w", newline="", encoding="utf-8") as fout:
        reader = csv.reader(fin, delimiter=args.delimiter)
        writer = csv.writer(fout, delimiter=args.delimiter)

        first = True
        for row in reader:
            total += 1
            if first:
                first = False
                if args.skip_header:
                    writer.writerow(row)
                    continue
            if not row:
                continue
            # Defensive index checks
            if args.ts_col_index < 0 or args.ts_col_index >= len(row):
                dropped_index_error += 1
                continue

            ts_raw = row[args.ts_col_index].strip()
            ts = parse_ts(ts_raw)
            if ts is None:
                dropped_unparsable += 1
                continue

            if args.order_scope == "per-key":
                key = tuple(row[i].strip() if i < len(row) else "" for i in args.key_col_index)
                prev = last_by_key.get(key, float('-inf'))
                if ts < prev or (args.drop_equals and ts == prev):
                    dropped_non_increasing += 1
                    continue
                last_by_key[key] = ts
            else:
                if ts < last_global or (args.drop_equals and ts == last_global):
                    dropped_non_increasing += 1
                    continue
                last_global = ts

            writer.writerow(row)
            kept += 1

    print(f"Input: {inp}")
    print(f"Output: {outp}")
    print(f"Rows total={total} kept={kept} dropped_non_increasing={dropped_non_increasing} dropped_unparsable={dropped_unparsable} dropped_index_error={dropped_index_error}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
