#!/usr/bin/env python3
"""TCP server that streams CSV rows to a client with strong timestamp ordering guarantees.

Highlights
- Per-key/global ordering enforcement with drop, nudge, or repair.
- Optional device/column filtering.
- Batching, rate control, TCP tuning.
- Optional in-memory pre-sort per key to make ordering bulletproof.
"""
from __future__ import annotations

import argparse
import csv
import heapq
import socket
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


def parse_arguments() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Stream CSV rows over TCP")
    p.add_argument("csv_path", type=Path, help="Path to the CSV file to stream")

    # Networking
    p.add_argument("--host", default="127.0.0.1", help="Host/IP to bind (default: 127.0.0.1)")
    p.add_argument("--port", type=int, default=32323, help="Port to bind (default: 32323)")
    p.add_argument("--tcp-nodelay", action="store_true", help="Enable TCP_NODELAY on client sockets")
    p.add_argument("--send-buffer", type=int, default=0, help="SO_SNDBUF bytes for client sockets (0 keeps default)")

    # Pacing & batching
    p.add_argument("--delay", type=float, default=0.0, help="Delay in seconds between rows (default: 0.0)")
    p.add_argument("--rows-per-sec", type=float, default=0.0, help="Target rows per second (mutually exclusive with --delay)")
    p.add_argument("--batch-size", type=int, default=1, help="Rows per socket write")
    p.add_argument("--batch-rows", type=int, default=None, help="Alias for --batch-size (overrides if set)")
    p.add_argument("--max-batch-bytes", type=int, default=0, help="Soft cap for bytes per batch (0 disables)")

    # CSV
    p.add_argument("--delimiter", default=",", help="CSV delimiter (default ',')")
    p.add_argument("--skip-header", action="store_true", help="Skip first line as header")
    p.add_argument("--ts-col-index", type=int, default=0, help="Zero-based index of timestamp column (default 0)")

    # Ordering
    p.add_argument("--order-scope", choices=["global", "per-key", "both"], default="global",
                   help="Ordering enforcement scope (requires keys for per-key/both)")
    p.add_argument("--key-col-index", type=int, nargs='*', default=None,
                   help="Zero-based key column index(es) for per-key ordering (e.g., 1 for device_id)")
    p.add_argument("--nudge-equal-seconds", type=int, default=0,
                   help="If >0, advance equal timestamps by this many seconds")
    p.add_argument("--repair-monotonic-seconds", type=int, default=0,
                   help="If >0, advance any non-increasing (<=) timestamp by this many seconds")

    # Filtering
    p.add_argument("--filter-device-id", nargs='+', default=None,
                   help="Only include rows with device_id equal to one of these values (assumes device_id col index 1)")
    p.add_argument("--filter-col-index", type=int, default=None,
                   help="Generic include filter: 0-based column index to match against --filter-values")
    p.add_argument("--filter-values", nargs='+', default=None,
                   help="Generic include filter values for --filter-col-index")

    # Mode & diagnostics
    p.add_argument("--no-order", action="store_true", help="Bypass ordering enforcement")
    p.add_argument("--preload", action="store_true", help="Load the file into memory before streaming")
    p.add_argument("--sort-per-key", action="store_true",
                   help="Pre-sort rows by key(s) and timestamp before streaming (implies in-memory load)")
    p.add_argument("--filtered-log", type=str, default=None,
                   help="Append filtered/nudged/repaired diagnostics as TSV to this path")
    p.add_argument("--sample-filtered", type=int, default=0,
                   help="Print first N filtered diagnostics to stdout")
    p.add_argument("--verbose", action="store_true", help="Print progress/diagnostics")
    p.add_argument("--quiet", action="store_true", help="Suppress non-error console output")
    p.add_argument("--loop", action="store_true", help="Replay file indefinitely")

    args = p.parse_args()

    # Normalize
    if args.batch_rows is not None:
        args.batch_size = args.batch_rows

    # Validate
    if args.delay > 0.0 and args.rows_per_sec > 0.0:
        p.error("--delay and --rows-per-sec are mutually exclusive")
    if args.batch_size < 1:
        p.error("--batch-size must be >= 1")
    if args.max_batch_bytes < 0:
        p.error("--max-batch-bytes must be >= 0")
    if args.rows_per_sec < 0.0:
        p.error("--rows-per-sec must be >= 0.0")
    if args.order_scope in ("per-key", "both") and (not args.key_col_index or len(args.key_col_index) == 0):
        p.error("--order-scope per-key/both requires --key-col-index <idx ...>")
    if args.sort_per_key and (not args.key_col_index or len(args.key_col_index) == 0):
        p.error("--sort-per-key requires --key-col-index <idx ...>")
    if args.no_order and (args.nudge_equal_seconds > 0 or args.repair_monotonic_seconds > 0 or args.order_scope != "global" or args.key_col_index):
        print("[WARN] --no-order disables ordering; nudge/repair/order-scope/key-col-index are ignored", file=sys.stderr)

    # Assemble filters
    if args.filter_device_id is not None:
        args.filter_col_index = 1
        args.filter_values = [str(v) for v in args.filter_device_id]
    elif args.filter_col_index is not None:
        if not args.filter_values:
            p.error("--filter-values required when --filter-col-index is set")
        args.filter_col_index = int(args.filter_col_index)
        args.filter_values = [str(v) for v in args.filter_values]

    return args


def _parse_timestamp_to_epoch_ms(ts_raw: str) -> float | None:
    s = ts_raw.strip()
    if not s:
        return None
    # Fast path: numeric seconds (int/float)
    try:
        return float(int(s)) * 1000.0
    except ValueError:
        try:
            return float(s) * 1000.0
        except ValueError:
            pass

    # ISO 8601 parsing with robust timezone normalization
    t = s
    # Handle trailing Z/z as UTC
    if t.endswith('Z') or t.endswith('z'):
        t = t[:-1] + '+00:00'
    # Allow space between date and time
    if 'T' not in t and ' ' in t:
        parts = t.split(' ')
        if len(parts) >= 2:
            t = parts[0] + 'T' + ' '.join(parts[1:])
    # Normalize timezone offsets:
    # Accept +HH, +HHMM, +HH:MM and normalize to +HH:MM
    try:
        import re
        # +HHMM → +HH:MM
        m = re.search(r"([+-]\d{2})(\d{2})$", t)
        if m:
            t = t[: -len(m.group(0))] + f"{m.group(1)}:{m.group(2)}"
        else:
            # +HH → +HH:00
            if re.search(r"[+-]\d{2}$", t):
                t = t + ":00"
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp() * 1000.0
    except Exception:
        return None


def _fmt_epoch_ms_to_seconds(ts_ms: float) -> str:
    return str(int(ts_ms // 1000))


def iter_csv_lines(
    csv_path: Path,
    loop: bool,
    delimiter: str,
    ts_col_index: int,
    skip_header: bool,
    verbose: bool,
    key_col_index: list[int] | None,
    order_scope: str,
    no_order: bool,
    preload: bool,
    nudge_equal_seconds: int,
    repair_monotonic_seconds: int,
    filter_col_index: int | None,
    filter_values: list[str] | None,
    filtered_log: str | None,
    sample_filtered: int,
    sort_per_key: bool,
) -> Iterator[bytes]:
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    filter_active = (filter_col_index is not None) and (filter_values is not None and len(filter_values) > 0)

    def log_filtered(kind: str, scope: str, key: str, prev: str, new: str, raw_ts: str, row_str: str | None = None):
        if verbose and row_str is not None:
            print(f"SAMPLE filtered ({scope}): prev={prev} ts='{raw_ts}' row='{row_str}'")
        if filtered_log:
            with open(filtered_log, "a", encoding="utf-8") as flog:
                flog.write(f"{kind}\t{scope}\t{key}\t{prev}\t{new}\t{raw_ts}\t\n")

    # Pre-sorted, per-key (bulletproof fix)
    if sort_per_key:
        groups: dict[tuple[str, ...], list[tuple[float, list[str]]]] = {}
        first = True
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=delimiter)
            for row in reader:
                if first:
                    first = False
                    if skip_header:
                        continue
                if not row:
                    continue
                if filter_active:
                    if filter_col_index < 0 or filter_col_index >= len(row):
                        continue
                    if row[filter_col_index].strip() not in filter_values:
                        continue
                if ts_col_index < 0 or ts_col_index >= len(row):
                    continue
                ts_raw = row[ts_col_index].strip()
                ts_val = _parse_timestamp_to_epoch_ms(ts_raw)
                if ts_val is None:
                    log_filtered("unparsable", "global", "", "", "", ts_raw, delimiter.join(row))
                    continue
                key = tuple(row[i].strip() if i < len(row) else "" for i in (key_col_index or []))
                groups.setdefault(key, []).append((ts_val, row))

        # Sort and repair per key
        for key, items in groups.items():
            items.sort(key=lambda t: t[0])
            prev = float('-inf')
            repaired: list[tuple[float, list[str]]] = []
            for ts_val, row in items:
                raw = _fmt_epoch_ms_to_seconds(ts_val)
                if ts_val <= prev:
                    if repair_monotonic_seconds > 0:
                        ts_val = prev + (repair_monotonic_seconds * 1000)
                        row[ts_col_index] = _fmt_epoch_ms_to_seconds(ts_val)
                        log_filtered("repaired_key_presort", "per-key", ",".join(key), str(prev), row[ts_col_index], raw)
                    elif nudge_equal_seconds > 0 and ts_val == prev:
                        ts_val = prev + (nudge_equal_seconds * 1000)
                        row[ts_col_index] = _fmt_epoch_ms_to_seconds(ts_val)
                        log_filtered("nudged_key_presort", "per-key", ",".join(key), str(prev), row[ts_col_index], raw)
                    else:
                        log_filtered("nonincreasing_key_presort", "per-key", ",".join(key), str(prev), "", raw, delimiter.join(row))
                        continue
                repaired.append((ts_val, row))
                prev = ts_val
            groups[key] = repaired

        # Global merge and yield
        heap: list[tuple[float, int, int]] = []  # (ts, group_idx, item_idx)
        keys = list(groups.keys())
        for gi, key in enumerate(keys):
            if groups[key]:
                heapq.heappush(heap, (groups[key][0][0], gi, 0))
        last_global = float('-inf')
        while heap:
            ts_val, gi, idx = heapq.heappop(heap)
            key = keys[gi]
            cur_ts, row = groups[key][idx]
            raw = _fmt_epoch_ms_to_seconds(cur_ts)
            if order_scope in ("global", "both") and cur_ts <= last_global:
                if repair_monotonic_seconds > 0:
                    cur_ts = last_global + (repair_monotonic_seconds * 1000)
                    row[ts_col_index] = _fmt_epoch_ms_to_seconds(cur_ts)
                    log_filtered("repaired_global_presort", "global", "", str(last_global), row[ts_col_index], raw)
                elif nudge_equal_seconds > 0 and cur_ts == last_global:
                    cur_ts = last_global + (nudge_equal_seconds * 1000)
                    row[ts_col_index] = _fmt_epoch_ms_to_seconds(cur_ts)
                    log_filtered("nudged_global_presort", "global", "", str(last_global), row[ts_col_index], raw)
                else:
                    log_filtered("nonincreasing_global_presort", "global", "", str(last_global), "", raw, delimiter.join(row))
                    # advance pointer and continue
                    idx += 1
                    if idx < len(groups[key]):
                        heapq.heappush(heap, (groups[key][idx][0], gi, idx))
                    continue
            last_global = cur_ts
            yield (delimiter.join(row) + "\n").encode("utf-8")
            # advance pointer
            idx += 1
            if idx < len(groups[key]):
                heapq.heappush(heap, (groups[key][idx][0], gi, idx))
        return

    # No-order fast path (still apply filter if configured)
    if no_order:
        if preload:
            raw_lines = csv_path.read_bytes().splitlines(keepends=True)
            while True:
                first = True
                for line in raw_lines:
                    if first:
                        first = False
                        if skip_header:
                            continue
                    if filter_active:
                        row = line.decode("utf-8").rstrip("\n").split(delimiter)
                        if not row:
                            continue
                        if filter_col_index < 0 or filter_col_index >= len(row) or row[filter_col_index].strip() not in filter_values:
                            continue
                    if not line.endswith(b"\n"):
                        yield line + b"\n"
                    else:
                        yield line
                if not loop:
                    break
        else:
            while True:
                with csv_path.open("rb") as fb:
                    first = True
                    for line in fb:
                        if first:
                            first = False
                            if skip_header:
                                continue
                        if filter_active:
                            row = line.decode("utf-8").rstrip("\n").split(delimiter)
                            if not row:
                                continue
                            if filter_col_index < 0 or filter_col_index >= len(row) or row[filter_col_index].strip() not in filter_values:
                                continue
                        yield line
                if not loop:
                    break
        return

    # Streaming enforcement path
    last_global = float('-inf')
    last_by_key: dict[tuple[str, ...], float] | None = {} if key_col_index else None

    while True:
        if preload:
            rows_iter = list(csv.reader(csv_path.open("r", newline="", encoding="utf-8"), delimiter=delimiter))
        else:
            f = csv_path.open("r", newline="", encoding="utf-8")
            rows_iter = csv.reader(f, delimiter=delimiter)
        try:
            first = True
            for row in rows_iter:
                if first:
                    first = False
                    if skip_header:
                        continue
                if not row:
                    continue
                if filter_active:
                    if filter_col_index < 0 or filter_col_index >= len(row):
                        continue
                    if row[filter_col_index].strip() not in filter_values:
                        continue
                if ts_col_index < 0 or ts_col_index >= len(row):
                    yield (delimiter.join(row) + "\n").encode("utf-8")
                    continue
                ts_raw = row[ts_col_index].strip()
                ts_val = _parse_timestamp_to_epoch_ms(ts_raw)
                if ts_val is None:
                    log_filtered("unparsable", "global", "", "", "", ts_raw, delimiter.join(row))
                    continue
                # per-key enforcement
                if order_scope in ("per-key", "both") and last_by_key is not None:
                    key = tuple(row[i].strip() if i < len(row) else "" for i in key_col_index)
                    prev = last_by_key.get(key, float('-inf'))
                    if ts_val <= prev:
                        if repair_monotonic_seconds > 0:
                            ts_val = prev + (repair_monotonic_seconds * 1000)
                            row[ts_col_index] = _fmt_epoch_ms_to_seconds(ts_val)
                            log_filtered("repaired_key", "per-key", ",".join(key), str(prev), row[ts_col_index], ts_raw)
                        elif nudge_equal_seconds > 0 and ts_val == prev:
                            ts_val = prev + (nudge_equal_seconds * 1000)
                            row[ts_col_index] = _fmt_epoch_ms_to_seconds(ts_val)
                            log_filtered("nudged_key", "per-key", ",".join(key), str(prev), row[ts_col_index], ts_raw)
                        else:
                            log_filtered("nonincreasing_key", "per-key", ",".join(key), str(prev), "", ts_raw, delimiter.join(row))
                            continue
                    last_by_key[key] = ts_val
                # global enforcement
                if order_scope in ("global", "both"):
                    if ts_val <= last_global:
                        if repair_monotonic_seconds > 0:
                            ts_val = last_global + (repair_monotonic_seconds * 1000)
                            row[ts_col_index] = _fmt_epoch_ms_to_seconds(ts_val)
                            log_filtered("repaired_global", "global", "", str(last_global), row[ts_col_index], ts_raw)
                        elif nudge_equal_seconds > 0 and ts_val == last_global:
                            ts_val = last_global + (nudge_equal_seconds * 1000)
                            row[ts_col_index] = _fmt_epoch_ms_to_seconds(ts_val)
                            log_filtered("nudged_global", "global", "", str(last_global), row[ts_col_index], ts_raw)
                        else:
                            log_filtered("nonincreasing_global", "global", "", str(last_global), "", ts_raw, delimiter.join(row))
                            continue
                    last_global = ts_val
                yield (delimiter.join(row) + "\n").encode("utf-8")
        finally:
            if not preload and 'f' in locals():
                f.close()
        if not loop:
            break


def stream_to_client(
    client_sock: socket.socket,
    csv_path: Path,
    delay: float,
    rows_per_sec: float,
    batch_size: int,
    max_batch_bytes: int,
    loop: bool,
    delimiter: str,
    ts_col_index: int,
    skip_header: bool,
    verbose: bool,
    key_col_index: list[int] | None,
    order_scope: str,
    no_order: bool,
    preload: bool,
    nudge_equal_seconds: int,
    repair_monotonic_seconds: int,
    filter_col_index: int | None,
    filter_values: list[str] | None,
    filtered_log: str | None,
    sample_filtered: int,
    sort_per_key: bool,
) -> None:
    if delay > 0.0 and rows_per_sec > 0.0:
        raise ValueError("--delay and --rows-per-sec are mutually exclusive")
    per_row_delay = delay if delay > 0.0 else (1.0 / rows_per_sec if rows_per_sec > 0.0 else 0.0)

    batch: list[bytes] = []
    batch_bytes = 0
    last_send_ts = time.perf_counter()
    tuples_sent = 0
    try:
        for line in iter_csv_lines(
            csv_path,
            loop,
            delimiter,
            ts_col_index,
            skip_header,
            verbose,
            key_col_index,
            order_scope,
            no_order,
            preload,
            nudge_equal_seconds,
            repair_monotonic_seconds,
            filter_col_index,
            filter_values,
            filtered_log,
            sample_filtered,
            sort_per_key,
        ):
            batch.append(line)
            batch_bytes += len(line)
            should_flush = len(batch) >= batch_size or (max_batch_bytes > 0 and batch_bytes >= max_batch_bytes)
            if should_flush:
                flushed = len(batch)
                client_sock.sendall(b"".join(batch))
                batch.clear()
                batch_bytes = 0
                tuples_sent += flushed
                if per_row_delay > 0.0:
                    rows_sent = flushed
                    target_elapsed = per_row_delay * rows_sent
                    now = time.perf_counter()
                    wake_at = last_send_ts + target_elapsed
                    sleep_for = wake_at - now
                    if sleep_for > 0:
                        time.sleep(sleep_for)
                        last_send_ts = wake_at
                    else:
                        last_send_ts = now
        if batch:
            # Flush remaining rows
            client_sock.sendall(b"".join(batch))
            tuples_sent += len(batch)
    except (BrokenPipeError, ConnectionResetError):
        pass
    finally:
        # Report how many tuples have been sent to this client
        if verbose:
            try:
                print(f"Total tuples sent: {tuples_sent}")
            except Exception:
                pass
        try:
            client_sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        client_sock.close()


def run_server(
    csv_path: Path,
    host: str,
    port: int,
    delay: float,
    rows_per_sec: float,
    batch_size: int,
    max_batch_bytes: int,
    loop: bool,
    delimiter: str,
    ts_col_index: int,
    skip_header: bool,
    verbose: bool,
    key_col_index: list[int] | None,
    order_scope: str,
    tcp_nodelay: bool,
    send_buffer: int,
    no_order: bool,
    preload: bool,
    nudge_equal_seconds: int,
    repair_monotonic_seconds: int,
    filter_col_index: int | None,
    filter_values: list[str] | None,
    filtered_log: str | None,
    sample_filtered: int,
    sort_per_key: bool,
) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((host, port))
        server_sock.listen(1)
        if verbose and not (False):
            print(
                f"Streaming '{csv_path}' on {host}:{port} "
                f"(delay={delay}s, rows_per_sec={rows_per_sec}, batch_size={batch_size}, loop={loop}, "
                f"ts_col_index={ts_col_index}, delimiter='{delimiter}', skip_header={skip_header}, "
                f"key_col_index={key_col_index}, order_scope={order_scope}, tcp_nodelay={tcp_nodelay}, send_buffer={send_buffer}, "
                f"no_order={no_order}, preload={preload}, sort_per_key={sort_per_key})"
            )
        while True:
            try:
                client_sock, addr = server_sock.accept()
            except KeyboardInterrupt:
                print("\nServer interrupted. Exiting.")
                break
            if verbose:
                print(f"Client connected from {addr[0]}:{addr[1]}")
            try:
                if tcp_nodelay:
                    client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                if send_buffer > 0:
                    client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, send_buffer)
            except OSError:
                pass
            stream_to_client(
                client_sock,
                csv_path,
                delay,
                rows_per_sec,
                batch_size,
                max_batch_bytes,
                loop,
                delimiter,
                ts_col_index,
                skip_header,
                verbose,
                key_col_index,
                order_scope,
                no_order,
                preload,
                nudge_equal_seconds,
                repair_monotonic_seconds,
                filter_col_index,
                filter_values,
                filtered_log,
                sample_filtered,
                sort_per_key,
            )
            if verbose:
                print(f"Client {addr[0]}:{addr[1]} disconnected")


def main() -> int:
    args = parse_arguments()
    try:
        run_server(
            args.csv_path,
            args.host,
            args.port,
            args.delay,
            args.rows_per_sec,
            args.batch_size,
            args.max_batch_bytes,
            args.loop,
            args.delimiter,
            args.ts_col_index,
            args.skip_header,
            False if args.quiet else args.verbose,
            args.key_col_index,
            args.order_scope,
            args.tcp_nodelay,
            args.send_buffer,
            args.no_order,
            args.preload,
            args.nudge_equal_seconds,
            args.repair_monotonic_seconds,
            args.filter_col_index,
            args.filter_values,
            args.filtered_log,
            args.sample_filtered,
            args.sort_per_key,
        )
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted. Exiting.")
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
