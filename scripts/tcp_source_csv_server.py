#!/usr/bin/env python3
"""Simple TCP server that streams rows from a CSV file to clients.

Usage:
    python scripts/tcp_source_csv_server.py path/to/file.csv \
        --host 0.0.0.0 --port 32323 --delay 0.05 --loop \
        [--batch-size 100 --rows-per-sec 50000 --tcp-nodelay --send-buffer 1048576 \
         --no-order --preload --batch-rows 1000 --quiet]

The server accepts a single client at a time. Once a client connects, the
configured CSV file is streamed line-by-line with the configured delay between
records. If `--loop` is supplied, the file is replayed indefinitely; otherwise
the connection is closed after the last row is sent. When the client
disconnects, the server waits for the next connection.

Performance notes:
- Use --batch-size to send multiple rows per socket write.
- Use --rows-per-sec (instead of --delay) to target a rate.
- Optionally enable --tcp-nodelay and/or increase --send-buffer for tuning.
- For maximum throughput (lowest CPU), combine --no-order and --preload.
"""

from __future__ import annotations

import argparse
import socket
import sys
import time
from pathlib import Path
import csv
import re
from datetime import datetime, timezone
from typing import Iterator


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream CSV rows over TCP")
    parser.add_argument("csv_path", type=Path, help="Path to the CSV file to stream")
    parser.add_argument("--host", default="127.0.0.1", help="Host/IP to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=32323, help="Port to bind (default: 32323)")
    parser.add_argument(
        "--delay",
        type=float,
        default=0.0,
        help="Delay in seconds between rows (default: 0.0)",
    )
    parser.add_argument(
        "--rows-per-sec",
        type=float,
        default=0.0,
        help=(
            "Target rows per second. Incompatible with --delay. "
            "When set, pacing is derived as 1/rate per row (batched)."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1,
        help=(
            "Number of rows to send per socket write (default: 1). "
            "Larger batches reduce per-call overhead and improve throughput."
        ),
    )
    parser.add_argument(
        "--batch-rows",
        type=int,
        default=None,
        help=(
            "Alias for --batch-size. If set, overrides --batch-size."
        ),
    )
    parser.add_argument(
        "--max-batch-bytes",
        type=int,
        default=0,
        help=(
            "Optional soft limit for bytes per batch (0 disables). "
            "When >0, a batch will be flushed when this threshold is reached."
        ),
    )
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Continuously loop over the CSV file for each client",
    )
    parser.add_argument(
        "--ts-col-index",
        type=int,
        default=0,
        help="Zero-based index of the timestamp column to enforce ordering (default: 0)",
    )
    parser.add_argument(
        "--delimiter",
        default=",",
        help="CSV delimiter (default: ',')",
    )
    parser.add_argument(
        "--skip-header",
        action="store_true",
        help="Skip the first line as header (not sent to client)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print basic stats about filtered/forwarded rows",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress non-error console output (overrides --verbose)",
    )
    parser.add_argument(
        "--key-col-index",
        type=int,
        nargs='*',
        default=None,
        help=(
            "Zero-based column index(es) forming a key to enforce ordering per key. "
            "If omitted, ordering is enforced globally. Example: --key-col-index 1 for device_id column."
        ),
    )
    parser.add_argument(
        "--order-scope",
        choices=["global", "per-key", "both"],
        default="global",
        help=(
            "Ordering enforcement scope: 'global' (default) enforces strictly increasing timestamps across all rows; "
            "'per-key' enforces within each key only (requires --key-col-index); 'both' enforces both."
        ),
    )
    parser.add_argument(
        "--nudge-equal-seconds",
        type=int,
        default=0,
        help=(
            "If >0, when a timestamp equals the previous one in the enforced scope, advance it by the given number of seconds instead of dropping."
        ),
    )

    parser.add_argument(
        "--repair-monotonic-seconds",
        type=int,
        default=0,
        help=(
            "If >0, enforce strictly increasing timestamps by nudging any non-increasing row "
            "(<= previous) forward by the given number of seconds within the enforced scope."
        ),
    )
    parser.add_argument(
        "--tcp-nodelay",
        action="store_true",
        help="Enable TCP_NODELAY on client sockets to minimize latency",
    )
    parser.add_argument(
        "--send-buffer",
        type=int,
        default=0,
        help="Optional SO_SNDBUF size in bytes for client sockets (0 keeps system default)",
    )
    parser.add_argument(
        "--no-order",
        action="store_true",
        help="Do not enforce timestamp ordering; forward rows as-is",
    )
    parser.add_argument(
        "--preload",
        action="store_true",
        help="Load the CSV into memory once before streaming (use with --no-order for best speed)",
    )
    parser.add_argument(
        "--filtered-log",
        type=str,
        default=None,
        help="Optional path to append filtered/nudged/repaired diagnostics as TSV",
    )
    parser.add_argument(
        "--sample-filtered",
        type=int,
        default=0,
        help="Print first N filtered diagnostics to stdout",
    )


    parser.add_argument(
        "--filter-device-id",
        nargs='+',
        default=None,
        help="Only include rows where device_id equals one of these values (assumes device_id column index 1)",
    )
    parser.add_argument(
        "--filter-col-index",
        type=int,
        default=None,
        help="Generic inclusion filter: 0-based column index to match against --filter-values",
    )
    parser.add_argument(
        "--filter-values",
        nargs='+',
        default=None,
        help="Generic inclusion filter: values to include for --filter-col-index",
    )

    args = parser.parse_args()

    if args.batch_rows is not None:
        args.batch_size = args.batch_rows

    if args.delay > 0.0 and args.rows_per_sec > 0.0:
        parser.error("--delay and --rows-per-sec are mutually exclusive")
    if args.batch_size < 1:
        parser.error("--batch-size must be >= 1")
    if args.max_batch_bytes < 0:
        parser.error("--max-batch-bytes must be >= 0")
    if args.rows_per_sec < 0.0:
        parser.error("--rows-per-sec must be >= 0.0")

    # Validate per-key/both requires key columns
    if args.order_scope in ("per-key", "both") and (not args.key_col_index or len(args.key_col_index) == 0):
        parser.error("--order-scope per-key/both requires --key-col-index <idx ...>")

    # Warn on conflicting flags when --no-order
    if args.no_order and (getattr(args, "nudge_equal_seconds", 0) > 0 or getattr(args, "repair_monotonic_seconds", 0) > 0 or args.order_scope != "global" or args.key_col_index):
        print("[WARN] --no-order disables ordering enforcement; nudge/repair/order-scope/key-col-index are ignored", file=sys.stderr)


    # Compute effective filter settings
    filter_col_index = None
    filter_values = None
    if args.filter_device_id is not None:
        filter_col_index = 1
        filter_values = [str(v) for v in args.filter_device_id]
    elif args.filter_col_index is not None:
        if not args.filter_values:
            parser.error("--filter-values required when --filter-col-index is set")
        filter_col_index = int(args.filter_col_index)
        filter_values = [str(v) for v in args.filter_values]
    args.filter_col_index = filter_col_index
    args.filter_values = filter_values


    return args


def _parse_timestamp_to_epoch_ms(ts_raw: str) -> float | None:
    """Parse a timestamp string/number into epoch milliseconds.

    Handles:
    - integer/float seconds
    - ISO-like strings: 'YYYY-MM-DD HH:MM:SS[.fff][Z|+HH[:MM]|+HHMM|+HH]'
    Returns None if parsing fails.
    """
    s = ts_raw.strip()
    if not s:
        return None
    # Fast path: numeric seconds
    try:
        return float(int(s)) * 1000.0
    except ValueError:
        try:
            return float(s) * 1000.0
        except ValueError:
            pass

    # Normalize ISO-like formatting
    t = s
    if t.endswith('Z') or t.endswith('z'):
        t = t[:-1] + '+00:00'
    if 'T' not in t and ' ' in t:
        # Replace first space between date and time with 'T'
        parts = t.split(' ')
        if len(parts) >= 2:
            t = parts[0] + 'T' + ' '.join(parts[1:])
    # Normalize timezone: +HH, +HHMM -> +HH:MM
    m = re.search(r'([+-])(\d{2})(:?)(\d{0,2})$', t)
    if m:
        sign, hh, colon, mm = m.groups()
        if not mm:
            mm = '00'
        if colon != ':':
            t = t[: m.start()] + f"{sign}{hh}:{mm}"
    try:
        dt = datetime.fromisoformat(t)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp() * 1000.0
    except Exception:
        return None


def _format_epoch_ms_to_csv_seconds(ts_ms: float) -> str:
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
) -> Iterator[bytes]:
    """Yield lines from the CSV file.

    If no_order is True, forward rows as-is (optionally skipping header). Otherwise,
    enforce strictly increasing ordering based on timestamp and optionally key(s).

    Any row whose timestamp is less than or equal to the last forwarded timestamp
    is ignored. Timestamp values are parsed as integers if possible, otherwise as
    floats. If parsing fails, the row is forwarded (conservative behavior).
    """

    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # Fast path: no ordering (still apply filtering if configured)
    filter_active = (filter_col_index is not None) and (filter_values is not None and len(filter_values) > 0)
    if no_order:
        if preload:
            # Preload file into memory as raw lines for fastest replay
            with csv_path.open("rb") as fb:
                raw_lines = fb.read().splitlines(keepends=True)
            while True:
                first = True
                for line in raw_lines:
                    if first:
                        first = False
                        if skip_header:
                            continue
                    # ensure newline terminator
                    if not line.endswith(b"\n"):
                        yield line + b"\n"
                    else:
                        yield line
                if not loop:
                    break
        else:
            # Stream directly from file descriptor without CSV parsing
            while True:
                with csv_path.open("rb") as fb:
                    first = True
                    for line in fb:
                        if first:
                            first = False
                            if skip_header:
                                continue
                        # line already includes trailing newline
                        yield line
                if not loop:
                    break
        return

    # Ordering-enabled path
    # When key_col_index is set, track last_ts per key; otherwise use global
    last_ts_global: float = float("-inf")
    last_ts_by_key: dict[tuple[str, ...], float] | None = {} if key_col_index else None
    total_forwarded = 0
    total_filtered = 0
    total_nudged = 0
    total_repaired = 0
    printed_samples = 0

    while True:
        rows_iter = None
        if preload:
            # Preload and iterate rows (as lists) to allow ordering checks
            with csv_path.open("r", newline="", encoding="utf-8") as f:
                rows_iter = list(csv.reader(f, delimiter=delimiter))
        else:
            f = csv_path.open("r", newline="", encoding="utf-8")
            rows_iter = csv.reader(f, delimiter=delimiter)

        try:
            first = True
            for row in rows_iter:
                # Optionally skip header
                if first:
                    first = False
                    if skip_header:
                        continue
                if not row:
                    continue


                # Optional filter by a column's value(s)
                if filter_active:
                    if filter_col_index < 0 or filter_col_index >= len(row):
                        continue
                    if row[filter_col_index].strip() not in filter_values:
                        continue
                # Optional filter by a column's value(s)
                if filter_active:
                    if filter_col_index < 0 or filter_col_index >= len(row):
                        continue
                    if row[filter_col_index].strip() not in filter_values:
                        continue

                # Defensive: guard index
                if ts_col_index < 0 or ts_col_index >= len(row):
                    # Forward as-is if we cannot determine timestamp
                    line = (delimiter.join(row) + "\n").encode("utf-8")
                    yield line
                    total_forwarded += 1
                    continue

                ts_raw = row[ts_col_index].strip()
                ts_val_ms = _parse_timestamp_to_epoch_ms(ts_raw)
                if ts_val_ms is None:
                    total_filtered += 1
                    if verbose:
                        msg = f"Filtered row ts(unparsable)='{ts_raw}'"
                        print(msg)
                    if filtered_log:
                        with open(filtered_log, "a", encoding="utf-8") as flog:
                            flog.write(f"unparsable	global			{ts_raw}	\n")
                    if sample_filtered > 0 and printed_samples < sample_filtered:
                        print(f"SAMPLE filtered: unparsable ts='{ts_raw}' row='{delimiter.join(row)}'")
                        printed_samples += 1
                    continue

                # Compute key (if configured), else use global
                if order_scope in ("per-key", "both") and last_ts_by_key is not None:
                    key = tuple(row[i].strip() if i < len(row) else "" for i in key_col_index)
                    prev = last_ts_by_key.get(key, float("-inf"))
                    if ts_val_ms <= prev:
                        if repair_monotonic_seconds > 0:
                            ts_val_ms = prev + (repair_monotonic_seconds * 1000)
                            row[ts_col_index] = str(int(ts_val_ms // 1000))
                            if verbose:
                                print(f"Repaired non-increasing ts for key={key}: {ts_raw} -> {row[ts_col_index]}")
                        elif ts_val_ms == prev and nudge_equal_seconds > 0:
                            ts_val_ms = prev + (nudge_equal_seconds * 1000)
                            row[ts_col_index] = str(int(ts_val_ms // 1000))
                            if verbose:
                                print(f"Nudged equal ts for key={key}: {ts_raw} -> {row[ts_col_index]}")
                        else:
                            total_filtered += 1
                            if verbose:
                                print(f"Filtered row key={key} ts={ts_raw} (<= last_ts={prev})")
                            continue
                    last_ts_by_key[key] = ts_val_ms
                if order_scope in ("global", "both"):
                    # Enforce strictly increasing timestamps globally
                    if ts_val_ms <= last_ts_global:
                        if repair_monotonic_seconds > 0:
                            ts_val_ms = last_ts_global + (repair_monotonic_seconds * 1000)
                            row[ts_col_index] = str(int(ts_val_ms // 1000))
                            if verbose:
                                print(f"Repaired non-increasing ts global: {ts_raw} -> {row[ts_col_index]}")
                        elif ts_val_ms == last_ts_global and nudge_equal_seconds > 0:
                            ts_val_ms = last_ts_global + (nudge_equal_seconds * 1000)
                            row[ts_col_index] = str(int(ts_val_ms // 1000))
                            if verbose:
                                print(f"Nudged equal ts global: {ts_raw} -> {row[ts_col_index]}")
                        else:
                            total_filtered += 1
                            if verbose:
                                print(f"Filtered row ts={ts_raw} (<= last_ts={last_ts_global})")
                            continue
                    last_ts_global = ts_val_ms
                line = (delimiter.join(row) + "\n").encode("utf-8")
                yield line
                total_forwarded += 1
        finally:
            if not preload and 'f' in locals():
                f.close()

        if verbose:
            print(
                f"Completed pass over {csv_path.name}: forwarded={total_forwarded}, filtered={total_filtered}, nudged={total_nudged}, repaired={total_repaired}"
            )
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
) -> None:
    # Compute pacing per batch if requested
    if delay > 0.0 and rows_per_sec > 0.0:
        raise ValueError("--delay and --rows-per-sec are mutually exclusive")
    per_row_delay = delay if delay > 0.0 else (1.0 / rows_per_sec if rows_per_sec > 0.0 else 0.0)

    batch: list[bytes] = []
    batch_bytes = 0
    last_send_ts = time.perf_counter()
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
        ):
            batch.append(line)
            batch_bytes += len(line)

            should_flush = len(batch) >= batch_size
            if not should_flush and max_batch_bytes > 0 and batch_bytes >= max_batch_bytes:
                should_flush = True

            if should_flush:
                rows_sent = len(batch)
                payload = b"".join(batch)
                client_sock.sendall(payload)
                batch.clear()
                batch_bytes = 0

                # Pacing: sleep proportional to number of rows sent
                if per_row_delay > 0.0:
                    target_elapsed = per_row_delay * rows_sent
                    # Keep schedule relative to last send to avoid drift
                    now = time.perf_counter()
                    wake_at = last_send_ts + target_elapsed
                    sleep_for = wake_at - now
                    if sleep_for > 0:
                        time.sleep(sleep_for)
                        last_send_ts = wake_at
                    else:
                        # If we're behind schedule, send immediately and reset anchor
                        last_send_ts = now

        # Flush any remaining rows
        if batch:
            client_sock.sendall(b"".join(batch))
    except (BrokenPipeError, ConnectionResetError):
        # Client disconnected mid-stream; simply stop sending.
        pass
    finally:
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
) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((host, port))
        server_sock.listen(1)

        if verbose:
            print(
                f"Streaming '{csv_path}' on {host}:{port} "
                f"(delay={delay}s, rows_per_sec={rows_per_sec}, batch_size={batch_size}, loop={loop}, "
                f"ts_col_index={ts_col_index}, delimiter='{delimiter}', skip_header={skip_header}, "
                f"key_col_index={key_col_index}, order_scope={order_scope}, tcp_nodelay={tcp_nodelay}, send_buffer={send_buffer}, "
                f"no_order={no_order}, preload={preload})"
            )
        while True:
            try:
                client_sock, addr = server_sock.accept()
            except KeyboardInterrupt:
                print("\nServer interrupted. Exiting.")
                break

            if verbose:
                print(f"Client connected from {addr[0]}:{addr[1]}")
            # Optional per-connection socket tuning
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
