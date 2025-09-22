#!/usr/bin/env python3
"""Simple TCP server that streams rows from a CSV file to clients.

Usage:
    python scripts/tcp_source_csv_server.py path/to/file.csv \
        --host 0.0.0.0 --port 32323 --delay 0.05 --loop

The server accepts a single client at a time. Once a client connects, the
configured CSV file is streamed line-by-line with the configured delay between
records. If `--loop` is supplied, the file is replayed indefinitely; otherwise
the connection is closed after the last row is sent. When the client
disconnects, the server waits for the next connection.
"""

from __future__ import annotations

import argparse
import socket
import sys
import time
from pathlib import Path
import csv
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
    return parser.parse_args()


def iter_filtered_csv_lines(
    csv_path: Path,
    loop: bool,
    delimiter: str,
    ts_col_index: int,
    skip_header: bool,
    verbose: bool,
    key_col_index: list[int] | None,
    order_scope: str,
) -> Iterator[bytes]:
    """Yield lines from the CSV file, enforcing strictly increasing order on timestamp column.

    Any row whose timestamp is less than or equal to the last forwarded timestamp
    is ignored. Timestamp values are parsed as integers if possible, otherwise as
    floats. If parsing fails, the row is forwarded (conservative behavior).
    """

    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    # When key_col_index is set, track last_ts per key; otherwise use global
    last_ts_global: float = float("-inf")
    last_ts_by_key: dict[tuple[str, ...], float] | None = {} if key_col_index else None
    total_forwarded = 0
    total_filtered = 0

    while True:
        with csv_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=delimiter)
            first = True
            for row in reader:
                # Optionally skip header
                if first:
                    first = False
                    if skip_header:
                        continue
                if not row:
                    continue

                # Defensive: guard index
                if ts_col_index < 0 or ts_col_index >= len(row):
                    # Forward as-is if we cannot determine timestamp
                    line = (delimiter.join(row) + "\n").encode("utf-8")
                    yield line
                    total_forwarded += 1
                    continue

                ts_raw = row[ts_col_index].strip()
                ts_val: float
                try:
                    # Prefer integer parse, then float
                    ts_val = float(int(ts_raw))
                except ValueError:
                    try:
                        ts_val = float(ts_raw)
                    except ValueError:
                        # Unknown timestamp format -> forward conservatively
                        line = (delimiter.join(row) + "\n").encode("utf-8")
                        yield line
                        total_forwarded += 1
                        continue

                # Compute key (if configured), else use global
                if order_scope in ("per-key", "both") and last_ts_by_key is not None:
                    key = tuple(row[i].strip() if i < len(row) else "" for i in key_col_index)
                    prev = last_ts_by_key.get(key, float("-inf"))
                    if ts_val <= prev:
                        total_filtered += 1
                        if verbose:
                            print(f"Filtered row key={key} ts={ts_raw} (<= last_ts={prev})")
                        continue
                    last_ts_by_key[key] = ts_val
                if order_scope in ("global", "both"):
                    # Enforce strictly increasing timestamps globally
                    if ts_val <= last_ts_global:
                        total_filtered += 1
                        if verbose:
                            print(f"Filtered row ts={ts_raw} (<= last_ts={last_ts_global})")
                        continue
                    last_ts_global = ts_val
                line = (delimiter.join(row) + "\n").encode("utf-8")
                yield line
                total_forwarded += 1

        if verbose:
            print(
                f"Completed pass over {csv_path.name}: forwarded={total_forwarded}, filtered(out-of-order)={total_filtered}"
            )
        if not loop:
            break


def stream_to_client(
    client_sock: socket.socket,
    csv_path: Path,
    delay: float,
    loop: bool,
    delimiter: str,
    ts_col_index: int,
    skip_header: bool,
    verbose: bool,
    key_col_index: list[int] | None,
    order_scope: str,
) -> None:
    try:
        for line in iter_filtered_csv_lines(csv_path, loop, delimiter, ts_col_index, skip_header, verbose, key_col_index, order_scope):
            client_sock.sendall(line)
            if delay > 0:
                time.sleep(delay)
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
    loop: bool,
    delimiter: str,
    ts_col_index: int,
    skip_header: bool,
    verbose: bool,
    key_col_index: list[int] | None,
    order_scope: str,
) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((host, port))
        server_sock.listen(1)

        print(
            f"Streaming '{csv_path}' on {host}:{port} "
            f"(delay={delay}s, loop={loop}, ts_col_index={ts_col_index}, delimiter='{delimiter}', "
            f"skip_header={skip_header}, key_col_index={key_col_index}, order_scope={order_scope})"
        )
        while True:
            try:
                client_sock, addr = server_sock.accept()
            except KeyboardInterrupt:
                print("\nServer interrupted. Exiting.")
                break

            print(f"Client connected from {addr[0]}:{addr[1]}")
            stream_to_client(client_sock, csv_path, delay, loop, delimiter, ts_col_index, skip_header, verbose, key_col_index, order_scope)
            print(f"Client {addr[0]}:{addr[1]} disconnected")


def main() -> int:
    args = parse_arguments()
    try:
        run_server(
            args.csv_path,
            args.host,
            args.port,
            args.delay,
            args.loop,
            args.delimiter,
            args.ts_col_index,
            args.skip_header,
            args.verbose,
            args.key_col_index,
            args.order_scope,
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
