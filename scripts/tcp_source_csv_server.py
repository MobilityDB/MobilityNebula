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
    return parser.parse_args()


def iter_csv_lines(csv_path: Path, loop: bool) -> Iterator[bytes]:
    """Yield lines from the CSV file, optionally looping forever."""

    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    while True:
        with csv_path.open("rb") as csv_file:
            for line in csv_file:
                # Ensure each row ends with a newline so TCP clients can split
                yield line if line.endswith(b"\n") else line + b"\n"
        if not loop:
            break


def stream_to_client(client_sock: socket.socket, csv_path: Path, delay: float, loop: bool) -> None:
    try:
        for line in iter_csv_lines(csv_path, loop):
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


def run_server(csv_path: Path, host: str, port: int, delay: float, loop: bool) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((host, port))
        server_sock.listen(1)

        print(f"Streaming '{csv_path}' on {host}:{port} (delay={delay}s, loop={loop})")
        while True:
            try:
                client_sock, addr = server_sock.accept()
            except KeyboardInterrupt:
                print("\nServer interrupted. Exiting.")
                break

            print(f"Client connected from {addr[0]}:{addr[1]}")
            stream_to_client(client_sock, csv_path, delay, loop)
            print(f"Client {addr[0]}:{addr[1]} disconnected")


def main() -> int:
    args = parse_arguments()
    try:
        run_server(args.csv_path, args.host, args.port, args.delay, args.loop)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\nInterrupted. Exiting.")
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())

