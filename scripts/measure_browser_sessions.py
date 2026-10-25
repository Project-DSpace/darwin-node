#!/usr/bin/env python3
"""Measure what browser sessions cost a node: memory per session, idle and
with its data lane carrying chunk reads, and the send rate of each read.

For each session count it starts `darwin-harness browser-node`: a two-node
devnet on loopback whose first node serves browsers (WebRTC Direct) under the
limits of a test config, with records of the given sizes stored on it. It then
runs `darwin-harness browser-load` against that node: the sessions open (ICE,
DTLS, SCTP, the post-quantum handshake and HELLO), stay idle, then each reads
records over its data channel as a browser does (`chunk_protocol` GETs, one at
a time per session) for --get-seconds, and close. The node's process memory is
sampled before the sessions open, while they are idle, during the reads and
after they close. The node runs in its own process, fresh for each count, so
nothing the client allocates or a previous count left behind is counted.

Memory per session is the slope of memory against the session count (a least
squares fit over the counts), so what every count shares (the devnet, its
second node, the records' cache, the byte budgets the reads fill) drops out.

The test config raises the per-address limits and the request capacity so
that every session can come from 127.0.0.1; the node-wide byte budget and its
bulk-read queue stay as configured (--max-in-flight-bytes).

Usage, from the repository root, after
`cargo build --release --features test-utils --bin darwin-harness`:

  python3 scripts/measure_browser_sessions.py
  python3 scripts/measure_browser_sessions.py --sessions 0,32,64,128,250 --get-seconds 30

Writes summary.json and summary.md under the work folder
(target/browser-sessions/<time> unless --work names one).
"""

import argparse
import datetime
import json
import os
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
EXE = ".exe" if os.name == "nt" else ""

# Records stored on the measured node: size in bytes, then how many.
DEFAULT_RECORDS = "4194304x16,1048576x16,262144x16,65536x16,4096x16"

# How long memory settles after a phase starts, and how often it is sampled.
SETTLE_SECONDS = 5
SAMPLE_SECONDS = 0.5
SAMPLES_AT_REST = 10


def process_memory(pid):
    """(private bytes, resident bytes) of process `pid`."""
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [
                ("cb", wintypes.DWORD),
                ("PageFaultCount", wintypes.DWORD),
                ("PeakWorkingSetSize", ctypes.c_size_t),
                ("WorkingSetSize", ctypes.c_size_t),
                ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPagedPoolUsage", ctypes.c_size_t),
                ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                ("PagefileUsage", ctypes.c_size_t),
                ("PeakPagefileUsage", ctypes.c_size_t),
                ("PrivateUsage", ctypes.c_size_t),
            ]

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        psapi = ctypes.WinDLL("psapi", use_last_error=True)
        kernel32.OpenProcess.restype = wintypes.HANDLE
        handle = kernel32.OpenProcess(0x1000 | 0x0010, False, pid)
        if not handle:
            raise OSError(f"cannot open process {pid}")
        try:
            counters = Counters()
            counters.cb = ctypes.sizeof(Counters)
            if not psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
                raise OSError(f"cannot read the memory of process {pid}")
            return counters.PrivateUsage, counters.WorkingSetSize
        finally:
            kernel32.CloseHandle(handle)
    private = resident = None
    with open(f"/proc/{pid}/smaps_rollup") as rollup:
        fields = {}
        for line in rollup:
            parts = line.split()
            if len(parts) >= 2 and parts[0].endswith(":"):
                fields[parts[0][:-1]] = int(parts[1]) * 1024 if parts[1].isdigit() else 0
        private = fields.get("Private_Clean", 0) + fields.get("Private_Dirty", 0)
        resident = fields.get("Rss", 0)
    return private, resident


def sample_at_rest(pid, count=SAMPLES_AT_REST):
    """The median of `count` samples, SAMPLE_SECONDS apart."""
    samples = []
    for _ in range(count):
        samples.append(process_memory(pid))
        time.sleep(SAMPLE_SECONDS)
    return {
        "private": statistics.median(s[0] for s in samples),
        "resident": statistics.median(s[1] for s in samples),
    }


def write_test_config(path, args):
    """A node config whose [webrtc_direct] lets every session come from one
    address, with the node-wide byte budget left as given."""
    sessions = args.max_sessions
    channels = 2
    lines = [
        "[webrtc_direct]",
        f"max_connections = {sessions}",
        f"max_connections_per_ip = {sessions - 1}",
        f"max_channels_per_connection = {channels}",
        f"max_channels = {sessions * channels}",
        f"max_concurrent_requests = {sessions * channels + 1}",
        f"max_requests_per_second = {args.max_rate + 1}",
        f"max_requests_per_second_per_ip = {args.max_rate}",
        "max_requests_per_second_per_connection = 16",
        f"max_in_flight_bytes = {args.max_in_flight_bytes}",
        f"max_in_flight_bytes_per_ip = {args.max_in_flight_bytes - 1}",
        f"new_sessions_burst_per_ip = {sessions * 4}",
        f"new_sessions_per_minute_per_ip = {sessions * 4}",
        "",
    ]
    path.write_text("\n".join(lines))


class Node:
    """`darwin-harness browser-node`, stopped by closing its standard input."""

    def __init__(self, harness, work, config, records):
        self.ready = work / "ready.json"
        self.ready.unlink(missing_ok=True)
        self.log = open(work / "node.log", "w")
        self.process = subprocess.Popen(
            [str(harness), "browser-node", "--config", str(config), "--records", records,
             "--ready", str(self.ready), "--snapshots", str(work / "snapshots.jsonl")],
            stdin=subprocess.PIPE, stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.time() + 180
        while not self.ready.exists():
            if self.process.poll() is not None:
                raise RuntimeError(f"browser-node exited; see {work / 'node.log'}")
            if time.time() > deadline:
                raise RuntimeError("browser-node was not ready within 3 minutes")
            time.sleep(0.5)
        time.sleep(0.5)
        self.info = json.loads(self.ready.read_text())
        self.pid = self.info["pid"]

    def stop(self):
        try:
            self.process.stdin.close()
            self.process.wait(timeout=60)
        except Exception:
            self.process.kill()
        self.log.close()


def run_count(args, harness, work, config, sessions):
    work.mkdir(parents=True, exist_ok=True)
    node = Node(harness, work, config, args.records)
    result = {"sessions": sessions}
    try:
        time.sleep(SETTLE_SECONDS)
        result["before"] = sample_at_rest(node.pid)
        if sessions == 0:
            result["idle"] = result["before"]
            result["reading"] = result["before"]
            result["after"] = result["before"]
            return result
        load = subprocess.Popen(
            [str(harness), "browser-load", "--ready", str(node.ready), "--sessions", str(sessions),
             "--get-seconds", str(args.get_seconds), "--max-rate", str(args.max_rate),
             "--out", str(work / "load.json")],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=open(work / "load.log", "w"),
            text=True, bufsize=1)

        def event():
            line = load.stdout.readline()
            if not line:
                raise RuntimeError(f"browser-load ended early; see {work / 'load.log'}")
            return json.loads(line)

        opened = event()
        result["opened"] = opened
        time.sleep(SETTLE_SECONDS)
        result["idle"] = sample_at_rest(node.pid)
        load.stdin.write("go\n")
        # Sample while the reads run, after they have filled the node's queues.
        samples = []
        done = {}

        def wait_done():
            done.update(event())

        waiter = threading.Thread(target=wait_done)
        waiter.start()
        started = time.time()
        while waiter.is_alive():
            if time.time() - started >= SETTLE_SECONDS:
                samples.append(process_memory(node.pid))
            time.sleep(SAMPLE_SECONDS)
        waiter.join()
        result["reads"] = done
        result["reading"] = {
            "private": statistics.median(s[0] for s in samples) if samples else None,
            "resident": statistics.median(s[1] for s in samples) if samples else None,
            "private_max": max((s[0] for s in samples), default=None),
        }
        load.stdin.write("go\n")
        closed = event()
        result["closed"] = closed
        load.wait(timeout=120)
        time.sleep(SETTLE_SECONDS)
        result["after"] = sample_at_rest(node.pid)
        result["load"] = json.loads((work / "load.json").read_text())["summary"]
        return result
    finally:
        node.stop()


def slope(points):
    """Least squares slope and intercept of (x, y) points."""
    xs = [p[0] for p in points]
    ys = [p[1] for p in points]
    mean_x, mean_y = statistics.fmean(xs), statistics.fmean(ys)
    sxx = sum((x - mean_x) ** 2 for x in xs)
    if sxx == 0:
        return None, None
    m = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)) / sxx
    return m, mean_y - m * mean_x


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bin", default=str(REPO / "target" / "release"),
                        help="the folder holding darwin-harness")
    parser.add_argument("--sessions", default="0,32,64,128,250",
                        help="session counts, each measured on a fresh node")
    parser.add_argument("--max-sessions", type=int, default=256,
                        help="the test config's session limit")
    parser.add_argument("--get-seconds", type=int, default=30)
    parser.add_argument("--max-rate", type=int, default=200,
                        help="reads a second across all sessions")
    parser.add_argument("--max-in-flight-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--records", default=DEFAULT_RECORDS)
    parser.add_argument("--work", help="where to write (default target/browser-sessions/<time>)")
    args = parser.parse_args()

    harness = Path(args.bin) / f"darwin-harness{EXE}"
    if not harness.exists():
        sys.exit(f"{harness} not found")
    stamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    work = Path(args.work) if args.work else REPO / "target" / "browser-sessions" / stamp
    work.mkdir(parents=True, exist_ok=True)
    config = work / "browser-test.toml"
    write_test_config(config, args)

    counts = [int(c) for c in args.sessions.split(",")]
    results = []
    for sessions in counts:
        print(f"measuring {sessions} sessions", flush=True)
        result = run_count(args, harness, work / f"sessions-{sessions}", config, sessions)
        results.append(result)
        (work / "summary.json").write_text(json.dumps({"results": results}, indent=2))

    fits = {}
    for phase in ("idle", "reading"):
        for kind in ("private", "resident"):
            points = [(r["sessions"], r[phase][kind] - r["before"][kind]) for r in results
                      if r[phase].get(kind) is not None]
            m, b = slope(points)
            fits[f"{phase}_{kind}"] = {"bytes_per_session": m, "intercept": b}
    summary = {"results": results, "fits": fits, "config": config.read_text(),
               "records": args.records, "get_seconds": args.get_seconds,
               "max_rate": args.max_rate}
    (work / "summary.json").write_text(json.dumps(summary, indent=2))

    rows = ["| sessions | opened | idle Δ private | reading Δ private | after Δ private | "
            "reads | MB/s per read (median, 4 MiB) | MB/s node |",
            "|---|---|---|---|---|---|---|---|"]
    for r in results:
        before = r["before"]["private"]
        load = r.get("load", {})
        rows.append(
            f"| {r['sessions']} | {r.get('opened', {}).get('opened', 0)} "
            f"| {(r['idle']['private'] - before) / 2**20:.1f} MiB "
            f"| {((r['reading']['private'] or before) - before) / 2**20:.1f} MiB "
            f"| {(r['after']['private'] - before) / 2**20:.1f} MiB "
            f"| {load.get('reads_ok', 0)} ok, {load.get('reads_failed', 0)} failed "
            f"| {load.get('full_read_mb_per_s_median', 0):.1f} "
            f"| {load.get('node_mb_per_s', 0):.1f} |")
    for name, fit in fits.items():
        if fit["bytes_per_session"] is not None:
            rows.append(f"\n{name}: {fit['bytes_per_session'] / 1024:.1f} KiB per session")
    (work / "summary.md").write_text("\n".join(rows) + "\n")
    print("\n".join(rows))
    print(f"written to {work}")


if __name__ == "__main__":
    main()
