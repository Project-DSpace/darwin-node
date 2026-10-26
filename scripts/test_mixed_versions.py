#!/usr/bin/env python3
"""The mixed-version harness: nodes of this tree and of the previous release
on one local network, before a node release goes to its canary.

Each run starts a devnet with `ant-devnet --enable-evm --manifest`: a local
Anvil chain with the payment contracts, and two in-process nodes that bootstrap
the network. The devnet's in-process nodes serve the chunk protocol only (they
run no replication), so the run then starts standalone `ant-node` processes
that join from the manifest (`--bootstrap`, `--evm-rpc-url`,
`--evm-payment-token`, `--evm-payment-vault`, a test network mode): first the
incumbents, of the devnet's release, then the joiners. Records of random
bytes are stored before and after the joiners join, each paid on the chain as
a client pays (`darwin-harness upload`), and read back at the end
(`darwin-harness check`).

The runs (`--runs`):

  all-old        the previous release throughout: the baseline, run
                 --baselines times (all-old, all-old-2, ...)
  new-hosts-old  this tree's devnet and incumbents; the previous release joins
  old-hosts-new  the previous release's devnet and incumbents; this tree joins
  all-new        this tree throughout (not run unless asked for)
  full           this tree throughout, with --full joiners whose disk reads as
                 full through the free-space seam (run when --full is given),
                 and the incumbents under disk pressure once the records are
                 stored, so they erase what they may (see "The full run")
  paged          this tree throughout, its incumbents admitting --busy-limit
                 sync requests at once through the admission seam, so that
                 joiners syncing with them together are answered busy (run
                 when --paged is given)
  rollback       this tree throughout, but one joiner, started again part way
                 on the previous release's binary with its data folder, then
                 on this tree's again (run when --rollback is given)

In the runs that page (every run but the all-old and full ones), this tree's
nodes cut their neighbour-sync hints into pages of --page-bytes of keys,
through the page-size seam, so that the few records of a run take more than
one page. And until the release that reads pages is cut, they announce that
release (NEIGHBOR_SYNC_PAGES_FROM_RELEASE, read from src/replication/config.rs)
through the announced-release seam: nodes built between releases announce
the last release's version, the version the previous release's nodes
announce too, so without it they would page to no one, and no lower paging
release could tell them from the previous release's nodes.

Every run must show, over the time from the joiners' start to the last read:
no message that failed to decode, in any node's log; no audit failed for a
reply that didn't decode; every record stored by a majority of its close
group, and still held at the end by a majority of the nodes that stored it
(a node that joins later gets a record only by repair, which waits for the
record to age, so the nodes nearest it now are counted but not required);
and every standalone node received neighbour sync, in single messages or
in pages. And of neighbour sync, as the requesters' logs show it, with the
releases the two nodes ran when each exchange started: no node paged its
hints to a node of the previous release, which can't read pages; in a run
that pages, this tree's nodes paged to each other; with nodes of the
previous release beside them, they sent those nodes single messages (pages,
if that release reads them); and no requester charged its responder anything
during an exchange the responder answered busy.

Each mixed run, and the rollback run, must also show no penalty heavier than
the all-old runs': the
penalties (saorsa-core's trust events that lower a peer's score) that the
devnet's nodes, the incumbents and the joiners charge the incumbents and the
joiners, counted for each pair of roles and each reason, may exceed the
highest all-old count by --penalty-slack (with a single all-old run, by
--penalty-ratio of its count instead, if that is more). And no new failure
reason: a reason no all-old run charged the incumbents (or the joiners)
with may come to --new-reason-allowance against them, and no more. The
devnet's nodes answer no replication, so every release finds them silent:
penalties on them are reported, not compared. So are the penalties for not
holding a record whose PUT the node refused; the refused PUTs are compared
instead, the same way. The report and summary.json list every count so
measured, with the highest all-old count, the excess over it and the limit,
within the limit or not, so a small excess shows before the canary. The
penalties one release's nodes charge the other's are reported by reason. And
the message shapes: each role received every message family it received at
least SHAPE_FLOOR times in every all-old run, and a family that only this
tree knows (--new-only-family) reached nodes of this tree and no others. The
paged run is measured for new failure reasons only: its pages of two keys
send far more messages than any all-old run.

The paged run must show, besides: an exchange in which each node's replica
hints and paid hints took more than one page, both ways; every exchange's
pages served from the hints its responder built at the first page, built
once; each exchange's pages served in order, the next only after the
last; and pages answered busy, with no penalty for them.

The rollback run must show, besides: the rolled-back node starting on the
previous release's binary with the data folder this tree's binary wrote, and
on this tree's again; while it ran the previous release, a single message to
it from a node of this tree, no page reaching it (no message that failed to
decode), and from each node at most one page, left unanswered, over the
connection its earlier process left; and once it came back, a page to it
from a node of this tree, answered. Each of the run's three phases (before,
during and after) lasts --rollback-minutes, and the last two go on, up to
ROLLBACK_WAIT_LIMIT minutes, until they show their exchange: a node syncs with
a peer at most once an hour. Penalties on the node from each of its stops
until RESTART_GRACE seconds after it is up again, for requests it couldn't
answer while it restarted, are reported, not compared.

The test-only seams (src/test_seams.rs) are compiled only into builds with the
`test-utils` feature, which both releases are built with here. The free-space
seam makes a node's disk read as full (the `full` run), and in that run the
full-notice seam lets a full node of this tree tell the others that it is
full, which they read from the release that brings the notice on; the
page-size, announced-release and admission seams are the ones above. The
harness refuses a run with a binary that lacks the seams it sets.

The full run. Every node of it runs with short timings through the seams
(neighbour sync every FULL_RUN_SYNC_SECS, an out-of-range wait of
FULL_RUN_HYSTERESIS_SECS, a "full" mark fresh for FULL_RUN_FRESHNESS_SECS),
so erasure in two phases goes ahead within the run. The --full first joiners
read a full disk from the start. After the uploads, and a few sync cycles, the
harness asks each record's nearest nodes (up to 20) for it: the early count.
It then puts every incumbent under pressure at the falling floor's last step,
above the full line, so each may erase the copies it holds out of its range;
asks again halfway through the observation (the mid count); stops the first
full joiner, whose mark then lapses at its peers; and asks again at the end.
It must show, beyond what every run shows:

- copies stay at 7: at each count, every record is held by at least 7 nodes
  not full, less the devnet's nodes among its 7 nearest not full (they take no
  replicas), and, at the end, less the stopped node where it is among them;
- stand-ins aren't erased: a node not full that holds a record at the early
  count from beyond the 9 nearest it, while full nodes widen its window, still
  holds it at the mid count, before any mark lapses;
- erasure goes ahead with 6 real proofs: every erasure a node logs needed at
  least 6 proofs, and had them;
- a lapsed mark counts as missing (C06): once the stopped node's mark has
  lapsed, no erasure counts more members marked full than full nodes still
  run.

The report lists, for each count, the records by how many of their 7 + f
nearest are full (f), so runs with --full 1, 2, 3 and 4 show which f each
reached. Run them one at a time (--work naming a folder for each), so a
failure names its f.

Usage, from the repository root (needs cargo, git, Python 3.8+, and Foundry's
`anvil` on PATH):

  python3 scripts/test_mixed_versions.py
  python3 scripts/test_mixed_versions.py --old-ref v0.23.0+ds.15 --observe-minutes 25
  python3 scripts/test_mixed_versions.py --old-bin path/to/old --new-bin path/to/new
  python3 scripts/test_mixed_versions.py --runs paged --observe-minutes 10
  python3 scripts/test_mixed_versions.py --paged --rollback

The previous release is the newest v*+ds.* tag before HEAD, built from an
export of the tree at that tag under target/mv/ with its own target folder,
kept for later runs. --old-bin and --new-bin name folders already holding the
binaries instead. Where memory is short for the release profile's link,
--cargo-arg=-j1 links one binary at a time. Everything a run writes is kept
under the work folder (target/mv/runs/<time> unless --work names one); the
summary is summary.json there. Exits 0 when every run passes.

With the defaults a run takes about 30 minutes, most of it --observe-minutes,
and the harness makes four: two all-old runs, then the two mixed ones. The
rollback run takes 40 minutes or more, up to about two and a half hours.
The standalone nodes run in testnet mode, which binds every interface; on a
desktop whose firewall asks about that, --network-mode development keeps them
on loopback. A run during which the machine stood still (asleep, say) fails
as not comparable; on Windows the harness asks the machine not to sleep
while it runs. One run can be made again into the same --work folder after
removing its own folder there: the verdict reads every run the folder holds.
"""

import argparse
import datetime
import io
import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import tarfile
import threading
import time
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
EXE = ".exe" if os.name == "nt" else ""

# A majority of a close group of 7 (CLOSE_GROUP_MAJORITY).
MAJORITY = 4

# Record sizes, cycled: small and full-size chunks (MAX_CHUNK_SIZE is 4 MiB).
RECORD_SIZES = [4096, 65536, 1048576, 4194304]

# How far a mixed run's penalties (or refused PUTs) may exceed the all-old
# runs' before the run fails, for each pair of roles and reason: by
# PENALTY_SLACK above the highest all-old count. Counts vary between runs of
# one release: a possession check that times out on a busy machine charges
# its peer twice. In six runs of v0.23.0+ds.15 against itself (two harness
# runs of three runs each, on one machine), one pair of roles' count for one
# reason ranged from 0 to 10, and a mixed run's exceeded its all-old run's by
# up to 9. A message one release can't read fails every exchange of it, tens
# to hundreds in a run, and leaves decode failures besides.
PENALTY_SLACK = 10

# Against a single all-old run, a count may also exceed it by this share of
# it, if that is more than PENALTY_SLACK. None until a spread in proportion
# to the count is measured: the counts compared in the six runs were 12 or
# fewer, and spread within the slack.
PENALTY_RATIO = 0.0

# Penalties for a reason no all-old run charged a role with (the incumbents,
# or the joiners), from any observer: a new failure reason, failed when
# there are more than this against that role. In the six runs above,
# measured as the harness measures them (each mixed run against the all-old
# run made with it), such a reason came to at most 2.
NEW_REASON_ALLOWANCE = 3

# All-old runs the mixed runs are measured against (all-old, all-old-2, ...).
# A role's zero for a reason in one all-old run can be noise: of the 30
# comparisons of one of the six runs against another, 2 would have failed as
# a new reason (application failures on the incumbents, none in one run, 4
# and 10 in others); of the 60 against two of them, none.
BASELINES = 2

# A run fails when the machine stood still this long, asleep say: every
# node's timers jump at once then, and the run can't be compared.
STALL_LIMIT = 30

# A message family a role received at least this often in the all-old run
# must reach it in a mixed run too. Rarer ones come and go between runs of one
# release.
SHAPE_FLOOR = 10

# The longest an upload or a read-back may take: each record's lookups and
# requests time out well inside it.
CLIENT_TIMEOUT = 1800

# The seam whose name, found in a binary, shows it was built with test-utils.
FREE_SPACE_SEAM = "ANT_TEST_SEAM_FREE_SPACE_FILE"

# The seams this tree's nodes are given in the runs that page: the bytes of
# keys on one neighbour-sync page; the release they announce in their user
# agent, as its +ds number; and, for the paged run's incumbents, the sync
# requests a node admits at once.
PAGE_SEAM = "ANT_TEST_SEAM_SYNC_PAGE_BYTES"
ANNOUNCED_SEAM = "ANT_TEST_SEAM_ANNOUNCED_RELEASE"
ADMISSION_SEAM = "ANT_TEST_SEAM_SYNC_ADMISSION_LIMIT"

# The runs in which this tree's nodes page, and those with nodes of the
# previous release beside them.
PAGING_RUNS = ("new-hosts-old", "old-hosts-new", "all-new", "paged", "rollback")
OLD_RUNS = ("new-hosts-old", "old-hosts-new", "rollback")

# The family of a sync page request: a binary that names it reads pages,
# whatever features it was built with.
PAGE_REQUEST_FAMILY = "neighbor_sync_page_request"

# Where this tree sets the first release whose nodes read pages.
PAGING_RELEASE = re.compile(r"pub const NEIGHBOR_SYNC_PAGES_FROM_RELEASE: u64 = (\d+);")
# A version's release: n of <version>+ds.<n>.
RELEASE_NUMBER = re.compile(r"\+ds\.(\d+)")

# Bytes of keys on a page in the runs that page: two keys, so that each list
# of a run's records takes more than one page.
PAGE_BYTES = 64

# Sync requests the paged run's incumbents admit at once (8 without the
# seam), so that joiners syncing with them together are answered busy.
BUSY_LIMIT = 1

# The rollback run: each of its three phases lasts --rollback-minutes, and the
# last two go on, up to ROLLBACK_WAIT_LIMIT minutes, until they show the
# exchange they must: a node syncs with a peer at most once an hour, so a
# phase may wait out that hour for the nodes that synced with the rolled-back
# node last.
ROLLBACK_MINUTES = 10
ROLLBACK_WAIT_LIMIT = 70

# Penalties on the rolled-back node from each of its stops until this many
# seconds after it is up again, for requests it couldn't answer while it
# restarted (a request waits 30 seconds at most): reported, not compared, as
# no all-old run restarts a node.
RESTART_GRACE = 60

# A penalty a requester charges its responder up to this many seconds after
# an exchange the responder answered busy is charged for that exchange.
BUSY_CHARGE_GRACE = 1.0

# The message families of paged neighbour sync, and the single-message family
# each takes the place of between two nodes that read pages.
PAGE_FAMILIES = {"neighbor_sync_page_request": "neighbor_sync_request",
                 "neighbor_sync_page_response": "neighbor_sync_response"}

# The lines a node logs for each exchange it starts, paged or single; for the
# hints it builds as a responder at an exchange's first page; for each page
# it serves; and for each page it answers busy.
PAGED_EXCHANGE_LINE = "Neighbor-sync paged exchange"
SINGLE_EXCHANGE_LINE = "Neighbor-sync single exchange"
HINT_SET_LINE = "Neighbor-sync hint set built"
PAGE_SERVED_LINE = "Neighbor-sync page served"
BUSY_LINE = "Neighbor-sync page answered busy"

# The seam naming the first release whose nodes a full node tells it is full
# (src/test_seams.rs). This tree's nodes announce the last release's version
# until the release that brings the notice is cut, so in the `full` run, where
# every node is of this tree, it is set to 0 on each node whose binary has it:
# a full node then tells the others, as it will once released.
FULL_NOTICE_SEAM = "ANT_TEST_SEAM_FULL_NOTICE_FROM_RELEASE"

# The full run's timing seams (src/test_seams.rs), set on every node of the
# run, so that the several sync cycles an erasure takes, the out-of-range
# wait and a mark's lapse all fall within it. A sync cycle every 30 to 60 s
# gives an erasure (selected, then tried two epochs later) within a few
# minutes; a mark fresh for 120 s, renewed every 30 s, lapses two minutes
# after its node stops.
NEIGHBOR_SYNC_SEAM = "ANT_TEST_SEAM_NEIGHBOR_SYNC_SECS"
HYSTERESIS_SEAM = "ANT_TEST_SEAM_PRUNE_HYSTERESIS_SECS"
FRESHNESS_SEAM = "ANT_TEST_SEAM_FULL_FRESHNESS_SECS"
FULL_RUN_SYNC_SECS = 30
FULL_RUN_HYSTERESIS_SECS = 60
FULL_RUN_FRESHNESS_SECS = 120

# The incumbents' disk under pressure in the full run: 2 GiB free of a 1 TB
# partition, under the falling floor's last step (2.5%), so a node may erase
# anything out of its range, and above the full line (1 GiB), so it isn't
# full.
PRESSURED_FREE = 2 * 1024 ** 3
PRESSURED_TOTAL = 10 ** 12

# A key's close group, the storage admission width, the proofs an erasure
# needs from the 7 nearest not full, and the widest a window grows.
CLOSE_GROUP = 7
ADMISSION_WIDTH = 9
ERASE_PROOFS = 6
WIDEST = 20

# The line a node logs for each record it erases, with its proofs.
ERASED_LINE = "Erased an out-of-range record under disk pressure"

# What each node logs: info, and debug for the code whose debug lines name
# messages that didn't decode (src/replication, the chunk handler, listings).
LOG_FILTER = ("info,ant_node::replication=debug,ant_node::storage::handler=debug,"
              "ant_node::listings=debug")

# Lines that say a message from another node didn't decode, or wasn't the
# reply asked for.
DECODE_FAILURE = re.compile(
    r"decode error|failed to decode|doesn't decode|wrong id for its family"
    r"|unexpected response type|deserialization failed",
    re.IGNORECASE)

# saorsa-core's line for every change of a peer's trust score; a penalty is
# one that lowers it.
TRUST_LINE = "peer trust score changed"

# The cumulative summary lines a node logs every 5 minutes.
TRAFFIC_LINE = "replication traffic summary (cumulative)"
AUDIT_LINE = "audit outcome summary (cumulative)"

ANSI = re.compile(r"\x1b\[[0-9;]*m")
TIMESTAMP = re.compile(r"^(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(\.\d+)?Z")
FIELD = re.compile(r'(\w+)=("[^"]*"|\S+)')
PEER_ID_LINE = re.compile(r"Node identity resolved.*?peer_id=([0-9a-f]{64})")
# A replication request another node didn't answer in time: which kind, and
# to whom (the sender's debug lines).
UNANSWERED = re.compile(r"(Sync with|Verification request to|Fetch request to) "
                        r"([0-9a-f]{64}) failed: Operation timed out")
UNANSWERED_KIND = {"Sync with": "neighbour sync", "Verification request to": "verification",
                   "Fetch request to": "fetch"}
# A possession check a peer failed: the peer and the key.
POSSESSION_FAILED = re.compile(r"Possession check: ([0-9a-f]{64}) failed to prove possession "
                               r"for ([0-9a-f]{64})")
RUNS = ("all-old", "new-hosts-old", "old-hosts-new", "all-new", "full", "paged", "rollback")
# The all-old runs: all-old, and all-old-2 on when there are more.
BASELINE_RUN = re.compile(r"all-old(?:-([2-9]|[1-9][0-9]+))?")


def log(message):
    stamp = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{stamp}] {message}", flush=True)


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc)


class HarnessError(Exception):
    pass


class StallWatch:
    """Notes the longest time the machine stood still while a run went on
    (asleep, say), from a clock read every second."""

    def __init__(self):
        self.longest = 0.0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._watch, daemon=True)
        self._thread.start()

    def _watch(self):
        last = time.time()
        while not self._stop.wait(1):
            now = time.time()
            self.longest = max(self.longest, now - last - 1)
            last = now

    def stop(self):
        self._stop.set()
        self._thread.join()
        return round(self.longest)


def keep_awake():
    """Asks Windows not to sleep while the harness runs: a desktop left alone
    would otherwise sleep through a run. The request ends with the process."""
    if os.name != "nt":
        return
    try:
        import ctypes
        es_continuous, es_system_required = 0x80000000, 0x00000001
        ctypes.windll.kernel32.SetThreadExecutionState(es_continuous | es_system_required)
    except (AttributeError, OSError):
        pass


# --------------------------------------------------------------------------
# Binaries
# --------------------------------------------------------------------------

def profile_dir(profile):
    return {"dev": "debug", "test": "debug", "release": "release"}.get(profile, profile)


def git(*args, cwd=REPO):
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True,
                          text=True).stdout.strip()


def previous_release():
    """The newest release tag before HEAD."""
    return git("describe", "--tags", "--abbrev=0", "--match", "v*+ds.*", "HEAD^")


def paging_release():
    """The first release whose nodes read paged sync, as this tree sets it, or
    None for a tree without pages."""
    try:
        found = PAGING_RELEASE.search(
            (REPO / "src" / "replication" / "config.rs").read_text(encoding="utf-8"))
    except OSError:
        return None
    return int(found.group(1)) if found else None


def cargo_build(root, bins, profile, cargo_args, env=None):
    command = ["cargo", "build", "--locked", "--profile", profile, "--features", "test-utils"]
    for name in bins:
        command += ["--bin", name]
    command += cargo_args
    log(f"building {', '.join(bins)} in {root} ({profile})")
    result = subprocess.run(command, cwd=root, env=env)
    if result.returncode != 0:
        raise HarnessError(f"cargo build failed in {root}")


def build_new(args):
    if args.new_bin:
        return Path(args.new_bin)
    cargo_build(REPO, ["ant-node", "ant-devnet", "darwin-harness"], args.profile, args.cargo_arg)
    # Where cargo put them: CARGO_TARGET_DIR when set, as on a build machine.
    target = Path(os.environ.get("CARGO_TARGET_DIR") or REPO / "target")
    return target / profile_dir(args.profile)


def build_old(args, cache):
    """The previous release's binaries: given, or built from an export of its
    tree, kept under `cache` for later runs."""
    if args.old_bin:
        return Path(args.old_bin), args.old_ref or "given binaries"
    ref = args.old_ref or previous_release()
    commit = git("rev-parse", f"{ref}^{{commit}}")
    tree = cache / commit[:12]
    stamp = tree / ".harness-build"
    # The job count changes how a build runs, not what it makes.
    settings = [arg for arg in args.cargo_arg
                if not (arg.startswith("-j") or arg.startswith("--jobs"))]
    wanted = json.dumps({"commit": commit, "profile": args.profile, "cargo_args": settings})
    bins = tree / "target" / profile_dir(args.profile)
    if stamp.exists() and stamp.read_text() == wanted and all(
            (bins / f"{name}{EXE}").exists() for name in ("ant-node", "ant-devnet")):
        log(f"previous release {ref} ({commit[:12]}): built already")
        return bins, f"{ref} ({commit[:12]})"
    if not (tree / "Cargo.toml").exists():
        log(f"exporting {ref} ({commit[:12]}) to {tree}")
        tree.mkdir(parents=True, exist_ok=True)
        archive = subprocess.run(["git", "archive", "--format=tar", commit], cwd=REPO,
                                 check=True, capture_output=True).stdout
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
            if hasattr(tarfile, "data_filter"):
                tar.extractall(tree, filter="data")
            else:
                tar.extractall(tree)
    # The export sits inside this repository; without the ceiling, its build
    # script would read this checkout's commit as its own.
    env = dict(os.environ, GIT_CEILING_DIRECTORIES=str(cache))
    cargo_build(tree, ["ant-node", "ant-devnet"], args.profile, args.cargo_arg, env)
    stamp.write_text(wanted)
    return bins, f"{ref} ({commit[:12]})"


class Release:
    """One release's binaries."""

    def __init__(self, name, label, folder):
        self.name = name
        self.label = label
        self.node = folder / f"ant-node{EXE}"
        self.devnet = folder / f"ant-devnet{EXE}"
        for binary in (self.node, self.devnet):
            if not binary.exists():
                raise HarnessError(f"{binary} is missing")
        self.version = subprocess.run([str(self.node), "--version"], capture_output=True,
                                      text=True).stdout.strip()
        binary = self.node.read_bytes()
        self.seams = FREE_SPACE_SEAM.encode() in binary
        self.page_seams = PAGE_SEAM.encode() in binary
        self.announce_seam = ANNOUNCED_SEAM.encode() in binary
        self.admission_seam = ADMISSION_SEAM.encode() in binary
        self.reads_pages = PAGE_REQUEST_FAMILY.encode() in binary
        number = RELEASE_NUMBER.search(self.version)
        self.number = int(number.group(1)) if number else None
        self.full_notice_seam = FULL_NOTICE_SEAM.encode() in binary
        self.timing_seams = all(seam.encode() in binary for seam in
                                (NEIGHBOR_SYNC_SEAM, HYSTERESIS_SEAM, FRESHNESS_SEAM))

    def __str__(self):
        return f"{self.name}: {self.version}, {self.label}"


# --------------------------------------------------------------------------
# Processes
# --------------------------------------------------------------------------

class Process:
    def __init__(self, name, command, log_path, env):
        self.name = name
        self.log_path = log_path
        self.log_file = open(log_path, "ab")
        flags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        self.popen = subprocess.Popen(command, stdout=self.log_file, stderr=subprocess.STDOUT,
                                      stdin=subprocess.DEVNULL, env=env, creationflags=flags,
                                      start_new_session=os.name != "nt")

    def alive(self):
        return self.popen.poll() is None

    def children(self):
        """The process's children, on Linux, so they can be stopped with it."""
        path = Path(f"/proc/{self.popen.pid}/task/{self.popen.pid}/children")
        try:
            return [int(pid) for pid in path.read_text().split()]
        except OSError:
            return []

    def signal(self):
        """Asks the process to stop: SIGINT, which the devnet and the node
        both stop on cleanly. No console signal reaches one process alone on
        Windows, so there the whole tree is stopped (the devnet's Anvil with
        it)."""
        if not self.alive():
            return
        if os.name == "nt":
            subprocess.run(["taskkill", "/PID", str(self.popen.pid), "/T", "/F"],
                           capture_output=True)
        else:
            self.stray = self.children()
            self.popen.send_signal(signal.SIGINT)

    def stop(self):
        """Waits for the process to stop, and kills it if it doesn't, then
        any Anvil it started that is still running."""
        try:
            self.popen.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.popen.kill()
            self.popen.wait(timeout=10)
        for pid in getattr(self, "stray", []):
            try:
                command = Path(f"/proc/{pid}/cmdline").read_bytes()
            except OSError:
                continue
            if b"anvil" in command:
                try:
                    os.kill(pid, signal.SIGKILL)
                except OSError:
                    pass
        self.log_file.close()


def stop_all(processes):
    """Stops every process: each is asked first, then all are waited for."""
    for process in reversed(processes):
        process.signal()
    for process in reversed(processes):
        process.stop()


def wait_for(what, check, timeout, process=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = check()
        if value:
            return value
        if process is not None and not process.alive():
            raise HarnessError(f"{process.name} exited while waiting for {what}; "
                               f"see {process.log_path}")
        time.sleep(0.5)
    raise HarnessError(f"timed out waiting for {what}")


def socket_address(multiaddr):
    """`/ip4/127.0.0.1/udp/1234/quic` as `127.0.0.1:1234`."""
    parts = multiaddr.strip("/").split("/")
    if len(parts) >= 4 and parts[0] == "ip4" and parts[2] == "udp":
        return f"{parts[1]}:{parts[3]}"
    raise HarnessError(f"can't read the bootstrap address {multiaddr}")


# --------------------------------------------------------------------------
# One run
# --------------------------------------------------------------------------

class Node:
    def __init__(self, name, role, release, root, full, rewards):
        self.name = name
        self.role = role
        self.release = release
        self.process = None
        self.root = root
        self.full = full
        self.rewards = rewards
        self.peer_id = None


class ExchangeWatch:
    """Follows logs for the neighbour-sync exchanges their nodes start with
    one peer, as they are written."""

    def __init__(self, logs, peer_id):
        self.offsets = {path: 0 for path in logs}
        self.peer_id = peer_id
        self.exchanges = []

    def poll(self):
        """Reads what the logs gained since the last poll; returns every
        exchange with the peer seen so far."""
        for path, offset in self.offsets.items():
            try:
                with open(path, "rb") as handle:
                    handle.seek(offset)
                    data = handle.read()
            except OSError:
                continue
            whole = data.rfind(b"\n") + 1
            self.offsets[path] = offset + whole
            for raw in data[:whole].splitlines():
                text = ANSI.sub("", raw.decode("utf-8", "replace"))
                exchange = exchange_in(text)
                if exchange and exchange.get("peer") == self.peer_id:
                    self.exchanges.append(exchange)
        return self.exchanges

    def seen(self, form, since, answered=False):
        """The exchanges in `form` ("paged" or "single") that started at
        `since` or later, with a page answered if `answered`."""
        return [exchange for exchange in self.poll()
                if exchange["form"] == form and exchange["start"] >= since
                and (not answered or exchange.get("pages_answered", 0) >= 1)]


def paging_plan(name, new, old, paging_from, args):
    """What this tree's nodes are given in run `name` so that they page, and
    what its neighbour sync must then show. Until the release that reads
    pages is cut, this tree's nodes announce the last release's version, as
    the previous release's nodes do: without the announced-release seam they
    would page to no one, and no lower paging release would tell them from
    the previous release's nodes."""
    pages = bool(new.reads_pages and paging_from is not None)
    native = pages and new.number is not None and new.number >= paging_from
    announced = paging_from if pages and not native and name in PAGING_RUNS else None
    return {
        "announced": announced,
        "page_bytes": args.page_bytes if pages and name in PAGING_RUNS else None,
        "busy_limit": args.busy_limit if name == "paged" and args.busy_limit else None,
        "new_pages": bool(native or announced) and not is_baseline(name),
        "old_present": name in OLD_RUNS,
        "old_reads_pages": bool(old is not None and old.reads_pages),
    }


def run_one(name, network_release, joiner_release, args, harness, folder, old=None,
            paging=None):
    """Runs one scenario under `folder`, and records there (run.json) what the
    analysis needs. `old` is the previous release, which the rollback run
    starts one of this tree's nodes on part way; `paging` is what this tree's
    nodes are given to page (paging_plan)."""
    folder.mkdir(parents=True)
    env = dict(os.environ, RUST_LOG=LOG_FILTER, NO_COLOR="1",
               XDG_DATA_HOME=str(folder / "xdg"))
    # Nodes started by the harness don't upgrade themselves. A node of ds.15
    # or earlier first checks for a release a check interval after it
    # starts, a year away here; a later one checks within minutes, and takes
    # a release at its publication plus its rollout offset, somewhere in a
    # window of a year here. A run's nodes are of the newest published
    # release or newer, so no check finds a newer one unless one is published
    # during the run.
    node_config = folder / "node.toml"
    node_config.write_text("[upgrade]\ncheck_interval_hours = 8760\n"
                           "staged_rollout_hours = 8760\n")
    processes = []
    nodes = []
    timeline = {"started": utc_now().isoformat()}
    stall = StallWatch()
    full_count = args.full if name == "full" else 0
    paging = paging or {}
    rolled = None
    try:
        manifest_path = folder / "manifest.json"
        devnet_data = folder / "devnet"
        devnet = Process("devnet", [
            str(network_release.devnet), "--enable-evm", "--manifest", str(manifest_path),
            "--nodes", "2", "--bootstrap-count", "1", "--data-dir", str(devnet_data),
            "--enable-logging", "--log-level", "info",
        ], folder / "devnet.log", env)
        processes.append(devnet)
        wait_for("the devnet's manifest", manifest_path.exists, 300, devnet)
        time.sleep(1)
        manifest = json.loads(manifest_path.read_text())
        evm = manifest.get("evm") or {}
        bootstrap = [socket_address(address) for address in manifest["bootstrap"]]
        log(f"{name}: devnet up ({network_release.name}), bootstrap {', '.join(bootstrap)}")

        def node_env(node, release):
            """The environment `node` runs `release`'s binary with."""
            values = dict(env)
            if release.name == "new":
                # Small sync pages, and the release that reads pages
                # announced until it is cut: see paging_plan.
                if paging.get("page_bytes"):
                    values[PAGE_SEAM] = str(paging["page_bytes"])
                if paging.get("announced"):
                    values[ANNOUNCED_SEAM] = str(paging["announced"])
                if paging.get("busy_limit") and node.role == "incumbent":
                    values[ADMISSION_SEAM] = str(paging["busy_limit"])
            if name == "full" and release.full_notice_seam:
                values[FULL_NOTICE_SEAM] = "0"
            if name == "full":
                values[NEIGHBOR_SYNC_SEAM] = str(FULL_RUN_SYNC_SECS)
                values[HYSTERESIS_SEAM] = str(FULL_RUN_HYSTERESIS_SECS)
                values[FRESHNESS_SEAM] = str(FULL_RUN_FRESHNESS_SECS)
            if node.full or (name == "full" and node.role == "incumbent"):
                values[FREE_SPACE_SEAM] = str(folder / "seams" / f"{node.name}.free")
            return values

        def launch(node, release):
            """Starts `release`'s binary as `node`, on its data folder."""
            command = [
                str(release.node), "--root-dir", str(node.root), "--port", "0", "--ipv4-only",
                "--network-mode", args.network_mode, "--metrics-port", "0",
                "--evm-rpc-url", evm["rpc_url"],
                "--evm-payment-token", evm["payment_token_address"],
                "--evm-payment-vault", evm["payment_vault_address"],
                "--rewards-address", node.rewards,
                "--config", str(node_config), "--enable-logging", "--log-level", "info",
            ]
            if args.network_mode == "development":
                command += ["--webrtc-direct-bind", "127.0.0.1:0"]
            for address in bootstrap:
                command += ["--bootstrap", address]
            node.process = Process(node.name, command, folder / f"{node.name}.log",
                                   node_env(node, release))
            processes.append(node.process)

        def start(role, index, release, full):
            node_name = f"{role}-{index:02d}"
            root = folder / "nodes" / node_name
            root.mkdir(parents=True)
            if full or (name == "full" and role == "incumbent"):
                # A full joiner reads no free space; an incumbent of the full
                # run reads the host's own until it is put under pressure.
                seam = folder / "seams" / f"{node_name}.free"
                seam.parent.mkdir(exist_ok=True)
                disk = shutil.disk_usage(folder)
                seam.write_text(f"0 {disk.total}\n" if full else f"{disk.free} {disk.total}\n")
            node = Node(node_name, role, release, root, full,
                        "0x" + "d1" * 18 + f"{len(nodes) + 1:04x}")
            nodes.append(node)
            launch(node, release)
            return node

        def started(group):
            for node in group:
                node.peer_id = wait_for(
                    f"{node.name}'s identity",
                    lambda node=node: peer_id_in(node.process.log_path), 120, node.process)

        # Nodes the full run stops on purpose.
        stopped = set()

        def observe(minutes, until=None, most=None):
            """Lets the nodes run for `minutes`, and then, while `until` finds
            nothing, on up to `most` minutes in all; fails when a node stops
            but one stopped on purpose. Returns what `until` found last."""
            began = time.monotonic()
            found = None
            while True:
                for process in processes:
                    if process.name not in stopped and not process.alive():
                        raise HarnessError(f"{process.name} exited; see {process.log_path}")
                passed = (time.monotonic() - began) / 60
                if passed >= minutes:
                    if until is None:
                        return None
                    found = until()
                    if found or passed >= most:
                        return found
                time.sleep(5)

        def restart(node, release):
            """Stops `node` and starts `release`'s binary as it, on its data
            folder; returns when the new process has logged its identity,
            with the times the node stopped and came up."""
            offset = node.process.log_path.stat().st_size
            stopped = utc_now()
            node.process.signal()
            node.process.stop()
            processes.remove(node.process)
            launch(node, release)
            wait_for(f"{node.name}'s identity on {release.name}",
                     lambda: peer_id_in(node.process.log_path, offset), 120, node.process)
            return stopped, utc_now()

        def roll_back(node):
            """The rollback run's three phases: `node` on this tree's binary,
            then on the previous release's, then on this tree's again, each
            until the nodes of this tree show what they must of it."""
            new = node.release
            watch = ExchangeWatch([folder / f"{other.name}.log" for other in nodes
                                   if other.release.name == "new" and other is not node],
                                  node.peer_id)
            log(f"{name}: observing for {args.rollback_minutes} min before rolling back")
            observe(args.rollback_minutes)
            log(f"{name}: {node.name} started again on {old.name} ({old.version}), its data "
                "folder kept")
            stopped_old, up_old = restart(node, old)
            found = observe(args.rollback_minutes, lambda: watch.seen("single", up_old),
                            ROLLBACK_WAIT_LIMIT)
            log(f"{name}: {len(found or [])} single exchange(s) to {node.name} from this "
                "tree's nodes while it ran the previous release")
            stopped_new, up_new = restart(node, new)
            log(f"{name}: {node.name} back on this tree's binary")
            found = observe(args.rollback_minutes,
                            lambda: watch.seen("paged", up_new, answered=True),
                            ROLLBACK_WAIT_LIMIT)
            log(f"{name}: {len(found or [])} paged exchange(s) to {node.name} since it came "
                "back")
            return {"node": node.name, "peer_id": node.peer_id,
                    "old_from": stopped_old.isoformat(), "old_until": stopped_new.isoformat(),
                    "restarts": [[stopped_old.isoformat(), up_old.isoformat()],
                                 [stopped_new.isoformat(), up_new.isoformat()]]}

        incumbents = [start("incumbent", index, network_release, False)
                      for index in range(args.incumbents)]
        started(incumbents)
        log(f"{name}: {len(incumbents)} incumbents up; settling {args.settle_seconds} s")
        time.sleep(args.settle_seconds)

        uploads = []
        uploads.append(upload(harness, manifest_path, folder / "uploads-before.json",
                              args.records_before))
        timeline["joiners"] = utc_now().isoformat()
        joiners = [start("joiner", index, joiner_release, index < full_count)
                   for index in range(args.joiners)]
        started(joiners)
        log(f"{name}: {len(joiners)} joiners up ({joiner_release.name}); settling "
            f"{args.join_seconds} s")
        time.sleep(args.join_seconds)
        uploads.append(upload(harness, manifest_path, folder / "uploads-after.json",
                              args.records_after))

        def check_all(phase):
            suffix = f"-{phase}" if phase else ""
            return [check(harness, manifest_path, path,
                          folder / f"check{suffix}-{path.stem}.json") for path in uploads]

        phases = {}
        if name == "rollback":
            rolled = roll_back(joiners[0] if joiners else incumbents[0])
        elif name == "full":
            # A few sync cycles for the fresh offers and the full nodes'
            # notices to land, then the early count.
            log(f"{name}: waiting {4 * FULL_RUN_SYNC_SECS} s, then counting copies")
            observe(4 * FULL_RUN_SYNC_SECS / 60)
            phases["early"] = [path.name for path in check_all("early")]
            for node in incumbents:
                (folder / "seams" / f"{node.name}.free").write_text(
                    f"{PRESSURED_FREE} {PRESSURED_TOTAL}\n")
            timeline["pressure"] = utc_now().isoformat()
            first_half = args.observe_minutes / 2
            log(f"{name}: incumbents under pressure; observing for {first_half:g} min")
            observe(first_half)
            phases["mid"] = [path.name for path in check_all("mid")]
            full_joiners = [node for node in joiners if node.full]
            victim = full_joiners[0]
            victim.process.signal()
            victim.process.stop()
            stopped.add(victim.process.name)
            timeline["stopped"] = {"node": victim.name, "peer_id": victim.peer_id,
                                   "at": utc_now().isoformat()}
            second_half = max(args.observe_minutes / 2,
                              (FULL_RUN_FRESHNESS_SECS + 6 * FULL_RUN_SYNC_SECS) / 60)
            log(f"{name}: stopped {victim.name}, whose mark lapses in "
                f"{FULL_RUN_FRESHNESS_SECS} s; observing for {second_half:g} min")
            observe(second_half)
        else:
            log(f"{name}: observing for {args.observe_minutes} min")
            observe(args.observe_minutes)
        checks = check_all(None)
        timeline["ended"] = utc_now().isoformat()
        devnet_peers = sorted(path.name for path in (devnet_data / "nodes").iterdir()
                              if path.is_dir())
    finally:
        stop_all(processes)
        stalled = stall.stop()
    # What the analysis needs, so a finished run can be read again
    # (--analyze) without running it again.
    record = {
        "name": name,
        "network": network_release.name,
        "joining": joiner_release.name,
        "window": [timeline["joiners"], timeline["ended"]],
        "stalled_seconds": stalled,
        "devnet_peers": devnet_peers,
        "nodes": [{"name": node.name, "role": node.role, "release": node.release.name,
                   "ran": started_as(folder / f"{node.name}.log"), "peer_id": node.peer_id,
                   "full": node.full, "log": f"{node.name}.log"} for node in nodes],
        "uploads": [path.name for path in uploads],
        "checks": [path.name for path in checks],
        "page_bytes": paging.get("page_bytes"),
        "paging": paging,
        "rollback": rolled,
        "phase_checks": phases,
        "full_phases": {key: timeline[key] for key in ("pressure", "stopped")
                        if key in timeline},
    }
    (folder / "run.json").write_text(json.dumps(record, indent=2))
    return folder


def load_run(folder):
    """A finished run, from its folder, as the analysis reads it."""
    record = json.loads((folder / "run.json").read_text())
    record["folder"] = folder
    record["window"] = tuple(datetime.datetime.fromisoformat(stamp)
                             for stamp in record["window"])
    record["uploads"] = [json.loads((folder / name).read_text()) for name in record["uploads"]]
    record["checks"] = [json.loads((folder / name).read_text()) for name in record["checks"]]
    rollback = record.get("rollback")
    if rollback:
        for key in ("old_from", "old_until"):
            rollback[key] = datetime.datetime.fromisoformat(rollback[key])
        rollback["restarts"] = [[datetime.datetime.fromisoformat(stamp) for stamp in pair]
                                for pair in rollback["restarts"]]
    record["phase_checks"] = {
        phase: [found for name in names
                for found in json.loads((folder / name).read_text())["records"]]
        for phase, names in record.get("phase_checks", {}).items()}
    return record


def started_as(log_path):
    """The version and commit a node's log says it started as."""
    with open(log_path, "rb") as handle:
        for raw in handle:
            text = ANSI.sub("", raw.decode("utf-8", "replace"))
            if "ant-node starting" in text:
                found = fields(text)
                return f"{found.get('version', '?')} (commit {found.get('commit', '?')})"
    return "unknown"


def peer_id_in(log_path, offset=0):
    """The peer id a node's log names, from `offset` on."""
    try:
        with open(log_path, "rb") as handle:
            handle.seek(offset)
            for raw in handle:
                match = PEER_ID_LINE.search(ANSI.sub("", raw.decode("utf-8", "replace")))
                if match:
                    return match.group(1)
    except OSError:
        pass
    return None


def client(harness, arguments, out):
    """Runs `darwin-harness` with `arguments`, its output beside `out`."""
    try:
        result = subprocess.run([str(harness), *arguments, "--out", str(out)],
                                capture_output=True, text=True, timeout=CLIENT_TIMEOUT)
    except subprocess.TimeoutExpired as error:
        raise HarnessError(f"darwin-harness {arguments[0]} took over "
                           f"{CLIENT_TIMEOUT} s") from error
    (out.parent / f"{out.stem}.out").write_text(result.stdout + result.stderr)
    if result.returncode != 0 or not out.exists():
        raise HarnessError(f"darwin-harness {arguments[0]} failed: {result.stderr[-2000:]}")
    return out


def upload(harness, manifest, out, records):
    sizes = ",".join(str(RECORD_SIZES[index % len(RECORD_SIZES)]) for index in range(records))
    log(f"uploading {records} records")
    return client(harness, ["upload", "--manifest", str(manifest), "--sizes", sizes], out)


def check(harness, manifest, uploads, out):
    log(f"reading back {uploads.name}")
    return client(harness, ["check", "--manifest", str(manifest), "--uploads", str(uploads)],
                  out)


# --------------------------------------------------------------------------
# Reading the logs
# --------------------------------------------------------------------------

def parse_time(text):
    match = TIMESTAMP.match(text)
    if not match:
        return None
    fraction = (match.group(2) or ".")[:7].ljust(7, "0")
    return datetime.datetime.fromisoformat(match.group(1) + fraction + "+00:00")


def fields(line):
    return {key: value.strip('"') for key, value in FIELD.findall(line)}


def read_log(path, window):
    """The lines of a log within `window`, as (time, text), one at a time;
    lines with no time of their own (a panic's message) take the last time
    seen."""
    start, end = window
    last = None
    with open(path, "rb") as handle:
        for raw in handle:
            text = ANSI.sub("", raw.decode("utf-8", "replace")).rstrip("\r\n")
            stamp = parse_time(text)
            if stamp is not None:
                last = stamp
            if last is not None and start <= last <= end:
                yield last, text


def merge_counters(groups):
    """The counters of the last summary line of each group, merged (the
    group's own number left out)."""
    counters = {}
    for found in groups.values():
        for key, value in found.items():
            if key != "group" and value.isdigit():
                counters[key] = int(value)
    return counters


def analyze(run):
    window = run["window"]
    folder = run["folder"]
    roles = {peer: "devnet" for peer in run["devnet_peers"]}
    releases = {peer: run["network"] for peer in run["devnet_peers"]}
    for node in run["nodes"]:
        roles[node["peer_id"]] = node["role"]
        releases[node["peer_id"]] = node["release"]
    for uploads in run["uploads"]:
        roles.setdefault(uploads["client"], "client")
    rollback = run.get("rollback")

    def release_at(peer, when):
        return release_of(rollback, releases, peer, when)

    full_peers = {node["peer_id"] for node in run["nodes"] if node["full"]}
    # A node that refused a record's PUT doesn't hold it, and its close group's
    # possession checks then charge it: those penalties follow the refusal,
    # which is counted (and compared) on its own.
    refused = {(answer["peer"], record["address"])
               for uploads in run["uploads"] for record in uploads["records"]
               for answer in record["puts"]
               if answer["outcome"] not in ("stored", "already_stored")}
    refusals = {}
    for peer, _ in refused:
        role = roles.get(peer, "other")
        refusals[role] = refusals.get(role, 0) + 1

    sources = [("devnet", "devnet", None, folder / "devnet.log")]
    sources += [(node["name"], node["role"], node["peer_id"], folder / node["log"])
                for node in run["nodes"]]
    decode_failures = []
    unanswered = {}
    penalties = {}
    penalties_on_full = {}
    penalties_on_refusals = {}
    penalties_on_restart = {}
    between_releases = {}
    traffic = {}
    audits = {}
    charges = []
    sync = {"paged": [], "single": [], "builds": {}, "served": {}, "busy": 0}
    for source, role, source_peer, path in sources:
        traffic_groups = {}
        audit_groups = {}
        possession_failed = None
        for stamp, text in read_log(path, window):
            own_release = (run["network"] if source_peer is None
                           else release_at(source_peer, stamp))
            note_sync(sync, source, own_release, stamp, text, release_at)
            failed = POSSESSION_FAILED.search(text)
            if failed:
                possession_failed = (failed.group(1), failed.group(2))
            if TRAFFIC_LINE in text:
                traffic_groups[fields(text).get("group", "")] = fields(text)
            elif AUDIT_LINE in text:
                audit_groups[fields(text).get("group", "")] = fields(text)
            silent = UNANSWERED.search(text)
            if silent:
                key = (role, roles.get(silent.group(2), "other"),
                       UNANSWERED_KIND[silent.group(1)])
                unanswered[key] = unanswered.get(key, 0) + 1
            if DECODE_FAILURE.search(text):
                decode_failures.append(f"{source}: {text[:400]}")
            if TRUST_LINE in text:
                found = fields(text)
                try:
                    lowered = float(found.get("delta", "0")) < 0
                except ValueError:
                    lowered = False
                if not lowered:
                    continue
                reason = found.get("reason", "")
                target = found.get("peer_id", "")
                weight = float(found.get("weight", "1") or 1)
                charges.append((source, target, stamp, reason))
                if target in full_peers:
                    book = penalties_on_full
                elif restarting(rollback, target, stamp):
                    book = penalties_on_restart
                elif (possession_failed and possession_failed[0] == target
                      and possession_failed in refused):
                    book = penalties_on_refusals
                    possession_failed = None
                else:
                    book = penalties
                key = (role, roles.get(target, "other"), reason)
                count, total = book.get(key, (0, 0.0))
                book[key] = (count + 1, total + weight)
                # One release's nodes charging the other's, whatever the
                # book: what the rule for messages between nodes governs.
                target_release = release_at(target, stamp)
                if (roles.get(target) in ("incumbent", "joiner")
                        and target_release != own_release):
                    key = (own_release, target_release, reason)
                    count, total = between_releases.get(key, (0, 0.0))
                    between_releases[key] = (count + 1, total + weight)
        if source != "devnet":
            traffic[source] = merge_counters(traffic_groups)
            audits[source] = merge_counters(audit_groups)
    sync["busy_charged"] = busy_charges(sync, charges)

    records = [record for uploads in run["uploads"] for record in uploads["records"]]
    checks = [record for checks in run["checks"] for record in checks["records"]]
    full_evidence = {}
    for node in run["nodes"]:
        if node["full"]:
            text = (folder / node["log"]).read_bytes().decode("utf-8", "replace")
            # A refusal for lack of space (the seam's 0 free bytes are under
            # any reserve, where the host's own disk has room), or the node's
            # own line on entering "full": a full node no PUT reached logs
            # only the latter.
            full_evidence[node["name"]] = ("Insufficient disk space" in text
                                           or "Node full: since" in text)
    full_run = full_run_checks(run, records, checks) if run.get("full_phases") else None
    return {
        "full_run": full_run,
        "decode_failures": decode_failures,
        "penalties": penalties,
        "penalties_on_full": penalties_on_full,
        "penalties_on_refusals": penalties_on_refusals,
        "penalties_on_restart": penalties_on_restart,
        "between_releases": between_releases,
        "refusals": refusals,
        "traffic": traffic,
        "audits": audits,
        "records": records,
        "checks": checks,
        "full_evidence": full_evidence,
        "unanswered": unanswered,
        "sync": sync,
        "paging": run.get("paging") or {},
        "rollback": rollback,
        "stalled_seconds": run.get("stalled_seconds", 0),
        "roles": {node["name"]: node["role"] for node in run["nodes"]},
        "ran": {role: sorted({node.get("ran") or started_as(folder / node["log"])
                              for node in run["nodes"] if node["role"] == role})
                for role in ("incumbent", "joiner")},
        "releases": {node["name"]: node["release"] for node in run["nodes"]},
    }


def release_of(rollback, releases, peer, when):
    """The release `peer` ran at `when`: the one it was started on, but for
    the rollback run's node from its stop until it came back, when it was
    the previous release's."""
    if (rollback and peer == rollback["peer_id"] and when is not None
            and rollback["old_from"] <= when < rollback["old_until"]):
        return "old"
    return releases.get(peer, "other")


def restarting(rollback, peer, when):
    """Whether `when` falls in one of the rollback run's restarts of `peer`:
    from its stop until RESTART_GRACE seconds after it was up again, when
    requests sent to it before or during the restart still fail."""
    if not rollback or peer != rollback["peer_id"]:
        return False
    grace = datetime.timedelta(seconds=RESTART_GRACE)
    return any(stopped <= when <= up + grace for stopped, up in rollback["restarts"])


def busy_charges(sync, charges):
    """The penalties a requester charged its responder during a paged exchange
    the responder answered busy, up to BUSY_CHARGE_GRACE after it ended."""
    grace = datetime.timedelta(seconds=BUSY_CHARGE_GRACE)
    found = []
    for exchange in sync.get("paged", []):
        if exchange.get("end") != "Busy" or "start" not in exchange:
            continue
        for observer, target, stamp, reason in charges:
            if (observer == exchange["from"] and target == exchange.get("peer")
                    and exchange["start"] <= stamp <= exchange["at"] + grace):
                found.append({"from": observer, "peer": target, "at": stamp,
                              "reason": reason})
    return found


def exchange_in(text):
    """The neighbour-sync exchange a log line ends, with the time it started,
    or None: the requester logs one line for each exchange it starts, paged or
    single, when it ends, with how long it took."""
    if PAGED_EXCHANGE_LINE in text:
        form = "paged"
    elif SINGLE_EXCHANGE_LINE in text:
        form = "single"
    else:
        return None
    at = parse_time(text)
    if at is None:
        return None
    exchange = {key: int(value) if value.isdigit() else value
                for key, value in fields(text).items()}
    exchange["form"] = form
    exchange["at"] = at
    elapsed = exchange.get("elapsed_ms", 0)
    exchange["start"] = at - datetime.timedelta(
        milliseconds=elapsed if isinstance(elapsed, int) else 0)
    return exchange


def note_sync(sync, source, release, stamp, text, release_at):
    """Notes a neighbour-sync line of `source`'s log in `sync`: an exchange it
    started, paged or single, with the releases of the two nodes when it
    started; a hint set it built, and a page it served, as a responder, by
    the requester and the exchange's number; or a page it answered busy."""
    exchange = exchange_in(text)
    if exchange is not None:
        exchange.update({"from": source, "release": release,
                         "peer_release": release_at(exchange.get("peer", ""),
                                                    exchange["start"])})
        sync[exchange["form"]].append(exchange)
    elif HINT_SET_LINE in text:
        found = fields(text)
        key = f"{source} {found.get('source', '?')} {found.get('cycle', '?')}"
        sync["builds"][key] = sync["builds"].get(key, 0) + 1
    elif PAGE_SERVED_LINE in text:
        found = fields(text)
        key = f"{source} {found.get('source', '?')} {found.get('cycle', '?')}"
        page = found.get("page", "")
        sync["served"].setdefault(key, []).append(int(page) if page.isdigit() else -1)
    elif BUSY_LINE in text:
        sync["busy"] += 1


def multi_page(exchange):
    """Whether an exchange took more than one page for each list, both ways."""
    return all(exchange.get(f"{kind}_pages_{way}", 0) >= 2
               for kind in ("replica", "paid") for way in ("sent", "received"))


def window_of(nearest, full):
    """A record's 7 nearest not full, and its full nodes among them: walking
    its nearest nodes, nearest first, until 7 not full are passed (at most
    WIDEST)."""
    not_full, full_inside = [], []
    for peer in nearest[:WIDEST]:
        if len(not_full) == CLOSE_GROUP:
            break
        (full_inside if peer in full else not_full).append(peer)
    return not_full, full_inside


def holders(record):
    return {answer["peer"] for answer in record["answers"] if answer["outcome"] == "held"}


def full_run_checks(run, records, end_checks):
    """The full run's checks: copies at each count, stand-ins kept, the
    erasures' proofs, and a lapsed mark counted as missing."""
    full = {node["peer_id"] for node in run["nodes"] if node["full"]}
    devnet = set(run["devnet_peers"])
    stopped = (run["full_phases"].get("stopped") or {}).get("peer_id")
    failures = []
    by_f = {}
    counts = {}

    def copies(phase, phase_checks, full_now, gone):
        for record in phase_checks:
            nearest = record.get("nearest") or record["close_group"]
            not_full, full_inside = window_of(nearest, full_now)
            f = len(full_inside)
            by_f.setdefault(phase, {}).setdefault(f, 0)
            by_f[phase][f] += 1
            held = holders(record) - full_now - gone
            missing = sum(1 for peer in not_full if peer in devnet or peer in gone)
            needed = CLOSE_GROUP - missing
            counts.setdefault(phase, []).append(len(held))
            if len(held) < needed:
                failures.append(f"{phase}: record {record['address']} (f = {f}) is held by "
                                f"{len(held)} nodes not full, fewer than {needed}")

    early = run["phase_checks"].get("early", [])
    mid = run["phase_checks"].get("mid", [])
    copies("early", early, full, set())
    copies("mid", mid, full, set())
    gone = {stopped} if stopped else set()
    copies("end", end_checks, full - gone, gone)

    # Stand-ins: held at the early count from beyond the 9 nearest, inside
    # the window full nodes widen, and still held at the mid count.
    mid_by_address = {record["address"]: record for record in mid}
    stand_ins_kept = 0
    for record in early:
        nearest = record.get("nearest") or []
        _, full_inside = window_of(nearest, full)
        width = ADMISSION_WIDTH + len(full_inside)
        stand_ins = {peer for peer in nearest[ADMISSION_WIDTH:width] if peer not in full}
        later = mid_by_address.get(record["address"])
        if later is None:
            continue
        for peer in stand_ins & holders(record):
            if peer in holders(later):
                stand_ins_kept += 1
            else:
                failures.append(f"mid: stand-in {peer[:16]} no longer holds record "
                                f"{record['address']}, while the marks are fresh")

    # The erasures' proofs, from every node's log.
    erasures = []
    stopped_at = (run["full_phases"].get("stopped") or {}).get("at")
    lapsed_at = None
    if stopped_at:
        lapsed_at = (datetime.datetime.fromisoformat(stopped_at)
                     + datetime.timedelta(seconds=FULL_RUN_FRESHNESS_SECS))
    still_full = len(full - ({stopped} if stopped else set()))
    for node in run["nodes"]:
        for stamp, text in read_log(run["folder"] / node["log"], run["window"]):
            if ERASED_LINE not in text:
                continue
            found = fields(text)
            try:
                proofs = int(found.get("proofs_not_full", "0")) + int(found.get("proofs_full", "0"))
                needed = int(found.get("proofs_needed", "0"))
                group_full = int(found.get("group_full", "0"))
            except ValueError:
                failures.append(f"{node['name']}: an erasure line that doesn't parse: {text}")
                continue
            after_lapse = lapsed_at is not None and stamp > lapsed_at
            erasures.append({"node": node["name"], "record": found.get("erased_record"),
                             "proofs": proofs, "needed": needed, "group_full": group_full,
                             "after_lapse": after_lapse})
            if needed < ERASE_PROOFS or proofs < needed:
                failures.append(f"{node['name']} erased {found.get('erased_record')} with "
                                f"{proofs} proofs, needing {needed}: fewer than "
                                f"{ERASE_PROOFS} real proofs")
            if after_lapse and group_full > still_full:
                failures.append(f"{node['name']} erased {found.get('erased_record')} after "
                                f"the stopped node's mark lapsed counting {group_full} "
                                f"members marked full, more than the {still_full} still "
                                "running: a lapsed mark wasn't counted as missing")
    return {"failures": failures, "by_f": by_f, "copies": counts,
            "stand_ins_kept": stand_ins_kept, "erasures": erasures,
            "stopped": run["full_phases"].get("stopped")}


def received_families(result, role):
    """The message families the nodes of `role` received, with counts. A page
    family counts towards the single-message family it takes the place of
    too, so a node that read pages from its peers received neighbour sync."""
    families = {}
    for name, counters in result["traffic"].items():
        if result["roles"].get(name) != role:
            continue
        for key, value in counters.items():
            if key.endswith("_rx_count"):
                family = key[:-len("_rx_count")]
                families[family] = families.get(family, 0) + value
                if family in PAGE_FAMILIES:
                    single = PAGE_FAMILIES[family]
                    families[single] = families.get(single, 0) + value
    return families


def is_baseline(name):
    return BASELINE_RUN.fullmatch(name) is not None


def runs_in(work):
    """The finished runs in a work folder, the all-old runs first."""
    found = {path.name for path in work.iterdir() if (path / "run.json").exists()}
    baselines = sorted((name for name in found if is_baseline(name)),
                       key=lambda name: int(BASELINE_RUN.fullmatch(name).group(1) or 1))
    return baselines + [name for name in RUNS if name in found and not is_baseline(name)]


def limit(bases, args):
    """The most a mixed run may count, for one pair of roles and reason (or
    one role's refused PUTs), against the all-old runs' counts: the highest
    plus the slack, or against a single all-old run, its count plus the slack
    or the ratio's share of it, if that is more."""
    top = max(bases)
    if len(bases) == 1:
        return top + max(args.penalty_slack, math.ceil(top * args.penalty_ratio))
    return top + args.penalty_slack


def against(bases):
    if len(bases) == 1:
        return f"{bases[0]} in the all-old run"
    return f"at most {max(bases)} in the {len(bases)} all-old runs ({', '.join(map(str, bases))})"


def measured(name, baselines):
    """What of a run is measured against the all-old runs, when there is one:
    for the mixed runs and the rollback run, everything (the penalties for
    each pair of roles and reason, new failure reasons, refused PUTs and the
    message families); for the paged run, new failure reasons only, since its
    pages of two keys send far more messages than any all-old run; for the
    all-old runs and the full run, nothing."""
    if not baselines or is_baseline(name) or name == "full":
        return set()
    if name == "paged":
        return {"new reason"}
    return {"penalties", "new reason", "refused PUTs", "families"}


def sync_failures(name, result):
    """What fails a run in its neighbour sync.

    In every run: a node paging to a node that can't read pages; this tree's
    nodes, in a run in which they page, not paging to each other; with the
    previous release's nodes beside them, no single message to those nodes
    (or, if they read pages, no page); and a requester charging its responder
    during an exchange the responder answered busy. In the rollback run: no
    single message to the rolled-back node while it ran the previous release,
    more than one page from a node left unanswered over the connection its
    earlier process left, and no page to it once it came back. In the paged
    run: no exchange taking more than one page for each list both ways,
    hints built more than once in an exchange, an exchange's pages served
    out of order, and, with the incumbents' admission limit lowered, no page
    answered busy."""
    failures = []
    sync = result.get("sync") or {}
    paging = result.get("paging") or {}
    rollback = result.get("rollback")
    rolled = rollback["peer_id"] if rollback else None
    paged = sync.get("paged", [])
    single = sync.get("single", [])

    def between(exchanges, release, peer_release):
        return [exchange for exchange in exchanges if exchange.get("release") == release
                and exchange.get("peer_release") == peer_release]

    # Pages to nodes that can't read them. Over a connection the rolled-back
    # node's earlier process left, a page may go out once from each node
    # before the node's agent shows its release, and is never answered.
    left_over = {}
    if not paging.get("old_reads_pages"):
        for exchange in between(paged, "new", "old"):
            if (rolled is not None and exchange.get("peer") == rolled
                    and exchange.get("pages_answered", 0) == 0):
                left_over[exchange["from"]] = left_over.get(exchange["from"], 0) + 1
                continue
            failures.append(f"{name}: {exchange['from']} paged its hints to a node of the "
                            "previous release, which can't read pages")
    for requester, count in sorted(left_over.items()):
        if count > 1:
            failures.append(f"{name}: {requester} paged {count} times to the rolled-back node "
                            "while it ran the previous release: a first page left unanswered "
                            "should make the next exchange a single message")

    if paging.get("new_pages") and not between(paged, "new", "new"):
        failures.append(f"{name}: no node of this tree paged its hints to another")
    # The rollback run's previous release is its one node, checked below.
    if paging.get("old_present") and not rollback:
        if paging.get("old_reads_pages"):
            if not between(paged, "new", "old"):
                failures.append(f"{name}: no node of this tree paged its hints to a node of "
                                "the previous release, which reads pages")
        elif not between(single, "new", "old"):
            failures.append(f"{name}: no node of this tree sent its hints to a node of the "
                            "previous release in a single message")

    for charged in sync.get("busy_charged", []):
        failures.append(f"{name}: {charged['from']} charged {charged['peer'][:16]} "
                        f"({charged['reason']}) during an exchange it answered busy")

    if rollback:
        if paging.get("old_reads_pages"):
            if not any(exchange.get("peer") == rolled and exchange.get("pages_answered", 0) >= 1
                       for exchange in between(paged, "new", "old")):
                failures.append(f"{name}: no node of this tree paged to the rolled-back node "
                                "while it ran the previous release: observe for longer")
        elif not any(exchange.get("peer") == rolled
                     for exchange in between(single, "new", "old")):
            failures.append(f"{name}: no node of this tree sent the rolled-back node a single "
                            "message while it ran the previous release: observe for longer")
        if not any(exchange.get("peer") == rolled and exchange.get("pages_answered", 0) >= 1
                   and exchange.get("start", rollback["old_until"]) >= rollback["old_until"]
                   for exchange in between(paged, "new", "new")):
            failures.append(f"{name}: no node of this tree paged to the rolled-back node once "
                            "it came back: observe for longer")

    if name != "paged":
        return failures
    if not any(multi_page(exchange) for exchange in paged):
        failures.append(f"{name}: no exchange took more than one page for each list, both "
                        "ways: no paging happened at this page size")
    for key, count in sorted(sync.get("builds", {}).items()):
        if count > 1:
            failures.append(f"{name}: {key}: the responder built its hints {count} times in "
                            "one exchange, not once")
    served_from_one = 0
    for key, pages in sorted(sync.get("served", {}).items()):
        if pages != list(range(len(pages))):
            failures.append(f"{name}: {key}: pages served out of order: {pages[:20]}")
        elif len(pages) > 1 and sync.get("builds", {}).get(key) == 1:
            served_from_one += 1
    if served_from_one == 0:
        failures.append(f"{name}: no responder served more than one page from hints built "
                        "once")
    if paging.get("busy_limit") and sync.get("busy", 0) == 0:
        failures.append(f"{name}: no page was answered busy, with the incumbents admitting "
                        f"{paging['busy_limit']} sync request(s) at once")
    return failures


def comparisons(result, baselines, args):
    """Every count of a run measured against the all-old runs, passing or
    not: each pair of roles and reason it charged the incumbents or the
    joiners with ("penalties"), each reason no all-old run charged a role
    with, counted over every observer ("new reason"), and each role's
    refused paid PUTs ("refused PUTs"). Each gives the run's count, the
    all-old runs' counts, the highest of them (0 for a new reason), the
    excess over it, and the most the run may count; a count over its limit
    fails the run."""
    def entry(kind, observer, target, reason, count, bases, most):
        return {"kind": kind, "observer": observer, "target": target, "reason": reason,
                "count": count, "all_old": bases, "highest": max(bases),
                "excess": count - max(bases), "limit": most}

    # The reasons the all-old runs charged each role with, from any
    # observer: one they never did is a new failure reason.
    known = {(target, reason) for baseline in baselines
             for (_, target, reason) in baseline["penalties"]}
    entries = []
    new_reasons = {}
    for key, (count, _) in sorted(result["penalties"].items()):
        observer, target, reason = key
        # The devnet's nodes run no replication, so every release's nodes
        # find them silent: penalties on them, and on the client, are
        # reported, not compared.
        if target not in ("incumbent", "joiner"):
            continue
        if (target, reason) not in known:
            new_reasons[(target, reason)] = new_reasons.get((target, reason), 0) + count
            continue
        bases = [baseline["penalties"].get(key, (0, 0.0))[0] for baseline in baselines]
        entries.append(entry("penalties", observer, target, reason, count, bases,
                             limit(bases, args)))
    for (target, reason), count in sorted(new_reasons.items()):
        entries.append(entry("new reason", None, target, reason, count, [0] * len(baselines),
                             args.new_reason_allowance))
    for role in ("incumbent", "joiner"):
        bases = [baseline["refusals"].get(role, 0) for baseline in baselines]
        entries.append(entry("refused PUTs", None, role, None, result["refusals"].get(role, 0),
                             bases, limit(bases, args)))
    return entries


def verdicts(results, args):
    """Every failure, as text; empty when every run passed."""
    failures = []
    baselines = [result for name, result in results.items() if is_baseline(name)]
    for name, result in results.items():
        if result["stalled_seconds"] > STALL_LIMIT:
            failures.append(f"{name}: the machine stood still for {result['stalled_seconds']} s "
                            "during the run (asleep?), so it can't be compared: run it again")
        if result["decode_failures"]:
            failures.append(f"{name}: {len(result['decode_failures'])} message(s) didn't "
                            f"decode, first: {result['decode_failures'][0]}")
        for node, counters in result["audits"].items():
            for key, value in counters.items():
                if key.endswith("_fail_malformed") and value > 0:
                    failures.append(f"{name}: {node} failed {value} audit(s) on a reply "
                                    f"that didn't decode ({key})")
        for record in result["records"]:
            if not record.get("stored"):
                failures.append(f"{name}: record {record['address']} wasn't stored: "
                                f"{record.get('error')}")
        if result.get("full_run"):
            failures.extend(f"{name}: {failure}" for failure in result["full_run"]["failures"])
        for record in result["checks"]:
            # In the full run the incumbents may erase what they stored out
            # of their range: the copies counted there stand in for this.
            if result.get("full_run"):
                break
            if record["held_by_storers"] < MAJORITY:
                failures.append(f"{name}: record {record['address']} is held at the end by "
                                f"{record['held_by_storers']} of the "
                                f"{len(record['storers'])} nodes that stored it")
        for node, ok in result["full_evidence"].items():
            if not ok:
                failures.append(f"{name}: {node} never refused for lack of space nor "
                                "entered full: the free-space seam didn't reach it")
        for node, counters in sorted(result["traffic"].items()):
            if not counters:
                failures.append(f"{name}: {node} logged no traffic summary in the window "
                                "(one every 5 minutes): observe for longer")
        for role in ("incumbent", "joiner"):
            families = received_families(result, role)
            for family in ("neighbor_sync_request", "neighbor_sync_response"):
                if families.get(family, 0) == 0:
                    failures.append(f"{name}: the {role}s received no {family}")
        failures += sync_failures(name, result)
        kinds = measured(name, baselines)
        if not kinds:
            continue
        for compared in comparisons(result, baselines, args):
            count, target, reason = compared["count"], compared["target"], compared["reason"]
            if compared["kind"] not in kinds or count <= compared["limit"]:
                continue
            if compared["kind"] == "penalties":
                failures.append(f"{name}: {count} {reason} penalties by "
                                f"{compared['observer']}s on {target}s, against "
                                f"{against(compared['all_old'])}")
            elif compared["kind"] == "new reason":
                failures.append(f"{name}: {count} {reason} penalties on {target}s, a new "
                                f"failure reason: no all-old run charged {target}s with it")
            else:
                failures.append(f"{name}: the {target}s refused {count} paid PUTs, against "
                                f"{against(compared['all_old'])}")
        if "families" not in kinds:
            continue
        for role in ("incumbent", "joiner"):
            befores = [received_families(baseline, role) for baseline in baselines]
            now = received_families(result, role)
            for family in sorted(set().union(*befores)):
                fewest = min(before.get(family, 0) for before in befores)
                if fewest >= SHAPE_FLOOR and now.get(family, 0) == 0:
                    failures.append(f"{name}: the {role}s received no {family}, which "
                                    f"they did {fewest} times or more in every all-old run")
        for family in args.new_only_family:
            reached_new = 0
            for node, counters in result["traffic"].items():
                release = result["releases"].get(node)
                received = counters.get(f"{family}_rx_count", 0)
                if release == "old" and received:
                    failures.append(f"{name}: {node}, of the previous release, received "
                                    f"{received} {family}")
                if release == "new":
                    reached_new += received
            if reached_new == 0:
                failures.append(f"{name}: no node of this tree received {family}")
    return failures


def report(results, args):
    lines = []
    baselines = [name for name in results if is_baseline(name)]
    if len(baselines) > 1:
        lines.append(f"Mixed runs are measured against {len(baselines)} all-old runs "
                     f"({', '.join(baselines)}): a pair of roles' penalties for a reason "
                     f"may exceed the highest of their counts by {args.penalty_slack}.")
    elif baselines:
        ratio = (f", or by {args.penalty_ratio:g} times it if that is more"
                 if args.penalty_ratio > 0 else "")
        lines.append(f"Mixed runs are measured against one all-old run: a pair of roles' "
                     f"penalties for a reason may exceed its count by {args.penalty_slack}"
                     f"{ratio}.")
    if baselines:
        lines.append(f"A reason no all-old run charged a role with may come to "
                     f"{args.new_reason_allowance} against it. Each mixed run lists every "
                     f"count measured, with the highest all-old count, the excess over it "
                     f"and the limit, within the limit or not.")
    baseline_results = [results[name] for name in baselines]
    for name, result in results.items():
        lines.append(f"\n== {name} ==")
        for role, ran in result["ran"].items():
            lines.append(f"{role}s ran: {', '.join(ran)}")
        lines.append(f"decode failures: {len(result['decode_failures'])}")
        lines.append("requests not answered in time (sender -> target, kind: count):")
        for (sender, target, kind), count in sorted(result["unanswered"].items()):
            lines.append(f"  {sender:>9} -> {target:<9} {kind:<19} {count:>5}")
        lines.append("penalties (observer -> target, reason: count, weight):")
        for (observer, target, reason), (count, weight) in sorted(result["penalties"].items()):
            lines.append(f"  {observer:>9} -> {target:<9} {reason:<19} {count:>5} {weight:8.2f}")
        refusals = ", ".join(f"{role}s {count}" for role, count in
                             sorted(result["refusals"].items()))
        lines.append(f"paid PUTs refused: {refusals or 'none'}")
        kinds = measured(name, baseline_results)
        if kinds:
            lines.append("measured against the all-old runs (observer -> target, reason: "
                         "count, highest all-old, excess, limit):")
            for compared in comparisons(result, baseline_results, args):
                if compared["kind"] not in kinds:
                    continue
                if compared["kind"] == "penalties":
                    what = (f"{compared['observer']:>9} -> {compared['target']:<9} "
                            f"{compared['reason']:<19}")
                elif compared["kind"] == "new reason":
                    what = (f"{'any':>9} -> {compared['target']:<9} "
                            f"{compared['reason'] + ' (new)':<19}")
                else:
                    what = f"{'':>9}    {compared['target']:<9} {'refused PUTs':<19}"
                over = " OVER" if compared["count"] > compared["limit"] else ""
                lines.append(f"  {what} {compared['count']:>5} {compared['highest']:>5} "
                             f"{compared['excess']:>+6} {compared['limit']:>5}{over}")
        if result["penalties_on_refusals"]:
            lines.append("penalties for not holding a record whose PUT the node refused:")
            for key, (count, weight) in sorted(result["penalties_on_refusals"].items()):
                lines.append(f"  {key[0]:>9} -> {key[1]:<9} {key[2]:<19} {count:>5} "
                             f"{weight:8.2f}")
        if result["between_releases"]:
            lines.append("penalties one release's nodes charged the other's "
                         "(observer's release -> target's, reason: count, weight):")
            for (observer, target, reason), (count, weight) in sorted(
                    result["between_releases"].items()):
                lines.append(f"  {observer:>9} -> {target:<9} {reason:<19} {count:>5} "
                             f"{weight:8.2f}")
        if result.get("penalties_on_restart"):
            lines.append("penalties on the rolled-back node while it restarted:")
            for key, (count, weight) in sorted(result["penalties_on_restart"].items()):
                lines.append(f"  {key[0]:>9} -> {key[1]:<9} {key[2]:<19} {count:>5} "
                             f"{weight:8.2f}")
        if result.get("full_run"):
            run = result["full_run"]
            for phase in ("early", "mid", "end"):
                by_f = ", ".join(f"f = {f}: {count}" for f, count in
                                 sorted(run["by_f"].get(phase, {}).items()))
                held = run["copies"].get(phase, [])
                lines.append(f"{phase} count: records by full nodes in their window: "
                             f"{by_f or 'none'}; held by {min(held, default=0)} to "
                             f"{max(held, default=0)} nodes not full")
            lines.append(f"stand-in copies kept while the marks were fresh: "
                         f"{run['stand_ins_kept']}")
            erasures = run["erasures"]
            lapsed = sum(1 for erasure in erasures if erasure["after_lapse"])
            fewest = min((erasure["proofs"] for erasure in erasures), default=0)
            lines.append(f"erasures: {len(erasures)} ({lapsed} after the stopped node's "
                         f"mark lapsed), with {fewest} proofs or more each")
            if run["stopped"]:
                lines.append(f"stopped: {run['stopped']['node']} at {run['stopped']['at']}")
        if result["penalties_on_full"]:
            lines.append("penalties on full nodes:")
            for key, (count, weight) in sorted(result["penalties_on_full"].items()):
                lines.append(f"  {key[0]:>9} -> full      {key[2]:<19} {count:>5} {weight:8.2f}")
        for role in ("incumbent", "joiner"):
            families = received_families(result, role)
            shown = ", ".join(f"{family} {count}" for family, count in sorted(families.items())
                              if count)
            lines.append(f"{role}s received: {shown}")
        lines.extend(sync_lines(result))
        kept = [record["held_by_storers"] for record in result["checks"]]
        near = [record["held_by_close_group"] for record in result["checks"]]
        stored = sum(1 for record in result["records"] if record.get("stored"))
        lines.append(f"records: {stored} of {len(result['records'])} stored; at the end, "
                     f"held by {min(kept, default=0)} to {max(kept, default=0)} of the nodes "
                     f"that stored them, and by {min(near, default=0)} to "
                     f"{max(near, default=0)} of the 7 nearest them now")
        audits = {}
        for counters in result["audits"].values():
            for key, value in counters.items():
                audits[key] = audits.get(key, 0) + value
        counted = ", ".join(f"{key} {value}" for key, value in sorted(audits.items()) if value)
        lines.append(f"audit outcomes: {counted or 'none counted yet'}")
    return "\n".join(lines)


def sync_lines(result):
    """The report's lines on neighbour sync: exchanges paged and single, by the
    releases of the two nodes, the most pages one took, busy answers, and how
    many times a responder built its hints in one exchange."""
    sync = result.get("sync") or {}
    lines = ["neighbour-sync exchanges (requester's release -> peer's: paged, single):"]
    pairs = {}
    for form in ("paged", "single"):
        for exchange in sync.get(form, []):
            pair = (exchange["release"], exchange["peer_release"])
            counts = pairs.setdefault(pair, [0, 0])
            counts[0 if form == "paged" else 1] += 1
    for (release, peer_release), (paged, single) in sorted(pairs.items()):
        lines.append(f"  {release:>9} -> {peer_release:<9} {paged:>5} {single:>5}")
    paged = sync.get("paged", [])
    if paged:
        sent = max(exchange.get("pages_sent", 0) for exchange in paged)
        received = max(exchange.get("pages_received", 0) for exchange in paged)
        lines.append(f"  most pages in an exchange: {sent} sent, {received} received; "
                     f"{sum(1 for exchange in paged if multi_page(exchange))} exchange(s) "
                     "took more than one page for each list both ways")
    builds = sync.get("builds", {})
    busy = sum(1 for exchange in paged if exchange.get("end") == "Busy")
    lines.append(f"  pages answered busy: {sync.get('busy', 0)}; exchanges ended busy: {busy}, "
                 f"charged for: {len(sync.get('busy_charged', []))}; hint sets built: "
                 f"{sum(builds.values())}, at most {max(builds.values(), default=0)} in one "
                 "exchange")
    # A responder keeps repair proofs only for pages a later request showed
    # arrived, so its one-page exchanges give it none: audits per exchange
    # measure what that costs, run against run.
    audits = sum(value for counters in (result.get("audits") or {}).values()
                 for key, value in counters.items()
                 if key.startswith(("responsible_audit_", "subtree_audit_")))
    exchanges = len(paged) + len(sync.get("single", []))
    per_exchange = f"{audits / exchanges:.3f}" if exchanges else "no exchanges"
    lines.append(f"  audits made: {audits} over {exchanges} exchange(s), per exchange: "
                 f"{per_exchange}")
    rollback = result.get("rollback")
    if rollback:
        rolled = rollback["peer_id"]
        to_rolled = [exchange for form in ("paged", "single") for exchange in sync.get(form, [])
                     if exchange.get("peer") == rolled and exchange.get("release") == "new"]
        while_old = [exchange for exchange in to_rolled if exchange["peer_release"] == "old"]
        after = [exchange for exchange in to_rolled
                 if exchange.get("start", rollback["old_until"]) >= rollback["old_until"]]
        lines.append(
            f"  rolled back: {rollback['node']} ran the previous release from "
            f"{rollback['old_from']:%H:%M:%S} to {rollback['old_until']:%H:%M:%S}; this tree's "
            f"nodes sent it {sum(1 for e in while_old if e['form'] == 'single')} single and "
            f"{sum(1 for e in while_old if e['form'] == 'paged')} paged exchange(s) then, and "
            f"{sum(1 for e in after if e['form'] == 'paged')} paged and "
            f"{sum(1 for e in after if e['form'] == 'single')} single after it came back")
    return lines


def summary_json(results, failures, releases, args):
    def plain(result):
        return {
            "decode_failures": result["decode_failures"],
            "unanswered": [{"sender": s, "target": t, "kind": k, "count": c}
                           for (s, t, k), c in sorted(result["unanswered"].items())],
            "penalties": [{"observer": o, "target": t, "reason": r, "count": c, "weight": w}
                          for (o, t, r), (c, w) in sorted(result["penalties"].items())],
            "refusals": result["refusals"],
            "penalties_on_refusals": [{"observer": o, "target": t, "reason": r, "count": c,
                                       "weight": w}
                                      for (o, t, r), (c, w) in
                                      sorted(result["penalties_on_refusals"].items())],
            "penalties_on_full": [{"observer": o, "target": t, "reason": r, "count": c,
                                   "weight": w}
                                  for (o, t, r), (c, w) in
                                  sorted(result["penalties_on_full"].items())],
            "penalties_on_restart": [{"observer": o, "target": t, "reason": r, "count": c,
                                      "weight": w}
                                     for (o, t, r), (c, w) in
                                     sorted(result.get("penalties_on_restart", {}).items())],
            "between_releases": [{"observer_release": o, "target_release": t, "reason": r,
                                  "count": c, "weight": w}
                                 for (o, t, r), (c, w) in
                                 sorted(result["between_releases"].items())],
            "traffic": result["traffic"],
            "audits": result["audits"],
            "held_by_storers": [record["held_by_storers"] for record in result["checks"]],
            "held_by_close_group": [record["held_by_close_group"]
                                    for record in result["checks"]],
            "stored": [bool(record.get("stored")) for record in result["records"]],
            "full_evidence": result["full_evidence"],
            "sync": result.get("sync"),
            "paging": result.get("paging"),
            "rollback": result.get("rollback"),
            "full_run": result.get("full_run"),
        }
    baselines = [result for name, result in results.items() if is_baseline(name)]
    runs = {}
    for name, result in results.items():
        runs[name] = plain(result)
        # Every count measured against the all-old runs, within its limit or
        # not, so a small excess shows before the canary.
        kinds = measured(name, baselines)
        if kinds:
            runs[name]["comparisons"] = [compared for compared in
                                         comparisons(result, baselines, args)
                                         if compared["kind"] in kinds]
    return {
        "releases": releases,
        "measured_against": [name for name in results if is_baseline(name)],
        "thresholds": {"penalty_slack": args.penalty_slack,
                       "penalty_ratio": args.penalty_ratio,
                       "new_reason_allowance": args.new_reason_allowance,
                       "shape_floor": SHAPE_FLOOR, "stall_limit": STALL_LIMIT},
        "runs": runs,
        "failures": failures,
        "passed": not failures,
    }


# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--old-ref", help="the previous release's tag or commit "
                        "(default: the newest v*+ds.* tag before HEAD)")
    parser.add_argument("--old-bin", help="a folder with the previous release's ant-node "
                        "and ant-devnet, instead of building them")
    parser.add_argument("--new-bin", help="a folder with this tree's ant-node, ant-devnet "
                        "and darwin-harness, instead of building them")
    parser.add_argument("--profile", default="release", help="cargo profile (default: release)")
    parser.add_argument("--cargo-arg", action="append", default=[],
                        help="an argument for every cargo build, e.g. --cargo-arg=-j2")
    parser.add_argument("--runs", default="all-old,new-hosts-old,old-hosts-new",
                        help="the runs, in order (all-old first: the others are measured "
                        f"against it); any of {', '.join(RUNS)}")
    parser.add_argument("--baselines", type=int, default=BASELINES,
                        help="how many all-old runs to make (all-old, all-old-2, ...): "
                        "the mixed runs are measured against all of them "
                        f"(default {BASELINES})")
    parser.add_argument("--incumbents", type=int, default=8)
    parser.add_argument("--joiners", type=int, default=8)
    parser.add_argument("--records-before", type=int, default=8,
                        help="records stored before the joiners join")
    parser.add_argument("--records-after", type=int, default=8,
                        help="records stored after they join")
    parser.add_argument("--settle-seconds", type=int, default=180,
                        help="wait after the incumbents start, before the first records "
                        "(default 180: a node refuses a paid PUT while its routing table "
                        "lacks the node it was paid through)")
    parser.add_argument("--join-seconds", type=int, default=120,
                        help="wait after the joiners start (default 120)")
    parser.add_argument("--observe-minutes", type=float, default=20,
                        help="how long the nodes run after the last upload (default 20: "
                        "every node's audit and neighbour sync tick once in 10 to 20)")
    parser.add_argument("--full", type=int, default=0,
                        help="joiners of the full run whose disk reads as full (adds the run)")
    parser.add_argument("--paged", action="store_true",
                        help="add the paged run, its incumbents admitting --busy-limit sync "
                        "requests at once")
    parser.add_argument("--page-bytes", type=int, default=PAGE_BYTES,
                        help="bytes of keys on a sync page in the runs that page, at least "
                        f"one key's 32 (default {PAGE_BYTES}: two keys)")
    parser.add_argument("--busy-limit", type=int, default=BUSY_LIMIT,
                        help="sync requests the paged run's incumbents admit at once, through "
                        f"the admission seam; 0 leaves their limit as it is (default "
                        f"{BUSY_LIMIT})")
    parser.add_argument("--rollback", action="store_true",
                        help="add the rollback run: a joiner started again on the previous "
                        "release's binary, then on this tree's")
    parser.add_argument("--rollback-minutes", type=float, default=ROLLBACK_MINUTES,
                        help="the least each phase of the rollback run lasts (default "
                        f"{ROLLBACK_MINUTES}); the last two go on, up to "
                        f"{ROLLBACK_WAIT_LIMIT} minutes, until they show their exchange")
    parser.add_argument("--penalty-slack", type=int, default=PENALTY_SLACK,
                        help="penalties a mixed run may count above the highest all-old "
                        f"count, for each pair of roles and reason (default {PENALTY_SLACK})")
    parser.add_argument("--penalty-ratio", type=float, default=PENALTY_RATIO,
                        help="against a single all-old run, or this share of its count, if "
                        f"more (default {PENALTY_RATIO})")
    parser.add_argument("--new-reason-allowance", type=int, default=NEW_REASON_ALLOWANCE,
                        help="penalties a mixed run may count against a role for a reason no "
                        f"all-old run charged that role with (default {NEW_REASON_ALLOWANCE})")
    parser.add_argument("--new-only-family", action="append", default=[],
                        help="a message family (as the traffic summary names it) only this "
                        "tree knows: no node of the previous release may receive it")
    parser.add_argument("--network-mode", choices=["testnet", "development"],
                        default="testnet", help="the standalone nodes' network mode; "
                        "development binds loopback only")
    parser.add_argument("--work", help="where runs write (default target/mv/runs/<time>)")
    parser.add_argument("--analyze", metavar="WORK",
                        help="read the runs a finished harness wrote to WORK again, with "
                        "these options' thresholds, without running anything")
    args = parser.parse_args()
    if args.analyze:
        work = Path(args.analyze)
        found = runs_in(work)
        if not found:
            raise HarnessError(f"no runs in {work}")
        return conclude(work, found, args)

    runs = [run.strip() for run in args.runs.split(",") if run.strip()]
    if args.full and "full" not in runs:
        runs.append("full")
    if args.paged and "paged" not in runs:
        runs.append("paged")
    if args.rollback and "rollback" not in runs:
        runs.append("rollback")
    if args.page_bytes < 32:
        parser.error("--page-bytes must be at least one key's 32 bytes")
    if args.busy_limit < 0:
        parser.error("--busy-limit must be 0 or more")
    if args.rollback_minutes < 5:
        parser.error("--rollback-minutes must be 5 or more: nodes log their traffic every 5")
    unknown = [run for run in runs if run not in RUNS]
    if unknown:
        parser.error(f"unknown runs: {', '.join(unknown)}")
    if "full" in runs and args.full < 1:
        parser.error("the full run needs --full N, with N of the joiners full")
    if args.observe_minutes < 5:
        parser.error("observe for 5 minutes or more: nodes log their traffic every 5")
    if args.baselines < 1:
        parser.error("--baselines must be 1 or more")
    if "all-old" in runs:
        at = runs.index("all-old") + 1
        runs[at:at] = [f"all-old-{number}" for number in range(2, args.baselines + 1)]
    if shutil.which("anvil") is None:
        raise HarnessError("anvil (Foundry) is not on PATH: the devnet's chain needs it")

    cache = REPO / "target" / "mv"
    new_folder = build_new(args)
    new = Release("new", f"this tree ({git('rev-parse', '--short', 'HEAD')})", new_folder)
    harness = new_folder / f"darwin-harness{EXE}"
    if not harness.exists():
        raise HarnessError(f"{harness} is missing")
    needs_old = any(run in ("all-old",) + OLD_RUNS for run in runs)
    releases = [new]
    old = None
    if needs_old:
        old_folder, label = build_old(args, cache)
        old = Release("old", label, old_folder)
        releases.insert(0, old)
    for release in releases:
        log(str(release))
    if "full" in runs and not new.seams:
        raise HarnessError("this tree's ant-node has no free-space seam: build it with "
                           "--features test-utils")
    if "paged" in runs and not new.reads_pages:
        raise HarnessError("this tree's ant-node reads no sync pages: the paged run needs a "
                           "release that pages")
    # The rollback run sets no seam on the previous release's binary, so it
    # needs none: a release before the seams, ds.15 among them, runs as it is.
    paging_from = paging_release()
    plans_paging = {name: paging_plan(name, new, old, paging_from, args) for name in runs}
    for name, paging in plans_paging.items():
        needed = []
        if paging["page_bytes"] and not new.page_seams:
            needed.append(PAGE_SEAM)
        if paging["announced"] and not new.announce_seam:
            needed.append(ANNOUNCED_SEAM)
        if paging["busy_limit"] and not new.admission_seam:
            needed.append(ADMISSION_SEAM)
        if needed:
            raise HarnessError(f"this tree's ant-node lacks the seam(s) the {name} run sets "
                               f"({', '.join(needed)}): build it with --features test-utils")
        if paging["announced"]:
            log(f"{name}: this tree's nodes announce release {paging['announced']}, the first "
                "that reads pages")
    if "full" in runs and not new.timing_seams:
        raise HarnessError("this tree's ant-node lacks the sync, out-of-range wait or "
                           "freshness seam the full run needs")

    work = Path(args.work) if args.work else (
        cache / "runs" / datetime.datetime.now().strftime("%Y%m%d-%H%M%S"))
    work.mkdir(parents=True, exist_ok=True)
    log(f"writing to {work}")
    disk = shutil.disk_usage(work)
    if disk.free * 100 < disk.total * 15:
        # Nodes read the host's disk: below 10% free they are under disk
        # pressure, and may erase records far from them, as in production.
        log(f"warning: {disk.free * 100 // disk.total}% of the disk under {work} is free; "
            "below 10% every node is under disk pressure")
    plans = {
        "all-old": (old, old),
        "new-hosts-old": (new, old),
        "old-hosts-new": (old, new),
        "all-new": (new, new),
        "full": (new, new),
        "paged": (new, new),
        "rollback": (new, new),
    }
    keep_awake()
    for name in runs:
        network_release, joiner_release = plans["all-old" if is_baseline(name) else name]
        run_one(name, network_release, joiner_release, args, harness, work / name, old=old,
                paging=plans_paging[name])
        log(f"{name}: done")
    (work / "releases.json").write_text(json.dumps(
        {release.name: {"version": release.version, "from": release.label,
                        "seams": release.seams, "page_seams": release.page_seams,
                        "announce_seam": release.announce_seam,
                        "admission_seam": release.admission_seam,
                        "reads_pages": release.reads_pages}
         for release in releases}, indent=2))
    # Every run in the work folder: one run again into a folder whose other
    # runs passed is measured against the same all-old runs.
    return conclude(work, runs_in(work), args)


def conclude(work, runs, args):
    """Reads the runs in `work`, prints the report and the verdict, and
    writes summary.json."""
    results = {name: analyze(load_run(work / name)) for name in runs}
    failures = verdicts(results, args)
    print(report(results, args))
    releases = json.loads((work / "releases.json").read_text())
    summary = summary_json(results, failures, releases, args)
    (work / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    if failures:
        print("\nFAILED:")
        for failure in failures:
            print(f"  - {failure}")
        print(f"\nLogs and results: {work}")
        return 1
    print(f"\nPASSED: {', '.join(runs)}. Logs and results: {work}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except HarnessError as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(2)
