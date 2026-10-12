#!/usr/bin/env bash
#
# End-to-end test of uploads with an envelope (ant-listings v2) against a
# local devnet with EVM payments.
#
# This script:
# 1. Builds ant-devnet (this repo) and the `darwin` CLI (../darwin-client)
# 2. Starts a devnet with EVM payment enforcement (Anvil + nodes)
# 3. Uploads a small random file for every combination of readable (yes/no),
#    in the index (yes/no) and the 32 sets of shown payment fields. For each:
#    whether an envelope is stored, the saved .datamap and .record files, the
#    warnings, one vault transaction for the whole upload, the record checked
#    from the chain, and every way to download it: by address (or the
#    withheld message), with --datamap, and with --proof
# 4. Uploads with a name and content type, on private files too
# 5. Uploads a file of 64 or more chunks, over the old batch threshold: the
#    client pays per chunk (the nodes refuse merkle batches), so every chunk
#    and the envelope must be paid per chunk, with no batch paid on-chain
# 6. Checks `ant listings`: exactly the uploads in the index, in index order,
#    hidden fields absent, the right check levels, `--since`
# 7. Checks `ant listing <address>` for the uploads outside the index
# 8. Writes proofs with `ant file prove` and downloads with them
# 9. Stops the nodes (Anvil keeps running) and checks every proof with
#    `ant file verify` from the chain alone; tampered proofs must fail
# 10. Stops Anvil and reports results
#
# Exit 0 if ALL checks pass, non-zero otherwise.
#
# Needs cargo, Foundry (anvil and cast on PATH) and python3, on Linux, macOS
# or WSL. ant-devnet and ant must be built against the same ant-listings
# (version 2 envelopes, topic listings.v2).
#
# The devnet's nodes start dual-stack, or IPv4-only (`ant-devnet
# --ipv4-only`) on a host that can't bind an IPv6 loopback socket.
#
# Environment:
#   ANT_CLIENT_DIR               darwin-client checkout (default: ../darwin-client)
#   ANT_TEST_SKIP_BUILD=1        don't build; use ANT_DEVNET and ANT_CLI
#   ANT_DEVNET, ANT_CLI          binaries (default: the builds below)
#   ANT_TEST_CARGO_PROFILE       cargo profile to build (default: release)
#   ANT_TEST_DEVNET_NODES        node count (default: 25)
#   ANT_TEST_BOOTSTRAP_COUNT     bootstrap nodes (default: 3)
#   ANT_TEST_DEVNET_LOG_LEVEL    devnet log level (default: warn)
#   ANT_TEST_IPV4_ONLY           1: start the devnet IPv4-only; 0: dual-stack;
#                                default: IPv4-only when this host can't bind
#                                an IPv6 loopback socket
#   ANT_TEST_STABILIZE_SECS      wait after the devnet starts (default: 15)
#   ANT_TEST_SHOWN_SETS          shown sets to test, as bit masks 0-31 (bit 0
#                                tx hash, 1 block, 2 timestamp, 3 log index,
#                                4 quote), e.g. "0 1 16 31" (default: all 32)
#   ANT_TEST_SKIP_LARGE=1        skip the upload of 64 or more chunks
#   ANT_TEST_LARGE_FILE_MB       its size in MiB (default: 264, 67 chunks)
#   ANT_TEST_LISTINGS_WAIT_SECS  how long to wait for every listing to be
#                                served (default: 900)
#   ANT_TEST_KEEP=1              keep the work directory (it is also kept,
#                                without the devnet's data, when a check fails)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ANT_CLIENT_DIR="${ANT_CLIENT_DIR:-${PROJECT_DIR}/../darwin-client}"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ant_e2e_listings.XXXXXX")"
MANIFEST_FILE="${WORK_DIR}/manifest.json"
CHAIN_ONLY_MANIFEST="${WORK_DIR}/chain-only-manifest.json"
DEVNET_DATA="${WORK_DIR}/devnet"
DEVNET_LOG="${WORK_DIR}/devnet.log"
FILES_DIR="${WORK_DIR}/files"
OUT_DIR="${WORK_DIR}/out"
DL_DIR="${WORK_DIR}/downloads"
PROOFS_DIR="${WORK_DIR}/proofs"
CLIENT_HOME="${WORK_DIR}/client-home"
CHECK_PY="${WORK_DIR}/check.py"
EXPECTED_TSV="${WORK_DIR}/expected.tsv"
PLAIN_ADDRESSES="${WORK_DIR}/plain-addresses.txt"

DEVNET_NODES="${ANT_TEST_DEVNET_NODES:-25}"
BOOTSTRAP_COUNT="${ANT_TEST_BOOTSTRAP_COUNT:-3}"
DEVNET_LOG_LEVEL="${ANT_TEST_DEVNET_LOG_LEVEL:-warn}"
STABILIZE_SECS="${ANT_TEST_STABILIZE_SECS:-15}"
SHOWN_SETS="${ANT_TEST_SHOWN_SETS:-$(seq 0 31 | tr '\n' ' ')}"
LARGE_FILE_MB="${ANT_TEST_LARGE_FILE_MB:-264}"
LISTINGS_WAIT_SECS="${ANT_TEST_LISTINGS_WAIT_SECS:-900}"
CARGO_PROFILE="${ANT_TEST_CARGO_PROFILE:-release}"

# The payment fields in flag-bit order, as `--hide` names them.
FIELD_NAMES=(tx-hash block timestamp log-index quote)

DEVNET_PID=""
ANVIL_PID=""
ANVIL_WINPID=""
ANT_MANIFEST="${MANIFEST_FILE}"
RPC_URL=""
WALLET_KEY=""
VAULT_ADDRESS=""
NODE_COUNT=""
PASS_COUNT=0
FAIL_COUNT=0
TOTAL_COUNT=0

mkdir -p "${FILES_DIR}" "${OUT_DIR}" "${DL_DIR}" "${PROOFS_DIR}" "${CLIENT_HOME}"
printf 'tag\taddress\treadable\tindex\tbits\tname\tctype\tsize\ttx\tblock\tts\tlog\tquote\tt0\tt1\tfile\trecord\tdatamap\n' \
    > "${EXPECTED_TSV}"
: > "${PLAIN_ADDRESSES}"

# Stop the devnet. INT shuts it down (its Anvil stops with it); KILL stops
# only the nodes, which run inside the devnet process, and leaves Anvil.
stop_devnet() {
    local signal="$1" i
    if [ -z "${DEVNET_PID}" ]; then
        return 0
    fi
    if kill -0 "${DEVNET_PID}" 2>/dev/null; then
        kill "-${signal}" "${DEVNET_PID}" 2>/dev/null || true
        for i in $(seq 1 20); do
            kill -0 "${DEVNET_PID}" 2>/dev/null || break
            sleep 1
        done
        if kill -0 "${DEVNET_PID}" 2>/dev/null; then
            kill -KILL "${DEVNET_PID}" 2>/dev/null || true
        fi
    fi
    wait "${DEVNET_PID}" 2>/dev/null || true
    DEVNET_PID=""
}

# shellcheck disable=SC2329  # run by the EXIT trap
cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    stop_devnet INT
    if [ -n "${ANVIL_PID}" ] && kill -0 "${ANVIL_PID}" 2>/dev/null; then
        echo "Stopping Anvil (PID ${ANVIL_PID})..."
        kill "${ANVIL_PID}" 2>/dev/null || true
    fi
    if [ -n "${ANVIL_WINPID}" ]; then
        echo "Stopping Anvil (Windows PID ${ANVIL_WINPID})..."
        taskkill //F //PID "${ANVIL_WINPID}" >/dev/null 2>&1 || true
    fi
    if command -v pkill >/dev/null 2>&1; then
        pkill -P $$ 2>/dev/null || true
    fi
    if [ "${ANT_TEST_KEEP:-0}" = "1" ]; then
        echo "Work directory kept: ${WORK_DIR}"
    elif [ "${FAIL_COUNT}" -gt 0 ]; then
        rm -rf "${DEVNET_DATA}" "${FILES_DIR}/large.bin"
        echo "Work directory kept for the failures (logs, CLI output): ${WORK_DIR}"
    else
        rm -rf "${WORK_DIR}"
    fi
    echo "Cleanup complete."
}

trap cleanup EXIT

pass() {
    local test_name="$1"
    PASS_COUNT=$((PASS_COUNT + 1))
    TOTAL_COUNT=$((TOTAL_COUNT + 1))
    echo "  PASS: ${test_name}"
}

fail() {
    local test_name="$1"
    local reason="${2:-}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
    TOTAL_COUNT=$((TOTAL_COUNT + 1))
    echo "  FAIL: ${test_name}"
    if [ -n "${reason}" ]; then
        echo "        Reason: ${reason}"
    fi
}

# Fail with the problems a check wrote to a file, one per line.
fail_with() {
    local test_name="$1" problems_file="$2"
    fail "${test_name}"
    sed -e 's/^PROBLEM: /        - /' "${problems_file}" 2>/dev/null | head -20 || true
}

# Strip ANSI escape codes from stdin
strip_ansi() {
    sed $'s/\x1b\\[[0-9;]*m//g'
}

# The last lines of a CLI run's stderr, on one line.
last_error() {
    tail -3 "${OUT_DIR}/$1.err" 2>/dev/null | strip_ansi | tr '\n' ' ' | cut -c1-300
}

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

py() {
    "${PY}" "${CHECK_PY}" "$@"
}

# Run the `ant` CLI against the devnet: `run_ant pay|free NAME ARGS...`.
# stdout goes to $OUT_DIR/NAME.out and stderr to NAME.err; returns ant's
# exit status. `pay` hands it the devnet wallet's key. The client keeps its
# state (peer cache, payment checkpoints, kept records) under the work
# directory, away from the user's own.
run_ant() {
    local mode="$1" name="$2"
    shift 2
    local key=""
    if [ "${mode}" = "pay" ]; then
        key="${WALLET_KEY}"
    fi
    env -u SECRET_KEY \
        HOME="${CLIENT_HOME}" \
        XDG_DATA_HOME="${CLIENT_HOME}/.local/share" \
        XDG_CONFIG_HOME="${CLIENT_HOME}/.config" \
        ${key:+"SECRET_KEY=${key}"} \
        "${ANT_CLI}" \
        --devnet-manifest "${ANT_MANIFEST}" \
        --evm-network local \
        --allow-loopback \
        --ipv4-only \
        "$@" \
        > "${OUT_DIR}/${name}.out" 2> "${OUT_DIR}/${name}.err"
}

block_number() {
    "${CAST}" block-number --rpc-url "${RPC_URL}"
}

# `readable=yes index=no shown=tx-hash,quote` for a test's name.
case_label() {
    local readable="$1" index="$2" bits="$3" shown="" i
    for i in 0 1 2 3 4; do
        if [ $(( bits >> i & 1 )) -eq 1 ]; then
            shown="${shown:+${shown},}${FIELD_NAMES[$i]}"
        fi
    done
    [ "${readable}" = 1 ] && readable=yes || readable=no
    [ "${index}" = 1 ] && index=yes || index=no
    echo "readable=${readable} index=${index} shown=${shown:-none}"
}

echo "=============================================="
echo "  ant-node E2E Test: uploads with an envelope"
echo "=============================================="
echo "Work directory: ${WORK_DIR}"
echo ""

# Tools
PY="${PY:-python3}"
command -v "${PY}" >/dev/null 2>&1 || { echo "ERROR: ${PY} not found (set PY)"; exit 1; }
CAST="${CAST:-cast}"
command -v "${CAST}" >/dev/null 2>&1 || { echo "ERROR: cast (Foundry) not found (set CAST)"; exit 1; }
command -v anvil >/dev/null 2>&1 || { echo "ERROR: anvil (Foundry) not found on PATH"; exit 1; }
# The devnet's nodes start dual-stack unless told otherwise; a host that
# can't bind an IPv6 loopback socket needs them IPv4-only.
IPV4_ONLY="${ANT_TEST_IPV4_ONLY:-}"
if [ -z "${IPV4_ONLY}" ]; then
    if "${PY}" -c 'import socket; s = socket.socket(socket.AF_INET6); s.bind(("::1", 0)); s.close()' 2>/dev/null; then
        IPV4_ONLY=0
    else
        IPV4_ONLY=1
        echo "This host has no IPv6: starting the devnet IPv4-only."
    fi
fi
DEVNET_IP_FLAGS=()
if [ "${IPV4_ONLY}" = "1" ]; then
    DEVNET_IP_FLAGS+=(--ipv4-only)
fi

# The checks on the CLI's JSON output and the chain, in Python.
cat > "${CHECK_PY}" <<'PYEOF'
"""Checks for test_e2e_listings.sh. Each command reads the `ant` CLI's JSON
output (and the chain, through `cast`), prints values for the script as shell
assignments, prints what doesn't match the upload's choices to stderr as
`PROBLEM:` lines, and exits 1 if there is any."""

import json
import os
import shlex
import subprocess
import sys

# The five payment fields in flag-bit order, as `--hide` names them.
FIELDS = ["tx-hash", "block", "timestamp", "log-index", "quote"]
# How the upload warnings name each field, and what it gives away.
TEXT = {"tx-hash": "tx hash", "block": "block number", "timestamp": "block timestamp",
        "log-index": "log index", "quote": "quote"}
GIVES = {"tx-hash": "the transaction", "block": "the block", "timestamp": "the time",
         "log-index": "the log index", "quote": "the quote"}
# The key each shown field has in `ant listings --json`.
KEY = {"tx-hash": "tx_hash", "block": "block_number", "timestamp": "block_timestamp",
       "log-index": "log_index", "quote": "quote_hash"}
TX_WITHOUT_QUOTE = ("tx hash shown without the quote: the wallet and time stay public, "
                    "but nobody can check that the payment was for this file")
METADATA_ON_PRIVATE = ("the name and content type of this private file are readable by anyone "
                       "with the envelope's address, and by the operators of the nodes storing "
                       "it, though the file is not")
SHOWS_WALLET = ("the payment fields shown give away the paying wallet to anyone "
                "with the envelope's address")
INDEX_SHOWS_WALLET = ("a listing in the index shows roughly when it was paid, which can be "
                      "matched against the vault's payments to find the paying wallet")
MAX_CHUNK_SIZE = 4_190_208       # self_encryption: a file under 3 of these is 3 chunks
MERKLE_THRESHOLD = 64            # the old batch threshold: the large upload is over it
HOUR = 3600

PROBLEMS = []

# Python on Windows writes \r\n to a pipe; the script compares lines ending in
# \n.
for stream in (sys.stdout, sys.stderr):
    stream.reconfigure(newline="\n")


def same_path(printed, expected):
    """Whether the CLI's path is the script's: on Windows it joins with a
    backslash."""
    return printed is not None and (os.path.normcase(os.path.normpath(printed))
                                    == os.path.normcase(os.path.normpath(expected)))


def problem(text):
    PROBLEMS.append(text)


def finish(**values):
    for name, value in values.items():
        print(f"{name}={shlex.quote('' if value is None else str(value))}")
    for text in PROBLEMS:
        print(f"PROBLEM: {text}", file=sys.stderr)
    sys.exit(1 if PROBLEMS else 0)


def load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError) as error:
        problem(f"{path}: no JSON ({error})")
        finish()


def flag(text):
    return text in ("1", "yes", "true")


def opt(text):
    return None if text in ("-", "") else text


def num(value):
    """A JSON-RPC quantity ("0x1a") or a plain number."""
    if isinstance(value, str):
        return int(value, 16) if value.startswith("0x") else int(value)
    return int(value)


def norm(value):
    """Hex without 0x, lower case."""
    if value is None:
        return None
    value = str(value).lower()
    return value[2:] if value.startswith("0x") else value


def shown(bits, field):
    return bool(bits >> FIELDS.index(field) & 1)


def hidden_names(bits):
    return [field for field in FIELDS if not shown(bits, field)]


def needs_envelope(in_index, bits, has_metadata):
    """No envelope only out of the index, all five hidden and no metadata."""
    return in_index or bits != 0 or has_metadata


def given_away(hidden, by, extent):
    by_text = [TEXT[field] for field in by]
    verb = "is" if len(by) == 1 else "are"
    text = (f"{TEXT[hidden]} hidden, but {' and '.join(by_text)} {verb} shown, "
            f"so {GIVES[hidden]} is still public")
    if extent == "roughly":
        text += ", roughly (a second holds only a few blocks)"
    elif extent == "if-alone":
        text += " if the transaction paid for the envelope alone"
    return text


def warnings(readable, in_index, bits, has_metadata):
    """The warnings `ant file upload` gives, in its order, written out here
    independently of the client's code."""
    if not needs_envelope(in_index, bits, has_metadata):
        return []

    def is_shown(field):
        return shown(bits, field)

    out = []
    for hidden in hidden_names(bits):
        full = {
            "timestamp": ["block", "tx-hash", "quote"],
            "block": ["tx-hash", "quote"],
            "tx-hash": (["block", "log-index"] if is_shown("block") and is_shown("log-index")
                        else []) + ["quote"],
            "log-index": ["quote"],
            "quote": [],
        }[hidden]
        full = [field for field in full if is_shown(field)]
        if full:
            out.append(given_away(hidden, full, "fully"))
        elif hidden == "block" and is_shown("timestamp"):
            out.append(given_away(hidden, ["timestamp"], "roughly"))
        elif hidden == "tx-hash" and is_shown("timestamp") and is_shown("log-index"):
            out.append(given_away(hidden, ["timestamp", "log-index"], "roughly"))
        elif hidden == "log-index" and is_shown("tx-hash"):
            out.append(given_away(hidden, ["tx-hash"], "if-alone"))
    if is_shown("tx-hash") and not is_shown("quote"):
        out.append(TX_WITHOUT_QUOTE)
    if not readable and has_metadata:
        out.append(METADATA_ON_PRIVATE)
    if bits != 0:
        out.append(SHOWS_WALLET)
    elif in_index:
        out.append(INDEX_SHOWS_WALLET)
    return out


def expect(data, key, want, where=""):
    if data.get(key) != want:
        problem(f"{where}{key} is {data.get(key)!r}, expected {want!r}")


# ---------- an upload ----------

def cmd_upload(path, readable, in_index, bits, file, name, content_type, min_content):
    """`ant --json file upload`: the choices, the envelope, the saved files,
    the warnings and the chunk count."""
    j = load(path)
    readable, in_index, bits = flag(readable), flag(in_index), int(bits)
    name, content_type = opt(name), opt(content_type)
    has_metadata = name is not None or content_type is not None
    envelope = needs_envelope(in_index, bits, has_metadata)
    size = os.path.getsize(file)

    expect(j, "readable", readable)
    expect(j, "in_index", in_index)
    expect(j, "hidden", hidden_names(bits))
    expect(j, "mode", "listed" if readable and in_index else "public" if readable else "private")
    expect(j, "size", size)
    expect(j, "chunks_failed", 0)
    total = j.get("total_chunks") or 0
    if j.get("chunks_stored") != total:
        problem(f"{j.get('chunks_stored')} of {total} chunks stored")
    # One more chunk than the content for an envelope, or for a readable
    # upload's plain data map chunk.
    content = total - (1 if envelope or readable else 0)
    if min_content == "-":
        if size < 3 * MAX_CHUNK_SIZE and content != 3:
            problem(f"{total} chunks: {content} for the content, expected 3"
                    f"{' + 1 envelope' if envelope else ' + 1 data map' if readable else ''}")
    elif content < int(min_content):
        problem(f"{content} content chunks, expected at least {min_content}")

    env_address = j.get("envelope_address")
    if envelope and env_address is None:
        problem("no envelope, but these choices need one")
    if not envelope and env_address is not None:
        problem("an envelope was stored, but these choices need none")
    address = j.get("address")
    if readable:
        if not address:
            problem("a readable upload without an address")
        elif env_address is not None and address != env_address:
            problem(f"address {address} is not the envelope's {env_address}")
    elif address is not None:
        problem(f"a private upload with a download address {address}")

    # The saved files: <file>.datamap for a private upload, <file>.record
    # with an envelope.
    datamap, record = j.get("datamap"), j.get("record")
    if readable:
        if datamap is not None:
            problem(f"a readable upload saved a data map: {datamap}")
    elif not same_path(datamap, file + ".datamap"):
        problem(f"data map saved as {datamap!r}, expected {file}.datamap")
    elif not os.path.isfile(datamap):
        problem(f"{datamap} is missing")
    if envelope:
        if not same_path(record, file + ".record"):
            problem(f"record saved as {record!r}, expected {file}.record")
        elif not os.path.isfile(record):
            problem(f"{record} is missing")
        else:
            with open(record) as f:
                text = f.read().strip()
            # Version 1, then the envelope's address.
            if env_address and not text.startswith("01" + env_address):
                problem("the record doesn't start with 01 and the envelope's address")
        if j.get("record_checked") is not True:
            problem("the record was not checked against the chain")
        if "record_problem" in j:
            problem(f"record problem: {j['record_problem']}")
    else:
        for key in ("record", "record_checked", "record_problem"):
            if key in j:
                problem(f"{key} without an envelope: {j[key]!r}")

    got = j.get("warnings", [])
    want = warnings(readable, in_index, bits, has_metadata)
    if got != want:
        problem(f"warnings {got}, expected {want}")
    finish(ENVELOPE=env_address, ADDRESS=address, DATAMAP=datamap, RECORD=record,
           TOTAL_CHUNKS=total)


def cmd_warnings(readable, in_index, bits, has_metadata):
    for text in warnings(flag(readable), flag(in_index), int(bits), flag(has_metadata)):
        print(text)


# ---------- the chain ----------

def cast_json(cast, *args):
    out = subprocess.run([cast, *args, "--json"], capture_output=True, text=True, check=False)
    if out.returncode != 0:
        problem(f"cast {' '.join(args)}: {out.stderr.strip()[:200]}")
        finish()
    value = json.loads(out.stdout)
    # Newer cast (1.8) wraps some commands' JSON, `cast block` among them:
    # {"schema_version", "success", "data", "errors"}.
    if isinstance(value, dict) and "schema_version" in value and "data" in value:
        if not value.get("success"):
            problem(f"cast {' '.join(args)}: {value.get('errors')}")
            finish()
        value = value["data"]
    return value


def cmd_vault(cast, rpc, vault, dpm_topic, mpm_topic, first, last, total, mode):
    """The vault transactions in blocks first..last, read with cast: one
    per-chunk payment for everything (`single`), or per-chunk payments only,
    no merkle batch, for an upload of 64 chunks or more (`large`): the nodes
    refuse batch payments."""
    vault, dpm_topic, mpm_topic = vault.lower(), dpm_topic.lower(), mpm_topic.lower()
    total = int(total)
    txs = []
    for number in range(int(first), int(last) + 1):
        block = cast_json(cast, "block", str(number), "--rpc-url", rpc)
        for tx in block.get("transactions", []):
            tx_hash = tx if isinstance(tx, str) else tx.get("hash")
            receipt = cast_json(cast, "receipt", tx_hash, "--rpc-url", rpc)
            if (receipt.get("to") or "").lower() != vault:
                continue
            vault_logs = [log for log in receipt.get("logs", [])
                          if log.get("address", "").lower() == vault and log.get("topics")]
            payments = [(num(log["logIndex"]), norm(log["topics"][3]))
                        for log in vault_logs
                        if log["topics"][0].lower() == dpm_topic and len(log["topics"]) > 3]
            leaves = sum(2 ** int(log["data"][2:66], 16)
                         for log in vault_logs if log["topics"][0].lower() == mpm_topic)
            txs.append({"hash": tx_hash, "ok": num(receipt.get("status", "0x0")) == 1,
                        "payments": payments, "leaves": leaves,
                        "block": num(receipt["blockNumber"]), "time": num(block["timestamp"])})
    for tx in txs:
        if not tx["ok"]:
            problem(f"vault transaction {tx['hash']} failed")
        if not tx["payments"] and not tx["leaves"]:
            problem(f"vault transaction {tx['hash']} logged no payment")
    per_chunk = [tx for tx in txs if tx["payments"]]
    batches = [tx for tx in txs if tx["leaves"]]
    if mode == "single":
        if len(txs) != 1:
            problem(f"{len(txs)} vault transactions, expected 1 for the whole upload")
        if batches:
            problem("a merkle batch payment for an upload under 64 chunks")
        if len(per_chunk) == 1 and len(per_chunk[0]["payments"]) != total:
            problem(f"the payment paid {len(per_chunk[0]['payments'])} chunks, expected all {total}")
    else:
        content = total - 1
        if content < MERKLE_THRESHOLD:
            problem(f"{content} content chunks: under {MERKLE_THRESHOLD}, not a large upload "
                    "(the old batch threshold)")
        if batches:
            problem(f"{len(batches)} merkle batch payment(s) on-chain: the nodes must refuse "
                    "batches at quote time, before the client pays")
        paid = sum(len(tx["payments"]) for tx in per_chunk)
        if paid != total:
            problem(f"per-chunk payments paid {paid} chunks, expected all {total} "
                    "(the content and the envelope)")
    # The envelope is paid last, so a large upload's record is checked
    # against its last per-chunk payment.
    if mode == "single":
        pay = per_chunk[0] if len(per_chunk) == 1 else None
    else:
        pay = max(per_chunk, key=lambda tx: tx["block"]) if per_chunk else None
    finish(PAY_TX=pay and pay["hash"], PAY_BLOCK=pay and pay["block"],
           PAY_TS=pay and pay["time"],
           PAY_LOGS=pay and ",".join(f"{index}:{quote}" for index, quote in pay["payments"]),
           VAULT_TXS=len(txs))


# ---------- a proof ----------

def cmd_verify(path, envelope, readable, in_index, bits, revealed, name, content_type, size,
               file_matches, pay_tx, pay_block, pay_ts, pay_logs):
    """`ant --json file verify`: the proven envelope and payment, against the
    choices and against the vault transaction read with cast."""
    j = load(path)
    expect(j, "valid", True)
    expect(j, "address", envelope)
    expect(j, "readable", flag(readable))
    expect(j, "in_index", flag(in_index))
    expect(j, "hidden", hidden_names(int(bits)))
    expect(j, "revealed", flag(revealed))
    expect(j, "name", opt(name))
    expect(j, "content_type", opt(content_type))
    expect(j, "size", None if opt(size) is None else int(size))
    if file_matches == "-":
        if "file_matches" in j:
            problem(f"file_matches {j['file_matches']} without --file")
    else:
        expect(j, "file_matches", True)
    if not j.get("quote_hash"):
        problem("no quote hash")
    if opt(pay_tx) is not None:
        if norm(j.get("tx_hash")) != norm(pay_tx):
            problem(f"the proof's tx {j.get('tx_hash')} is not the upload's payment {pay_tx}")
        expect(j, "block_number", int(pay_block))
        expect(j, "block_timestamp", int(pay_ts))
        if f"{j.get('log_index')}:{norm(j.get('quote_hash'))}" not in pay_logs.split(","):
            problem(f"log {j.get('log_index')} of the payment doesn't pay quote {j.get('quote_hash')}")
    finish(TX=j.get("tx_hash"), BLOCK=j.get("block_number"), TS=j.get("block_timestamp"),
           LOG=j.get("log_index"), QUOTE=j.get("quote_hash"))


def cmd_prove(path, address, private, reveals):
    """`ant --json file prove`."""
    j = load(path)
    expect(j, "address", address)
    expect(j, "private", flag(private))
    expect(j, "reveals", flag(reveals))
    if not os.path.isfile(j.get("proof", "")):
        problem(f"no proof written at {j.get('proof')}")
    finish()


def varint(data, at):
    value = shift = 0
    while True:
        byte = data[at]
        at += 1
        value |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            return value, at


def proof_spans(data):
    """Where each field is in a proof's bytes: version 1, then postcard of
    (address [32], envelope Vec, tx hash [32], quote Vec, data map
    Option<Vec>, salt Option<[32]>)."""
    if data[0] != 1:
        raise ValueError(f"proof version {data[0]}")
    spans, at = {}, 1
    spans["address"] = (at, at + 32)
    at += 32
    length, at = varint(data, at)
    spans["envelope"] = (at, at + length)
    at += length
    spans["tx"] = (at, at + 32)
    at += 32
    length, at = varint(data, at)
    spans["quote"] = (at, at + length)
    at += length
    if data[at] == 1:
        length, at = varint(data, at + 1)
        spans["data_map"] = (at, at + length)
        at += length
    else:
        at += 1
    if data[at] == 1:
        spans["salt"] = (at + 1, at + 33)
        at += 33
    else:
        at += 1
    if at != len(data):
        raise ValueError(f"{len(data) - at} bytes after the proof")
    return spans


def cmd_tamper(source, target, field):
    """Write the proof at `source` to `target` with one byte of `field`
    changed: the first byte of the address, tx hash or salt, the last of the
    envelope or the quote (its signature)."""
    with open(source) as f:
        data = bytearray(bytes.fromhex(f.read().strip()))
    try:
        spans = proof_spans(data)
    except (ValueError, IndexError) as error:
        problem(f"can't read the proof: {error}")
        finish()
    if field not in spans:
        problem(f"the proof has no {field}")
        finish()
    start, end = spans[field]
    at = end - 1 if field in ("envelope", "quote") else start
    data[at] ^= 0x01
    with open(target, "w") as f:
        f.write(data.hex() + "\n")
    finish()


# ---------- listings ----------

def rows(tsv):
    with open(tsv) as f:
        header = f.readline().rstrip("\n").split("\t")
        out = {}
        for line in f:
            row = dict(zip(header, line.rstrip("\n").split("\t")))
            out[row["address"]] = row
        return out


def check_entry(entry, row, nearest, where):
    """One listing from `ant listings` or `ant listing`, against the upload's
    choices and its payment as `ant file verify` read it from the chain."""
    address = row["address"]

    def bad(text):
        problem(f"{where} {row['tag']} ({address[:12]}): {text}")

    readable, in_index, bits = flag(row["readable"]), flag(row["index"]), int(row["bits"])
    if entry.get("address") != address:
        bad(f"address {entry.get('address')}")
    for key, want in (("readable", readable), ("in_index", in_index),
                      ("hidden", hidden_names(bits)), ("name", opt(row["name"])),
                      ("content_type", opt(row["ctype"])),
                      ("size", None if opt(row["size"]) is None else int(row["size"]))):
        if entry.get(key) != want:
            bad(f"{key} is {entry.get(key)!r}, expected {want!r}")
    real = {"tx-hash": norm(opt(row["tx"])), "block": opt(row["block"]),
            "timestamp": opt(row["ts"]), "log-index": opt(row["log"]),
            "quote": norm(opt(row["quote"]))}
    for field in FIELDS:
        key = KEY[field]
        if not shown(bits, field):
            if key in entry:
                bad(f"{key} shown, but the upload hid it")
            continue
        if key not in entry:
            bad(f"{key} absent, but the upload shows it")
            continue
        if real[field] is None:
            continue
        value = entry[key]
        value = norm(value) if field in ("tx-hash", "quote") else str(value)
        if value != real[field]:
            bad(f"{key} is {entry[key]}, the chain says {real[field]}")
    # first_seen: in the index with the time hidden only, on the hour, during
    # the upload.
    first_seen = entry.get("first_seen")
    if in_index and not shown(bits, "timestamp"):
        if first_seen is None:
            bad("no first_seen, but it is in the index with the time hidden")
        elif first_seen % HOUR:
            bad(f"first_seen {first_seen} is not on the hour")
        elif not int(row["t0"]) // HOUR * HOUR <= first_seen <= int(row["t1"]):
            bad(f"first_seen {first_seen} is outside the upload ({row['t0']}..{row['t1']})")
    elif first_seen is not None:
        bad(f"first_seen {first_seen}, but only a listing in the index hiding its time has one")
    if shown(bits, "timestamp"):
        want_time = None if real["timestamp"] is None else int(real["timestamp"])
    else:
        want_time = first_seen if first_seen is not None else 0
    if want_time is not None and entry.get("time") != want_time:
        bad(f"time {entry.get('time')}, expected {want_time}")
    want_check = "verified" if shown(bits, "quote") else "reported"
    if entry.get("check") != want_check:
        bad(f"check {entry.get('check')!r}, expected {want_check} "
            f"(problem: {entry.get('problem')})")
    if entry.get("valid") is not True:
        bad(f"not valid: {entry.get('problem')}")
    if entry.get("quorum") is not True:
        bad(f"no quorum: {entry.get('nodes')} of {entry.get('nearest')} nearest nodes serve it")
    if not isinstance(entry.get("nodes"), int) or entry["nodes"] < 4:
        bad(f"served by {entry.get('nodes')} of the nearest nodes, expected at least 4")
    if entry.get("nearest") != nearest:
        bad(f"{entry.get('nearest')} nearest nodes asked, expected {nearest}")
    if readable != bool(entry.get("data_map")):
        bad("a data map on a private listing" if not readable else "no data map on a readable listing")


def index_key(entry):
    return (entry.get("time", 0), entry.get("block_number", 0), entry.get("log_index", 0),
            entry.get("address", ""))


def cmd_listings_ready(path, tsv):
    """Exit 0 once `ant listings` serves exactly the uploads in the index."""
    want = {address for address, row in rows(tsv).items() if flag(row["index"])}
    got = {entry.get("address") for entry in load(path) if entry.get("valid")}
    sys.exit(0 if got == want else 1)


def cmd_listings(path, tsv, nearest):
    """`ant --json listings --all`: every upload in the index and nothing
    else, all valid, in index order, each as its choices say."""
    nearest = int(nearest)
    entries = load(path)
    expected = {address: row for address, row in rows(tsv).items() if flag(row["index"])}
    others = {address: row for address, row in rows(tsv).items() if not flag(row["index"])}
    addresses = [entry.get("address") for entry in entries]
    if len(set(addresses)) != len(addresses):
        problem("an address is listed more than once")
    missing = set(expected) - set(addresses)
    for address in sorted(missing):
        problem(f"{expected[address]['tag']} ({address[:12]}) is in the index but not listed")
    for address in sorted(set(addresses) - set(expected)):
        tag = others[address]["tag"] if address in others else "an unknown upload"
        problem(f"{tag} ({address[:12]}) is listed, but it is not in the index")
    valid = [entry for entry in entries if entry.get("valid")]
    keys = [index_key(entry) for entry in valid]
    if keys != sorted(keys):
        problem("listings are not in index order (time, block, log index, address)")
    for entry in entries:
        if entry.get("address") in expected:
            check_entry(entry, expected[entry["address"]], nearest, "ant listings:")
    finish(LISTED=len(entries))


def cmd_since_pick(path):
    """A time that leaves some listings out: the middle of the distinct times."""
    times = sorted({entry.get("time", 0) for entry in load(path) if entry.get("valid")})
    times = [time for time in times if time > 0] or [1]
    finish(SINCE=times[len(times) // 2])


def cmd_since(full_path, part_path, since):
    """`ant listings --since S`: the listings from time S on, in order."""
    since = int(since)
    want = [entry["address"] for entry in load(full_path)
            if entry.get("valid") and entry.get("time", 0) >= since]
    got = [entry.get("address") for entry in load(part_path)]
    if got != want:
        problem(f"--since {since} gave {len(got)} listings, expected {len(want)} "
                f"(those from that time on, in order)")
    finish(COUNT=len(got))


def cmd_listing(path, tsv, address, nearest):
    """`ant --json listing <address>` for an upload with an envelope."""
    j = load(path)
    row = rows(tsv)[address]
    listing = j.get("listing")
    if listing is None:
        problem(f"no valid listing; rejected: {[r.get('problem') for r in j.get('rejected', [])]}")
    else:
        check_entry(listing, row, int(nearest), "ant listing:")
    if j.get("rejected"):
        problem(f"rejected copies: {[r.get('problem') for r in j['rejected']]}")
    expect(j, "nearest", int(nearest))
    finish()


def cmd_no_listing(path):
    """`ant --json listing <address>` for an address with no envelope."""
    j = load(path)
    if j.get("listing") is not None:
        problem(f"a listing for an address with no envelope: {j['listing']}")
    finish()


# ---------- the devnet ----------

def cmd_manifest(path):
    m = load(path)
    evm = m.get("evm") or {}
    for key in ("rpc_url", "wallet_private_key", "payment_vault_address"):
        if not evm.get(key):
            problem(f"the manifest has no evm.{key}")
    finish(RPC_URL=evm.get("rpc_url"), WALLET_KEY=evm.get("wallet_private_key"),
           VAULT_ADDRESS=evm.get("payment_vault_address"), NODE_COUNT=m.get("node_count"))


def cmd_chain_only(source, target):
    """The manifest without a single node: only the chain is left."""
    m = load(source)
    m["bootstrap"] = []
    m.pop("bootstrap_webrtc", None)
    with open(target, "w") as f:
        json.dump(m, f, indent=2)
    finish()


COMMANDS = {
    "upload": cmd_upload, "warnings": cmd_warnings, "vault": cmd_vault, "verify": cmd_verify,
    "prove": cmd_prove, "tamper": cmd_tamper, "listings-ready": cmd_listings_ready,
    "listings": cmd_listings, "since-pick": cmd_since_pick, "since": cmd_since,
    "listing": cmd_listing, "no-listing": cmd_no_listing, "manifest": cmd_manifest,
    "chain-only": cmd_chain_only,
}

if __name__ == "__main__":
    COMMANDS[sys.argv[1]](*sys.argv[2:])
PYEOF

# Step 1: Build
echo "=== Step 1: Building ant-devnet and ant ==="
if [ "${ANT_TEST_SKIP_BUILD:-0}" = "1" ]; then
    echo "Skipping the build (ANT_TEST_SKIP_BUILD=1)"
else
    command -v cargo >/dev/null 2>&1 || { echo "ERROR: cargo not found"; exit 1; }
    target_dir() {
        (cd "$1" && cargo metadata --format-version 1 --no-deps 2>/dev/null) \
            | "${PY}" -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])'
    }
    profile_dir="${CARGO_PROFILE}"
    [ "${profile_dir}" = "dev" ] && profile_dir="debug"
    (cd "${PROJECT_DIR}" && cargo build --profile "${CARGO_PROFILE}" --bin ant-devnet 2>&1 | tail -3)
    (cd "${ANT_CLIENT_DIR}" && cargo build --profile "${CARGO_PROFILE}" -p ant-cli --bin darwin 2>&1 | tail -3)
    ANT_DEVNET="${ANT_DEVNET:-$(target_dir "${PROJECT_DIR}")/${profile_dir}/ant-devnet}"
    ANT_CLI="${ANT_CLI:-$(target_dir "${ANT_CLIENT_DIR}")/${profile_dir}/darwin}"
fi
ANT_DEVNET="${ANT_DEVNET:-${PROJECT_DIR}/target/release/ant-devnet}"
ANT_CLI="${ANT_CLI:-${ANT_CLIENT_DIR}/target/release/darwin}"
for binary in "${ANT_DEVNET}" "${ANT_CLI}"; do
    if [ ! -x "${binary}" ]; then
        echo "ERROR: ${binary} not found"
        exit 1
    fi
done
echo "ant-devnet: ${ANT_DEVNET}"
echo "ant:        ${ANT_CLI}"
echo ""

# Step 2: Start devnet with EVM
echo "=== Step 2: Starting devnet with EVM (${DEVNET_NODES} nodes, ${BOOTSTRAP_COUNT} bootstrap) ==="
"${ANT_DEVNET}" \
    --nodes "${DEVNET_NODES}" \
    --bootstrap-count "${BOOTSTRAP_COUNT}" \
    --enable-evm \
    --manifest "${MANIFEST_FILE}" \
    --data-dir "${DEVNET_DATA}" \
    --stabilization-timeout-secs 180 \
    --enable-logging \
    --log-level "${DEVNET_LOG_LEVEL}" \
    ${DEVNET_IP_FLAGS[@]+"${DEVNET_IP_FLAGS[@]}"} \
    > "${DEVNET_LOG}" 2>&1 &
DEVNET_PID=$!
echo "Devnet starting (PID ${DEVNET_PID}), waiting for manifest..."

WAIT_COUNT=0
MAX_WAIT=300
while [ ! -f "${MANIFEST_FILE}" ] && [ ${WAIT_COUNT} -lt ${MAX_WAIT} ]; do
    if ! kill -0 "${DEVNET_PID}" 2>/dev/null; then
        echo "ERROR: Devnet process died before producing manifest."
        tail -30 "${DEVNET_LOG}" 2>/dev/null || true
        if [ "${IPV4_ONLY}" != "1" ] && grep -qiE "dual-stack|Failed to create transport|Address family not supported" "${DEVNET_LOG}" 2>/dev/null; then
            echo "This host may have no IPv6: rerun with ANT_TEST_IPV4_ONLY=1."
        fi
        exit 1
    fi
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done
if [ ! -f "${MANIFEST_FILE}" ]; then
    echo "ERROR: Manifest not created after ${MAX_WAIT} seconds."
    tail -30 "${DEVNET_LOG}" 2>/dev/null || true
    exit 1
fi
# The manifest is written in one go, but give the writer a moment.
sleep 1
echo "Manifest created at ${MANIFEST_FILE}"

MANIFEST_VARS=""
if ! MANIFEST_VARS=$(py manifest "${MANIFEST_FILE}"); then
    echo "ERROR: Could not read the EVM block of the manifest."
    exit 1
fi
eval "${MANIFEST_VARS}"
NEAREST=$(( NODE_COUNT < 7 ? NODE_COUNT : 7 ))

# Anvil is the devnet's child. Remember it: Step 9 stops the nodes and keeps
# Anvil, and cleanup stops it.
ANVIL_PID=$(ps -A -o pid= -o ppid= -o comm= 2>/dev/null \
    | awk -v parent="${DEVNET_PID}" '$2 == parent && $3 ~ /(^|\/)anvil$/ { print $1; exit }' || true)
# Git Bash on Windows: `ps` can't list it, and the devnet, a native process,
# ignores SIGINT there (stop_devnet kills it), so Anvil is found by the port it
# listens on and stopped by its Windows process ID.
if [ -z "${ANVIL_PID}" ] && command -v taskkill >/dev/null 2>&1; then
    ANVIL_PORT=$(printf '%s' "${RPC_URL}" | sed -nE 's#^[a-z]+://[^/]*:([0-9]+).*#\1#p')
    ANVIL_WINPID=$(netstat -ano 2>/dev/null | tr -d '\r' \
        | awk -v suffix=":${ANVIL_PORT}" '$1 == "TCP" && $4 == "LISTENING" && substr($2, length($2) - length(suffix) + 1) == suffix { print $5; exit }' || true)
fi
if [ -z "${ANVIL_PID}" ] && [ -z "${ANVIL_WINPID}" ]; then
    echo "WARNING: Anvil's process not found among the devnet's children; stop it by hand"
    echo "         after the test if it is still running (${RPC_URL})."
fi

if "${CAST}" block-number --rpc-url "${RPC_URL}" >/dev/null 2>&1; then
    echo "Anvil RPC confirmed working: ${RPC_URL}"
else
    echo "ERROR: Anvil RPC not responding at ${RPC_URL}"
    exit 1
fi
DPM_TOPIC=$("${CAST}" keccak "DataPaymentMade(address,uint256,bytes32)")
MPM_TOPIC=$("${CAST}" keccak "MerklePaymentMade(bytes32,uint8,uint256,uint64)")

echo "Waiting ${STABILIZE_SECS} seconds for the network to settle..."
sleep "${STABILIZE_SECS}"
echo ""

# Check a download: `check_download LABEL NAME ORIGINAL ARGS...`.
check_download() {
    local label="$1" name="$2" original="$3"
    shift 3
    local out="${DL_DIR}/${name}"
    rm -f "${out}"
    if run_ant free "${name}" --json file download "$@" -o "${out}"; then
        if [ -f "${out}" ] && [ "$(sha256_of "${out}")" = "$(sha256_of "${original}")" ]; then
            pass "${label}"
        else
            fail "${label}" "the downloaded file differs from the original"
        fi
    else
        fail "${label}" "$(last_error "${name}")"
    fi
    rm -f "${out}"
}

# Upload a file and check everything about it:
# `upload_case READABLE INDEX BITS NAME CONTENT_TYPE` (NAME, CONTENT_TYPE may
# be empty).
upload_case() {
    local readable="$1" index="$2" bits="$3" name="$4" ctype="$5"
    local tag label file size flags=() hidden=() field i
    local b0 b1 t0 t1 vars ok problems warnings_got warnings_want
    local ENVELOPE="" ADDRESS="" DATAMAP="" RECORD="" TOTAL_CHUNKS=""
    local PAY_TX="" PAY_BLOCK="" PAY_TS="" PAY_LOGS="" VAULT_TXS=""
    local TX="-" BLOCK="-" TS="-" LOG="-" QUOTE="-"

    tag="r${readable}i${index}s$(printf '%02d' "${bits}")${name:+n}${ctype:+t}"
    label="$(case_label "${readable}" "${index}" "${bits}")${name:+ name=${name}}${ctype:+ type=${ctype}}"
    file="${FILES_DIR}/${tag}.bin"
    size=$(( 4096 + RANDOM % 61440 ))
    head -c "${size}" /dev/urandom > "${file}"
    problems="${OUT_DIR}/${tag}.problems"

    # Readable and in the index: the shorthands for some, the flags for
    # others, and the defaults (private, all shown) once.
    case "${readable}${index}" in
        00)
            if [ -z "${name}${ctype}" ] && [ "${bits}" -eq 31 ]; then
                :
            elif [ $(( bits % 2 )) -eq 0 ]; then
                flags+=(--visibility private)
            else
                flags+=(--readable no --index no)
            fi
            ;;
        10)
            if [ $(( bits % 4 )) -eq 1 ]; then
                flags+=(--public)
            elif [ $(( bits % 2 )) -eq 0 ]; then
                flags+=(--visibility unlisted)
            else
                flags+=(--readable yes --index no)
            fi
            ;;
        11)
            if [ $(( bits % 2 )) -eq 0 ]; then
                flags+=(--visibility listed)
            else
                flags+=(--readable yes --index yes)
            fi
            ;;
        01)
            flags+=(--readable no --index yes)
            ;;
    esac
    # Hidden fields: `all`, a comma list, or the flag repeated.
    for i in 0 1 2 3 4; do
        if [ $(( bits >> i & 1 )) -eq 0 ]; then
            hidden+=("${FIELD_NAMES[$i]}")
        fi
    done
    if [ "${bits}" -eq 0 ]; then
        flags+=(--hide all)
    elif [ "${#hidden[@]}" -gt 0 ] && [ $(( bits % 3 )) -eq 0 ]; then
        for field in "${hidden[@]}"; do
            flags+=(--hide "${field}")
        done
    elif [ "${#hidden[@]}" -gt 0 ]; then
        flags+=(--hide "$(IFS=,; echo "${hidden[*]}")")
    fi
    if [ -n "${name}" ]; then
        flags+=(--name "${name}")
    fi
    if [ -n "${ctype}" ]; then
        flags+=(--content-type "${ctype}")
    fi

    echo ""
    echo "--- ${label} (${size} bytes) ---"
    b0=$(block_number)
    t0=$(date +%s)
    if ! run_ant pay "up_${tag}" --json file upload "${file}" ${flags[@]+"${flags[@]}"}; then
        fail "${label}: upload" "$(last_error "up_${tag}")"
        return 0
    fi
    t1=$(date +%s)
    b1=$(block_number)

    # The result: envelope or not, saved files, warnings, chunks.
    ok=0
    if vars=$(py upload "${OUT_DIR}/up_${tag}.out" "${readable}" "${index}" "${bits}" "${file}" \
        "${name:--}" "${ctype:--}" - 2> "${problems}"); then
        ok=1
    fi
    eval "${vars}"
    if [ "${ok}" = 1 ]; then
        pass "${label}: upload result (${TOTAL_CHUNKS} chunks${ENVELOPE:+, envelope ${ENVELOPE:0:12}}${DATAMAP:+, .datamap}${RECORD:+, .record})"
    else
        fail_with "${label}: upload result" "${problems}"
    fi
    if [ "${readable}" = 1 ] && [ -z "${ENVELOPE}" ] && [ -n "${ADDRESS}" ]; then
        echo "${ADDRESS}" >> "${PLAIN_ADDRESSES}"
    fi

    # The same warnings on stderr, before anything is paid.
    warnings_got=$(grep '^warning: ' "${OUT_DIR}/up_${tag}.err" | sed 's/^warning: //' | sort || true)
    warnings_want=$(py warnings "${readable}" "${index}" "${bits}" \
        "$( [ -n "${name}${ctype}" ] && echo 1 || echo 0)" | sort)
    if [ "${warnings_got}" = "${warnings_want}" ]; then
        pass "${label}: warnings on stderr ($(printf '%s' "${warnings_want}" | grep -c . || true))"
    else
        fail "${label}: warnings on stderr" "got: ${warnings_got:-none}; expected: ${warnings_want:-none}"
    fi

    # One vault transaction for the content and the envelope.
    if vars=$(py vault "${CAST}" "${RPC_URL}" "${VAULT_ADDRESS}" "${DPM_TOPIC}" "${MPM_TOPIC}" \
        "$((b0 + 1))" "${b1}" "${TOTAL_CHUNKS:-0}" single 2> "${problems}"); then
        pass "${label}: one vault transaction for all ${TOTAL_CHUNKS} chunks"
    else
        fail_with "${label}: one vault transaction" "${problems}"
    fi
    eval "${vars}"

    # The record, checked from the chain, against the vault transaction.
    if [ -n "${RECORD}" ]; then
        if run_ant free "verify_${tag}" --json file verify "${RECORD}" --file "${file}"; then
            if vars=$(py verify "${OUT_DIR}/verify_${tag}.out" "${ENVELOPE}" "${readable}" \
                "${index}" "${bits}" "$( [ "${readable}" = 1 ] && echo 0 || echo 1)" \
                "${name:--}" "${ctype:--}" "$( [ "${readable}" = 1 ] && echo "${size}" || echo -)" \
                true "${PAY_TX:--}" "${PAY_BLOCK:--}" "${PAY_TS:--}" "${PAY_LOGS:--}" \
                2> "${problems}"); then
                pass "${label}: record checks out against the payment"
            else
                fail_with "${label}: record checks out against the payment" "${problems}"
            fi
            eval "${vars}"
        else
            fail "${label}: ant file verify (record)" "$(last_error "verify_${tag}")"
        fi
        printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
            "${tag}" "${ENVELOPE}" "${readable}" "${index}" "${bits}" "${name:--}" "${ctype:--}" \
            "$( [ "${readable}" = 1 ] && echo "${size}" || echo -)" \
            "${TX:--}" "${BLOCK:--}" "${TS:--}" "${LOG:--}" "${QUOTE:--}" "${t0}" "${t1}" \
            "${file}" "${RECORD}" "${DATAMAP:--}" >> "${EXPECTED_TSV}"
    fi

    # Downloads.
    if [ "${readable}" = 1 ] && [ -n "${ADDRESS}" ]; then
        check_download "${label}: download by address" "dl_${tag}_address" "${file}" "${ADDRESS}"
    elif [ -n "${ENVELOPE}" ]; then
        # A private file's envelope withholds the data map.
        if run_ant free "dl_${tag}_withheld" --json file download "${ENVELOPE}" -o "${DL_DIR}/withheld"; then
            fail "${label}: withheld message" "the envelope's address downloaded a private file"
        elif grep -qF "private file's envelope" "${OUT_DIR}/dl_${tag}_withheld.err"; then
            pass "${label}: by address, the withheld message"
        else
            fail "${label}: withheld message" "$(last_error "dl_${tag}_withheld")"
        fi
        rm -f "${DL_DIR}/withheld"
    fi
    if [ -n "${DATAMAP}" ]; then
        check_download "${label}: download with --datamap" "dl_${tag}_datamap" "${file}" \
            --datamap "${DATAMAP}"
        if [ -n "${ENVELOPE}" ]; then
            # With the envelope's address too: the data map is checked
            # against the envelope, through the record next to it.
            check_download "${label}: download with the address and --datamap" \
                "dl_${tag}_address_datamap" "${file}" "${ENVELOPE}" --datamap "${DATAMAP}"
        fi
    fi
    if [ -n "${RECORD}" ]; then
        check_download "${label}: download with --proof (the record)" "dl_${tag}_record" "${file}" \
            --proof "${RECORD}"
    fi
    return 0
}

# Step 3: Every combination of readable, in the index and shown fields
echo "=== Step 3: Uploads: readable x in the index x shown fields ==="
CASE_COUNT=0
for bits in ${SHOWN_SETS}; do
    if ! [ "${bits}" -ge 0 ] 2>/dev/null || [ "${bits}" -gt 31 ]; then
        echo "ERROR: ANT_TEST_SHOWN_SETS takes bit masks 0-31, not ${bits}"
        exit 1
    fi
    bits=$((10#${bits}))
    for readable in 0 1; do
        for index in 0 1; do
            CASE_COUNT=$((CASE_COUNT + 1))
            upload_case "${readable}" "${index}" "${bits}" "" ""
        done
    done
done
echo ""
echo "${CASE_COUNT} combinations uploaded."
echo ""

# Step 4: Name and content type
#
# Metadata is public on every visibility. On a private file it needs an
# envelope even with every field hidden, and the upload warns that anyone
# with the envelope's address can read it.
echo "=== Step 4: Uploads with a name and content type ==="
upload_case 0 0 31 "notes.txt" "text/plain"
upload_case 0 0 0 "dated-notes.txt" ""
upload_case 1 0 0 "" "application/octet-stream"
echo ""

# Step 5: An upload of 64 chunks or more
#
# Over the old batch threshold (64 chunks). The client pays per chunk, in
# transactions of up to 64 chunks, and the nodes refuse batch payments, so
# the upload must be paid per chunk, with no batch paid on-chain.
echo "=== Step 5: An upload of 64 or more chunks ==="
if [ "${ANT_TEST_SKIP_LARGE:-0}" = "1" ]; then
    echo "  Skipping (ANT_TEST_SKIP_LARGE=1)"
else
    LARGE_FILE="${FILES_DIR}/large.bin"
    LARGE_LABEL="large listed file (${LARGE_FILE_MB} MiB)"
    LARGE_PROBLEMS="${OUT_DIR}/large.problems"
    echo "  Generating ${LARGE_FILE_MB} MiB of random data..."
    if ! dd if=/dev/urandom of="${LARGE_FILE}" bs=1048576 count="${LARGE_FILE_MB}" 2>/dev/null; then
        dd if=/dev/urandom of="${LARGE_FILE}" bs=1m count="${LARGE_FILE_MB}" 2>/dev/null
    fi
    ENVELOPE="" ADDRESS="" DATAMAP="" RECORD="" TOTAL_CHUNKS=""
    PAY_TX="" PAY_BLOCK="" PAY_TS="" PAY_LOGS="" VAULT_TXS=""
    TX="-" BLOCK="-" TS="-" LOG="-" QUOTE="-"
    b0=$(block_number)
    t0=$(date +%s)
    if run_ant pay up_large --quote-timeout-secs 60 --store-timeout-secs 300 \
        --json file upload "${LARGE_FILE}" --visibility listed; then
        t1=$(date +%s)
        b1=$(block_number)
        if vars=$(py upload "${OUT_DIR}/up_large.out" 1 1 31 "${LARGE_FILE}" - - 64 \
            2> "${LARGE_PROBLEMS}"); then
            pass "${LARGE_LABEL}: upload result"
        else
            fail_with "${LARGE_LABEL}: upload result" "${LARGE_PROBLEMS}"
        fi
        eval "${vars}"
        ok=0
        if vars=$(py vault "${CAST}" "${RPC_URL}" "${VAULT_ADDRESS}" "${DPM_TOPIC}" "${MPM_TOPIC}" \
            "$((b0 + 1))" "${b1}" "${TOTAL_CHUNKS:-0}" large 2> "${LARGE_PROBLEMS}"); then
            ok=1
        fi
        eval "${vars}"
        if [ "${ok}" = 1 ]; then
            pass "${LARGE_LABEL}: ${TOTAL_CHUNKS} chunks paid per chunk, no batch (${VAULT_TXS} vault transactions)"
        else
            fail_with "${LARGE_LABEL}: payments" "${LARGE_PROBLEMS}"
        fi
        if [ -n "${RECORD}" ] && run_ant free verify_large --json file verify "${RECORD}"; then
            if vars=$(py verify "${OUT_DIR}/verify_large.out" "${ENVELOPE}" 1 1 31 0 - - - - \
                "${PAY_TX:--}" "${PAY_BLOCK:--}" "${PAY_TS:--}" "${PAY_LOGS:--}" \
                2> "${LARGE_PROBLEMS}"); then
                pass "${LARGE_LABEL}: record checks out against the per-chunk payment"
            else
                fail_with "${LARGE_LABEL}: record" "${LARGE_PROBLEMS}"
            fi
            eval "${vars}"
            # A data map this large is shrunk into a child map: no size.
            printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
                large "${ENVELOPE}" 1 1 31 - - - "${TX:--}" "${BLOCK:--}" "${TS:--}" "${LOG:--}" \
                "${QUOTE:--}" "${t0}" "${t1}" "${LARGE_FILE}" "${RECORD}" - >> "${EXPECTED_TSV}"
        else
            fail "${LARGE_LABEL}: ant file verify (record)" "$(last_error verify_large)"
        fi
        if [ -n "${ADDRESS}" ]; then
            check_download "${LARGE_LABEL}: download by address" dl_large "${LARGE_FILE}" "${ADDRESS}"
        fi
    else
        fail "${LARGE_LABEL}: upload" "$(last_error up_large)"
    fi
fi
echo ""

# Step 6: The index
echo "=== Step 6: ant listings ==="
INDEXED=$(awk -F'\t' 'NR > 1 && $4 == 1' "${EXPECTED_TSV}" | wc -l | tr -d ' ')
echo "  Waiting for the ${INDEXED} uploads in the index to be served (up to ${LISTINGS_WAIT_SECS} s)..."
DEADLINE=$(( $(date +%s) + LISTINGS_WAIT_SECS ))
while :; do
    if run_ant free listings_poll --json listings \
        && py listings-ready "${OUT_DIR}/listings_poll.out" "${EXPECTED_TSV}"; then
        echo "  All served."
        break
    fi
    if [ "$(date +%s)" -ge "${DEADLINE}" ]; then
        echo "  Not all served after ${LISTINGS_WAIT_SECS} s; checking what there is."
        break
    fi
    sleep 30
done
if run_ant free listings --json listings --all; then
    if vars=$(py listings "${OUT_DIR}/listings.out" "${EXPECTED_TSV}" "${NEAREST}" \
        2> "${OUT_DIR}/listings.problems"); then
        pass "ant listings: exactly the ${INDEXED} uploads in the index, in order, as chosen"
    else
        fail_with "ant listings" "${OUT_DIR}/listings.problems"
    fi
    SINCE=""
    if vars=$(py since-pick "${OUT_DIR}/listings.out"); then
        eval "${vars}"
    fi
    if [ -z "${SINCE}" ]; then
        fail "ant listings --since" "no listing to take a time from"
    elif ! run_ant free listings_since --json listings --since "${SINCE}"; then
        fail "ant listings --since ${SINCE}" "$(last_error listings_since)"
    elif py since "${OUT_DIR}/listings.out" "${OUT_DIR}/listings_since.out" "${SINCE}" \
        > /dev/null 2> "${OUT_DIR}/listings_since.problems"; then
        pass "ant listings --since ${SINCE}: the listings from then on, in order"
    else
        fail_with "ant listings --since ${SINCE}" "${OUT_DIR}/listings_since.problems"
    fi
else
    fail "ant listings" "$(last_error listings)"
fi
echo ""

# Step 7: One listing at a time, for the uploads outside the index
echo "=== Step 7: ant listing <address> ==="
while IFS=$'\t' read -r tag address readable index bits _rest <&3; do
    if [ "${index}" = 1 ]; then
        continue
    fi
    label="ant listing: $(case_label "${readable}" "${index}" "${bits}") (${tag})"
    if run_ant free "listing_${tag}" --json listing "${address}"; then
        if py listing "${OUT_DIR}/listing_${tag}.out" "${EXPECTED_TSV}" "${address}" "${NEAREST}" \
            > /dev/null 2> "${OUT_DIR}/listing_${tag}.problems"; then
            pass "${label}"
        else
            fail_with "${label}" "${OUT_DIR}/listing_${tag}.problems"
        fi
    else
        fail "${label}" "$(last_error "listing_${tag}")"
    fi
done 3< <(tail -n +2 "${EXPECTED_TSV}")
# A readable upload without an envelope, and an address with nothing at it,
# have no listing: `ant listing` fails.
n=0
while read -r address <&3; do
    n=$((n + 1))
    if run_ant free "no_listing_${n}" --json listing "${address}"; then
        fail "ant listing: a plain data map has no listing" "it found one for ${address}"
    elif py no-listing "${OUT_DIR}/no_listing_${n}.out" > /dev/null 2> "${OUT_DIR}/no_listing_${n}.problems"; then
        pass "ant listing: a plain data map has no listing"
    else
        fail_with "ant listing: a plain data map has no listing" "${OUT_DIR}/no_listing_${n}.problems"
    fi
done 3< "${PLAIN_ADDRESSES}"
RANDOM_ADDRESS=$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')
if run_ant free no_listing_random --json listing "${RANDOM_ADDRESS}"; then
    fail "ant listing: an empty address has no listing" "it found one"
else
    pass "ant listing: an empty address has no listing"
fi
echo ""

# Step 8: Proofs
#
# Every record gives a proof that reveals the file; a private one also a
# proof that doesn't. Representative uploads (all or nothing shown, or with
# metadata) are downloaded with their proofs.
echo "=== Step 8: ant file prove ==="
FIRST_RECORD=""
while IFS=$'\t' read -r tag address readable index bits name ctype _size _tx _block _ts _log _quote _t0 _t1 file record _datamap <&3; do
    private=$([ "${readable}" = 1 ] && echo 0 || echo 1)
    label="ant file prove: $(case_label "${readable}" "${index}" "${bits}") (${tag})"
    if run_ant free "prove_${tag}" --json file prove "${record}" -o "${PROOFS_DIR}/${tag}.proof" \
        && py prove "${OUT_DIR}/prove_${tag}.out" "${address}" "${private}" "${private}" \
            > /dev/null 2> "${OUT_DIR}/prove_${tag}.problems"; then
        pass "${label}"
    else
        fail "${label}" "$(last_error "prove_${tag}")$(cat "${OUT_DIR}/prove_${tag}.problems" 2>/dev/null)"
    fi
    if [ "${private}" = 1 ]; then
        if run_ant free "prove_nr_${tag}" --json file prove "${record}" --no-reveal \
                -o "${PROOFS_DIR}/${tag}.noreveal.proof" \
            && py prove "${OUT_DIR}/prove_nr_${tag}.out" "${address}" 1 0 \
                > /dev/null 2> "${OUT_DIR}/prove_nr_${tag}.problems"; then
            pass "${label} --no-reveal"
        else
            fail "${label} --no-reveal" "$(last_error "prove_nr_${tag}")"
        fi
    fi
    FIRST_RECORD="${FIRST_RECORD:-${record}}"
    if [ "${bits}" = 0 ] || [ "${bits}" = 31 ] || [ "${name}" != "-" ] || [ "${ctype}" != "-" ]; then
        if [ "${tag}" != large ]; then
            check_download "download with --proof: ${tag}" "dl_${tag}_proof" "${file}" \
                --proof "${PROOFS_DIR}/${tag}.proof"
        fi
        if [ "${private}" = 1 ]; then
            if run_ant free "dl_${tag}_noreveal" --json file download \
                --proof "${PROOFS_DIR}/${tag}.noreveal.proof" -o "${DL_DIR}/noreveal"; then
                fail "download with a proof that doesn't reveal: ${tag}" "it downloaded"
            elif grep -qF "gives no data map" "${OUT_DIR}/dl_${tag}_noreveal.err"; then
                pass "download with a proof that doesn't reveal: ${tag} (refused)"
            else
                fail "download with a proof that doesn't reveal: ${tag}" "$(last_error "dl_${tag}_noreveal")"
            fi
            rm -f "${DL_DIR}/noreveal"
        fi
    fi
done 3< <(tail -n +2 "${EXPECTED_TSV}")
if [ -n "${FIRST_RECORD}" ]; then
    RECORD_SHA=$(sha256_of "${FIRST_RECORD}")
    if run_ant free prove_onto_record --json file prove "${FIRST_RECORD}" -o "${FIRST_RECORD}" --force; then
        fail "ant file prove never replaces its record" "it wrote over ${FIRST_RECORD}"
    elif [ "$(sha256_of "${FIRST_RECORD}")" = "${RECORD_SHA}" ]; then
        pass "ant file prove never replaces its record, even with --force"
    else
        fail "ant file prove never replaces its record" "the record changed"
    fi
    if run_ant free prove_again --json file prove "${FIRST_RECORD}" -o "${PROOFS_DIR}/again.proof" \
        && ! run_ant free prove_exists --json file prove "${FIRST_RECORD}" -o "${PROOFS_DIR}/again.proof" \
        && run_ant free prove_force --json file prove "${FIRST_RECORD}" -o "${PROOFS_DIR}/again.proof" --force; then
        pass "ant file prove replaces an existing proof only with --force"
    else
        fail "ant file prove replaces an existing proof only with --force"
    fi
fi
echo ""

# Step 9: Proofs checked from the chain alone
#
# The nodes run inside the devnet process: killing it stops them all, while
# Anvil, a separate process, keeps the chain. `ant file verify` then gets a
# manifest without a single node.
echo "=== Step 9: ant file verify from the chain alone ==="
OLD_DEVNET_PID="${DEVNET_PID}"
stop_devnet KILL
if kill -0 "${OLD_DEVNET_PID}" 2>/dev/null; then
    fail "the nodes are stopped" "the devnet process is still running"
else
    pass "the nodes are stopped (devnet process ${OLD_DEVNET_PID} gone)"
fi
if "${CAST}" block-number --rpc-url "${RPC_URL}" >/dev/null 2>&1; then
    pass "Anvil still answers at ${RPC_URL}"
else
    fail "Anvil still answers" "no answer at ${RPC_URL}; the proofs can't be checked"
fi
if py chain-only "${MANIFEST_FILE}" "${CHAIN_ONLY_MANIFEST}" > /dev/null; then
    ANT_MANIFEST="${CHAIN_ONLY_MANIFEST}"
fi
OTHER_FILE=""
while IFS=$'\t' read -r tag address readable index bits name ctype size tx block ts log quote _t0 _t1 file _record _datamap <&3; do
    private=$([ "${readable}" = 1 ] && echo 0 || echo 1)
    label="ant file verify: $(case_label "${readable}" "${index}" "${bits}") (${tag})"
    proof="${PROOFS_DIR}/${tag}.proof"
    [ -f "${proof}" ] || continue
    # The proof, and that the local file is the one it is for.
    if run_ant free "vp_${tag}" --json file verify "${proof}" --file "${file}"; then
        if py verify "${OUT_DIR}/vp_${tag}.out" "${address}" "${readable}" "${index}" "${bits}" \
            "${private}" "${name}" "${ctype}" "${size}" true "${tx}" "${block}" "${ts}" "${log}:${quote#0x}" \
            > /dev/null 2> "${OUT_DIR}/vp_${tag}.problems"; then
            pass "${label}"
        else
            fail_with "${label}" "${OUT_DIR}/vp_${tag}.problems"
        fi
    else
        fail "${label}" "$(last_error "vp_${tag}")"
    fi
    if [ "${private}" = 1 ] && [ -f "${PROOFS_DIR}/${tag}.noreveal.proof" ]; then
        if run_ant free "vpn_${tag}" --json file verify "${PROOFS_DIR}/${tag}.noreveal.proof" \
            && py verify "${OUT_DIR}/vpn_${tag}.out" "${address}" "${readable}" "${index}" "${bits}" \
                0 "${name}" "${ctype}" "${size}" - "${tx}" "${block}" "${ts}" "${log}:${quote#0x}" \
                > /dev/null 2> "${OUT_DIR}/vpn_${tag}.problems"; then
            pass "${label} --no-reveal proof"
        else
            fail "${label} --no-reveal proof" "$(last_error "vpn_${tag}")$(head -3 "${OUT_DIR}/vpn_${tag}.problems" 2>/dev/null)"
        fi
        if run_ant free "vpnf_${tag}" --json file verify "${PROOFS_DIR}/${tag}.noreveal.proof" --file "${file}"; then
            fail "${label}: a proof that doesn't reveal can't be compared with a file" "it compared"
        else
            pass "${label}: a proof that doesn't reveal can't be compared with a file"
        fi
    fi
    if [ "${tag}" = large ]; then
        continue
    fi
    if [ "${bits}" = 0 ] || [ "${bits}" = 31 ] || [ "${name}" != "-" ] || [ "${ctype}" != "-" ]; then
        # Another file is not the one the proof is for.
        if [ -n "${OTHER_FILE}" ] && [ "${OTHER_FILE}" != "${file}" ]; then
            if run_ant free "vpo_${tag}" --json file verify "${proof}" --file "${OTHER_FILE}"; then
                fail "${label}: another file doesn't match" "it matched"
            elif grep -qF "is not the file this proof is for" "${OUT_DIR}/vpo_${tag}.err"; then
                pass "${label}: another file doesn't match"
            else
                fail "${label}: another file doesn't match" "$(last_error "vpo_${tag}")"
            fi
        fi
        OTHER_FILE="${file}"
        # Tampered proofs fail.
        for field in address envelope tx quote salt; do
            if [ "${field}" = salt ] && [ "${private}" = 0 ]; then
                continue
            fi
            tampered="${PROOFS_DIR}/${tag}.${field}.tampered"
            if ! py tamper "${proof}" "${tampered}" "${field}" 2> "${OUT_DIR}/tamper_${tag}_${field}.problems"; then
                fail_with "${label}: tamper with the ${field}" "${OUT_DIR}/tamper_${tag}_${field}.problems"
                continue
            fi
            if run_ant free "vpt_${tag}_${field}" --json file verify "${tampered}"; then
                fail "${label}: a proof with its ${field} changed fails" "it checked out"
            else
                pass "${label}: a proof with its ${field} changed fails ($(last_error "vpt_${tag}_${field}" | cut -c1-80))"
            fi
        done
    fi
done 3< <(tail -n +2 "${EXPECTED_TSV}")
echo ""

# Step 10: Summary
echo "=============================================="
echo "  E2E Test Results"
echo "=============================================="
echo "  Total: ${TOTAL_COUNT}"
echo "  Passed: ${PASS_COUNT}"
echo "  Failed: ${FAIL_COUNT}"
echo "=============================================="

if [ "${FAIL_COUNT}" -gt 0 ]; then
    echo ""
    echo "RESULT: FAILED (${FAIL_COUNT} failures)"
    exit 1
else
    echo ""
    echo "RESULT: ALL TESTS PASSED"
    exit 0
fi
