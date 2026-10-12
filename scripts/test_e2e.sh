#!/usr/bin/env bash
#
# End-to-end integration test for file upload/download with EVM payments,
# with the `darwin` CLI from ../darwin-client against this repo's devnet.
#
# This script:
# 1. Builds ant-devnet (this repo) and the `darwin` CLI (../darwin-client)
# 2. Starts a devnet with EVM payment enforcement (Anvil + nodes)
# 3. Uploads each file in ./ugly_files/ with payment, downloads it and
#    compares checksums
# 4. Checks every upload was paid per chunk on-chain, with no merkle batch
# 5. Checks every payment transaction succeeded on Anvil
# 5b. Uploads a large file (well over 64 chunks): paid per chunk, no batch,
#    and it downloads intact. Checks `--merkle` is refused before paying.
# 5c. Uploads several files at once from this one host (one IP), as the
#    agent gateway does, each paid from its own wallet: all must complete. Reports any rate-limit refusals
#    the nodes logged.
# 6. Tests client-side payment rejection (upload without SECRET_KEY)
# 7. Tests chunk put rejection without a wallet
# 8. Stops the devnet and reports results
#
# Exit 0 if ALL tests pass, non-zero otherwise.
#
# Needs cargo, Foundry (anvil and cast on PATH), python3 and curl.
#
# Environment:
#   ANT_CLIENT_DIR               darwin-client checkout (default: ../darwin-client)
#   ANT_TEST_SKIP_BUILD=1        don't build; use ANT_DEVNET and ANT_CLI
#   ANT_DEVNET, ANT_CLI          binaries (default: the builds below)
#   ANT_TEST_CARGO_PROFILE       cargo profile to build (default: release)
#   ANT_TEST_DEVNET_NODES        node count (default: 16)
#   ANT_TEST_BOOTSTRAP_COUNT     bootstrap nodes (default: 3)
#   ANT_TEST_DEVNET_LOG_LEVEL    devnet log filter (default: warn, plus the
#                                storage handler's debug lines, which name
#                                rate-limit refusals)
#   ANT_TEST_IPV4_ONLY           1: start the devnet IPv4-only; 0: dual-stack;
#                                default: IPv4-only when this host can't bind
#                                an IPv6 loopback socket
#   ANT_TEST_STABILIZE_SECS      wait after the devnet starts (default: 15)
#   ANT_TEST_MAX_FILE_SIZE       largest ugly_files/ file uploaded (default 1 MB)
#   ANT_TEST_SKIP_LARGE=1        skip Step 5b's large upload
#                                (ANT_TEST_SKIP_MERKLE=1 is still accepted)
#   ANT_TEST_LARGE_FILE_MB       its size in MiB (default: 280, about 70 chunks;
#                                ANT_TEST_MERKLE_FILE_MB is still accepted)
#   ANT_TEST_CONCURRENT_UPLOADS  Step 5c's simultaneous uploads (default: 4;
#                                0 skips the step)
#   ANT_TEST_CONCURRENT_FILE_MB  size of each, in MiB (default: 48)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ANT_CLIENT_DIR="${ANT_CLIENT_DIR:-${PROJECT_DIR}/../darwin-client}"
UGLY_FILES_DIR="${PROJECT_DIR}/ugly_files"
WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/ant_e2e.XXXXXX")"
MANIFEST_FILE="${WORK_DIR}/manifest.json"
DOWNLOAD_DIR="${WORK_DIR}/downloads"
FILES_DIR="${WORK_DIR}/files"
OUT_DIR="${WORK_DIR}/out"
LOG_FILE="${WORK_DIR}/devnet.log"
CLIENT_HOME="${WORK_DIR}/client-home"

DEVNET_NODES="${ANT_TEST_DEVNET_NODES:-16}"
BOOTSTRAP_COUNT="${ANT_TEST_BOOTSTRAP_COUNT:-3}"
DEVNET_LOG_LEVEL="${ANT_TEST_DEVNET_LOG_LEVEL:-warn,ant_node::storage::handler=debug}"
STABILIZE_SECS="${ANT_TEST_STABILIZE_SECS:-15}"
MAX_FILE_SIZE="${ANT_TEST_MAX_FILE_SIZE:-1048576}"
SKIP_LARGE="${ANT_TEST_SKIP_LARGE:-${ANT_TEST_SKIP_MERKLE:-0}}"
LARGE_FILE_MB="${ANT_TEST_LARGE_FILE_MB:-${ANT_TEST_MERKLE_FILE_MB:-280}}"
CONCURRENT_UPLOADS="${ANT_TEST_CONCURRENT_UPLOADS:-4}"
CONCURRENT_FILE_MB="${ANT_TEST_CONCURRENT_FILE_MB:-48}"
CARGO_PROFILE="${ANT_TEST_CARGO_PROFILE:-release}"
PY="${PY:-python3}"
CAST="${CAST:-cast}"

DEVNET_PID=""
PASS_COUNT=0
FAIL_COUNT=0
TOTAL_COUNT=0

mkdir -p "${DOWNLOAD_DIR}" "${FILES_DIR}" "${OUT_DIR}" "${CLIENT_HOME}"

# Anvil's Windows process ID, found by the port it listens on. Git Bash on
# Windows can't deliver SIGINT to the devnet, a native process, so the devnet
# is killed and its Anvil (a separate process there) must be stopped too.
# Prints nothing elsewhere.
anvil_windows_pid() {
    local port
    command -v taskkill >/dev/null 2>&1 || return 0
    port=$(printf '%s' "${RPC_URL:-}" | sed -nE 's#^[a-z]+://[^/]*:([0-9]+).*#\1#p')
    [ -n "${port}" ] || return 0
    netstat -ano 2>/dev/null | tr -d '\r' \
        | awk -v suffix=":${port}" '$1 == "TCP" && $4 == "LISTENING" && substr($2, length($2) - length(suffix) + 1) == suffix { print $5; exit }'
}

# Stop the devnet: SIGINT (its Anvil stops with it), then SIGKILL after 20
# seconds, then Anvil by its port where it outlived the devnet.
stop_devnet() {
    local i anvil_pid
    if [ -n "${DEVNET_PID}" ] && kill -0 "${DEVNET_PID}" 2>/dev/null; then
        echo "Stopping devnet (PID ${DEVNET_PID})..."
        kill -INT "${DEVNET_PID}" 2>/dev/null || true
        for i in $(seq 1 20); do
            kill -0 "${DEVNET_PID}" 2>/dev/null || break
            sleep 1
        done
        if kill -0 "${DEVNET_PID}" 2>/dev/null; then
            kill -KILL "${DEVNET_PID}" 2>/dev/null || true
        fi
        wait "${DEVNET_PID}" 2>/dev/null || true
    fi
    DEVNET_PID=""
    # Until nothing listens on Anvil's port any more.
    for i in 1 2 3 4 5; do
        anvil_pid=$(anvil_windows_pid || true)
        [ -n "${anvil_pid}" ] || break
        taskkill //F //PID "${anvil_pid}" >/dev/null 2>&1 || true
        sleep 1
    done
}

cleanup() {
    echo ""
    echo "=== Cleaning up ==="
    stop_devnet
    if command -v pkill >/dev/null 2>&1; then
        pkill -P $$ 2>/dev/null || true
    fi
    if [ "${FAIL_COUNT}" -gt 0 ] || [ "${ANT_TEST_KEEP:-0}" = "1" ]; then
        rm -rf "${FILES_DIR}" "${DOWNLOAD_DIR}"
        echo "Work directory kept (logs, CLI output): ${WORK_DIR}"
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

# Random bytes: `random_file PATH MIB`.
random_file() {
    if ! dd if=/dev/urandom of="$1" bs=1048576 count="$2" 2>/dev/null; then
        dd if=/dev/urandom of="$1" bs=1m count="$2" 2>/dev/null
    fi
}

# A field of a JSON file: `json_field FILE KEY` (empty if absent).
json_field() {
    "${PY}" -c 'import json, sys
value = json.load(open(sys.argv[1])).get(sys.argv[2])
print("" if value is None else value)' "$1" "$2"
}

# Run the CLI against the devnet: `run_ant pay|free|KEY NAME ARGS...`, paying
# from the devnet's wallet, from none, or from the wallet with that private
# key. stdout goes to $OUT_DIR/NAME.out, stderr to NAME.err; returns its exit
# status.
# The client keeps its state under the work directory, away from the user's.
run_ant() {
    local mode="$1" name="$2"
    shift 2
    local key=""
    if [ "${mode}" = "pay" ]; then
        key="${WALLET_KEY}"
    elif [ "${mode}" != "free" ]; then
        key="${mode}"
    fi
    env -u SECRET_KEY \
        HOME="${CLIENT_HOME}" \
        XDG_DATA_HOME="${CLIENT_HOME}/.local/share" \
        XDG_CONFIG_HOME="${CLIENT_HOME}/.config" \
        ${key:+"SECRET_KEY=${key}"} \
        "${ANT_CLI}" \
        --devnet-manifest "${MANIFEST_FILE}" \
        --evm-network local \
        --allow-loopback \
        --ipv4-only \
        "$@" \
        > "${OUT_DIR}/${name}.out" 2> "${OUT_DIR}/${name}.err"
}

block_number() {
    "${CAST}" block-number --rpc-url "${RPC_URL}"
}

# The vault's payment logs in blocks FIRST..LAST: prints `PER_CHUNK BATCHES
# TX_HASHES` (per-chunk payments, merkle batch payments, the comma-separated
# transactions that logged them).
vault_payments() {
    local first="$1" last="$2"
    if [ "${first}" -gt "${last}" ]; then
        echo "0 0 -"
        return 0
    fi
    "${CAST}" logs --json --from-block "${first}" --to-block "${last}" \
        --address "${VAULT_ADDRESS}" --rpc-url "${RPC_URL}" \
        | "${PY}" -c 'import json, sys
dpm, mpm = sys.argv[1].lower(), sys.argv[2].lower()
logs = json.load(sys.stdin)
if isinstance(logs, dict):  # newer cast wraps output: {"success", "data", "errors"}
    if not logs.get("success", True):
        sys.exit(f"cast logs: {logs.get('errors')}")
    logs = logs.get("data") or []
topic = lambda log: (log.get("topics") or [""])[0].lower()
per_chunk = [log for log in logs if topic(log) == dpm]
batches = [log for log in logs if topic(log) == mpm]
txs = sorted({log["transactionHash"] for log in per_chunk + batches})
print(len(per_chunk), len(batches), ",".join(txs) or "-")' "${DPM_TOPIC}" "${MPM_TOPIC}"
}

# Upload a file, download it and compare: `upload_and_check LABEL NAME FILE
# MIN_CHUNKS [GLOBAL FLAGS...]` (flags before the subcommand, such as
# --quote-timeout-secs). Sets UPLOAD_SECS and UPLOAD_CHUNKS.
upload_and_check() {
    local label="$1" name="$2" file="$3" min_chunks="$4"
    shift 4
    local b0 b1 address datamap failed stored total payments
    UPLOAD_SECS="" UPLOAD_CHUNKS=""
    b0=$(block_number)
    if ! run_ant pay "up_${name}" "$@" --json file upload "${file}"; then
        fail "${label} upload" "$(last_error "up_${name}")"
        return 0
    fi
    b1=$(block_number)
    total=$(json_field "${OUT_DIR}/up_${name}.out" total_chunks)
    stored=$(json_field "${OUT_DIR}/up_${name}.out" chunks_stored)
    failed=$(json_field "${OUT_DIR}/up_${name}.out" chunks_failed)
    address=$(json_field "${OUT_DIR}/up_${name}.out" address)
    datamap=$(json_field "${OUT_DIR}/up_${name}.out" datamap)
    UPLOAD_SECS=$(json_field "${OUT_DIR}/up_${name}.out" elapsed_secs)
    UPLOAD_CHUNKS="${total}"
    if [ "${failed}" != "0" ] || [ "${stored}" != "${total}" ] || [ "${total}" -lt "${min_chunks}" ]; then
        fail "${label} upload" "${stored} of ${total} chunks stored, ${failed} failed (expected at least ${min_chunks})"
        return 0
    fi
    pass "${label} upload (${total} chunks in ${UPLOAD_SECS%.*} s)"

    # Paid per chunk on-chain, no merkle batch.
    read -r payments batches txs <<< "$(vault_payments "$((b0 + 1))" "${b1}")"
    if [ "${batches}" != "0" ]; then
        fail "${label} payment" "${batches} merkle batch payment(s) on-chain"
    elif [ "${payments}" -lt "${total}" ]; then
        fail "${label} payment" "${payments} per-chunk payments for ${total} chunks"
    else
        pass "${label} payment (${payments} per-chunk payments, no batch)"
        if [ "${txs}" != "-" ]; then
            ALL_TX_HASHES="${ALL_TX_HASHES:+${ALL_TX_HASHES},}${txs}"
        fi
    fi

    local out="${DOWNLOAD_DIR}/${name}"
    if [ -n "${address}" ]; then
        run_ant free "dl_${name}" --json file download "${address}" -o "${out}" || true
    elif [ -n "${datamap}" ]; then
        run_ant free "dl_${name}" --json file download --datamap "${datamap}" -o "${out}" || true
    else
        fail "${label} download" "the upload gave neither an address nor a data map"
        return 0
    fi
    if [ ! -f "${out}" ]; then
        fail "${label} download" "$(last_error "dl_${name}")"
    elif [ "$(sha256_of "${out}")" = "$(sha256_of "${file}")" ]; then
        pass "${label} integrity (SHA256 match)"
    else
        fail "${label} integrity" "SHA256 mismatch"
    fi
    rm -f "${out}"
    return 0
}

echo "=============================================="
echo "  ant-node E2E Integration Test"
echo "=============================================="
echo "Work directory: ${WORK_DIR}"
echo ""

command -v "${PY}" >/dev/null 2>&1 || { echo "ERROR: ${PY} not found (set PY)"; exit 1; }
command -v "${CAST}" >/dev/null 2>&1 || { echo "ERROR: cast (Foundry) not found (set CAST)"; exit 1; }
command -v anvil >/dev/null 2>&1 || { echo "ERROR: anvil (Foundry) not found on PATH"; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "ERROR: curl not found"; exit 1; }

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

# Step 1: Build
echo "=== Step 1: Building ant-devnet and darwin ==="
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
    if [ ! -x "${binary}" ] && [ ! -x "${binary}.exe" ]; then
        echo "ERROR: ${binary} not found"
        exit 1
    fi
done
echo "ant-devnet: ${ANT_DEVNET}"
echo "darwin:     ${ANT_CLI}"
echo ""

# Step 2: Start devnet with EVM
echo "=== Step 2: Starting devnet with EVM (${DEVNET_NODES} nodes, ${BOOTSTRAP_COUNT} bootstrap) ==="
"${ANT_DEVNET}" \
    --nodes "${DEVNET_NODES}" \
    --bootstrap-count "${BOOTSTRAP_COUNT}" \
    --enable-evm \
    --manifest "${MANIFEST_FILE}" \
    --data-dir "${WORK_DIR}/devnet" \
    --stabilization-timeout-secs 120 \
    --enable-logging \
    --log-level "${DEVNET_LOG_LEVEL}" \
    ${DEVNET_IP_FLAGS[@]+"${DEVNET_IP_FLAGS[@]}"} \
    > "${LOG_FILE}" 2>&1 &
DEVNET_PID=$!

echo "Devnet starting (PID ${DEVNET_PID}), waiting for manifest..."

# Wait for manifest file to appear (max 180 seconds)
WAIT_COUNT=0
MAX_WAIT=180
while [ ! -f "${MANIFEST_FILE}" ] && [ ${WAIT_COUNT} -lt ${MAX_WAIT} ]; do
    if ! kill -0 "${DEVNET_PID}" 2>/dev/null; then
        echo "ERROR: Devnet process died before producing manifest."
        echo "Log output:"
        tail -50 "${LOG_FILE}" 2>/dev/null || true
        if [ "${IPV4_ONLY}" != "1" ] && grep -qiE "dual-stack|Failed to create transport|Address family not supported" "${LOG_FILE}" 2>/dev/null; then
            echo "This host may have no IPv6: rerun with ANT_TEST_IPV4_ONLY=1."
        fi
        exit 1
    fi
    sleep 1
    WAIT_COUNT=$((WAIT_COUNT + 1))
done

if [ ! -f "${MANIFEST_FILE}" ]; then
    echo "ERROR: Manifest not created after ${MAX_WAIT} seconds."
    echo "Log tail:"
    tail -30 "${LOG_FILE}" 2>/dev/null || true
    exit 1
fi
# The manifest is written in one go, but give the writer a moment.
sleep 1
echo "Manifest created at ${MANIFEST_FILE}"

# Extract EVM info from manifest
read -r WALLET_KEY RPC_URL VAULT_ADDRESS <<< "$("${PY}" -c 'import json, sys
evm = json.load(open(sys.argv[1])).get("evm") or {}
print(evm.get("wallet_private_key") or "-", evm.get("rpc_url") or "-",
      evm.get("payment_vault_address") or "-")' "${MANIFEST_FILE}" 2>/dev/null || echo "- - -")"

if [ "${WALLET_KEY}" = "-" ] || [ "${RPC_URL}" = "-" ] || [ "${VAULT_ADDRESS}" = "-" ]; then
    echo "ERROR: Could not extract EVM info from manifest."
    cat "${MANIFEST_FILE}"
    exit 1
fi

echo "Wallet key: ${WALLET_KEY:0:10}..."
echo "Anvil RPC: ${RPC_URL}"
echo "Vault: ${VAULT_ADDRESS}"
echo ""

# Verify Anvil is responding
if block_number >/dev/null 2>&1; then
    echo "Anvil RPC confirmed working"
else
    echo "ERROR: Anvil RPC not responding at ${RPC_URL}"
    exit 1
fi
DPM_TOPIC=$("${CAST}" keccak "DataPaymentMade(address,uint256,bytes32)")
MPM_TOPIC=$("${CAST}" keccak "MerklePaymentMade(bytes32,uint8,uint256,uint64)")

# Wait for network to stabilize
echo "Waiting ${STABILIZE_SECS} seconds for network stabilization..."
sleep "${STABILIZE_SECS}"
echo ""

# Accumulate TX hashes from all uploads for on-chain verification in Step 5
ALL_TX_HASHES=""

# Step 3 & 4: Upload and download each file in ugly_files/
echo "=== Step 3: File upload/download tests ==="

# Find test files (skip directories, .DS_Store, and files larger than MAX_FILE_SIZE)
TEST_FILES=()

if [ -d "${UGLY_FILES_DIR}" ]; then
    while IFS= read -r -d '' file; do
        fsize=$(wc -c < "${file}" | tr -d ' ')
        if [ "${fsize}" -le "${MAX_FILE_SIZE}" ] && [ "${fsize}" -ge 3 ]; then
            TEST_FILES+=("${file}")
        else
            echo "Skipping ${file} (${fsize} bytes: outside 3..${MAX_FILE_SIZE})"
        fi
    done < <(find "${UGLY_FILES_DIR}" -maxdepth 1 -type f ! -name '.DS_Store' -print0 2>/dev/null | sort -z)
fi

# If no ugly_files found, create a synthetic test file
if [ ${#TEST_FILES[@]} -eq 0 ]; then
    echo "No test files in ${UGLY_FILES_DIR}, creating synthetic test file..."
    SYNTHETIC_FILE="${FILES_DIR}/synthetic.txt"
    echo "ant E2E test payload: $(date -u +%Y-%m-%dT%H:%M:%SZ) run=$$" > "${SYNTHETIC_FILE}"
    TEST_FILES+=("${SYNTHETIC_FILE}")
fi

n=0
for filepath in "${TEST_FILES[@]}"; do
    n=$((n + 1))
    filename=$(basename "${filepath}")
    filesize=$(wc -c < "${filepath}" | tr -d ' ')
    echo ""
    echo "--- Testing file: ${filename} (${filesize} bytes) ---"
    # A copy, so the data map the upload saves next to it stays in the work
    # directory.
    cp "${filepath}" "${FILES_DIR}/small_${n}.bin"
    upload_and_check "${filename}" "small_${n}" "${FILES_DIR}/small_${n}.bin" 1
done

echo ""

# Step 5: On-chain payment verification (every payment transaction succeeded)
echo "=== Step 5: On-chain payment verification ==="

if [ -z "${ALL_TX_HASHES}" ]; then
    fail "On-chain payment verification" "No TX hashes collected from uploads"
else
    VERIFIED_TX=0
    FAILED_TX=0
    IFS=',' read -ra TX_ARRAY <<< "${ALL_TX_HASHES}"
    TOTAL_TX=${#TX_ARRAY[@]}
    echo "  Verifying ${TOTAL_TX} transaction hash(es) on Anvil..."

    for tx_hash in "${TX_ARRAY[@]}"; do
        status=$("${CAST}" receipt "${tx_hash}" status --rpc-url "${RPC_URL}" 2>/dev/null | awk '{print $1}' || true)
        # `1 (success)` from older cast, `true` from newer.
        if [ "${status}" = "1" ] || [ "${status}" = "true" ]; then
            VERIFIED_TX=$((VERIFIED_TX + 1))
        else
            FAILED_TX=$((FAILED_TX + 1))
            echo "  TX failed or not found on chain: ${tx_hash}"
        fi
    done

    if [ "${VERIFIED_TX}" -gt 0 ] && [ "${FAILED_TX}" -eq 0 ]; then
        pass "On-chain payment verification (${VERIFIED_TX}/${TOTAL_TX} TXs succeeded on Anvil)"
    else
        fail "On-chain payment verification" "${FAILED_TX}/${TOTAL_TX} TXs failed or not found on Anvil"
    fi
fi

echo ""

# Step 5b: A large upload, and merkle batch payments refused
#
# The nodes refuse merkle (batch) payments, and the client pays per chunk,
# in transactions of up to 64 chunks. A file well over the old batch
# threshold (64 chunks) must upload, paid per chunk with no batch on-chain,
# and download intact. Its quotes and PUTs also run into the nodes' limits
# on unpaid requests if those are too tight for a real upload.
#
# `--merkle` is refused by the client before it asks any node or pays; the
# nodes' own refusal of batch quotes and proofs is covered by the Rust e2e
# tests (tests/e2e/merkle_payment.rs), since the CLI can no longer ask.
echo "=== Step 5b: Large upload, per-chunk payments ==="

if [ "${SKIP_LARGE}" = "1" ]; then
    echo "  Skipping (ANT_TEST_SKIP_LARGE=1)"
else
    LARGE_FILE="${FILES_DIR}/large.bin"
    echo "  Generating ${LARGE_FILE_MB} MiB of random data..."
    random_file "${LARGE_FILE}" "${LARGE_FILE_MB}"
    upload_and_check "large file (${LARGE_FILE_MB} MiB)" large "${LARGE_FILE}" 65 \
        --quote-timeout-secs 60 --store-timeout-secs 300

    b0=$(block_number)
    if run_ant pay merkle --json file upload --merkle "${LARGE_FILE}"; then
        fail "--merkle refused" "a --merkle upload succeeded"
    else
        b1=$(block_number)
        read -r payments batches _txs <<< "$(vault_payments "$((b0 + 1))" "${b1}")"
        if [ "${payments}" = "0" ] && [ "${batches}" = "0" ]; then
            pass "--merkle refused before anything was paid"
        else
            fail "--merkle refused" "${payments} per-chunk and ${batches} batch payment(s) on-chain"
        fi
    fi
    rm -f "${LARGE_FILE}" "${LARGE_FILE}".datamap*
fi

echo ""

# Step 5c: Several uploads at once from one host
#
# The agent gateway uploads for every agent from one IP (and one client).
# The nodes' per-IP limits on quote requests and unpaid PUTs must not stop
# simultaneous paid uploads from one address.
echo "=== Step 5c: ${CONCURRENT_UPLOADS} simultaneous uploads from one host ==="

if [ "${CONCURRENT_UPLOADS}" -lt 1 ]; then
    echo "  Skipping (ANT_TEST_CONCURRENT_UPLOADS=0)"
else
    # Each upload pays from its own wallet, as the gateway's agents do: one
    # wallet paying from several processes at once races itself for nonces.
    # Every wallet gets gas and a share of the devnet wallet's tokens.
    echo "  Funding ${CONCURRENT_UPLOADS} wallets and generating ${CONCURRENT_FILE_MB} MiB files..."
    TOKEN_ADDRESS=$("${PY}" -c 'import json, sys
print((json.load(open(sys.argv[1])).get("evm") or {}).get("payment_token_address") or "")' "${MANIFEST_FILE}")
    FUNDER=$("${CAST}" wallet address --private-key "${WALLET_KEY}")
    TOKEN_SHARE=$("${PY}" -c 'import sys; print(int(sys.argv[1].split()[0]) // (int(sys.argv[2]) + 1))' \
        "$("${CAST}" call "${TOKEN_ADDRESS}" "balanceOf(address)(uint256)" "${FUNDER}" --rpc-url "${RPC_URL}")" \
        "${CONCURRENT_UPLOADS}")
    KEYS=()
    for i in $(seq 1 "${CONCURRENT_UPLOADS}"); do
        key="0x$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')"
        address=$("${CAST}" wallet address --private-key "${key}")
        "${CAST}" send "${address}" --value 1ether --private-key "${WALLET_KEY}" \
            --rpc-url "${RPC_URL}" > /dev/null
        "${CAST}" send "${TOKEN_ADDRESS}" "transfer(address,uint256)" "${address}" "${TOKEN_SHARE}" \
            --private-key "${WALLET_KEY}" --rpc-url "${RPC_URL}" > /dev/null
        KEYS+=("${key}")
        random_file "${FILES_DIR}/concurrent_${i}.bin" "${CONCURRENT_FILE_MB}"
    done
    b0=$(block_number)
    STARTED=$(date +%s)
    PIDS=()
    for i in $(seq 1 "${CONCURRENT_UPLOADS}"); do
        run_ant "${KEYS[$((i - 1))]}" "up_concurrent_${i}" --quote-timeout-secs 60 \
            --store-timeout-secs 300 --json file upload "${FILES_DIR}/concurrent_${i}.bin" &
        PIDS+=($!)
    done
    for i in $(seq 1 "${CONCURRENT_UPLOADS}"); do
        if wait "${PIDS[$((i - 1))]}"; then
            total=$(json_field "${OUT_DIR}/up_concurrent_${i}.out" total_chunks)
            stored=$(json_field "${OUT_DIR}/up_concurrent_${i}.out" chunks_stored)
            secs=$(json_field "${OUT_DIR}/up_concurrent_${i}.out" elapsed_secs)
            if [ "${stored}" = "${total}" ]; then
                pass "simultaneous upload ${i} (${total} chunks in ${secs%.*} s)"
            else
                fail "simultaneous upload ${i}" "${stored} of ${total} chunks stored"
            fi
        else
            fail "simultaneous upload ${i}" "$(last_error "up_concurrent_${i}")"
        fi
    done
    echo "  All ${CONCURRENT_UPLOADS} finished in $(( $(date +%s) - STARTED )) s."
    b1=$(block_number)
    read -r payments batches _txs <<< "$(vault_payments "$((b0 + 1))" "${b1}")"
    if [ "${batches}" = "0" ]; then
        pass "simultaneous uploads paid per chunk (${payments} payments, no batch)"
    else
        fail "simultaneous uploads paid per chunk" "${batches} merkle batch payment(s) on-chain"
    fi
    for i in $(seq 1 "${CONCURRENT_UPLOADS}"); do
        datamap=$(json_field "${OUT_DIR}/up_concurrent_${i}.out" datamap 2>/dev/null || true)
        address=$(json_field "${OUT_DIR}/up_concurrent_${i}.out" address 2>/dev/null || true)
        out="${DOWNLOAD_DIR}/concurrent_${i}"
        if [ -n "${address}" ]; then
            run_ant free "dl_concurrent_${i}" file download "${address}" -o "${out}" || true
        elif [ -n "${datamap}" ]; then
            run_ant free "dl_concurrent_${i}" file download --datamap "${datamap}" -o "${out}" || true
        else
            continue
        fi
        if [ -f "${out}" ] && [ "$(sha256_of "${out}")" = "$(sha256_of "${FILES_DIR}/concurrent_${i}.bin")" ]; then
            pass "simultaneous upload ${i} integrity (SHA256 match)"
        else
            fail "simultaneous upload ${i} integrity" "$(last_error "dl_concurrent_${i}")"
        fi
        rm -f "${out}"
    done
    rm -f "${FILES_DIR}"/concurrent_*
fi

# Requests the nodes refused for their per-peer and per-IP limits. Legitimate
# uploads should meet none; they are logged at debug level by the storage
# handler, so this count needs ANT_TEST_DEVNET_LOG_LEVEL to include that.
REFUSALS=$(grep -c "over its quote rate\|too many unpaid PUTs" "${LOG_FILE}" 2>/dev/null || true)
echo "  Rate-limit refusals in the node logs: ${REFUSALS:-0} (quotes: $(grep -c "over its quote rate" "${LOG_FILE}" 2>/dev/null || true), PUTs: $(grep -c "too many unpaid PUTs" "${LOG_FILE}" 2>/dev/null || true))"

echo ""

# Step 6: Test client-side payment rejection (upload without SECRET_KEY)
echo "=== Step 6: Client-side payment rejection test ==="

REJECTION_FILE=""
for filepath in "${TEST_FILES[@]}"; do
    filesize=$(wc -c < "${filepath}" | tr -d ' ')
    if [ "${filesize}" -lt 1000000 ]; then
        REJECTION_FILE="${filepath}"
        break
    fi
done

if [ -n "${REJECTION_FILE}" ]; then
    echo "  Attempting upload WITHOUT SECRET_KEY (should fail at client)..."
    cp "${REJECTION_FILE}" "${FILES_DIR}/rejection.bin"
    run_ant free reject_upload file upload "${FILES_DIR}/rejection.bin" || true
    if strip_ansi < "${OUT_DIR}/reject_upload.err" | grep -qi "SECRET_KEY"; then
        pass "Client-side payment rejection (SECRET_KEY required)"
    else
        fail "Client-side payment rejection" "Expected SECRET_KEY error from client"
        echo "  Output: $(last_error reject_upload)"
    fi
else
    echo "  WARNING: No test files available for rejection test"
fi

echo ""

# Step 7: Test chunk put rejection without wallet
echo "=== Step 7: Chunk put rejection without wallet ==="
echo "  Attempting chunk put WITHOUT SECRET_KEY (should fail at client)..."
echo "test data for rejection e2e" > "${FILES_DIR}/rejection_chunk.txt"
run_ant free reject_chunk chunk put "${FILES_DIR}/rejection_chunk.txt" || true

if strip_ansi < "${OUT_DIR}/reject_chunk.err" | grep -qi "SECRET_KEY\|wallet\|payment"; then
    pass "Chunk put rejection without wallet"
else
    fail "Chunk put rejection" "Expected wallet/payment error"
    echo "  Output: $(last_error reject_chunk)"
fi

echo ""

# Step 8: Summary
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
