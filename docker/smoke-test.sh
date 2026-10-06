#!/usr/bin/env bash
# Tests the storage-node image on a small isolated network, never the live one:
# a seed node with no bootstrap peers, plus test containers that bootstrap only
# from it. Containers sharing the host network need their own MANAGER_PORT. Run by CI after building the image; needs Docker with host networking.
#
#   docker/smoke-test.sh <image>
#
# Checks: nodes start and join; browser addresses are published on the fixed
# ports; a restart keeps node identities and starts the nodes again; the
# previous image's /data/node-<port> layout is taken over with identities
# kept; lowering NODE_COUNT retires a node; changing PORT_START moves the
# nodes with identities kept; docker stop is clean and timely;
# the node manager's disk-pressure eviction removes nodes (never the last)
# when the disk is nearly full; and a data folder mounted noexec is refused.
set -euo pipefail

IMAGE=${1:?usage: smoke-test.sh <image>}
WORK=$(mktemp -d)
IP=$(hostname -I | awk '{print $1}')
WALLET=0x000000000000000000000000000000000000dEaD   # test nodes earn nothing
# Testnet mode: the image defaults to production mode, which counts at most 2
# nodes per IP, and every test node here runs on this one machine.
COMMON=(--network host --stop-timeout 120 -e ANT_NETWORK_MODE=testnet -e REWARDS_ADDRESS=$WALLET -e PUID=$(id -u) -e PGID=$(id -g))

fail() {
    echo "FAILED: $*"
    for c in seed smoke evict noexec; do
        docker ps -a --format '{{.Names}}' | grep -qx $c || continue
        echo "--- $c log (last 40 lines)"; docker logs $c 2>&1 | grep -vE '^\s*$' | tail -40
    done
    exit 1
}
cleanup() { docker rm -f seed smoke evict noexec >/dev/null 2>&1 || true; sudo rm -rf "$WORK"; }
trap cleanup EXIT

count() { docker logs "$1" 2>&1 | grep -cE "$2" || true; }
wait_for() { # container pattern [count] [seconds]
    for _ in $(seq 1 $(( ${4:-180} / 2 ))); do
        (( $(count "$1" "$2") >= ${3:-1} )) && return 0
        sleep 2
    done
    return 1
}
peer_ids() { sudo sh -c "cat $1/nodes/node-*/webrtc-direct.multiaddr" 2>/dev/null | sed 's#.*/p2p/##' | sort | tr '\n' ' '; }
registry() { sudo jq -r "$2" "$1/manager/ant/node_registry.json"; }

echo "== seed node (its only bootstrap peer is itself: starts an isolated network)"
docker run -d --name seed "${COMMON[@]}" -e MANAGER_PORT=12601 -e NODE_COUNT=1 -e PORT_START=10900 \
    -e PUBLIC_IP=$IP -e BOOTSTRAP_PEERS="$IP:10900" -v "$WORK/seed:/data" "$IMAGE" >/dev/null
wait_for seed 'Running 1 node' 1 60 || fail "seed did not start"
[[ "$(registry "$WORK/seed" '.nodes["1"].bootstrap_peers | length')" == 0 ]] || fail "the seed lists itself as a bootstrap peer"

echo "== three nodes joining the seed, browser ports 11910-11912"
run_smoke() { # node_count [port_start]
    docker run -d --name smoke "${COMMON[@]}" -e MANAGER_PORT=12602 -e NODE_COUNT=$1 -e PORT_START=${2:-10910} \
        -e BROWSER_PORT_START=11910 -e PUBLIC_IP=$IP -e BOOTSTRAP_PEERS="$IP:10900" \
        -v "$WORK/smoke:/data" "$IMAGE" >/dev/null
}
run_smoke 3
wait_for smoke 'Running 3 node' 1 60 || fail "smoke container did not start 3 nodes"
wait_for smoke '\[node-[0-9]+\].*Successfully connected to [1-9]' 3 || fail "nodes did not join the seed"
for i in 1 2 3; do
    sudo grep -qE "^/ip4/$IP/udp/1191$((i - 1))/webrtc-direct/certhash/u[A-Za-z0-9_-]+/p2p/[0-9a-f]{64}$" \
        "$WORK/smoke/nodes/node-$i/webrtc-direct.multiaddr" || fail "node-$i did not publish its browser address"
done
ids=$(peer_ids "$WORK/smoke")
echo "peer ids: $ids"

echo "== restart keeps identities and starts the nodes again"
docker restart -t 120 smoke >/dev/null
wait_for smoke 'Running 3 node' 2 60 || fail "nodes did not start after a restart"
[[ "$(peer_ids "$WORK/smoke")" == "$ids" ]] || fail "identities changed across a restart"

echo "== the previous image's layout is taken over with identities kept"
docker stop -t 120 smoke >/dev/null && docker rm smoke >/dev/null
for i in 1 2 3; do sudo mv "$WORK/smoke/nodes/node-$i" "$WORK/smoke/node-1091$((i - 1))"; done
sudo rm -rf "$WORK/smoke/manager" "$WORK/smoke/logs" "$WORK/smoke/nodes"
run_smoke 3
wait_for smoke 'Taking over node-10912 as node-3' 1 60 || fail "old layout not taken over"
wait_for smoke 'Running 3 node' 1 60 || fail "taken-over nodes did not start"
[[ "$(peer_ids "$WORK/smoke")" == "$ids" ]] || fail "identities changed when taking over the old layout"
[[ "$(registry "$WORK/smoke" '[.nodes[].node_port] | sort | join(",")')" == "10910,10911,10912" ]] || fail "ports changed"

echo "== lowering NODE_COUNT retires the node on the highest port"
docker stop -t 120 smoke >/dev/null && docker rm smoke >/dev/null
run_smoke 2
wait_for smoke 'Retiring node-3 \(port 10912\)' 1 60 || fail "node-3 was not retired"
wait_for smoke 'Running 2 node' 1 60 || fail "2 nodes did not start"
sudo sh -c "ls -d $WORK/smoke/retired/*-node-3" >/dev/null || fail "node-3's data was not kept in /data/retired"

echo "== changing PORT_START moves the nodes, identities kept"
docker stop -t 120 smoke >/dev/null && docker rm smoke >/dev/null
ids=$(peer_ids "$WORK/smoke")
run_smoke 2 10920
wait_for smoke 'Moving node-2 from UDP port 10911 to 10921' 1 60 || fail "nodes were not moved to the new ports"
wait_for smoke 'Running 2 node' 1 60 || fail "moved nodes did not start"
[[ "$(registry "$WORK/smoke" '[.nodes[].node_port] | sort | join(",")')" == "10920,10921" ]] || fail "ports not changed"
[[ "$(peer_ids "$WORK/smoke")" == "$ids" ]] || fail "identities changed when moving ports"
wait_for smoke '\[node-[0-9]+\].*Successfully connected to [1-9]' 2 || fail "moved nodes did not rejoin the seed"

echo "== docker stop stops the nodes cleanly"
start=$(date +%s); docker stop -t 120 smoke >/dev/null; took=$(( $(date +%s) - start ))
code=$(docker inspect -f '{{.State.ExitCode}}' smoke)
echo "stopped in ${took}s, exit code $code"
(( took < 100 )) || fail "stopping took ${took}s"
[[ "$code" == 0 ]] || fail "container exited with code $code"
(( $(count smoke 'Stopping nodes') >= 1 )) || fail "nodes were not stopped by the entrypoint"

echo "== disk-pressure eviction on a 1.5 GB disk"
docker run -d --name evict "${COMMON[@]}" -e MANAGER_PORT=12603 --tmpfs /data:rw,exec,size=1500m -e NODE_COUNT=3 -e PORT_START=10930 \
    -e BOOTSTRAP_PEERS="$IP:10900" "$IMAGE" >/dev/null
wait_for evict 'Running 3 node' 1 60 || fail "eviction test nodes did not start"
(( $(count evict 'WARNING: only [0-9]+ GiB free') >= 1 )) || fail "no low-disk warning at startup"
avail=$(docker exec evict df -B1 --output=avail /data | tail -n 1 | tr -d ' ')
docker exec evict fallocate -l $((avail - 300 * 1024 * 1024)) /data/filler
for _ in $(seq 1 60); do
    evicted=$(docker exec evict jq '[.nodes[] | select(.eviction != null)] | length' /data/manager/ant/node_registry.json)
    (( evicted >= 2 )) && break
    sleep 3
done
echo "evicted: $evicted of 3"
(( evicted == 2 )) || fail "expected 2 of 3 nodes evicted (never the last), got $evicted"
docker exec evict nodes status >/dev/null || fail "node manager not answering after evictions"

echo "== a data folder mounted noexec is refused; production mode warns above 2 nodes"
docker run -d --name noexec "${COMMON[@]}" -e MANAGER_PORT=12604 --tmpfs /data:rw,noexec,size=100m \
    -e ANT_NETWORK_MODE=production -e NODE_COUNT=3 -e PORT_START=10940 "$IMAGE" >/dev/null
wait_for noexec 'mounted noexec' 1 30 || fail "a noexec data folder was not refused"
(( $(count noexec 'WARNING: NODE_COUNT is 3') >= 1 )) || fail "no warning for 3 nodes in production mode"

echo "All smoke tests passed"
