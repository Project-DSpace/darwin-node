#!/usr/bin/env bash
# Tests the storage-node image on a small isolated network, never the live one:
# a seed node with no bootstrap peers, plus test containers that bootstrap only
# from it. Containers sharing the host network need their own MANAGER_PORT. Run by CI after building the image; needs Docker with host networking.
#
#   docker/smoke-test.sh <image>
#
# Checks: the image is set for the Darwin network (DARWIN's token and vault,
# Robinhood Chain's RPC, the bootstrap servers, production mode, IPv4 only)
# and its node's bootstrap file names no peers; nodes start and join, and
# each names DARWIN's token and vault and only its RPC's host when it starts;
# browser addresses are published on the fixed
# ports; a restart keeps node identities and starts the nodes again; the
# previous image's /data/node-<port> layout is taken over with identities
# kept; lowering NODE_COUNT retires a node; retired data is deleted at a start
# 4 or more days after its retirement, however long it sat unchanged before;
# changing PORT_START moves the
# nodes with identities kept; docker stop is clean and timely;
# the node manager's disk-pressure eviction removes nodes (never the last)
# when the disk is nearly full, and `nodes clear-evicted` forgets them; a data
# folder mounted noexec is refused;
# production mode warns above 2 nodes; the old PublicNode RPC_URL is pointed
# out; an RPC_URL of only spaces leaves the image's RPCs in place;
# ANT_RELAY_SERVING=false reaches every node and switches its relay server
# off, while nodes without it keep serving; and an ANT_RELAY_SERVING other
# than true or false is refused, an empty one ignored.
set -euo pipefail

IMAGE=${1:?usage: smoke-test.sh <image>}
WORK=$(mktemp -d)
IP=$(hostname -I | awk '{print $1}')
WALLET=0x000000000000000000000000000000000000dEaD   # test nodes earn nothing
# Testnet mode: the image defaults to production mode, whose routing tables
# take at most 2 nodes from one IP into each bucket, and every test node here
# runs on this one machine.
COMMON=(--network host --stop-timeout 120 -e ANT_NETWORK_MODE=testnet -e REWARDS_ADDRESS=$WALLET -e PUID=$(id -u) -e PGID=$(id -g))

fail() {
    echo "FAILED: $*"
    for c in seed smoke evict noexec rpc relay; do
        docker ps -a --format '{{.Names}}' | grep -qx $c || continue
        echo "--- $c log (last 40 lines)"; docker logs $c 2>&1 | grep -vE '^\s*$' | tail -40
    done
    exit 1
}
cleanup() { docker rm -f seed smoke evict noexec rpc relay >/dev/null 2>&1 || true; sudo rm -rf "$WORK"; }
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

# The Darwin network the image is built for (tests/darwin_network.rs checks
# that docker/Dockerfile sets the same).
TOKEN=0x511f76a3de41f000ec11ea4fd212fb3587c6cef3
VAULT=0xe0f627986c0438de0e314b644f3c8aa21d3f4f6b
SERVERS="142.93.32.90:10000 165.22.193.90:10000 142.93.96.232:10000"
# The line each node logs as it starts: the token and vault it is paid
# through, and its RPC's host alone, since an RPC URL can carry an API key.
# Addresses are checksummed there, so compared ignoring case.
PAYMENTS="\[node-[0-9]+\].*Payments: token $TOKEN, vault $VAULT, RPC host [a-z0-9.-]+([^a-z0-9.:/?@-]|\$)"
logs_payments() { # container count
    for _ in $(seq 1 30); do
        (( $(docker logs "$1" 2>&1 | grep -ciE "$PAYMENTS" || true) >= $2 )) && return 0
        sleep 2
    done
    return 1
}

echo "== the image's settings: the Darwin network, and no bootstrap peers in the node's file"
image_env=$(docker image inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$IMAGE")
for setting in "ANT_EVM_PAYMENT_TOKEN=$TOKEN" "ANT_EVM_PAYMENT_VAULT=$VAULT" "BOOTSTRAP_PEERS=$SERVERS" \
    ANT_NETWORK_MODE=production IPV4_ONLY=true; do
    grep -qixF "$setting" <<<"$image_env" || fail "the image does not set $setting"
done
grep -qE '^RPC_URLS=(.* )?https://rpc\.mainnet\.chain\.robinhood\.com( |$)' <<<"$image_env" \
    || fail "the image's RPC_URLS don't include Robinhood Chain's RPC"
peers=$(docker run --rm --entrypoint cat "$IMAGE" /opt/ant/bootstrap_peers.toml | grep -vE '^[[:space:]]*(#|$)' || true)
[[ "$peers" == "peers = []" ]] || fail "/opt/ant/bootstrap_peers.toml names peers: $peers"

echo "== seed node (its only bootstrap peer is itself: starts an isolated network)"
docker run -d --name seed "${COMMON[@]}" -e MANAGER_PORT=12601 -e NODE_COUNT=1 -e PORT_START=10900 \
    -e PUBLIC_IP=$IP -e BOOTSTRAP_PEERS="$IP:10900" -v "$WORK/seed:/data" "$IMAGE" >/dev/null
wait_for seed 'Running 1 node' 1 60 || fail "seed did not start"
[[ "$(registry "$WORK/seed" '.nodes["1"].bootstrap_peers | length')" == 0 ]] || fail "the seed lists itself as a bootstrap peer"
logs_payments seed 1 || fail "the seed's node did not name DARWIN's token and vault and its RPC's host alone"

echo "== three nodes joining the seed, browser ports 11910-11912, relay serving off"
run_smoke() { # node_count [port_start]
    docker run -d --name smoke "${COMMON[@]}" -e MANAGER_PORT=12602 -e NODE_COUNT=$1 -e PORT_START=${2:-10910} \
        -e BROWSER_PORT_START=11910 -e PUBLIC_IP=$IP -e BOOTSTRAP_PEERS="$IP:10900" -e ANT_RELAY_SERVING=false \
        -v "$WORK/smoke:/data" "$IMAGE" >/dev/null
}
run_smoke 3
wait_for smoke 'Running 3 node' 1 60 || fail "smoke container did not start 3 nodes"
wait_for smoke '\[node-[0-9]+\].*Successfully connected to [1-9]' 3 || fail "nodes did not join the seed"
logs_payments smoke 3 || fail "not every node named DARWIN's token and vault and its RPC's host alone"
# The node's own line, then saorsa-transport's from the relay server itself.
wait_for smoke '\[node-[0-9]+\].*Relay serving is off' 3 || fail "ANT_RELAY_SERVING=false did not reach all 3 nodes"
wait_for smoke '\[node-[0-9]+\].*Relay serving disabled' 3 || fail "the nodes' relay servers were not switched off"
(( $(count seed 'Relay serving (is off|disabled)') == 0 )) || fail "relay serving is off on the seed, which doesn't set ANT_RELAY_SERVING"
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

echo "== lowering NODE_COUNT retires the node on the highest port; retired data is kept 3 days"
docker stop -t 120 smoke >/dev/null && docker rm smoke >/dev/null
# Retired folders from earlier starts: one retired 5 days ago goes at this
# start, one retired 84 hours ago stays.
sudo mkdir "$WORK/smoke/retired/old-node-8" "$WORK/smoke/retired/recent-node-9"
sudo touch -d '5 days ago' "$WORK/smoke/retired/old-node-8"
sudo touch -d '84 hours ago' "$WORK/smoke/retired/recent-node-9"
# node-3's folder, unchanged for 5 days, keeps its 3 days once retired. Its
# process file is gone first, so nothing before the move changes the folder.
sudo rm -f "$WORK/smoke/nodes/node-3/node.pid"
sudo touch -d '5 days ago' "$WORK/smoke/nodes/node-3"
run_smoke 2
wait_for smoke 'Retiring node-3 \(port 10912\)' 1 60 || fail "node-3 was not retired"
wait_for smoke 'Running 2 node' 1 60 || fail "2 nodes did not start"
sudo sh -c "ls -d $WORK/smoke/retired/*-node-3" >/dev/null || fail "node-3's data was not kept in /data/retired"
sudo test ! -e "$WORK/smoke/retired/old-node-8" || fail "data retired 5 days ago was not deleted at start"
sudo test -d "$WORK/smoke/retired/recent-node-9" || fail "data retired 84 hours ago was deleted at start"

echo "== changing PORT_START moves the nodes, identities kept"
docker stop -t 120 smoke >/dev/null && docker rm smoke >/dev/null
ids=$(peer_ids "$WORK/smoke")
run_smoke 2 10920
wait_for smoke 'Moving node-2 from UDP port 10911 to 10921' 1 60 || fail "nodes were not moved to the new ports"
wait_for smoke 'Running 2 node' 1 60 || fail "moved nodes did not start"
# The first start after node-3 was retired: its folder, unchanged for 5 days
# before that, is still within its 3 days.
sudo sh -c "ls -d $WORK/smoke/retired/*-node-3" >/dev/null \
    || fail "node-3's data, retired at the last start, was deleted at this one"
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

echo "== nodes clear-evicted forgets the evicted nodes and keeps the last"
# This container's /data is a tmpfs, so it is not restarted to see them replaced.
cleared=$(docker exec evict nodes clear-evicted) || fail "nodes clear-evicted failed: $cleared"
(( $(grep -c '^Cleared node-[0-9]*; restart the container to replace it$' <<<"$cleared" || true) == 2 )) \
    || fail "nodes clear-evicted did not clear the 2 evicted nodes: $cleared"
for _ in $(seq 1 10); do
    remaining=$(docker exec evict jq '.nodes | length' /data/manager/ant/node_registry.json)
    (( remaining == 1 )) && break
    sleep 1
done
(( remaining == 1 )) || fail "the registry has $remaining nodes after nodes clear-evicted, expected 1"
docker exec evict nodes status >/dev/null || fail "node manager not answering after nodes clear-evicted"
[[ "$(docker exec evict nodes clear-evicted)" == "No removed nodes to clear" ]] \
    || fail "a second nodes clear-evicted did not say there was nothing to clear"

echo "== a data folder mounted noexec is refused; production mode warns above 2 nodes; the old RPC_URL is pointed out"
# In production mode although COMMON sets testnet: of two -e for the same
# variable, Docker keeps the later one.
docker run -d --name noexec "${COMMON[@]}" -e MANAGER_PORT=12604 --tmpfs /data:rw,noexec,size=100m \
    -e ANT_NETWORK_MODE=production -e NODE_COUNT=3 -e PORT_START=10940 \
    -e RPC_URL=https://robinhood-rpc.publicnode.com "$IMAGE" >/dev/null
wait_for noexec 'mounted noexec' 1 30 || fail "a noexec data folder was not refused"
(( $(count noexec 'WARNING: NODE_COUNT is 3') >= 1 )) || fail "no warning for 3 nodes in production mode"
(( $(count noexec 'RPC_URL is PublicNode') >= 1 )) || fail "no notice for the old PublicNode RPC_URL"

echo "== an RPC_URL of only spaces leaves the image's RPCs in place; an empty ANT_RELAY_SERVING is ignored"
# Refused at the noexec data folder too, which is checked after the settings.
docker run -d --name rpc "${COMMON[@]}" -e MANAGER_PORT=12605 --tmpfs /data:rw,noexec,size=100m \
    -e RPC_URL=" " -e ANT_RELAY_SERVING= -e PORT_START=10950 "$IMAGE" >/dev/null
wait_for rpc 'mounted noexec' 1 30 || fail "an RPC_URL of only spaces or an empty ANT_RELAY_SERVING was refused"

echo "== an ANT_RELAY_SERVING other than true or false is refused"
docker run -d --name relay "${COMMON[@]}" -e MANAGER_PORT=12606 --tmpfs /data:rw,noexec,size=100m \
    -e ANT_RELAY_SERVING=no -e PORT_START=10960 "$IMAGE" >/dev/null
wait_for relay 'ANT_RELAY_SERVING must be true or false' 1 30 || fail "ANT_RELAY_SERVING=no was not refused"

echo "All smoke tests passed"
