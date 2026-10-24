#!/usr/bin/env bash
# Tests the storage-node image on a small isolated network, never the live one:
# a seed node with no bootstrap peers, plus test containers that bootstrap only
# from it. Containers sharing the host network need their own MANAGER_PORT. Run by CI after building the image; needs Docker with host networking,
# and pulls the image of the release before (PREVIOUS_IMAGE) to test the move
# from it.
#
#   docker/smoke-test.sh <image>
#
# Checks: the image is set for the Darwin network (DARWIN's token and vault,
# Robinhood Chain's RPC, the bootstrap servers, production mode, IPv4 only)
# and its node's bootstrap file names no peers; its node manager is
# darwin-nodes, built from the darwin-client commit docker/manager-revision
# pins, with its licences and notices, and Autonomi's `ant` is gone; nodes
# start and join, and each names DARWIN's token and vault and only its RPC's
# host when it starts; every node gets the image marker, which the registry
# never holds; browser addresses are published on the fixed
# ports; a restart keeps node identities and starts the nodes again; the
# previous image's /data/node-<port> layout is taken over with identities
# kept; lowering NODE_COUNT retires a node; retired data is deleted at a start
# 4 or more days after its retirement, however long it sat unchanged before;
# node folders no node is registered for are set aside in retired/unregistered,
# which is never deleted; changing PORT_START moves the
# nodes with identities kept; docker stop is clean and timely;
# on a nearly full disk the node manager reports the disk as critical and
# nothing is evicted or deleted, and `nodes clear-evicted` does nothing; a
# /data written by the release before, whose node manager was Autonomi's
# `ant` 0.3.9, keeps every node's id, port, data and identity, with nothing
# retired; nodes that manager evicted are dismissed once, and NODE_COUNT is
# made up with new nodes; a data folder mounted noexec is refused;
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
# The image of release 0.23.0+ds.15, the last release whose image ran
# Autonomi's node manager, `ant` 0.3.9, which evicted nodes on a nearly full
# disk: the image installs move to this one from.
PREVIOUS_IMAGE=ghcr.io/project-dspace/storage-node:20fbb745d06909a5adee55bcd9fc9113e466ce16@sha256:edd2e1623fa31176f37c99ff6d3e0f40743fba0680f86dc2eac59b5b974e0208
# The darwin-client commit the image's node manager is built from.
MANAGER_REVISION=$(tail -n 1 "$(dirname "${BASH_SOURCE[0]}")/manager-revision")
CONTAINERS="seed smoke full previous upgraded evicting dismissing noexec rpc relay"

fail() {
    echo "FAILED: $*"
    for c in $CONTAINERS; do
        docker ps -a --format '{{.Names}}' | grep -qx $c || continue
        echo "--- $c log (last 40 lines)"; docker logs $c 2>&1 | grep -vE '^\s*$' | tail -40
    done
    exit 1
}
cleanup() { docker rm -f $CONTAINERS >/dev/null 2>&1 || true; sudo rm -rf "$WORK"; }
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
# The image marker in the environment of each node process in a container,
# one line a node.
node_markers() {
    docker exec "$1" sh -c 'for pid in $(pgrep -x ant-node); do tr "\0" "\n" < /proc/$pid/environ | grep "^ANT_IMAGE_VERSION=" || echo none; done'
}
has_marker() { # container nodes
    local markers
    markers=$(node_markers "$1")
    [[ "$(grep -c . <<<"$markers")" == "$2" && "$(sort -u <<<"$markers")" == "ANT_IMAGE_VERSION=$MARKER" ]]
}
# A file put in each node folder under a /data, to see that none is deleted,
# and how many of them are there.
mark_folders() { sudo sh -c "for dir in $1/nodes/node-*; do echo kept > \$dir/smoke-kept; done"; }
marked_folders() { sudo sh -c "cat $1/nodes/node-*/smoke-kept 2>/dev/null" | grep -c '^kept$' || true; }

# The Darwin network the image is built for (tests/darwin_network.rs checks
# that docker/Dockerfile sets the same).
TOKEN=0x511f76a3de41f000ec11ea4fd212fb3587c6cef3
VAULT=0xe0f627986c0438de0e314b644f3c8aa21d3f4f6b
VAULT_DEPLOYED=81897121
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
for setting in "ANT_EVM_PAYMENT_TOKEN=$TOKEN" "ANT_EVM_PAYMENT_VAULT=$VAULT" \
    "ANT_EVM_PAYMENT_VAULT_DEPLOYMENT_BLOCK=$VAULT_DEPLOYED" "BOOTSTRAP_PEERS=$SERVERS" \
    ANT_NETWORK_MODE=production IPV4_ONLY=true; do
    grep -qixF "$setting" <<<"$image_env" || fail "the image does not set $setting"
done
grep -qE '^RPC_URLS=(.* )?https://rpc\.mainnet\.chain\.robinhood\.com( |$)' <<<"$image_env" \
    || fail "the image's RPC_URLS don't include Robinhood Chain's RPC"
peers=$(docker run --rm --entrypoint cat "$IMAGE" /opt/ant/bootstrap_peers.toml | grep -vE '^[[:space:]]*(#|$)' || true)
[[ "$peers" == "peers = []" ]] || fail "/opt/ant/bootstrap_peers.toml names peers: $peers"

echo "== the image's node manager: darwin-nodes from the pinned commit, with its licences and notices, and no Autonomi ant"
[[ "$MANAGER_REVISION" =~ ^[0-9a-f]{40}$ ]] || fail "docker/manager-revision does not end with a commit: $MANAGER_REVISION"
version=$(docker run --rm --entrypoint darwin-nodes "$IMAGE" --version) || fail "the image has no darwin-nodes"
echo "$version"
[[ "$version" == "darwin-nodes "*"commit ${MANAGER_REVISION:0:7}" ]] || fail "darwin-nodes is not built from ${MANAGER_REVISION:0:7}: $version"
docker run --rm --entrypoint sh "$IMAGE" -c '! command -v ant' >/dev/null || fail "the image still has Autonomi's ant"
docker run --rm --entrypoint sh "$IMAGE" -c 'cd /usr/share/doc/storage-node/darwin-nodes &&
    for f in LICENSE-MIT LICENSE-APACHE THIRD-PARTY-NOTICES.txt RUST-STD-COPYRIGHT.html; do test -s "$f" || exit 1; done' \
    || fail "darwin-nodes' licences and notices are not in /usr/share/doc/storage-node/darwin-nodes"
docker run --rm --entrypoint grep "$IMAGE" -qF "$MANAGER_REVISION" /usr/share/doc/storage-node/NOTICE \
    || fail "the image's NOTICE does not name darwin-nodes' commit"
MARKER=$(docker run --rm --entrypoint cat "$IMAGE" /opt/ant/image-version)
[[ "$MARKER" =~ ^[0-9a-f]{40}$ ]] || fail "the image marker is '$MARKER', not a commit"

echo "== seed node (its only bootstrap peer is itself: starts an isolated network)"
docker run -d --name seed "${COMMON[@]}" -e MANAGER_PORT=12601 -e NODE_COUNT=1 -e PORT_START=10900 \
    -e PUBLIC_IP=$IP -e BOOTSTRAP_PEERS="$IP:10900" -v "$WORK/seed:/data" "$IMAGE" >/dev/null
wait_for seed 'Running 1 node' 1 60 || fail "seed did not start"
[[ "$(registry "$WORK/seed" '.nodes["1"].bootstrap_peers | length')" == 0 ]] || fail "the seed lists itself as a bootstrap peer"
logs_payments seed 1 || fail "the seed's node did not name DARWIN's token and vault and its RPC's host alone"

echo "== three nodes joining the seed, browser ports 11910-11912, relay serving off; each gets the image marker"
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
has_marker smoke 3 || fail "not every node has the image marker $MARKER: $(node_markers smoke | tr '\n' ' ')"
[[ "$(registry "$WORK/smoke" '[.nodes[].env_variables | has("ANT_IMAGE_VERSION")] | any')" == false ]] \
    || fail "the registry holds the image marker"
# The manager's API keeps the shape that scripts reading it rely on.
docker exec smoke curl -fsS -o /dev/null http://127.0.0.1:12602/api/v1/status || fail "the manager's /api/v1/status did not answer"
api_nodes=$(docker exec smoke curl -fsS http://127.0.0.1:12602/api/v1/nodes/status) \
    || fail "the manager's /api/v1/nodes/status did not answer"
jq -e '(.nodes | length) == 3 and all(.nodes[]; (.node_id | type) == "number" and (.version | type) == "string"
    and .status == "running" and (.uptime_secs | type) == "number")' <<<"$api_nodes" >/dev/null \
    || fail "the manager's /api/v1/nodes/status changed shape: $api_nodes"
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

echo "== lowering NODE_COUNT retires the node on the highest port; retired data is kept 3 days; unregistered folders are set aside for good"
docker stop -t 120 smoke >/dev/null && docker rm smoke >/dev/null
# Retired folders from earlier starts: one retired 5 days ago goes at this
# start, one retired 84 hours ago stays.
sudo mkdir "$WORK/smoke/retired/old-node-8" "$WORK/smoke/retired/recent-node-9"
sudo touch -d '5 days ago' "$WORK/smoke/retired/old-node-8"
sudo touch -d '84 hours ago' "$WORK/smoke/retired/recent-node-9"
# node-7, a node folder no node is registered for, is set aside at this
# start; old-node-6 was set aside at a start 5 days ago, and unregistered
# itself last changed then.
sudo mkdir -p "$WORK/smoke/nodes/node-7" "$WORK/smoke/retired/unregistered/old-node-6"
sudo sh -c "echo kept > $WORK/smoke/nodes/node-7/smoke-kept; echo kept > $WORK/smoke/retired/unregistered/old-node-6/smoke-kept"
sudo touch -d '5 days ago' "$WORK/smoke/retired/unregistered/old-node-6" "$WORK/smoke/retired/unregistered"
# node-3's folder, unchanged for 5 days, keeps its 3 days once retired. Its
# process file is gone first, so nothing before the move changes the folder.
sudo rm -f "$WORK/smoke/nodes/node-3/node.pid"
sudo touch -d '5 days ago' "$WORK/smoke/nodes/node-3"
run_smoke 2
wait_for smoke 'Retiring node-3 \(port 10912\)' 1 60 || fail "node-3 was not retired"
wait_for smoke 'Setting aside node-7, which no registered node has' 1 60 || fail "node-7's folder was not set aside"
wait_for smoke 'Running 2 node' 1 60 || fail "2 nodes did not start"
sudo sh -c "ls -d $WORK/smoke/retired/*-node-3" >/dev/null || fail "node-3's data was not kept in /data/retired"
sudo test ! -e "$WORK/smoke/retired/old-node-8" || fail "data retired 5 days ago was not deleted at start"
sudo test -d "$WORK/smoke/retired/recent-node-9" || fail "data retired 84 hours ago was deleted at start"
sudo sh -c "cat $WORK/smoke/retired/unregistered/*-node-7/smoke-kept" >/dev/null \
    || fail "node-7's folder was not kept in /data/retired/unregistered"
sudo test -f "$WORK/smoke/retired/unregistered/old-node-6/smoke-kept" || fail "a folder set aside 5 days ago was deleted"

echo "== changing PORT_START moves the nodes, identities kept"
docker stop -t 120 smoke >/dev/null && docker rm smoke >/dev/null
ids=$(peer_ids "$WORK/smoke")
sudo touch -d '5 days ago' "$WORK/smoke/retired/unregistered" "$WORK/smoke/retired/unregistered"/*-node-7
run_smoke 2 10920
wait_for smoke 'Moving node-2 from UDP port 10911 to 10921' 1 60 || fail "nodes were not moved to the new ports"
wait_for smoke 'Running 2 node' 1 60 || fail "moved nodes did not start"
# The first start after node-3 was retired: its folder, unchanged for 5 days
# before that, is still within its 3 days.
sudo sh -c "ls -d $WORK/smoke/retired/*-node-3" >/dev/null \
    || fail "node-3's data, retired at the last start, was deleted at this one"
(( $(sudo sh -c "cat $WORK/smoke/retired/unregistered/*/smoke-kept" | grep -c '^kept$') == 2 )) \
    || fail "a folder set aside in /data/retired/unregistered was deleted"
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

echo "== a nearly full 1.5 GB disk: the node manager reports it, and nothing is evicted or deleted"
docker run -d --name full "${COMMON[@]}" -e MANAGER_PORT=12603 --tmpfs /data:rw,exec,size=1500m -e NODE_COUNT=3 -e PORT_START=10930 \
    -e BOOTSTRAP_PEERS="$IP:10900" "$IMAGE" >/dev/null
wait_for full 'Running 3 node' 1 60 || fail "the nodes on the small disk did not start"
(( $(count full 'WARNING: only [0-9]+ GiB free') >= 1 )) || fail "no low-disk warning at startup"
docker exec full sh -c 'for dir in /data/nodes/node-*; do echo kept > "$dir/smoke-kept"; done'
in_full() { docker exec full sh -c "$1"; }
registry_full() { in_full "jq -cS .nodes /data/manager/ant/node_registry.json"; }
nodes_before=$(registry_full)
avail=$(in_full "df -B1 --output=avail /data" | tail -n 1 | tr -d ' ')
in_full "fallocate -l $((avail - 300 * 1024 * 1024)) /data/filler"
# The manager reads the disk every 30 seconds. Once it reports the disk as
# critical (500 MiB free or less) it has read the disk nearly full, and
# Autonomi's manager evicted a node at each such reading, 2 of these 3
# within 3 minutes. Four more readings after that.
health() { in_full "curl -fsS http://127.0.0.1:12603/api/v1/health" || true; }
for _ in $(seq 1 45); do
    [[ "$(health | jq -r '.overall // empty')" == critical ]] && break
    sleep 2
done
[[ "$(health | jq -r '.overall // empty')" == critical ]] || fail "the node manager did not report the nearly full disk: $(health)"
echo "health: $(health | jq -c '[.checks[] | {level, summary}]')"
sleep 120
[[ "$(registry_full)" == "$nodes_before" ]] || fail "the registry changed on the nearly full disk: $(registry_full)"
(( $(in_full 'cat /data/nodes/node-*/smoke-kept 2>/dev/null' | grep -c '^kept$' || true) == 3 )) \
    || fail "a node's folder was deleted on the nearly full disk"
(( $(count full '^\[manager\].*[Ee]vict') == 0 )) \
    || fail "the node manager logged an eviction: $(docker logs full 2>&1 | grep -E '^\[manager\].*[Ee]vict' | head -5)"
docker exec full nodes status >/dev/null || fail "nodes status did not answer on the nearly full disk"
status=$(docker exec full nodes status --json) || fail "nodes status --json did not answer on the nearly full disk"
[[ "$(jq '[.nodes[] | select(.status == "evicted")] | length' <<<"$status")" == 0 && "$(jq '.nodes | length' <<<"$status")" == 3 ]] \
    || fail "nodes status shows an evicted node or fewer than 3: $status"
[[ "$(jq -r '.health.overall' <<<"$status")" == critical ]] || fail "nodes status does not report the disk as critical: $status"
for _ in $(seq 1 15); do
    [[ "$(docker exec full nodes status --json | jq '.total_running')" == 3 ]] && break
    sleep 2
done
[[ "$(docker exec full nodes status --json | jq '.total_running')" == 3 ]] || fail "not every node is running on the nearly full disk"

echo "== nodes clear-evicted does nothing"
cleared=$(docker exec full nodes clear-evicted) || fail "nodes clear-evicted failed: $cleared"
[[ "$cleared" == "No removed nodes to clear: "* ]] || fail "nodes clear-evicted said: $cleared"
[[ "$(registry_full)" == "$nodes_before" ]] || fail "nodes clear-evicted changed the registry"
# The tmpfs holds its 1.5 GB in memory: freed before the next ones.
docker rm -f full >/dev/null

echo "== a /data written by the release before, under Autonomi's node manager: every node keeps its id, port, data and identity, and nothing is retired"
docker pull -q "$PREVIOUS_IMAGE" >/dev/null || fail "could not pull $PREVIOUS_IMAGE"
run_previous() { # container image
    docker run -d --name "$1" "${COMMON[@]}" -e MANAGER_PORT=12607 -e NODE_COUNT=3 -e PORT_START=10970 \
        -e BROWSER_PORT_START=11970 -e PUBLIC_IP=$IP -e BOOTSTRAP_PEERS="$IP:10900" -v "$WORK/previous:/data" "$2" >/dev/null
}
run_previous previous "$PREVIOUS_IMAGE"
wait_for previous 'Running 3 node' 1 60 || fail "the release before did not start 3 nodes"
wait_for previous '\[node-[0-9]+\].*Successfully connected to [1-9]' 3 || fail "the release before's nodes did not join the seed"
for _ in $(seq 1 30); do
    (( $(wc -w <<<"$(peer_ids "$WORK/previous")") == 3 )) && break
    sleep 2
done
ids=$(peer_ids "$WORK/previous")
(( $(wc -w <<<"$ids") == 3 )) || fail "the release before's nodes did not publish their browser addresses"
layout() { registry "$WORK/previous" '.nodes | map_values({id, service_name, node_port, data_dir, log_dir, binary_path}) | tojson'; }
layout_before=$(layout)
docker stop -t 120 previous >/dev/null && docker rm previous >/dev/null
# The browser address files are written again as each node starts.
sudo sh -c "rm -f $WORK/previous/nodes/node-*/webrtc-direct.multiaddr"
mark_folders "$WORK/previous"
run_previous upgraded "$IMAGE"
wait_for upgraded 'Running 3 node' 1 60 || fail "the image did not start the release before's 3 nodes"
wait_for upgraded '\[node-[0-9]+\].*Successfully connected to [1-9]' 3 || fail "the nodes did not rejoin the seed under the image"
for _ in $(seq 1 30); do
    (( $(wc -w <<<"$(peer_ids "$WORK/previous")") == 3 )) && break
    sleep 2
done
[[ "$(peer_ids "$WORK/previous")" == "$ids" ]] || fail "identities changed on the move: $ids -> $(peer_ids "$WORK/previous")"
[[ "$(layout)" == "$layout_before" ]] || fail "the registry's nodes changed on the move: $layout_before -> $(layout)"
(( $(marked_folders "$WORK/previous") == 3 )) || fail "a node's data folder was deleted on the move"
[[ "$(sudo find "$WORK/previous/manager" -name node_registry.json)" == "$WORK/previous/manager/ant/node_registry.json" ]] \
    || fail "the registry was not read in place"
[[ -z "$(sudo find "$WORK/previous/retired" -mindepth 1 ! -path "$WORK/previous/retired/unregistered")" ]] \
    || fail "the move retired or set aside something: $(sudo find "$WORK/previous/retired" -mindepth 1)"
(( $(count upgraded 'Retiring|Setting aside|Dismissing|Taking over|Moving node') == 0 )) \
    || fail "the move retired, set aside, dismissed or moved a node"
has_marker upgraded 3 || fail "not every node has the image marker after the move: $(node_markers upgraded | tr '\n' ' ')"
docker rm -f upgraded >/dev/null

echo "== nodes the release before evicted are dismissed once, and NODE_COUNT is made up with new nodes"
docker run -d --name evicting "${COMMON[@]}" -e MANAGER_PORT=12608 --tmpfs /data:rw,exec,size=1500m -e NODE_COUNT=3 \
    -e PORT_START=10980 -e BOOTSTRAP_PEERS="$IP:10900" "$PREVIOUS_IMAGE" >/dev/null
wait_for evicting 'Running 3 node' 1 60 || fail "the release before did not start 3 nodes on the small disk"
avail=$(docker exec evicting df -B1 --output=avail /data | tail -n 1 | tr -d ' ')
docker exec evicting fallocate -l $((avail - 300 * 1024 * 1024)) /data/filler
evicted=0
for _ in $(seq 1 60); do
    evicted=$(docker exec evicting jq '[.nodes[] | select(.eviction != null)] | length' /data/manager/ant/node_registry.json)
    (( evicted >= 2 )) && break
    sleep 3
done
(( evicted == 2 )) || fail "the release before's node manager evicted $evicted of 3 nodes, not 2"
evicted_ids=$(docker exec evicting jq -r '[.nodes | to_entries[] | select(.value.eviction != null) | .key] | join(" ")' /data/manager/ant/node_registry.json)
kept_id=$(docker exec evicting jq -r '.nodes | to_entries[] | select(.value.eviction == null) | .key' /data/manager/ant/node_registry.json)
kept_port=$(docker exec evicting jq -r ".nodes[\"$kept_id\"].node_port" /data/manager/ant/node_registry.json)
echo "evicted: node-${evicted_ids// / node-}; kept: node-$kept_id"
# /data as that manager left it, copied off the small disk once its nodes
# are stopped, without the filler.
docker exec evicting nodes stop >/dev/null 2>&1 || true
sudo mkdir "$WORK/evicted"
docker exec evicting tar -C /data --exclude=./filler -cf - . | sudo tar -C "$WORK/evicted" --numeric-owner -xpf -
docker rm -f evicting >/dev/null
mark_folders "$WORK/evicted"
docker run -d --name dismissing "${COMMON[@]}" -e MANAGER_PORT=12608 -e NODE_COUNT=3 -e PORT_START=10980 \
    -e BOOTSTRAP_PEERS="$IP:10900" -v "$WORK/evicted:/data" "$IMAGE" >/dev/null
wait_for dismissing 'Running 3 node' 1 90 || fail "the image did not run 3 nodes in place of the evicted ones"
for id in $evicted_ids; do
    (( $(count dismissing "^Dismissing node-$id, which an earlier node manager evicted") == 1 )) || fail "node-$id was not dismissed with its line"
    [[ "$(registry "$WORK/evicted" ".nodes | has(\"$id\")")" == false ]] || fail "node-$id is still registered"
done
(( $(count dismissing '^Dismissing node-') == 2 )) || fail "other nodes were dismissed"
[[ "$(registry "$WORK/evicted" '[.nodes[] | select(.eviction != null)] | length')" == 0 ]] || fail "the registry still marks a node evicted"
[[ "$(registry "$WORK/evicted" ".nodes[\"$kept_id\"].node_port")" == "$kept_port" ]] || fail "node-$kept_id lost its port"
(( $(marked_folders "$WORK/evicted") == 1 )) || fail "node-$kept_id's data folder was deleted"
(( $(count dismissing 'Added node on UDP port') == 2 )) || fail "the 2 evicted nodes were not replaced with new ones"
for id in $(registry "$WORK/evicted" '.nodes | keys[]'); do
    [[ " $evicted_ids " != *" $id "* ]] || fail "node-$id, which was evicted, is registered again"
done
sudo sh -c "ls -d $WORK/evicted/retired/*-node-*-logs" >/dev/null || fail "the evicted nodes' logs were not retired"
docker restart -t 120 dismissing >/dev/null
wait_for dismissing 'Running 3 node' 2 90 || fail "the nodes did not start after a restart"
(( $(count dismissing '^Dismissing node-') == 2 )) || fail "nodes were dismissed again after a restart"
has_marker dismissing 3 || fail "not every node has the image marker: $(node_markers dismissing | tr '\n' ' ')"
docker rm -f dismissing >/dev/null

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
