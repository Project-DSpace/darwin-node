# Storage node image: our build of ant-node, run by our node manager,
# darwin-nodes, plus our network's settings. Built by
# .github/workflows/build.yml; home operators and our servers run the same image.
FROM ubuntu:24.04

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl jq procps tini util-linux \
    && rm -rf /var/lib/apt/lists/*

# The node manager: darwin-nodes, built from darwin-client
# (https://github.com/Project-DSpace/darwin-client) at the commit
# docker/manager-revision pins, which docker/NOTICE names too. It runs, stops
# and supervises the nodes from the binary each has in its folder, keeps their
# registry in $XDG_DATA_HOME/ant, deletes no node's data, downloads nothing and
# forwards no logs.
COPY dist/darwin-nodes/darwin-nodes /usr/local/bin/darwin-nodes
COPY dist/ant-node dist/bootstrap_peers.toml /opt/ant/
# Licences and notices, packaged by the build with each program: ant-node's
# here, darwin-nodes' in darwin-nodes/. NOTICE says what each program is.
COPY dist/LICENSE-MIT dist/LICENSE-APACHE dist/THIRD-PARTY-NOTICES.txt dist/RUST-STD-COPYRIGHT.html \
     docker/NOTICE /usr/share/doc/storage-node/
COPY dist/darwin-nodes/LICENSE-MIT dist/darwin-nodes/LICENSE-APACHE dist/darwin-nodes/THIRD-PARTY-NOTICES.txt \
     dist/darwin-nodes/RUST-STD-COPYRIGHT.html /usr/share/doc/storage-node/darwin-nodes/
COPY docker/entrypoint.sh /entrypoint.sh
COPY docker/nodes /usr/local/bin/nodes
RUN chmod 755 /opt/ant/ant-node /usr/local/bin/darwin-nodes /entrypoint.sh /usr/local/bin/nodes

# The image marker: the darwin-node commit the image is built from, which the
# entrypoint passes to every node (ANT_IMAGE_VERSION). build.yml gives it.
ARG IMAGE_VERSION
RUN echo "$IMAGE_VERSION" | grep -qxE '[0-9a-f]{40}' \
        || { echo "Build with --build-arg IMAGE_VERSION=<the darwin-node commit the image is built from>" >&2; exit 1; } \
    && echo "$IMAGE_VERSION" > /opt/ant/image-version

# Our network, set the way our servers run it: production mode (each node's
# routing table takes at most 2 nodes from one IP in each bucket and among the
# 20 nearest), IPv4 only, the 3 bootstrap servers,
# and Robinhood Chain's own RPC (RPCs listed in RPC_URLS are shared out over
# the nodes). Operators set
# REWARDS_ADDRESS, NODE_COUNT and usually BROWSER_PORT_START; servers also set
# PUBLIC_IP. RPC_URL, if set, replaces RPC_URLS with one provider of the
# operator's choice. The token, vault (with the block it was deployed in) and
# bootstrap peers change only with a
# new image. Browser access is off unless BROWSER_PORT_START is set (the compose
# file sets 11000), so older installs don't advertise ports they never
# forwarded. The XDG paths keep the node manager's state on the /data volume
# and let `docker exec ... nodes` find it.
ENV ANT_NETWORK_MODE=production \
    ANT_EVM_PAYMENT_TOKEN=0x511f76a3de41f000ec11ea4fd212fb3587c6cef3 \
    ANT_EVM_PAYMENT_VAULT=0xE0f627986c0438De0E314B644f3C8aA21D3f4f6B \
    ANT_EVM_PAYMENT_VAULT_DEPLOYMENT_BLOCK=81897121 \
    BOOTSTRAP_PEERS="142.93.32.90:10000 165.22.193.90:10000 142.93.96.232:10000" \
    RPC_URL="" \
    RPC_URLS="https://rpc.mainnet.chain.robinhood.com" \
    NODE_COUNT=1 \
    PORT_START=10000 \
    BROWSER_PORT_START=0 \
    PUBLIC_IP="" \
    IPV4_ONLY=true \
    MANAGER_PORT=12600 \
    PUID=99 \
    PGID=100 \
    HOME=/data/manager \
    XDG_DATA_HOME=/data/manager \
    XDG_CONFIG_HOME=/data/manager/config \
    NO_COLOR=1

VOLUME /data
ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
