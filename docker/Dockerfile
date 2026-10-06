# Storage node image: our build of ant-node, run by Autonomi's own node
# manager (`ant node`), plus our network's settings. Built by
# .github/workflows/build.yml; home operators and our servers run the same image.
FROM ubuntu:24.04

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl jq procps tini util-linux \
    && rm -rf /var/lib/apt/lists/*

# Autonomi's published `ant` release. Its node manager is the same code as in
# our client fork (no changes there); only the node binary is ours.
ARG ANT_VERSION=0.3.9
ARG ANT_SHA256=fec136205c6e8dff1289ccdd922a306e3f4d3ac2f7480d9f4b6b78488ea0b4e6
RUN curl -fsSL -o /tmp/ant.tar.gz \
        "https://github.com/WithAutonomi/ant-client/releases/download/ant-cli-v${ANT_VERSION}/ant-${ANT_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
    && echo "${ANT_SHA256}  /tmp/ant.tar.gz" | sha256sum -c - \
    && tar -xzf /tmp/ant.tar.gz -C /tmp \
    && install -m 755 "/tmp/ant-${ANT_VERSION}-x86_64-unknown-linux-musl/ant" /usr/local/bin/ant \
    && rm -rf /tmp/ant*

COPY dist/ant-node dist/bootstrap_peers.toml /opt/ant/
COPY docker/entrypoint.sh /entrypoint.sh
COPY docker/nodes /usr/local/bin/nodes
RUN chmod 755 /opt/ant/ant-node /entrypoint.sh /usr/local/bin/nodes

# Our network. Operators set REWARDS_ADDRESS, NODE_COUNT and usually
# BROWSER_PORT_START; servers also set PUBLIC_IP and RPC_URLS. The rest change
# only with a new image (for example the switch to the main token). Browser
# access is off unless BROWSER_PORT_START is set (the Unraid template sets
# 11000), so older installs don't advertise ports they never forwarded.
# The XDG paths keep the node manager's state on the /data volume and let
# `docker exec ... nodes` find it.
ENV ANT_NETWORK_MODE=testnet \
    ANT_EVM_PAYMENT_TOKEN=0x511f76a3de41f000ec11ea4fd212fb3587c6cef3 \
    ANT_EVM_PAYMENT_VAULT=0xE0f627986c0438De0E314B644f3C8aA21D3f4f6B \
    BOOTSTRAP_PEERS="142.93.32.90:10000 165.22.193.90:10000" \
    RPC_URL=https://robinhood-rpc.publicnode.com \
    RPC_URLS="" \
    NODE_COUNT=1 \
    PORT_START=10000 \
    BROWSER_PORT_START=0 \
    PUBLIC_IP="" \
    IPV4_ONLY=false \
    MANAGER_PORT=12600 \
    PUID=99 \
    PGID=100 \
    HOME=/data/manager \
    XDG_DATA_HOME=/data/manager \
    XDG_CONFIG_HOME=/data/manager/config \
    NO_COLOR=1

VOLUME /data
ENTRYPOINT ["/usr/bin/tini", "--", "/entrypoint.sh"]
