# Running storage nodes with Docker

This sets up storage nodes for Darwin, our storage network, in a Docker
container on a Linux server, using Docker Compose. Inside the container, a node
manager runs and looks after the nodes. It takes about 15 minutes. On Unraid, use
the template instead: see [On Unraid](#on-unraid).
You'll need:

- A **Linux server** with Docker and its Compose plugin (`docker compose
  version` should print a version). Docker Desktop on Mac or Windows won't do:
  the nodes need the server's own network, which only Linux gives a container.
- A **wallet address** for your storage fees: any Ethereum-style address
  starting with `0x`, for example from MetaMask.
- **Access to your router**, to forward some UDP ports.
- **Spare disk space**: at least 20 GB per node (about 35 GB recommended), plus
  for each node some memory, CPU and part of your internet connection.
  As a guide, a node computer should have a 4-core CPU, 8 GB of RAM and 8 Mbps
  up and down.

Fees are paid in DARWIN, the network's token, to the rewards address you set.

The image comes set up for the network as our servers run it: production mode,
IPv4 only, the 3 bootstrap servers (142.93.32.90, 165.22.193.90 and
142.93.96.232, UDP port 10000) and Robinhood Chain RPCs. You don't need to set
any of these.

Running nodes from before 2026-10-06? See [Rejoining after the 2026-10-06
reset](#rejoining-after-the-2026-10-06-reset).

## 1. Forward the ports on your router

Each node uses two UDP ports: one for the storage network and one for web
browsers fetching files. Run 2 nodes and forward **UDP 10000–10001** and
**UDP 11000–11001** to your server's local IP address. The exact menu depends
on your router; it is usually called "Port forwarding" or "Virtual servers".
Use the same port numbers outside and inside.

Use either manual forwarding or your router's automatic forwarding (UPnP) for
the storage ports, not both: the nodes ask UPnP-capable routers for those
ports themselves, and a manual rule on the same ports makes the router hand
them random ones instead. If your router has UPnP on, forward only the browser
ports by hand.

If you can't forward the browser ports, set `BROWSER_PORT_START` to `0` (step
2): the nodes then store and earn as usual, but don't serve web browsers.

If the server runs a firewall (such as `ufw`), open the same UDP ports there too.

## 2. Set up and start the container

1. Make a folder for the nodes and download `docker-compose.yml` into it:

   ```
   mkdir ~/storage-node && cd ~/storage-node
   curl -fsSLO https://raw.githubusercontent.com/Project-DSpace/darwin-node/main/docker/docker-compose.yml
   ```

2. Open `docker-compose.yml` and fill in:
   - **`REWARDS_ADDRESS`**: your `0x…` address, in place of
     `0xYOUR_WALLET_ADDRESS`.
   - **`NODE_COUNT`**: 2 (or 1 if your disk, memory or connection is
     short). The network counts at most 2 nodes per IP address in any close
     group, so more than 2 nodes add load without adding storage, and the
     container warns about it. Each node needs at least 20 GB of free disk
     space.
   - **The data folder**: node data goes in `data` next to the file. To use
     another disk, change `./data` in the `volumes` line to a folder on that
     disk, such as `/mnt/storage/storage-node:/data`. The nodes measure free
     space on whichever disk that folder is on.
3. In that folder, run:

   ```
   docker compose up -d
   ```

   Docker downloads the image and starts the nodes.

The other settings in the file are explained in its comments; the defaults suit
most servers.

## 3. Check it's working

In the same folder, run `docker compose logs -f` (Ctrl+C to stop watching).
Within a minute you should see:

```
Added node on UDP port 10000
Added node on UDP port 10001
Running 2 node(s) on UDP ports 10000-10001; browser access: UDP 11000-11001; storage fees go to 0x…
[node-1] … Successfully connected to 3 bootstrap peers
```

If it stops with `REWARDS_ADDRESS is not a wallet address`, put your wallet
address in `docker-compose.yml` and run `docker compose up -d` again.



## Day to day

Run these in the folder with `docker-compose.yml`.

- **Updates**: `docker compose pull`, then `docker compose up -d`.
- **Keep it running**: nodes that are offline for long get dropped by the
  network and stop earning. Restarts and reboots are fine: the container
  starts again with Docker.
- **Stopping**: `docker compose down`. Your node data stays in the data folder;
  `docker compose up -d` starts the nodes again.
- **Status**: `docker exec storage-node nodes status` lists your nodes.
- **More or fewer nodes**: change `NODE_COUNT` in `docker-compose.yml` and run
  `docker compose up -d`. Fewer nodes removes any the disk-full check removed
  first (see below), then the ones on the highest ports. Removed nodes' data is
  kept in `retired/` inside the data folder for 3 days, in case you change your
  mind, then deleted.
- **Different ports**: change `PORT_START` or `BROWSER_PORT_START` and run
  `docker compose up -d`. The nodes move to the new ports and keep their
  identities; change your router's port forwarding to match.
- **When the disk gets full**: the node manager checks the free space where
  the data folder is every 30 seconds. At 1 GiB (1,024 MiB) free or less,
  `docker exec storage-node nodes status` shows a warning naming the node that
  would be removed next, as does the manager's `GET /api/v1/health` on
  `127.0.0.1:MANAGER_PORT`; nothing is written to the log. At 500 MiB free or
  less, it removes the node holding the least data (never the last one),
  deletes that node's data so the others have room, and logs `Evicted node`
  with how much it freed. Removed nodes stay
  removed: free up space, run `docker exec storage-node nodes clear-evicted`,
  then `docker compose restart` to replace them, or lower `NODE_COUNT`.
- **IPv4 only**: the network runs on IPv4. Leave `IPV4_ONLY` at `"true"`.
- **Relaying for other nodes**: each node also runs a relay, on which anyone
  on the network can open a session and have your node send UDP for them, to
  any address, from your IP address. Replies come back to a port the relay picks
  at random for each session, so behind a router or firewall that doesn't let
  them in, only the sending works. To turn relaying off, add
  `ANT_RELAY_SERVING: "false"` under `environment:` in `docker-compose.yml`
  and run `docker compose up -d` (on Unraid, add a variable
  `ANT_RELAY_SERVING` with the value `false`). Each node then logs `Relay
  serving is off` when it starts; storing and earning don't change. Leave it
  out, or set `"true"`, to keep relaying on.

## Rejoining after the 2026-10-06 reset

On 2026-10-06 the network restarted in production mode, paying in DARWIN, and
the old network's data was wiped. Nodes from before then hold data and
settings for the old network (testnet mode, up to 6 nodes, IPv6 on), so start
them fresh. With Docker Compose, in the folder with `docker-compose.yml`:

1. Stop the container: `docker compose down`.
2. Wipe the data folder: `sudo rm -rf ./data/*` (or the folder in your
   `volumes` line).
3. Replace `docker-compose.yml` with the current one (step 2 above) and put
   your `REWARDS_ADDRESS` back in. If you keep your old file instead, set
   `NODE_COUNT` to 2 and `IPV4_ONLY` to `"true"`, and delete the `RPC_URL`
   line.
4. Pull the new image and recreate the container: `docker compose pull`, then
   `docker compose up -d`.

On Unraid: stop the container, then wipe its data folder in the Unraid
terminal (for the default folder, `rm -rf /mnt/user/appdata/storage-node/*`).
Edit the container: set **Number of nodes** to 2, **IPv4 only** to `true` and
clear **Blockchain RPC** (both under "Show more settings"), remove any
**Storage limit** variable, and set **Extra Parameters** as in [Add the
container](#add-the-container). Click **Apply**: Unraid downloads the current
image and recreates the container.

The nodes start with new identities; your rewards address stays the same.

## On Unraid

Unraid runs containers from templates rather than Compose files. Follow step 1
above, then:

### Add the container

1. In the Unraid web interface, open the terminal (the `>_` icon, top right)
   and download the template:

   ```
   curl -fsSL -o /boot/config/plugins/dockerMan/templates-user/my-storage-node.xml \
     https://raw.githubusercontent.com/Project-DSpace/darwin-node/main/docker/unraid-storage-node.xml
   ```

2. In Unraid, go to **Docker → Add Container**, and choose **storage-node** from
   the Template list.
3. Fill in:
   - **Rewards wallet**: your `0x…` address.
   - **Number of nodes**: as in step 2 above.
   - **Data folder**: the default `/mnt/user/appdata/storage-node` is fine if
     your `appdata` share can spill over to the array (**Shares → appdata**,
     Secondary storage set to Array). If `appdata` lives only on a cache drive,
     pick a folder on the disk you want the nodes to use instead: the nodes
     measure free space where the folder is, and on `/mnt/user` that's the
     whole array's free space.
4. Click **Apply**. Unraid downloads the image and starts the nodes.

Without the template file, fill in Add Container by hand instead: Repository
`ghcr.io/project-dspace/storage-node:latest`, Network Type **Host**, a path
`/data` → `/mnt/user/appdata/storage-node`, and variables `REWARDS_ADDRESS`
(your wallet), `NODE_COUNT` (2) and `BROWSER_PORT_START` (11000). Under
**Advanced view → Extra Parameters**, add
`--ulimit nofile=65536:65536 --stop-timeout 120 --log-opt max-size=100m --log-opt max-file=3`
(stopping the nodes cleanly can take a minute or two, and the log options keep
the container's log from filling the disk).

### Check it's working

On the Docker tab, click the container's icon and choose **Logs**, and look for
the lines shown in step 3. The container's network should show as **host**, with
the IP and port columns blank. If it says **bridge**, edit the container and set
Network Type to **Host**: in bridge mode nothing from the internet can reach
the nodes.

### Day to day on Unraid

Everything under [Day to day](#day-to-day) applies, but instead of editing
`docker-compose.yml`, edit the container and change the matching setting
(**Number of nodes**, **First UDP port**, **First browser UDP port**, **IPv4
only** under "Show more settings"), then click **Apply**. For updates, click
**Check for Updates** on the Docker tab, then **Apply Update**. Run the
`docker exec` commands in the Unraid terminal, using your container's name if
it isn't `storage-node`, and restart the container from the Docker tab.

## Licences

The licences and notices of the software in the image are in
`/usr/share/doc/storage-node/` inside it (`docker exec storage-node ls
/usr/share/doc/storage-node` lists them). `NOTICE` says what each program is
and where its source is. `THIRD-PARTY-NOTICES.txt` has the licences of the
code built into the node, including self_encryption, which the node links
under GPL-3.0 with a linking exception.
