# Running storage nodes on Unraid

This sets up storage nodes for Darwin, our storage network, in a Docker
container on your Unraid server. Inside the container, a node manager runs and
looks after the nodes. It takes about 15 minutes. You'll need:

- A **GitHub account** (free), to download the private image.
- A **wallet address** for your storage fees: any Ethereum-style address
  starting with `0x`, for example from MetaMask.
- **Access to your router**, to forward some UDP ports.
- **Spare disk space**: at least 20 GB per node (about 35 GB recommended), plus
  for each node some memory, CPU and part of your internet connection.
  As a guide, a node computer should have a 4-core CPU, 8 GB of RAM and 8 Mbps
  up and down.

We're in a test phase: fees are paid in a test token with no real value yet.

## 1. Check your internet connection can host nodes

Nodes must be reachable from the internet. Compare the WAN/Internet IP shown on
your router's status page with what [whatismyip.com](https://www.whatismyip.com)
shows. If they're **different**, your provider uses "carrier-grade NAT" and port
forwarding won't work; tell us before going further.

## 2. Get access to the image

1. Send your GitHub username to the person who gave you this guide. They'll
   give your account read access to the image.
2. On GitHub, go to **Settings → Developer settings → Personal access tokens →
   Tokens (classic) → Generate new token (classic)**. Tick only
   **`read:packages`**, set an expiry (for example 1 year), generate it and copy
   the token.

## 3. Log in to the image registry from Unraid

In the Unraid web interface, open the terminal (the `>_` icon, top right) and run,
with your GitHub username:

```
docker login ghcr.io -u YOUR_GITHUB_USERNAME
```

Paste the token when asked for a password. You should see `Login Succeeded`.

Unraid forgets this login when it reboots. To keep it, run once:

```
mkdir -p /boot/config/docker-auth && cp /root/.docker/config.json /boot/config/docker-auth/
echo 'mkdir -p /root/.docker && cp /boot/config/docker-auth/config.json /root/.docker/' >> /boot/config/go
```

(Running nodes keep working after a reboot either way; the login only matters
for downloading updates.)

## 4. Forward the ports on your router

Each node uses two UDP ports: one for the storage network and one for web
browsers fetching files. With 2 nodes, forward **UDP 10000–10001** and **UDP
11000–11001** to your Unraid server's local IP address (with 6 nodes,
10000–10005 and 11000–11005). The exact menu depends
on your router; it is usually called "Port forwarding" or "Virtual servers".
Use the same port numbers outside and inside.

Use either manual forwarding or your router's automatic forwarding (UPnP) for
the storage ports, not both: the nodes ask UPnP-capable routers for those
ports themselves, and a manual rule on the same ports makes the router hand
them random ones instead. If your router has UPnP on, forward only the browser
ports by hand.

If you can't forward the browser ports, set **First browser UDP port** to `0`: the nodes then store and earn as
usual, but don't serve web browsers.

## 5. Add the container

1. Copy `unraid-storage-node.xml` (sent with this guide) to your Unraid flash
   drive as `config/plugins/dockerMan/templates-user/my-storage-node.xml`. Over
   the network, the flash drive is the `flash` share.
2. In Unraid, go to **Docker → Add Container**, and choose **storage-node** from
   the Template list.
3. Fill in:
   - **Rewards wallet**: your `0x…` address.
   - **Number of nodes**: start with one or two, and add more later as your
     disk, memory and connection allow (6 at most for now). Each node needs at
     least 20 GB of free disk space.
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
`--ulimit nofile=65536:65536 --stop-timeout 120` (stopping the nodes cleanly
can take a minute or two).

## 6. Check it's working

On the Docker tab, click the container's icon and choose **Logs**. Within a
minute you should see:

```
Added node on UDP port 10000
Added node on UDP port 10001
Running 2 node(s) on UDP ports 10000-10001; browser access: UDP 11000-11001; storage fees go to 0x…
[node-1] … Successfully connected to 2 bootstrap peers
```

On the Docker tab the container's network should show as **host**, with the IP
and port columns blank. If it says **bridge**, edit the container and set
Network Type to **Host**: in bridge mode nothing from the internet can reach
the nodes.

Then send your public IP address to the person who gave you this guide, so they
can confirm from the network side that your nodes are reachable.

## Day to day

- **Updates**: on the Docker tab, click **Check for Updates**, then **Apply
  Update** when one is available.
- **Keep it running**: nodes that are offline for long get dropped by the
  network and stop earning. Restarts and reboots are fine.
- **Stopping**: stop the container on the Docker tab. Your node data stays in the
  data folder.
- **Status**: in the Unraid terminal, `docker exec storage-node nodes status`
  lists your nodes (use your container's name if it isn't `storage-node`).
- **More or fewer nodes**: edit the container, change **Number of nodes** and
  click **Apply**. Fewer nodes removes any the disk-full check removed first
  (see below), then the ones on the highest ports. Removed nodes' data is kept
  in `retired/` inside the data folder for 3 days, in case you change your
  mind, then deleted.
- **Different ports**: change **First UDP port** or **First browser UDP port**
  and click **Apply**. The nodes move to the new ports and keep their
  identities; change your router's port forwarding to match.
- **When the disk gets full**: the node manager watches free space where the
  data folder is. Below 1 GB free it warns in the log; below 500 MB it
  removes the node holding the least data (never the last one) and
  deletes that node's data, so the others have room. Removed nodes stay
  removed: free up space, run `docker exec storage-node nodes clear-evicted`,
  then restart the container to replace them, or lower **Number of nodes**.
- **No IPv6?** If the logs show connection problems and your connection has no
  working IPv6, set **IPv4 only** to `true` (under "Show more settings").

## Upgrading from the first version

Your nodes keep their identities and data: the container takes over the old
`node-<port>` folders on its first start. After **Apply Update**, edit the
container and:

- remove the **Storage limit** variable (nodes now use the free space on the
  disk);
- set **Number of nodes** to match your disk, memory and connection;
- add the browser port variable `BROWSER_PORT_START` = `11000` and forward
  those ports, or leave it out to keep browser access off;
- change **Extra Parameters** to
  `--ulimit nofile=65536:65536 --stop-timeout 120`.

