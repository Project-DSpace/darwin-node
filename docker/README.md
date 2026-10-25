# Running storage nodes with Docker

This runs storage nodes for Darwin, our storage network, in one Docker
container. They earn storage fees in DARWIN, paid to your wallet.

## Requirements

- An x86-64 machine running Linux with
  [Docker Engine](https://docs.docker.com/engine/install/) and its Compose
  plugin, or Windows 11 with WSL 2 ([On Windows](#on-windows-wsl-2)). The image
  is x86-64 only; Macs and Docker Desktop aren't supported.
- A wallet address (`0x…`, any Ethereum-style address).
- Access to your router, to forward UDP ports.
- An IPv4 address. The network runs on IPv4 only; IPv6 alone won't do.
- At least 20 GB of disk per node (about 35 GB recommended). As a guide: a
  4-core CPU, 8 GB of RAM and 8 Mbps up and down.

## 1. Forward the ports

Each node uses two UDP ports: one for storage, one for web browsers. For 2
nodes:

- **Browser ports 11000–11001:** forward them by hand to the server's local IP
  (`hostname -I`), same numbers outside and inside. If you can't, set
  `BROWSER_PORT_START: 0`; the nodes still store and earn.
- **Storage ports 10000–10001:** the nodes map them by UPnP if your router has
  it on; otherwise forward them by hand. Never both: a manual rule makes the
  router give UPnP random ports.
- **Firewall:** open all four (`sudo ufw allow 10000:10001/udp` and
  `sudo ufw allow 11000:11001/udp`). A server with its own public IP needs
  only this, plus any cloud firewall.

## 2. Set up and start

```sh
mkdir ~/storage-node && cd ~/storage-node
curl -fsSLO https://raw.githubusercontent.com/Project-DSpace/darwin-node/main/docker/docker-compose.yml
```

Edit `docker-compose.yml`:

- `REWARDS_ADDRESS`: your wallet address, in place of `0xYOUR_WALLET_ADDRESS`.
- `NODE_COUNT`: 2, or 1 on a small machine. Each node's routing table takes at
  most 2 nodes from one IP in each bucket and among its 20 nearest, so more
  add load for less storage.
- Data folder: `./data`; the nodes can fill its disk
  ([When the disk fills](#when-the-disk-fills)). For another disk, change the
  `volumes` line, for example to `/mnt/storage/storage-node:/data`. Not on a
  `noexec` mount: the nodes run their program from it.
- Leave the rest. Change `PUID`/`PGID` only before the first start.

Run `docker compose up -d` (with `sudo` unless you're in the `docker` group).

## 3. Check it works

`docker compose logs -f` shows, on a first start (among `[manager]` lines):

```
Added node on UDP port 10000
Added node on UDP port 10001
Running 2 node(s) on UDP ports 10000-10001; browser access: UDP 11000-11001; storage fees go to 0x…
  ● Fleet health: Healthy
```

then each node as `● Running`, then the node logs, including
`[node-1] … Successfully connected to 3 bootstrap peers` (or fewer).

- With UPnP, each node logs `upnp: gateway mapping active`. If you left the
  storage ports to UPnP and none does, forward them by hand.
- A start-up error, such as `REWARDS_ADDRESS is not a wallet address`, repeats
  until you fix the file and rerun `docker compose up -d`.

## On Windows (WSL 2)

Use Docker Engine inside WSL 2, not Docker Desktop (quit it and stop it
starting at sign-in). Not yet tested by us. Needs Windows 11 22H2+ and WSL
2.5.4+ (check with `wsl --version`, update with `wsl --update`). Without WSL,
run `wsl --install --no-distribution` in PowerShell as administrator, then
restart.

1. In PowerShell, run `notepad $env:USERPROFILE\.wslconfig`, add these lines
   (into existing sections, if any), save, then run `wsl --shutdown`:

   ```ini
   [wsl2]
   networkingMode=mirrored

   [general]
   instanceIdleTimeout=-1
   ```

2. Install Ubuntu on a fixed-size disk, so the disk checks see real free space:
   about 35 GB per node plus room for Ubuntu (80GB fits 2 nodes). It takes
   that space at once; add `--location D:\WSL\storage-node` for another drive.

   ```powershell
   wsl --install Ubuntu-24.04 --name storage-node --fixed-vhd --vhd-size 80GB
   ```

3. In Ubuntu (`wsl -d storage-node`), check that `wslinfo --networking-mode`
   prints `mirrored` (if not, redo step 1). Install
   [Docker Engine](https://docs.docker.com/engine/install/ubuntu/), run
   `sudo usermod -aG docker $USER`, `exit`, then
   `wsl --terminate storage-node`.
4. Open the Hyper-V and Windows firewalls (as administrator):

   ```powershell
   New-NetFirewallHyperVRule -Name StorageNodes -DisplayName "Storage nodes" -Direction Inbound -VMCreatorId '{40E0AC32-46A5-438A-A0B2-2B479E8F2E90}' -Protocol UDP -LocalPorts 10000-10001,11000-11001 -Action Allow
   New-NetFirewallRule -Name StorageNodes -DisplayName "Storage nodes" -Direction Inbound -Protocol UDP -LocalPort 10000-10001,11000-11001 -Action Allow
   ```

5. [Forward the ports](#1-forward-the-ports) to the PC's IPv4 address
   (`ipconfig`), with the same UPnP rule. Skip `ufw`.
6. [Set up and start](#2-set-up-and-start) in your Ubuntu home folder, with
   the data folder off `/mnt/` (the Windows drives).
7. Stop sleep and hibernation (keep a laptop plugged in, lid set to Do
   nothing):

   ```powershell
   powercfg /change standby-timeout-ac 0
   powercfg /change hibernate-timeout-ac 0
   ```

8. Start the nodes at sign-in, and within 15 minutes whenever WSL stops (such
   as after a WSL update):

   ```powershell
   $a = New-ScheduledTaskAction -Execute "C:\Program Files\WSL\wslg.exe" -Argument "-d storage-node --shell-type none -- /bin/true"
   $t = @(
     New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
     New-ScheduledTaskTrigger -Once -At (Get-Date) -RepetitionInterval (New-TimeSpan -Minutes 15)
   )
   $s = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries
   Register-ScheduledTask -TaskName "storage-node" -Action $a -Trigger $t -Settings $s
   ```

   After a reboot, the nodes wait for you to sign in. Lock the PC rather than
   signing out.

Run the [Day to day](#day-to-day) commands in Ubuntu.

## Day to day

In the folder with `docker-compose.yml`:

- **Status:** `docker exec storage-node nodes status`, or
  `http://127.0.0.1:12600/console` on the server (from elsewhere:
  `ssh -L 12600:127.0.0.1:12600 you@server`).
- **Updates:** the nodes install signed releases themselves: each checks
  within minutes of starting and hourly after, and takes a release at its
  own point in the 24 hours after the release's publication (nodes up to
  ds.15: in the 24 hours after they first see it, starting again at a
  restart). The rest of the container, the node manager
  among it, changes only with a new image: `docker compose pull`, then
  `docker compose up -d`. Pull at least once if
  `docker exec storage-node darwin-nodes --version` fails: your image still
  runs Autonomi's node manager, which deletes a node's data when the disk is
  nearly full ([When the disk fills](#when-the-disk-fills)), and the nodes'
  own updates don't replace it.
- **Stop:** `docker compose down` (data stays); `docker compose up -d` starts
  again. Otherwise keep it running: nodes offline for long are dropped and
  stop earning. Docker restarts it after a reboot.
- **Change settings:** edit the file, run `docker compose up -d`.
  - More nodes take the next ports: forward and open those too.
  - Fewer nodes: the highest ports go first. Their data moves to `retired/`
    and is deleted at a container start 4 or more days later.
  - New `PORT_START` or `BROWSER_PORT_START`: nodes keep their data; update
    your router and firewall.
  - A new `REWARDS_ADDRESS` applies to every node.
- **Set-aside folders:** a node folder under `nodes/` that no registered
  node has moves to `unregistered/`, beside `retired/`, which no image
  deletes, the images before this one included. It may hold a node's
  identity and data: delete it yourself once you know it isn't needed.

## When the disk fills

- **Nodes keep every record while the disk has room.** Below 10% free, a node
  may erase records far from it that its nearer nodes prove they hold, until
  15% is free, farthest first. From ds.17 the reach grows as free space
  falls: records beyond 10 times the distance to the node's 7th-nearest peer
  below 10% free, 5 times below 7.5%, 2 times below 5%, and every record
  outside the node's own range below 2.5%. Each step also fires once free
  space is down to the full line plus 4 MiB, so on a small disk (under about
  40 GiB) they all run before the node is full. A record leaves a node's
  range only after 3 days out of it, and that wait survives restarts from
  ds.17. Nodes up to ds.16 stop at 10 times, and never erase while the
  network is small.
- **Free inodes count as free space** from ds.17: the smaller share of free
  bytes and free inodes drives the steps above, and below 2.5% free inodes a
  node is at the full line. Suggested for a disk of its own: XFS, or ext4
  made with `-i 4096`, so inodes don't run out before bytes.
- **At 1 GiB free or less**, `docker exec storage-node nodes status` warns,
  and at 500 MiB or less it reports the disk as critical. The node manager
  deletes no node's data, however little space is left.
- **At 1 GiB free**, nodes stop taking new data: they refuse to store a
  record that would leave less (nodes up to ds.15 did so at 500 MiB). From
  ds.17 they stop pricing new data there too, so no client pays them for a
  record they couldn't keep. Their logs give way there too: each node
  deletes its oldest log files first, then writes only warnings and errors
  into the room that frees, and nothing once none are left to delete; when
  the disk has room again it logs how many lines it dropped.
- **Full, not faulty** (from ds.17): a node at the line with nothing left it
  may erase is full. It keeps serving and proving what it holds, takes
  updates of the pointers it holds, refuses new records, and tells its
  peers, who stop charging it for the records it can't take and send them
  to the next nearest node instead. It logs `Node full: since ...` on
  entering and `Node full: no longer` on leaving, which it does once 256 MiB
  above the line is free again, and `nodes status` lists the full nodes.
  A restart (after adding disk, say) starts a node not full: with room it
  logs `Node full: not at start`, and `nodes status` stops listing it; at
  the line it is full again once a prune pass finds nothing it may erase.
  Nodes up to ds.16 keep charging a full node until they update. To store
  more, add disk or lower `NODE_COUNT`.
- **Logs:** each node's files in `logs/node-<id>/logs/` start anew at 50 MiB
  or each hour and take at most 500 MiB in all, the oldest deleted first.
- **Older images** (where `docker exec storage-node darwin-nodes --version`
  fails) run Autonomi's node manager, which at 500 MiB free evicts the node
  holding the least data (never the last one): it deletes that node's data
  and logs `Evicted node`. Pull the new image ([Updates](#day-to-day)). At
  its first start, nodes evicted before are dismissed (`Dismissing node-2,
  which an earlier node manager evicted ...`) and new nodes take their
  places up to `NODE_COUNT`. If that manager could not delete an evicted
  node's data, the line says so, and the folder is set aside in
  `unregistered/` ([Set-aside folders](#day-to-day)). `nodes clear-evicted`
  does nothing any more.

## Relaying

Each node runs a relay: anyone on the network can have your node send UDP, to
any address, from your IP (with UPnP, each relay session also opens a port on
your router). To turn it off, add `ANT_RELAY_SERVING: "false"` under
`environment:` and run `docker compose up -d`; each node then logs
`Relay serving is off`. Storing and earning don't change.

## Licences

In `/usr/share/doc/storage-node/` in the image (`docker exec storage-node ls
-R /usr/share/doc/storage-node` lists them). `NOTICE` says what each program
is and where its source is. `THIRD-PARTY-NOTICES.txt` has the licences of the
code built into the node, and `darwin-nodes/THIRD-PARTY-NOTICES.txt` those of
the node manager, including self_encryption, which both link under GPL-3.0
with a linking exception.
