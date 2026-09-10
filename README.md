# Recache

A lightweight, Redis-compatible in-memory data store built for Windows developers who want the Redis development experience without the complexity of setting up Redis through WSL, Docker, or other workarounds.

It speaks RESP2 over TCP, so existing Redis clients (`redis-cli`, StackExchange.Redis, RedisInsight, etc.) can talk to it directly.

```
                         ┌──────────────────────┐
                         │      redis-cli        │
                         │ StackExchange.Redis  │
                         │     Other clients     │
                         └──────────┬───────────┘
                                    │
                             TCP / RESP2
                                    │
                    ┌───────────────▼────────────────┐
                    │       Recache Server            │
                    │                                 │
                    │  Connection Manager             │
                    │       │                         │
                    │       ▼                         │
                    │  RESP Reader                    │
                    │       │                         │
                    │       ▼                         │
                    │  Command Dispatcher             │
                    │       │                         │
                    │       ▼                         │
                    │  Cluster Slot Gate (optional)   │
                    │  key → CRC16 slot → owner?       │
                    │  MOVED / ASK / CROSSSLOT         │
                    │       │                         │
                    │  ┌────┴────────────────────┐   │
                    │  │                         │   │
                    │  ▼                         ▼   │
                    │ String Commands        Hash/List│
                    │ GET/SET/DEL             Commands│
                    │                                 │
                    │       │                         │
                    │       ▼                         │
                    │     Cache Engine                │
                    │       │                         │
                    │  ┌────┼─────┬──────┐            │
                    │  ▼    ▼     ▼      ▼            │
                    │ TTL  LRU/LFU Memory Persistence │
                    │                                 │
                    │ Metrics / Health / Logging      │
                    └─────────────────────────────────┘
```

## Features

- **Data types**: String, Hash, List, Set, Sorted Set, and JSON (RedisJSON-compatible `JSON.*` commands with JSONPath support)
- **Expiration**: `TTL`, `PTTL`, `EXPIRE`, `PEXPIRE`, `PERSIST`
- **Auth**: password-based `AUTH`, matching real Redis's `NOAUTH`/`WRONGPASS` semantics
- **Transactions**: `MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH` — `EXEC` runs the whole queued batch under an exclusive lock for real cross-connection atomicity (this server isn't single-threaded like Redis, so that guarantee has to be built rather than assumed), and `WATCH` tracks real per-key versions rather than a coarse "anything changed" flag
- **RedisInsight-compatible**: `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN`, `INFO`, `CONFIG GET`, `CLIENT`, `MODULE LIST`
- **Cluster mode**: hash-slot sharding across statically-configured nodes — `CLUSTER` commands, `MOVED`/`ASK`/`CROSSSLOT`/`CLUSTERDOWN`, CRC16 slot hashing that matches real Redis key-for-key. See [Cluster mode](#cluster-mode) below for what's covered and what isn't yet.
- **Logging**: rolling daily log files with automatic retention, alongside console output
- Runs as a normal console app or as a Windows Service

---

## For users

**1. Download** the latest installer from [Releases](https://github.com/sajidur/recache/releases/latest) — `RecacheSetup.exe`.

**2. Run it.** It's self-signed, not signed by a commercial CA, so Windows SmartScreen and/or your antivirus may flag it as coming from an unrecognized publisher — click "More info" → "Run anyway" if prompted. The installer needs administrator rights (UAC prompt) because it registers a Windows Service.

That's it — Recache installs to `Program Files\Recache`, registers itself as the **Recache** Windows Service, starts automatically, and will keep running in the background (including across reboots) from then on.

**3. Set a real password.** The installer ships with a placeholder password. Edit `appsettings.yaml` in the install folder (`Password: "..."` under `Recache:`), then restart the service so it picks up the change:

```powershell
notepad "C:\Program Files\Recache\appsettings.yaml"
Restart-Service Recache
```

**4. Connect** with any Redis client, pointed at `localhost:6380` with that password — `redis-cli -p 6380 -a yourpassword`, RedisInsight, StackExchange.Redis, etc.

**Managing the service** (PowerShell, run elevated for start/stop):
```powershell
Get-Service Recache          # check status
Restart-Service Recache      # apply a config change
Stop-Service Recache
Start-Service Recache
```

**To uninstall**, use "Apps & Features" / "Programs and Features" in Windows — it cleanly stops and removes the service along with the files.

**Logs** live in a `logs\` folder next to the executable (`C:\Program Files\Recache\logs\` for an installed service), one file per day (`recache-20260826.log`), with old files deleted automatically once there are more than `LogRetentionDays` (default 7). Console output is unaffected — you still see logs there too when running interactively. Adjust in `appsettings.yaml`:

```yaml
Recache:
  EnableFileLogging: true
  LogDirectory: "logs"     # relative to the install folder, or an absolute path
  LogRetentionDays: 7
```

### Docker (Windows, macOS, or Linux)

Prefer a container instead of the Windows installer? The image is on [Docker Hub](https://hub.docker.com/r/sajidur/recache):

```bash
docker run -d --name recache -p 6380:6380 \
  -e Recache__RequireAuth=true \
  -e Recache__Password=yourpassword \
  sajidur/recache:latest
```

Or with Compose — grab [docker-compose.yml](docker-compose.yml), edit the password in it, then:

```bash
docker compose up -d
```

Configuration is via `Recache__<Property>` environment variables (`Recache__Port`, `Recache__MaxMemoryBytes`, etc.), or mount your own `appsettings.yaml` to `/app/appsettings.yaml` instead — see the comments in `docker-compose.yml`. Logs go to `/app/logs` inside the container; the Compose file mounts a named volume there so they survive container recreation.

---

## For developers

**Prerequisites**: [.NET 10 SDK](https://dotnet.microsoft.com/download).

**Clone and run:**
```powershell
git clone https://github.com/sajidur/recache.git
cd recache\recache
copy appsettings.example.yaml appsettings.yaml   # then set a real password
dotnet run
```

This starts an ordinary console app listening on `localhost:6380` — no service registration involved, `Ctrl+C` to stop.

### Project layout

```
recache/
  Commands/        RESP command dispatch (CommandDispatcher.cs)
  Storage/          Data types: CacheStore, RedisHash/List/Set/SortedSet/Json, JSONPath engine
  Protocol/         RESP2 wire format (reader/writer/value)
  Server/           TCP accept loop (RecacheServer, a BackgroundService) and per-connection handling
  Security/         AUTH / password validation, per-connection session state
  Configuration/    RecacheOptions bound from appsettings.yaml
  Clustering/       Hash-slot cluster mode: CRC16/slot math, ClusterState (topology + nodes.conf persistence), CLUSTER MEET's RESP-based bootstrap client
scripts/            install-service.ps1 / uninstall-service.ps1 — for local dev/test service installs
installer/          Inno Setup script (recache.iss) that builds the distributable installer
.github/workflows/  CI: builds and pushes the Docker image on version tags
Dockerfile, docker-compose.yml, .dockerignore
```

### Building a Release build

```powershell
cd recache
dotnet publish -c Release -o publish
```

### Installing as a Windows Service (for local testing)

From an **elevated** PowerShell prompt:
```powershell
.\scripts\install-service.ps1     # publishes + registers + starts the service
.\scripts\uninstall-service.ps1   # stops + removes it
```

### Building the distributable installer

Requires [Inno Setup 6](https://jrsoftware.org/isinfo.php).

```powershell
cd recache
dotnet publish -c Release -o publish
& "C:\Program Files (x86)\Inno Setup 6\ISCC.exe" ..\installer\recache.iss
```

Produces `installer\Output\RecacheSetup.exe`. It's unsigned by default — if you have a code-signing certificate, sign both `recache\publish\recache.exe` and the compiled installer with `signtool.exe` before distributing; without a certificate from a recognized CA, expect SmartScreen/AV false positives on other machines (self-signing only helps machines that explicitly import your certificate).

### Docker

```bash
docker build -t recache .
docker run -d -p 6380:6380 -e Recache__RequireAuth=true -e Recache__Password=devpass recache
```

The container needs no config file present — `RecacheOptions` has safe built-in defaults (auth disabled, port 6380), and `Recache__<Property>` environment variables override them. `.dockerignore` deliberately excludes `appsettings.yaml` so a real local password can never end up baked into an image layer.

**Publishing to Docker Hub** happens via [`.github/workflows/docker-publish.yml`](.github/workflows/docker-publish.yml) rather than a local build — GitHub's hosted runners have Docker built in, so this works even without Docker installed locally. It fires on any `v*.*.*` tag push (or manually via the Actions tab) and pushes `sajidur/recache:<version>` and `:latest`. One-time setup: add `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` (a Docker Hub access token, not your password — generate one at hub.docker.com/settings/security) as repo secrets under Settings → Secrets and variables → Actions.

### Benchmarking

`benchmark/` is a small load-test client (reuses recache's own RESP reader/writer, so results reflect the real protocol path). Run a Release build of recache, then:

```powershell
cd benchmark
dotnet run -c Release -- --host 127.0.0.1 --port 6380 --password yourpassword --connections 50 --requests 2000 --pipeline 1
```

Reports SET/GET/PING throughput and p50/p95/p99 latency. `--pipeline N` batches N requests per round trip before reading replies, for throughput-oriented testing.

### Cluster mode

recache can shard keys across multiple statically-configured instances using the same 16384-slot model as real Redis Cluster — `CLUSTER KEYSLOT` returns the exact same slot number real Redis would for any given key (CRC16/XMODEM, with `{hashtag}` support for co-locating related keys), and `redis-cli -c` / cluster-aware client libraries get real `MOVED`/`ASK` redirection.

Off by default (`Recache:ClusterEnabled: false` / `Recache__ClusterEnabled=false`). A standalone node's behavior is completely unaffected — the slot gate described below only exists in the dispatch path when cluster mode is on.

#### Architecture

Each recache process is still just one ordinary node — "cluster mode" doesn't change how a node stores data, only what happens *before* a command reaches storage:

1. **`ClusterKeyMap`** (`Clustering/ClusterKeyMap.cs`) — a static table of which argument positions are keys for every command (`GET key`, `MSET`-style multi-key spans, etc.), the same `first/last/step` convention Redis's own `COMMAND INFO` uses.
2. **`HashSlot.Compute(key)`** (`Clustering/HashSlot.cs`) — CRC16/XMODEM over the key bytes (or just the `{...}` hashtag substring, if present), masked to 0–16383.
3. **`ClusterState`** (`Clustering/ClusterState.cs`) — this node's view of the topology: a 16384-entry slot→node-id table, the set of known nodes, and this node's own id/epoch. Guarded by its own lock, independent of the `ReaderWriterLockSlim` that protects key-space reads/writes — a `CLUSTER ADDSLOTS` doesn't contend with concurrent `GET`s.
4. `CommandDispatcher.ExecuteAsync` runs the gate right after auth, before a command can queue into `MULTI` or reach a handler: compute the slot(s) for the command's keys, then:
   - keys span more than one slot → `-CROSSSLOT`
   - slot has no owner yet → `-CLUSTERDOWN`
   - a different node owns it → `-MOVED <slot> <host>:<port>` (or `-ASK` if the owner has marked the slot `MIGRATING` away and the key isn't present locally — see below)
   - this node owns it → the command runs exactly as it would standalone

There is no coordinator process and no consensus protocol. Each node's `ClusterState` is independently persisted and only ever changes because of a local `CLUSTER` command or a one-shot `CLUSTER MEET` import (next section) — see [Roadmap](#roadmap) for what a real gossip protocol would add.

#### Slot migration state machine

A slot is always in one of three states on the node that currently or formerly owned it:

| State | Set by | Effect |
|---|---|---|
| **STABLE** | default / `CLUSTER SETSLOT <slot> STABLE` | normal — only the owner serves it |
| **MIGRATING** | `CLUSTER SETSLOT <slot> MIGRATING <target-id>` (owner only) | owner still serves keys that exist locally; a request for a key that *doesn't* exist locally gets `-ASK <slot> <target-host>:<target-port>` instead of just creating it, so the client retries against the node it's moving to |
| **IMPORTING** | `CLUSTER SETSLOT <slot> IMPORTING <source-id>` (non-owner only) | the target node will accept commands for this slot, but only when the client sent `ASKING` immediately before — a bare request without `ASKING` still gets `-MOVED` back to the real owner |

`CLUSTER SETSLOT <slot> NODE <id>` finalizes a migration by reassigning ownership outright (clearing any in-progress state). Note that **recache doesn't move the key data itself** — there's no `MIGRATE` command yet, so moving a slot's keys is a manual `DUMP`/copy/`DEL` exercise (or just letting them land on the new owner naturally as new writes arrive) before calling `SETSLOT NODE` to finalize. `MIGRATING`/`IMPORTING` state is in-memory only and does not survive a restart (matching real Redis — a restart always resumes as `STABLE`).

#### `CLUSTER` command reference

| Command | What it does |
|---|---|
| `CLUSTER INFO` | Bulk-string status block (`cluster_enabled`, `cluster_state`, slot/epoch counters) |
| `CLUSTER MYID` | This node's 40-hex-char id |
| `CLUSTER NODES` | Full topology as newline-separated lines (see file format below) |
| `CLUSTER SLOTS` / `CLUSTER SHARDS` | Slot ranges with owning node address, in the nested-array shapes cluster-aware clients expect |
| `CLUSTER KEYSLOT <key>` | The slot a key hashes to |
| `CLUSTER COUNTKEYSINSLOT <slot>` / `GETKEYSINSLOT <slot> <count>` | Scans local keys for ones in a given slot (O(keys) — fine for a dev tool, not for huge keyspaces) |
| `CLUSTER ADDSLOTS <slot> [slot ...]` / `ADDSLOTSRANGE <start> <end> [...]` | Assigns slots to this node; errors if any requested slot already has an owner |
| `CLUSTER DELSLOTS` / `DELSLOTSRANGE` | Unassigns slots (also clears any in-flight migration state on them) |
| `CLUSTER SETSLOT <slot> MIGRATING\|IMPORTING\|STABLE\|NODE ...` | Drives the migration state machine above |
| `CLUSTER MEET <ip> <port>` | See below — pulls the peer's topology over ordinary RESP |
| `CLUSTER FORGET <id>` | Removes a node and frees any slots it owned |
| `CLUSTER REPLICATE <id>` | Marks this node as a replica of `<id>` (bookkeeping only today — see Roadmap) |
| `CLUSTER RESET [HARD]` | Clears slot ownership and known peers; `HARD` also discards this node's own id/epoch |
| `CLUSTER SET-CONFIG-EPOCH <n>` / `BUMPEPOCH` | Manual epoch control |
| `CLUSTER SAVECONFIG` | No-op — `nodes.conf` is written on every topology mutation already |
| `ASKING` | One-shot per-connection flag consumed by the next command, per the `IMPORTING` row above |

#### `CLUSTER MEET` and topology bootstrap

Real Redis Cluster nodes discover each other and keep their topology in sync via a binary gossip protocol over a second "cluster bus" port. recache doesn't have that yet, so `CLUSTER MEET <ip> <port>` does the next best thing: it opens an ordinary RESP connection to the peer (the same protocol any client uses), runs `CLUSTER NODES` against it, and merges the result into this node's own `ClusterState` — including any nodes *that peer* already knew about, so a full mesh only needs one `MEET` per pair, not per node. It's a real, working one-shot import, not a stub — but it's exactly that: one-shot. If topology changes on one node afterward (a new `ADDSLOTS`, a new peer), other nodes won't find out until someone runs `CLUSTER MEET` again.

#### `nodes.conf` file format

Each node persists its view of the cluster to `Recache:ClusterConfigFile` (default `nodes.conf`, resolved relative to the content root) on every topology change, and reloads it on startup so node identity and slot ownership survive restarts. One line per known node:

```
<node-id> <ip>:<port>@<cluster-bus-port> <flags> <master-id-or-"-"> <ping-sent> <pong-recv> <config-epoch> <link-state> [<slot-or-range> ...]
```

e.g.

```
a76782250e645b8d3162187a0339cd661a97d56e 127.0.0.1:6380@16380 myself,master - 0 0 0 connected 0-8191
2902dec4d2a5b4b0a8136cfcd3d8910bb3c5712f 127.0.0.1:6381@16381 master - 0 0 0 connected 8192-16383
```

This mirrors real Redis's `nodes.conf` shape closely enough to be readable by anyone who's seen the real thing, but it's recache's own format (no cluster-bus handshake data, no vector clocks) — it's not interchangeable with an actual Redis node's `nodes.conf`. `ping-sent`/`pong-recv`/`link-state` are placeholders today since there's no bus to actually ping over yet.

#### Roadmap

What's here now is real, working **static** multi-node sharding: manual slot assignment, correct `MOVED`/`ASK`/`CROSSSLOT` redirection, and a one-shot topology bootstrap via `CLUSTER MEET`. What's deliberately not built yet:

- **Gossip / cluster bus** — a second listener per node exchanging periodic PING/PONG topology + health gossip, so `ADDSLOTS`/`MEET`/node failures propagate automatically instead of needing another manual `CLUSTER MEET`.
- **Replication** — `CLUSTER REPLICATE` currently only flips a local role flag; there's no actual command stream from master to replica yet.
- **Automatic failover** — no failure detection (`PFAIL`/`FAIL`), no quorum voting, no automatic replica promotion. `CLUSTER FAILOVER` isn't implemented.

**Bring-up** (two nodes on localhost):

```powershell
# Terminal 1
$env:Recache__ClusterEnabled = "true"
$env:Recache__Port = "6380"
$env:Recache__ClusterConfigFile = "node1-nodes.conf"
dotnet run

# Terminal 2
$env:Recache__ClusterEnabled = "true"
$env:Recache__Port = "6381"
$env:Recache__ClusterConfigFile = "node2-nodes.conf"
dotnet run
```

Then, against each node (e.g. with `redis-cli -p 6380` / `-p 6381`):

```
127.0.0.1:6380> CLUSTER ADDSLOTSRANGE 0 8191
127.0.0.1:6381> CLUSTER ADDSLOTSRANGE 8192 16383
127.0.0.1:6380> CLUSTER MEET 127.0.0.1 6381
127.0.0.1:6381> CLUSTER MEET 127.0.0.1 6380
```

From here, `redis-cli -c -p 6380 SET somekey value` transparently follows any `MOVED` reply to the node that actually owns the key's slot. `CLUSTER NODES` on either node shows the full two-node topology.

If either node isn't reachable at the address it was started with (containers, NAT, multiple NICs), set `Recache__ClusterAnnounceIp`/`Recache__ClusterAnnouncePort` so `MOVED`/`CLUSTER NODES` advertise an address peers can actually reach.

### License

Apache License 2.0 — see [LICENSE](LICENSE).
