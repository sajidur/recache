# Recache

A lightweight, Redis-compatible in-memory data store built for Windows developers who want the Redis development experience without the complexity of setting up Redis through WSL, Docker, or other workarounds — and that scales out to a replicated, self-healing cluster when you need one.

It speaks RESP2 over TCP, so existing Redis clients (`redis-cli`, StackExchange.Redis, redis-py, RedisInsight, etc.) can talk to it directly.

```
   clients (redis-cli, StackExchange.Redis, redis-py, RedisInsight, ...)
                               │  TCP / RESP2
┌──────────────────────────────▼───────────────────────────────────────┐
│ Recache node                                                          │
│                                                                       │
│  Connection loop: read what arrived → parse every whole command →     │
│  execute → answer the whole batch with one send                       │
│                               │                                       │
│  Command dispatcher ── cluster slot gate (MOVED / ASK / CROSSSLOT)    │
│                               │                                       │
│  Key locks (striped, shared for reads / exclusive for writes)         │
│                               │                                       │
│  Store: 1024-shard keyspace, strings/hashes/lists/sets/zsets/JSON     │
│                               │ every write, in apply order           │
│                               ▼                                       │
│  Replication stream (offset-addressed backlog)                        │
│      ├──► AOF writer thread ──► appendonly.aof (fsync policy)         │
│      └──► replica senders  ──► replicas (PSYNC full / partial)        │
│                                                                       │
│  Cluster bus (port + 10000): gossip, failure detection, elections     │
└───────────────────────────────────────────────────────────────────────┘
```

## Features

- **Data types**: String, Hash, List, Set, Sorted Set, and JSON (RedisJSON-compatible `JSON.*` commands with JSONPath support)
- **Expiration**: `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT`, `TTL`/`PTTL`/`EXPIRETIME`/`PEXPIRETIME`, `PERSIST`, `SET ... EX|PX|EXAT|PXAT|KEEPTTL` — lazy expiry on access plus an active background cycle, so expired keys are reclaimed even if never read again
- **Persistence**: an append-only file with `always` / `everysec` / `no` fsync, crash-safe replay (a torn final write is repaired automatically), and non-blocking background rewrite (compaction)
- **Replication**: asynchronous master → replica streaming with full and partial resync, PSYNC2-style history so replicas keep partial-resyncing across failovers, read-only replicas, chained replicas, and `WAIT` for synchronous confirmation when you need it
- **Cluster mode**: 16384-slot sharding that matches real Redis key-for-key, a gossip bus for membership and failure detection, **automatic failover**, **zero-downtime planned failover** (`CLUSTER FAILOVER`, and on every clean shutdown of a master), and live resharding with `MIGRATE`
- **Transactions**: `MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH` — `EXEC` holds the locks of every key it touches for its whole duration, so no other client can see a transaction half-applied
- **Auth**: password-based `AUTH`, matching real Redis's `NOAUTH`/`WRONGPASS` semantics
- **RedisInsight-compatible**: `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN`, `INFO`, `CONFIG GET`, `CLIENT`, `COMMAND`, `MODULE LIST`
- **Logging**: rolling daily log files (millisecond timestamps) with automatic retention, alongside console output
- Runs as a normal console app, as a Windows Service, or in a container

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

**Logs** live in a `logs\` folder next to the executable (`C:\Program Files\Recache\logs\` for an installed service), one file per day (`recache-20260826.log`), with old files deleted automatically once there are more than `LogRetentionDays` (default 7).

**Data** (the append-only file, when `EnableAof` is on) lives in `data\` next to the executable by default — `C:\Program Files\Recache\data\appendonly.aof` for an installed service. Set `AofPath` to put it elsewhere.

### Configuration

Settings live under `Recache:` in `appsettings.yaml` (see [`appsettings.example.yaml`](recache/appsettings.example.yaml) for a commented copy). Every setting can also be given as an environment variable (`Recache__Port=6381`) or on the command line (`--Recache:Port=6381`); those take precedence over the file, in that order.

| Setting | Default | |
|---|---|---|
| `Host`, `Port` | `0.0.0.0`, `6380` | Where clients connect |
| `RequireAuth`, `Password` | `false`, — | Password authentication |
| `MaxConnections`, `MaxRequestBytes` | `10000`, 16 MB | Limits |
| `EnableAof` | `false` | Persist writes to the append-only file |
| `AofPath` | `data/appendonly.aof` | Relative paths are next to the executable |
| `AofFsync` | `everysec` | `always` (never lose an acknowledged write), `everysec` (≤ ~1 s lost on power loss), `no` |
| `AofRewritePercentage`, `AofRewriteMinBytes` | `100`, 64 MB | Automatic rewrite once the file doubles (0 = off) |
| `AofLoadTruncated` | `true` | Repair a torn final write on startup instead of refusing to start |
| `ReplicaOf` | — | `"host port"`: start as a replica (standalone mode) |
| `MasterAuth` | `Password` | Password presented to the master |
| `ReplicationBacklogBytes` | 64 MB | Recent history kept for partial resyncs |
| `ClusterEnabled` | `false` | Hash-slot cluster mode |
| `ClusterConfigFile` | `nodes.conf` | This node's identity, topology and vote history |
| `ClusterAnnounceIp`, `ClusterAnnouncePort` | learned / `Port` | The address peers and clients use for this node |
| `ClusterBusPort` | `Port + 10000` | Node-to-node bus |
| `ClusterNodeTimeoutMs` | `5000` | Failure detection; automatic failover takes ~1–2× this |
| `ClusterFailoverOnShutdown` | `true` | A stopping master hands its slots to a replica first |
| `ClusterRequireFullCoverage` | `true` | Refuse key commands while any slot is unserved |
| `ClusterAllowReadsWhenDown` | `false` | Keep serving reads while the cluster is down |
| `ClusterReplicaNoFailover` | `false` | Never start an automatic election from this replica |

### Docker (Windows, macOS, or Linux)

Prefer a container instead of the Windows installer? The image is on [Docker Hub](https://hub.docker.com/r/sajidur/recache):

```bash
docker run -d --name recache -p 6380:6380 \
  -e Recache__RequireAuth=true \
  -e Recache__Password=yourpassword \
  -e Recache__EnableAof=true \
  -v recache-data:/data \
  sajidur/recache:latest
```

Or with Compose — grab [docker-compose.yml](docker-compose.yml), edit the password in it, then `docker compose up -d`.

Everything that must survive a restart (the AOF, and in cluster mode `nodes.conf`) is written to `/data` inside the container — mount a volume there. Logs go to `/app/logs`.

---

## Persistence (append-only file)

With `EnableAof: true`, every write is appended to the AOF and replayed on startup.

**How it works.** Commands never touch the file themselves. Every write is appended, in the exact order it was applied, to an in-memory replication stream; a dedicated thread drains that stream into the file. Writes are logged in deterministic form — anything relative to "now" (`EXPIRE k 10`, `SET k v EX 10`, `SETEX`) is rewritten with the absolute instant that was actually stored (`PEXPIREAT`, `SET ... PXAT`), and keys that expire are logged as `DEL`. Replaying the file therefore reproduces the dataset exactly, TTLs included, no matter when it's replayed.

**Durability (`AofFsync`):**

- `always` — a write's reply is sent only after an fsync covers it. Concurrent writers, and every write in a pipelined batch, share one fsync (group commit), so this costs latency, not one fsync per command. No acknowledged write is lost, even on power failure.
- `everysec` (default, same as Redis) — a write's reply is sent once the write has been handed to the OS, and fsync runs once a second in the background, so no client waits on the disk. A crash of the recache process loses nothing that was acknowledged; a power failure can lose up to about a second.
- `no` — like `everysec`, but flushing to disk is left to the OS.

**Crash recovery.** A crash can leave a half-written command, or a `MULTI` block without its `EXEC`, at the end of the file. On startup that tail is detected, dropped and truncated away (logged as a warning), and everything before it is loaded. Corruption anywhere else stops startup with the exact byte offset, rather than silently skipping data. Set `AofLoadTruncated: false` to refuse to start on a torn tail as well.

**Rewrite (compaction).** `BGREWRITEAOF`, or automatically once the file has doubled since the last rewrite (and is at least `AofRewriteMinBytes`). It doesn't block anything: a point-in-time snapshot of the dataset becomes the new file's base, everything written since the snapshot is appended after it, and the new file then replaces the old one with an atomic rename. The old file stays complete and valid until that instant. Snapshots don't use `fork()`; each key is copied the first time a writer is about to change it (or by a background pass, whichever comes first), under that key's lock — so the snapshot is exactly the dataset at one point in the stream.

`INFO persistence` reports `aof_enabled`, `aof_rewrite_in_progress`, `aof_last_bgrewrite_status`, `aof_current_size`, `aof_base_size`, `aof_last_write_status` and more.

## Replication

```
replica> REPLICAOF 10.0.0.5 6380        # or Recache:ReplicaOf: "10.0.0.5 6380"
replica> REPLICAOF NO ONE               # promote back to a master
```

A replica connects to its master, sends `PSYNC <replid> <offset>` and either:

- **continues** from where it left off (partial resync) if the master still has that part of its history in its replication backlog (`ReplicationBacklogBytes`, 64 MB by default) — the normal case after a dropped connection or network blip, or after a failover to a sibling replica; or
- **fully resyncs** — a new replica, one that fell too far behind, or one whose master restarted (a restarted master begins a new history): the master takes a non-blocking snapshot and streams it; the replica loads it into a separate store while it keeps serving its old data, then swaps the new dataset in all at once.

After that it applies the master's write stream and appends the same bytes to its own stream, so its replication offsets stay identical to its master's. That's what lets other replicas continue with a partial resync after a failover (the promoted replica keeps the old master's history as its secondary id, like Redis's PSYNC2), and lets a replica serve replicas of its own.

Replicas are read-only (`-READONLY` for writes), never expire keys on their own clock (the master's `DEL` does — so both stay identical regardless of clock skew), and report `REPLCONF ACK <offset>` every second.

**Asynchronous by default, synchronous on request.** Like Redis, a write is acknowledged once applied on the master; replicas follow typically within a millisecond. A client that needs a write on N replicas before it continues uses `WAIT N timeout_ms`, which returns how many replicas confirmed it.

`ROLE` and `INFO replication` (`role`, `connected_slaves`, `master_link_status`, `master_repl_offset`, `master_replid`/`master_replid2`, backlog stats) report the state. `CLIENT KILL TYPE master|replica` drops replication links (they reconnect by themselves).

## Cluster mode

Cluster mode shards keys across nodes using the same 16384-slot model as Redis Cluster — `CLUSTER KEYSLOT` returns exactly the slot real Redis would (CRC16/XMODEM, with `{hashtag}` support to co-locate related keys) — and keeps the cluster available through node failures.

Off by default (`ClusterEnabled: false`); a standalone node's behaviour is unaffected.

### Bringing up a cluster

Six nodes: three masters with one replica each (automatic failover needs a majority of masters, so use at least three). On each node set `ClusterEnabled: true`, a distinct `Port` (and `ClusterConfigFile` if they share a folder), and `ClusterAnnounceIp` if peers can't reach it at the address it binds to (containers, NAT, multiple NICs).

```powershell
# One terminal per node: ports 7001..7006 (bus ports 17001..17006)
dotnet run --project recache -- --Recache:ClusterEnabled=true --Recache:Port=7001 `
    --Recache:ClusterConfigFile=node-7001.conf --Recache:EnableAof=true `
    --Recache:AofPath=data-7001/appendonly.aof
```

Then, with `redis-cli`:

```
redis-cli -p 7001 CLUSTER ADDSLOTSRANGE 0 5460
redis-cli -p 7002 CLUSTER ADDSLOTSRANGE 5461 10922
redis-cli -p 7003 CLUSTER ADDSLOTSRANGE 10923 16383

# One node meets the others; gossip introduces everyone to everyone.
redis-cli -p 7001 CLUSTER MEET 127.0.0.1 7002
redis-cli -p 7001 CLUSTER MEET 127.0.0.1 7003
redis-cli -p 7001 CLUSTER MEET 127.0.0.1 7004
redis-cli -p 7001 CLUSTER MEET 127.0.0.1 7005
redis-cli -p 7001 CLUSTER MEET 127.0.0.1 7006

# Attach replicas (ids from CLUSTER MYID / CLUSTER NODES).
redis-cli -p 7004 CLUSTER REPLICATE <id of 7001>
redis-cli -p 7005 CLUSTER REPLICATE <id of 7002>
redis-cli -p 7006 CLUSTER REPLICATE <id of 7003>
```

`CLUSTER INFO` shows `cluster_state:ok` on every node once gossip has converged (typically within a second). Cluster-aware clients (`redis-cli -c`, StackExchange.Redis, redis-py `RedisCluster`, Lettuce, ...) take it from there — routing by slot, following `MOVED`/`ASK`, and re-learning the topology after a failover.

### How it works

- **Routing.** Every key-bearing command's keys are hashed to a slot; keys in different slots → `-CROSSSLOT`, a slot owned elsewhere → `-MOVED <slot> <host>:<port>`, a slot being migrated away whose key already moved → `-ASK`. A replica answers reads for its master's slots itself after `READONLY`.
- **Gossip.** Each node pings every peer over the cluster bus (`Port + 10000`) at least once a second. Every message carries the sender's full view of itself — role, epochs, replication offset, a slot bitmap — plus gossip about other nodes, so new nodes, slot changes and failure reports spread without a full mesh of `MEET`s.
- **Failure detection.** A node that misses PONGs for `ClusterNodeTimeoutMs` is suspected (`fail?`); once a majority of slot-owning masters report it, it's marked `fail` cluster-wide.
- **Automatic failover.** A replica of a failed master waits a short, randomized, rank-based delay (the replica with the most data goes first), asks the masters for votes in a new epoch, and with a majority promotes itself, claiming its old master's slots with that higher *config epoch*. A slot always belongs to whichever claimant has the highest config epoch, so every node — including the old master if it comes back, and its other replicas — converges on the new owner by itself: the old master turns into a replica of its replacement, and the other replicas re-point at it (normally with a partial resync).
- **Split-brain protection.** A master that can't reach a majority of masters stops accepting key commands (`-CLUSTERDOWN`) — it may be on the minority side of a partition, where a replica on the majority side is about to replace it. A restarted master also holds off for a moment so gossip can tell it whether it was replaced while it was down.

Each node persists its id, topology, epochs and last vote to `ClusterConfigFile` (`nodes.conf`, written atomically) on every change, so identity and election safety survive restarts.

### Zero-downtime operations

**Planned failover — `CLUSTER FAILOVER`** (sent to a replica): the master pauses writes (they wait; they aren't failed), the replica catches up to the master's exact final replication offset, wins an election and takes over the slots; the paused writes are then re-checked, find the new owner, and get `MOVED` there. No acknowledged write is lost and clients see no errors. In the test suite this handover takes ~70–140 ms under continuous write load. `CLUSTER FAILOVER FORCE` skips the handshake with the master (use when it's down); `CLUSTER FAILOVER TAKEOVER` skips the election too (last resort, e.g. no master majority reachable).

**Rolling restarts and upgrades.** With `ClusterFailoverOnShutdown: true` (default), stopping a master cleanly — `Stop-Service Recache`, Ctrl+C, `docker stop`, `SHUTDOWN` — first hands its slots to its most up-to-date replica with exactly the handover above, then exits. Restart it and it rejoins as a replica. So: restart each node in turn, and the cluster stays fully available throughout.

**Live resharding.** Move a slot with the same sequence `redis-cli --cluster reshard` uses: `CLUSTER SETSLOT <slot> IMPORTING <src>` on the target, `CLUSTER SETSLOT <slot> MIGRATING <dst>` on the source, `CLUSTER GETKEYSINSLOT` + `MIGRATE ... KEYS ...` until the slot is empty (clients are sent to the target with `-ASK` for keys that already moved), then `CLUSTER SETSLOT <slot> NODE <dst>`. Other nodes learn the new owner through gossip.

**Unplanned failure.** A crashed master's replica takes over automatically after roughly `ClusterNodeTimeoutMs` plus 0.5–1.5 s (~2.5 s with a 1.5 s timeout in the tests). Writes acknowledged before the crash but not yet replicated can be lost, as with Redis — use `WAIT 1 <timeout>` for writes that must survive one.

### `CLUSTER` command reference

| Command | What it does |
|---|---|
| `CLUSTER INFO` / `MYID` / `MYSHARDID` | Status (`cluster_state`, slot and epoch counters), this node's id |
| `CLUSTER NODES` / `SLOTS` / `SHARDS` / `REPLICAS <id>` | Topology, in the shapes cluster-aware clients expect (replicas included) |
| `CLUSTER KEYSLOT <key>` | The slot a key hashes to |
| `CLUSTER COUNTKEYSINSLOT <slot>` / `GETKEYSINSLOT <slot> <count>` | Keys in a slot (indexed per slot in cluster mode) |
| `CLUSTER ADDSLOTS` / `ADDSLOTSRANGE` / `DELSLOTS` / `DELSLOTSRANGE` / `FLUSHSLOTS` | Slot assignment |
| `CLUSTER SETSLOT <slot> MIGRATING\|IMPORTING\|STABLE\|NODE ...` | Slot migration state machine |
| `CLUSTER MEET <ip> <port> [bus-port]` | Introduce a node (asynchronous; gossip spreads it) |
| `CLUSTER FORGET <id>` | Remove a node (not re-added by gossip for 60 s) |
| `CLUSTER REPLICATE <id>` | Become a replica of a master and start replicating |
| `CLUSTER FAILOVER [FORCE\|TAKEOVER]` | Planned failover, sent to a replica |
| `CLUSTER RESET [HARD\|SOFT]`, `SET-CONFIG-EPOCH`, `BUMPEPOCH`, `COUNT-FAILURE-REPORTS`, `SAVECONFIG` | Maintenance |
| `ASKING`, `READONLY`, `READWRITE` | Per-connection routing flags |

### Compatibility notes

- The cluster *bus* protocol is recache's own (RESP-encoded), so recache and Redis nodes can't be mixed in one cluster; clients don't care.
- `DUMP` payloads are recache's own format (not RDB), so `MIGRATE`/`RESTORE` work between recache nodes.
- Only database 0 exists (as in Redis Cluster).

---

## Performance

What makes it fast:

- **Batched I/O.** A connection reads whatever the socket has, executes every complete command in it, and answers the whole batch with one send — instead of one awaited write (one TCP packet) per reply and one awaited read per byte.
- **Allocation-light parsing** straight from the receive buffer; command names are looked up without allocating; common replies are shared.
- **Multi-core execution.** Commands run in parallel across cores; striped reader/writer key locks (4096 stripes) keep per-key operations atomic, and the replication stream is appended under the same lock, so the AOF and replicas see each key's writes in the order they were applied.
- **No stop-the-world pauses from the data structures.** The keyspace is split into 1024 independently growing shards (a single hash table doubling at a million keys froze all writers for hundreds of milliseconds); entries are updated in place; `SCAN` pages through one shard at a time.
- **GC tuned for a cache.** Server GC (collections run on all cores), with .NET's adaptive heap sizing (DATAS) turned off: DATAS keeps young-generation budgets small to save memory, so a write-heavy load collects constantly, and every collection also re-scans the long-lived keyspace — on 2M inserts that was 27–33 collections and 5–6 s of total pause versus 1–3 collections and ~0.3–0.6 s without it. Full collections run in the background (`SustainedLowLatency`). `INFO gc` reports collection counts, total pause and the longest pause. To trade throughput back for a smaller memory footprint, set `DOTNET_GCDynamicAdaptationMode=1`.
- **Inline socket completions on Linux** — request handling runs on the epoll threads instead of hopping to the thread pool.

### Compared with Redis 7.4

`redis-benchmark` (the official C client, 4 threads) against Redis 7.4 (default, single-threaded), Redis 7.4 with `--io-threads 4 --io-threads-do-reads yes`, and recache, all in Docker on one 12-thread laptop (Intel i7-1355U, Docker Desktop/WSL2), persistence off, 1M-key random keyspace. Each suite runs against all three servers back to back, twice, in alternating order; numbers are the average of the two rounds (requests/second).

| Test | Redis | Redis io-threads | recache | recache ÷ Redis |
|---|---|---|---|---|
| **50 clients, no pipelining** | | | | |
| SET | 65.2K | 82.8K | 59.9K | 0.92× |
| GET | 60.5K | 77.4K | 61.5K | 1.02× |
| INCR | 65.2K | 69.9K | 68.5K | 1.05× |
| MSET (10 keys) | 46.1K | 39.3K | 52.1K | 1.13× |
| HSET / LPUSH / SADD (one shared key) | 65.3K / 70.6K / 70.5K | 72.8K / 66.6K / 80.0K | 61.4K / 53.5K / 52.9K | 0.94× / 0.76× / 0.75× |
| LRANGE (100 elements) | 49.5K | 51.0K | 40.6K | 0.82× |
| **50 clients, pipeline 16** | | | | |
| SET | 482K | 414K | 572K | 1.19× |
| GET | 585K | 489K | 706K | 1.21× |
| INCR | 615K | 470K | 648K | 1.05× |
| MSET (10 keys) | 121K | 81K | 89K | 0.73× |
| LPUSH / HSET (one shared key) | 774K / 546K | 648K / 414K | 545K / 212K | 0.70× / 0.39× |
| **200 clients, no pipelining** | | | | |
| SET / GET | 65.6K / 66.6K | 65.6K / 93.9K | 72.4K / 82.1K | 1.10× / 1.23× |
| **1 client** | | | | |
| SET / GET (p50 latency) | 0.055 / 0.055 ms | 0.055 / 0.063 ms | 0.063 / 0.059 ms | ~parity |

What this shows:

- recache is **faster than single-threaded Redis on random-key reads and writes under concurrency** — pipelined SET/GET by ~20%, 200 clients by 10–23% — and on multi-key MSET without pipelining. Single-client latency is at parity.
- It is **slower when every client hammers the same single key** (one list, hash or set): Redis runs those serially on one hot core, while recache's threads pass one lock between cores. Pipelined HSET to one hash is the worst case (0.39×).
- **Latency: better median, worse tail.** With 50 clients recache's median latency is lower than Redis's (0.33–0.44 ms vs 0.57–0.92 ms) but its p99 is higher (2.9–4.7 ms vs 1.5–3.2 ms). Under heavy pipelined writes the single slowest request was 0.7–1.3 s — a .NET garbage collection promoting a large batch of newly inserted objects — versus ≤ 40 ms for Redis. Read-heavy and update-in-place workloads rarely trigger that. `INFO gc` shows the longest pause so far.
- Redis with I/O threads beats both on unpipelined small requests on this machine.

These were measured on a laptop with background load, so treat them as relative, not absolute. To reproduce on your own hardware: `benchmark/compare-with-redis.sh` (needs Docker; builds recache from this checkout, runs the same interleaved suites, prints the table).

Also measured: against the previous version of recache, pipelined SET/GET went from ~91K/97K to 435K/549K requests/second (same machine and client); the engine alone (in-process, 8 threads) serves GET at 2.2M ops/s with p99 4.4 µs.

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

This starts an ordinary console app listening on `localhost:6380` — no service registration involved, `Ctrl+C` to stop (which runs the same graceful shutdown as a service stop).

### Tests

```powershell
dotnet test tests/recache.Tests
```

Unit tests cover the RESP parser, slot hashing, the replication backlog, `DUMP` payloads, and a snapshot-consistency property (a snapshot taken under concurrent non-idempotent writes, plus the stream from its offset, rebuilds the live dataset exactly). Integration tests start real server processes — so they can hard-kill them — and cover: AOF recovery of every data type after a kill (TTLs to the millisecond), torn-write repair, rewrite under load; full/partial resync, PSYNC2 continuity after promotion, chained replicas, `WAIT`, expiry propagation; gossip-formed clusters, replica reads, live resharding with `MIGRATE`/`ASK`; and failover: `CLUSTER FAILOVER` and a graceful master shutdown under continuous write load (asserting zero client errors and zero lost acknowledged writes), and automatic failover after a crash with the old master rejoining as a replica.

### Project layout

```
recache/
  Commands/        CommandDispatcher (partial classes: core execution, data types, keys/DUMP/MIGRATE,
                   server/admin, replication, cluster) and CommandTable (flags + key positions)
  Storage/         CacheStore (sharded keyspace), data types, KeyLocks, ExpirationManager, DumpSerializer
  Persistence/     AofManager (writer thread, fsync, rewrite), AofLoader, SnapshotService
  Replication/     ReplicationBacklog (the write stream), ReplicationManager (roles, PSYNC, WAIT), ReplicaLink
  Clustering/      ClusterState (topology + nodes.conf), ClusterManager (gossip, failure detection,
                   failover), ClusterLink/ClusterMessage (the bus), HashSlot/Crc16
  Protocol/        RESP2 parser, serializer, client-side reader/writer
  Server/          RecacheServer (accept loop), ClientConnection (batched I/O), RecacheNode (composition
                   root and startup sequence), WriteGate (write pauses)
  Security/        AUTH and per-connection session state
  Configuration/   RecacheOptions
tests/recache.Tests  Unit and process-level integration tests
benchmark/           Load generator (pipelines like redis-benchmark)
scripts/             install-service.ps1 / uninstall-service.ps1 — local dev/test service installs
installer/           Inno Setup script (recache.iss)
.github/workflows/   CI: builds and pushes the Docker image on version tags
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
Remove-Item publish -Recurse -ErrorAction SilentlyContinue
dotnet publish -c Release -r win-x64 --self-contained -o publish
& "C:\Program Files (x86)\Inno Setup 6\ISCC.exe" ..\installer\recache.iss
```

Produces `installer\Output\RecacheSetup.exe`. The publish is self-contained (it carries the .NET runtime), so the installed service runs on machines without .NET installed. The installer never packages the build machine's own `appsettings.yaml` (which holds your real password): it always installs `appsettings.example.yaml` as `appsettings.yaml`, and keeps an existing one on upgrade.

It's unsigned by default — if you have a code-signing certificate, sign `recache\publish\recache.exe` and `recache.dll` before compiling, and the compiled installer after, with `signtool.exe`; without a certificate from a recognized CA, expect SmartScreen/AV false positives on other machines.

### Docker

```bash
docker build -t recache .
docker run -d -p 6380:6380 -v recache-data:/data \
  -e Recache__RequireAuth=true -e Recache__Password=devpass recache
```

The container needs no config file present — `RecacheOptions` has safe built-in defaults (auth disabled, port 6380), and `Recache__<Property>` environment variables override them. `.dockerignore` deliberately excludes `appsettings.yaml` so a real local password can never end up baked into an image layer.

**Publishing to Docker Hub** happens via [`.github/workflows/docker-publish.yml`](.github/workflows/docker-publish.yml) on any `v*.*.*` tag push (or manually via the Actions tab), pushing `sajidur/recache:<version>` and `:latest`. One-time setup: add `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` as repo secrets.

### Benchmarking

`benchmark/` is a load generator that, like `redis-benchmark`, sends each pipeline batch in a single write. Run a Release build of recache, then:

```powershell
cd benchmark
dotnet run -c Release -- --port 6380 --password yourpassword --connections 50 --requests 20000 --pipeline 16 --tests set,get,incr,lpush,rpop,hset,mset,ping
```

It reports throughput and p50/p95/p99/max latency per test; `--keyspace N` sets how many distinct keys are used.

### License

Apache License 2.0 — see [LICENSE](LICENSE).
