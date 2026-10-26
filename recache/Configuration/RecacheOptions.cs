using System;
using System.Collections.Generic;
using System.Text;

namespace recache.Configuration
{
        public sealed class RecacheOptions
        {
            public string Host { get; set; } = "0.0.0.0";

            public int Port { get; set; } = 6380;

            public int MaxConnections { get; set; } = 10_000;

            public int MaxRequestBytes { get; set; } = 16 * 1024 * 1024;

            public int MaxValueBytes { get; set; } = 8 * 1024 * 1024;

            public long MaxMemoryBytes { get; set; } =
                4L * 1024 * 1024 * 1024;

            public string? Password { get; set; }

            public bool RequireAuth { get; set; } = false;

            public bool EnableAof { get; set; } = false;

            // Relative paths resolve against the content root, same
            // as LogDirectory. The directory holds the AOF itself
            // plus the temp file a background rewrite builds before
            // atomically swapping it in, and the temp snapshots used
            // to bring replicas up to date. (The Docker image sets
            // this to /data/appendonly.aof, its persistent volume.)
            public string AofPath { get; set; } =
                "data/appendonly.aof";

            // When written AOF data is forced to disk:
            //   "always"   — fsync before replying to the client
            //                (group-committed, so concurrent writers
            //                share one fsync). No acknowledged write
            //                is ever lost.
            //   "everysec" — fsync once a second in the background.
            //                At most ~1s of writes lost on a power
            //                cut; the same default as real Redis.
            //   "no"       — leave flushing to the OS.
            public string AofFsync { get; set; } = "everysec";

            // Automatic background rewrite (compaction): triggered
            // once the file is both at least AofRewriteMinBytes and
            // this many percent larger than it was right after the
            // previous rewrite. 0 disables automatic rewrites
            // (BGREWRITEAOF still works).
            public int AofRewritePercentage { get; set; } = 100;

            public long AofRewriteMinBytes { get; set; } =
                64L * 1024 * 1024;

            // A crash mid-append can leave a half-written command at
            // the end of the file. true (the Redis default) truncates
            // that partial tail and starts normally; false refuses
            // to start so it can be inspected first.
            public bool AofLoadTruncated { get; set; } = true;

            public int DatabaseCount { get; set; } = 16;

            public bool EnableFileLogging { get; set; } = true;

            // Relative paths resolve against the content root (the
            // executable's own folder for a Windows Service or
            // container, the project folder for `dotnet run`).
            public string LogDirectory { get; set; } = "logs";

            // One log file per day; files older than this are
            // deleted automatically.
            public int LogRetentionDays { get; set; } = 7;

            // ---- Replication ----

            // "host port" — start up as a replica of that master.
            // Standalone mode only; in cluster mode replication is
            // driven by CLUSTER REPLICATE and failover instead.
            public string? ReplicaOf { get; set; }

            // Password this node presents to its master. Defaults to
            // Password, since nodes of one deployment normally share
            // a single credential.
            public string? MasterAuth { get; set; }

            // How much recent write traffic a master keeps in memory
            // so a briefly disconnected replica (or a former replica
            // after failover) can catch up with a partial resync
            // instead of a full dataset transfer.
            public long ReplicationBacklogBytes { get; set; } =
                64L * 1024 * 1024;

            // Hard cap on how far a replica may fall behind (or how
            // much stream a full sync may need retained) before the
            // master drops it and lets it start over.
            public long ReplicaOutputLimitBytes { get; set; } =
                512L * 1024 * 1024;

            // A master writes a PING into the replication stream this
            // often so replicas can tell a quiet master from a dead
            // one.
            public int ReplicaPingPeriodSeconds { get; set; } = 10;

            // A replica that hears nothing from its master for this
            // long drops the link and reconnects.
            public int ReplicationTimeoutSeconds { get; set; } = 60;

            // ---- Cluster ----

            // Turns on hash-slot cluster mode: CLUSTER commands,
            // MOVED/ASK redirection, and CROSSSLOT/CLUSTERDOWN
            // enforcement on every key-bearing command. Off by
            // default — a standalone node behaves exactly as before.
            public bool ClusterEnabled { get; set; } = false;

            // Relative paths resolve against the content root, same
            // as LogDirectory. Stores this node's id, role, and slot
            // ownership so identity survives restarts.
            public string ClusterConfigFile { get; set; } = "nodes.conf";

            // The host:port other nodes should use to reach this one
            // in MOVED/ASK replies and CLUSTER NODES/SLOTS output.
            // Defaults to Host/Port, but those are often "0.0.0.0" or
            // a container-internal address that peers can't dial.
            public string? ClusterAnnounceIp { get; set; }

            public int ClusterAnnouncePort { get; set; } = 0;

            // Port of the node-to-node cluster bus (gossip, failure
            // detection, failover elections). 0 = Port + 10000, the
            // same convention as real Redis.
            public int ClusterBusPort { get; set; } = 0;

            public int ClusterAnnounceBusPort { get; set; } = 0;

            // How long a node may be unreachable before it's
            // suspected (PFAIL), and the basis for every other
            // failover timer. Automatic failover completes in
            // roughly 1-2x this. Real Redis defaults to 15000.
            public int ClusterNodeTimeoutMs { get; set; } = 5000;

            // A replica with this set never starts an automatic
            // election (it still follows a promoted peer, and a
            // manual CLUSTER FAILOVER still works).
            public bool ClusterReplicaNoFailover { get; set; } = false;

            // true: the whole cluster refuses key commands while any
            // slot is unserved. false: keep serving the slots that
            // are still covered.
            public bool ClusterRequireFullCoverage { get; set; } = true;

            // Serve reads (never writes) even while this node sees
            // the cluster as down.
            public bool ClusterAllowReadsWhenDown { get; set; } = false;

            // On a clean shutdown (service stop, Ctrl+C, SIGTERM) a
            // master hands its slots to its most up-to-date replica
            // with a coordinated, zero-data-loss failover before
            // exiting, so rolling restarts and upgrades cause no
            // downtime. Cluster-aware clients just see MOVED.
            public bool ClusterFailoverOnShutdown { get; set; } = true;

            public int ClusterFailoverOnShutdownTimeoutMs { get; set; } =
                10_000;
        }
    }
