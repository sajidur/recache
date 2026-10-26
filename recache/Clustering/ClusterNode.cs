using System;
using System.Collections.Generic;

namespace recache.Clustering
{
    public enum ClusterNodeRole
    {
        Master,
        Replica
    }

    [Flags]
    public enum ClusterNodeFlags
    {
        None = 0,
        Myself = 1 << 0,
        Handshake = 1 << 1,

        // Suspected failing: no PONG within the node timeout, as seen
        // by this node alone.
        PFail = 1 << 2,

        // Agreed failing: a majority of masters reported it PFAIL.
        // A replica of a FAIL master starts an election.
        Fail = 1 << 3,

        NoAddr = 1 << 4
    }

    public sealed class ClusterNode
    {
        public required string Id { get; init; }

        public string Host { get; set; } = "";

        public int Port { get; set; }

        // Node-to-node bus port; Port + 10000 unless configured.
        public int ClusterBusPort { get; set; }

        public ClusterNodeRole Role { get; set; } = ClusterNodeRole.Master;

        public string? MasterId { get; set; }

        // The epoch at which this node's slot claims were last set.
        // When two nodes claim the same slot, the higher configEpoch
        // wins — that's how a promoted replica's claim supersedes its
        // failed master's.
        public long ConfigEpoch { get; set; }

        public ClusterNodeFlags Flags { get; set; } = ClusterNodeFlags.None;

        // ---- Runtime state (not persisted) ----

        // Unix ms. PingSentMs is non-zero while a PING is unanswered.
        public long PingSentMs { get; set; }

        public long PongReceivedMs { get; set; }

        public long LastPingSentAttemptMs { get; set; }

        // Any message from the node counts as a sign of life, so a
        // broken outbound link alone doesn't get a healthy node
        // suspected.
        public long LastDataReceivedMs { get; set; }

        public long FailTimeMs { get; set; }

        // When this node (as a master) last gave a failover vote to one
        // of this node's replicas — at most one per 2x node timeout.
        public long VotedTimeMs { get; set; }

        // Replication offset the node last advertised.
        public long ReplOffset { get; set; }

        public bool LinkConnected { get; set; }

        // reporter node id -> Unix ms of its latest report that this
        // node looked PFAIL/FAIL to it. Guarded by ClusterState's lock.
        public Dictionary<string, long> FailureReports { get; } =
            new(StringComparer.Ordinal);

        public string HostAndPort => $"{Host}:{Port}";

        public bool IsMaster => Role == ClusterNodeRole.Master;

        public bool IsReplica => Role == ClusterNodeRole.Replica;

        public bool IsFailed => (Flags & ClusterNodeFlags.Fail) != 0;

        public bool IsPFail => (Flags & ClusterNodeFlags.PFail) != 0;

        public bool IsFailing => (Flags & (ClusterNodeFlags.Fail | ClusterNodeFlags.PFail)) != 0;

        public bool IsMyself => (Flags & ClusterNodeFlags.Myself) != 0;
    }
}
