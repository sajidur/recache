using System;

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
        PFail = 1 << 2,
        Fail = 1 << 3,
        NoAddr = 1 << 4
    }

    public sealed class ClusterNode
    {
        public required string Id { get; init; }

        public string Host { get; set; } = "";

        public int Port { get; set; }

        // Cluster-bus port (Phase 2). Conventionally Port + 10000 in
        // real Redis; recorded now so CLUSTER NODES output has the
        // right shape even before the bus itself exists.
        public int ClusterBusPort { get; set; }

        public ClusterNodeRole Role { get; set; } = ClusterNodeRole.Master;

        public string? MasterId { get; set; }

        public long ConfigEpoch { get; set; }

        public ClusterNodeFlags Flags { get; set; } = ClusterNodeFlags.None;

        public string HostAndPort => $"{Host}:{Port}";
    }
}
