using System.Collections.Generic;
using System.Threading;
using recache.Protocol;

namespace recache.Security
{
    // What the connection loop should do after writing the current
    // reply.
    public enum SessionHandoff
    {
        None,

        // Close the connection (QUIT).
        Close,

        // The client sent PSYNC/SYNC: the socket now belongs to the
        // replication code, which streams the dataset and writes to
        // it from then on.
        Replication,
    }

    public sealed class ClientSession
    {
        private static long s_nextId;

        public long Id { get; } = Interlocked.Increment(ref s_nextId);

        public string? Name { get; set; }

        public string RemoteAddress { get; set; } = "";

        public bool IsAuthenticated { get; set; }

        // The replica side of a replication link applying its
        // master's stream (or the AOF loader): bypasses auth, the
        // read-only-replica guard, cluster redirection and write
        // pauses — those commands were already accepted by the master.
        public bool IsMasterLink { get; set; }

        // Set by ASKING, consumed by the next command's slot check —
        // real Redis's one-shot opt-in to query a slot this node is
        // IMPORTING but doesn't yet own.
        public bool Asking { get; set; }

        // Set by READONLY: in cluster mode a replica serves reads for
        // its master's slots on this connection instead of
        // redirecting them.
        public bool ReadOnlyMode { get; set; }

        public bool InTransaction { get; set; }

        // Set when a command fails to queue (e.g. unknown command
        // name) — causes EXEC to abort the whole transaction rather
        // than run a partial batch.
        public bool TransactionDirty { get; set; }

        public List<RespValue> QueuedCommands { get; } = new();

        // Key -> version snapshot at WATCH time. A null value means
        // the key didn't exist yet when watched (still a valid,
        // distinct state — later creation is itself a conflict).
        public Dictionary<string, long?> WatchedKeys { get; } = new();

        // Replication-stream offset just past this client's latest
        // propagated write — what WAIT waits for replicas to ACK.
        public long LastWriteOffset { get; set; }

        // REPLCONF listening-port / ip-address, sent by a replica
        // before PSYNC so INFO/ROLE can show where it listens.
        public int ReplicaListeningPort { get; set; }

        public string? ReplicaAnnouncedIp { get; set; }

        public SessionHandoff Handoff { get; set; }

        public IReadOnlyList<RespValue>? HandoffArgs { get; set; }

        // Raw bytes of a MULTI...EXEC block received on a master
        // link, appended to this node's own stream in one piece at
        // EXEC so a snapshot can never split it.
        public System.Buffers.ArrayBufferWriter<byte>? ReplicatedTransactionBytes { get; set; }
    }
}
