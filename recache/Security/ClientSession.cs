using System.Collections.Generic;
using recache.Protocol;

namespace recache.Security
{
    public sealed class ClientSession
    {
        public bool IsAuthenticated { get; set; }

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
    }
}
