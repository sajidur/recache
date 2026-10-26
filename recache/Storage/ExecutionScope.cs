using recache.Protocol;
using System.Collections.Generic;

namespace recache.Storage
{
    // Per-thread state for the command currently executing. Command
    // handlers run synchronously start to finish on one thread (they
    // never await while holding key locks), so a [ThreadStatic] slot
    // is exactly "this command" without threading a context object
    // through ~100 handler signatures.
    internal static class ExecutionScope
    {
        // Bumped by CacheStore on every real mutation. A write
        // command that leaves this at zero changed nothing (SET NX on
        // an existing key, SREM of a non-member, DEL of a missing
        // key) and is not propagated — the same rule as Redis's
        // server.dirty.
        [ThreadStatic]
        public static long Dirty;

        // Set while replaying the AOF or applying a master's
        // replication stream. Expiration is then decided only by the
        // DELs in that stream, never by this node's clock: replaying
        // "INCR k" must find k even if k's TTL has passed by now,
        // or the replica/reloaded dataset would diverge from what
        // the master actually did.
        [ThreadStatic]
        public static bool ApplyingReplicated;

        // A handler that needs its effect propagated as something
        // other than its own argv (MIGRATE → DEL of the moved keys)
        // records the replacement commands here.
        [ThreadStatic]
        public static List<RespValue[]>? PropagationOverride;

        public static void Override(params RespValue[] command)
        {
            (PropagationOverride ??= new List<RespValue[]>()).Add(command);
        }
    }
}
