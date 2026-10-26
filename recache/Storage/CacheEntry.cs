using System.Threading;

namespace recache.Storage
{
    // One key's value and metadata. Updated in place (under the key's
    // exclusive lock) rather than replaced: an update to an existing
    // key allocates nothing but the new value, so the store doesn't
    // churn out short-lived entry objects for the GC to promote.
    public sealed class CacheEntry
    {
        private object _payload;
        private long _expiresAtMs;

        public CacheEntry(object payload, long? expiresAtMs)
        {
            _payload = payload;
            _expiresAtMs = expiresAtMs ?? 0;
        }

        // byte[] for strings, RedisHash/RedisList/... for the rest.
        // The type is derived from the payload, so changing both is a
        // single atomic reference write.
        public object Payload
        {
            get => Volatile.Read(ref _payload);
            set => Volatile.Write(ref _payload, value);
        }

        public CacheValueType Type => Payload switch
        {
            byte[] => CacheValueType.String,
            RedisHash => CacheValueType.Hash,
            RedisList => CacheValueType.List,
            RedisSet => CacheValueType.Set,
            RedisSortedSet => CacheValueType.SortedSet,
            _ => CacheValueType.Json,
        };

        // Absolute expiry as Unix time in milliseconds (null = no
        // TTL). Absolute rather than relative so the exact same
        // instant can be written to the AOF and replication stream
        // (as PEXPIREAT / SET ... PXAT) and reproduced on replay.
        // Stored as one long (0 = none) so unlocked readers can't see
        // a torn value.
        public long? ExpiresAtMs
        {
            get
            {
                var value = Interlocked.Read(ref _expiresAtMs);
                return value == 0 ? null : value;
            }

            set => Interlocked.Exchange(ref _expiresAtMs, value ?? 0);
        }

        public bool HasExpiry => Interlocked.Read(ref _expiresAtMs) != 0;

        // Environment.TickCount64 of the last read — groundwork for
        // LRU eviction. Plain (non-interlocked) writes, at most once
        // per millisecond, so a hot key doesn't turn every read into
        // contended cache-line traffic.
        public long LastAccessTicks;

        // Bumped on every write to this key (value change, TTL
        // change, or compound-type mutation) — the basis for
        // WATCH's change detection.
        public long Version;

        // Approximate payload size in bytes, so removing a hash or
        // list gives back what its mutations added to used_memory.
        public long Bytes;

        // The snapshot generation this entry has already been
        // captured in (or was created after). See SnapshotService.
        public int SnapshotEpoch;
    }
}
