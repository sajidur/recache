namespace recache.Storage
{
    public sealed class CacheEntry
    {
        public required CacheValueType Type { get; init; }

        public required object Payload { get; init; }

        public long? ExpirationTimestamp { get; init; }

        public long LastAccessTimestamp;

        public long Frequency;

        // Bumped on every write to this key (value change, TTL
        // change, or compound-type mutation) — the basis for
        // WATCH's change detection.
        public long Version;
    }
}
