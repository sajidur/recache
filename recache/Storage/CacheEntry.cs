namespace recache.Storage
{
    public sealed class CacheEntry
    {
        public required CacheValueType Type { get; init; }

        public required object Payload { get; init; }

        public long? ExpirationTimestamp { get; init; }

        public long LastAccessTimestamp;

        public long Frequency;
    }
}
