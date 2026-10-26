using System;
using System.Collections.Generic;

namespace recache.Storage
{
    public interface ICacheStore
    {
        // String
        bool TryGet(
            string key,
            out byte[]? value);

        bool TryGet(
            ReadOnlySpan<byte> utf8Key,
            out byte[]? value);

        void Set(
            string key,
            byte[] value,
            TimeSpan? expiration = null);

        // Absolute expiry (Unix ms) — what SET ... PXAT, the AOF and
        // the replication stream carry.
        void SetWithExpireAt(
            string key,
            byte[] value,
            long? expiresAtMs);

        // Replace a string's value, keeping its exact expiry.
        void SetKeepTtl(string key, byte[] value);

        long Increment(string key, long amount);

        bool SetIfAbsent(
            string key,
            byte[] value,
            TimeSpan? expiration = null);

        bool SetIfAbsentWithExpireAt(
            string key,
            byte[] value,
            long? expiresAtMs);

        // Hash
        RedisHash GetOrCreateHash(string key);

        RedisHash? TryGetHash(string key);

        // List
        RedisList GetOrCreateList(string key);

        RedisList? TryGetList(string key);

        // Set
        RedisSet GetOrCreateSet(string key);

        RedisSet? TryGetSet(string key);

        // Sorted set
        RedisSortedSet GetOrCreateSortedSet(string key);

        RedisSortedSet? TryGetSortedSet(string key);

        // JSON
        RedisJson GetOrCreateJson(string key);

        RedisJson? TryGetJson(string key);

        // Installs a whole value (RESTORE, snapshot loading).
        void Restore(
            string key,
            CacheValueType type,
            object payload,
            long? expiresAtMs,
            long bytes);

        // Type-agnostic
        bool Remove(string key);

        bool Exists(string key);

        // Like Exists, but never deletes an expired key — safe without
        // holding the key's lock.
        bool IsLive(string key, out CacheValueType type);

        bool TryGetTtl(string key, out TimeSpan? ttl);

        bool TryGetExpireAt(string key, out long? expiresAtMs);

        bool TryExpire(string key, TimeSpan? ttl);

        bool TryExpireAt(string key, long? expiresAtMs);

        bool TryPersist(string key);

        bool TryGetType(string key, out CacheValueType type);

        void AdjustMemory(string key, long byteDelta);

        // The key's current write-version, for WATCH. Returns false
        // if the key doesn't exist (distinct from existing at
        // version 0 — WATCH needs to tell "gone" from "unchanged").
        bool TryGetVersion(string key, out long version);

        // Raw entry, no expiry side effects (DUMP, snapshots).
        CacheEntry? Peek(string key);

        void Clear();

        // Keys in one cluster hash slot (indexed in cluster mode).
        IEnumerable<string> KeysInSlot(int slot);

        long CountKeysInSlot(int slot);

        IEnumerable<string> Keys { get; }

        // One SCAN page: about `count` keys from `cursor`; nextCursor
        // is 0 when the iteration is complete.
        IReadOnlyList<string> ScanKeys(long cursor, int count, out long nextCursor);

        long Count { get; }

        long ExpiringCount { get; }

        long MemoryBytes { get; }
    }
}
