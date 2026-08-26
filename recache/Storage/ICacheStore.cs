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

        void Set(
            string key,
            byte[] value,
            TimeSpan? expiration = null);

        long Increment(string key, long amount);

        bool SetIfAbsent(
            string key,
            byte[] value,
            TimeSpan? expiration = null);

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

        // Type-agnostic
        bool Remove(string key);

        bool Exists(string key);

        bool TryGetTtl(string key, out TimeSpan? ttl);

        bool TryExpire(string key, TimeSpan? ttl);

        bool TryPersist(string key);

        bool TryGetType(string key, out CacheValueType type);

        void AdjustMemory(string key, long byteDelta);

        // The key's current write-version, for WATCH. Returns false
        // if the key doesn't exist (distinct from existing at
        // version 0 — WATCH needs to tell "gone" from "unchanged").
        bool TryGetVersion(string key, out long version);

        IEnumerable<string> Keys { get; }

        long Count { get; }

        long MemoryBytes { get; }
    }
}
