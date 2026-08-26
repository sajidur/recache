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

        void AdjustMemory(long byteDelta);

        IEnumerable<string> Keys { get; }

        long Count { get; }

        long MemoryBytes { get; }
    }
}
