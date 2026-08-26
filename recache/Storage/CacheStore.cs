using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace recache.Storage
{
    public sealed class CacheStore : ICacheStore
    {
        private readonly ConcurrentDictionary<
            string,
            CacheEntry> _entries =
            new(StringComparer.Ordinal);

        private long _memoryBytes;

        public long Count => _entries.Count;

        public long MemoryBytes =>
            Interlocked.Read(ref _memoryBytes);

        public void AdjustMemory(long byteDelta)
        {
            Interlocked.Add(ref _memoryBytes, byteDelta);
        }

        // ---- String ----

        public long Increment(
            string key,
            long amount)
        {
            while (true)
            {
                var existing = GetLiveEntry(key);

                if (existing == null)
                {
                    var newEntry = new CacheEntry
                    {
                        Type = CacheValueType.String,
                        Payload = Encoding.UTF8.GetBytes(
                            amount.ToString(
                                CultureInfo.InvariantCulture)),

                        LastAccessTimestamp =
                            Stopwatch.GetTimestamp(),

                        Frequency = 1
                    };

                    if (_entries.TryAdd(key, newEntry))
                    {
                        Interlocked.Add(
                            ref _memoryBytes,
                            ((byte[])newEntry.Payload).Length);

                        return amount;
                    }

                    continue;
                }

                if (existing.Type != CacheValueType.String)
                    throw new WrongTypeException();

                var existingValue = (byte[])existing.Payload;

                var current =
                    long.Parse(
                        Encoding.UTF8.GetString(existingValue),
                        CultureInfo.InvariantCulture);

                var updated = checked(current + amount);

                var newValue =
                    Encoding.UTF8.GetBytes(
                        updated.ToString(
                            CultureInfo.InvariantCulture));

                var replacement = new CacheEntry
                {
                    Type = CacheValueType.String,
                    Payload = newValue,
                    ExpirationTimestamp =
                        existing.ExpirationTimestamp,

                    LastAccessTimestamp =
                        Stopwatch.GetTimestamp(),

                    Frequency = existing.Frequency + 1
                };

                if (_entries.TryUpdate(key, replacement, existing))
                {
                    Interlocked.Add(
                        ref _memoryBytes,
                        newValue.Length - existingValue.Length);

                    return updated;
                }
            }
        }

        public bool TryGet(
            string key,
            out byte[]? value)
        {
            value = null;

            var entry = GetLiveEntry(key);

            if (entry == null)
                return false;

            if (entry.Type != CacheValueType.String)
                throw new WrongTypeException();

            Touch(entry);

            value = (byte[])entry.Payload;

            return true;
        }

        public void Set(
            string key,
            byte[] value,
            TimeSpan? expiration = null)
        {
            var entry = new CacheEntry
            {
                Type = CacheValueType.String,
                Payload = value,
                ExpirationTimestamp =
                    expiration.HasValue
                        ? DateTimeOffset.UtcNow
                            .Add(expiration.Value)
                            .UtcTicks
                        : null,

                LastAccessTimestamp =
                    Stopwatch.GetTimestamp(),

                Frequency = 1
            };

            if (_entries.TryGetValue(key, out var old))
            {
                var oldSize =
                    old.Type == CacheValueType.String
                        ? ((byte[])old.Payload).Length
                        : 0;

                _entries[key] = entry;

                Interlocked.Add(
                    ref _memoryBytes,
                    value.Length - oldSize);

                return;
            }

            if (_entries.TryAdd(key, entry))
            {
                Interlocked.Add(ref _memoryBytes, value.Length);
            }
        }

        // Atomic "set only if the key doesn't currently exist" —
        // the building block for SETNX and SET ... NX.
        public bool SetIfAbsent(
            string key,
            byte[] value,
            TimeSpan? expiration = null)
        {
            // Clear a stale (expired-but-not-yet-swept) entry first
            // so the TryAdd below can succeed.
            if (_entries.TryGetValue(key, out var existing) &&
                IsExpired(existing))
            {
                Remove(key);
            }

            var entry = new CacheEntry
            {
                Type = CacheValueType.String,
                Payload = value,
                ExpirationTimestamp =
                    expiration.HasValue
                        ? DateTimeOffset.UtcNow
                            .Add(expiration.Value)
                            .UtcTicks
                        : null,

                LastAccessTimestamp = Stopwatch.GetTimestamp(),
                Frequency = 1
            };

            if (!_entries.TryAdd(key, entry))
                return false;

            Interlocked.Add(ref _memoryBytes, value.Length);
            return true;
        }

        // ---- Hash / List / Set / SortedSet ----

        public RedisHash GetOrCreateHash(string key) =>
            GetOrCreate(key, CacheValueType.Hash, () => new RedisHash());

        public RedisHash? TryGetHash(string key) =>
            TryGetTyped<RedisHash>(key, CacheValueType.Hash);

        public RedisList GetOrCreateList(string key) =>
            GetOrCreate(key, CacheValueType.List, () => new RedisList());

        public RedisList? TryGetList(string key) =>
            TryGetTyped<RedisList>(key, CacheValueType.List);

        public RedisSet GetOrCreateSet(string key) =>
            GetOrCreate(key, CacheValueType.Set, () => new RedisSet());

        public RedisSet? TryGetSet(string key) =>
            TryGetTyped<RedisSet>(key, CacheValueType.Set);

        public RedisSortedSet GetOrCreateSortedSet(string key) =>
            GetOrCreate(
                key,
                CacheValueType.SortedSet,
                () => new RedisSortedSet());

        public RedisSortedSet? TryGetSortedSet(string key) =>
            TryGetTyped<RedisSortedSet>(key, CacheValueType.SortedSet);

        public RedisJson GetOrCreateJson(string key) =>
            GetOrCreate(key, CacheValueType.Json, () => new RedisJson());

        public RedisJson? TryGetJson(string key) =>
            TryGetTyped<RedisJson>(key, CacheValueType.Json);

        private T GetOrCreate<T>(
            string key,
            CacheValueType type,
            Func<T> factory)
            where T : class
        {
            while (true)
            {
                var entry = GetLiveEntry(key);

                if (entry == null)
                {
                    var payload = factory();

                    var newEntry = new CacheEntry
                    {
                        Type = type,
                        Payload = payload,
                        LastAccessTimestamp =
                            Stopwatch.GetTimestamp(),
                        Frequency = 1
                    };

                    if (_entries.TryAdd(key, newEntry))
                        return payload;

                    continue;
                }

                if (entry.Type != type)
                    throw new WrongTypeException();

                Touch(entry);

                return (T)entry.Payload;
            }
        }

        private T? TryGetTyped<T>(string key, CacheValueType type)
            where T : class
        {
            var entry = GetLiveEntry(key);

            if (entry == null)
                return null;

            if (entry.Type != type)
                throw new WrongTypeException();

            Touch(entry);

            return (T)entry.Payload;
        }

        // ---- Type-agnostic ----

        public bool Remove(string key)
        {
            if (!_entries.TryRemove(key, out var entry))
                return false;

            if (entry.Type == CacheValueType.String)
            {
                Interlocked.Add(
                    ref _memoryBytes,
                    -((byte[])entry.Payload).Length);
            }

            return true;
        }

        public bool Exists(string key)
        {
            return GetLiveEntry(key) != null;
        }

        // Sets (ttl.HasValue) or clears (ttl == null) a key's
        // expiration without disturbing its value. Returns false
        // if the key doesn't exist.
        public bool TryExpire(string key, TimeSpan? ttl)
        {
            while (true)
            {
                var entry = GetLiveEntry(key);

                if (entry == null)
                    return false;

                var replacement = new CacheEntry
                {
                    Type = entry.Type,
                    Payload = entry.Payload,
                    ExpirationTimestamp =
                        ttl.HasValue
                            ? DateTimeOffset.UtcNow
                                .Add(ttl.Value)
                                .UtcTicks
                            : null,

                    LastAccessTimestamp = entry.LastAccessTimestamp,
                    Frequency = entry.Frequency
                };

                if (_entries.TryUpdate(key, replacement, entry))
                    return true;
            }
        }

        // Clears a key's expiration. Returns false if the key
        // doesn't exist or already had no expiration set.
        public bool TryPersist(string key)
        {
            while (true)
            {
                var entry = GetLiveEntry(key);

                if (entry == null || entry.ExpirationTimestamp == null)
                    return false;

                var replacement = new CacheEntry
                {
                    Type = entry.Type,
                    Payload = entry.Payload,
                    ExpirationTimestamp = null,
                    LastAccessTimestamp = entry.LastAccessTimestamp,
                    Frequency = entry.Frequency
                };

                if (_entries.TryUpdate(key, replacement, entry))
                    return true;
            }
        }

        public bool TryGetTtl(string key, out TimeSpan? ttl)
        {
            ttl = null;

            var entry = GetLiveEntry(key);

            if (entry == null)
                return false;

            if (entry.ExpirationTimestamp is { } expirationTicks)
            {
                var remainingTicks =
                    expirationTicks - DateTimeOffset.UtcNow.UtcTicks;

                ttl = TimeSpan.FromTicks(Math.Max(remainingTicks, 0));
            }

            return true;
        }

        public bool TryGetType(string key, out CacheValueType type)
        {
            var entry = GetLiveEntry(key);

            if (entry == null)
            {
                type = default;
                return false;
            }

            type = entry.Type;
            return true;
        }

        public IEnumerable<string> Keys => _entries.Keys;

        private CacheEntry? GetLiveEntry(string key)
        {
            if (!_entries.TryGetValue(key, out var entry))
                return null;

            if (IsExpired(entry))
            {
                Remove(key);
                return null;
            }

            return entry;
        }

        private static void Touch(CacheEntry entry)
        {
            Interlocked.Increment(ref entry.Frequency);

            Volatile.Write(
                ref entry.LastAccessTimestamp,
                Stopwatch.GetTimestamp());
        }

        private static bool IsExpired(CacheEntry entry)
        {
            var expiration = entry.ExpirationTimestamp;

            return expiration.HasValue &&
                   expiration.Value <=
                   DateTimeOffset.UtcNow.UtcTicks;
        }
    }
}
