using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace recache.Storage
{
    public sealed class CacheStore : ICacheStore
    {
        // Swapped wholesale (not cleared in place) by FLUSHALL and by
        // a replica's full resync, so readers always see either the
        // complete old dataset or the complete new one.
        private volatile ShardedMap<CacheEntry> _entries = new();

        // Keys that currently carry a TTL — what the active expiry
        // cycle walks, so expired keys that are never read again
        // still get reclaimed.
        private volatile ShardedMap<byte> _expiring = new();

        private long _memoryBytes;

        // Maintained alongside every insert/remove: counting the
        // dictionaries themselves would take all of their locks.
        private long _count;
        private long _expiringCount;

        // Each incarnation of a key starts its WATCH version at a fresh
        // 2^32-wide range, so a key deleted and re-created can never
        // land back on a version a WATCH recorded for the old one.
        private long _versionBase;

        // Cluster mode only: the keys in each hash slot, so CLUSTER
        // GETKEYSINSLOT / COUNTKEYSINSLOT (called once per slot while
        // resharding) and dropping a lost slot don't scan the whole
        // keyspace.
        private volatile ConcurrentDictionary<string, byte>[]? _slotIndex;

        public CacheStore()
            : this(trackSlots: false)
        {
        }

        public CacheStore(bool trackSlots)
        {
            if (trackSlots)
                _slotIndex = NewSlotIndex();
        }

        public bool TracksSlots => _slotIndex is not null;

        public IEnumerable<string> KeysInSlot(int slot)
        {
            if (_slotIndex is { } index)
                return index[slot].Keys;

            return ScanSlot(slot);
        }

        public long CountKeysInSlot(int slot)
        {
            if (_slotIndex is { } index)
                return index[slot].Count;

            long count = 0;

            foreach (var _ in ScanSlot(slot))
                count++;

            return count;
        }

        private IEnumerable<string> ScanSlot(int slot)
        {
            foreach (var key in _entries.Keys)
            {
                if (Clustering.HashSlot.Compute(key) == slot)
                    yield return key;
            }
        }

        private static ConcurrentDictionary<string, byte>[] NewSlotIndex()
        {
            var index = new ConcurrentDictionary<string, byte>[Clustering.HashSlot.SlotCount];

            for (var i = 0; i < index.Length; i++)
                index[i] = new ConcurrentDictionary<string, byte>(StringComparer.Ordinal);

            return index;
        }

        private void IndexAdd(string key)
        {
            if (_slotIndex is { } index)
                index[Clustering.HashSlot.Compute(key)][key] = 0;
        }

        private void IndexRemove(string key)
        {
            if (_slotIndex is { } index)
                index[Clustering.HashSlot.Compute(key)].TryRemove(key, out _);
        }

        // Set by SnapshotService (while it holds every key lock) when
        // a snapshot starts. Entries created from then on belong to
        // the post-snapshot world and are skipped by its iterator.
        public int CurrentSnapshotEpoch;

        // Replicas never delete expired keys on their own — their
        // master's DEL (sent when it expires the key) does, keeping
        // both datasets identical regardless of clock skew. Client
        // reads on a replica still see an expired key as missing.
        public volatile bool IsReplica;

        // Raised (with the key's lock held) when this node expires a
        // key, so the expiry can be propagated as a DEL.
        public Action<string>? KeyExpired;

        public long Count => Interlocked.Read(ref _count);

        public long ExpiringCount => Interlocked.Read(ref _expiringCount);

        public long MemoryBytes =>
            Interlocked.Read(ref _memoryBytes);

        public static long NowMs =>
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        public void AdjustMemory(string key, long byteDelta)
        {
            Interlocked.Add(ref _memoryBytes, byteDelta);

            if (_entries.TryGetValue(key, out var entry))
            {
                Interlocked.Add(ref entry.Bytes, byteDelta);
                Interlocked.Increment(ref entry.Version);
            }

            ExecutionScope.Dirty++;
        }

        public bool TryGetVersion(string key, out long version)
        {
            var entry = GetLiveEntry(key);

            if (entry == null)
            {
                version = 0;
                return false;
            }

            version = Interlocked.Read(ref entry.Version);
            return true;
        }

        // ---- String ----

        public long Increment(
            string key,
            long amount)
        {
            var existing = GetLiveEntry(key);

            if (existing == null)
            {
                var created = Encoding.UTF8.GetBytes(
                    amount.ToString(CultureInfo.InvariantCulture));

                Write(key, RawEntry(key), created, null, created.Length);
                return amount;
            }

            if (existing.Payload is not byte[] existingValue)
                throw new WrongTypeException();

            if (!long.TryParse(
                    Encoding.UTF8.GetString(existingValue),
                    NumberStyles.AllowLeadingSign,
                    CultureInfo.InvariantCulture,
                    out var current))
            {
                throw new CommandErrorException(
                    "ERR value is not an integer or out of range");
            }

            long updated;

            try
            {
                updated = checked(current + amount);
            }
            catch (OverflowException)
            {
                throw new CommandErrorException(
                    "ERR increment or decrement would overflow");
            }

            var newValue = Encoding.UTF8.GetBytes(
                updated.ToString(CultureInfo.InvariantCulture));

            Write(key, existing, newValue, existing.ExpiresAtMs, newValue.Length);
            return updated;
        }

        public bool TryGet(
            string key,
            out byte[]? value)
        {
            value = null;

            var entry = GetLiveEntry(key);

            if (entry == null)
                return false;

            if (entry.Payload is not byte[] bytes)
                throw new WrongTypeException();

            Touch(entry);

            value = bytes;
            return true;
        }

        // GET straight from the request's key bytes: no key string is
        // allocated on the hottest read path. Anything that needs the
        // key as a string (an expired entry to delete and propagate)
        // falls back to the string overload.
        public bool TryGet(ReadOnlySpan<byte> utf8Key, out byte[]? value)
        {
            if (utf8Key.Length > 256)
                return TryGet(Encoding.UTF8.GetString(utf8Key), out value);

            Span<char> chars = stackalloc char[utf8Key.Length];
            var key = chars[..Encoding.UTF8.GetChars(utf8Key, chars)];

            value = null;

            if (!_entries.TryGetValue(key, out var entry))
                return false;

            if (entry.ExpiresAtMs is { } expiresAt && expiresAt <= NowMs)
                return TryGet(new string(key), out value);

            if (entry.Payload is not byte[] bytes)
                throw new WrongTypeException();

            Touch(entry);

            value = bytes;
            return true;
        }

        public void Set(
            string key,
            byte[] value,
            TimeSpan? expiration = null)
        {
            SetWithExpireAt(
                key,
                value,
                expiration.HasValue
                    ? NowMs + (long)expiration.Value.TotalMilliseconds
                    : null);
        }

        public void SetWithExpireAt(
            string key,
            byte[] value,
            long? expiresAtMs)
        {
            Write(key, RawEntry(key), value, expiresAtMs, value.Length);
        }

        // Replaces a string's value while keeping its absolute expiry
        // exactly — APPEND, SETRANGE, SET KEEPTTL. Re-deriving it
        // from the remaining TTL would drift by however long the
        // command took, and differently on every replica.
        public void SetKeepTtl(string key, byte[] value)
        {
            var live = GetLiveEntry(key);

            SetWithExpireAt(key, value, live?.ExpiresAtMs);
        }

        // Atomic "set only if the key doesn't currently exist" —
        // the building block for SETNX and SET ... NX.
        public bool SetIfAbsent(
            string key,
            byte[] value,
            TimeSpan? expiration = null)
        {
            return SetIfAbsentWithExpireAt(
                key,
                value,
                expiration.HasValue
                    ? NowMs + (long)expiration.Value.TotalMilliseconds
                    : null);
        }

        public bool SetIfAbsentWithExpireAt(
            string key,
            byte[] value,
            long? expiresAtMs)
        {
            if (GetLiveEntry(key) != null)
                return false;

            Write(key, RawEntry(key), value, expiresAtMs, value.Length);
            return true;
        }

        // ---- Hash / List / Set / SortedSet / JSON ----

        public RedisHash GetOrCreateHash(string key) =>
            GetOrCreate(key, () => new RedisHash());

        public RedisHash? TryGetHash(string key) =>
            TryGetTyped<RedisHash>(key);

        public RedisList GetOrCreateList(string key) =>
            GetOrCreate(key, () => new RedisList());

        public RedisList? TryGetList(string key) =>
            TryGetTyped<RedisList>(key);

        public RedisSet GetOrCreateSet(string key) =>
            GetOrCreate(key, () => new RedisSet());

        public RedisSet? TryGetSet(string key) =>
            TryGetTyped<RedisSet>(key);

        public RedisSortedSet GetOrCreateSortedSet(string key) =>
            GetOrCreate(key, () => new RedisSortedSet());

        public RedisSortedSet? TryGetSortedSet(string key) =>
            TryGetTyped<RedisSortedSet>(key);

        public RedisJson GetOrCreateJson(string key) =>
            GetOrCreate(key, () => new RedisJson());

        public RedisJson? TryGetJson(string key) =>
            TryGetTyped<RedisJson>(key);

        private T GetOrCreate<T>(
            string key,
            Func<T> factory)
            where T : class
        {
            var entry = GetLiveEntry(key);

            if (entry == null)
            {
                var payload = factory();

                Write(key, RawEntry(key), payload, null, 0);
                return payload;
            }

            if (entry.Payload is not T typed)
                throw new WrongTypeException();

            Touch(entry);

            return typed;
        }

        private T? TryGetTyped<T>(string key)
            where T : class
        {
            var entry = GetLiveEntry(key);

            if (entry == null)
                return null;

            if (entry.Payload is not T typed)
                throw new WrongTypeException();

            Touch(entry);

            return typed;
        }

        // Installs a fully-formed value (RESTORE, snapshot loading),
        // replacing whatever the key held.
        public void Restore(
            string key,
            CacheValueType type,
            object payload,
            long? expiresAtMs,
            long bytes)
        {
            Write(key, RawEntry(key), payload, expiresAtMs, bytes);
        }

        // ---- Type-agnostic ----

        public bool Remove(string key)
        {
            var entries = _entries;

            if (!entries.TryGetValue(key, out var entry))
                return false;

            if (IsExpired(entry) && !ExecutionScope.ApplyingReplicated)
            {
                // DEL of an already-expired key removes nothing as far
                // as the client is concerned (reply 0, like Redis);
                // the expiry itself is propagated separately.
                if (!IsReplica)
                    ExpireNow(entries, key, entry);

                return false;
            }

            if (!entries.TryRemove(
                    new KeyValuePair<string, CacheEntry>(key, entry)))
            {
                return false;
            }

            Interlocked.Add(ref _memoryBytes, -Interlocked.Read(ref entry.Bytes));
            Interlocked.Decrement(ref _count);

            if (entry.HasExpiry)
                ExpiringRemove(key);

            IndexRemove(key);
            ExecutionScope.Dirty++;
            return true;
        }

        public bool Exists(string key)
        {
            return GetLiveEntry(key) != null;
        }

        // Existence (and type) check with no side effects, for callers
        // that don't hold the key's lock — SCAN, KEYS, cluster routing.
        // Lazy expiry may only delete under the key's lock: an
        // unlocked delete could race a concurrent write to the key.
        public bool IsLive(string key, out CacheValueType type)
        {
            type = default;

            if (!_entries.TryGetValue(key, out var entry))
                return false;

            if (IsExpired(entry) && !ExecutionScope.ApplyingReplicated)
                return false;

            type = entry.Type;
            return true;
        }

        // Sets (ttl.HasValue) or clears (ttl == null) a key's
        // expiration without disturbing its value. Returns false
        // if the key doesn't exist.
        public bool TryExpire(string key, TimeSpan? ttl)
        {
            return TryExpireAt(
                key,
                ttl.HasValue
                    ? NowMs + (long)ttl.Value.TotalMilliseconds
                    : null);
        }

        public bool TryExpireAt(string key, long? expiresAtMs)
        {
            var entry = GetLiveEntry(key);

            if (entry == null)
                return false;

            SetExpiry(key, entry, expiresAtMs);
            return true;
        }

        // Clears a key's expiration. Returns false if the key
        // doesn't exist or already had no expiration set.
        public bool TryPersist(string key)
        {
            var entry = GetLiveEntry(key);

            if (entry == null || !entry.HasExpiry)
                return false;

            SetExpiry(key, entry, null);
            return true;
        }

        public bool TryGetTtl(string key, out TimeSpan? ttl)
        {
            ttl = null;

            var entry = GetLiveEntry(key);

            if (entry == null)
                return false;

            if (entry.ExpiresAtMs is { } expiresAt)
            {
                ttl = TimeSpan.FromMilliseconds(
                    Math.Max(expiresAt - NowMs, 0));
            }

            return true;
        }

        public bool TryGetExpireAt(string key, out long? expiresAtMs)
        {
            var entry = GetLiveEntry(key);

            expiresAtMs = entry?.ExpiresAtMs;
            return entry != null;
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

        private const int ScanOffsetBits = 24;

        // SCAN: the cursor encodes (shard << 24) | offset into that
        // shard's sorted keys. Each call examines about `count` keys,
        // touching only the shards it walks through. Keys present for
        // the whole iteration are returned (a concurrent delete can
        // shift a page boundary by a key, as with Redis's own cursor
        // under rehashing, so callers should tolerate duplicates).
        public IReadOnlyList<string> ScanKeys(long cursor, int count, out long nextCursor)
        {
            var shard = (int)(cursor >> ScanOffsetBits);
            var offset = (int)(cursor & ((1L << ScanOffsetBits) - 1));
            var result = new List<string>(count);
            var entries = _entries;

            while (shard < ShardedMap<CacheEntry>.ShardCount && result.Count < count)
            {
                var keys = entries.SortedKeysOfShard(shard);

                while (offset < keys.Length && result.Count < count)
                    result.Add(keys[offset++]);

                if (offset >= keys.Length)
                {
                    shard++;
                    offset = 0;
                }
            }

            nextCursor = shard >= ShardedMap<CacheEntry>.ShardCount
                ? 0
                : ((long)shard << ScanOffsetBits) | (uint)offset;

            return result;
        }

        // Raw access for snapshotting: no expiry side effects, no
        // access-time update.
        public CacheEntry? Peek(string key)
            => _entries.TryGetValue(key, out var entry) ? entry : null;

        public IEnumerable<KeyValuePair<string, CacheEntry>> Entries => _entries.Pairs;

        public IEnumerable<string> ExpiringKeys => _expiring.Keys;

        // Active expiry: deletes the key if its TTL has passed.
        // Callers hold the key's exclusive lock.
        public bool ExpireIfDue(string key)
        {
            var entries = _entries;

            if (!entries.TryGetValue(key, out var entry) || !entry.HasExpiry)
            {
                ExpiringRemove(key);
                return false;
            }

            if (!IsExpired(entry))
                return false;

            ExpireNow(entries, key, entry);
            return true;
        }

        // FLUSHALL. Callers hold every key lock.
        public void Clear()
        {
            _entries = new ShardedMap<CacheEntry>();
            _expiring = new ShardedMap<byte>();
            Interlocked.Exchange(ref _count, 0);
            Interlocked.Exchange(ref _expiringCount, 0);

            if (_slotIndex is not null)
                _slotIndex = NewSlotIndex();

            Interlocked.Exchange(ref _memoryBytes, 0);
            ExecutionScope.Dirty++;
        }

        // A replica's full resync: adopt a dataset that was loaded
        // off to the side while this one kept serving reads.
        public void ReplaceContents(CacheStore source)
        {
            _entries = source._entries;
            _expiring = source._expiring;
            Interlocked.Exchange(ref _count, source.Count);
            Interlocked.Exchange(ref _expiringCount, source.ExpiringCount);

            if (_slotIndex is not null)
            {
                if (source._slotIndex is { } sourceIndex)
                {
                    _slotIndex = sourceIndex;
                }
                else
                {
                    var index = NewSlotIndex();

                    foreach (var key in source._entries.Keys)
                        index[Clustering.HashSlot.Compute(key)][key] = 0;

                    _slotIndex = index;
                }
            }

            // Versions must keep growing past anything a WATCH could
            // have recorded against the old dataset.
            Interlocked.Exchange(
                ref _versionBase,
                Math.Max(Interlocked.Read(ref _versionBase), Interlocked.Read(ref source._versionBase)));

            Interlocked.Exchange(ref _memoryBytes, source.MemoryBytes);
        }

        private CacheEntry? RawEntry(string key)
            => _entries.TryGetValue(key, out var entry) ? entry : null;

        private CacheEntry? GetLiveEntry(string key)
        {
            var entries = _entries;

            if (!entries.TryGetValue(key, out var entry))
                return null;

            if (entry.ExpiresAtMs is { } expiresAt &&
                expiresAt <= NowMs)
            {
                if (ExecutionScope.ApplyingReplicated)
                    return entry;

                if (!IsReplica)
                    ExpireNow(entries, key, entry);

                return null;
            }

            return entry;
        }

        private void ExpireNow(
            ShardedMap<CacheEntry> entries,
            string key,
            CacheEntry entry)
        {
            if (!entries.TryRemove(
                    new KeyValuePair<string, CacheEntry>(key, entry)))
            {
                return;
            }

            Interlocked.Add(ref _memoryBytes, -Interlocked.Read(ref entry.Bytes));
            Interlocked.Decrement(ref _count);
            ExpiringRemove(key);
            IndexRemove(key);

            KeyExpired?.Invoke(key);
        }

        // Stores `payload` for `key`. `current` is whatever entry the
        // key maps to right now (live, stale-expired, or null when
        // absent) and is updated in place; only a brand new key
        // allocates an entry. Callers hold the key's exclusive lock,
        // and an in-progress snapshot has already captured the old
        // value (CommandDispatcher.BeforeWrite).
        private void Write(
            string key,
            CacheEntry? current,
            object payload,
            long? expiresAtMs,
            long bytes)
        {
            if (current is not null)
            {
                var hadExpiry = current.HasExpiry;
                var oldBytes = Interlocked.Exchange(ref current.Bytes, bytes);

                current.Payload = payload;
                current.ExpiresAtMs = expiresAtMs;
                current.LastAccessTicks = Environment.TickCount64;
                Interlocked.Increment(ref current.Version);
                Interlocked.Add(ref _memoryBytes, bytes - oldBytes);

                if (expiresAtMs.HasValue && !hadExpiry)
                    ExpiringAdd(key);
                else if (!expiresAtMs.HasValue && hadExpiry)
                    ExpiringRemove(key);
            }
            else
            {
                var entry = new CacheEntry(payload, expiresAtMs)
                {
                    LastAccessTicks = Environment.TickCount64,
                    Version = Interlocked.Add(ref _versionBase, 1L << 32),
                    Bytes = bytes,
                    SnapshotEpoch = Volatile.Read(ref CurrentSnapshotEpoch),
                };

                _entries[key] = entry;
                Interlocked.Increment(ref _count);
                Interlocked.Add(ref _memoryBytes, bytes);
                IndexAdd(key);

                if (expiresAtMs.HasValue)
                    ExpiringAdd(key);
            }

            ExecutionScope.Dirty++;
        }

        private void SetExpiry(string key, CacheEntry entry, long? expiresAtMs)
        {
            var hadExpiry = entry.HasExpiry;

            entry.ExpiresAtMs = expiresAtMs;
            Interlocked.Increment(ref entry.Version);

            if (expiresAtMs.HasValue && !hadExpiry)
                ExpiringAdd(key);
            else if (!expiresAtMs.HasValue && hadExpiry)
                ExpiringRemove(key);

            ExecutionScope.Dirty++;
        }

        private void ExpiringAdd(string key)
        {
            if (_expiring.TryAdd(key, 0))
                Interlocked.Increment(ref _expiringCount);
        }

        private void ExpiringRemove(string key)
        {
            if (_expiring.TryRemove(key, out _))
                Interlocked.Decrement(ref _expiringCount);
        }

        private static void Touch(CacheEntry entry)
        {
            var now = Environment.TickCount64;

            if (entry.LastAccessTicks != now)
                entry.LastAccessTicks = now;
        }

        private static bool IsExpired(CacheEntry entry)
        {
            var expiration = entry.ExpiresAtMs;

            return expiration.HasValue &&
                   expiration.Value <= NowMs;
        }
    }
}
