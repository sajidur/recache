using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace recache.Storage
{
    // The keyspace's dictionary, split into independently growing
    // shards.
    //
    // A single ConcurrentDictionary doubles its table by taking every
    // internal lock and rehashing every entry at once — at a million
    // keys that froze all writers for 100-300 ms (Redis avoids the
    // same problem with incremental rehashing). With 1024 shards each
    // resize touches ~0.1% of the keys and blocks only writers to that
    // shard, for microseconds.
    internal sealed class ShardedMap<TValue>
    {
        public const int ShardCount = 1024;
        private const int Mask = ShardCount - 1;

        private readonly ConcurrentDictionary<string, TValue>[] _shards;

        public ShardedMap()
        {
            _shards = new ConcurrentDictionary<string, TValue>[ShardCount];

            for (var i = 0; i < _shards.Length; i++)
            {
                _shards[i] = new ConcurrentDictionary<string, TValue>(
                    concurrencyLevel: 2,
                    capacity: 7,
                    StringComparer.Ordinal);
            }
        }

        private ConcurrentDictionary<string, TValue> Shard(string key)
            => _shards[key.GetHashCode() & Mask];

        // string.GetHashCode(span) equals the same string's
        // GetHashCode(), so span lookups land on the same shard.
        private ConcurrentDictionary<string, TValue> Shard(ReadOnlySpan<char> key)
            => _shards[string.GetHashCode(key) & Mask];

        public bool TryGetValue(string key, out TValue value)
            => Shard(key).TryGetValue(key, out value!);

        public bool TryGetValue(ReadOnlySpan<char> key, out TValue value)
            => Shard(key)
                .GetAlternateLookup<ReadOnlySpan<char>>()
                .TryGetValue(key, out value!);

        public TValue this[string key]
        {
            set => Shard(key)[key] = value;
        }

        public bool TryAdd(string key, TValue value)
            => Shard(key).TryAdd(key, value);

        public bool TryRemove(string key, out TValue value)
            => Shard(key).TryRemove(key, out value!);

        // Removes only if the key still maps to exactly this value.
        public bool TryRemove(KeyValuePair<string, TValue> pair)
            => Shard(pair.Key).TryRemove(pair);

        // One shard's keys, sorted — the unit SCAN pages through, so a
        // SCAN call costs O(shard) rather than sorting the whole
        // keyspace.
        public string[] SortedKeysOfShard(int shard)
        {
            var keys = new List<string>(_shards[shard].Count);

            foreach (var pair in _shards[shard])
                keys.Add(pair.Key);

            keys.Sort(StringComparer.Ordinal);
            return keys.ToArray();
        }

        // Lock-free, weakly consistent enumeration (like the
        // underlying dictionaries).
        public IEnumerable<string> Keys
        {
            get
            {
                foreach (var shard in _shards)
                {
                    foreach (var pair in shard)
                        yield return pair.Key;
                }
            }
        }

        public IEnumerable<KeyValuePair<string, TValue>> Pairs
        {
            get
            {
                foreach (var shard in _shards)
                {
                    foreach (var pair in shard)
                        yield return pair;
                }
            }
        }
    }
}
