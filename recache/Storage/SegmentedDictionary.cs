using System.Collections.Generic;

namespace recache.Storage
{
    // A Dictionary that splits itself into independently growing
    // segments once it gets big — the storage behind large hashes,
    // sets and sorted sets.
    //
    // A plain Dictionary resizes by rehashing every entry at once; for
    // a million-field hash that's a stall of tens to hundreds of
    // milliseconds, taken while the key's lock is held, so every client
    // touching that key waits. Redis avoids the same thing with
    // incremental rehashing. Here, past SplitThreshold entries the
    // dictionary is split once into 64 segments, after which each
    // resize touches 1/64 of the entries. Small collections (the vast
    // majority) stay a single Dictionary and pay nothing.
    //
    // Not thread-safe: the owning collection serializes access.
    internal sealed class SegmentedDictionary<TKey, TValue>
        where TKey : notnull
    {
        private const int SplitThreshold = 8192;
        private const int SegmentCount = 64;

        private readonly IEqualityComparer<TKey> _comparer;
        private Dictionary<TKey, TValue>? _single;
        private Dictionary<TKey, TValue>[]? _segments;
        private int _count;

        public SegmentedDictionary(IEqualityComparer<TKey> comparer)
        {
            _comparer = comparer;
            _single = new Dictionary<TKey, TValue>(comparer);
        }

        public int Count => _count;

        private Dictionary<TKey, TValue> For(TKey key)
            => _single ?? _segments![(_comparer.GetHashCode(key) & 0x7FFFFFFF) % SegmentCount];

        public bool TryGetValue(TKey key, out TValue value)
            => For(key).TryGetValue(key, out value!);

        public bool ContainsKey(TKey key)
            => For(key).ContainsKey(key);

        // Returns true if the key was added (false: replaced).
        public bool Set(TKey key, TValue value, out TValue? previous)
        {
            var dictionary = For(key);

            if (dictionary.TryGetValue(key, out var old))
            {
                previous = old;
                dictionary[key] = value;
                return false;
            }

            previous = default;
            dictionary[key] = value;
            _count++;

            if (_single is not null && _count > SplitThreshold)
                Split();

            return true;
        }

        public bool Remove(TKey key, out TValue value)
        {
            if (!For(key).Remove(key, out value!))
                return false;

            _count--;
            return true;
        }

        public IEnumerable<KeyValuePair<TKey, TValue>> Pairs
        {
            get
            {
                if (_single is not null)
                {
                    foreach (var pair in _single)
                        yield return pair;

                    yield break;
                }

                foreach (var segment in _segments!)
                {
                    foreach (var pair in segment)
                        yield return pair;
                }
            }
        }

        private void Split()
        {
            var segments = new Dictionary<TKey, TValue>[SegmentCount];

            for (var i = 0; i < segments.Length; i++)
                segments[i] = new Dictionary<TKey, TValue>(_count / SegmentCount * 2, _comparer);

            foreach (var (key, value) in _single!)
                segments[(_comparer.GetHashCode(key) & 0x7FFFFFFF) % SegmentCount][key] = value;

            _segments = segments;
            _single = null;
        }
    }
}
