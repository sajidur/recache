using System.Collections.Generic;
using System.Text;

namespace recache.Storage
{
    public sealed class RedisHash
    {
        // Segments itself once large, so a million-field hash never
        // rehashes all of its fields in one go (see SegmentedDictionary).
        private readonly SegmentedDictionary<string, byte[]> _fields =
            new(StringComparer.Ordinal);

        private readonly object _gate = new();

        public int Count
        {
            get { lock (_gate) return _fields.Count; }
        }

        // Returns true if a new field was created (vs. an existing
        // one overwritten), and the resulting change in byte size.
        public bool Set(string field, byte[] value, out long byteDelta)
        {
            lock (_gate)
            {
                if (!_fields.Set(field, value, out var old))
                {
                    byteDelta = value.Length - old!.Length;
                    return false;
                }

                byteDelta =
                    Encoding.UTF8.GetByteCount(field) +
                    value.Length;

                return true;
            }
        }

        public bool Remove(string field, out long byteDelta)
        {
            lock (_gate)
            {
                if (_fields.Remove(field, out var old))
                {
                    byteDelta =
                        -(Encoding.UTF8.GetByteCount(field) +
                          old.Length);

                    return true;
                }

                byteDelta = 0;
                return false;
            }
        }

        public bool TryGet(string field, out byte[]? value)
        {
            lock (_gate)
            {
                return _fields.TryGetValue(field, out value!);
            }
        }

        public bool ContainsField(string field)
        {
            lock (_gate)
            {
                return _fields.ContainsKey(field);
            }
        }

        public KeyValuePair<string, byte[]>[] Snapshot()
        {
            lock (_gate)
            {
                var result =
                    new KeyValuePair<string, byte[]>[_fields.Count];

                var i = 0;

                foreach (var kvp in _fields.Pairs)
                    result[i++] = kvp;

                return result;
            }
        }
    }
}
