using System.Collections.Generic;

namespace recache.Storage
{
    public sealed class RedisSet
    {
        // Segments itself once large, so a huge set never rehashes all
        // of its members in one go (see SegmentedDictionary).
        private readonly SegmentedDictionary<byte[], byte> _members =
            new(ByteArrayComparer.Instance);

        private readonly object _gate = new();

        public int Count
        {
            get { lock (_gate) return _members.Count; }
        }

        public bool Add(byte[] value, out long byteDelta)
        {
            lock (_gate)
            {
                if (!_members.ContainsKey(value))
                {
                    _members.Set(value, 0, out _);
                    byteDelta = value.Length;
                    return true;
                }

                byteDelta = 0;
                return false;
            }
        }

        public bool Remove(byte[] value, out long byteDelta)
        {
            lock (_gate)
            {
                if (_members.Remove(value, out _))
                {
                    byteDelta = -value.Length;
                    return true;
                }

                byteDelta = 0;
                return false;
            }
        }

        public bool Contains(byte[] value)
        {
            lock (_gate)
            {
                return _members.ContainsKey(value);
            }
        }

        public byte[][] Snapshot()
        {
            lock (_gate)
            {
                var result = new byte[_members.Count][];
                var i = 0;

                foreach (var pair in _members.Pairs)
                    result[i++] = pair.Key;

                return result;
            }
        }
    }
}
