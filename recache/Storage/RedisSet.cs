using System.Collections.Generic;

namespace recache.Storage
{
    public sealed class RedisSet
    {
        private readonly HashSet<byte[]> _members =
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
                if (_members.Add(value))
                {
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
                if (_members.Remove(value))
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
                return _members.Contains(value);
            }
        }

        public byte[][] Snapshot()
        {
            lock (_gate)
            {
                var result = new byte[_members.Count][];
                _members.CopyTo(result);
                return result;
            }
        }
    }
}
