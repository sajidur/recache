using System.Collections.Generic;

namespace recache.Storage
{
    public sealed class RedisList
    {
        private readonly List<byte[]> _items = new();
        private readonly object _gate = new();

        public int Count
        {
            get { lock (_gate) return _items.Count; }
        }

        public long PushLeft(byte[] value)
        {
            lock (_gate)
            {
                _items.Insert(0, value);
                return value.Length;
            }
        }

        public long PushRight(byte[] value)
        {
            lock (_gate)
            {
                _items.Add(value);
                return value.Length;
            }
        }

        // Returns the removed value (and its byte length via
        // byteDelta, always negative) or null if the list was empty.
        public byte[]? PopLeft(out long byteDelta)
        {
            lock (_gate)
            {
                if (_items.Count == 0)
                {
                    byteDelta = 0;
                    return null;
                }

                var value = _items[0];
                _items.RemoveAt(0);
                byteDelta = -value.Length;
                return value;
            }
        }

        public byte[]? PopRight(out long byteDelta)
        {
            lock (_gate)
            {
                if (_items.Count == 0)
                {
                    byteDelta = 0;
                    return null;
                }

                var index = _items.Count - 1;
                var value = _items[index];
                _items.RemoveAt(index);
                byteDelta = -value.Length;
                return value;
            }
        }

        public byte[][] Range(long start, long stop)
        {
            lock (_gate)
            {
                var count = _items.Count;
                var (s, e) = NormalizeRange(start, stop, count);

                if (count == 0 || s > e)
                    return Array.Empty<byte[]>();

                return _items.GetRange(s, e - s + 1).ToArray();
            }
        }

        public byte[]? Index(long index)
        {
            lock (_gate)
            {
                var count = _items.Count;
                var i = index < 0 ? count + index : index;

                if (i < 0 || i >= count)
                    return null;

                return _items[(int)i];
            }
        }

        public bool SetAt(long index, byte[] value, out long byteDelta)
        {
            lock (_gate)
            {
                var count = _items.Count;
                var i = index < 0 ? count + index : index;

                if (i < 0 || i >= count)
                {
                    byteDelta = 0;
                    return false;
                }

                var old = _items[(int)i];
                byteDelta = value.Length - old.Length;
                _items[(int)i] = value;
                return true;
            }
        }

        // Removes up to |count| occurrences of value. count > 0 scans
        // head-to-tail, count < 0 scans tail-to-head, count == 0
        // removes all occurrences. Returns the number removed.
        public long Remove(byte[] value, long count, out long byteDelta)
        {
            lock (_gate)
            {
                var removed = 0L;
                var delta = 0L;

                if (count >= 0)
                {
                    var limit = count == 0 ? long.MaxValue : count;

                    for (var i = 0; i < _items.Count && removed < limit;)
                    {
                        if (ByteArrayComparer.Instance.Equals(
                                _items[i], value))
                        {
                            delta -= _items[i].Length;
                            _items.RemoveAt(i);
                            removed++;
                        }
                        else
                        {
                            i++;
                        }
                    }
                }
                else
                {
                    for (var i = _items.Count - 1;
                         i >= 0 && removed < -count;
                         i--)
                    {
                        if (ByteArrayComparer.Instance.Equals(
                                _items[i], value))
                        {
                            delta -= _items[i].Length;
                            _items.RemoveAt(i);
                            removed++;
                        }
                    }
                }

                byteDelta = delta;
                return removed;
            }
        }

        private static (int start, int end) NormalizeRange(
            long start,
            long stop,
            int count)
        {
            if (start < 0)
                start = Math.Max(count + start, 0);

            if (stop < 0)
                stop = count + stop;

            if (stop >= count)
                stop = count - 1;

            if (start > count)
                start = count;

            return ((int)Math.Min(start, int.MaxValue),
                    (int)Math.Min(Math.Max(stop, -1), int.MaxValue));
        }
    }
}
