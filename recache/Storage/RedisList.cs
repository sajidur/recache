using System.Collections.Generic;

namespace recache.Storage
{
    public sealed class RedisList
    {
        // A growable ring buffer: O(1) push/pop at both ends and O(1)
        // indexing. The previous List<byte[]> made every LPUSH/LPOP
        // an O(n) shift of the whole list.
        private byte[][] _items = new byte[4][];
        private int _head;
        private int _count;
        private readonly object _gate = new();

        public int Count
        {
            get { lock (_gate) return _count; }
        }

        public long PushLeft(byte[] value)
        {
            lock (_gate)
            {
                EnsureCapacity(_count + 1);
                _head = (_head - 1 + _items.Length) % _items.Length;
                _items[_head] = value;
                _count++;
                return value.Length;
            }
        }

        public long PushRight(byte[] value)
        {
            lock (_gate)
            {
                EnsureCapacity(_count + 1);
                _items[(_head + _count) % _items.Length] = value;
                _count++;
                return value.Length;
            }
        }

        // Returns the removed value (and its byte length via
        // byteDelta, always negative) or null if the list was empty.
        public byte[]? PopLeft(out long byteDelta)
        {
            lock (_gate)
            {
                if (_count == 0)
                {
                    byteDelta = 0;
                    return null;
                }

                var value = _items[_head];
                _items[_head] = null!;
                _head = (_head + 1) % _items.Length;
                _count--;
                byteDelta = -value.Length;
                return value;
            }
        }

        public byte[]? PopRight(out long byteDelta)
        {
            lock (_gate)
            {
                if (_count == 0)
                {
                    byteDelta = 0;
                    return null;
                }

                var index = (_head + _count - 1) % _items.Length;
                var value = _items[index];
                _items[index] = null!;
                _count--;
                byteDelta = -value.Length;
                return value;
            }
        }

        public byte[][] Range(long start, long stop)
        {
            lock (_gate)
            {
                var (s, e) = NormalizeRange(start, stop, _count);

                if (_count == 0 || s > e)
                    return Array.Empty<byte[]>();

                var result = new byte[e - s + 1][];

                for (var i = 0; i < result.Length; i++)
                    result[i] = At(s + i);

                return result;
            }
        }

        public byte[]? Index(long index)
        {
            lock (_gate)
            {
                var i = index < 0 ? _count + index : index;

                if (i < 0 || i >= _count)
                    return null;

                return At((int)i);
            }
        }

        public bool SetAt(long index, byte[] value, out long byteDelta)
        {
            lock (_gate)
            {
                var i = index < 0 ? _count + index : index;

                if (i < 0 || i >= _count)
                {
                    byteDelta = 0;
                    return false;
                }

                var slot = (_head + (int)i) % _items.Length;
                var old = _items[slot];
                byteDelta = value.Length - old.Length;
                _items[slot] = value;
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
                var items = new List<byte[]>(_count);

                for (var i = 0; i < _count; i++)
                    items.Add(At(i));

                var removed = 0L;
                var delta = 0L;

                if (count >= 0)
                {
                    var limit = count == 0 ? long.MaxValue : count;

                    for (var i = 0; i < items.Count && removed < limit;)
                    {
                        if (ByteArrayComparer.Instance.Equals(items[i], value))
                        {
                            delta -= items[i].Length;
                            items.RemoveAt(i);
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
                    for (var i = items.Count - 1;
                         i >= 0 && removed < -count;
                         i--)
                    {
                        if (ByteArrayComparer.Instance.Equals(items[i], value))
                        {
                            delta -= items[i].Length;
                            items.RemoveAt(i);
                            removed++;
                        }
                    }
                }

                if (removed > 0)
                {
                    _items = new byte[Math.Max(4, items.Count)][];
                    items.CopyTo(_items);
                    _head = 0;
                    _count = items.Count;
                }

                byteDelta = delta;
                return removed;
            }
        }

        private byte[] At(int logicalIndex)
            => _items[(_head + logicalIndex) % _items.Length];

        private void EnsureCapacity(int required)
        {
            if (required <= _items.Length)
                return;

            var grown = new byte[_items.Length * 2][];

            for (var i = 0; i < _count; i++)
                grown[i] = At(i);

            _items = grown;
            _head = 0;
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
