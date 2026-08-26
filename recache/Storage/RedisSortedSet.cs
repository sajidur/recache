using System.Collections.Generic;
using System.Text;

namespace recache.Storage
{
    public sealed class RedisSortedSet
    {
        private sealed class Entry
        {
            public required string Member;
            public double Score;
        }

        private static readonly EntryComparer Comparer = new();

        private sealed class EntryComparer : IComparer<Entry>
        {
            public int Compare(Entry? x, Entry? y)
            {
                if (x is null || y is null)
                    return 0;

                var byScore = x.Score.CompareTo(y.Score);

                return byScore != 0
                    ? byScore
                    : string.CompareOrdinal(x.Member, y.Member);
            }
        }

        private readonly Dictionary<string, Entry> _byMember =
            new(StringComparer.Ordinal);

        private readonly SortedSet<Entry> _byScore = new(Comparer);
        private readonly object _gate = new();

        public int Count
        {
            get { lock (_gate) return _byMember.Count; }
        }

        // Returns true if the member is new (vs. its score updated),
        // and the byte-size delta.
        public bool Add(string member, double score, out long byteDelta)
        {
            lock (_gate)
            {
                if (_byMember.TryGetValue(member, out var existing))
                {
                    _byScore.Remove(existing);
                    existing.Score = score;
                    _byScore.Add(existing);
                    byteDelta = 0;
                    return false;
                }

                var entry = new Entry
                {
                    Member = member,
                    Score = score
                };

                _byMember[member] = entry;
                _byScore.Add(entry);

                byteDelta =
                    Encoding.UTF8.GetByteCount(member) + sizeof(double);

                return true;
            }
        }

        public bool Remove(string member, out long byteDelta)
        {
            lock (_gate)
            {
                if (_byMember.Remove(member, out var entry))
                {
                    _byScore.Remove(entry);

                    byteDelta =
                        -(Encoding.UTF8.GetByteCount(member) +
                          sizeof(double));

                    return true;
                }

                byteDelta = 0;
                return false;
            }
        }

        public bool TryGetScore(string member, out double score)
        {
            lock (_gate)
            {
                if (_byMember.TryGetValue(member, out var entry))
                {
                    score = entry.Score;
                    return true;
                }

                score = 0;
                return false;
            }
        }

        // Zero-based rank in ascending score order, or -1 if absent.
        public long Rank(string member)
        {
            lock (_gate)
            {
                if (!_byMember.TryGetValue(member, out var target))
                    return -1;

                long rank = 0;

                foreach (var entry in _byScore)
                {
                    if (ReferenceEquals(entry, target))
                        return rank;

                    rank++;
                }

                return -1;
            }
        }

        public (string Member, double Score)[] Range(
            long start,
            long stop)
        {
            lock (_gate)
            {
                var count = _byScore.Count;

                if (start < 0)
                    start = Math.Max(count + start, 0);

                if (stop < 0)
                    stop = count + stop;

                if (stop >= count)
                    stop = count - 1;

                if (count == 0 || start > stop || start >= count)
                    return Array.Empty<(string, double)>();

                var result =
                    new (string, double)[stop - start + 1];

                long i = 0;
                var written = 0;

                foreach (var entry in _byScore)
                {
                    if (i >= start && i <= stop)
                        result[written++] = (entry.Member, entry.Score);

                    if (i > stop)
                        break;

                    i++;
                }

                return result;
            }
        }
    }
}
