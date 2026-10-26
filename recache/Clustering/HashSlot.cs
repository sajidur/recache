using System;
using System.Text;

namespace recache.Clustering
{
    public static class HashSlot
    {
        public const int SlotCount = 16384;

        public static int Compute(string key)
        {
            if (key.Length <= 256)
            {
                Span<byte> bytes = stackalloc byte[Encoding.UTF8.GetMaxByteCount(key.Length)];
                var count = Encoding.UTF8.GetBytes(key, bytes);
                return Compute(bytes[..count]);
            }

            return Compute(Encoding.UTF8.GetBytes(key));
        }

        // Straight from the request's key bytes — no string needed.
        public static int Compute(ReadOnlySpan<byte> key)
            => Crc16.Compute(ExtractHashTag(key)) & (SlotCount - 1);

        // Redis hashtag rule: if the key contains '{', and there is
        // a '}' after it with at least one character in between,
        // only that substring is hashed — lets callers co-locate
        // related keys onto the same slot deliberately.
        private static ReadOnlySpan<byte> ExtractHashTag(ReadOnlySpan<byte> key)
        {
            var open = key.IndexOf((byte)'{');

            if (open < 0)
                return key;

            var close = key[(open + 1)..].IndexOf((byte)'}');

            if (close <= 0)
                return key;

            return key.Slice(open + 1, close);
        }
    }
}
