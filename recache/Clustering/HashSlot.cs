using System;
using System.Text;

namespace recache.Clustering
{
    public static class HashSlot
    {
        public const int SlotCount = 16384;

        public static int Compute(string key)
        {
            var tagged = ExtractHashTag(key);
            var bytes = Encoding.UTF8.GetBytes(tagged);

            return Crc16.Compute(bytes) & (SlotCount - 1);
        }

        // Redis hashtag rule: if the key contains '{', and there is
        // a '}' after it with at least one character in between,
        // only that substring is hashed — lets callers co-locate
        // related keys onto the same slot deliberately.
        private static string ExtractHashTag(string key)
        {
            var open = key.IndexOf('{');

            if (open < 0)
                return key;

            var close = key.IndexOf('}', open + 1);

            if (close <= open + 1)
                return key;

            return key.Substring(open + 1, close - open - 1);
        }
    }
}
