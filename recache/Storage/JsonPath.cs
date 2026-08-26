using System.Collections.Generic;
using System.Text.Json.Nodes;

namespace recache.Storage
{
    // A pragmatic JSONPath subset: root "$", dotted/bracketed field
    // access ($.a.b, $['a']['b']), array indices (including
    // negative, e.g. [-1]), and single-level wildcards (.* / [*]).
    // Not supported: recursive descent (..), filter expressions
    // ([?(...)]), slices, and unions — RedisJSON's less common,
    // more elaborate query features.
    public static class JsonPath
    {
        public static List<JsonPathSegment> Parse(string path)
        {
            var segments = new List<JsonPathSegment>();
            var span = path.AsSpan();
            var i = 0;

            if (span.Length == 0)
                return segments;

            if (span[0] == '$')
                i = 1;

            while (i < span.Length)
            {
                var c = span[i];

                if (c == '.')
                {
                    i++;

                    if (i < span.Length && span[i] == '*')
                    {
                        segments.Add(new JsonWildcardSegment());
                        i++;
                        continue;
                    }

                    var start = i;

                    while (i < span.Length &&
                           span[i] != '.' &&
                           span[i] != '[')
                    {
                        i++;
                    }

                    if (i == start)
                    {
                        throw new JsonPathException(
                            $"Invalid path: expected a field name " +
                            $"near position {i}");
                    }

                    segments.Add(
                        new JsonFieldSegment(span[start..i].ToString()));
                }
                else if (c == '[')
                {
                    i++;

                    if (i >= span.Length)
                        throw new JsonPathException("Unterminated '[' in path");

                    if (span[i] == '*')
                    {
                        i++;

                        if (i >= span.Length || span[i] != ']')
                            throw new JsonPathException("Expected ']' after '[*'");

                        i++;
                        segments.Add(new JsonWildcardSegment());
                    }
                    else if (span[i] == '\'' || span[i] == '"')
                    {
                        var quote = span[i];
                        i++;
                        var start = i;

                        while (i < span.Length && span[i] != quote)
                            i++;

                        if (i >= span.Length)
                        {
                            throw new JsonPathException(
                                "Unterminated quoted field in path");
                        }

                        var name = span[start..i].ToString();
                        i++;

                        if (i >= span.Length || span[i] != ']')
                        {
                            throw new JsonPathException(
                                "Expected ']' after quoted field");
                        }

                        i++;
                        segments.Add(new JsonFieldSegment(name));
                    }
                    else
                    {
                        var start = i;

                        while (i < span.Length && span[i] != ']')
                            i++;

                        if (i >= span.Length)
                            throw new JsonPathException("Unterminated '[' in path");

                        var numberText = span[start..i].ToString();

                        if (!int.TryParse(numberText, out var index))
                        {
                            throw new JsonPathException(
                                $"Invalid array index '{numberText}' " +
                                $"in path");
                        }

                        i++;
                        segments.Add(new JsonIndexSegment(index));
                    }
                }
                else
                {
                    throw new JsonPathException(
                        $"Unexpected character '{c}' in path");
                }
            }

            return segments;
        }

        public static bool HasWildcard(
            IReadOnlyList<JsonPathSegment> segments)
        {
            foreach (var segment in segments)
            {
                if (segment is JsonWildcardSegment)
                    return true;
            }

            return false;
        }

        // Returns every node matched by the path. An empty result
        // means "no match" — distinct from a match whose value is
        // JSON null, which yields a single `null` entry.
        public static List<JsonNode?> Query(
            JsonNode? root,
            IReadOnlyList<JsonPathSegment> segments)
        {
            var current = new List<JsonNode?> { root };

            foreach (var segment in segments)
            {
                var next = new List<JsonNode?>();

                foreach (var node in current)
                {
                    switch (segment)
                    {
                        case JsonFieldSegment f:
                            if (node is JsonObject obj &&
                                obj.TryGetPropertyValue(f.Name, out var v))
                            {
                                next.Add(v);
                            }

                            break;

                        case JsonIndexSegment idx:
                            if (node is JsonArray arr)
                            {
                                var i = idx.Index < 0
                                    ? arr.Count + idx.Index
                                    : idx.Index;

                                if (i >= 0 && i < arr.Count)
                                    next.Add(arr[i]);
                            }

                            break;

                        case JsonWildcardSegment:
                            if (node is JsonObject wobj)
                            {
                                foreach (var kv in wobj)
                                    next.Add(kv.Value);
                            }
                            else if (node is JsonArray warr)
                            {
                                foreach (var item in warr)
                                    next.Add(item);
                            }

                            break;
                    }
                }

                current = next;
            }

            return current;
        }

        // Navigates to the parent of the final (non-wildcard)
        // segment and sets/overwrites it. All segments except the
        // last must already resolve to an existing object/array —
        // this does not auto-create missing intermediate structure,
        // matching real RedisJSON's JSON.SET.
        public static bool TrySet(
            JsonNode root,
            IReadOnlyList<JsonPathSegment> segments,
            JsonNode? value)
        {
            if (segments.Count == 0)
                return false;

            JsonNode? current = root;

            for (var i = 0; i < segments.Count - 1; i++)
            {
                current = Step(current, segments[i]);

                if (current == null)
                    return false;
            }

            switch (segments[^1])
            {
                case JsonFieldSegment f:
                    if (current is not JsonObject targetObj)
                        return false;

                    targetObj[f.Name] = value;
                    return true;

                case JsonIndexSegment idx:
                    if (current is not JsonArray targetArr)
                        return false;

                    var i = idx.Index < 0
                        ? targetArr.Count + idx.Index
                        : idx.Index;

                    if (i < 0 || i >= targetArr.Count)
                        return false;

                    targetArr[i] = value;
                    return true;

                default:
                    return false;
            }
        }

        public static bool Delete(
            JsonNode? root,
            IReadOnlyList<JsonPathSegment> segments)
        {
            if (segments.Count == 0 || root == null)
                return false;

            JsonNode? current = root;

            for (var i = 0; i < segments.Count - 1; i++)
            {
                current = Step(current, segments[i]);

                if (current == null)
                    return false;
            }

            switch (segments[^1])
            {
                case JsonFieldSegment f:
                    return current is JsonObject obj &&
                           obj.Remove(f.Name);

                case JsonIndexSegment idx:
                    if (current is not JsonArray arr)
                        return false;

                    var i = idx.Index < 0
                        ? arr.Count + idx.Index
                        : idx.Index;

                    if (i < 0 || i >= arr.Count)
                        return false;

                    arr.RemoveAt(i);
                    return true;

                default:
                    return false;
            }
        }

        public static string TypeName(JsonNode? node)
        {
            return node switch
            {
                null => "null",
                JsonObject => "object",
                JsonArray => "array",
                JsonValue v when v.TryGetValue<bool>(out _) => "boolean",
                JsonValue v when v.TryGetValue<long>(out _) => "integer",
                JsonValue v when v.TryGetValue<double>(out _) => "number",
                JsonValue => "string",
                _ => "null"
            };
        }

        private static JsonNode? Step(
            JsonNode? current,
            JsonPathSegment segment)
        {
            return segment switch
            {
                JsonFieldSegment f =>
                    current is JsonObject obj &&
                    obj.TryGetPropertyValue(f.Name, out var v)
                        ? v
                        : null,

                JsonIndexSegment idx when current is JsonArray arr =>
                    IndexOrNull(arr, idx.Index),

                _ => null
            };
        }

        private static JsonNode? IndexOrNull(JsonArray arr, int index)
        {
            var i = index < 0 ? arr.Count + index : index;
            return i >= 0 && i < arr.Count ? arr[i] : null;
        }
    }
}
