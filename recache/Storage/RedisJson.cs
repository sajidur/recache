using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;

namespace recache.Storage
{
    public sealed class RedisJson
    {
        private JsonNode? _root;
        private bool _hasRoot;
        private readonly object _gate = new();

        public bool TrySet(string path, JsonNode? value, out long byteDelta)
        {
            lock (_gate)
            {
                var before = SizeUnlocked();
                var segments = JsonPath.Parse(path);
                bool ok;

                if (segments.Count == 0)
                {
                    _root = value;
                    _hasRoot = true;
                    ok = true;
                }
                else if (!_hasRoot)
                {
                    ok = false;
                }
                else
                {
                    ok = JsonPath.TrySet(_root!, segments, value);
                }

                byteDelta = SizeUnlocked() - before;
                return ok;
            }
        }

        public bool PathExists(string path)
        {
            lock (_gate)
            {
                if (!_hasRoot)
                    return false;

                var segments = JsonPath.Parse(path);

                return segments.Count == 0 ||
                       JsonPath.Query(_root, segments).Count > 0;
            }
        }

        // Single-path form returns the raw value (or a JSON array
        // when the path contains a wildcard). The multi-path form
        // returns an object mapping each path to its result array,
        // matching RedisJSON's JSON.GET with multiple paths.
        public bool TryGet(IReadOnlyList<string> paths, out string? json)
        {
            lock (_gate)
            {
                if (!_hasRoot)
                {
                    json = null;
                    return false;
                }

                if (paths.Count == 0)
                {
                    json = _root?.ToJsonString() ?? "null";
                    return true;
                }

                if (paths.Count == 1)
                {
                    var segments = JsonPath.Parse(paths[0]);

                    if (segments.Count == 0)
                    {
                        json = _root?.ToJsonString() ?? "null";
                        return true;
                    }

                    var results = JsonPath.Query(_root, segments);

                    if (results.Count == 0)
                    {
                        json = null;
                        return false;
                    }

                    json = JsonPath.HasWildcard(segments)
                        ? ToJsonArray(results)
                        : results[0]?.ToJsonString() ?? "null";

                    return true;
                }

                var parts = paths.Select(path =>
                {
                    var segments = JsonPath.Parse(path);

                    var results = segments.Count == 0
                        ? new List<JsonNode?> { _root }
                        : JsonPath.Query(_root, segments);

                    var key = JsonValue.Create(path)!.ToJsonString();

                    return $"{key}:{ToJsonArray(results)}";
                });

                json = "{" + string.Join(",", parts) + "}";
                return true;
            }
        }

        public int Delete(string path)
        {
            lock (_gate)
            {
                if (!_hasRoot)
                    return 0;

                var segments = JsonPath.Parse(path);

                if (segments.Count == 0)
                {
                    _root = null;
                    _hasRoot = false;
                    return 1;
                }

                return JsonPath.Delete(_root, segments) ? 1 : 0;
            }
        }

        public bool TryGetType(string path, out string? type)
        {
            lock (_gate)
            {
                var node = ResolveSingleUnlocked(path, out var found);

                if (!found)
                {
                    type = null;
                    return false;
                }

                type = JsonPath.TypeName(node);
                return true;
            }
        }

        public bool TryObjKeys(string path, out string[]? keys)
        {
            lock (_gate)
            {
                var node = ResolveSingleUnlocked(path, out _);

                if (node is not JsonObject obj)
                {
                    keys = null;
                    return false;
                }

                keys = obj.Select(kv => kv.Key).ToArray();
                return true;
            }
        }

        public bool TryStrLen(string path, out int length)
        {
            lock (_gate)
            {
                var node = ResolveSingleUnlocked(path, out _);

                if (node is JsonValue v && v.TryGetValue<string>(out var s))
                {
                    length = s.Length;
                    return true;
                }

                length = 0;
                return false;
            }
        }

        public bool TryArrLen(string path, out int length)
        {
            lock (_gate)
            {
                var node = ResolveSingleUnlocked(path, out _);

                if (node is JsonArray arr)
                {
                    length = arr.Count;
                    return true;
                }

                length = 0;
                return false;
            }
        }

        public bool TryObjLen(string path, out int length)
        {
            lock (_gate)
            {
                var node = ResolveSingleUnlocked(path, out _);

                if (node is JsonObject obj)
                {
                    length = obj.Count;
                    return true;
                }

                length = 0;
                return false;
            }
        }

        public bool TryArrAppend(
            string path,
            IReadOnlyList<JsonNode?> values,
            out int newLength,
            out long byteDelta)
        {
            lock (_gate)
            {
                var before = SizeUnlocked();
                var node = ResolveSingleUnlocked(path, out _);

                if (node is not JsonArray arr)
                {
                    newLength = 0;
                    byteDelta = 0;
                    return false;
                }

                foreach (var value in values)
                    arr.Add(value);

                newLength = arr.Count;
                byteDelta = SizeUnlocked() - before;
                return true;
            }
        }

        public bool TryNumIncrBy(
            string path,
            double amount,
            out double result,
            out long byteDelta)
        {
            lock (_gate)
            {
                var before = SizeUnlocked();
                var node = ResolveSingleUnlocked(path, out var found);

                if (!found ||
                    node is not JsonValue value ||
                    !value.TryGetValue<double>(out var current))
                {
                    result = 0;
                    byteDelta = 0;
                    return false;
                }

                result = current + amount;
                var newNode = JsonValue.Create(result);

                var segments = JsonPath.Parse(path);

                var ok = segments.Count == 0
                    ? SetRootUnlocked(newNode)
                    : JsonPath.TrySet(_root!, segments, newNode);

                byteDelta = SizeUnlocked() - before;
                return ok;
            }
        }

        private bool SetRootUnlocked(JsonNode? value)
        {
            _root = value;
            _hasRoot = true;
            return true;
        }

        // Resolves a path to a single node. `found` distinguishes
        // "path doesn't exist" from "path exists and its value is
        // JSON null".
        private JsonNode? ResolveSingleUnlocked(string path, out bool found)
        {
            if (!_hasRoot)
            {
                found = false;
                return null;
            }

            var segments = JsonPath.Parse(path);

            if (segments.Count == 0)
            {
                found = true;
                return _root;
            }

            var results = JsonPath.Query(_root, segments);

            found = results.Count > 0;
            return found ? results[0] : null;
        }

        private static string ToJsonArray(IEnumerable<JsonNode?> nodes)
        {
            return "[" + string.Join(
                ",",
                nodes.Select(n => n?.ToJsonString() ?? "null")) + "]";
        }

        private long SizeUnlocked() =>
            _hasRoot ? (_root?.ToJsonString().Length ?? 4) : 0;
    }
}
