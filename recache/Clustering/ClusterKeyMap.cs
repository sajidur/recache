using System;
using System.Collections.Generic;

namespace recache.Clustering
{
    // Which argument positions of a command are keys, using the
    // same First/Last/Step convention as Redis's COMMAND INFO
    // (Last == -1 means "through the end of args"). Commands not
    // present here (PING, INFO, CLIENT, CONFIG, COMMAND, SELECT,
    // DBSIZE, ECHO, SCAN, MODULE, AUTH, MULTI, EXEC, DISCARD,
    // UNWATCH, CLUSTER, ASKING) take no keys and are never subject
    // to slot-ownership checks.
    public readonly record struct KeySpec(int First, int Last, int Step);

    public static class ClusterKeyMap
    {
        private static readonly Dictionary<string, KeySpec> Map =
            new(StringComparer.Ordinal)
        {
            ["GET"] = new KeySpec(1, 1, 1),
            ["SET"] = new KeySpec(1, 1, 1),
            ["SETEX"] = new KeySpec(1, 1, 1),
            ["PSETEX"] = new KeySpec(1, 1, 1),
            ["SETNX"] = new KeySpec(1, 1, 1),
            ["STRLEN"] = new KeySpec(1, 1, 1),
            ["GETRANGE"] = new KeySpec(1, 1, 1),
            ["SETRANGE"] = new KeySpec(1, 1, 1),
            ["APPEND"] = new KeySpec(1, 1, 1),
            ["INCR"] = new KeySpec(1, 1, 1),
            ["DECR"] = new KeySpec(1, 1, 1),
            ["HSCAN"] = new KeySpec(1, 1, 1),
            ["SSCAN"] = new KeySpec(1, 1, 1),
            ["ZSCAN"] = new KeySpec(1, 1, 1),
            ["TYPE"] = new KeySpec(1, 1, 1),
            ["TTL"] = new KeySpec(1, 1, 1),
            ["PTTL"] = new KeySpec(1, 1, 1),
            ["EXPIRE"] = new KeySpec(1, 1, 1),
            ["PEXPIRE"] = new KeySpec(1, 1, 1),
            ["PERSIST"] = new KeySpec(1, 1, 1),
            ["HSET"] = new KeySpec(1, 1, 1),
            ["HGET"] = new KeySpec(1, 1, 1),
            ["HGETALL"] = new KeySpec(1, 1, 1),
            ["HDEL"] = new KeySpec(1, 1, 1),
            ["HEXISTS"] = new KeySpec(1, 1, 1),
            ["HLEN"] = new KeySpec(1, 1, 1),
            ["HKEYS"] = new KeySpec(1, 1, 1),
            ["HVALS"] = new KeySpec(1, 1, 1),
            ["HMGET"] = new KeySpec(1, 1, 1),
            ["LPUSH"] = new KeySpec(1, 1, 1),
            ["RPUSH"] = new KeySpec(1, 1, 1),
            ["LRANGE"] = new KeySpec(1, 1, 1),
            ["LLEN"] = new KeySpec(1, 1, 1),
            ["LPOP"] = new KeySpec(1, 1, 1),
            ["RPOP"] = new KeySpec(1, 1, 1),
            ["LREM"] = new KeySpec(1, 1, 1),
            ["LSET"] = new KeySpec(1, 1, 1),
            ["LINDEX"] = new KeySpec(1, 1, 1),
            ["SADD"] = new KeySpec(1, 1, 1),
            ["SREM"] = new KeySpec(1, 1, 1),
            ["SMEMBERS"] = new KeySpec(1, 1, 1),
            ["SISMEMBER"] = new KeySpec(1, 1, 1),
            ["SCARD"] = new KeySpec(1, 1, 1),
            ["ZADD"] = new KeySpec(1, 1, 1),
            ["ZREM"] = new KeySpec(1, 1, 1),
            ["ZSCORE"] = new KeySpec(1, 1, 1),
            ["ZRANGE"] = new KeySpec(1, 1, 1),
            ["ZCARD"] = new KeySpec(1, 1, 1),
            ["ZRANK"] = new KeySpec(1, 1, 1),
            ["JSON.SET"] = new KeySpec(1, 1, 1),
            ["JSON.GET"] = new KeySpec(1, 1, 1),
            ["JSON.DEL"] = new KeySpec(1, 1, 1),
            ["JSON.FORGET"] = new KeySpec(1, 1, 1),
            ["JSON.TYPE"] = new KeySpec(1, 1, 1),
            ["JSON.OBJKEYS"] = new KeySpec(1, 1, 1),
            ["JSON.STRLEN"] = new KeySpec(1, 1, 1),
            ["JSON.ARRLEN"] = new KeySpec(1, 1, 1),
            ["JSON.OBJLEN"] = new KeySpec(1, 1, 1),
            ["JSON.ARRAPPEND"] = new KeySpec(1, 1, 1),
            ["JSON.NUMINCRBY"] = new KeySpec(1, 1, 1),

            ["DEL"] = new KeySpec(1, -1, 1),
            ["EXISTS"] = new KeySpec(1, -1, 1),
            ["WATCH"] = new KeySpec(1, -1, 1),
        };

        public static IReadOnlyList<int> GetKeyIndexes(
            string command,
            int argCount)
        {
            if (!Map.TryGetValue(command, out var spec))
                return Array.Empty<int>();

            var last = spec.Last == -1 ? argCount - 1 : spec.Last;

            if (last < spec.First || last >= argCount)
                return Array.Empty<int>();

            var result = new List<int>();

            for (var i = spec.First; i <= last; i += spec.Step)
                result.Add(i);

            return result;
        }
    }
}
