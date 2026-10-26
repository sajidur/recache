using recache.Protocol;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace recache.Commands
{
    [Flags]
    public enum CommandFlags
    {
        None = 0,

        // Modifies the keyspace: runs under exclusive key locks, is
        // propagated to the AOF and replicas, is refused on a
        // replica, and waits out a write pause (CLIENT PAUSE, or a
        // coordinated failover handing this node's slots over).
        Write = 1 << 0,

        // Reads keys: runs under shared key locks; a replica serves
        // it in cluster mode after READONLY.
        ReadOnly = 1 << 1,

        Admin = 1 << 2,

        Fast = 1 << 3,

        // Touches every key (FLUSHALL): locks the whole keyspace.
        AllKeys = 1 << 4,

        // Connection/session-level: handled before the normal
        // execution path and never queued by MULTI.
        Session = 1 << 5,

        // Rejected between MULTI and EXEC.
        NoMulti = 1 << 6,
    }

    public sealed class CommandInfo
    {
        public CommandInfo(
            string name,
            int arity,
            CommandFlags flags,
            int firstKey,
            int lastKey,
            int step,
            Func<IReadOnlyList<RespValue>, IReadOnlyList<int>>? keyExtractor = null)
        {
            Name = name;
            NameValue = RespValue.Bulk(name);
            Arity = arity;
            Flags = flags;
            FirstKey = firstKey;
            LastKey = lastKey;
            Step = step;
            KeyExtractor = keyExtractor;
        }

        public string Name { get; }

        // Shared argv[0] for requests naming this command (see
        // RespParser.CommandNameInterner).
        public RespValue NameValue { get; }

        // Redis convention: positive = exact argument count
        // (including the command name), negative = at least |arity|.
        public int Arity { get; }

        public CommandFlags Flags { get; }

        public int FirstKey { get; }

        // -1 = through the last argument.
        public int LastKey { get; }

        public int Step { get; }

        // For the few commands whose keys can't be described by a
        // first/last/step range (MIGRATE's "KEYS k1 k2 ..." form).
        public Func<IReadOnlyList<RespValue>, IReadOnlyList<int>>? KeyExtractor { get; }

        public bool IsWrite => (Flags & CommandFlags.Write) != 0;

        public bool HasKeys => FirstKey > 0 || KeyExtractor is not null;

        public IReadOnlyList<int> GetKeyIndexes(IReadOnlyList<RespValue> args)
        {
            if (KeyExtractor is not null)
                return KeyExtractor(args);

            if (FirstKey <= 0 || args.Count <= FirstKey)
                return Array.Empty<int>();

            var last = LastKey < 0 ? args.Count + LastKey : LastKey;

            if (last >= args.Count)
                last = args.Count - 1;

            if (last < FirstKey)
                return Array.Empty<int>();

            if (last == FirstKey)
                return SingleKeyIndexes[FirstKey];

            var result = new List<int>((last - FirstKey) / Step + 1);

            for (var i = FirstKey; i <= last; i += Step)
                result.Add(i);

            return result;
        }

        private static readonly int[][] SingleKeyIndexes =
        {
            Array.Empty<int>(),
            new[] { 1 },
            new[] { 2 },
            new[] { 3 },
        };
    }

    public static class CommandTable
    {
        private const CommandFlags W = CommandFlags.Write;
        private const CommandFlags R = CommandFlags.ReadOnly;
        private const CommandFlags F = CommandFlags.Fast;
        private const CommandFlags A = CommandFlags.Admin;
        private const CommandFlags S = CommandFlags.Session;

        private static readonly Dictionary<string, CommandInfo> Commands =
            Build();

        private static readonly Dictionary<string, CommandInfo>.AlternateLookup<ReadOnlySpan<char>>
            SpanLookup = Commands.GetAlternateLookup<ReadOnlySpan<char>>();

        static CommandTable()
        {
            RespParser.CommandNameInterner = name => Lookup(name)?.NameValue;
        }

        // Forces the static constructor (and so the parser's
        // command-name interning) to run at startup.
        public static void Initialize()
        {
        }

        public static IEnumerable<CommandInfo> All => Commands.Values;

        public static int Count => Commands.Count;

        public static CommandInfo? Lookup(string name)
            => Commands.TryGetValue(name, out var info) ? info : null;

        // Names of up to 8 bytes (nearly every hot command) packed into
        // one uppercase ulong: the lookup is a single integer-keyed
        // dictionary probe.
        private static readonly Dictionary<ulong, CommandInfo> PackedLookup =
            Commands.Values
                .Where(c => c.Name.Length <= 8)
                .ToDictionary(c => Pack(Encoding.ASCII.GetBytes(c.Name)));

        private static ulong Pack(ReadOnlySpan<byte> name)
        {
            ulong packed = 0;

            foreach (var raw in name)
            {
                var b = raw;

                if ((uint)(b - 'a') < 26u)
                    b -= 32;

                packed = (packed << 8) | b;
            }

            return packed;
        }

        // Case-insensitive lookup straight from the request bytes,
        // without allocating a string — this runs once per command.
        public static CommandInfo? Lookup(ReadOnlySpan<byte> name)
        {
            if (name.Length is > 0 and <= 8)
                return PackedLookup.TryGetValue(Pack(name), out var packed) ? packed : null;

            if (name.Length == 0 || name.Length > 32)
                return null;

            Span<char> chars = stackalloc char[name.Length];

            for (var i = 0; i < name.Length; i++)
            {
                var b = name[i];

                if (b >= 0x80)
                    return null;

                chars[i] = (char)b;
            }

            return SpanLookup.TryGetValue(chars, out var info) ? info : null;
        }

        private static Dictionary<string, CommandInfo> Build()
        {
            var table = new Dictionary<string, CommandInfo>(
                StringComparer.OrdinalIgnoreCase);

            void Add(
                string name,
                int arity,
                CommandFlags flags,
                int first = 0,
                int last = 0,
                int step = 0,
                Func<IReadOnlyList<RespValue>, IReadOnlyList<int>>? extractor = null)
            {
                table[name] = new CommandInfo(
                    name,
                    arity,
                    flags,
                    first,
                    last,
                    step,
                    extractor);
            }

            // ---- Strings ----
            Add("GET", 2, R | F, 1, 1, 1);
            Add("SET", -3, W, 1, 1, 1);
            Add("SETEX", 4, W, 1, 1, 1);
            Add("PSETEX", 4, W, 1, 1, 1);
            Add("SETNX", 3, W | F, 1, 1, 1);
            Add("STRLEN", 2, R | F, 1, 1, 1);
            Add("GETRANGE", 4, R, 1, 1, 1);
            Add("SETRANGE", 4, W, 1, 1, 1);
            Add("APPEND", 3, W | F, 1, 1, 1);
            Add("INCR", 2, W | F, 1, 1, 1);
            Add("DECR", 2, W | F, 1, 1, 1);
            Add("INCRBY", 3, W | F, 1, 1, 1);
            Add("DECRBY", 3, W | F, 1, 1, 1);
            Add("MGET", -2, R | F, 1, -1, 1);
            Add("MSET", -3, W, 1, -1, 2);

            // ---- Generic keyspace ----
            Add("DEL", -2, W, 1, -1, 1);
            Add("UNLINK", -2, W | F, 1, -1, 1);
            Add("EXISTS", -2, R | F, 1, -1, 1);
            Add("TYPE", 2, R | F, 1, 1, 1);
            Add("TTL", 2, R | F, 1, 1, 1);
            Add("PTTL", 2, R | F, 1, 1, 1);
            Add("EXPIRETIME", 2, R | F, 1, 1, 1);
            Add("PEXPIRETIME", 2, R | F, 1, 1, 1);
            Add("EXPIRE", -3, W | F, 1, 1, 1);
            Add("PEXPIRE", -3, W | F, 1, 1, 1);
            Add("EXPIREAT", -3, W | F, 1, 1, 1);
            Add("PEXPIREAT", -3, W | F, 1, 1, 1);
            Add("PERSIST", 2, W | F, 1, 1, 1);
            Add("DUMP", 2, R, 1, 1, 1);
            Add("RESTORE", -4, W, 1, 1, 1);
            Add("RESTORE-ASKING", -4, W, 1, 1, 1);
            Add("MIGRATE", -6, W, 0, 0, 0, MigrateKeys);
            Add("SCAN", -2, R);
            Add("KEYS", 2, R);
            Add("DBSIZE", 1, R | F);
            Add("FLUSHALL", -1, W | CommandFlags.AllKeys);
            Add("FLUSHDB", -1, W | CommandFlags.AllKeys);

            // ---- Hashes ----
            Add("HSET", -4, W | F, 1, 1, 1);
            Add("HMSET", -4, W | F, 1, 1, 1);
            Add("HGET", 3, R | F, 1, 1, 1);
            Add("HGETALL", 2, R, 1, 1, 1);
            Add("HDEL", -3, W | F, 1, 1, 1);
            Add("HEXISTS", 3, R | F, 1, 1, 1);
            Add("HLEN", 2, R | F, 1, 1, 1);
            Add("HKEYS", 2, R, 1, 1, 1);
            Add("HVALS", 2, R, 1, 1, 1);
            Add("HMGET", -3, R | F, 1, 1, 1);
            Add("HSCAN", -3, R, 1, 1, 1);

            // ---- Lists ----
            Add("LPUSH", -3, W | F, 1, 1, 1);
            Add("RPUSH", -3, W | F, 1, 1, 1);
            Add("LRANGE", 4, R, 1, 1, 1);
            Add("LLEN", 2, R | F, 1, 1, 1);
            Add("LPOP", -2, W | F, 1, 1, 1);
            Add("RPOP", -2, W | F, 1, 1, 1);
            Add("LREM", 4, W, 1, 1, 1);
            Add("LSET", 4, W, 1, 1, 1);
            Add("LINDEX", 3, R, 1, 1, 1);

            // ---- Sets ----
            Add("SADD", -3, W | F, 1, 1, 1);
            Add("SREM", -3, W | F, 1, 1, 1);
            Add("SMEMBERS", 2, R, 1, 1, 1);
            Add("SISMEMBER", 3, R | F, 1, 1, 1);
            Add("SCARD", 2, R | F, 1, 1, 1);
            Add("SSCAN", -3, R, 1, 1, 1);

            // ---- Sorted sets ----
            Add("ZADD", -4, W | F, 1, 1, 1);
            Add("ZREM", -3, W | F, 1, 1, 1);
            Add("ZSCORE", 3, R | F, 1, 1, 1);
            Add("ZRANGE", -4, R, 1, 1, 1);
            Add("ZCARD", 2, R | F, 1, 1, 1);
            Add("ZRANK", 3, R | F, 1, 1, 1);
            Add("ZSCAN", -3, R, 1, 1, 1);

            // ---- JSON ----
            Add("JSON.SET", -4, W, 1, 1, 1);
            Add("JSON.GET", -2, R, 1, 1, 1);
            Add("JSON.DEL", -2, W, 1, 1, 1);
            Add("JSON.FORGET", -2, W, 1, 1, 1);
            Add("JSON.TYPE", -2, R, 1, 1, 1);
            Add("JSON.OBJKEYS", -2, R, 1, 1, 1);
            Add("JSON.STRLEN", -2, R, 1, 1, 1);
            Add("JSON.ARRLEN", -2, R, 1, 1, 1);
            Add("JSON.OBJLEN", -2, R, 1, 1, 1);
            Add("JSON.ARRAPPEND", -4, W, 1, 1, 1);
            Add("JSON.NUMINCRBY", 4, W, 1, 1, 1);

            // ---- Server / connection ----
            Add("PING", -1, F);
            Add("ECHO", 2, F);
            Add("TIME", 1, F);
            Add("INFO", -1, A);
            Add("CLIENT", -2, A);
            Add("CONFIG", -2, A);
            Add("COMMAND", -1, CommandFlags.None);
            Add("SELECT", 2, F);
            Add("MODULE", -2, A);
            Add("BGREWRITEAOF", 1, A | CommandFlags.NoMulti);
            Add("SHUTDOWN", -1, A | CommandFlags.NoMulti);
            Add("QUIT", -1, S | F);
            Add("AUTH", -2, S | F | CommandFlags.NoMulti);

            // ---- Transactions ----
            Add("MULTI", 1, S | F);
            Add("EXEC", 1, S);
            Add("DISCARD", 1, S | F);
            Add("WATCH", -2, S | F | CommandFlags.NoMulti, 1, -1, 1);
            Add("UNWATCH", 1, S | F);

            // ---- Replication ----
            Add("REPLICAOF", 3, A | CommandFlags.NoMulti);
            Add("SLAVEOF", 3, A | CommandFlags.NoMulti);
            Add("ROLE", 1, F);
            Add("WAIT", 3, S | CommandFlags.NoMulti);
            Add("REPLCONF", -1, A | CommandFlags.NoMulti);
            Add("PSYNC", -3, S | A | CommandFlags.NoMulti);
            Add("SYNC", 1, S | A | CommandFlags.NoMulti);

            // ---- Cluster ----
            Add("CLUSTER", -2, A);
            Add("ASKING", 1, S | F);
            Add("READONLY", 1, S | F);
            Add("READWRITE", 1, S | F);

            return table;
        }

        // MIGRATE host port key|"" db timeout [COPY] [REPLACE]
        //   [AUTH pw] [AUTH2 user pw] [KEYS key ...]
        private static IReadOnlyList<int> MigrateKeys(IReadOnlyList<RespValue> args)
        {
            if (args.Count < 6)
                return Array.Empty<int>();

            var single = args[3].BinaryValue;

            if (single is { Length: > 0 })
                return new[] { 3 };

            for (var i = 6; i < args.Count; i++)
            {
                var token = args[i].BinaryValue;

                if (token is null)
                    continue;

                if (Ascii.EqualsIgnoreCase(token, "AUTH"u8))
                {
                    i++;
                }
                else if (Ascii.EqualsIgnoreCase(token, "AUTH2"u8))
                {
                    i += 2;
                }
                else if (Ascii.EqualsIgnoreCase(token, "KEYS"u8))
                {
                    var keys = new List<int>(args.Count - i - 1);

                    for (var k = i + 1; k < args.Count; k++)
                        keys.Add(k);

                    return keys;
                }
            }

            return Array.Empty<int>();
        }
    }
}
