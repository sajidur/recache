using recache.Clustering;
using recache.Protocol;
using recache.Storage;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net.Sockets;
using System.Text;

namespace recache.Commands
{
    // Strings and generic keyspace commands beyond the original set,
    // plus DUMP / RESTORE / MIGRATE — the primitives live resharding
    // (redis-cli --cluster reshard) is built on.
    public sealed partial class CommandDispatcher
    {
        private ValueTask<RespValue> IncrementBy(
            IReadOnlyList<RespValue> args,
            bool negate)
        {
            if (!long.TryParse(
                    GetString(args[2]),
                    NumberStyles.AllowLeadingSign,
                    CultureInfo.InvariantCulture,
                    out var amount))
            {
                return Error("ERR value is not an integer or out of range");
            }

            if (negate)
            {
                if (amount == long.MinValue)
                    return Error("ERR decrement would overflow");

                amount = -amount;
            }

            return ValueTask.FromResult(
                RespValue.Integer(
                    _store.Increment(GetString(args[1]), amount)));
        }

        private ValueTask<RespValue> MGet(IReadOnlyList<RespValue> args)
        {
            var result = new RespValue[args.Count - 1];

            for (var i = 1; i < args.Count; i++)
            {
                try
                {
                    result[i - 1] = _store.TryGet(GetString(args[i]), out var value)
                        ? RespValue.Bulk(value!)
                        : RespValue.Null();
                }
                catch (WrongTypeException)
                {
                    // MGET reports a non-string key as nil rather than
                    // failing the whole call, like Redis.
                    result[i - 1] = RespValue.Null();
                }
            }

            return ValueTask.FromResult(RespValue.Array(result));
        }

        private ValueTask<RespValue> MSet(IReadOnlyList<RespValue> args)
        {
            if (args.Count % 2 != 1)
                return Error("ERR wrong number of arguments for 'mset' command");

            // Every key's lock is held for the whole command, so no
            // other client sees some of these keys updated and others
            // not.
            for (var i = 1; i < args.Count; i += 2)
                _store.Set(GetString(args[i]), GetBytes(args[i + 1]));

            return ValueTask.FromResult(RespValue.Ok);
        }

        private ValueTask<RespValue> HMSet(IReadOnlyList<RespValue> args)
        {
            var result = HSet(args).Result;

            return ValueTask.FromResult(result.IsError ? result : RespValue.Ok);
        }

        private ValueTask<RespValue> ExpireAt(
            IReadOnlyList<RespValue> args,
            bool milliseconds)
        {
            if (args.Count != 3)
            {
                return Error(
                    $"ERR wrong number of arguments for " +
                    $"'{(milliseconds ? "pexpireat" : "expireat")}' command");
            }

            if (!long.TryParse(GetString(args[2]), out var when))
                return Error("ERR value is not an integer or out of range");

            var key = GetString(args[1]);
            var expiresAt = milliseconds ? when : when * 1000;

            // An instant already in the past deletes the key.
            if (expiresAt <= CacheStore.NowMs)
            {
                return ValueTask.FromResult(
                    RespValue.Integer(_store.Remove(key) ? 1 : 0));
            }

            return ValueTask.FromResult(
                RespValue.Integer(_store.TryExpireAt(key, expiresAt) ? 1 : 0));
        }

        private ValueTask<RespValue> ExpireTime(
            IReadOnlyList<RespValue> args,
            bool milliseconds)
        {
            if (!_store.TryGetExpireAt(GetString(args[1]), out var expiresAt))
                return ValueTask.FromResult(RespValue.Integer(-2));

            if (expiresAt is not { } ms)
                return ValueTask.FromResult(RespValue.Integer(-1));

            return ValueTask.FromResult(
                RespValue.Integer(milliseconds ? ms : ms / 1000));
        }

        private ValueTask<RespValue> KeysCommand(IReadOnlyList<RespValue> args)
        {
            var pattern = GetString(args[1]);
            var result = new List<RespValue>();

            foreach (var key in _store.Keys)
            {
                if (pattern != "*" && !GlobMatch(pattern, key))
                    continue;

                // No key locks held here, so no lazy deletion either.
                if (!_store.IsLive(key, out _))
                    continue;

                result.Add(RespValue.Bulk(key));
            }

            return ValueTask.FromResult(RespValue.Array(result));
        }

        private ValueTask<RespValue> FlushAll(IReadOnlyList<RespValue> args)
        {
            for (var i = 1; i < args.Count; i++)
            {
                var mode = GetString(args[i]).ToUpperInvariant();

                if (mode != "ASYNC" && mode != "SYNC")
                    return Error("ERR syntax error");
            }

            _store.Clear();
            return ValueTask.FromResult(RespValue.Ok);
        }

        private static ValueTask<RespValue> Time()
        {
            var now = DateTimeOffset.UtcNow;
            var seconds = now.ToUnixTimeSeconds();
            var micros = (now.UtcTicks % TimeSpan.TicksPerSecond) / 10;

            return ValueTask.FromResult(RespValue.Array(new[]
            {
                RespValue.Bulk(seconds.ToString(CultureInfo.InvariantCulture)),
                RespValue.Bulk(micros.ToString(CultureInfo.InvariantCulture)),
            }));
        }

        // ---- DUMP / RESTORE / MIGRATE ----

        private ValueTask<RespValue> Dump(IReadOnlyList<RespValue> args)
        {
            var key = GetString(args[1]);

            if (!_store.Exists(key))
                return ValueTask.FromResult(RespValue.Null());

            var entry = _store.Peek(key)!;

            return ValueTask.FromResult(
                RespValue.Bulk(DumpSerializer.Serialize(entry)));
        }

        // RESTORE key ttl payload [REPLACE] [ABSTTL] [IDLETIME s] [FREQ f]
        private ValueTask<RespValue> Restore(IReadOnlyList<RespValue> args)
        {
            var key = GetString(args[1]);

            if (!long.TryParse(GetString(args[2]), out var ttl) || ttl < 0)
                return Error("ERR Invalid TTL value, must be >= 0");

            var replace = false;
            var absolute = false;

            for (var i = 4; i < args.Count; i++)
            {
                var option = GetString(args[i]).ToUpperInvariant();

                switch (option)
                {
                    case "REPLACE":
                        replace = true;
                        break;

                    case "ABSTTL":
                        absolute = true;
                        break;

                    case "IDLETIME":
                    case "FREQ":
                        // Eviction hints; accepted and ignored.
                        i++;
                        break;

                    default:
                        return Error("ERR syntax error");
                }
            }

            if (!replace && _store.Exists(key))
                return Error("BUSYKEY Target key name already exists.");

            if (!DumpSerializer.TryDeserialize(
                    GetBytes(args[3]),
                    out var type,
                    out var value,
                    out var bytes))
            {
                return Error("ERR DUMP payload version or checksum are wrong");
            }

            long? expiresAt = ttl == 0
                ? null
                : absolute ? ttl : CacheStore.NowMs + ttl;

            if (expiresAt is { } ms && ms <= CacheStore.NowMs)
            {
                // Already expired: the net effect is just removing
                // whatever was there.
                if (replace)
                    _store.Remove(key);

                return ValueTask.FromResult(RespValue.Ok);
            }

            _store.Restore(key, type, value!, expiresAt, bytes);
            return ValueTask.FromResult(RespValue.Ok);
        }

        // MIGRATE host port key|"" destination-db timeout [COPY]
        //   [REPLACE] [AUTH password] [AUTH2 username password]
        //   [KEYS key [key ...]]
        //
        // Ships keys to another node with RESTORE-ASKING (accepted
        // there even while its slot is only IMPORTING) and, unless
        // COPY, deletes them here. Holds the keys' locks for the whole
        // exchange, so no write to them can land on the source after
        // it has been copied.
        private ValueTask<RespValue> Migrate(IReadOnlyList<RespValue> args)
        {
            var host = GetString(args[1]);

            if (!int.TryParse(GetString(args[2]), out var port) ||
                !int.TryParse(GetString(args[4]), out var db) ||
                !long.TryParse(GetString(args[5]), out var timeoutMs))
            {
                return Error("ERR value is not an integer or out of range");
            }

            if (db != 0)
                return Error("ERR Recache supports only database 0");

            var copy = false;
            var replace = false;
            string? authUser = null;
            string? authPassword = null;
            var keys = new List<string>();

            var singleKey = GetString(args[3]);

            if (singleKey.Length > 0)
                keys.Add(singleKey);

            for (var i = 6; i < args.Count; i++)
            {
                var option = GetString(args[i]).ToUpperInvariant();

                switch (option)
                {
                    case "COPY":
                        copy = true;
                        break;

                    case "REPLACE":
                        replace = true;
                        break;

                    case "AUTH" when i + 1 < args.Count:
                        authPassword = GetString(args[++i]);
                        break;

                    case "AUTH2" when i + 2 < args.Count:
                        authUser = GetString(args[++i]);
                        authPassword = GetString(args[++i]);
                        break;

                    case "KEYS":
                        if (singleKey.Length > 0)
                        {
                            return Error(
                                "ERR When using MIGRATE KEYS option, the key " +
                                "argument must be set to the empty string");
                        }

                        for (i++; i < args.Count; i++)
                            keys.Add(GetString(args[i]));

                        break;

                    default:
                        return Error("ERR syntax error");
                }
            }

            var present = new List<(string Key, byte[] Payload, long Ttl)>();
            var now = CacheStore.NowMs;

            foreach (var key in keys)
            {
                if (!_store.Exists(key))
                    continue;

                var entry = _store.Peek(key)!;
                var ttl = entry.ExpiresAtMs is { } expiresAt
                    ? Math.Max(1, expiresAt - now)
                    : 0;

                present.Add((key, DumpSerializer.Serialize(entry), ttl));
            }

            if (present.Count == 0)
                return ValueTask.FromResult(RespValue.Simple("NOKEY"));

            var timeout = TimeSpan.FromMilliseconds(timeoutMs <= 0 ? 1000 : timeoutMs);
            var restoreCommand = _cluster is not null ? "RESTORE-ASKING" : "RESTORE";

            try
            {
                using var client = new TcpClient { NoDelay = true };

                if (!client.ConnectAsync(host, port).Wait(timeout))
                    return Error($"IOERR error or timeout connecting to the client");

                var socket = client.Client;
                socket.ReceiveTimeout = (int)timeout.TotalMilliseconds;
                socket.SendTimeout = (int)timeout.TotalMilliseconds;

                using var stream = client.GetStream();

                var batch = new List<RespValue>();
                var expectedReplies = 0;

                if (authPassword is not null)
                {
                    batch.Add(authUser is null
                        ? RespValue.Command("AUTH", authPassword)
                        : RespValue.Command("AUTH", authUser, authPassword));

                    expectedReplies++;
                }

                foreach (var (key, payload, ttl) in present)
                {
                    var items = new List<RespValue>
                    {
                        RespValue.Bulk(restoreCommand),
                        RespValue.Bulk(key),
                        RespValue.Bulk(ttl.ToString(CultureInfo.InvariantCulture)),
                        RespValue.Bulk(payload),
                    };

                    if (replace)
                        items.Add(RespValue.Bulk("REPLACE"));

                    batch.Add(RespValue.Array(items));
                    expectedReplies++;
                }

                var writer = new RespWriter(stream);
                writer.WriteManyAsync(batch, default).AsTask().GetAwaiter().GetResult();

                var reader = new RespReader(stream, 64 * 1024 * 1024);
                string? firstError = null;
                var restored = new List<string>();

                for (var i = 0; i < expectedReplies; i++)
                {
                    var reply = reader.ReadAsync(default).AsTask().GetAwaiter().GetResult()
                        ?? throw new IOException("target closed the connection");

                    var keyIndex = authPassword is null ? i : i - 1;

                    if (reply.IsError)
                    {
                        firstError ??= reply.StringValue;
                    }
                    else if (keyIndex >= 0)
                    {
                        restored.Add(present[keyIndex].Key);
                    }
                }

                if (!copy)
                {
                    foreach (var key in restored)
                        _store.Remove(key);

                    // Replicas and the AOF see the keys leave, not a
                    // MIGRATE they would try to repeat.
                    if (restored.Count > 0)
                    {
                        var del = new RespValue[restored.Count + 1];
                        del[0] = DelName;

                        for (var i = 0; i < restored.Count; i++)
                            del[i + 1] = RespValue.Bulk(restored[i]);

                        ExecutionScope.Override(del);
                    }
                }

                if (firstError is not null)
                    return Error($"ERR Target instance replied with error: {firstError}");

                return ValueTask.FromResult(RespValue.Ok);
            }
            catch (Exception ex) when (ex is IOException or SocketException or AggregateException or RespProtocolException)
            {
                return Error($"IOERR error or timeout reading to target instance: {ex.Message}");
            }
        }
    }
}
