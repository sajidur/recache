using recache.Configuration;
using recache.Protocol;
using recache.Security;
using recache.Storage;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace recache.Commands
{
    public sealed class CommandDispatcher
    {
        private readonly ICacheStore _store;
        private readonly AuthenticationManager _auth;
        private readonly RecacheOptions _options;
        private readonly string _runId;
        private readonly DateTimeOffset _startedAt;

        public CommandDispatcher(
            ICacheStore store,
            AuthenticationManager auth,
            RecacheOptions options)
        {
            _store = store;
            _auth = auth;
            _options = options;
            _runId = Guid.NewGuid().ToString("N");
            _startedAt = DateTimeOffset.UtcNow;
        }

        public ValueTask<RespValue> ExecuteAsync(
            RespValue request,
            ClientSession session,
            CancellationToken cancellationToken)
        {
            if (request.Type != RespType.Array)
            {
                return ValueTask.FromResult(
                    RespValue.Error(
                        "ERR expected array"));
            }

            var args = request.Items!;

            if (args.Count == 0)
            {
                return ValueTask.FromResult(
                    RespValue.Error(
                        "ERR empty command"));
            }

            var command =
                GetString(args[0])
                    .ToUpperInvariant();

            if (command == "AUTH")
                return Auth(args, session);

            if (_auth.RequiresAuth && !session.IsAuthenticated)
            {
                return ValueTask.FromResult(
                    RespValue.Error(
                        "NOAUTH Authentication required."));
            }

            try
            {
                return command switch
                {
                    "PING" => Ping(args),

                    "GET" => Get(args),

                    "SET" => Set(args),

                    "SETEX" => SetEx(args, milliseconds: false),

                    "PSETEX" => SetEx(args, milliseconds: true),

                    "SETNX" => SetNx(args),

                    "STRLEN" => StrLen(args),

                    "GETRANGE" => GetRange(args),

                    "SETRANGE" => SetRange(args),

                    "APPEND" => Append(args),

                    "DEL" => Del(args),

                    "EXISTS" => Exists(args),

                    "INCR" => Increment(args, 1),

                    "DECR" => Increment(args, -1),

                    "INFO" => Info(args),

                    "CLIENT" => Client(args),

                    "CONFIG" => Config(args),

                    "COMMAND" => Command(args),

                    "SELECT" => Select(args),

                    "DBSIZE" => ValueTask.FromResult(
                        RespValue.Integer(_store.Count)),

                    "ECHO" => Echo(args),

                    "SCAN" => Scan(args),

                    "HSCAN" => HScan(args),

                    "SSCAN" => SScan(args),

                    "ZSCAN" => ZScan(args),

                    "TYPE" => Type(args),

                    "TTL" => Ttl(args),

                    "PTTL" => PTtl(args),

                    "EXPIRE" => Expire(args),

                    "PEXPIRE" => PExpire(args),

                    "PERSIST" => Persist(args),

                    "HSET" => HSet(args),

                    "HGET" => HGet(args),

                    "HGETALL" => HGetAll(args),

                    "HDEL" => HDel(args),

                    "HEXISTS" => HExists(args),

                    "HLEN" => HLen(args),

                    "HKEYS" => HKeys(args),

                    "HVALS" => HVals(args),

                    "HMGET" => HMGet(args),

                    "LPUSH" => Push(args, left: true),

                    "RPUSH" => Push(args, left: false),

                    "LRANGE" => LRange(args),

                    "LLEN" => LLen(args),

                    "LPOP" => Pop(args, left: true),

                    "RPOP" => Pop(args, left: false),

                    "LREM" => LRem(args),

                    "LSET" => LSet(args),

                    "LINDEX" => LIndex(args),

                    "SADD" => SAdd(args),

                    "SREM" => SRem(args),

                    "SMEMBERS" => SMembers(args),

                    "SISMEMBER" => SIsMember(args),

                    "SCARD" => SCard(args),

                    "ZADD" => ZAdd(args),

                    "ZREM" => ZRem(args),

                    "ZSCORE" => ZScore(args),

                    "ZRANGE" => ZRange(args),

                    "ZCARD" => ZCard(args),

                    "ZRANK" => ZRank(args),

                    "MODULE" => Module(args),

                    "JSON.SET" => JsonSet(args),

                    "JSON.GET" => JsonGet(args),

                    "JSON.DEL" or "JSON.FORGET" => JsonDel(args),

                    "JSON.TYPE" => JsonType(args),

                    "JSON.OBJKEYS" => JsonObjKeys(args),

                    "JSON.STRLEN" => JsonLength(
                        args,
                        "json.strlen",
                        (j, p) => (j.TryStrLen(p, out var l), l)),

                    "JSON.ARRLEN" => JsonLength(
                        args,
                        "json.arrlen",
                        (j, p) => (j.TryArrLen(p, out var l), l)),

                    "JSON.OBJLEN" => JsonLength(
                        args,
                        "json.objlen",
                        (j, p) => (j.TryObjLen(p, out var l), l)),

                    "JSON.ARRAPPEND" => JsonArrAppend(args),

                    "JSON.NUMINCRBY" => JsonNumIncrBy(args),

                    _ => Unknown(command)
                };
            }
            catch (WrongTypeException ex)
            {
                return Error(ex.Message);
            }
            catch (JsonPathException ex)
            {
                return Error($"ERR {ex.Message}");
            }
        }

        private ValueTask<RespValue> Auth(
            IReadOnlyList<RespValue> args,
            ClientSession session)
        {
            if (!_auth.RequiresAuth)
            {
                return Error(
                    "ERR Client sent AUTH, but no password is set. " +
                    "Did you mean AUTH <username> <password>?");
            }

            // Support both "AUTH <password>" and the ACL-style
            // "AUTH <username> <password>" (username is ignored —
            // there is only a single default user).
            string password;

            if (args.Count == 2)
            {
                password = GetString(args[1]);
            }
            else if (args.Count == 3)
            {
                password = GetString(args[2]);
            }
            else
            {
                return Error(
                    "ERR wrong number of arguments for 'auth' command");
            }

            if (!_auth.Validate(password))
            {
                return Error(
                    "WRONGPASS invalid username-password pair " +
                    "or user is disabled.");
            }

            session.IsAuthenticated = true;

            return ValueTask.FromResult(
                RespValue.Simple("OK"));
        }

        private static ValueTask<RespValue> Ping(
            IReadOnlyList<RespValue> args)
        {
            return ValueTask.FromResult(
                RespValue.Simple("PONG"));
        }

        private ValueTask<RespValue> Get(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 2)
                return Error(
                    "ERR wrong number of arguments for GET");

            var key = GetString(args[1]);

            if (!_store.TryGet(
                key,
                out var value))
            {
                return ValueTask.FromResult(
                    RespValue.Null());
            }

            return ValueTask.FromResult(
                RespValue.Bulk(value!));
        }

        private ValueTask<RespValue> Set(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'set' command");
            }

            var key = GetString(args[1]);
            var value = GetBytes(args[2]);

            TimeSpan? expiry = null;
            string? condition = null;
            var keepTtl = false;
            var getOld = false;

            for (var i = 3; i < args.Count; i++)
            {
                var option = GetString(args[i]).ToUpperInvariant();

                if (option == "EX" || option == "PX")
                {
                    if (expiry.HasValue || keepTtl)
                        return Error("ERR syntax error");

                    if (i + 1 >= args.Count ||
                        !long.TryParse(
                            GetString(args[++i]),
                            out var amount) ||
                        amount <= 0)
                    {
                        return Error(
                            "ERR value is not an integer " +
                            "or out of range");
                    }

                    expiry = option == "EX"
                        ? TimeSpan.FromSeconds(amount)
                        : TimeSpan.FromMilliseconds(amount);
                }
                else if (option == "NX" || option == "XX")
                {
                    if (condition != null)
                        return Error("ERR syntax error");

                    condition = option;
                }
                else if (option == "KEEPTTL")
                {
                    if (expiry.HasValue)
                        return Error("ERR syntax error");

                    keepTtl = true;
                }
                else if (option == "GET")
                {
                    getOld = true;
                }
                else
                {
                    return Error("ERR syntax error");
                }
            }

            // Fast, fully atomic path for the common case of a bare
            // conditional create — this is exactly SETNX.
            if (condition == "NX" && !getOld && !keepTtl)
            {
                var didSet = _store.SetIfAbsent(key, value, expiry);

                return ValueTask.FromResult(
                    didSet
                        ? RespValue.Simple("OK")
                        : RespValue.Null());
            }

            byte[]? oldValue = null;
            var hadOld = false;

            if (getOld)
            {
                // Matches real Redis: SET ... GET on a key holding
                // a non-string value is a WRONGTYPE error.
                hadOld = _store.TryGet(key, out oldValue);
            }

            var exists = getOld ? hadOld : _store.Exists(key);

            if (condition == "NX" && exists)
            {
                return ValueTask.FromResult(
                    getOld && hadOld
                        ? RespValue.Bulk(oldValue!)
                        : RespValue.Null());
            }

            if (condition == "XX" && !exists)
                return ValueTask.FromResult(RespValue.Null());

            TimeSpan? preservedTtl = null;

            if (keepTtl)
                _store.TryGetTtl(key, out preservedTtl);

            _store.Set(key, value, expiry ?? preservedTtl);

            return ValueTask.FromResult(
                getOld
                    ? (hadOld
                        ? RespValue.Bulk(oldValue!)
                        : RespValue.Null())
                    : RespValue.Simple("OK"));
        }

        private ValueTask<RespValue> SetEx(
            IReadOnlyList<RespValue> args,
            bool milliseconds)
        {
            var name = milliseconds ? "psetex" : "setex";

            if (args.Count != 4)
                return Error(
                    $"ERR wrong number of arguments for '{name}' command");

            if (!long.TryParse(GetString(args[2]), out var amount) ||
                amount <= 0)
            {
                return Error(
                    $"ERR invalid expire time in '{name}' command");
            }

            var key = GetString(args[1]);
            var value = GetBytes(args[3]);

            var expiry = milliseconds
                ? TimeSpan.FromMilliseconds(amount)
                : TimeSpan.FromSeconds(amount);

            _store.Set(key, value, expiry);

            return ValueTask.FromResult(RespValue.Simple("OK"));
        }

        private ValueTask<RespValue> SetNx(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'setnx' command");
            }

            var key = GetString(args[1]);
            var value = GetBytes(args[2]);

            return ValueTask.FromResult(
                RespValue.Integer(
                    _store.SetIfAbsent(key, value) ? 1 : 0));
        }

        private ValueTask<RespValue> StrLen(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'strlen' command");
            }

            var key = GetString(args[1]);

            return ValueTask.FromResult(
                RespValue.Integer(
                    _store.TryGet(key, out var value)
                        ? value!.Length
                        : 0));
        }

        private ValueTask<RespValue> GetRange(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 4)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'getrange' command");
            }

            if (!long.TryParse(GetString(args[2]), out var start) ||
                !long.TryParse(GetString(args[3]), out var end))
            {
                return Error(
                    "ERR value is not an integer or out of range");
            }

            var key = GetString(args[1]);

            if (!_store.TryGet(key, out var value) ||
                value!.Length == 0)
            {
                return ValueTask.FromResult(
                    RespValue.Bulk(Array.Empty<byte>()));
            }

            var len = value.Length;

            var s = start < 0 ? Math.Max(len + start, 0) : start;
            var e = end < 0 ? len + end : end;

            if (e >= len)
                e = len - 1;

            if (s > e || s >= len || e < 0)
                return ValueTask.FromResult(RespValue.Bulk(Array.Empty<byte>()));

            var sliceLen = (int)(e - s + 1);
            var result = new byte[sliceLen];

            Array.Copy(value, (int)s, result, 0, sliceLen);

            return ValueTask.FromResult(RespValue.Bulk(result));
        }

        private ValueTask<RespValue> SetRange(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 4)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'setrange' command");
            }

            if (!long.TryParse(GetString(args[2]), out var offset) ||
                offset < 0)
            {
                return Error("ERR offset is out of range");
            }

            var key = GetString(args[1]);
            var patch = GetBytes(args[3]);

            _store.TryGet(key, out var existing);

            var existingLen = existing?.Length ?? 0;

            if (patch.Length == 0)
                return ValueTask.FromResult(RespValue.Integer(existingLen));

            var newLen = (int)Math.Max(existingLen, offset + patch.Length);
            var result = new byte[newLen];

            if (existing != null)
                Array.Copy(existing, result, existing.Length);

            Array.Copy(patch, 0, result, (int)offset, patch.Length);

            // SETRANGE modifies in place — unlike a plain SET, it
            // must not clear an existing TTL.
            _store.TryGetTtl(key, out var ttl);
            _store.Set(key, result, ttl);

            return ValueTask.FromResult(RespValue.Integer(newLen));
        }

        private ValueTask<RespValue> Append(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'append' command");
            }

            var key = GetString(args[1]);
            var addition = GetBytes(args[2]);

            _store.TryGet(key, out var existing);

            byte[] result;

            if (existing == null)
            {
                result = addition;
            }
            else
            {
                result = new byte[existing.Length + addition.Length];
                Array.Copy(existing, result, existing.Length);
                Array.Copy(
                    addition,
                    0,
                    result,
                    existing.Length,
                    addition.Length);
            }

            // Same as SETRANGE: an in-place modification, so any
            // existing TTL must be preserved.
            _store.TryGetTtl(key, out var ttl);
            _store.Set(key, result, ttl);

            return ValueTask.FromResult(RespValue.Integer(result.Length));
        }

        private ValueTask<RespValue> Del(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 2)
                return Error(
                    "ERR wrong number of arguments for DEL");

            var removed = 0;

            for (var i = 1; i < args.Count; i++)
            {
                if (_store.Remove(
                    GetString(args[i])))
                {
                    removed++;
                }
            }

            return ValueTask.FromResult(
                RespValue.Integer(removed));
        }

        private ValueTask<RespValue> Exists(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 2)
                return Error(
                    "ERR wrong number of arguments for EXISTS");

            var count = 0;

            for (var i = 1; i < args.Count; i++)
            {
                if (_store.Exists(
                    GetString(args[i])))
                {
                    count++;
                }
            }

            return ValueTask.FromResult(
                RespValue.Integer(count));
        }

        private ValueTask<RespValue> Increment(
            IReadOnlyList<RespValue> args,
            long amount)
        {
            if (args.Count != 2)
                return Error(
                    "ERR wrong number of arguments");

            var key = GetString(args[1]);

            var result = _store.Increment(key, amount);

            return ValueTask.FromResult(
                RespValue.Integer(result));
        }

        private ValueTask<RespValue> Scan(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'scan' command");
            }

            if (!long.TryParse(
                    GetString(args[1]),
                    out var cursor) ||
                cursor < 0)
            {
                return Error(
                    "ERR invalid cursor");
            }

            string? matchPattern = null;
            string? typeFilter = null;
            var count = 10;

            for (var i = 2; i < args.Count; i++)
            {
                var option = GetString(args[i]).ToUpperInvariant();

                if (option == "MATCH" && i + 1 < args.Count)
                {
                    matchPattern = GetString(args[++i]);
                }
                else if (option == "COUNT" && i + 1 < args.Count)
                {
                    if (!int.TryParse(
                            GetString(args[++i]),
                            out count) ||
                        count <= 0)
                    {
                        return Error(
                            "ERR value is not an integer " +
                            "or out of range");
                    }
                }
                else if (option == "TYPE" && i + 1 < args.Count)
                {
                    typeFilter = GetString(args[++i]);
                }
                else
                {
                    return Error("ERR syntax error");
                }
            }

            // Simplified cursor: an index into a stable, sorted
            // snapshot of the current key set. This does not give
            // Redis's exact "every key present for the whole scan
            // is returned at least once under concurrent mutation"
            // guarantee, but is correct and stable for the common
            // case of browsing a mostly-static keyspace.
            var snapshot = new List<string>(_store.Keys);
            snapshot.Sort(StringComparer.Ordinal);

            var start = (int)Math.Min(cursor, snapshot.Count);
            var end = (int)Math.Min(start + count, snapshot.Count);

            var page = new List<RespValue>();

            for (var i = start; i < end; i++)
            {
                var key = snapshot[i];

                if (matchPattern != null &&
                    !GlobMatch(matchPattern, key))
                {
                    continue;
                }

                // Skip keys that expired since the snapshot
                // was taken, without materializing their value.
                if (!_store.TryGetType(key, out var keyType))
                    continue;

                if (typeFilter != null &&
                    !string.Equals(
                        TypeName(keyType),
                        typeFilter,
                        StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                page.Add(
                    RespValue.Bulk(
                        Encoding.UTF8.GetBytes(key)));
            }

            var nextCursor =
                end >= snapshot.Count ? 0 : end;

            return ValueTask.FromResult(
                RespValue.Array(new RespValue[]
                {
                    RespValue.Bulk(
                        Encoding.UTF8.GetBytes(
                            nextCursor.ToString())),

                    RespValue.Array(page)
                }));
        }

        private static bool GlobMatch(
            string pattern,
            string value)
        {
            return GlobMatch(
                pattern.AsSpan(),
                value.AsSpan());
        }

        private static bool GlobMatch(
            ReadOnlySpan<char> pattern,
            ReadOnlySpan<char> value)
        {
            while (pattern.Length > 0)
            {
                if (pattern[0] == '*')
                {
                    while (pattern.Length > 0 &&
                           pattern[0] == '*')
                    {
                        pattern = pattern[1..];
                    }

                    if (pattern.Length == 0)
                        return true;

                    for (var i = 0; i <= value.Length; i++)
                    {
                        if (GlobMatch(pattern, value[i..]))
                            return true;
                    }

                    return false;
                }

                if (pattern[0] == '?')
                {
                    if (value.Length == 0)
                        return false;

                    pattern = pattern[1..];
                    value = value[1..];
                    continue;
                }

                if (value.Length == 0 ||
                    value[0] != pattern[0])
                {
                    return false;
                }

                pattern = pattern[1..];
                value = value[1..];
            }

            return value.Length == 0;
        }

        private ValueTask<RespValue> HScan(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'hscan' command");
            }

            if (!long.TryParse(
                    GetString(args[2]),
                    out var cursor) ||
                cursor < 0)
            {
                return Error("ERR invalid cursor");
            }

            string? matchPattern = null;
            var count = 10;
            var noValues = false;

            for (var i = 3; i < args.Count; i++)
            {
                var option = GetString(args[i]).ToUpperInvariant();

                if (option == "MATCH" && i + 1 < args.Count)
                {
                    matchPattern = GetString(args[++i]);
                }
                else if (option == "COUNT" && i + 1 < args.Count)
                {
                    if (!int.TryParse(
                            GetString(args[++i]),
                            out count) ||
                        count <= 0)
                    {
                        return Error(
                            "ERR value is not an integer " +
                            "or out of range");
                    }
                }
                else if (option == "NOVALUES")
                {
                    noValues = true;
                }
                else
                {
                    return Error("ERR syntax error");
                }
            }

            var hash = _store.TryGetHash(GetString(args[1]));

            var snapshot =
                hash?.Snapshot() ??
                Array.Empty<KeyValuePair<string, byte[]>>();

            Array.Sort(
                snapshot,
                (a, b) => string.CompareOrdinal(a.Key, b.Key));

            var start = (int)Math.Min(cursor, snapshot.Length);
            var end = (int)Math.Min(start + count, snapshot.Length);

            var page = new List<RespValue>();

            for (var i = start; i < end; i++)
            {
                var field = snapshot[i].Key;

                if (matchPattern != null &&
                    !GlobMatch(matchPattern, field))
                {
                    continue;
                }

                page.Add(
                    RespValue.Bulk(
                        Encoding.UTF8.GetBytes(field)));

                if (!noValues)
                    page.Add(RespValue.Bulk(snapshot[i].Value));
            }

            var nextCursor =
                end >= snapshot.Length ? 0 : end;

            return ValueTask.FromResult(
                RespValue.Array(new RespValue[]
                {
                    RespValue.Bulk(
                        Encoding.UTF8.GetBytes(
                            nextCursor.ToString())),

                    RespValue.Array(page)
                }));
        }

        private ValueTask<RespValue> SScan(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'sscan' command");
            }

            if (!long.TryParse(
                    GetString(args[2]),
                    out var cursor) ||
                cursor < 0)
            {
                return Error("ERR invalid cursor");
            }

            string? matchPattern = null;
            var count = 10;

            for (var i = 3; i < args.Count; i++)
            {
                var option = GetString(args[i]).ToUpperInvariant();

                if (option == "MATCH" && i + 1 < args.Count)
                {
                    matchPattern = GetString(args[++i]);
                }
                else if (option == "COUNT" && i + 1 < args.Count)
                {
                    if (!int.TryParse(
                            GetString(args[++i]),
                            out count) ||
                        count <= 0)
                    {
                        return Error(
                            "ERR value is not an integer " +
                            "or out of range");
                    }
                }
                else
                {
                    return Error("ERR syntax error");
                }
            }

            var set = _store.TryGetSet(GetString(args[1]));
            var snapshot = set?.Snapshot() ?? Array.Empty<byte[]>();

            Array.Sort(
                snapshot,
                (a, b) => a.AsSpan().SequenceCompareTo(b));

            var start = (int)Math.Min(cursor, snapshot.Length);
            var end = (int)Math.Min(start + count, snapshot.Length);

            var page = new List<RespValue>();

            for (var i = start; i < end; i++)
            {
                var member = snapshot[i];

                if (matchPattern != null &&
                    !GlobMatch(
                        matchPattern,
                        Encoding.UTF8.GetString(member)))
                {
                    continue;
                }

                page.Add(RespValue.Bulk(member));
            }

            var nextCursor =
                end >= snapshot.Length ? 0 : end;

            return ValueTask.FromResult(
                RespValue.Array(new RespValue[]
                {
                    RespValue.Bulk(
                        Encoding.UTF8.GetBytes(
                            nextCursor.ToString())),

                    RespValue.Array(page)
                }));
        }

        private ValueTask<RespValue> ZScan(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'zscan' command");
            }

            if (!long.TryParse(
                    GetString(args[2]),
                    out var cursor) ||
                cursor < 0)
            {
                return Error("ERR invalid cursor");
            }

            string? matchPattern = null;
            var count = 10;

            for (var i = 3; i < args.Count; i++)
            {
                var option = GetString(args[i]).ToUpperInvariant();

                if (option == "MATCH" && i + 1 < args.Count)
                {
                    matchPattern = GetString(args[++i]);
                }
                else if (option == "COUNT" && i + 1 < args.Count)
                {
                    if (!int.TryParse(
                            GetString(args[++i]),
                            out count) ||
                        count <= 0)
                    {
                        return Error(
                            "ERR value is not an integer " +
                            "or out of range");
                    }
                }
                else
                {
                    return Error("ERR syntax error");
                }
            }

            var zset = _store.TryGetSortedSet(GetString(args[1]));

            // Already ordered by score (then member) — a stable,
            // if not lexicographic, snapshot order.
            var snapshot =
                zset?.Range(0, -1) ??
                Array.Empty<(string Member, double Score)>();

            var start = (int)Math.Min(cursor, snapshot.Length);
            var end = (int)Math.Min(start + count, snapshot.Length);

            var page = new List<RespValue>();

            for (var i = start; i < end; i++)
            {
                var (member, score) = snapshot[i];

                if (matchPattern != null &&
                    !GlobMatch(matchPattern, member))
                {
                    continue;
                }

                page.Add(
                    RespValue.Bulk(
                        Encoding.UTF8.GetBytes(member)));

                page.Add(
                    RespValue.Bulk(
                        Encoding.UTF8.GetBytes(
                            FormatScore(score))));
            }

            var nextCursor =
                end >= snapshot.Length ? 0 : end;

            return ValueTask.FromResult(
                RespValue.Array(new RespValue[]
                {
                    RespValue.Bulk(
                        Encoding.UTF8.GetBytes(
                            nextCursor.ToString())),

                    RespValue.Array(page)
                }));
        }

        private ValueTask<RespValue> Type(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'type' command");
            }

            var key = GetString(args[1]);

            return ValueTask.FromResult(
                RespValue.Simple(
                    _store.TryGetType(key, out var type)
                        ? TypeName(type)
                        : "none"));
        }

        private static string TypeName(CacheValueType type) =>
            type switch
            {
                CacheValueType.String => "string",
                CacheValueType.Hash => "hash",
                CacheValueType.List => "list",
                CacheValueType.Set => "set",
                CacheValueType.SortedSet => "zset",
                CacheValueType.Json => "ReJSON-RL",
                _ => "none"
            };

        // ---- Module introspection ----

        private static ValueTask<RespValue> Module(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count >= 2 &&
                string.Equals(
                    GetString(args[1]),
                    "LIST",
                    StringComparison.OrdinalIgnoreCase))
            {
                var moduleInfo = RespValue.Array(new RespValue[]
                {
                    RespValue.Bulk(Encoding.UTF8.GetBytes("name")),
                    RespValue.Bulk(Encoding.UTF8.GetBytes("ReJSON")),
                    RespValue.Bulk(Encoding.UTF8.GetBytes("ver")),
                    RespValue.Integer(20000)
                });

                return ValueTask.FromResult(
                    RespValue.Array(new[] { moduleInfo }));
            }

            return ValueTask.FromResult(
                RespValue.Array(Array.Empty<RespValue>()));
        }

        // ---- JSON ----

        private ValueTask<RespValue> JsonSet(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 4 || args.Count > 5)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'json.set' command");
            }

            var key = GetString(args[1]);
            var path = GetString(args[2]);
            var valueText = GetString(args[3]);

            string? condition = null;

            if (args.Count == 5)
            {
                condition = GetString(args[4]).ToUpperInvariant();

                if (condition != "NX" && condition != "XX")
                    return Error("ERR syntax error");
            }

            JsonNode? valueNode;

            try
            {
                valueNode = JsonNode.Parse(valueText);
            }
            catch (JsonException)
            {
                return Error("ERR invalid JSON value");
            }

            var existingJson = _store.TryGetJson(key);
            var pathExists = existingJson?.PathExists(path) ?? false;

            if (condition == "NX" && pathExists)
                return ValueTask.FromResult(RespValue.Null());

            if (condition == "XX" && !pathExists)
                return ValueTask.FromResult(RespValue.Null());

            var isRootPath = JsonPath.Parse(path).Count == 0;

            if (existingJson == null && !isRootPath)
            {
                return Error(
                    "ERR new objects must be created with a root path");
            }

            var json = _store.GetOrCreateJson(key);

            if (!json.TrySet(path, valueNode, out var byteDelta))
                return Error("ERR path does not exist");

            _store.AdjustMemory(byteDelta);

            return ValueTask.FromResult(RespValue.Simple("OK"));
        }

        private ValueTask<RespValue> JsonGet(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'json.get' command");
            }

            var key = GetString(args[1]);
            var paths = new List<string>();

            for (var i = 2; i < args.Count; i++)
                paths.Add(GetString(args[i]));

            var json = _store.TryGetJson(key);

            if (json == null || !json.TryGet(paths, out var text))
                return ValueTask.FromResult(RespValue.Null());

            return ValueTask.FromResult(
                RespValue.Bulk(Encoding.UTF8.GetBytes(text!)));
        }

        private ValueTask<RespValue> JsonDel(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 2 || args.Count > 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'json.del' command");
            }

            var key = GetString(args[1]);
            var path = args.Count == 3 ? GetString(args[2]) : "$";

            var json = _store.TryGetJson(key);

            if (json == null)
                return ValueTask.FromResult(RespValue.Integer(0));

            var removed = json.Delete(path);

            if (path == "$" && removed > 0)
                _store.Remove(key);

            return ValueTask.FromResult(RespValue.Integer(removed));
        }

        private ValueTask<RespValue> JsonType(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 2 || args.Count > 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'json.type' command");
            }

            var key = GetString(args[1]);
            var path = args.Count == 3 ? GetString(args[2]) : "$";

            var json = _store.TryGetJson(key);

            if (json == null || !json.TryGetType(path, out var type))
                return ValueTask.FromResult(RespValue.Null());

            return ValueTask.FromResult(
                RespValue.Bulk(Encoding.UTF8.GetBytes(type!)));
        }

        private ValueTask<RespValue> JsonObjKeys(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 2 || args.Count > 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'json.objkeys' command");
            }

            var key = GetString(args[1]);
            var path = args.Count == 3 ? GetString(args[2]) : "$";

            var json = _store.TryGetJson(key);

            if (json == null || !json.TryObjKeys(path, out var keys))
                return ValueTask.FromResult(RespValue.Null());

            var result = new RespValue[keys!.Length];

            for (var i = 0; i < keys.Length; i++)
                result[i] = RespValue.Bulk(Encoding.UTF8.GetBytes(keys[i]));

            return ValueTask.FromResult(RespValue.Array(result));
        }

        private ValueTask<RespValue> JsonLength(
            IReadOnlyList<RespValue> args,
            string commandName,
            Func<RedisJson, string, (bool Ok, int Length)> resolver)
        {
            if (args.Count < 2 || args.Count > 3)
                return Error($"ERR wrong number of arguments for '{commandName}' command");

            var key = GetString(args[1]);
            var path = args.Count == 3 ? GetString(args[2]) : "$";

            var json = _store.TryGetJson(key);

            if (json == null)
                return ValueTask.FromResult(RespValue.Null());

            var (ok, length) = resolver(json, path);

            return ValueTask.FromResult(
                ok ? RespValue.Integer(length) : RespValue.Null());
        }

        private ValueTask<RespValue> JsonArrAppend(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 4)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'json.arrappend' command");
            }

            var key = GetString(args[1]);
            var path = GetString(args[2]);

            var values = new List<JsonNode?>();

            for (var i = 3; i < args.Count; i++)
            {
                try
                {
                    values.Add(JsonNode.Parse(GetString(args[i])));
                }
                catch (JsonException)
                {
                    return Error("ERR invalid JSON value");
                }
            }

            var json = _store.TryGetJson(key);

            if (json == null)
            {
                return Error(
                    "ERR could not perform this operation on a " +
                    "key that doesn't exist");
            }

            if (!json.TryArrAppend(
                    path,
                    values,
                    out var newLength,
                    out var byteDelta))
            {
                return Error(
                    "ERR path does not exist or does not " +
                    "point to an array");
            }

            _store.AdjustMemory(byteDelta);

            return ValueTask.FromResult(RespValue.Integer(newLength));
        }

        private ValueTask<RespValue> JsonNumIncrBy(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 4)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'json.numincrby' command");
            }

            var key = GetString(args[1]);
            var path = GetString(args[2]);

            if (!double.TryParse(
                    GetString(args[3]),
                    NumberStyles.Float,
                    CultureInfo.InvariantCulture,
                    out var amount))
            {
                return Error("ERR value is not a valid number");
            }

            var json = _store.TryGetJson(key);

            if (json == null)
            {
                return Error(
                    "ERR could not perform this operation on a " +
                    "key that doesn't exist");
            }

            if (!json.TryNumIncrBy(
                    path,
                    amount,
                    out var result,
                    out var byteDelta))
            {
                return Error(
                    "ERR path does not exist or does not " +
                    "point to a number");
            }

            _store.AdjustMemory(byteDelta);

            return ValueTask.FromResult(
                RespValue.Bulk(
                    Encoding.UTF8.GetBytes(FormatScore(result))));
        }

        private ValueTask<RespValue> Ttl(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'ttl' command");
            }

            var key = GetString(args[1]);

            if (!_store.TryGetTtl(key, out var ttl))
                return ValueTask.FromResult(RespValue.Integer(-2));

            if (ttl == null)
                return ValueTask.FromResult(RespValue.Integer(-1));

            return ValueTask.FromResult(
                RespValue.Integer(
                    (long)Math.Ceiling(ttl.Value.TotalSeconds)));
        }

        private ValueTask<RespValue> PTtl(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'pttl' command");
            }

            var key = GetString(args[1]);

            if (!_store.TryGetTtl(key, out var ttl))
                return ValueTask.FromResult(RespValue.Integer(-2));

            if (ttl == null)
                return ValueTask.FromResult(RespValue.Integer(-1));

            return ValueTask.FromResult(
                RespValue.Integer(
                    (long)Math.Ceiling(ttl.Value.TotalMilliseconds)));
        }

        private ValueTask<RespValue> Expire(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'expire' command");
            }

            if (!long.TryParse(GetString(args[2]), out var seconds))
                return Error("ERR value is not an integer or out of range");

            var key = GetString(args[1]);

            // A non-positive TTL means "expire immediately",
            // matching real Redis semantics.
            if (seconds <= 0)
            {
                return ValueTask.FromResult(
                    RespValue.Integer(_store.Remove(key) ? 1 : 0));
            }

            return ValueTask.FromResult(
                RespValue.Integer(
                    _store.TryExpire(
                        key,
                        TimeSpan.FromSeconds(seconds))
                        ? 1
                        : 0));
        }

        private ValueTask<RespValue> PExpire(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'pexpire' command");
            }

            if (!long.TryParse(
                    GetString(args[2]),
                    out var milliseconds))
            {
                return Error(
                    "ERR value is not an integer or out of range");
            }

            var key = GetString(args[1]);

            if (milliseconds <= 0)
            {
                return ValueTask.FromResult(
                    RespValue.Integer(_store.Remove(key) ? 1 : 0));
            }

            return ValueTask.FromResult(
                RespValue.Integer(
                    _store.TryExpire(
                        key,
                        TimeSpan.FromMilliseconds(milliseconds))
                        ? 1
                        : 0));
        }

        private ValueTask<RespValue> Persist(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'persist' command");
            }

            var key = GetString(args[1]);

            return ValueTask.FromResult(
                RespValue.Integer(
                    _store.TryPersist(key) ? 1 : 0));
        }

        // ---- Hash ----

        private ValueTask<RespValue> HSet(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 4 || args.Count % 2 != 0)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'hset' command");
            }

            var key = GetString(args[1]);
            var hash = _store.GetOrCreateHash(key);

            var added = 0;

            for (var i = 2; i < args.Count; i += 2)
            {
                var field = GetString(args[i]);
                var value = GetBytes(args[i + 1]);

                if (hash.Set(field, value, out var delta))
                    added++;

                _store.AdjustMemory(delta);
            }

            return ValueTask.FromResult(RespValue.Integer(added));
        }

        private ValueTask<RespValue> HGet(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'hget' command");
            }

            var hash = _store.TryGetHash(GetString(args[1]));

            if (hash == null ||
                !hash.TryGet(GetString(args[2]), out var value))
            {
                return ValueTask.FromResult(RespValue.Null());
            }

            return ValueTask.FromResult(RespValue.Bulk(value!));
        }

        private ValueTask<RespValue> HGetAll(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'hgetall' command");
            }

            var hash = _store.TryGetHash(GetString(args[1]));

            if (hash == null)
            {
                return ValueTask.FromResult(
                    RespValue.Array(Array.Empty<RespValue>()));
            }

            var snapshot = hash.Snapshot();
            var result = new RespValue[snapshot.Length * 2];

            for (var i = 0; i < snapshot.Length; i++)
            {
                result[i * 2] = RespValue.Bulk(
                    Encoding.UTF8.GetBytes(snapshot[i].Key));

                result[i * 2 + 1] = RespValue.Bulk(snapshot[i].Value);
            }

            return ValueTask.FromResult(RespValue.Array(result));
        }

        private ValueTask<RespValue> HDel(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'hdel' command");
            }

            var key = GetString(args[1]);
            var hash = _store.TryGetHash(key);

            if (hash == null)
                return ValueTask.FromResult(RespValue.Integer(0));

            var removed = 0;

            for (var i = 2; i < args.Count; i++)
            {
                if (hash.Remove(GetString(args[i]), out var delta))
                {
                    removed++;
                    _store.AdjustMemory(delta);
                }
            }

            if (hash.Count == 0)
                _store.Remove(key);

            return ValueTask.FromResult(RespValue.Integer(removed));
        }

        private ValueTask<RespValue> HExists(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'hexists' command");
            }

            var hash = _store.TryGetHash(GetString(args[1]));

            var exists =
                hash != null &&
                hash.ContainsField(GetString(args[2]));

            return ValueTask.FromResult(
                RespValue.Integer(exists ? 1 : 0));
        }

        private ValueTask<RespValue> HLen(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'hlen' command");
            }

            var hash = _store.TryGetHash(GetString(args[1]));

            return ValueTask.FromResult(
                RespValue.Integer(hash?.Count ?? 0));
        }

        private ValueTask<RespValue> HKeys(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'hkeys' command");
            }

            var hash = _store.TryGetHash(GetString(args[1]));

            if (hash == null)
            {
                return ValueTask.FromResult(
                    RespValue.Array(Array.Empty<RespValue>()));
            }

            var snapshot = hash.Snapshot();
            var result = new RespValue[snapshot.Length];

            for (var i = 0; i < snapshot.Length; i++)
            {
                result[i] = RespValue.Bulk(
                    Encoding.UTF8.GetBytes(snapshot[i].Key));
            }

            return ValueTask.FromResult(RespValue.Array(result));
        }

        private ValueTask<RespValue> HVals(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'hvals' command");
            }

            var hash = _store.TryGetHash(GetString(args[1]));

            if (hash == null)
            {
                return ValueTask.FromResult(
                    RespValue.Array(Array.Empty<RespValue>()));
            }

            var snapshot = hash.Snapshot();
            var result = new RespValue[snapshot.Length];

            for (var i = 0; i < snapshot.Length; i++)
                result[i] = RespValue.Bulk(snapshot[i].Value);

            return ValueTask.FromResult(RespValue.Array(result));
        }

        private ValueTask<RespValue> HMGet(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'hmget' command");
            }

            var hash = _store.TryGetHash(GetString(args[1]));
            var result = new RespValue[args.Count - 2];

            for (var i = 2; i < args.Count; i++)
            {
                result[i - 2] =
                    hash != null &&
                    hash.TryGet(GetString(args[i]), out var value)
                        ? RespValue.Bulk(value!)
                        : RespValue.Null();
            }

            return ValueTask.FromResult(RespValue.Array(result));
        }

        // ---- List ----

        private ValueTask<RespValue> Push(
            IReadOnlyList<RespValue> args,
            bool left)
        {
            if (args.Count < 3)
            {
                return Error(
                    $"ERR wrong number of arguments for " +
                    $"'{(left ? "lpush" : "rpush")}' command");
            }

            var key = GetString(args[1]);
            var list = _store.GetOrCreateList(key);

            for (var i = 2; i < args.Count; i++)
            {
                var value = GetBytes(args[i]);

                var delta = left
                    ? list.PushLeft(value)
                    : list.PushRight(value);

                _store.AdjustMemory(delta);
            }

            return ValueTask.FromResult(
                RespValue.Integer(list.Count));
        }

        private ValueTask<RespValue> LRange(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 4)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'lrange' command");
            }

            if (!long.TryParse(GetString(args[2]), out var start) ||
                !long.TryParse(GetString(args[3]), out var stop))
            {
                return Error(
                    "ERR value is not an integer or out of range");
            }

            var list = _store.TryGetList(GetString(args[1]));

            if (list == null)
            {
                return ValueTask.FromResult(
                    RespValue.Array(Array.Empty<RespValue>()));
            }

            var items = list.Range(start, stop);
            var result = new RespValue[items.Length];

            for (var i = 0; i < items.Length; i++)
                result[i] = RespValue.Bulk(items[i]);

            return ValueTask.FromResult(RespValue.Array(result));
        }

        private ValueTask<RespValue> LLen(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'llen' command");
            }

            var list = _store.TryGetList(GetString(args[1]));

            return ValueTask.FromResult(
                RespValue.Integer(list?.Count ?? 0));
        }

        private ValueTask<RespValue> Pop(
            IReadOnlyList<RespValue> args,
            bool left)
        {
            if (args.Count != 2)
            {
                return Error(
                    $"ERR wrong number of arguments for " +
                    $"'{(left ? "lpop" : "rpop")}' command");
            }

            var key = GetString(args[1]);
            var list = _store.TryGetList(key);

            if (list == null)
                return ValueTask.FromResult(RespValue.Null());

            byte[]? value;
            long delta;

            if (left)
                value = list.PopLeft(out delta);
            else
                value = list.PopRight(out delta);

            if (value == null)
                return ValueTask.FromResult(RespValue.Null());

            _store.AdjustMemory(delta);

            if (list.Count == 0)
                _store.Remove(key);

            return ValueTask.FromResult(RespValue.Bulk(value));
        }

        private ValueTask<RespValue> LRem(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 4)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'lrem' command");
            }

            if (!long.TryParse(GetString(args[2]), out var count))
                return Error("ERR value is not an integer or out of range");

            var key = GetString(args[1]);
            var list = _store.TryGetList(key);

            if (list == null)
                return ValueTask.FromResult(RespValue.Integer(0));

            var value = GetBytes(args[3]);
            var removed = list.Remove(value, count, out var delta);

            _store.AdjustMemory(delta);

            if (list.Count == 0)
                _store.Remove(key);

            return ValueTask.FromResult(RespValue.Integer(removed));
        }

        private ValueTask<RespValue> LSet(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 4)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'lset' command");
            }

            if (!long.TryParse(GetString(args[2]), out var index))
                return Error("ERR value is not an integer or out of range");

            var list = _store.TryGetList(GetString(args[1]));

            if (list == null)
                return Error("ERR no such key");

            var value = GetBytes(args[3]);

            if (!list.SetAt(index, value, out var delta))
                return Error("ERR index out of range");

            _store.AdjustMemory(delta);

            return ValueTask.FromResult(RespValue.Simple("OK"));
        }

        private ValueTask<RespValue> LIndex(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'lindex' command");
            }

            if (!long.TryParse(GetString(args[2]), out var index))
                return Error("ERR value is not an integer or out of range");

            var list = _store.TryGetList(GetString(args[1]));
            var value = list?.Index(index);

            return ValueTask.FromResult(
                value == null
                    ? RespValue.Null()
                    : RespValue.Bulk(value));
        }

        // ---- Set ----

        private ValueTask<RespValue> SAdd(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'sadd' command");
            }

            var key = GetString(args[1]);
            var set = _store.GetOrCreateSet(key);

            var added = 0;

            for (var i = 2; i < args.Count; i++)
            {
                if (set.Add(GetBytes(args[i]), out var delta))
                {
                    added++;
                    _store.AdjustMemory(delta);
                }
            }

            return ValueTask.FromResult(RespValue.Integer(added));
        }

        private ValueTask<RespValue> SRem(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'srem' command");
            }

            var key = GetString(args[1]);
            var set = _store.TryGetSet(key);

            if (set == null)
                return ValueTask.FromResult(RespValue.Integer(0));

            var removed = 0;

            for (var i = 2; i < args.Count; i++)
            {
                if (set.Remove(GetBytes(args[i]), out var delta))
                {
                    removed++;
                    _store.AdjustMemory(delta);
                }
            }

            if (set.Count == 0)
                _store.Remove(key);

            return ValueTask.FromResult(RespValue.Integer(removed));
        }

        private ValueTask<RespValue> SMembers(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'smembers' command");
            }

            var set = _store.TryGetSet(GetString(args[1]));

            if (set == null)
            {
                return ValueTask.FromResult(
                    RespValue.Array(Array.Empty<RespValue>()));
            }

            var snapshot = set.Snapshot();
            var result = new RespValue[snapshot.Length];

            for (var i = 0; i < snapshot.Length; i++)
                result[i] = RespValue.Bulk(snapshot[i]);

            return ValueTask.FromResult(RespValue.Array(result));
        }

        private ValueTask<RespValue> SIsMember(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'sismember' command");
            }

            var set = _store.TryGetSet(GetString(args[1]));

            var isMember =
                set != null && set.Contains(GetBytes(args[2]));

            return ValueTask.FromResult(
                RespValue.Integer(isMember ? 1 : 0));
        }

        private ValueTask<RespValue> SCard(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'scard' command");
            }

            var set = _store.TryGetSet(GetString(args[1]));

            return ValueTask.FromResult(
                RespValue.Integer(set?.Count ?? 0));
        }

        // ---- Sorted set ----

        private ValueTask<RespValue> ZAdd(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 4 || args.Count % 2 != 0)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'zadd' command");
            }

            var key = GetString(args[1]);
            var zset = _store.GetOrCreateSortedSet(key);

            var added = 0;

            for (var i = 2; i < args.Count; i += 2)
            {
                if (!double.TryParse(
                        GetString(args[i]),
                        NumberStyles.Float,
                        CultureInfo.InvariantCulture,
                        out var score))
                {
                    return Error("ERR value is not a valid float");
                }

                var member = GetString(args[i + 1]);

                if (zset.Add(member, score, out var delta))
                    added++;

                _store.AdjustMemory(delta);
            }

            return ValueTask.FromResult(RespValue.Integer(added));
        }

        private ValueTask<RespValue> ZRem(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'zrem' command");
            }

            var key = GetString(args[1]);
            var zset = _store.TryGetSortedSet(key);

            if (zset == null)
                return ValueTask.FromResult(RespValue.Integer(0));

            var removed = 0;

            for (var i = 2; i < args.Count; i++)
            {
                if (zset.Remove(GetString(args[i]), out var delta))
                {
                    removed++;
                    _store.AdjustMemory(delta);
                }
            }

            if (zset.Count == 0)
                _store.Remove(key);

            return ValueTask.FromResult(RespValue.Integer(removed));
        }

        private ValueTask<RespValue> ZScore(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'zscore' command");
            }

            var zset = _store.TryGetSortedSet(GetString(args[1]));

            if (zset == null ||
                !zset.TryGetScore(GetString(args[2]), out var score))
            {
                return ValueTask.FromResult(RespValue.Null());
            }

            return ValueTask.FromResult(
                RespValue.Bulk(
                    Encoding.UTF8.GetBytes(FormatScore(score))));
        }

        private ValueTask<RespValue> ZCard(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'zcard' command");
            }

            var zset = _store.TryGetSortedSet(GetString(args[1]));

            return ValueTask.FromResult(
                RespValue.Integer(zset?.Count ?? 0));
        }

        private ValueTask<RespValue> ZRank(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'zrank' command");
            }

            var zset = _store.TryGetSortedSet(GetString(args[1]));
            var rank = zset?.Rank(GetString(args[2])) ?? -1;

            return ValueTask.FromResult(
                rank < 0
                    ? RespValue.Null()
                    : RespValue.Integer(rank));
        }

        private ValueTask<RespValue> ZRange(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 4)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'zrange' command");
            }

            if (!long.TryParse(GetString(args[2]), out var start) ||
                !long.TryParse(GetString(args[3]), out var stop))
            {
                return Error(
                    "ERR value is not an integer or out of range");
            }

            var withScores = false;

            for (var i = 4; i < args.Count; i++)
            {
                if (string.Equals(
                        GetString(args[i]),
                        "WITHSCORES",
                        StringComparison.OrdinalIgnoreCase))
                {
                    withScores = true;
                }
                else
                {
                    return Error("ERR syntax error");
                }
            }

            var zset = _store.TryGetSortedSet(GetString(args[1]));

            if (zset == null)
            {
                return ValueTask.FromResult(
                    RespValue.Array(Array.Empty<RespValue>()));
            }

            var items = zset.Range(start, stop);

            var result = withScores
                ? new RespValue[items.Length * 2]
                : new RespValue[items.Length];

            for (var i = 0; i < items.Length; i++)
            {
                if (withScores)
                {
                    result[i * 2] = RespValue.Bulk(
                        Encoding.UTF8.GetBytes(items[i].Member));

                    result[i * 2 + 1] = RespValue.Bulk(
                        Encoding.UTF8.GetBytes(
                            FormatScore(items[i].Score)));
                }
                else
                {
                    result[i] = RespValue.Bulk(
                        Encoding.UTF8.GetBytes(items[i].Member));
                }
            }

            return ValueTask.FromResult(RespValue.Array(result));
        }

        private static string FormatScore(double score)
        {
            return score == Math.Floor(score) &&
                   !double.IsInfinity(score)
                ? ((long)score).ToString(CultureInfo.InvariantCulture)
                : score.ToString("R", CultureInfo.InvariantCulture);
        }

        private ValueTask<RespValue> Info(
            IReadOnlyList<RespValue> args)
        {
            var uptime =
                (long)(DateTimeOffset.UtcNow - _startedAt)
                    .TotalSeconds;

            var sb = new StringBuilder();

            sb.Append("# Server\r\n");
            sb.Append("redis_version:7.4.0\r\n");
            sb.Append("redis_mode:standalone\r\n");
            sb.Append($"os:{Environment.OSVersion}\r\n");
            sb.Append("arch_bits:64\r\n");
            sb.Append($"process_id:{Environment.ProcessId}\r\n");
            sb.Append($"run_id:{_runId}\r\n");
            sb.Append($"tcp_port:{_options.Port}\r\n");
            sb.Append($"uptime_in_seconds:{uptime}\r\n");

            sb.Append("\r\n# Clients\r\n");
            sb.Append("connected_clients:1\r\n");

            sb.Append("\r\n# Memory\r\n");
            sb.Append($"used_memory:{_store.MemoryBytes}\r\n");
            sb.Append($"used_memory_human:{_store.MemoryBytes}B\r\n");
            sb.Append($"maxmemory:{_options.MaxMemoryBytes}\r\n");
            sb.Append("maxmemory_policy:noeviction\r\n");

            sb.Append("\r\n# Persistence\r\n");
            sb.Append("loading:0\r\n");
            sb.Append(
                $"aof_enabled:{(_options.EnableAof ? 1 : 0)}\r\n");

            sb.Append("\r\n# Stats\r\n");
            sb.Append("total_connections_received:0\r\n");
            sb.Append("total_commands_processed:0\r\n");

            sb.Append("\r\n# Replication\r\n");
            sb.Append("role:master\r\n");
            sb.Append("connected_slaves:0\r\n");

            sb.Append("\r\n# CPU\r\n");
            sb.Append("used_cpu_sys:0.0\r\n");
            sb.Append("used_cpu_user:0.0\r\n");

            sb.Append("\r\n# Keyspace\r\n");

            if (_store.Count > 0)
            {
                sb.Append(
                    $"db0:keys={_store.Count},expires=0,avg_ttl=0\r\n");
            }

            return ValueTask.FromResult(
                RespValue.Bulk(
                    Encoding.UTF8.GetBytes(sb.ToString())));
        }

        private ValueTask<RespValue> Client(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'client' command");
            }

            var sub = GetString(args[1]).ToUpperInvariant();

            return sub switch
            {
                "SETNAME" or "SETINFO" or "NO-EVICT" or "NO-TOUCH" =>
                    ValueTask.FromResult(RespValue.Simple("OK")),

                "GETNAME" => ValueTask.FromResult(
                    RespValue.Bulk(Array.Empty<byte>())),

                "ID" => ValueTask.FromResult(
                    RespValue.Integer(1)),

                "LIST" or "INFO" => ValueTask.FromResult(
                    RespValue.Bulk(Array.Empty<byte>())),

                _ => Error(
                    $"ERR Unknown CLIENT subcommand '{sub}'")
            };
        }

        private static readonly IReadOnlyDictionary<string, string>
            DefaultConfig = new Dictionary<string, string>(
                StringComparer.OrdinalIgnoreCase)
            {
                ["maxmemory"] = "0",
                ["maxmemory-policy"] = "noeviction",
                ["appendonly"] = "no",
                ["save"] = "",
                ["timeout"] = "0",
            };

        private ValueTask<RespValue> Config(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'config' command");
            }

            var sub = GetString(args[1]).ToUpperInvariant();

            if (sub == "GET")
            {
                if (args.Count != 3)
                {
                    return Error(
                        "ERR wrong number of arguments " +
                        "for 'config|get' command");
                }

                var pattern = GetString(args[2]);
                var results = new List<RespValue>();

                foreach (var kv in DefaultConfig)
                {
                    if (pattern == "*" ||
                        string.Equals(
                            pattern,
                            kv.Key,
                            StringComparison.OrdinalIgnoreCase))
                    {
                        results.Add(
                            RespValue.Bulk(
                                Encoding.UTF8.GetBytes(kv.Key)));

                        results.Add(
                            RespValue.Bulk(
                                Encoding.UTF8.GetBytes(kv.Value)));
                    }
                }

                if (pattern == "databases")
                {
                    results.Add(
                        RespValue.Bulk(
                            Encoding.UTF8.GetBytes("databases")));

                    results.Add(
                        RespValue.Bulk(
                            Encoding.UTF8.GetBytes(
                                _options.DatabaseCount
                                    .ToString())));
                }

                return ValueTask.FromResult(
                    RespValue.Array(results));
            }

            if (sub == "SET")
            {
                return ValueTask.FromResult(
                    RespValue.Simple("OK"));
            }

            return Error(
                $"ERR Unknown CONFIG subcommand '{sub}'");
        }

        private static ValueTask<RespValue> Command(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count >= 2)
            {
                var sub = GetString(args[1]).ToUpperInvariant();

                if (sub == "COUNT")
                {
                    return ValueTask.FromResult(
                        RespValue.Integer(0));
                }

                if (sub == "DOCS")
                {
                    return ValueTask.FromResult(
                        RespValue.Array(Array.Empty<RespValue>()));
                }
            }

            return ValueTask.FromResult(
                RespValue.Array(Array.Empty<RespValue>()));
        }

        private ValueTask<RespValue> Select(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'select' command");
            }

            if (!int.TryParse(GetString(args[1]), out var index) ||
                index < 0 ||
                index >= _options.DatabaseCount)
            {
                return Error("ERR DB index is out of range");
            }

            if (index != 0)
            {
                return Error(
                    "ERR Recache currently supports only " +
                    "database 0 (single keyspace).");
            }

            return ValueTask.FromResult(
                RespValue.Simple("OK"));
        }

        private ValueTask<RespValue> Echo(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'echo' command");
            }

            return ValueTask.FromResult(
                RespValue.Bulk(GetBytes(args[1])));
        }

        private static string GetString(
            RespValue value)
        {
            return Encoding.UTF8.GetString(
                GetBytes(value));
        }

        private static byte[] GetBytes(
            RespValue value)
        {
            return value.BinaryValue
                ?? Encoding.UTF8.GetBytes(
                    value.StringValue ?? "");
        }

        private static ValueTask<RespValue> Error(
            string message)
        {
            return ValueTask.FromResult(
                RespValue.Error(message));
        }

        private static ValueTask<RespValue> Unknown(
            string command)
        {
            return ValueTask.FromResult(
                RespValue.Error(
                    $"ERR unknown command '{command}'"));
        }
    }
}
