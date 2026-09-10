using recache.Clustering;
using recache.Configuration;
using recache.Protocol;
using recache.Security;
using recache.Storage;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;

namespace recache.Commands
{
    public sealed class CommandDispatcher
    {
        private readonly ICacheStore _store;
        private readonly AuthenticationManager _auth;
        private readonly RecacheOptions _options;
        private readonly ClusterState? _cluster;
        private readonly string _runId;
        private readonly DateTimeOffset _startedAt;

        // Every command normally takes the read lock — many can run
        // concurrently, matching today's behaviour. EXEC takes the
        // write lock for its whole batch, giving transactions real
        // atomicity against other connections despite this server
        // not being single-threaded like real Redis.
        //
        // Safe only because every command handler below completes
        // synchronously (ValueTask.FromResult, no real await) — the
        // lock is always entered and exited on the same thread. If
        // a command ever needs genuine async I/O, this needs an
        // async-aware lock instead of ReaderWriterLockSlim.
        private readonly ReaderWriterLockSlim _executionLock =
            new(LockRecursionPolicy.NoRecursion);

        private static readonly HashSet<string> KnownCommands =
            new(StringComparer.Ordinal)
            {
                "PING", "GET", "SET", "SETEX", "PSETEX", "SETNX",
                "STRLEN", "GETRANGE", "SETRANGE", "APPEND", "DEL",
                "EXISTS", "INCR", "DECR", "INFO", "CLIENT", "CONFIG",
                "COMMAND", "SELECT", "DBSIZE", "ECHO", "SCAN",
                "HSCAN", "SSCAN", "ZSCAN", "TYPE", "TTL", "PTTL",
                "EXPIRE", "PEXPIRE", "PERSIST", "HSET", "HGET",
                "HGETALL", "HDEL", "HEXISTS", "HLEN", "HKEYS",
                "HVALS", "HMGET", "LPUSH", "RPUSH", "LRANGE", "LLEN",
                "LPOP", "RPOP", "LREM", "LSET", "LINDEX", "SADD",
                "SREM", "SMEMBERS", "SISMEMBER", "SCARD", "ZADD",
                "ZREM", "ZSCORE", "ZRANGE", "ZCARD", "ZRANK",
                "MODULE", "JSON.SET", "JSON.GET", "JSON.DEL",
                "JSON.FORGET", "JSON.TYPE", "JSON.OBJKEYS",
                "JSON.STRLEN", "JSON.ARRLEN", "JSON.OBJLEN",
                "JSON.ARRAPPEND", "JSON.NUMINCRBY", "AUTH", "MULTI",
                "EXEC", "DISCARD", "WATCH", "UNWATCH", "CLUSTER",
            };

        public CommandDispatcher(
            ICacheStore store,
            AuthenticationManager auth,
            RecacheOptions options,
            ClusterState? cluster = null)
        {
            _store = store;
            _auth = auth;
            _options = options;
            _cluster = cluster;
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

            switch (command)
            {
                case "MULTI":
                    return HandleMulti(session);

                case "EXEC":
                    return HandleExec(session);

                case "DISCARD":
                    return HandleDiscard(session);

                case "WATCH":
                    return HandleWatch(args, session);

                case "UNWATCH":
                    session.WatchedKeys.Clear();
                    return ValueTask.FromResult(RespValue.Simple("OK"));

                case "ASKING":
                    session.Asking = true;
                    return ValueTask.FromResult(RespValue.Simple("OK"));
            }

            if (_cluster is not null)
            {
                var redirect = CheckClusterRedirect(command, args, session);

                if (redirect is not null)
                    return ValueTask.FromResult(redirect);
            }

            if (session.InTransaction)
            {
                if (!KnownCommands.Contains(command))
                {
                    session.TransactionDirty = true;
                    return Error($"ERR unknown command '{command}'");
                }

                session.QueuedCommands.Add(request);

                return ValueTask.FromResult(
                    RespValue.Simple("QUEUED"));
            }

            _executionLock.EnterReadLock();

            try
            {
                return DispatchAsync(command, args);
            }
            finally
            {
                _executionLock.ExitReadLock();
            }
        }

        // MULTI/EXEC bookkeeping aside, this is identical to the
        // body ExecuteAsync used to run directly — EXEC re-enters it
        // once per queued command while holding the write lock.
        private ValueTask<RespValue> DispatchAsync(
            string command,
            IReadOnlyList<RespValue> args)
        {
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

                    "CLUSTER" => Cluster(args),

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

        private static ValueTask<RespValue> HandleMulti(
            ClientSession session)
        {
            if (session.InTransaction)
                return Error("ERR MULTI calls can not be nested");

            session.InTransaction = true;
            session.TransactionDirty = false;
            session.QueuedCommands.Clear();

            return ValueTask.FromResult(RespValue.Simple("OK"));
        }

        private static ValueTask<RespValue> HandleDiscard(
            ClientSession session)
        {
            if (!session.InTransaction)
                return Error("ERR DISCARD without MULTI");

            session.InTransaction = false;
            session.TransactionDirty = false;
            session.QueuedCommands.Clear();
            session.WatchedKeys.Clear();

            return ValueTask.FromResult(RespValue.Simple("OK"));
        }

        private ValueTask<RespValue> HandleWatch(
            IReadOnlyList<RespValue> args,
            ClientSession session)
        {
            if (args.Count < 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'watch' command");
            }

            if (session.InTransaction)
                return Error("ERR WATCH inside MULTI is not allowed");

            for (var i = 1; i < args.Count; i++)
            {
                var key = GetString(args[i]);

                session.WatchedKeys[key] =
                    _store.TryGetVersion(key, out var version)
                        ? version
                        : null;
            }

            return ValueTask.FromResult(RespValue.Simple("OK"));
        }

        // Executes the whole queued batch under the exclusive write
        // lock, giving real cross-connection atomicity. Aborts with
        // EXECABORT if a queue-time error dirtied the transaction,
        // or with a null array if a watched key changed underneath
        // it — both match real Redis's EXEC semantics.
        private ValueTask<RespValue> HandleExec(ClientSession session)
        {
            if (!session.InTransaction)
                return Error("ERR EXEC without MULTI");

            session.InTransaction = false;

            if (session.TransactionDirty)
            {
                session.TransactionDirty = false;
                session.QueuedCommands.Clear();
                session.WatchedKeys.Clear();

                return Error(
                    "EXECABORT Transaction discarded because of " +
                    "previous errors.");
            }

            var queued = session.QueuedCommands.ToArray();
            session.QueuedCommands.Clear();

            var watched =
                new Dictionary<string, long?>(session.WatchedKeys);

            session.WatchedKeys.Clear();

            _executionLock.EnterWriteLock();

            try
            {
                foreach (var (key, snapshotVersion) in watched)
                {
                    var existsNow =
                        _store.TryGetVersion(key, out var currentVersion);

                    var conflict = snapshotVersion.HasValue
                        ? !existsNow ||
                          currentVersion != snapshotVersion.Value
                        : existsNow;

                    if (conflict)
                        return ValueTask.FromResult(RespValue.NullArray());
                }

                var results = new RespValue[queued.Length];

                for (var i = 0; i < queued.Length; i++)
                {
                    var commandArgs = queued[i].Items!;

                    var commandName =
                        GetString(commandArgs[0]).ToUpperInvariant();

                    // Every handler completes synchronously (see
                    // the comment on _executionLock), so this never
                    // actually awaits — it's safe to read .Result
                    // immediately without yielding the thread that
                    // holds the write lock.
                    results[i] =
                        DispatchAsync(commandName, commandArgs).Result;
                }

                return ValueTask.FromResult(RespValue.Array(results));
            }
            finally
            {
                _executionLock.ExitWriteLock();
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

            _store.AdjustMemory(key, byteDelta);

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

            _store.AdjustMemory(key, byteDelta);

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

            _store.AdjustMemory(key, byteDelta);

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

                _store.AdjustMemory(key, delta);
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
                    _store.AdjustMemory(key, delta);
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

                _store.AdjustMemory(key, delta);
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

            _store.AdjustMemory(key, delta);

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

            _store.AdjustMemory(key, delta);

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

            var key = GetString(args[1]);
            var list = _store.TryGetList(key);

            if (list == null)
                return Error("ERR no such key");

            var value = GetBytes(args[3]);

            if (!list.SetAt(index, value, out var delta))
                return Error("ERR index out of range");

            _store.AdjustMemory(key, delta);

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
                    _store.AdjustMemory(key, delta);
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
                    _store.AdjustMemory(key, delta);
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

                _store.AdjustMemory(key, delta);
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
                    _store.AdjustMemory(key, delta);
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

            sb.Append(
                $"redis_mode:{(_cluster is null ? "standalone" : "cluster")}\r\n");

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

            sb.Append(
                $"role:{(_cluster is { Self.Role: ClusterNodeRole.Replica } ? "slave" : "master")}\r\n");

            sb.Append("connected_slaves:0\r\n");

            if (_cluster is not null)
            {
                sb.Append("\r\n# Cluster\r\n");
                sb.Append("cluster_enabled:1\r\n");
            }

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

        // Computes the slot for a key-bearing command's key(s) and
        // decides whether this node is allowed to serve it. Returns
        // null to let the command proceed normally.
        private RespValue? CheckClusterRedirect(
            string command,
            IReadOnlyList<RespValue> args,
            ClientSession session)
        {
            var keyIndexes = ClusterKeyMap.GetKeyIndexes(command, args.Count);

            if (keyIndexes.Count == 0)
                return null;

            var asking = session.Asking;
            session.Asking = false;

            int? slot = null;

            foreach (var index in keyIndexes)
            {
                var keySlot = HashSlot.Compute(GetString(args[index]));

                if (slot is null)
                    slot = keySlot;
                else if (slot != keySlot)
                {
                    return RespValue.Error(
                        "CROSSSLOT Keys in request don't hash to " +
                        "the same slot");
                }
            }

            var slotValue = slot!.Value;
            var owner = _cluster!.GetOwner(slotValue);

            if (owner is null)
                return RespValue.Error("CLUSTERDOWN Hash slot not served");

            if (owner.Id == _cluster.MyId)
            {
                var migration = _cluster.GetMigration(slotValue);

                if (migration is { State: SlotMigrationState.Migrating } m &&
                    keyIndexes.Count == 1 &&
                    !_store.Exists(GetString(args[keyIndexes[0]])))
                {
                    var target = _cluster.GetNode(m.PeerNodeId);

                    if (target is not null)
                    {
                        return RespValue.Error(
                            $"ASK {slotValue} {target.Host}:{target.Port}");
                    }
                }

                return null;
            }

            var importing = _cluster.GetMigration(slotValue);

            if (asking &&
                importing is { State: SlotMigrationState.Importing })
            {
                return null;
            }

            return RespValue.Error(
                $"MOVED {slotValue} {owner.Host}:{owner.Port}");
        }

        private ValueTask<RespValue> Cluster(
            IReadOnlyList<RespValue> args)
        {
            if (_cluster is null)
            {
                return Error(
                    "ERR This instance has cluster support disabled");
            }

            if (args.Count < 2)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'cluster' command");
            }

            var sub = GetString(args[1]).ToUpperInvariant();

            switch (sub)
            {
                case "MYID":
                    return ValueTask.FromResult(
                        RespValue.Bulk(
                            Encoding.UTF8.GetBytes(_cluster.MyId)));

                case "INFO":
                    return ValueTask.FromResult(ClusterInfo());

                case "NODES":
                    return ValueTask.FromResult(
                        RespValue.Bulk(
                            Encoding.UTF8.GetBytes(
                                _cluster.ToNodesText())));

                case "SLOTS":
                    return ValueTask.FromResult(ClusterSlotsReply());

                case "SHARDS":
                    return ValueTask.FromResult(ClusterShardsReply());

                case "KEYSLOT":
                    if (args.Count != 3)
                    {
                        return Error(
                            "ERR wrong number of arguments " +
                            "for 'cluster|keyslot' command");
                    }

                    return ValueTask.FromResult(
                        RespValue.Integer(
                            HashSlot.Compute(GetString(args[2]))));

                case "COUNTKEYSINSLOT":
                    return ClusterCountKeysInSlot(args);

                case "GETKEYSINSLOT":
                    return ClusterGetKeysInSlot(args);

                case "ADDSLOTS":
                    return ClusterAddSlots(args, ranges: false);

                case "ADDSLOTSRANGE":
                    return ClusterAddSlots(args, ranges: true);

                case "DELSLOTS":
                    return ClusterDelSlots(args, ranges: false);

                case "DELSLOTSRANGE":
                    return ClusterDelSlots(args, ranges: true);

                case "SETSLOT":
                    return ClusterSetSlot(args);

                case "MEET":
                    return ClusterMeet(args);

                case "FORGET":
                    if (args.Count != 3)
                    {
                        return Error(
                            "ERR wrong number of arguments " +
                            "for 'cluster|forget' command");
                    }

                    var forgetId = GetString(args[2]);

                    return ValueTask.FromResult(
                        _cluster.Forget(forgetId)
                            ? RespValue.Simple("OK")
                            : RespValue.Error(
                                $"ERR Unknown node {forgetId}"));

                case "REPLICATE":
                    return ClusterReplicate(args);

                case "RESET":
                    var hard = args.Count >= 3 &&
                        string.Equals(
                            GetString(args[2]),
                            "HARD",
                            StringComparison.OrdinalIgnoreCase);

                    _cluster.Reset(hard);

                    return ValueTask.FromResult(RespValue.Simple("OK"));

                case "SET-CONFIG-EPOCH":
                    if (args.Count != 3 ||
                        !long.TryParse(GetString(args[2]), out var epoch))
                    {
                        return Error(
                            "ERR Invalid config epoch specified: " +
                            "must be a non-negative integer");
                    }

                    _cluster.SetConfigEpoch(epoch);

                    return ValueTask.FromResult(RespValue.Simple("OK"));

                case "BUMPEPOCH":
                    var newEpoch = _cluster.BumpEpoch();

                    return ValueTask.FromResult(
                        RespValue.Simple($"BUMPED {newEpoch}"));

                case "SAVECONFIG":
                    // ClusterState persists to ClusterConfigFile on
                    // every mutation already, so there's nothing
                    // extra to flush.
                    return ValueTask.FromResult(RespValue.Simple("OK"));

                default:
                    return Error(
                        $"ERR Unknown CLUSTER subcommand '{sub}'");
            }
        }

        private ValueTask<RespValue> ClusterCountKeysInSlot(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 3 ||
                !int.TryParse(GetString(args[2]), out var slot) ||
                slot < 0 ||
                slot >= HashSlot.SlotCount)
            {
                return Error("ERR Invalid slot");
            }

            long count = 0;

            foreach (var key in _store.Keys)
            {
                if (HashSlot.Compute(key) == slot)
                    count++;
            }

            return ValueTask.FromResult(RespValue.Integer(count));
        }

        private ValueTask<RespValue> ClusterGetKeysInSlot(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 4 ||
                !int.TryParse(GetString(args[2]), out var slot) ||
                slot < 0 ||
                slot >= HashSlot.SlotCount ||
                !int.TryParse(GetString(args[3]), out var count) ||
                count < 0)
            {
                return Error("ERR Invalid slot or count");
            }

            var result = new List<RespValue>();

            foreach (var key in _store.Keys)
            {
                if (result.Count >= count)
                    break;

                if (HashSlot.Compute(key) == slot)
                {
                    result.Add(
                        RespValue.Bulk(Encoding.UTF8.GetBytes(key)));
                }
            }

            return ValueTask.FromResult(RespValue.Array(result));
        }

        private ValueTask<RespValue> ClusterAddSlots(
            IReadOnlyList<RespValue> args,
            bool ranges)
        {
            if (!TryParseSlotArgs(args, ranges, out var slots, out var err))
                return Error(err!);

            var busy = _cluster!.AddSlots(slots);

            if (busy.Count > 0)
            {
                return Error(
                    $"ERR Slot {busy[0]} is already busy");
            }

            return ValueTask.FromResult(RespValue.Simple("OK"));
        }

        private ValueTask<RespValue> ClusterDelSlots(
            IReadOnlyList<RespValue> args,
            bool ranges)
        {
            if (!TryParseSlotArgs(args, ranges, out var slots, out var err))
                return Error(err!);

            var free = _cluster!.RemoveSlots(slots);

            if (free.Count > 0)
            {
                return Error(
                    $"ERR Slot {free[0]} is already unassigned");
            }

            return ValueTask.FromResult(RespValue.Simple("OK"));
        }

        private static bool TryParseSlotArgs(
            IReadOnlyList<RespValue> args,
            bool ranges,
            out List<int> slots,
            out string? error)
        {
            slots = new List<int>();
            error = null;

            if (args.Count < 3 || (ranges && args.Count % 2 != 0))
            {
                error =
                    "ERR wrong number of arguments for 'cluster' command";

                return false;
            }

            if (ranges)
            {
                for (var i = 2; i < args.Count; i += 2)
                {
                    if (!TryParseSlot(GetString(args[i]), out var start) ||
                        !TryParseSlot(
                            GetString(args[i + 1]),
                            out var end) ||
                        start > end)
                    {
                        error = "ERR Invalid slot range";
                        return false;
                    }

                    for (var slot = start; slot <= end; slot++)
                        slots.Add(slot);
                }
            }
            else
            {
                for (var i = 2; i < args.Count; i++)
                {
                    if (!TryParseSlot(GetString(args[i]), out var slot))
                    {
                        error = "ERR Invalid slot";
                        return false;
                    }

                    slots.Add(slot);
                }
            }

            return true;
        }

        private static bool TryParseSlot(string text, out int slot)
        {
            return int.TryParse(text, out slot) &&
                slot >= 0 &&
                slot < HashSlot.SlotCount;
        }

        private ValueTask<RespValue> ClusterSetSlot(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 4 ||
                !TryParseSlot(GetString(args[2]), out var slot))
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'cluster|setslot' command");
            }

            var mode = GetString(args[3]).ToUpperInvariant();

            switch (mode)
            {
                case "MIGRATING":
                    if (args.Count != 5)
                        return Error("ERR wrong number of arguments");

                    var migrateTarget = GetString(args[4]);

                    if (_cluster!.GetNode(migrateTarget) is null)
                        return Error($"ERR Unknown node {migrateTarget}");

                    if (_cluster.GetOwner(slot)?.Id != _cluster.MyId)
                    {
                        return Error(
                            $"ERR I'm not the owner of hash slot {slot}");
                    }

                    _cluster.SetMigrating(slot, migrateTarget);
                    return ValueTask.FromResult(RespValue.Simple("OK"));

                case "IMPORTING":
                    if (args.Count != 5)
                        return Error("ERR wrong number of arguments");

                    var importSource = GetString(args[4]);

                    if (_cluster!.GetNode(importSource) is null)
                        return Error($"ERR Unknown node {importSource}");

                    if (_cluster.GetOwner(slot)?.Id == _cluster.MyId)
                    {
                        return Error(
                            $"ERR I'm already the owner of hash " +
                            $"slot {slot}");
                    }

                    _cluster.SetImporting(slot, importSource);
                    return ValueTask.FromResult(RespValue.Simple("OK"));

                case "STABLE":
                    _cluster!.ClearMigration(slot);
                    return ValueTask.FromResult(RespValue.Simple("OK"));

                case "NODE":
                    if (args.Count != 5)
                        return Error("ERR wrong number of arguments");

                    var owner = GetString(args[4]);

                    if (_cluster!.GetNode(owner) is null)
                        return Error($"ERR Unknown node {owner}");

                    _cluster.AssignSlot(slot, owner);
                    return ValueTask.FromResult(RespValue.Simple("OK"));

                default:
                    return Error(
                        $"ERR Unknown CLUSTER SETSLOT mode '{mode}'");
            }
        }

        private ValueTask<RespValue> ClusterMeet(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 4 ||
                !int.TryParse(GetString(args[3]), out var port))
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'cluster|meet' command");
            }

            var host = GetString(args[2]);

            var nodesText = ClusterMeetClient.FetchNodesText(
                host,
                port,
                TimeSpan.FromSeconds(3));

            if (nodesText is null)
            {
                return Error(
                    $"ERR Could not connect to node at {host}:{port}");
            }

            _cluster!.MergeNodesText(nodesText);

            return ValueTask.FromResult(RespValue.Simple("OK"));
        }

        private ValueTask<RespValue> ClusterReplicate(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count != 3)
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'cluster|replicate' command");
            }

            var masterId = GetString(args[2]);

            if (_cluster!.GetNode(masterId) is null)
                return Error($"ERR Unknown node {masterId}");

            _cluster.SetRole(ClusterNodeRole.Replica, masterId);

            return ValueTask.FromResult(RespValue.Simple("OK"));
        }

        private RespValue ClusterInfo()
        {
            var self = _cluster!.Self;
            var assigned = _cluster.SlotRanges()
                .Sum(r => r.End - r.Start + 1);

            var sb = new StringBuilder();

            sb.Append("cluster_enabled:1\r\n");

            sb.Append(
                $"cluster_state:" +
                $"{(assigned == HashSlot.SlotCount ? "ok" : "fail")}\r\n");

            sb.Append($"cluster_slots_assigned:{assigned}\r\n");
            sb.Append("cluster_slots_ok:" + assigned + "\r\n");
            sb.Append("cluster_slots_pfail:0\r\n");
            sb.Append("cluster_slots_fail:0\r\n");
            sb.Append($"cluster_known_nodes:{_cluster.Nodes.Count}\r\n");

            sb.Append(
                "cluster_size:" +
                $"{_cluster.Nodes.Count(n => n.Role == ClusterNodeRole.Master)}" +
                "\r\n");

            sb.Append($"cluster_current_epoch:{_cluster.CurrentEpoch}\r\n");
            sb.Append($"cluster_my_epoch:{self.ConfigEpoch}\r\n");
            sb.Append("cluster_stats_messages_sent:0\r\n");
            sb.Append("cluster_stats_messages_received:0\r\n");
            sb.Append("total_cluster_links_buffer_limit_exceeded:0\r\n");

            return RespValue.Bulk(Encoding.UTF8.GetBytes(sb.ToString()));
        }

        private RespValue ClusterSlotsReply()
        {
            var result = new List<RespValue>();

            foreach (var (start, end, nodeId) in _cluster!.SlotRanges())
            {
                var node = _cluster.GetNode(nodeId);

                if (node is null)
                    continue;

                result.Add(RespValue.Array(new[]
                {
                    RespValue.Integer(start),
                    RespValue.Integer(end),
                    RespValue.Array(new[]
                    {
                        RespValue.Bulk(Encoding.UTF8.GetBytes(node.Host)),
                        RespValue.Integer(node.Port),
                        RespValue.Bulk(Encoding.UTF8.GetBytes(node.Id)),
                        RespValue.Array(Array.Empty<RespValue>()),
                    }),
                }));
            }

            return RespValue.Array(result);
        }

        private RespValue ClusterShardsReply()
        {
            var result = new List<RespValue>();

            foreach (var (start, end, nodeId) in _cluster!.SlotRanges())
            {
                var node = _cluster.GetNode(nodeId);

                if (node is null)
                    continue;

                result.Add(RespValue.Array(new[]
                {
                    RespValue.Bulk(Encoding.UTF8.GetBytes("slots")),
                    RespValue.Array(new[]
                    {
                        RespValue.Integer(start),
                        RespValue.Integer(end),
                    }),
                    RespValue.Bulk(Encoding.UTF8.GetBytes("nodes")),
                    RespValue.Array(new[]
                    {
                        RespValue.Array(new[]
                        {
                            RespValue.Bulk(
                                Encoding.UTF8.GetBytes("id")),
                            RespValue.Bulk(
                                Encoding.UTF8.GetBytes(node.Id)),
                            RespValue.Bulk(
                                Encoding.UTF8.GetBytes("port")),
                            RespValue.Integer(node.Port),
                            RespValue.Bulk(
                                Encoding.UTF8.GetBytes("ip")),
                            RespValue.Bulk(
                                Encoding.UTF8.GetBytes(node.Host)),
                            RespValue.Bulk(
                                Encoding.UTF8.GetBytes("role")),
                            RespValue.Bulk(
                                Encoding.UTF8.GetBytes(
                                    node.Role == ClusterNodeRole.Master
                                        ? "master"
                                        : "replica")),
                        }),
                    }),
                }));
            }

            return RespValue.Array(result);
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
