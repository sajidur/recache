using recache.Clustering;
using recache.Configuration;
using recache.Monitoring;
using recache.Persistence;
using recache.Protocol;
using recache.Replication;
using recache.Security;
using recache.Server;
using recache.Storage;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;

namespace recache.Commands
{
    public sealed partial class CommandDispatcher
    {
        // Returned by commands that write to the socket themselves
        // (PSYNC hands the connection over to the replication code).
        public static readonly RespValue NoReply = RespValue.Simple("<no reply>");

        private static readonly RespValue SetName = RespValue.Bulk("SET");
        private static readonly RespValue PxatName = RespValue.Bulk("PXAT");
        private static readonly RespValue PexpireatName = RespValue.Bulk("PEXPIREAT");
        private static readonly RespValue DelName = RespValue.Bulk("DEL");
        private static readonly RespValue RestoreName = RespValue.Bulk("RESTORE");
        private static readonly RespValue ReplaceName = RespValue.Bulk("REPLACE");
        private static readonly RespValue AbsttlName = RespValue.Bulk("ABSTTL");
        private static readonly RespValue MultiName = RespValue.Bulk("MULTI");
        private static readonly RespValue ExecName = RespValue.Bulk("EXEC");

        private readonly ICacheStore _store;
        private readonly AuthenticationManager _auth;
        private readonly RecacheOptions _options;
        private readonly ClusterState? _cluster;
        private readonly KeyLocks? _locks;
        private readonly string _runId;
        private readonly DateTimeOffset _startedAt;

        public CommandDispatcher(
            ICacheStore store,
            AuthenticationManager auth,
            RecacheOptions options,
            ClusterState? cluster = null,
            KeyLocks? locks = null)
        {
            _store = store;
            _auth = auth;
            _options = options;
            _cluster = cluster;
            _locks = locks;
            _runId = Guid.NewGuid().ToString("N");
            _startedAt = DateTimeOffset.UtcNow;
        }

        // Wired by RecacheNode after construction (several depend on
        // the dispatcher in turn). All optional: the throwaway
        // dispatcher that loads a replica's full-resync payload into a
        // side store has none of them.
        public ReplicationManager? Replication { get; set; }

        public SnapshotService? Snapshots { get; set; }

        public AofManager? Aof { get; set; }

        public WriteGate? WriteGate { get; set; }

        public RedisMetrics? Metrics { get; set; }

        public ClusterManager? ClusterManager { get; set; }

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

            Metrics?.CommandProcessed();

            var info = LookupCommand(args[0]);

            if (info is null)
            {
                if (session.InTransaction)
                    session.TransactionDirty = true;

                return Unknown(GetString(args[0]));
            }

            var command = info.Name;

            if (command == "AUTH")
                return Auth(args, session);

            if (_auth.RequiresAuth &&
                !session.IsAuthenticated &&
                !session.IsMasterLink &&
                command != "QUIT")
            {
                return ValueTask.FromResult(
                    RespValue.Error(
                        "NOAUTH Authentication required."));
            }

            if (!ArityMatches(info, args.Count))
            {
                if (session.InTransaction)
                    session.TransactionDirty = true;

                return Error(
                    $"ERR wrong number of arguments for " +
                    $"'{command.ToLowerInvariant()}' command");
            }

            if (session.InTransaction &&
                (info.Flags & CommandFlags.NoMulti) != 0 &&
                command != "WATCH")
            {
                session.TransactionDirty = true;
                return Error("ERR Command not allowed inside a transaction");
            }

            if ((info.Flags & CommandFlags.Session) != 0)
                return ExecuteSessionCommand(info, args, session, cancellationToken);

            if (info.IsWrite &&
                !session.IsMasterLink &&
                _cluster is null &&
                Replication is { IsReplica: true })
            {
                return Error(
                    "READONLY You can't write against a read only replica.");
            }

            if (_cluster is not null && !session.IsMasterLink)
            {
                var redirect = CheckClusterRedirect(info, args, session);

                if (redirect is not null)
                {
                    if (session.InTransaction)
                        session.TransactionDirty = true;

                    return ValueTask.FromResult(redirect);
                }
            }

            if (session.InTransaction)
            {
                session.QueuedCommands.Add(request);

                return ValueTask.FromResult(
                    RespValue.Simple("QUEUED"));
            }

            return Execute(info, args, request, session, cancellationToken);
        }

        // Runs one command under the locks its keys require, then
        // propagates it (to the AOF and replicas) if it changed
        // anything — still holding those locks, so the stream records
        // writes to any one key in exactly the order they were applied.
        private ValueTask<RespValue> Execute(
            CommandInfo info,
            IReadOnlyList<RespValue> args,
            RespValue request,
            ClientSession session,
            CancellationToken cancellationToken)
        {
            var gate = WriteGate;
            var write = info.IsWrite;

            if (gate is not null && gate.Blocks(write))
                return RetryAfterPauseAsync(request, session, cancellationToken);

            var locks = _locks;
            var keys = info.HasKeys
                ? info.GetKeyIndexes(args)
                : Array.Empty<int>();

            long offset = 0;
            var paused = false;
            RespValue reply;

            if (locks is null || (keys.Count == 0 && (info.Flags & CommandFlags.AllKeys) == 0))
            {
                reply = RunCommand(info, args, session, ref offset);
            }
            else if ((info.Flags & CommandFlags.AllKeys) != 0)
            {
                locks.EnterAll();

                try
                {
                    if (write && gate is not null && gate.Blocks(true))
                    {
                        paused = true;
                        reply = RespValue.Ok;
                    }
                    else
                    {
                        if (write)
                            Snapshots?.BeforeFlushAll();

                        reply = RunCommand(info, args, session, ref offset);
                    }
                }
                finally
                {
                    locks.ExitAll();
                }
            }
            else if (keys.Count == 1)
            {
                var stripe = KeyLocks.StripeOf(KeyBytes(args[keys[0]]));

                locks.Enter(stripe, write);

                try
                {
                    if (write && gate is not null && gate.Blocks(true))
                    {
                        paused = true;
                        reply = RespValue.Ok;
                    }
                    else
                    {
                        if (write)
                            BeforeWrite(args, keys);

                        reply = RunCommand(info, args, session, ref offset);
                    }
                }
                finally
                {
                    locks.Exit(stripe, write);
                }
            }
            else
            {
                Span<int> stripes = keys.Count <= 64
                    ? stackalloc int[keys.Count]
                    : new int[keys.Count];

                for (var i = 0; i < keys.Count; i++)
                    stripes[i] = KeyLocks.StripeOf(KeyBytes(args[keys[i]]));

                var held = stripes[..KeyLocks.Normalize(stripes)];

                locks.EnterMany(held, write);

                try
                {
                    if (write && gate is not null && gate.Blocks(true))
                    {
                        paused = true;
                        reply = RespValue.Ok;
                    }
                    else
                    {
                        if (write)
                            BeforeWrite(args, keys);

                        reply = RunCommand(info, args, session, ref offset);
                    }
                }
                finally
                {
                    locks.ExitMany(held, write);
                }
            }

            // Decided under the locks, acted on after releasing them:
            // waiting on the pause while holding a key lock would
            // deadlock against the pause's own drain.
            if (paused)
                return RetryAfterPauseAsync(request, session, cancellationToken);

            // The connection holds the reply until the AOF covers this
            // offset — once per batch, not per command (see
            // ClientConnection).
            if (offset > 0)
                session.LastWriteOffset = offset;

            return ValueTask.FromResult(reply);
        }

        private RespValue RunCommand(
            CommandInfo info,
            IReadOnlyList<RespValue> args,
            ClientSession session,
            ref long offset)
        {
            if (!info.IsWrite)
                return DispatchAsync(info.Name, args, session).Result;

            ExecutionScope.Dirty = 0;
            ExecutionScope.PropagationOverride = null;

            // Every handler completes synchronously (none awaits while
            // holding key locks), so .Result never blocks.
            var reply = DispatchAsync(info.Name, args, session).Result;

            if (ExecutionScope.Dirty > 0 &&
                Replication is { PropagationEnabled: true, Loading: false } replication)
            {
                offset = Propagate(replication.Backlog, info.Name, args);
            }

            ExecutionScope.PropagationOverride = null;
            return reply;
        }

        private async ValueTask<RespValue> RetryAfterPauseAsync(
            RespValue request,
            ClientSession session,
            CancellationToken cancellationToken)
        {
            await WriteGate!.WaitAsync().WaitAsync(cancellationToken);

            // Everything is re-checked from the top — notably cluster
            // slot ownership, which a coordinated failover changes
            // while writes are paused.
            return await ExecuteAsync(request, session, cancellationToken);
        }

        // Copy-on-write for an in-progress snapshot: capture each key's
        // pre-image before this write changes it.
        private void BeforeWrite(
            IReadOnlyList<RespValue> args,
            IReadOnlyList<int> keys)
        {
            var snapshots = Snapshots;

            if (snapshots is null || !snapshots.InProgress)
                return;

            foreach (var index in keys)
                snapshots.BeforeWrite(GetString(args[index]));
        }

        // Appends a write to the replication stream in deterministic
        // form: anything relative to "now" (EXPIRE 10, SET ... EX 10,
        // SETEX) is rewritten with the absolute instant this node
        // actually stored, so the AOF replayed later — or a replica
        // applying it a moment later — ends up with the same expiry.
        private long Propagate(
            ReplicationBacklog backlog,
            string command,
            IReadOnlyList<RespValue> args)
        {
            if (ExecutionScope.PropagationOverride is { } overrides)
            {
                long end = 0;

                foreach (var replacement in overrides)
                    end = backlog.AppendCommand(replacement);

                return end;
            }

            return backlog.AppendCommand(RewriteForPropagation(command, args));
        }

        private IReadOnlyList<RespValue> RewriteForPropagation(
            string command,
            IReadOnlyList<RespValue> args)
        {
            switch (command)
            {
                case "SET":
                case "SETEX":
                case "PSETEX":
                {
                    // The overwhelmingly common plain SET k v needs no
                    // rewrite (and no extra lookup).
                    if (command == "SET" && args.Count == 3)
                        return args;

                    if (!_store.TryGetExpireAt(GetString(args[1]), out var expiresAt))
                        return args;

                    var value = command == "SET" ? args[2] : args[3];

                    return expiresAt is { } ms
                        ? new[] { SetName, args[1], value, PxatName, RespValue.Bulk(ms.ToString(CultureInfo.InvariantCulture)) }
                        : new[] { SetName, args[1], value };
                }

                case "EXPIRE":
                case "PEXPIRE":
                case "EXPIREAT":
                case "PEXPIREAT":
                {
                    // A TTL in the past deleted the key.
                    return _store.TryGetExpireAt(GetString(args[1]), out var expiresAt) &&
                           expiresAt is { } ms
                        ? new[] { PexpireatName, args[1], RespValue.Bulk(ms.ToString(CultureInfo.InvariantCulture)) }
                        : new[] { DelName, args[1] };
                }

                case "RESTORE":
                case "RESTORE-ASKING":
                {
                    if (!_store.TryGetExpireAt(GetString(args[1]), out var expiresAt))
                        return args;

                    return expiresAt is { } ms
                        ? new[] { RestoreName, args[1], RespValue.Bulk(ms.ToString(CultureInfo.InvariantCulture)), args[3], ReplaceName, AbsttlName }
                        : new[] { RestoreName, args[1], RespValue.Bulk("0"), args[3], ReplaceName };
                }

                default:
                    return args;
            }
        }

        private ValueTask<RespValue> ExecuteSessionCommand(
            CommandInfo info,
            IReadOnlyList<RespValue> args,
            ClientSession session,
            CancellationToken cancellationToken)
        {
            switch (info.Name)
            {
                case "MULTI":
                    return HandleMulti(session);

                case "EXEC":
                    return HandleExec(session, cancellationToken);

                case "DISCARD":
                    return HandleDiscard(session);

                case "WATCH":
                    if (_cluster is not null)
                    {
                        var redirect = CheckClusterRedirect(info, args, session);

                        if (redirect is not null)
                            return ValueTask.FromResult(redirect);
                    }

                    return HandleWatch(args, session);

                case "UNWATCH":
                    session.WatchedKeys.Clear();
                    return ValueTask.FromResult(RespValue.Simple("OK"));

                case "ASKING":
                    session.Asking = true;
                    return ValueTask.FromResult(RespValue.Simple("OK"));

                case "READONLY":
                    if (_cluster is null)
                    {
                        return Error(
                            "ERR This instance has cluster support disabled");
                    }

                    session.ReadOnlyMode = true;
                    return ValueTask.FromResult(RespValue.Simple("OK"));

                case "READWRITE":
                    session.ReadOnlyMode = false;
                    return ValueTask.FromResult(RespValue.Simple("OK"));

                case "QUIT":
                    session.Handoff = SessionHandoff.Close;
                    return ValueTask.FromResult(RespValue.Simple("OK"));

                case "WAIT":
                    return Wait(args, session, cancellationToken);

                case "PSYNC":
                case "SYNC":
                    return HandlePsync(args, session);

                default:
                    return Unknown(info.Name);
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
                long? version = null;

                WithKeyLock(key, write: false, () =>
                {
                    version = _store.TryGetVersion(key, out var current)
                        ? current
                        : null;
                });

                session.WatchedKeys[key] = version;
            }

            return ValueTask.FromResult(RespValue.Simple("OK"));
        }

        // Aborts with EXECABORT if a queue-time error dirtied the
        // transaction; otherwise runs the whole batch atomically.
        private ValueTask<RespValue> HandleExec(
            ClientSession session,
            CancellationToken cancellationToken)
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

            return RunTransaction(queued, watched, session, cancellationToken);
        }

        // Executes the queued batch holding the locks of every key it
        // touches (and every watched key) for its whole duration, so
        // no other client — reader or writer — can observe it half
        // applied; that's real cross-connection atomicity despite
        // this server not being single-threaded like Redis. A failed
        // WATCH aborts with a null array, matching real Redis.
        private ValueTask<RespValue> RunTransaction(
            RespValue[] queued,
            Dictionary<string, long?> watched,
            ClientSession session,
            CancellationToken cancellationToken)
        {
            var infos = new CommandInfo[queued.Length];
            var stripes = new List<int>();
            var hasWrite = false;
            var allKeys = false;

            for (var i = 0; i < queued.Length; i++)
            {
                var commandArgs = queued[i].Items!;
                var info = LookupCommand(commandArgs[0])!;

                infos[i] = info;
                hasWrite |= info.IsWrite;
                allKeys |= (info.Flags & CommandFlags.AllKeys) != 0;

                if (info.HasKeys)
                {
                    foreach (var index in info.GetKeyIndexes(commandArgs))
                        stripes.Add(KeyLocks.StripeOf(KeyBytes(commandArgs[index])));
                }
            }

            foreach (var key in watched.Keys)
                stripes.Add(KeyLocks.StripeOf(key));

            var gate = WriteGate;

            if (hasWrite && gate is not null && gate.Blocks(true) && !session.IsMasterLink)
                return RetryTransactionAfterPauseAsync(queued, watched, session, cancellationToken);

            var held = stripes.ToArray();
            var count = KeyLocks.Normalize(held);
            var locks = _locks;
            var paused = false;
            long offset = 0;
            RespValue reply;

            if (locks is not null)
            {
                if (allKeys)
                    locks.EnterAll();
                else
                    locks.EnterMany(held.AsSpan(0, count), write: true);
            }

            try
            {
                if (hasWrite && gate is not null && gate.Blocks(true) && !session.IsMasterLink)
                {
                    paused = true;
                    reply = RespValue.Ok;
                }
                else if (WatchConflict(watched))
                {
                    reply = RespValue.NullArray();
                }
                else
                {
                    reply = ExecuteQueued(queued, infos, allKeys, session, ref offset);
                }
            }
            finally
            {
                if (locks is not null)
                {
                    if (allKeys)
                        locks.ExitAll();
                    else
                        locks.ExitMany(held.AsSpan(0, count), write: true);
                }
            }

            if (paused)
                return RetryTransactionAfterPauseAsync(queued, watched, session, cancellationToken);

            if (offset > 0)
                session.LastWriteOffset = offset;

            return ValueTask.FromResult(reply);
        }

        private bool WatchConflict(Dictionary<string, long?> watched)
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
                    return true;
            }

            return false;
        }

        private RespValue ExecuteQueued(
            RespValue[] queued,
            CommandInfo[] infos,
            bool allKeys,
            ClientSession session,
            ref long offset)
        {
            var snapshots = Snapshots;

            if (snapshots is { InProgress: true })
            {
                if (allKeys)
                    snapshots.BeforeFlushAll();

                for (var i = 0; i < queued.Length; i++)
                {
                    if (infos[i].IsWrite && infos[i].HasKeys)
                        BeforeWrite(queued[i].Items!, infos[i].GetKeyIndexes(queued[i].Items!));
                }
            }

            var replication = Replication;
            var propagate = replication is { PropagationEnabled: true, Loading: false };
            List<IReadOnlyList<RespValue>>? propagated = null;
            var results = new RespValue[queued.Length];

            for (var i = 0; i < queued.Length; i++)
            {
                var commandArgs = queued[i].Items!;

                ExecutionScope.Dirty = 0;
                ExecutionScope.PropagationOverride = null;

                results[i] = DispatchAsync(infos[i].Name, commandArgs, session).Result;

                if (propagate && infos[i].IsWrite && ExecutionScope.Dirty > 0)
                {
                    propagated ??= new List<IReadOnlyList<RespValue>>();

                    if (ExecutionScope.PropagationOverride is { } overrides)
                        propagated.AddRange(overrides);
                    else
                        propagated.Add(RewriteForPropagation(infos[i].Name, commandArgs));
                }
            }

            ExecutionScope.PropagationOverride = null;

            if (propagated is not null)
                offset = AppendTransaction(replication!.Backlog, propagated);

            return RespValue.Array(results);
        }

        // MULTI + commands + EXEC as one append, so neither a replica
        // nor a snapshot can ever see part of a transaction.
        private static long AppendTransaction(
            ReplicationBacklog backlog,
            List<IReadOnlyList<RespValue>> commands)
        {
            if (commands.Count == 1)
                return backlog.AppendCommand(commands[0]);

            var buffer = new ArrayBufferWriter<byte>(256 * (commands.Count + 2));

            RespSerializer.WriteCommand(buffer, new[] { MultiName });

            foreach (var command in commands)
                RespSerializer.WriteCommand(buffer, command);

            RespSerializer.WriteCommand(buffer, new[] { ExecName });

            return backlog.Append(buffer.WrittenSpan);
        }

        private async ValueTask<RespValue> RetryTransactionAfterPauseAsync(
            RespValue[] queued,
            Dictionary<string, long?> watched,
            ClientSession session,
            CancellationToken cancellationToken)
        {
            await WriteGate!.WaitAsync().WaitAsync(cancellationToken);

            // The pause may have been a failover that moved these slots
            // elsewhere; if so the client must retry the whole
            // transaction against the new owner.
            if (_cluster is not null)
            {
                foreach (var command in queued)
                {
                    var commandArgs = command.Items!;
                    var info = LookupCommand(commandArgs[0])!;
                    var redirect = CheckClusterRedirect(info, commandArgs, session);

                    if (redirect is not null)
                        return redirect;
                }
            }

            return await RunTransaction(queued, watched, session, cancellationToken);
        }

        // Applies one command from a master's replication stream, or
        // from an AOF / snapshot being loaded. Never re-propagated:
        // `raw` (the command's exact bytes from the stream) is appended
        // to this node's own stream instead — while the command's key
        // locks are still held, like any write — so this node's offsets
        // stay byte-identical to its master's. Empty when loading.
        public RespValue ExecuteReplicated(
            IReadOnlyList<RespValue> args,
            ClientSession session,
            ReadOnlySpan<byte> raw)
        {
            if (args.Count == 0)
                return RespValue.Error("ERR empty command");

            var backlog = raw.IsEmpty ? null : Replication?.Backlog;
            var info = LookupCommand(args[0]);

            if (info is null)
            {
                backlog?.Append(raw);
                return RespValue.Error($"ERR unknown command '{GetString(args[0])}'");
            }

            switch (info.Name)
            {
                case "MULTI":
                    session.InTransaction = true;
                    session.QueuedCommands.Clear();
                    session.ReplicatedTransactionBytes = backlog is null
                        ? null
                        : new ArrayBufferWriter<byte>(1024);

                    session.ReplicatedTransactionBytes?.Write(raw);
                    return RespValue.Ok;

                case "EXEC":
                    return ApplyReplicatedTransaction(session, raw);

                case "DISCARD":
                    session.InTransaction = false;
                    session.QueuedCommands.Clear();
                    session.ReplicatedTransactionBytes = null;
                    backlog?.Append(raw);
                    return RespValue.Ok;

                case "PING":
                case "SELECT":
                case "REPLCONF":
                    backlog?.Append(raw);
                    return RespValue.Ok;
            }

            if (session.InTransaction)
            {
                session.QueuedCommands.Add(RespValue.Array(args));
                session.ReplicatedTransactionBytes?.Write(raw);
                return RespValue.Simple("QUEUED");
            }

            var write = info.IsWrite;
            var keys = info.HasKeys ? info.GetKeyIndexes(args) : Array.Empty<int>();
            var locks = _locks;
            var allKeys = (info.Flags & CommandFlags.AllKeys) != 0;
            int[]? held = null;
            var count = 0;

            if (locks is not null)
            {
                if (allKeys)
                {
                    locks.EnterAll();
                }
                else if (keys.Count > 0)
                {
                    held = new int[keys.Count];

                    for (var i = 0; i < keys.Count; i++)
                        held[i] = KeyLocks.StripeOf(KeyBytes(args[keys[i]]));

                    count = KeyLocks.Normalize(held);
                    locks.EnterMany(held.AsSpan(0, count), write);
                }
            }

            try
            {
                ExecutionScope.ApplyingReplicated = true;

                if (write)
                {
                    if (allKeys)
                        Snapshots?.BeforeFlushAll();
                    else
                        BeforeWrite(args, keys);
                }

                var reply = DispatchAsync(info.Name, args, session).Result;

                backlog?.Append(raw);
                return reply;
            }
            finally
            {
                ExecutionScope.ApplyingReplicated = false;

                if (locks is not null)
                {
                    if (allKeys)
                        locks.ExitAll();
                    else if (held is not null)
                        locks.ExitMany(held.AsSpan(0, count), write);
                }
            }
        }

        private RespValue ApplyReplicatedTransaction(
            ClientSession session,
            ReadOnlySpan<byte> raw)
        {
            var backlog = raw.IsEmpty ? null : Replication?.Backlog;

            if (!session.InTransaction)
            {
                backlog?.Append(raw);
                return RespValue.Error("ERR EXEC without MULTI");
            }

            session.InTransaction = false;

            var bytes = session.ReplicatedTransactionBytes;
            session.ReplicatedTransactionBytes = null;
            bytes?.Write(raw);

            var queued = session.QueuedCommands.ToArray();
            session.QueuedCommands.Clear();

            var infos = new CommandInfo[queued.Length];
            var stripes = new List<int>();
            var allKeys = false;

            for (var i = 0; i < queued.Length; i++)
            {
                var commandArgs = queued[i].Items!;
                var info = LookupCommand(commandArgs[0]);

                if (info is null)
                    return RespValue.Error("ERR unknown command in replicated transaction");

                infos[i] = info;
                allKeys |= (info.Flags & CommandFlags.AllKeys) != 0;

                if (info.HasKeys)
                {
                    foreach (var index in info.GetKeyIndexes(commandArgs))
                        stripes.Add(KeyLocks.StripeOf(KeyBytes(commandArgs[index])));
                }
            }

            var held = stripes.ToArray();
            var count = KeyLocks.Normalize(held);
            var locks = _locks;

            if (locks is not null)
            {
                if (allKeys)
                    locks.EnterAll();
                else
                    locks.EnterMany(held.AsSpan(0, count), write: true);
            }

            try
            {
                ExecutionScope.ApplyingReplicated = true;

                var snapshots = Snapshots;

                if (snapshots is { InProgress: true })
                {
                    if (allKeys)
                        snapshots.BeforeFlushAll();

                    for (var i = 0; i < queued.Length; i++)
                    {
                        if (infos[i].IsWrite && infos[i].HasKeys)
                            BeforeWrite(queued[i].Items!, infos[i].GetKeyIndexes(queued[i].Items!));
                    }
                }

                var results = new RespValue[queued.Length];

                for (var i = 0; i < queued.Length; i++)
                    results[i] = DispatchAsync(infos[i].Name, queued[i].Items!, session).Result;

                if (bytes is not null)
                    backlog?.Append(bytes.WrittenSpan);

                return RespValue.Array(results);
            }
            finally
            {
                ExecutionScope.ApplyingReplicated = false;

                if (locks is not null)
                {
                    if (allKeys)
                        locks.ExitAll();
                    else
                        locks.ExitMany(held.AsSpan(0, count), write: true);
                }
            }
        }

        // Runs `action` holding one key's stripe lock.
        private void WithKeyLock(string key, bool write, Action action)
        {
            if (_locks is null)
            {
                action();
                return;
            }

            var stripe = KeyLocks.StripeOf(key);

            _locks.Enter(stripe, write);

            try
            {
                action();
            }
            finally
            {
                _locks.Exit(stripe, write);
            }
        }

        private static CommandInfo? LookupCommand(RespValue name)
            => name.BinaryValue is { } bytes
                ? CommandTable.Lookup(bytes)
                : CommandTable.Lookup(name.StringValue ?? "");

        private static bool ArityMatches(CommandInfo info, int count)
            => info.Arity >= 0
                ? count == info.Arity
                : count >= -info.Arity;

        private static ReadOnlySpan<byte> KeyBytes(RespValue value)
            => value.BinaryValue ?? Encoding.UTF8.GetBytes(value.StringValue ?? "");

        // MULTI/EXEC bookkeeping aside, this is the per-command switch.
        // Every handler completes synchronously.
        private ValueTask<RespValue> DispatchAsync(
            string command,
            IReadOnlyList<RespValue> args,
            ClientSession? session)
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

                    "DEL" or "UNLINK" => Del(args),

                    "EXISTS" => Exists(args),

                    "INCR" => Increment(args, 1),

                    "DECR" => Increment(args, -1),

                    "INCRBY" => IncrementBy(args, negate: false),

                    "DECRBY" => IncrementBy(args, negate: true),

                    "MGET" => MGet(args),

                    "MSET" => MSet(args),

                    "INFO" => Info(args),

                    "CLIENT" => Client(args, session),

                    "CONFIG" => Config(args),

                    "COMMAND" => Command(args),

                    "CLUSTER" => Cluster(args, session),

                    "SELECT" => Select(args),

                    "DBSIZE" => ValueTask.FromResult(
                        RespValue.Integer(_store.Count)),

                    "ECHO" => Echo(args),

                    "TIME" => Time(),

                    "SCAN" => Scan(args),

                    "KEYS" => KeysCommand(args),

                    "HSCAN" => HScan(args),

                    "SSCAN" => SScan(args),

                    "ZSCAN" => ZScan(args),

                    "TYPE" => Type(args),

                    "TTL" => Ttl(args),

                    "PTTL" => PTtl(args),

                    "EXPIRETIME" => ExpireTime(args, milliseconds: false),

                    "PEXPIRETIME" => ExpireTime(args, milliseconds: true),

                    "EXPIRE" => Expire(args),

                    "PEXPIRE" => PExpire(args),

                    "EXPIREAT" => ExpireAt(args, milliseconds: false),

                    "PEXPIREAT" => ExpireAt(args, milliseconds: true),

                    "PERSIST" => Persist(args),

                    "DUMP" => Dump(args),

                    "RESTORE" or "RESTORE-ASKING" => Restore(args),

                    "MIGRATE" => Migrate(args),

                    "FLUSHALL" or "FLUSHDB" => FlushAll(args),

                    "HSET" => HSet(args),

                    "HMSET" => HMSet(args),

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

                    "BGREWRITEAOF" => BgRewriteAof(),

                    "SHUTDOWN" => Shutdown(),

                    "REPLICAOF" or "SLAVEOF" => ReplicaOf(args),

                    "ROLE" => Role(),

                    "REPLCONF" => ReplConf(args, session),

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
            catch (CommandErrorException ex)
            {
                return Error(ex.Message);
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

            if (!_store.TryGet(
                KeyBytes(args[1]),
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

            if (args.Count == 3)
            {
                _store.Set(key, value);
                return ValueTask.FromResult(RespValue.Ok);
            }

            // Absolute Unix-ms expiry: EX/PX are relative to now,
            // EXAT/PXAT (what the AOF and replication stream carry)
            // are absolute.
            long? expiry = null;
            string? condition = null;
            var keepTtl = false;
            var getOld = false;

            for (var i = 3; i < args.Count; i++)
            {
                var option = GetString(args[i]).ToUpperInvariant();

                if (option is "EX" or "PX" or "EXAT" or "PXAT")
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
                            "ERR invalid expire time in 'set' command");
                    }

                    expiry = option switch
                    {
                        "EX" => CacheStore.NowMs + amount * 1000,
                        "PX" => CacheStore.NowMs + amount,
                        "EXAT" => amount * 1000,
                        _ => amount,
                    };
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
                var didSet = _store.SetIfAbsentWithExpireAt(key, value, expiry);

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

            if (keepTtl)
                _store.SetKeepTtl(key, value);
            else
                _store.SetWithExpireAt(key, value, expiry);

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
            // must not clear an existing TTL (or nudge it: the exact
            // absolute expiry is kept, so replicas agree).
            _store.SetKeepTtl(key, result);

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
            _store.SetKeepTtl(key, result);

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

            // Pages through one keyspace shard at a time (see
            // CacheStore.ScanKeys), so a SCAN call costs a fraction of
            // the keyspace instead of sorting all of it.
            var batch = _store.ScanKeys(cursor, count, out var nextCursor);
            var page = new List<RespValue>();

            foreach (var key in batch)
            {
                if (matchPattern != null &&
                    !GlobMatch(matchPattern, key))
                {
                    continue;
                }

                // Skip expired keys — without deleting them: SCAN
                // holds no key locks, and only lock holders may.
                if (!_store.IsLive(key, out var keyType))
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
            var values = new byte[snapshot.Length][];

            for (var i = 0; i < snapshot.Length; i++)
                values[i] = snapshot[i].Value;

            return ValueTask.FromResult(RespValue.BulkArray(values));
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

            // Range already copies the element references out under the
            // list's lock; reply with that array as-is.
            return ValueTask.FromResult(
                RespValue.BulkArray(list.Range(start, stop)));
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

            return ValueTask.FromResult(RespValue.BulkArray(set.Snapshot()));
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

            // Validate every score before touching the key — an
            // invalid one must not leave a half-applied (or freshly
            // created, empty) sorted set behind.
            var scores = new double[(args.Count - 2) / 2];

            for (var i = 2; i < args.Count; i += 2)
            {
                if (!TryParseScore(GetString(args[i]), out scores[(i - 2) / 2]))
                    return Error("ERR value is not a valid float");
            }

            var zset = _store.GetOrCreateSortedSet(key);

            var added = 0;

            for (var i = 2; i < args.Count; i += 2)
            {
                var member = GetString(args[i + 1]);

                if (zset.Add(member, scores[(i - 2) / 2], out var delta))
                    added++;

                _store.AdjustMemory(key, delta);
            }

            return ValueTask.FromResult(RespValue.Integer(added));
        }

        // Accepts Redis's spellings of infinity as well as .NET's (a
        // rewritten AOF formats scores with "R", which round-trips).
        private static bool TryParseScore(string text, out double score)
        {
            switch (text.ToLowerInvariant())
            {
                case "inf":
                case "+inf":
                case "infinity":
                case "+infinity":
                    score = double.PositiveInfinity;
                    return true;

                case "-inf":
                case "-infinity":
                    score = double.NegativeInfinity;
                    return true;
            }

            return double.TryParse(
                       text,
                       NumberStyles.Float,
                       CultureInfo.InvariantCulture,
                       out score) &&
                   !double.IsNaN(score);
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
