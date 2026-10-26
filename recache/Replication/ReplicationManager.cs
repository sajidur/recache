using Microsoft.Extensions.Logging;
using recache.Commands;
using recache.Configuration;
using recache.Persistence;
using recache.Protocol;
using recache.Security;
using recache.Storage;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace recache.Replication
{
    public enum ReplicationRole
    {
        Master,
        Replica
    }

    // A replica connected to this node.
    public sealed class ConnectedReplica
    {
        public required long Id { get; init; }

        public required string Address { get; init; }

        public int ListeningPort { get; init; }

        public string State { get; set; } = "wait_bgsave";

        private long _ackOffset = -1;

        public long AckOffset
        {
            get => Interlocked.Read(ref _ackOffset);
            set => Interlocked.Exchange(ref _ackOffset, value);
        }

        public long LastAckTimestamp;

        public CancellationTokenSource Cancellation { get; } = new();

        public bool Online => State == "online";
    }

    // Master/replica role, the PSYNC2 replication identity, serving
    // replicas (full and partial resync), and WAIT.
    //
    // Replication is asynchronous like Redis's: a write is
    // acknowledged once applied locally and appended to the stream;
    // replicas follow a few microseconds to milliseconds behind. A
    // client that needs a write on N replicas before proceeding uses
    // WAIT N timeout — and a coordinated failover (CLUSTER FAILOVER,
    // or failover on shutdown) never loses a write, because the old
    // master pauses writes until its replica has caught up exactly.
    public sealed class ReplicationManager : IDisposable
    {
        private const string NullReplId = "0000000000000000000000000000000000000000";

        private readonly RecacheOptions _options;
        private readonly CacheStore _store;
        private readonly KeyLocks _locks;
        private readonly ReplicationBacklog _backlog;
        private readonly SnapshotService _snapshots;
        private readonly ILogger _logger;
        private readonly object _roleLock = new();
        private readonly object _idLock = new();
        private readonly ConcurrentDictionary<long, ConnectedReplica> _replicas = new();
        private readonly CancellationTokenSource _shutdown = new();

        private TaskCompletionSource _ackSignal =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private volatile bool _propagationEnabled;
        private volatile ReplicationRole _role = ReplicationRole.Master;
        private string _replId = NewReplId();
        private string _replId2 = NullReplId;
        private long _secondReplOffset = -1;
        private string? _masterHost;
        private int _masterPort;
        private ReplicaLink? _link;

        public ReplicationManager(
            RecacheOptions options,
            CacheStore store,
            KeyLocks locks,
            ReplicationBacklog backlog,
            SnapshotService snapshots,
            string dataDirectory,
            ILogger logger)
        {
            _options = options;
            _store = store;
            _locks = locks;
            _backlog = backlog;
            _snapshots = snapshots;
            _logger = logger;
            DataDirectory = dataDirectory;

            _store.KeyExpired = OnKeyExpired;

            _ = PingReplicasAsync(_shutdown.Token);
        }

        // Wired by RecacheNode once the dispatcher exists.
        public CommandDispatcher? Dispatcher { get; set; }

        // Builds a dispatcher over a scratch store, for loading a full
        // resync payload off to the side.
        public Func<CacheStore, CommandDispatcher>? LoaderFactory { get; set; }

        public string DataDirectory { get; }

        // Raised after a replica's dataset was replaced by a full
        // resync.
        public event Action? FullSyncCompleted;

        // Raised whenever the link to this node's master goes up or
        // down (cluster mode uses it for failover bookkeeping).
        public event Action<bool>? MasterLinkChanged;

        public ReplicationBacklog Backlog => _backlog;

        public bool PropagationEnabled => _propagationEnabled;

        // True while the AOF is being replayed at startup — replayed
        // commands are already in the file and must not be appended
        // to the stream again.
        public bool Loading { get; set; }

        public ReplicationRole Role => _role;

        public bool IsReplica => _role == ReplicationRole.Replica;

        public string ReplId
        {
            get { lock (_idLock) return _replId; }
        }

        public long Offset => _backlog.EndOffset;

        public string? MasterHost => _masterHost;

        public int MasterPort => _masterPort;

        public bool MasterLinkUp => _link?.State == ReplicaLinkState.Connected;

        public int ConnectedReplicaCount => _replicas.Count(r => r.Value.Online);

        private long _fullSyncs;
        private long _partialSyncs;

        // Resyncs this node has served as a master (INFO stats).
        public long FullSyncs => Interlocked.Read(ref _fullSyncs);

        public long PartialSyncs => Interlocked.Read(ref _partialSyncs);

        // CLIENT KILL TYPE master: drop the link to our master; it
        // reconnects by itself (normally with a partial resync).
        public bool DropMasterLink()
        {
            var link = _link;

            if (link is null)
                return false;

            link.DropConnection();
            return true;
        }

        // CLIENT KILL TYPE replica: disconnect every replica; they
        // reconnect and resync by themselves.
        public int DisconnectAllReplicas()
        {
            var count = _replicas.Count;
            DisconnectReplicas();
            return count;
        }

        public IReadOnlyCollection<ConnectedReplica> Replicas => _replicas.Values.ToArray();

        // Turns on the stream: everything applied from now on is
        // appended to the backlog (needed by the AOF, by replicas, and
        // in cluster mode by whoever might become a replica later).
        // Standalone nodes with neither pay nothing until then.
        public void EnablePropagation(bool retainForPartialResync)
        {
            _propagationEnabled = true;

            if (retainForPartialResync)
                _backlog.RetainForPartialResync = true;
        }

        private void OnKeyExpired(string key)
        {
            if (_propagationEnabled && !Loading)
                _backlog.AppendCommand("DEL", key);
        }

        // ---- Role changes ----

        public void BecomeReplicaOf(string host, int port)
        {
            lock (_roleLock)
            {
                if (_role == ReplicationRole.Replica &&
                    _link is not null &&
                    string.Equals(_masterHost, host, StringComparison.OrdinalIgnoreCase) &&
                    _masterPort == port)
                {
                    return;
                }

                StopLinkLocked();

                // Chained replicas were following our old history;
                // they reconnect and resync against the new one.
                DisconnectReplicas();

                _role = ReplicationRole.Replica;
                _masterHost = host;
                _masterPort = port;
                _store.IsReplica = true;

                EnablePropagation(retainForPartialResync: true);

                _link = new ReplicaLink(this, host, port, _options, _logger);
                _link.Start();
            }

            _logger.LogInformation(
                "Now a replica of {Host}:{Port}",
                host,
                port);
        }

        public void PromoteToMaster()
        {
            lock (_roleLock)
            {
                if (_role == ReplicationRole.Master)
                    return;

                StopLinkLocked();

                _role = ReplicationRole.Master;
                _masterHost = null;
                _masterPort = 0;
                _store.IsReplica = false;

                // PSYNC2: start a new history but remember where the
                // old one ended, so the old master's other replicas
                // (and the old master itself, if it rejoins as a
                // replica having lost nothing) can continue with a
                // partial resync instead of a full transfer.
                lock (_idLock)
                {
                    _replId2 = _replId;
                    _secondReplOffset = _backlog.EndOffset;
                    _replId = NewReplId();
                }
            }

            _logger.LogInformation(
                "Promoted to master (replication offset {Offset})",
                _backlog.EndOffset);
        }

        private void StopLinkLocked()
        {
            var link = _link;
            _link = null;

            // Waits for an in-flight command from the old master to
            // finish applying before the caller changes the role.
            link?.Stop(TimeSpan.FromSeconds(5));
        }

        private void DisconnectReplicas()
        {
            foreach (var replica in _replicas.Values)
                replica.Cancellation.Cancel();
        }

        // ---- Replica side (called by ReplicaLink) ----

        internal (string ReplId, long Offset) GetPsyncRequest()
        {
            lock (_idLock)
                return (_replId, _backlog.EndOffset + 1);
        }

        internal void OnContinue(string? newReplId)
        {
            lock (_idLock)
            {
                if (newReplId is null || newReplId == _replId)
                    return;

                _replId2 = _replId;
                _secondReplOffset = _backlog.EndOffset;
                _replId = newReplId;
            }
        }

        internal void CompleteFullSync(CacheStore loaded, string replId, long offset)
        {
            _locks.EnterAll();

            try
            {
                _store.ReplaceContents(loaded);

                lock (_idLock)
                {
                    _replId = replId;
                    _replId2 = NullReplId;
                    _secondReplOffset = -1;
                }

                _backlog.Reset(offset);
            }
            finally
            {
                _locks.ExitAll();
            }

            FullSyncCompleted?.Invoke();
        }

        internal void NotifyMasterLink(bool up)
            => MasterLinkChanged?.Invoke(up);

        internal string TempSyncPath =>
            Path.Combine(DataDirectory, "replica-sync.tmp");

        internal bool StoreTracksSlots => _store.TracksSlots;

        internal ReplicaLink? Link => _link;

        // ---- Master side ----

        // Takes over a connection whose client just sent PSYNC/SYNC:
        // brings it up to date (partial or full resync), then streams
        // every subsequent write until it disconnects.
        public async Task ServeReplicaAsync(
            Socket socket,
            ClientSession session,
            IReadOnlyList<RespValue> args,
            ReadOnlyMemory<byte> leftover,
            CancellationToken cancellationToken)
        {
            if (IsReplica && !MasterLinkUp)
            {
                // A replica without a current dataset can't feed
                // anyone (Redis: "-NOMASTERLINK").
                await socket.SendAsync(
                    Encoding.ASCII.GetBytes(
                        "-NOMASTERLINK Can't SYNC while not connected with my master\r\n"),
                    SocketFlags.None,
                    cancellationToken);

                return;
            }

            var replica = new ConnectedReplica
            {
                Id = session.Id,
                Address = session.ReplicaAnnouncedIp ?? HostOf(session.RemoteAddress),
                ListeningPort = session.ReplicaListeningPort,
            };

            using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                replica.Cancellation.Token,
                _shutdown.Token);

            var token = linked.Token;
            var stream = new NetworkStream(socket, ownsSocket: false);
            ReplicationBacklog.Cursor? cursor = null;

            _replicas[replica.Id] = replica;

            try
            {
                EnablePropagation(retainForPartialResync: true);

                cursor = await BeginSyncAsync(stream, replica, args, token);

                replica.State = "online";
                replica.AckOffset = cursor.Position;

                _logger.LogInformation(
                    "Replica {Address}:{Port} is online at offset {Offset}",
                    replica.Address,
                    replica.ListeningPort,
                    cursor.Position);

                var acks = ReadAcksAsync(socket, leftover, replica, linked);

                await SendStreamAsync(stream, cursor, token);
                await acks;
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex) when (ex is IOException or SocketException or InvalidOperationException)
            {
                _logger.LogInformation(
                    "Replica {Address}:{Port} disconnected: {Reason}",
                    replica.Address,
                    replica.ListeningPort,
                    ex.Message);
            }
            finally
            {
                linked.Cancel();
                _replicas.TryRemove(replica.Id, out _);

                if (cursor is not null)
                    _backlog.Unregister(cursor);

                SignalAcks();
            }
        }

        private async Task<ReplicationBacklog.Cursor> BeginSyncAsync(
            NetworkStream stream,
            ConnectedReplica replica,
            IReadOnlyList<RespValue> args,
            CancellationToken cancellationToken)
        {
            var requestedId = args.Count >= 3 ? args[1].AsString() ?? "?" : "?";
            var requestedOffset = -1L;

            if (args.Count >= 3)
            {
                long.TryParse(
                    args[2].AsString(),
                    NumberStyles.Integer,
                    CultureInfo.InvariantCulture,
                    out requestedOffset);
            }

            if (TryBeginPartialSync(requestedId, requestedOffset - 1, replica, out var cursor, out var replId))
            {
                await WriteAsciiAsync(stream, $"+CONTINUE {replId}\r\n", cancellationToken);
                Interlocked.Increment(ref _partialSyncs);
                return cursor!;
            }

            Interlocked.Increment(ref _fullSyncs);

            _logger.LogInformation(
                "Full resync for replica {Address}:{Port} " +
                "(requested {ReplId} at {Offset})",
                replica.Address,
                replica.ListeningPort,
                requestedId,
                requestedOffset);

            Directory.CreateDirectory(DataDirectory);

            var path = Path.Combine(
                DataDirectory,
                $"replica-{replica.Id}.snapshot.tmp");

            // Keep the replica's handshake read from timing out while
            // the snapshot is built (Redis sends the same bare
            // newlines).
            using var keepalive = new CancellationTokenSource();
            var keepaliveTask = KeepAliveAsync(stream, keepalive.Token);

            SnapshotResult snapshot;

            try
            {
                snapshot = await _snapshots.CreateAsync(
                    path,
                    $"replica-{replica.Id}-sync",
                    cancellationToken);
            }
            finally
            {
                keepalive.Cancel();
                await keepaliveTask;
            }

            try
            {
                await WriteAsciiAsync(
                    stream,
                    $"+FULLRESYNC {ReplId} {snapshot.Offset}\r\n",
                    cancellationToken);

                replica.State = "send_bulk";

                await WriteAsciiAsync(
                    stream,
                    $"${snapshot.Bytes}\r\n",
                    cancellationToken);

                await using (var file = new FileStream(
                    path,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    bufferSize: 1024 * 1024,
                    useAsync: true))
                {
                    await file.CopyToAsync(stream, 1024 * 1024, cancellationToken);
                }

                return _backlog.Register(
                    $"replica-{replica.Id}",
                    snapshot.Offset,
                    evictable: true);
            }
            finally
            {
                _backlog.Unregister(snapshot.Cursor);

                try
                {
                    File.Delete(path);
                }
                catch (IOException)
                {
                }
            }
        }

        private bool TryBeginPartialSync(
            string requestedId,
            long wantedOffset,
            ConnectedReplica replica,
            out ReplicationBacklog.Cursor? cursor,
            out string replId)
        {
            cursor = null;

            lock (_idLock)
            {
                replId = _replId;

                var historyMatches =
                    requestedId == _replId ||
                    (requestedId == _replId2 && wantedOffset <= _secondReplOffset);

                if (!historyMatches || wantedOffset < 0)
                    return false;
            }

            if (!_backlog.Contains(wantedOffset))
                return false;

            try
            {
                cursor = _backlog.Register(
                    $"replica-{replica.Id}",
                    wantedOffset,
                    evictable: true);

                _logger.LogInformation(
                    "Partial resync for replica {Address}:{Port} " +
                    "from offset {Offset} ({Bytes} bytes behind)",
                    replica.Address,
                    replica.ListeningPort,
                    wantedOffset,
                    _backlog.EndOffset - wantedOffset);

                return true;
            }
            catch (InvalidOperationException)
            {
                // Trimmed between the check and the registration.
                return false;
            }
        }

        private async Task SendStreamAsync(
            NetworkStream stream,
            ReplicationBacklog.Cursor cursor,
            CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                if (cursor.Evicted)
                {
                    throw new IOException(
                        "replica fell further behind than " +
                        "ReplicaOutputLimitBytes (or the stream was reset); " +
                        "it will reconnect and resync");
                }

                var data = _backlog.Read(cursor.Position, 256 * 1024);

                if (data.IsEmpty)
                {
                    await _backlog.WaitForDataAsync(cursor.Position, cancellationToken);
                    continue;
                }

                await stream.WriteAsync(data, cancellationToken);
                cursor.Position += data.Length;
            }
        }

        // Reads REPLCONF ACK <offset> from a replica.
        private async Task ReadAcksAsync(
            Socket socket,
            ReadOnlyMemory<byte> leftover,
            ConnectedReplica replica,
            CancellationTokenSource link)
        {
            var buffer = new byte[Math.Max(4096, leftover.Length * 2)];
            leftover.CopyTo(buffer);
            var end = leftover.Length;

            try
            {
                while (!link.IsCancellationRequested)
                {
                    var start = 0;

                    while (start < end)
                    {
                        var status = RespParser.TryParseRequest(
                            buffer.AsSpan(start, end - start),
                            1024 * 1024,
                            out var request,
                            out var consumed,
                            out _,
                            out _);

                        if (status != RespParseStatus.Complete)
                        {
                            if (status == RespParseStatus.Invalid)
                                throw new IOException("Protocol error on replica link");

                            break;
                        }

                        start += consumed;

                        if (request?.Items is { Count: >= 3 } items &&
                            string.Equals(items[0].AsString(), "REPLCONF", StringComparison.OrdinalIgnoreCase) &&
                            string.Equals(items[1].AsString(), "ACK", StringComparison.OrdinalIgnoreCase) &&
                            long.TryParse(items[2].AsString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var offset))
                        {
                            replica.AckOffset = offset;
                            replica.LastAckTimestamp = Environment.TickCount64;
                            SignalAcks();
                        }
                    }

                    if (start > 0)
                    {
                        Buffer.BlockCopy(buffer, start, buffer, 0, end - start);
                        end -= start;
                    }

                    if (end == buffer.Length)
                        Array.Resize(ref buffer, buffer.Length * 2);

                    var read = await socket.ReceiveAsync(
                        buffer.AsMemory(end),
                        SocketFlags.None,
                        link.Token);

                    if (read == 0)
                        break;

                    end += read;
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or SocketException or IOException or ObjectDisposedException)
            {
            }
            finally
            {
                // The replica hung up (or misbehaved): stop streaming.
                link.Cancel();
            }
        }

        private void SignalAcks()
        {
            var previous = Interlocked.Exchange(
                ref _ackSignal,
                new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));

            previous.TrySetResult();
        }

        // WAIT numreplicas timeout: blocks this client (not the
        // server) until that many replicas have acknowledged its last
        // write, or the timeout passes. Returns how many did.
        public async ValueTask<RespValue> WaitForAcksAsync(
            int numReplicas,
            long timeoutMs,
            long targetOffset,
            CancellationToken cancellationToken)
        {
            int Count() => _replicas.Values.Count(
                r => r.Online && r.AckOffset >= targetOffset);

            var count = Count();

            if (count >= numReplicas || _replicas.IsEmpty)
                return RespValue.Integer(count);

            // Ask every replica to report its offset right away rather
            // than at its next once-a-second ACK.
            _backlog.AppendCommand("REPLCONF", "GETACK", "*");

            var deadline = timeoutMs > 0
                ? Environment.TickCount64 + timeoutMs
                : long.MaxValue;

            while (true)
            {
                var signal = Volatile.Read(ref _ackSignal).Task;

                count = Count();

                if (count >= numReplicas)
                    return RespValue.Integer(count);

                var remaining = deadline - Environment.TickCount64;

                if (remaining <= 0 || cancellationToken.IsCancellationRequested)
                    return RespValue.Integer(count);

                await Task.WhenAny(
                    signal,
                    Task.Delay(TimeSpan.FromMilliseconds(Math.Min(remaining, 1000)), cancellationToken))
                    .ConfigureAwait(false);
            }
        }

        // The highest offset any online replica has acknowledged —
        // a coordinated failover waits for this to reach the master's
        // own offset.
        public long BestReplicaAckOffset()
            => _replicas.Values
                .Where(r => r.Online)
                .Select(r => r.AckOffset)
                .DefaultIfEmpty(-1)
                .Max();

        public void RequestAcks()
        {
            if (!_replicas.IsEmpty)
                _backlog.AppendCommand("REPLCONF", "GETACK", "*");
        }

        public Task WaitForAckSignalAsync(TimeSpan timeout)
            => Task.WhenAny(Volatile.Read(ref _ackSignal).Task, Task.Delay(timeout));

        // ---- Introspection ----

        public void AppendInfo(StringBuilder sb)
        {
            string replId, replId2;
            long secondOffset;

            lock (_idLock)
            {
                replId = _replId;
                replId2 = _replId2;
                secondOffset = _secondReplOffset;
            }

            if (_role == ReplicationRole.Master)
            {
                sb.Append("role:master\r\n");

                var replicas = _replicas.Values.ToArray();
                sb.Append($"connected_slaves:{replicas.Length}\r\n");

                for (var i = 0; i < replicas.Length; i++)
                {
                    var r = replicas[i];
                    var lag = r.LastAckTimestamp == 0
                        ? 0
                        : (Environment.TickCount64 - r.LastAckTimestamp) / 1000;

                    sb.Append(
                        $"slave{i}:ip={r.Address},port={r.ListeningPort}," +
                        $"state={r.State},offset={Math.Max(r.AckOffset, 0)},lag={lag}\r\n");
                }
            }
            else
            {
                var link = _link;

                sb.Append("role:slave\r\n");
                sb.Append($"master_host:{_masterHost}\r\n");
                sb.Append($"master_port:{_masterPort}\r\n");
                sb.Append($"master_link_status:{(MasterLinkUp ? "up" : "down")}\r\n");

                sb.Append(
                    "master_last_io_seconds_ago:" +
                    (link is null || link.LastIoTimestamp == 0
                        ? -1
                        : (Environment.TickCount64 - link.LastIoTimestamp) / 1000) +
                    "\r\n");

                sb.Append($"master_sync_in_progress:{(link?.SyncInProgress == true ? 1 : 0)}\r\n");
                sb.Append($"slave_read_repl_offset:{_backlog.EndOffset}\r\n");
                sb.Append($"slave_repl_offset:{_backlog.EndOffset}\r\n");
                sb.Append("slave_priority:100\r\n");
                sb.Append("slave_read_only:1\r\n");
                sb.Append("replica_announced:1\r\n");
                sb.Append($"connected_slaves:{_replicas.Count}\r\n");
            }

            sb.Append($"master_replid:{replId}\r\n");
            sb.Append($"master_replid2:{replId2}\r\n");
            sb.Append($"master_repl_offset:{_backlog.EndOffset}\r\n");
            sb.Append($"second_repl_offset:{(secondOffset < 0 ? -1 : secondOffset + 1)}\r\n");
            sb.Append($"repl_backlog_active:{(_propagationEnabled ? 1 : 0)}\r\n");
            sb.Append($"repl_backlog_size:{_options.ReplicationBacklogBytes}\r\n");
            sb.Append($"repl_backlog_first_byte_offset:{_backlog.StartOffset + 1}\r\n");
            sb.Append($"repl_backlog_histlen:{_backlog.RetainedBytes}\r\n");
        }

        public RespValue RoleReply()
        {
            if (_role == ReplicationRole.Master)
            {
                var replicas = _replicas.Values
                    .Where(r => r.Online)
                    .Select(r => (RespValue)RespValue.Array(new[]
                    {
                        RespValue.Bulk(r.Address),
                        RespValue.Bulk(r.ListeningPort.ToString(CultureInfo.InvariantCulture)),
                        RespValue.Bulk(Math.Max(r.AckOffset, 0).ToString(CultureInfo.InvariantCulture)),
                    }))
                    .ToArray();

                return RespValue.Array(new[]
                {
                    RespValue.Bulk("master"),
                    RespValue.Integer(_backlog.EndOffset),
                    RespValue.Array(replicas),
                });
            }

            var state = _link?.State switch
            {
                ReplicaLinkState.Connected => "connected",
                ReplicaLinkState.Syncing => "sync",
                ReplicaLinkState.Handshake => "handshake",
                _ => "connect",
            };

            return RespValue.Array(new[]
            {
                RespValue.Bulk("slave"),
                RespValue.Bulk(_masterHost ?? ""),
                RespValue.Integer(_masterPort),
                RespValue.Bulk(state),
                RespValue.Integer(_backlog.EndOffset),
            });
        }

        // ---- Housekeeping ----

        // A PING in the stream every few seconds lets replicas tell an
        // idle master from a dead one.
        private async Task PingReplicasAsync(CancellationToken cancellationToken)
        {
            var period = TimeSpan.FromSeconds(Math.Max(1, _options.ReplicaPingPeriodSeconds));

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    await Task.Delay(period, cancellationToken);

                    if (_role == ReplicationRole.Master && !_replicas.IsEmpty)
                        _backlog.AppendCommand("PING");
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private static async Task KeepAliveAsync(
            NetworkStream stream,
            CancellationToken cancellationToken)
        {
            var newline = new byte[] { (byte)'\n' };

            try
            {
                while (true)
                {
                    await Task.Delay(1000, cancellationToken);
                    await stream.WriteAsync(newline, cancellationToken);
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or IOException or ObjectDisposedException)
            {
            }
        }

        private static ValueTask WriteAsciiAsync(
            NetworkStream stream,
            string text,
            CancellationToken cancellationToken)
            => stream.WriteAsync(Encoding.ASCII.GetBytes(text), cancellationToken);

        private static string HostOf(string endpoint)
        {
            if (string.IsNullOrEmpty(endpoint))
                return "?";

            var colon = endpoint.LastIndexOf(':');
            var host = colon > 0 ? endpoint[..colon] : endpoint;

            host = host.Trim('[', ']');

            return host.StartsWith("::ffff:", StringComparison.Ordinal)
                ? host[7..]
                : host;
        }

        private static string NewReplId()
            => Convert.ToHexString(RandomNumberGenerator.GetBytes(20)).ToLowerInvariant();

        public void Dispose()
        {
            _shutdown.Cancel();

            lock (_roleLock)
                StopLinkLocked();

            DisconnectReplicas();
        }
    }
}
