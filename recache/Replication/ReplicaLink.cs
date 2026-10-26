using Microsoft.Extensions.Logging;
using recache.Configuration;
using recache.Persistence;
using recache.Protocol;
using recache.Security;
using recache.Storage;
using System;
using System.Globalization;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace recache.Replication
{
    public enum ReplicaLinkState
    {
        Connecting,
        Handshake,
        Syncing,
        Connected
    }

    // The replica side of replication: connects to the master, asks
    // to continue from its own (replid, offset) with PSYNC, and then
    // applies the master's stream. Reconnects with backoff forever —
    // a master restart or network blip heals by itself, usually with
    // a partial resync.
    internal sealed class ReplicaLink
    {
        private readonly ReplicationManager _manager;
        private readonly string _host;
        private readonly int _port;
        private readonly RecacheOptions _options;
        private readonly ILogger _logger;
        private readonly CancellationTokenSource _cts = new();
        private readonly object _sendLock = new();

        private Task? _task;
        private volatile ReplicaLinkState _state = ReplicaLinkState.Connecting;

        public ReplicaLink(
            ReplicationManager manager,
            string host,
            int port,
            RecacheOptions options,
            ILogger logger)
        {
            _manager = manager;
            _host = host;
            _port = port;
            _options = options;
            _logger = logger;
        }

        public ReplicaLinkState State => _state;

        public long LastIoTimestamp;

        public volatile bool SyncInProgress;

        // Per-connection-attempt cancellation, so the current
        // connection can be dropped without stopping the link.
        private CancellationTokenSource? _attempt;

        public void Start()
            => _task = Task.Run(() => RunAsync(_cts.Token));

        public void DropConnection()
            => _attempt?.Cancel();

        public void Stop(TimeSpan wait)
        {
            _cts.Cancel();

            try
            {
                _task?.Wait(wait);
            }
            catch (AggregateException)
            {
            }
        }

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            var delay = TimeSpan.FromMilliseconds(100);

            while (!cancellationToken.IsCancellationRequested)
            {
                using var attempt = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                _attempt = attempt;

                try
                {
                    await SyncAndStreamAsync(attempt.Token);
                    delay = TimeSpan.FromMilliseconds(100);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(
                        "Replication link to master {Host}:{Port} lost: {Reason}",
                        _host,
                        _port,
                        ex.Message);
                }
                finally
                {
                    if (_state == ReplicaLinkState.Connected)
                        _manager.NotifyMasterLink(false);

                    _state = ReplicaLinkState.Connecting;
                    SyncInProgress = false;
                }

                try
                {
                    await Task.Delay(delay, cancellationToken);
                }
                catch (OperationCanceledException)
                {
                    break;
                }

                delay = TimeSpan.FromMilliseconds(
                    Math.Min(delay.TotalMilliseconds * 2, 2000));
            }
        }

        private async Task SyncAndStreamAsync(CancellationToken cancellationToken)
        {
            using var client = new TcpClient { NoDelay = true };

            using (var connectTimeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken))
            {
                connectTimeout.CancelAfter(TimeSpan.FromSeconds(5));
                await client.ConnectAsync(_host, _port, connectTimeout.Token);
            }

            var socket = client.Client;
            var stream = client.GetStream();
            var reader = new RespReader(stream, int.MaxValue);
            var writer = new RespWriter(stream);

            _state = ReplicaLinkState.Handshake;
            Touch();

            var auth = _options.MasterAuth ?? _options.Password;

            if (!string.IsNullOrEmpty(auth))
                await ExpectOkAsync(writer, reader, cancellationToken, "AUTH", auth);

            var pong = await RequestAsync(writer, reader, cancellationToken, "PING");

            if (pong.IsError)
                throw new IOException($"master rejected PING: {pong.StringValue}");

            var announcePort = _options.ClusterAnnouncePort > 0
                ? _options.ClusterAnnouncePort
                : _options.Port;

            await RequestAsync(
                writer,
                reader,
                cancellationToken,
                "REPLCONF",
                "listening-port",
                announcePort.ToString(CultureInfo.InvariantCulture));

            if (!string.IsNullOrWhiteSpace(_options.ClusterAnnounceIp))
            {
                await RequestAsync(
                    writer,
                    reader,
                    cancellationToken,
                    "REPLCONF",
                    "ip-address",
                    _options.ClusterAnnounceIp!);
            }

            await RequestAsync(writer, reader, cancellationToken, "REPLCONF", "capa", "psync2");

            var (replId, offset) = _manager.GetPsyncRequest();

            await writer.WriteAsync(
                RespValue.Command(
                    "PSYNC",
                    replId,
                    offset.ToString(CultureInfo.InvariantCulture)),
                cancellationToken);

            var reply = await ReadNonEmptyLineAsync(reader, cancellationToken);

            if (reply.StartsWith("+FULLRESYNC", StringComparison.Ordinal))
            {
                var parts = reply.Split(' ');

                if (parts.Length < 3 ||
                    !long.TryParse(parts[2], NumberStyles.Integer, CultureInfo.InvariantCulture, out var masterOffset))
                {
                    throw new IOException($"malformed FULLRESYNC reply: {reply}");
                }

                await FullSyncAsync(reader, parts[1], masterOffset, cancellationToken);
            }
            else if (reply.StartsWith("+CONTINUE", StringComparison.Ordinal))
            {
                var parts = reply.Split(' ');

                _manager.OnContinue(parts.Length >= 2 ? parts[1] : null);

                _logger.LogInformation(
                    "Partial resync with master {Host}:{Port} accepted at offset {Offset}",
                    _host,
                    _port,
                    _manager.Offset);
            }
            else
            {
                throw new IOException($"master refused PSYNC: {reply}");
            }

            _state = ReplicaLinkState.Connected;
            _manager.NotifyMasterLink(true);

            await StreamAsync(socket, reader.Buffered, cancellationToken);
        }

        private async Task FullSyncAsync(
            RespReader reader,
            string replId,
            long masterOffset,
            CancellationToken cancellationToken)
        {
            _state = ReplicaLinkState.Syncing;
            SyncInProgress = true;

            var header = await ReadNonEmptyLineAsync(reader, cancellationToken);

            if (!header.StartsWith('$') ||
                !long.TryParse(header.AsSpan(1), NumberStyles.Integer, CultureInfo.InvariantCulture, out var length) ||
                length < 0)
            {
                throw new IOException($"malformed snapshot header: {header}");
            }

            var path = _manager.TempSyncPath;
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);

            await using (var file = new FileStream(
                path,
                FileMode.Create,
                FileAccess.Write,
                FileShare.None,
                bufferSize: 1024 * 1024,
                useAsync: true))
            {
                await reader.ReadExactlyAsync(file, length, cancellationToken);
            }

            Touch();

            // Load into a separate store; this node keeps serving its
            // previous data until the new dataset is complete, then
            // swaps it in all at once.
            var loaded = new CacheStore(_manager.StoreTracksSlots);
            var loader = _manager.LoaderFactory!(loaded);
            var session = new ClientSession { IsMasterLink = true, IsAuthenticated = true };

            var result = await Task.Run(
                () => AofLoader.Load(
                    path,
                    (args, _) => loader.ExecuteReplicated(args, session, ReadOnlySpan<byte>.Empty),
                    allowTruncatedTail: false,
                    truncateFile: false),
                cancellationToken);

            cancellationToken.ThrowIfCancellationRequested();

            _manager.CompleteFullSync(loaded, replId, masterOffset);

            try
            {
                File.Delete(path);
            }
            catch (IOException)
            {
            }

            SyncInProgress = false;

            _logger.LogInformation(
                "Full resync from master {Host}:{Port} complete: " +
                "{Keys} keys ({Bytes} bytes) at offset {Offset}",
                _host,
                _port,
                loaded.Count,
                result.Bytes,
                masterOffset);
        }

        private async Task StreamAsync(
            Socket socket,
            ReadOnlyMemory<byte> leftover,
            CancellationToken cancellationToken)
        {
            var dispatcher = _manager.Dispatcher!;
            var session = new ClientSession { IsMasterLink = true, IsAuthenticated = true };
            var buffer = new byte[Math.Max(64 * 1024, leftover.Length * 2)];
            var end = leftover.Length;
            var needed = 0;

            leftover.CopyTo(buffer);

            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            var acks = SendPeriodicAcksAsync(socket, linked.Token);
            var timeout = TimeSpan.FromSeconds(Math.Max(5, _options.ReplicationTimeoutSeconds));

            try
            {
                while (true)
                {
                    var start = 0;

                    if (end >= needed)
                    {
                        while (start < end)
                        {
                            var status = RespParser.TryParseRequest(
                                buffer.AsSpan(start, end - start),
                                int.MaxValue,
                                out var request,
                                out var consumed,
                                out needed,
                                out var error);

                            // `needed` is relative to `start`, which
                            // becomes offset 0 once the buffer is
                            // compacted below.
                            if (status == RespParseStatus.Incomplete)
                                break;

                            if (status == RespParseStatus.Invalid)
                                throw new IOException($"protocol error in replication stream: {error}");

                            var raw = buffer.AsSpan(start, consumed);
                            start += consumed;
                            needed = 0;

                            if (request is null)
                                continue;

                            Apply(dispatcher, session, request, raw);
                        }
                    }

                    if (start > 0)
                    {
                        Buffer.BlockCopy(buffer, start, buffer, 0, end - start);
                        end -= start;
                    }

                    if (end == buffer.Length || needed > buffer.Length)
                        Array.Resize(ref buffer, Math.Max(buffer.Length * 2, needed));

                    using var readTimeout = CancellationTokenSource.CreateLinkedTokenSource(linked.Token);
                    readTimeout.CancelAfter(timeout);

                    int read;

                    try
                    {
                        read = await socket.ReceiveAsync(
                            buffer.AsMemory(end),
                            SocketFlags.None,
                            readTimeout.Token);
                    }
                    catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                    {
                        throw new IOException(
                            $"no data from master for {timeout.TotalSeconds:0}s");
                    }

                    if (read == 0)
                        throw new IOException("master closed the connection");

                    end += read;
                    Touch();
                }
            }
            finally
            {
                linked.Cancel();

                try
                {
                    await acks;
                }
                catch (OperationCanceledException)
                {
                }
            }

            void Apply(
                Commands.CommandDispatcher target,
                ClientSession masterSession,
                RespValue request,
                ReadOnlySpan<byte> raw)
            {
                var items = request.Items!;

                if (items.Count >= 2 &&
                    Ascii.EqualsIgnoreCase(items[0].BinaryValue, "REPLCONF"u8) &&
                    Ascii.EqualsIgnoreCase(items[1].BinaryValue, "GETACK"u8))
                {
                    // Counted in the offset like any other stream
                    // bytes, then answered with the new offset.
                    target.ExecuteReplicated(items, masterSession, raw);
                    SendAck(socket);
                    return;
                }

                target.ExecuteReplicated(items, masterSession, raw);
            }
        }

        private async Task SendPeriodicAcksAsync(Socket socket, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                await Task.Delay(1000, cancellationToken);
                SendAck(socket);
            }
        }

        private void SendAck(Socket socket)
        {
            var bytes = RespSerializer.ToBytes(
                RespValue.Command(
                    "REPLCONF",
                    "ACK",
                    _manager.Offset.ToString(CultureInfo.InvariantCulture)));

            try
            {
                lock (_sendLock)
                    socket.Send(bytes);
            }
            catch (Exception ex) when (ex is SocketException or ObjectDisposedException)
            {
            }
        }

        private void Touch()
            => Interlocked.Exchange(ref LastIoTimestamp, Environment.TickCount64);

        private static async Task<RespValue> RequestAsync(
            RespWriter writer,
            RespReader reader,
            CancellationToken cancellationToken,
            params string[] command)
        {
            await writer.WriteAsync(RespValue.Command(command), cancellationToken);

            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(TimeSpan.FromSeconds(10));

            return await reader.ReadAsync(timeout.Token)
                ?? throw new IOException("master closed the connection during the handshake");
        }

        private static async Task ExpectOkAsync(
            RespWriter writer,
            RespReader reader,
            CancellationToken cancellationToken,
            params string[] command)
        {
            var reply = await RequestAsync(writer, reader, cancellationToken, command);

            if (reply.IsError)
                throw new IOException($"master rejected {command[0]}: {reply.StringValue}");
        }

        // Skips the bare newlines a master sends as keepalives while it
        // prepares a snapshot.
        private async Task<string> ReadNonEmptyLineAsync(
            RespReader reader,
            CancellationToken cancellationToken)
        {
            var timeout = TimeSpan.FromSeconds(Math.Max(5, _options.ReplicationTimeoutSeconds));

            while (true)
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                cts.CancelAfter(timeout);

                var line = await reader.ReadLineAsync(cts.Token)
                    ?? throw new IOException("master closed the connection");

                Touch();

                if (line.Length > 0)
                    return line;
            }
        }
    }
}
