using recache.Protocol;
using System;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace recache.Clustering
{
    // One TCP connection on the cluster bus. Each node keeps an
    // outbound link to every peer (it sends PING/MEET/FAIL/AUTH
    // requests there and reads the replies), and accepts inbound
    // links from every peer (it reads their messages and replies on
    // the same connection). Sends go through a bounded queue drained
    // by one loop, so callers never block on the network.
    internal sealed class ClusterLink
    {
        private readonly Socket _socket;
        private readonly CancellationTokenSource _cts = new();

        private readonly Channel<byte[]> _queue =
            Channel.CreateBounded<byte[]>(new BoundedChannelOptions(1024)
            {
                FullMode = BoundedChannelFullMode.DropOldest,
                SingleReader = true,
            });

        private int _closed;

        public ClusterLink(Socket socket, bool outbound, string? nodeId, bool handshake)
        {
            _socket = socket;
            Outbound = outbound;
            NodeId = nodeId;
            Handshake = handshake;
            CreatedMs = Environment.TickCount64;

            RemoteHost = Describe(socket.RemoteEndPoint);
            LocalHost = Describe(socket.LocalEndPoint);
        }

        public bool Outbound { get; }

        // The peer this link belongs to, once known (an inbound link
        // learns it from the first message; a CLUSTER MEET link from
        // the first PONG).
        public string? NodeId { get; set; }

        // An outbound link opened by CLUSTER MEET to an address whose
        // node id isn't known yet.
        public bool Handshake { get; set; }

        // For a handshake link: the address that was dialled.
        public string? DialedHost { get; init; }

        public int DialedPort { get; init; }

        public int DialedBusPort { get; init; }

        public long CreatedMs { get; }

        public string RemoteHost { get; }

        public string LocalHost { get; }

        public bool Closed => Volatile.Read(ref _closed) != 0;

        public void Send(ClusterMessage message)
        {
            if (Closed)
                return;

            _queue.Writer.TryWrite(RespSerializer.ToBytes(message.Encode()));
        }

        public async Task RunAsync(
            Action<ClusterMessage, ClusterLink> onMessage,
            Action<ClusterLink> onClosed)
        {
            var token = _cts.Token;

            try
            {
                var sending = SendLoopAsync(token);
                var receiving = ReceiveLoopAsync(onMessage, token);

                await Task.WhenAny(sending, receiving);
            }
            catch
            {
            }
            finally
            {
                Close();
                onClosed(this);
            }
        }

        public void Close()
        {
            if (Interlocked.Exchange(ref _closed, 1) != 0)
                return;

            _cts.Cancel();
            _queue.Writer.TryComplete();

            try
            {
                _socket.Shutdown(SocketShutdown.Both);
            }
            catch (Exception ex) when (ex is SocketException or ObjectDisposedException)
            {
            }

            _socket.Dispose();
        }

        private async Task SendLoopAsync(CancellationToken cancellationToken)
        {
            try
            {
                await foreach (var bytes in _queue.Reader.ReadAllAsync(cancellationToken))
                    await _socket.SendAsync(bytes, SocketFlags.None, cancellationToken);
            }
            catch (Exception ex) when (ex is OperationCanceledException or SocketException or ObjectDisposedException)
            {
            }
        }

        private async Task ReceiveLoopAsync(
            Action<ClusterMessage, ClusterLink> onMessage,
            CancellationToken cancellationToken)
        {
            var buffer = new byte[16 * 1024];
            var end = 0;
            var needed = 0;

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    if (end == buffer.Length || needed > buffer.Length)
                        Array.Resize(ref buffer, Math.Max(buffer.Length * 2, needed));

                    var read = await _socket.ReceiveAsync(
                        buffer.AsMemory(end),
                        SocketFlags.None,
                        cancellationToken);

                    if (read == 0)
                        return;

                    end += read;

                    if (end < needed)
                        continue;

                    var start = 0;

                    while (start < end)
                    {
                        var status = RespParser.TryParseValue(
                            buffer.AsSpan(start, end - start),
                            1024 * 1024,
                            out var value,
                            out var consumed,
                            out needed,
                            out _);

                        if (status == RespParseStatus.Incomplete)
                            break;

                        if (status == RespParseStatus.Invalid)
                            return;

                        start += consumed;
                        needed = 0;

                        if (ClusterMessage.Decode(value!) is { } message)
                            onMessage(message, this);
                    }

                    if (start > 0)
                    {
                        Buffer.BlockCopy(buffer, start, buffer, 0, end - start);
                        end -= start;
                    }
                }
            }
            catch (Exception ex) when (ex is OperationCanceledException or SocketException or ObjectDisposedException)
            {
            }
        }

        private static string Describe(EndPoint? endpoint)
        {
            if (endpoint is not IPEndPoint ip)
                return "";

            var address = ip.Address.IsIPv4MappedToIPv6
                ? ip.Address.MapToIPv4()
                : ip.Address;

            return address.ToString();
        }
    }
}
