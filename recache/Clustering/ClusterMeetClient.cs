using recache.Protocol;
using System;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading;

namespace recache.Clustering
{
    // CLUSTER MEET has no cluster bus to shake hands over yet
    // (Phase 2), so it bootstraps topology the way any other client
    // would: connect over plain RESP and ask the peer for its own
    // CLUSTER NODES view. One-shot — not kept in sync afterwards.
    public static class ClusterMeetClient
    {
        public static string? FetchNodesText(
            string host,
            int port,
            TimeSpan timeout)
        {
            try
            {
                using var client = new TcpClient();
                var connectTask = client.ConnectAsync(host, port);

                if (!connectTask.Wait(timeout))
                    return null;

                client.NoDelay = true;

                using var stream = client.GetStream();
                stream.ReadTimeout = (int)timeout.TotalMilliseconds;
                stream.WriteTimeout = (int)timeout.TotalMilliseconds;

                var writer = new RespWriter(stream);
                var reader = new RespReader(stream, 16 * 1024 * 1024);

                var request = RespValue.Array(new[]
                {
                    RespValue.Bulk(Encoding.UTF8.GetBytes("CLUSTER")),
                    RespValue.Bulk(Encoding.UTF8.GetBytes("NODES")),
                });

                writer.WriteAsync(request, CancellationToken.None)
                    .AsTask()
                    .GetAwaiter()
                    .GetResult();

                var response = reader.ReadAsync(CancellationToken.None)
                    .AsTask()
                    .GetAwaiter()
                    .GetResult();

                if (response is null ||
                    response.Type != RespType.BulkString ||
                    response.BinaryValue is null)
                {
                    return null;
                }

                return Encoding.UTF8.GetString(response.BinaryValue);
            }
            catch (Exception ex) when (
                ex is SocketException or
                    IOException or
                    RespProtocolException)
            {
                return null;
            }
        }
    }
}
