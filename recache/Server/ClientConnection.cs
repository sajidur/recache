using recache.Commands;
using recache.Configuration;
using recache.Protocol;
using recache.Security;
using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;

namespace recache.Server
{
    public sealed class ClientConnection
    {
        private readonly TcpClient _client;
        private readonly CommandDispatcher _dispatcher;
        private readonly RecacheOptions _options;

        public ClientConnection(
            TcpClient client,
            CommandDispatcher dispatcher,
            RecacheOptions options)
        {
            _client = client;
            _dispatcher = dispatcher;
            _options = options;
        }

        public async Task RunAsync(
            CancellationToken cancellationToken)
        {
            using (_client)
            await using (var stream = _client.GetStream())
            {
                var reader = new RespReader(
                    stream,
                    _options.MaxRequestBytes);

                var writer = new RespWriter(stream);
                var session = new ClientSession();

                while (!cancellationToken.IsCancellationRequested)
                {
                    RespValue? request;

                    try
                    {
                        request =
                            await reader.ReadAsync(
                                cancellationToken);
                    }
                    catch (RespProtocolException ex)
                    {
                        await writer.WriteAsync(
                            RespValue.Error(
                                $"ERR {ex.Message}"),
                            cancellationToken);

                        break;
                    }

                    if (request == null)
                        break;

                    var response =
                        await _dispatcher.ExecuteAsync(
                            request,
                            session,
                            cancellationToken);

                    await writer.WriteAsync(
                        response,
                        cancellationToken);
                }
            }
        }
    }
}
