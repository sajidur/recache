using Microsoft.Extensions.Logging;
using recache.Commands;
using recache.Configuration;
using recache.Storage;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;

namespace recache.Server
{
    public sealed class RecacheServer
    {
            private readonly RecacheOptions _options;
            private readonly CommandDispatcher _dispatcher;
            private readonly ILogger<RecacheServer> _logger;

            private TcpListener? _listener;

            private int _connections;

            public RecacheServer(
                RecacheOptions options,
                CommandDispatcher dispatcher,
                ILogger<RecacheServer> logger)
            {
                _options = options;
                _dispatcher = dispatcher;
                _logger = logger;
            }

            public async Task StartAsync(
                CancellationToken cancellationToken)
            {
                var bindAllInterfaces = _options.Host == "0.0.0.0";

                // TcpListener bound to IPAddress.Any is IPv4-only.
                // "localhost" resolves to the IPv6 loopback (::1)
                // first on most systems, which would then fail to
                // connect entirely. Bind dual-stack instead so both
                // IPv4 and IPv6 (and IPv4-mapped) clients work.
                _listener = new TcpListener(
                    bindAllInterfaces
                        ? IPAddress.IPv6Any
                        : IPAddress.Parse(_options.Host),
                    _options.Port);

                if (bindAllInterfaces)
                    _listener.Server.DualMode = true;

                _listener.Start(
                    _options.MaxConnections);

                _logger.LogInformation(
                    "Recache listening on {Host}:{Port}",
                    _options.Host,
                    _options.Port);

                while (!cancellationToken.IsCancellationRequested)
                {
                    TcpClient client;

                    try
                    {
                        client =
                            await _listener.AcceptTcpClientAsync(
                                cancellationToken);
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }

                    client.NoDelay = true;

                    if (Interlocked.Increment(
                            ref _connections)
                        > _options.MaxConnections)
                    {
                        Interlocked.Decrement(
                            ref _connections);

                        client.Dispose();

                        continue;
                    }

                    _ = HandleClientAsync(
                        client,
                        cancellationToken);
                }
            }

            private async Task HandleClientAsync(
                TcpClient client,
                CancellationToken cancellationToken)
            {
                try
                {
                    var connection =
                        new ClientConnection(
                            client,
                            _dispatcher,
                            _options);

                    await connection.RunAsync(
                        cancellationToken);
                }
                catch (OperationCanceledException)
                {
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex,
                        "Unhandled error on client connection");
                }
                finally
                {
                    Interlocked.Decrement(
                        ref _connections);

                    client.Dispose();
                }
            }

            public void Stop()
            {
                _listener?.Stop();
            }
        }
    }