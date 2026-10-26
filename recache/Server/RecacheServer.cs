using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using recache.Configuration;
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;

namespace recache.Server
{
    // A BackgroundService (real IHostedService) rather than a plain
    // class driven manually — the Generic Host only fires
    // ApplicationStarted once every registered IHostedService's
    // StartAsync returns, and WindowsServiceLifetime only reports
    // "running" to the Service Control Manager once that happens.
    // Bypassing IHostedService here previously left the SCM waiting
    // until it timed out ("service did not respond in a timely
    // fashion"), even though the server itself was working fine.
    public sealed class RecacheServer : BackgroundService
    {
            private readonly RecacheOptions _options;
            private readonly RecacheNode _node;
            private readonly ILogger<RecacheServer> _logger;

            private TcpListener? _listener;

            private int _connections;

            public RecacheServer(
                RecacheOptions options,
                RecacheNode node,
                ILogger<RecacheServer> logger)
            {
                _options = options;
                _node = node;
                _logger = logger;
            }

            protected override async Task ExecuteAsync(
                CancellationToken stoppingToken)
            {
                // Let StartAsync return (and the service report
                // "running") before a potentially long AOF replay.
                await Task.Yield();

                _node.Start();

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
                    Math.Min(_options.MaxConnections, 4096));

                _logger.LogInformation(
                    "Recache listening on {Host}:{Port}",
                    _options.Host,
                    _options.Port);

                try
                {
                    while (!stoppingToken.IsCancellationRequested)
                    {
                        Socket socket;

                        try
                        {
                            socket =
                                await _listener.AcceptSocketAsync(
                                    stoppingToken);
                        }
                        catch (OperationCanceledException)
                        {
                            break;
                        }
                        catch (SocketException)
                        {
                            continue;
                        }

                        socket.NoDelay = true;

                        if (Interlocked.Increment(
                                ref _connections)
                            > _options.MaxConnections)
                        {
                            Interlocked.Decrement(
                                ref _connections);

                            _node.Metrics.ConnectionRejected();
                            socket.Dispose();

                            continue;
                        }

                        _ = HandleClientAsync(
                            socket,
                            stoppingToken);
                    }
                }
                finally
                {
                    _listener.Stop();
                }
            }

            public override async Task StopAsync(CancellationToken cancellationToken)
            {
                // Still accepting and serving here: during a cluster
                // handover, clients that hit this node are redirected
                // to the new owner instead of finding it gone.
                try
                {
                    await _node.PrepareShutdownAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error handing over before shutdown");
                }

                await base.StopAsync(cancellationToken);

                _node.Dispose();
            }

            private async Task HandleClientAsync(
                Socket socket,
                CancellationToken cancellationToken)
            {
                _node.Metrics.ClientConnected();

                try
                {
                    var connection =
                        new ClientConnection(
                            socket,
                            _node.Dispatcher,
                            _options,
                            _node.Replication,
                            _node.Aof);

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

                    _node.Metrics.ClientDisconnected();
                    socket.Dispose();
                }
            }
        }
    }
