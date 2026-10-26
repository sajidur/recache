using recache.Commands;
using recache.Configuration;
using recache.Monitoring;
using recache.Persistence;
using recache.Protocol;
using recache.Replication;
using recache.Security;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;

namespace recache.Server
{
    // One client connection. Reads whatever the socket has, executes
    // every complete command in it, and answers the whole batch with a
    // single send.
    //
    // That batching is the biggest single throughput factor: with
    // pipelining (or many concurrent requests on one connection) the
    // old loop paid one awaited socket write — one TCP packet, since
    // NoDelay is set — per reply, and one awaited read per byte. Now a
    // batch of N pipelined commands costs one receive and one send,
    // the same shape as Redis's event loop.
    public sealed class ClientConnection
    {
        private const int InitialBufferBytes = 16 * 1024;
        private const int FlushThresholdBytes = 64 * 1024;

        private readonly Socket _socket;
        private readonly CommandDispatcher _dispatcher;
        private readonly RecacheOptions _options;
        private readonly ReplicationManager? _replication;
        private readonly AofManager? _aof;

        private ArrayBufferWriter<byte> _output = new(4096);
        private ClientSession? _session;

        // The AOF already covers this client's writes up to here.
        private long _acknowledgedOffset;

        public ClientConnection(
            Socket socket,
            CommandDispatcher dispatcher,
            RecacheOptions options,
            ReplicationManager? replication,
            AofManager? aof = null)
        {
            _socket = socket;
            _dispatcher = dispatcher;
            _options = options;
            _replication = replication;
            _aof = aof;
        }

        public async Task RunAsync(
            CancellationToken cancellationToken)
        {
            var session = _session = new ClientSession
            {
                RemoteAddress = _socket.RemoteEndPoint?.ToString() ?? "",
            };

            var buffer = ArrayPool<byte>.Shared.Rent(InitialBufferBytes);
            var start = 0;
            var end = 0;
            var needed = 0;

            // Room for the largest allowed bulk string plus its framing
            // and a few more commands behind it.
            var maxBuffer = (long)_options.MaxRequestBytes + 1024 * 1024;

            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    if (end == buffer.Length || needed > buffer.Length - start)
                    {
                        if (start > 0)
                        {
                            Buffer.BlockCopy(buffer, start, buffer, 0, end - start);
                            end -= start;
                            start = 0;
                        }

                        if (end == buffer.Length || needed > buffer.Length)
                        {
                            var size = Math.Max((long)buffer.Length * 2, needed);

                            if (size > maxBuffer)
                                size = maxBuffer;

                            if (size <= buffer.Length)
                            {
                                await WriteErrorAndFlushAsync(
                                    "ERR Protocol error: request too large",
                                    cancellationToken);

                                return;
                            }

                            var grown = ArrayPool<byte>.Shared.Rent((int)size);
                            Buffer.BlockCopy(buffer, 0, grown, 0, end);
                            ArrayPool<byte>.Shared.Return(buffer);
                            buffer = grown;
                        }
                    }

                    var read = await _socket.ReceiveAsync(
                        buffer.AsMemory(end),
                        SocketFlags.None,
                        cancellationToken);

                    if (read == 0)
                        return;

                    end += read;

                    if (end - start < needed)
                        continue;

                    while (start < end)
                    {
                        var status = RespParser.TryParseRequest(
                            buffer.AsSpan(start, end - start),
                            _options.MaxRequestBytes,
                            out var request,
                            out var consumed,
                            out needed,
                            out var error);

                        if (status == RespParseStatus.Incomplete)
                            break;

                        if (status == RespParseStatus.Invalid)
                        {
                            await WriteErrorAndFlushAsync(
                                $"ERR {error}",
                                cancellationToken);

                            return;
                        }

                        start += consumed;
                        needed = 0;

                        if (request is null)
                            continue;

                        var pending = _dispatcher.ExecuteAsync(
                            request,
                            session,
                            cancellationToken);

                        RespValue reply;

                        if (pending.IsCompletedSuccessfully)
                        {
                            reply = pending.Result;
                        }
                        else
                        {
                            // The command has to wait (WAIT, a paused
                            // write, an fsync=always durability wait):
                            // send what's ready first so earlier
                            // replies aren't held hostage.
                            await FlushAsync(cancellationToken);
                            reply = await pending;
                        }

                        if (!ReferenceEquals(reply, CommandDispatcher.NoReply))
                            RespSerializer.Write(_output, reply);

                        if (session.Handoff != SessionHandoff.None)
                        {
                            await FlushAsync(cancellationToken);

                            if (session.Handoff == SessionHandoff.Replication &&
                                _replication is not null)
                            {
                                await _replication.ServeReplicaAsync(
                                    _socket,
                                    session,
                                    session.HandoffArgs!,
                                    buffer.AsMemory(start, end - start).ToArray(),
                                    cancellationToken);
                            }

                            return;
                        }

                        if (_output.WrittenCount >= FlushThresholdBytes)
                            await FlushAsync(cancellationToken);
                    }

                    if (start == end)
                        start = end = 0;

                    await FlushAsync(cancellationToken);
                }
            }
            catch (Exception ex) when (ex is SocketException or ObjectDisposedException)
            {
                // Client went away mid-read or mid-write.
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        private async ValueTask FlushAsync(CancellationToken cancellationToken)
        {
            if (_output.WrittenCount == 0)
                return;

            // Acknowledge writes only once the AOF has them (handed to
            // the OS, or fsynced with "always") — one wait for the whole
            // batch of replies, so pipelined writes share it.
            if (_aof is not null &&
                _session is { } session &&
                session.LastWriteOffset > _acknowledgedOffset)
            {
                await _aof.WaitAcknowledgeableAsync(session.LastWriteOffset);
                _acknowledgedOffset = session.LastWriteOffset;
            }

            var data = _output.WrittenMemory;

            while (!data.IsEmpty)
            {
                var sent = await _socket.SendAsync(data, SocketFlags.None, cancellationToken);
                data = data[sent..];
            }

            // A one-off huge reply shouldn't pin megabytes per
            // connection for the rest of its life.
            if (_output.Capacity > 1024 * 1024)
                _output = new ArrayBufferWriter<byte>(4096);
            else
                _output.ResetWrittenCount();
        }

        private async ValueTask WriteErrorAndFlushAsync(
            string message,
            CancellationToken cancellationToken)
        {
            RespSerializer.Write(_output, RespValue.Error(message));
            await FlushAsync(cancellationToken);
        }
    }
}
