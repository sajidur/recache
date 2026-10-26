using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace recache.Protocol
{
    // Stream-based reader for client-side code (the benchmark, the
    // CLUSTER MEET bootstrap, MIGRATE, the replication handshake,
    // tests). Buffers whatever the stream has and parses whole
    // values out of it synchronously with RespParser; only an empty
    // buffer costs an await.
    public sealed class RespReader
    {
        private readonly Stream _stream;
        private readonly int _maxRequestBytes;

        private byte[] _buffer = new byte[16 * 1024];
        private int _start;
        private int _end;

        public RespReader(
            Stream stream,
            int maxRequestBytes)
        {
            _stream = stream;
            _maxRequestBytes = maxRequestBytes;
        }

        // Bytes already read from the stream but not yet parsed —
        // the start of whatever follows a handshake reply (e.g. the
        // snapshot or command stream after +FULLRESYNC).
        public ReadOnlyMemory<byte> Buffered =>
            _buffer.AsMemory(_start, _end - _start);

        public void ConsumeBuffered(int count)
        {
            _start += count;

            if (_start == _end)
                _start = _end = 0;
        }

        public async ValueTask<RespValue?> ReadAsync(
            CancellationToken cancellationToken)
        {
            var needed = 0;

            while (true)
            {
                if (_end > _start && _end - _start >= needed)
                {
                    var status = RespParser.TryParseValue(
                        _buffer.AsSpan(_start, _end - _start),
                        _maxRequestBytes,
                        out var value,
                        out var consumed,
                        out needed,
                        out var error);

                    if (status == RespParseStatus.Complete)
                    {
                        ConsumeBuffered(consumed);
                        return value;
                    }

                    if (status == RespParseStatus.Invalid)
                        throw new RespProtocolException(error ?? "Protocol error");

                    needed = Math.Max(needed, _end - _start + 1);
                }

                if (!await FillAsync(needed, cancellationToken))
                {
                    if (_end == _start)
                        return null;

                    throw new EndOfStreamException();
                }
            }
        }

        // Reads exactly `count` raw bytes (e.g. a snapshot payload
        // that isn't CRLF-terminated) into `destination`.
        public async ValueTask ReadExactlyAsync(
            Stream destination,
            long count,
            CancellationToken cancellationToken)
        {
            while (count > 0)
            {
                if (_end == _start &&
                    !await FillAsync(1, cancellationToken))
                {
                    throw new EndOfStreamException();
                }

                var take = (int)Math.Min(count, _end - _start);

                await destination.WriteAsync(
                    _buffer.AsMemory(_start, take),
                    cancellationToken);

                ConsumeBuffered(take);
                count -= take;
            }
        }

        // Reads one CRLF-terminated line without RESP-parsing it.
        public async ValueTask<string?> ReadLineAsync(
            CancellationToken cancellationToken)
        {
            while (true)
            {
                var span = _buffer.AsSpan(_start, _end - _start);
                var lf = span.IndexOf((byte)'\n');

                if (lf >= 0)
                {
                    var line = span[..lf];

                    if (!line.IsEmpty && line[^1] == (byte)'\r')
                        line = line[..^1];

                    var text = Encoding.UTF8.GetString(line);
                    ConsumeBuffered(lf + 1);
                    return text;
                }

                if (_end - _start > 64 * 1024)
                    throw new RespProtocolException("Line too long");

                if (!await FillAsync(_end - _start + 1, cancellationToken))
                    return null;
            }
        }

        private async ValueTask<bool> FillAsync(
            int needed,
            CancellationToken cancellationToken)
        {
            if (_start > 0 && _end == _buffer.Length)
            {
                Buffer.BlockCopy(_buffer, _start, _buffer, 0, _end - _start);
                _end -= _start;
                _start = 0;
            }

            var required = Math.Max(needed, _end - _start + 1);

            if (required > _buffer.Length - _start)
            {
                var size = _buffer.Length;

                while (size < required)
                    size *= 2;

                var grown = new byte[size];
                Buffer.BlockCopy(_buffer, _start, grown, 0, _end - _start);
                _end -= _start;
                _start = 0;
                _buffer = grown;
            }

            var read = await _stream.ReadAsync(
                _buffer.AsMemory(_end),
                cancellationToken);

            if (read == 0)
                return false;

            _end += read;
            return true;
        }
    }
}
