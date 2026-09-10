using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace recache.Protocol
{
    public sealed class RespReader
    {
        private readonly Stream _stream;
        private readonly int _maxRequestBytes;

        // Every read (byte-at-a-time parsing and bulk-string bodies
        // alike) goes through this buffer instead of the stream
        // directly. Without it, a single ~35-byte SET command meant
        // 30+ individual async socket reads — one per byte — which
        // dominates latency far more than actual command execution.
        private readonly byte[] _buffer = new byte[8192];
        private int _bufferStart;
        private int _bufferEnd;

        public RespReader(
            Stream stream,
            int maxRequestBytes)
        {
            _stream = stream;
            _maxRequestBytes = maxRequestBytes;
        }

        public async ValueTask<RespValue?> ReadAsync(
            CancellationToken cancellationToken)
        {
            var type = await ReadByteAsync(cancellationToken);

            if (type == -1)
                return null;

            return type switch
            {
                '*' => await ReadArrayAsync(cancellationToken),
                '$' => await ReadBulkStringAsync(cancellationToken),
                '+' => RespValue.Simple(
                    await ReadLineAsync(cancellationToken)),

                '-' => RespValue.Error(
                    await ReadLineAsync(cancellationToken)),

                ':' => RespValue.Integer(
                    long.Parse(
                        await ReadLineAsync(cancellationToken))),

                _ => throw new RespProtocolException(
                    $"Unknown RESP type: {(char)type}")
            };
        }

        private async ValueTask<RespValue> ReadArrayAsync(
            CancellationToken cancellationToken)
        {
            var count = await ReadIntegerLineAsync(
                cancellationToken);

            if (count < 0)
                return RespValue.NullArray();

            if (count > 1024)
                throw new RespProtocolException(
                    "Too many array elements.");

            var items = new RespValue[count];

            for (var i = 0; i < count; i++)
            {
                var item = await ReadAsync(cancellationToken);

                if (item == null)
                    throw new EndOfStreamException();

                items[i] = item;
            }

            return RespValue.Array(items);
        }

        private async ValueTask<RespValue> ReadBulkStringAsync(
            CancellationToken cancellationToken)
        {
            var length = await ReadIntegerLineAsync(
                cancellationToken);

            if (length == -1)
                return RespValue.Null();

            if (length < 0 ||
                length > _maxRequestBytes)
            {
                throw new RespProtocolException(
                    "Bulk string too large.");
            }

            var buffer = GC.AllocateUninitializedArray<byte>(
                checked((int)length));

            await ReadExactlyAsync(
                buffer,
                cancellationToken);

            await ExpectCrlfAsync(cancellationToken);

            return RespValue.Bulk(buffer);
        }

        private async ValueTask<long> ReadIntegerLineAsync(
            CancellationToken cancellationToken)
        {
            var line =
                await ReadLineAsync(cancellationToken);

            if (!long.TryParse(line, out var result))
                throw new RespProtocolException(
                    $"Invalid integer: {line}");

            return result;
        }

        private async ValueTask<string> ReadLineAsync(
            CancellationToken cancellationToken)
        {
            var buffer = new ArrayBufferWriter<byte>();

            while (true)
            {
                var b = await ReadByteAsync(
                    cancellationToken);

                if (b == -1)
                    throw new EndOfStreamException();

                if (b == '\r')
                {
                    var next = await ReadByteAsync(
                        cancellationToken);

                    if (next != '\n')
                        throw new RespProtocolException(
                            "Invalid CRLF.");

                    return Encoding.UTF8.GetString(
                        buffer.WrittenSpan);
                }

                if (buffer.WrittenCount >= 64 * 1024)
                    throw new RespProtocolException(
                        "RESP line too long.");

                buffer.GetSpan(1)[0] = (byte)b;
                buffer.Advance(1);
            }
        }

        private async ValueTask<int> ReadByteAsync(
            CancellationToken cancellationToken)
        {
            if (_bufferStart == _bufferEnd &&
                !await FillBufferAsync(cancellationToken))
            {
                return -1;
            }

            return _buffer[_bufferStart++];
        }

        private async ValueTask ReadExactlyAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken)
        {
            var position = 0;

            while (position < buffer.Length)
            {
                if (_bufferStart == _bufferEnd &&
                    !await FillBufferAsync(cancellationToken))
                {
                    throw new EndOfStreamException();
                }

                var available = _bufferEnd - _bufferStart;
                var toCopy = Math.Min(
                    available,
                    buffer.Length - position);

                _buffer.AsSpan(_bufferStart, toCopy).CopyTo(
                    buffer.Span.Slice(position, toCopy));

                _bufferStart += toCopy;
                position += toCopy;
            }
        }

        // Refills the internal buffer from the stream. Returns
        // false at end of stream.
        private async ValueTask<bool> FillBufferAsync(
            CancellationToken cancellationToken)
        {
            _bufferEnd = await _stream.ReadAsync(
                _buffer,
                cancellationToken);

            _bufferStart = 0;

            return _bufferEnd != 0;
        }

        private async ValueTask ExpectCrlfAsync(
            CancellationToken cancellationToken)
        {
            var cr = await ReadByteAsync(cancellationToken);
            var lf = await ReadByteAsync(cancellationToken);

            if (cr != '\r' || lf != '\n')
                throw new RespProtocolException(
                    "Expected CRLF.");
        }
    }
}
