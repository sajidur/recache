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
                return RespValue.Null();

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
            var buffer = new byte[1];

            var count = await _stream.ReadAsync(
                buffer,
                cancellationToken);

            return count == 0
                ? -1
                : buffer[0];
        }

        private async ValueTask ReadExactlyAsync(
            Memory<byte> buffer,
            CancellationToken cancellationToken)
        {
            var position = 0;

            while (position < buffer.Length)
            {
                var read = await _stream.ReadAsync(
                    buffer[position..],
                    cancellationToken);

                if (read == 0)
                    throw new EndOfStreamException();

                position += read;
            }
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
