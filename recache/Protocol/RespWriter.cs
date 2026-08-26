using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace recache.Protocol
{
    public sealed class RespWriter
    {
        private readonly Stream _stream;

        // A single connection's writes are always sequential (one
        // reply is fully written, and awaited, before the next is
        // built), so it's safe to reuse one scratch buffer per
        // writer instance. Every reply is serialized here in full
        // and sent as exactly one stream write — previously a bulk
        // string alone took three separate writes (length prefix,
        // payload, CRLF), each its own TCP packet since NoDelay is
        // set, and an array multiplied that per element.
        private readonly ArrayBufferWriter<byte> _scratch = new();

        public RespWriter(Stream stream)
        {
            _stream = stream;
        }

        public ValueTask WriteAsync(
            RespValue value,
            CancellationToken cancellationToken)
        {
            _scratch.Clear();

            Serialize(value, _scratch);

            return _stream.WriteAsync(
                _scratch.WrittenMemory,
                cancellationToken);
        }

        private static void Serialize(
            RespValue value,
            ArrayBufferWriter<byte> buffer)
        {
            switch (value.Type)
            {
                case RespType.SimpleString:

                    WriteLine(buffer, '+', value.StringValue!);
                    break;

                case RespType.Error:

                    WriteLine(buffer, '-', value.StringValue!);
                    break;

                case RespType.Integer:

                    WriteLine(
                        buffer,
                        ':',
                        value.IntegerValue.ToString(
                            CultureInfo.InvariantCulture));

                    break;

                case RespType.Null:

                    WriteRaw(buffer, "$-1\r\n");
                    break;

                case RespType.NullArray:

                    WriteRaw(buffer, "*-1\r\n");
                    break;

                case RespType.BulkString:

                    var data = value.BinaryValue!;

                    WriteRaw(buffer, $"${data.Length}\r\n");
                    buffer.Write(data);
                    WriteRaw(buffer, "\r\n");

                    break;

                case RespType.Array:

                    WriteRaw(buffer, $"*{value.Items!.Count}\r\n");

                    foreach (var item in value.Items)
                        Serialize(item, buffer);

                    break;

                default:
                    throw new InvalidOperationException();
            }
        }

        private static void WriteLine(
            ArrayBufferWriter<byte> buffer,
            char prefix,
            string text)
        {
            WriteRaw(buffer, $"{prefix}{text}\r\n");
        }

        private static void WriteRaw(
            ArrayBufferWriter<byte> buffer,
            string text)
        {
            var byteCount = Encoding.UTF8.GetByteCount(text);
            var span = buffer.GetSpan(byteCount);
            var written = Encoding.UTF8.GetBytes(text, span);
            buffer.Advance(written);
        }

        public ValueTask SimpleStringAsync(
    string value,
    CancellationToken cancellationToken = default)
        {
            return WriteAsync(
                RespValue.Simple(value),
                cancellationToken);
        }

        public ValueTask ErrorAsync(
            string value,
            CancellationToken cancellationToken = default)
        {
            return WriteAsync(
                RespValue.Error(value),
                cancellationToken);
        }

        public ValueTask IntegerAsync(
            long value,
            CancellationToken cancellationToken = default)
        {
            return WriteAsync(
                RespValue.Integer(value),
                cancellationToken);
        }

        public ValueTask NullAsync(
            CancellationToken cancellationToken = default)
        {
            return WriteAsync(
                RespValue.Null(),
                cancellationToken);
        }

        public ValueTask BulkStringAsync(
            byte[] value,
            CancellationToken cancellationToken = default)
        {
            return WriteAsync(
                RespValue.Bulk(value),
                cancellationToken);
        }
    }
}
