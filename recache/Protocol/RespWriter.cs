using System;
using System.Collections.Generic;
using System.Text;

namespace recache.Protocol
{
    public sealed class RespWriter
    {
        private readonly Stream _stream;

        public RespWriter(Stream stream)
        {
            _stream = stream;
        }

        public async ValueTask WriteAsync(
            RespValue value,
            CancellationToken cancellationToken)
        {
            switch (value.Type)
            {
                case RespType.SimpleString:

                    await WriteStringAsync(
                        $"+{value.StringValue}\r\n",
                        cancellationToken);

                    break;

                case RespType.Error:

                    await WriteStringAsync(
                        $"-{value.StringValue}\r\n",
                        cancellationToken);

                    break;

                case RespType.Integer:

                    await WriteStringAsync(
                        $":{value.IntegerValue}\r\n",
                        cancellationToken);

                    break;

                case RespType.Null:

                    await WriteStringAsync(
                        "$-1\r\n",
                        cancellationToken);

                    break;

                case RespType.BulkString:

                    await WriteBulkAsync(
                        value.BinaryValue!,
                        cancellationToken);

                    break;

                case RespType.Array:

                    await WriteStringAsync(
                        $"*{value.Items!.Count}\r\n",
                        cancellationToken);

                    foreach (var item in value.Items)
                    {
                        await WriteAsync(
                            item,
                            cancellationToken);
                    }

                    break;

                default:
                    throw new InvalidOperationException();
            }
        }

        private async ValueTask WriteBulkAsync(
            byte[] data,
            CancellationToken cancellationToken)
        {
            await WriteStringAsync(
                $"${data.Length}\r\n",
                cancellationToken);

            await _stream.WriteAsync(
                data,
                cancellationToken);

            await _stream.WriteAsync(
                "\r\n"u8.ToArray(),
                cancellationToken);
        }

        private ValueTask WriteStringAsync(
            string value,
            CancellationToken cancellationToken)
        {
            return _stream.WriteAsync(
                Encoding.UTF8.GetBytes(value),
                cancellationToken);
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
