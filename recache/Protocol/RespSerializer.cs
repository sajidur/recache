using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Text;

namespace recache.Protocol
{
    // Formats RESP2 straight into a caller-supplied buffer. Lengths
    // and integers go through Utf8Formatter instead of string
    // interpolation, so serializing a reply allocates nothing.
    public static class RespSerializer
    {
        private static ReadOnlySpan<byte> Crlf => "\r\n"u8;

        private static ReadOnlySpan<byte> NullBulk => "$-1\r\n"u8;

        private static ReadOnlySpan<byte> NullArray => "*-1\r\n"u8;

        public static void Write(
            IBufferWriter<byte> writer,
            RespValue value)
        {
            switch (value.Type)
            {
                case RespType.SimpleString:
                    WriteLine(writer, (byte)'+', value.StringValue!);
                    break;

                case RespType.Error:
                    WriteLine(writer, (byte)'-', value.StringValue!);
                    break;

                case RespType.Integer:
                    WritePrefixedNumber(writer, (byte)':', value.IntegerValue);
                    break;

                case RespType.Null:
                    writer.Write(NullBulk);
                    break;

                case RespType.NullArray:
                    writer.Write(NullArray);
                    break;

                case RespType.BulkString:
                    WriteBulk(writer, value.BinaryValue!);
                    break;

                case RespType.Array:
                    if (value.BulkItems is { } bulks)
                    {
                        WriteArrayHeader(writer, bulks.Length);

                        foreach (var bulk in bulks)
                            WriteBulk(writer, bulk);

                        break;
                    }

                    var items = value.Items!;
                    WriteArrayHeader(writer, items.Count);

                    foreach (var item in items)
                        Write(writer, item);

                    break;

                default:
                    throw new InvalidOperationException(
                        $"Cannot serialize RESP type {value.Type}.");
            }
        }

        public static void WriteArrayHeader(
            IBufferWriter<byte> writer,
            int count)
            => WritePrefixedNumber(writer, (byte)'*', count);

        // One buffer reservation for header, payload and trailer — this
        // is every GET reply and every argument of a propagated write.
        public static void WriteBulk(
            IBufferWriter<byte> writer,
            ReadOnlySpan<byte> data)
        {
            var span = writer.GetSpan(data.Length + 15);

            span[0] = (byte)'$';
            Utf8Formatter.TryFormat(data.Length, span[1..], out var digits);
            span[digits + 1] = (byte)'\r';
            span[digits + 2] = (byte)'\n';

            var offset = digits + 3;

            data.CopyTo(span[offset..]);
            offset += data.Length;

            span[offset] = (byte)'\r';
            span[offset + 1] = (byte)'\n';
            writer.Advance(offset + 2);
        }

        public static void WriteBulk(
            IBufferWriter<byte> writer,
            string text)
        {
            var byteCount = Encoding.UTF8.GetByteCount(text);

            WritePrefixedNumber(writer, (byte)'$', byteCount);

            var span = writer.GetSpan(byteCount + 2);
            var written = Encoding.UTF8.GetBytes(text, span);

            span[written] = (byte)'\r';
            span[written + 1] = (byte)'\n';
            writer.Advance(written + 2);
        }

        public static void WriteBulk(
            IBufferWriter<byte> writer,
            long number)
        {
            Span<byte> digits = stackalloc byte[20];
            Utf8Formatter.TryFormat(number, digits, out var length);
            WriteBulk(writer, digits[..length]);
        }

        // Serializes a command — an array of bulk strings — the form
        // used for every propagated write and outgoing request.
        public static void WriteCommand(
            IBufferWriter<byte> writer,
            IReadOnlyList<RespValue> args)
        {
            WriteArrayHeader(writer, args.Count);

            foreach (var arg in args)
            {
                if (arg.BinaryValue is not null)
                    WriteBulk(writer, arg.BinaryValue);
                else
                    WriteBulk(writer, arg.StringValue ?? "");
            }
        }

        public static void WriteCommand(
            IBufferWriter<byte> writer,
            params string[] args)
        {
            WriteArrayHeader(writer, args.Length);

            foreach (var arg in args)
                WriteBulk(writer, arg);
        }

        public static byte[] ToBytes(RespValue value)
        {
            var buffer = new ArrayBufferWriter<byte>(64);
            Write(buffer, value);
            return buffer.WrittenSpan.ToArray();
        }

        private static void WriteLine(
            IBufferWriter<byte> writer,
            byte prefix,
            string text)
        {
            var byteCount = Encoding.UTF8.GetByteCount(text);
            var span = writer.GetSpan(byteCount + 3);

            span[0] = prefix;

            var written = Encoding.UTF8.GetBytes(text, span[1..]);

            span[written + 1] = (byte)'\r';
            span[written + 2] = (byte)'\n';
            writer.Advance(written + 3);
        }

        private static void WritePrefixedNumber(
            IBufferWriter<byte> writer,
            byte prefix,
            long number)
        {
            var span = writer.GetSpan(23);

            span[0] = prefix;
            Utf8Formatter.TryFormat(number, span[1..], out var length);
            span[length + 1] = (byte)'\r';
            span[length + 2] = (byte)'\n';
            writer.Advance(length + 3);
        }
    }
}
