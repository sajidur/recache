using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Text;

namespace recache.Protocol
{
    public enum RespParseStatus
    {
        Complete,
        Incomplete,
        Invalid
    }

    // Synchronous RESP2 parsing over bytes that have already arrived.
    // Callers read whatever the socket has, then parse as many whole
    // values out of it as possible without awaiting anything — the
    // old reader awaited once per byte, which dominated latency, and
    // could never see that a pipelined batch was sitting in the
    // buffer ready to be answered with a single write.
    //
    // Incomplete means "nothing consumed, read more and call again";
    // `needed` is then a lower bound on the total buffered bytes
    // required (0 if unknown), so a multi-megabyte value isn't
    // re-parsed from the top on every partial read.
    public static class RespParser
    {
        // Matches real Redis's per-request element ceiling; the old
        // 1024 cap rejected `CLUSTER ADDSLOTS 0 1 ... 5460`, which is
        // exactly what `redis-cli --cluster create` sends.
        public const int MaxArrayElements = 1024 * 1024;

        private const int MaxLineBytes = 64 * 1024;

        // Maps a request's first argument (the command name) to a
        // shared, pre-built RespValue, so "SET"/"GET" don't cost a
        // byte[] and a RespValue per request. Returns null for names
        // it doesn't know (those are allocated as usual). Set once at
        // startup by the command table.
        public static Func<ReadOnlySpan<byte>, RespValue?>? CommandNameInterner { get; set; }

        public static RespParseStatus TryParseRequest(
            ReadOnlySpan<byte> buffer,
            int maxBulkBytes,
            out RespValue? request,
            out int consumed,
            out int needed,
            out string? error)
        {
            request = null;
            consumed = 0;
            needed = 0;
            error = null;

            if (buffer.IsEmpty)
                return RespParseStatus.Incomplete;

            return buffer[0] == (byte)'*'
                ? ParseMultiBulk(
                    buffer,
                    maxBulkBytes,
                    out request,
                    out consumed,
                    out needed,
                    out error)
                : ParseInline(
                    buffer,
                    out request,
                    out consumed,
                    out error);
        }

        // A request of the form "*<n>\r\n$<len>\r\n<bytes>\r\n...".
        // A Complete result with a null request means "consumed but
        // nothing to execute" (an empty "*0" array), which real
        // Redis also silently ignores.
        private static RespParseStatus ParseMultiBulk(
            ReadOnlySpan<byte> buffer,
            int maxBulkBytes,
            out RespValue? request,
            out int consumed,
            out int needed,
            out string? error)
        {
            request = null;
            consumed = 0;
            needed = 0;
            error = null;

            var pos = 1;

            var status = TryReadIntegerLine(
                buffer,
                ref pos,
                out var count,
                out error);

            if (status != RespParseStatus.Complete)
            {
                error ??= status == RespParseStatus.Invalid
                    ? "Protocol error: invalid multibulk length"
                    : null;

                return status;
            }

            if (count > MaxArrayElements)
            {
                error = "Protocol error: invalid multibulk length";
                return RespParseStatus.Invalid;
            }

            if (count <= 0)
            {
                consumed = pos;
                return RespParseStatus.Complete;
            }

            var items = new RespValue[count];

            for (var i = 0; i < count; i++)
            {
                if (pos >= buffer.Length)
                    return RespParseStatus.Incomplete;

                if (buffer[pos] != (byte)'$')
                {
                    error =
                        $"Protocol error: expected '$', got " +
                        $"'{(char)buffer[pos]}'";

                    return RespParseStatus.Invalid;
                }

                pos++;

                status = TryReadIntegerLine(
                    buffer,
                    ref pos,
                    out var length,
                    out error);

                if (status != RespParseStatus.Complete)
                {
                    error ??= status == RespParseStatus.Invalid
                        ? "Protocol error: invalid bulk length"
                        : null;

                    return status;
                }

                if (length < 0 || length > maxBulkBytes)
                {
                    error = "Protocol error: invalid bulk length";
                    return RespParseStatus.Invalid;
                }

                var end = pos + (int)length;

                if (end + 2 > buffer.Length)
                {
                    needed = end + 2;
                    return RespParseStatus.Incomplete;
                }

                if (buffer[end] != (byte)'\r' ||
                    buffer[end + 1] != (byte)'\n')
                {
                    error = "Protocol error: expected CRLF";
                    return RespParseStatus.Invalid;
                }

                var data = buffer.Slice(pos, (int)length);

                items[i] = (i == 0 ? CommandNameInterner?.Invoke(data) : null)
                    ?? RespValue.Bulk(data.ToArray());

                pos = end + 2;
            }

            request = RespValue.Array(items);
            consumed = pos;
            return RespParseStatus.Complete;
        }

        // The telnet-friendly form: "PING\r\n", "SET k v\n". Used by
        // humans and by redis-benchmark's *_INLINE tests. Supports
        // the same quoting as redis-cli: "..." with backslash escapes
        // and '...' with \' only.
        private static RespParseStatus ParseInline(
            ReadOnlySpan<byte> buffer,
            out RespValue? request,
            out int consumed,
            out string? error)
        {
            request = null;
            consumed = 0;
            error = null;

            var newline = buffer.IndexOf((byte)'\n');

            if (newline < 0)
            {
                if (buffer.Length > MaxLineBytes)
                {
                    error = "Protocol error: too big inline request";
                    return RespParseStatus.Invalid;
                }

                return RespParseStatus.Incomplete;
            }

            var line = buffer[..newline];

            if (!line.IsEmpty && line[^1] == (byte)'\r')
                line = line[..^1];

            consumed = newline + 1;

            if (!TrySplitInlineArgs(line, out var parts))
            {
                error = "Protocol error: unbalanced quotes in request";
                return RespParseStatus.Invalid;
            }

            if (parts.Count == 0)
                return RespParseStatus.Complete;

            var items = new RespValue[parts.Count];

            for (var i = 0; i < parts.Count; i++)
                items[i] = RespValue.Bulk(parts[i]);

            request = RespValue.Array(items);
            return RespParseStatus.Complete;
        }

        private static bool TrySplitInlineArgs(
            ReadOnlySpan<byte> line,
            out List<byte[]> parts)
        {
            parts = new List<byte[]>();
            var current = new List<byte>();
            var i = 0;

            while (true)
            {
                while (i < line.Length && IsSpace(line[i]))
                    i++;

                if (i >= line.Length)
                    return true;

                current.Clear();

                var inDouble = false;
                var inSingle = false;
                var done = false;

                while (!done)
                {
                    if (inDouble)
                    {
                        if (i >= line.Length)
                            return false;

                        var c = line[i];

                        if (c == (byte)'\\' &&
                            i + 3 < line.Length &&
                            line[i + 1] == (byte)'x' &&
                            IsHex(line[i + 2]) &&
                            IsHex(line[i + 3]))
                        {
                            current.Add((byte)(
                                HexValue(line[i + 2]) * 16 +
                                HexValue(line[i + 3])));

                            i += 3;
                        }
                        else if (c == (byte)'\\' && i + 1 < line.Length)
                        {
                            i++;

                            current.Add(line[i] switch
                            {
                                (byte)'n' => (byte)'\n',
                                (byte)'r' => (byte)'\r',
                                (byte)'t' => (byte)'\t',
                                (byte)'b' => (byte)'\b',
                                (byte)'a' => (byte)'\a',
                                var other => other
                            });
                        }
                        else if (c == (byte)'"')
                        {
                            // A closing quote must be followed by a
                            // space or end of line.
                            if (i + 1 < line.Length && !IsSpace(line[i + 1]))
                                return false;

                            done = true;
                        }
                        else
                        {
                            current.Add(c);
                        }
                    }
                    else if (inSingle)
                    {
                        if (i >= line.Length)
                            return false;

                        var c = line[i];

                        if (c == (byte)'\\' &&
                            i + 1 < line.Length &&
                            line[i + 1] == (byte)'\'')
                        {
                            i++;
                            current.Add((byte)'\'');
                        }
                        else if (c == (byte)'\'')
                        {
                            if (i + 1 < line.Length && !IsSpace(line[i + 1]))
                                return false;

                            done = true;
                        }
                        else
                        {
                            current.Add(c);
                        }
                    }
                    else
                    {
                        if (i >= line.Length)
                        {
                            done = true;
                            break;
                        }

                        var c = line[i];

                        if (IsSpace(c))
                            done = true;
                        else if (c == (byte)'"')
                            inDouble = true;
                        else if (c == (byte)'\'')
                            inSingle = true;
                        else
                            current.Add(c);
                    }

                    if (i < line.Length)
                        i++;
                }

                parts.Add(current.ToArray());
            }
        }

        private static bool IsSpace(byte c)
            => c == (byte)' ' || c == (byte)'\t' ||
               c == (byte)'\r' || c == (byte)'\n';

        private static bool IsHex(byte c)
            => (c >= '0' && c <= '9') ||
               (c >= 'a' && c <= 'f') ||
               (c >= 'A' && c <= 'F');

        private static int HexValue(byte c)
            => c <= '9' ? c - '0' : (c | 0x20) - 'a' + 10;

        // Parses any RESP2 value — simple strings, errors, integers,
        // bulk strings, and (possibly nested) arrays, including the
        // null forms. Used for replies: the replication handshake,
        // the cluster bus, MIGRATE, and the client-side RespReader.
        public static RespParseStatus TryParseValue(
            ReadOnlySpan<byte> buffer,
            int maxBulkBytes,
            out RespValue? value,
            out int consumed,
            out int needed,
            out string? error)
        {
            var pos = 0;
            needed = 0;

            var status = ParseValue(
                buffer,
                maxBulkBytes,
                ref pos,
                ref needed,
                out value,
                out error,
                depth: 0);

            consumed = status == RespParseStatus.Complete ? pos : 0;
            return status;
        }

        private static RespParseStatus ParseValue(
            ReadOnlySpan<byte> buffer,
            int maxBulkBytes,
            ref int pos,
            ref int needed,
            out RespValue? value,
            out string? error,
            int depth)
        {
            value = null;
            error = null;

            if (pos >= buffer.Length)
                return RespParseStatus.Incomplete;

            if (depth > 32)
            {
                error = "Protocol error: nesting too deep";
                return RespParseStatus.Invalid;
            }

            var type = buffer[pos++];

            switch (type)
            {
                case (byte)'+':
                case (byte)'-':
                {
                    var status = TryReadLine(buffer, ref pos, out var line);

                    if (status == RespParseStatus.Incomplete)
                        return status;

                    if (status == RespParseStatus.Invalid)
                    {
                        error = "Protocol error: line too long";
                        return status;
                    }

                    var text = Encoding.UTF8.GetString(line);

                    value = type == (byte)'+'
                        ? RespValue.Simple(text)
                        : RespValue.Error(text);

                    return RespParseStatus.Complete;
                }

                case (byte)':':
                {
                    var status = TryReadIntegerLine(
                        buffer,
                        ref pos,
                        out var number,
                        out error);

                    if (status == RespParseStatus.Complete)
                        value = RespValue.Integer(number);

                    return status;
                }

                case (byte)'$':
                {
                    var status = TryReadIntegerLine(
                        buffer,
                        ref pos,
                        out var length,
                        out error);

                    if (status != RespParseStatus.Complete)
                        return status;

                    if (length == -1)
                    {
                        value = RespValue.Null();
                        return RespParseStatus.Complete;
                    }

                    if (length < 0 || length > maxBulkBytes)
                    {
                        error = "Protocol error: invalid bulk length";
                        return RespParseStatus.Invalid;
                    }

                    var end = pos + (int)length;

                    if (end + 2 > buffer.Length)
                    {
                        needed = Math.Max(needed, end + 2);
                        return RespParseStatus.Incomplete;
                    }

                    if (buffer[end] != (byte)'\r' ||
                        buffer[end + 1] != (byte)'\n')
                    {
                        error = "Protocol error: expected CRLF";
                        return RespParseStatus.Invalid;
                    }

                    value = RespValue.Bulk(
                        buffer.Slice(pos, (int)length).ToArray());

                    pos = end + 2;
                    return RespParseStatus.Complete;
                }

                case (byte)'*':
                {
                    var status = TryReadIntegerLine(
                        buffer,
                        ref pos,
                        out var count,
                        out error);

                    if (status != RespParseStatus.Complete)
                        return status;

                    if (count == -1)
                    {
                        value = RespValue.NullArray();
                        return RespParseStatus.Complete;
                    }

                    if (count < 0 || count > MaxArrayElements)
                    {
                        error = "Protocol error: invalid multibulk length";
                        return RespParseStatus.Invalid;
                    }

                    var items = new RespValue[count];

                    for (var i = 0; i < count; i++)
                    {
                        status = ParseValue(
                            buffer,
                            maxBulkBytes,
                            ref pos,
                            ref needed,
                            out var item,
                            out error,
                            depth + 1);

                        if (status != RespParseStatus.Complete)
                            return status;

                        items[i] = item!;
                    }

                    value = RespValue.Array(items);
                    return RespParseStatus.Complete;
                }

                default:
                    error =
                        $"Protocol error: unknown RESP type " +
                        $"'{(char)type}'";

                    return RespParseStatus.Invalid;
            }
        }

        private static RespParseStatus TryReadLine(
            ReadOnlySpan<byte> buffer,
            ref int pos,
            out ReadOnlySpan<byte> line)
        {
            line = default;

            var rest = buffer[pos..];
            var cr = rest.IndexOf((byte)'\r');

            if (cr < 0 || cr + 1 >= rest.Length)
            {
                return rest.Length > MaxLineBytes
                    ? RespParseStatus.Invalid
                    : RespParseStatus.Incomplete;
            }

            if (rest[cr + 1] != (byte)'\n')
                return RespParseStatus.Invalid;

            line = rest[..cr];
            pos += cr + 2;
            return RespParseStatus.Complete;
        }

        private static RespParseStatus TryReadIntegerLine(
            ReadOnlySpan<byte> buffer,
            ref int pos,
            out long value,
            out string? error)
        {
            value = 0;
            error = null;

            var start = pos;
            var status = TryReadLine(buffer, ref pos, out var line);

            if (status != RespParseStatus.Complete)
            {
                if (status == RespParseStatus.Invalid)
                    error = "Protocol error: invalid line";

                return status;
            }

            if (!Utf8Parser.TryParse(line, out value, out var used) ||
                used != line.Length)
            {
                pos = start;
                error = "Protocol error: invalid integer";
                return RespParseStatus.Invalid;
            }

            return RespParseStatus.Complete;
        }
    }
}
