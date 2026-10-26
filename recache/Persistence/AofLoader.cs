using recache.Protocol;
using System;
using System.Collections.Generic;
using System.IO;

namespace recache.Persistence
{
    public sealed class AofCorruptException : Exception
    {
        public AofCorruptException(string path, long offset, string detail)
            : base(
                $"AOF file '{path}' is corrupt at byte {offset}: {detail}. " +
                "Fix or truncate the file at that offset (everything " +
                "before it is intact), or move it aside to start empty.")
        {
            Offset = offset;
        }

        public long Offset { get; }
    }

    public sealed record AofLoadResult(
        long Commands,
        long Bytes,
        bool Truncated,
        long DiscardedBytes);

    // `raw` is the command's exact bytes in the file.
    public delegate void ReplayCommand(
        IReadOnlyList<RespValue> args,
        ReadOnlySpan<byte> raw);

    // Replays a file of RESP commands — the AOF at startup, or a
    // snapshot received during a replica's full resync.
    //
    // A crash can leave the file ending mid-command, or inside a
    // MULTI block whose EXEC was never written. Both are a clean,
    // expected tail: everything up to the last complete command
    // (outside a transaction) is applied, and the remainder is
    // reported — and, for the AOF, truncated away so new appends
    // don't land after garbage. Corruption anywhere else is fatal:
    // silently skipping data in the middle of a log isn't safe.
    public static class AofLoader
    {
        private const int ReadSize = 1024 * 1024;

        public static AofLoadResult Load(
            string path,
            ReplayCommand execute,
            bool allowTruncatedTail,
            bool truncateFile)
        {
            long commands = 0;
            long consumedTotal = 0;
            long safeOffset = 0;
            var inMulti = false;
            var buffer = new byte[ReadSize];
            var start = 0;
            var end = 0;
            var needed = 0;

            using (var file = new FileStream(
                path,
                FileMode.Open,
                FileAccess.Read,
                FileShare.ReadWrite,
                bufferSize: 1))
            {
                while (true)
                {
                    if (end == buffer.Length)
                    {
                        if (start > 0)
                        {
                            Buffer.BlockCopy(buffer, start, buffer, 0, end - start);
                            end -= start;
                            start = 0;
                        }
                        else
                        {
                            Array.Resize(ref buffer, buffer.Length * 2);
                        }
                    }

                    var read = file.Read(buffer, end, buffer.Length - end);

                    if (read == 0)
                        break;

                    end += read;

                    if (end - start < needed)
                        continue;

                    while (start < end)
                    {
                        var status = RespParser.TryParseRequest(
                            buffer.AsSpan(start, end - start),
                            int.MaxValue,
                            out var request,
                            out var consumed,
                            out needed,
                            out var error);

                        if (status == RespParseStatus.Incomplete)
                            break;

                        if (status == RespParseStatus.Invalid)
                            throw new AofCorruptException(path, consumedTotal, error ?? "invalid RESP");

                        var raw = buffer.AsSpan(start, consumed);

                        start += consumed;
                        consumedTotal += consumed;
                        needed = 0;

                        if (request is null)
                            continue;

                        var name = request.Items![0].AsString() ?? "";

                        if (name.Equals("MULTI", StringComparison.OrdinalIgnoreCase))
                            inMulti = true;
                        else if (name.Equals("EXEC", StringComparison.OrdinalIgnoreCase))
                            inMulti = false;

                        execute(request.Items!, raw);
                        commands++;

                        if (!inMulti)
                            safeOffset = consumedTotal;
                    }

                    if (start == end)
                        start = end = 0;
                }
            }

            var fileLength = new FileInfo(path).Length;
            var discarded = fileLength - safeOffset;

            if (discarded == 0)
                return new AofLoadResult(commands, fileLength, false, 0);

            if (!allowTruncatedTail)
            {
                throw new AofCorruptException(
                    path,
                    safeOffset,
                    $"the last {discarded} byte(s) are an incomplete " +
                    "command or an unterminated MULTI block " +
                    "(set AofLoadTruncated: true to drop them automatically)");
            }

            if (truncateFile)
            {
                using var file = new FileStream(
                    path,
                    FileMode.Open,
                    FileAccess.Write,
                    FileShare.Read);

                file.SetLength(safeOffset);
                file.Flush(flushToDisk: true);
            }

            return new AofLoadResult(commands, safeOffset, true, discarded);
        }
    }
}
