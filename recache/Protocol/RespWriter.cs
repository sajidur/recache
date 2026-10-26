using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace recache.Protocol
{
    // Stream-based writer for client-side code (the benchmark, the
    // CLUSTER MEET bootstrap, MIGRATE, tests). The server's own
    // connection loop serializes straight into its batched output
    // buffer with RespSerializer instead.
    public sealed class RespWriter
    {
        private readonly Stream _stream;

        // A single connection's writes are always sequential (one
        // write is fully issued, and awaited, before the next is
        // built), so it's safe to reuse one scratch buffer per
        // writer instance. Every value is serialized here in full
        // and sent as exactly one stream write.
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

            RespSerializer.Write(_scratch, value);

            return _stream.WriteAsync(
                _scratch.WrittenMemory,
                cancellationToken);
        }

        // Serializes several values into one write — how a client
        // actually pipelines (one TCP send for the whole batch).
        public ValueTask WriteManyAsync(
            IEnumerable<RespValue> values,
            CancellationToken cancellationToken)
        {
            _scratch.Clear();

            foreach (var value in values)
                RespSerializer.Write(_scratch, value);

            return _stream.WriteAsync(
                _scratch.WrittenMemory,
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
