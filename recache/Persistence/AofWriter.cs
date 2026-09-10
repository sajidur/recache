using recache.Protocol;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace recache.Persistence
{
    public sealed class AofWriter : IAofWriter
    {
        private readonly SemaphoreSlim _gate = new(1, 1);
        private readonly Stream _stream;
        private readonly bool _ownsStream;
        private readonly string? _path;

        public AofWriter(string path)
            : this(path, createDirectory: true)
        {
        }

        public AofWriter(string path, bool createDirectory)
        {
            if (string.IsNullOrWhiteSpace(path))
                throw new ArgumentException(
                    "AOF path cannot be empty.",
                    nameof(path));

            _path = path;

            var directory = System.IO.Path.GetDirectoryName(path);

            if (!string.IsNullOrEmpty(directory) && createDirectory)
                Directory.CreateDirectory(directory);

            _stream = new FileStream(
                path,
                FileMode.Append,
                FileAccess.Write,
                FileShare.Read);

            _ownsStream = true;
        }

        public AofWriter(Stream stream)
        {
            _stream = stream ?? throw new ArgumentNullException(nameof(stream));
            _path = stream is FileStream fileStream
                ? fileStream.Name
                : null;
            _ownsStream = false;
        }

        public string Path => _path ?? throw new InvalidOperationException(
            "AOF writer is not backed by a file path.");

        public void Append(string value)
        {
            if (value is null)
                throw new ArgumentNullException(nameof(value));

            Append(Encoding.UTF8.GetBytes(value));
        }

        public void Append(byte[] value)
        {
            if (value is null)
                throw new ArgumentNullException(nameof(value));

            Append(new ReadOnlySpan<byte>(value));
        }

        public void Append(ReadOnlySpan<byte> value)
        {
            _gate.Wait();

            try
            {
                _stream.Write(value);
            }
            finally
            {
                _gate.Release();
            }
        }

        public void Append(RespValue value)
        {
            if (value is null)
                throw new ArgumentNullException(nameof(value));

            Append(Serialize(value));
        }

        public void AppendCommand(IEnumerable<string?> arguments)
        {
            if (arguments is null)
                throw new ArgumentNullException(nameof(arguments));

            var items = new List<RespValue>();

            foreach (var argument in arguments)
            {
                if (argument is null)
                {
                    items.Add(RespValue.Null());
                    continue;
                }

                var bytes = Encoding.UTF8.GetBytes(argument);
                items.Add(RespValue.Bulk(bytes));
            }

            Append(RespValue.Array(items));
        }

        public void AppendCommand(params string?[] arguments)
            => AppendCommand((IEnumerable<string?>)arguments);

        public async Task AppendAsync(
            string value,
            CancellationToken cancellationToken = default)
        {
            if (value is null)
                throw new ArgumentNullException(nameof(value));

            await AppendAsync(
                Encoding.UTF8.GetBytes(value),
                cancellationToken)
                .ConfigureAwait(false);
        }

        public async Task AppendAsync(
            byte[] value,
            CancellationToken cancellationToken = default)
        {
            if (value is null)
                throw new ArgumentNullException(nameof(value));

            await AppendAsync(
                new ReadOnlyMemory<byte>(value),
                cancellationToken)
                .ConfigureAwait(false);
        }

        public async Task AppendAsync(
            ReadOnlyMemory<byte> value,
            CancellationToken cancellationToken = default)
        {
            await _gate.WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            try
            {
                await _stream.WriteAsync(
                    value,
                    cancellationToken)
                    .ConfigureAwait(false);
            }
            finally
            {
                _gate.Release();
            }
        }

        public async Task AppendAsync(
            RespValue value,
            CancellationToken cancellationToken = default)
        {
            if (value is null)
                throw new ArgumentNullException(nameof(value));

            await AppendAsync(
                Serialize(value),
                cancellationToken)
                .ConfigureAwait(false);
        }

        public void Flush()
        {
            _gate.Wait();

            try
            {
                _stream.Flush();
            }
            finally
            {
                _gate.Release();
            }
        }

        public async Task FlushAsync(
            CancellationToken cancellationToken = default)
        {
            await _gate.WaitAsync(cancellationToken)
                .ConfigureAwait(false);

            try
            {
                await _stream.FlushAsync(cancellationToken)
                    .ConfigureAwait(false);
            }
            finally
            {
                _gate.Release();
            }
        }

        public void Dispose()
        {
            if (_ownsStream)
            {
                _stream.Dispose();
            }
            else
            {
                _stream.Flush();
            }

            _gate.Dispose();
        }

        public void Write(string value)
            => Append(value);

        public Task WriteAsync(
            string value,
            CancellationToken cancellationToken = default)
            => AppendAsync(value, cancellationToken);

        public void WriteCommand(IEnumerable<string?> arguments)
            => AppendCommand(arguments);

        public void WriteCommand(params string?[] arguments)
            => AppendCommand(arguments);

        public Task WriteCommandAsync(
            IEnumerable<string?> arguments,
            CancellationToken cancellationToken = default)
        {
            return WriteCommandAsyncCore(arguments, cancellationToken);
        }

        public Task WriteCommandAsync(
            CancellationToken cancellationToken = default,
            params string?[] arguments)
        {
            return WriteCommandAsyncCore(arguments, cancellationToken);
        }

        private static byte[] Serialize(RespValue value)
        {
            using var buffer = new MemoryStream();
            var writer = new RespWriter(buffer);

            writer.WriteAsync(
                value,
                CancellationToken.None)
                .AsTask()
                .GetAwaiter()
                .GetResult();

            return buffer.ToArray();
        }

        private async Task WriteCommandAsyncCore(
            IEnumerable<string?> arguments,
            CancellationToken cancellationToken)
        {
            if (arguments is null)
                throw new ArgumentNullException(nameof(arguments));

            var items = new List<RespValue>();

            foreach (var argument in arguments)
            {
                if (argument is null)
                {
                    items.Add(RespValue.Null());
                    continue;
                }

                items.Add(
                    RespValue.Bulk(
                        Encoding.UTF8.GetBytes(argument)));
            }

            await AppendAsync(
                RespValue.Array(items),
                cancellationToken)
                .ConfigureAwait(false);
        }
    }
}
