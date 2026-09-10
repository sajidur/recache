using System;
using recache.Protocol;

namespace recache.Persistence
{
    public interface IAofWriter : IDisposable
    {
        string Path { get; }

        void Append(string value);

        void Append(byte[] value);

        void Append(ReadOnlySpan<byte> value);

        void Append(RespValue value);

        void AppendCommand(IEnumerable<string?> arguments);

        void AppendCommand(params string?[] arguments);

        Task AppendAsync(
            string value,
            CancellationToken cancellationToken = default);

        Task AppendAsync(
            byte[] value,
            CancellationToken cancellationToken = default);

        Task AppendAsync(
            ReadOnlyMemory<byte> value,
            CancellationToken cancellationToken = default);

        Task AppendAsync(
            RespValue value,
            CancellationToken cancellationToken = default);

        void Flush();

        Task FlushAsync(
            CancellationToken cancellationToken = default);
    }
}
