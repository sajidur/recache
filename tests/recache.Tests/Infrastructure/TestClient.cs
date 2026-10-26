using recache.Protocol;
using System.Net.Sockets;

namespace recache.Tests.Infrastructure;

// Minimal RESP client for tests, built on the server's own protocol
// classes.
public sealed class TestClient : IAsyncDisposable
{
    private readonly Socket _socket;
    private readonly NetworkStream _stream;
    private readonly RespReader _reader;
    private readonly RespWriter _writer;

    private TestClient(Socket socket, string endpoint)
    {
        _socket = socket;
        _stream = new NetworkStream(socket, ownsSocket: true);
        _reader = new RespReader(_stream, 512 * 1024 * 1024);
        _writer = new RespWriter(_stream);
        Endpoint = endpoint;
    }

    public string Endpoint { get; }

    public static async Task<TestClient> ConnectAsync(int port, string host = "127.0.0.1")
    {
        var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };

        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await socket.ConnectAsync(host, port, timeout.Token);

        return new TestClient(socket, $"{host}:{port}");
    }

    public async Task<RespValue> SendAsync(params string[] args)
    {
        await _writer.WriteAsync(RespValue.Command(args), default);
        return await ReadAsync();
    }

    public async Task<RespValue> SendAsync(IReadOnlyList<RespValue> args)
    {
        await _writer.WriteAsync(RespValue.Array(args), default);
        return await ReadAsync();
    }

    public async Task<RespValue[]> PipelineAsync(IEnumerable<string[]> commands)
    {
        var list = commands.Select(c => RespValue.Command(c)).ToList();

        await _writer.WriteManyAsync(list, default);

        var replies = new RespValue[list.Count];

        for (var i = 0; i < replies.Length; i++)
            replies[i] = await ReadAsync();

        return replies;
    }

    public async Task SendRawAsync(byte[] data)
        => await _stream.WriteAsync(data);

    public async Task<RespValue> ReadAsync(TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(30));

        return await _reader.ReadAsync(cts.Token)
            ?? throw new IOException($"{Endpoint} closed the connection");
    }

    public async Task<string?> StringAsync(params string[] args)
    {
        var reply = await SendAsync(args);

        if (reply.IsError)
            throw new InvalidOperationException($"{string.Join(' ', args)} -> {reply.StringValue}");

        return reply.Type is RespType.Null ? null : reply.AsString();
    }

    public async Task<long> IntAsync(params string[] args)
    {
        var reply = await SendAsync(args);

        if (reply.Type != RespType.Integer)
            throw new InvalidOperationException($"{string.Join(' ', args)} -> {reply.Type} {reply.AsString()}");

        return reply.IntegerValue;
    }

    public async Task OkAsync(params string[] args)
    {
        var reply = await SendAsync(args);

        if (reply.IsError || reply.AsString() != "OK")
            throw new InvalidOperationException($"{string.Join(' ', args)} -> {reply.AsString()}");
    }

    // INFO <section> parsed into key/value pairs.
    public async Task<Dictionary<string, string>> InfoAsync(string section = "all")
    {
        var text = await StringAsync("INFO", section) ?? "";

        return text
            .Split("\r\n", StringSplitOptions.RemoveEmptyEntries)
            .Where(l => !l.StartsWith('#') && l.Contains(':'))
            .Select(l => l.Split(':', 2))
            .GroupBy(p => p[0])
            .ToDictionary(g => g.Key, g => g.First()[1]);
    }

    public ValueTask DisposeAsync()
    {
        _stream.Dispose();
        return ValueTask.CompletedTask;
    }
}
