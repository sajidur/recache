using System.Buffers;
using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using recache.Protocol;

var host = GetArg(args, "--host", "127.0.0.1")!;
var port = int.Parse(GetArg(args, "--port", "6380")!);
var password = GetArg(args, "--password", null);
var connections = int.Parse(GetArg(args, "--connections", "50")!);
var requestsPerConn = int.Parse(GetArg(args, "--requests", "2000")!);
var pipeline = int.Parse(GetArg(args, "--pipeline", "1")!);
var valueSize = int.Parse(GetArg(args, "--value-size", "64")!);
var keySpace = int.Parse(GetArg(args, "--keyspace", "10000")!);
var tests = (GetArg(args, "--tests", "set,get,incr,lpush,rpop,hset,ping") ?? "")
    .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
    .Select(t => t.ToLowerInvariant())
    .ToArray();

var value = new string('x', valueSize);

Console.WriteLine(
    $"recache benchmark: {host}:{port}  " +
    $"connections={connections}  requests/conn={requestsPerConn}  " +
    $"pipeline={pipeline}  value-size={valueSize}B  keyspace={keySpace}");
Console.WriteLine();

foreach (var test in tests)
{
    Func<int, string[]> factory = test switch
    {
        "set" => i => ["SET", $"key:{i % keySpace}", value],
        "get" => i => ["GET", $"key:{i % keySpace}"],
        "incr" => i => ["INCR", $"counter:{i % keySpace}"],
        "lpush" => _ => ["LPUSH", "mylist", value],
        "rpop" => _ => ["RPOP", "mylist"],
        "hset" => i => ["HSET", "myhash", $"field:{i % keySpace}", value],
        "sadd" => i => ["SADD", "myset", $"member:{i % keySpace}"],
        "mset" => i => ["MSET",
            $"{{t}}k1:{i % keySpace}", value, $"{{t}}k2:{i % keySpace}", value,
            $"{{t}}k3:{i % keySpace}", value, $"{{t}}k4:{i % keySpace}", value,
            $"{{t}}k5:{i % keySpace}", value],
        "ping" => _ => ["PING"],
        _ => throw new ArgumentException($"unknown test '{test}'"),
    };

    Print(await BenchAsync(test.ToUpperInvariant(), factory));
}

async Task<BenchResult> BenchAsync(
    string name,
    Func<int, string[]> commandFactory)
{
    // Connect (and authenticate) every client before the clock starts.
    var clients = await Task.WhenAll(
        Enumerable.Range(0, connections).Select(_ => ConnectAsync()));

    var overallSw = Stopwatch.StartNew();
    var results = await Task.WhenAll(
        clients.Select((c, index) => RunClientAsync(c, index, commandFactory)));
    overallSw.Stop();

    foreach (var client in clients)
        client.Socket.Dispose();

    var totalOps = results.Sum(r => r.Count);
    var allLatencies = results
        .SelectMany(r => r.LatenciesMs)
        .OrderBy(x => x)
        .ToArray();

    return new BenchResult
    {
        Name = name,
        TotalOps = totalOps,
        ElapsedMs = overallSw.Elapsed.TotalMilliseconds,
        OpsPerSec = totalOps / (overallSw.Elapsed.TotalMilliseconds / 1000.0),
        P50 = Percentile(allLatencies, 0.50),
        P95 = Percentile(allLatencies, 0.95),
        P99 = Percentile(allLatencies, 0.99),
        Max = allLatencies.Length > 0 ? allLatencies[^1] : 0,
    };
}

async Task<(Socket Socket, RespReader Reader)> ConnectAsync()
{
    var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
    await socket.ConnectAsync(host, port);

    var stream = new NetworkStream(socket, ownsSocket: false);
    var reader = new RespReader(stream, 64 * 1024 * 1024);

    if (!string.IsNullOrEmpty(password))
    {
        await socket.SendAsync(
            RespSerializer.ToBytes(RespValue.Command("AUTH", password)),
            SocketFlags.None);

        var reply = await reader.ReadAsync(default);

        if (reply is null || reply.IsError)
            throw new InvalidOperationException($"AUTH failed: {reply?.StringValue}");
    }

    return (socket, reader);
}

async Task<(long Count, List<double> LatenciesMs)> RunClientAsync(
    (Socket Socket, RespReader Reader) client,
    int clientIndex,
    Func<int, string[]> commandFactory)
{
    var (socket, reader) = client;
    var latencies = new List<double>(requestsPerConn / pipeline + 1);
    var batch = new ArrayBufferWriter<byte>(4096);
    long errors = 0;

    for (var i = 0; i < requestsPerConn; i += pipeline)
    {
        var batchSize = Math.Min(pipeline, requestsPerConn - i);

        // Like redis-benchmark: the whole pipeline goes out in one
        // write, so the measurement is of the server, not of this
        // client's per-command syscalls.
        batch.ResetWrittenCount();

        for (var j = 0; j < batchSize; j++)
        {
            var n = clientIndex * requestsPerConn + i + j;
            RespSerializer.WriteCommand(batch, commandFactory(n));
        }

        var batchSw = Stopwatch.StartNew();

        var data = batch.WrittenMemory;

        while (!data.IsEmpty)
            data = data[await socket.SendAsync(data, SocketFlags.None)..];

        for (var j = 0; j < batchSize; j++)
        {
            var reply = await reader.ReadAsync(default);

            if (reply is null)
                throw new IOException("server closed the connection");

            if (reply.IsError)
                errors++;
        }

        batchSw.Stop();
        latencies.Add(batchSw.Elapsed.TotalMilliseconds / batchSize);
    }

    if (errors > 0)
        Console.WriteLine($"  ({errors} error replies on one connection)");

    return (requestsPerConn, latencies);
}

static double Percentile(double[] sorted, double p)
{
    if (sorted.Length == 0)
        return 0;

    var idx = (int)Math.Ceiling(p * sorted.Length) - 1;
    return sorted[Math.Clamp(idx, 0, sorted.Length - 1)];
}

static string? GetArg(string[] args, string name, string? def)
{
    var idx = Array.IndexOf(args, name);
    return idx >= 0 && idx + 1 < args.Length ? args[idx + 1] : def;
}

void Print(BenchResult r)
{
    Console.WriteLine(
        $"{r.Name,-6} {r.OpsPerSec,12:N0} ops/sec   " +
        $"p50={r.P50:F3}ms  p95={r.P95:F3}ms  p99={r.P99:F3}ms  " +
        $"max={r.Max:F3}ms  (total {r.TotalOps} ops in {r.ElapsedMs:F0}ms)");
}

sealed class BenchResult
{
    public string Name = "";
    public long TotalOps;
    public double ElapsedMs;
    public double OpsPerSec;
    public double P50, P95, P99, Max;
}
