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

var value = new string('x', valueSize);

Console.WriteLine(
    $"recache benchmark: {host}:{port}  " +
    $"connections={connections}  requests/conn={requestsPerConn}  " +
    $"pipeline={pipeline}  value-size={valueSize}B");
Console.WriteLine();

var setResult = await BenchAsync("SET", i => ["SET", $"key:{i % 10000}", value]);
Print(setResult);

var getResult = await BenchAsync("GET", i => ["GET", $"key:{i % 10000}"]);
Print(getResult);

var pingResult = await BenchAsync("PING", _ => ["PING"]);
Print(pingResult);

async Task<BenchResult> BenchAsync(
    string name,
    Func<int, string[]> commandFactory)
{
    var tasks = Enumerable.Range(0, connections)
        .Select(_ => RunClientAsync(commandFactory))
        .ToArray();

    var overallSw = Stopwatch.StartNew();
    var results = await Task.WhenAll(tasks);
    overallSw.Stop();

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

async Task<(long Count, List<double> LatenciesMs)> RunClientAsync(
    Func<int, string[]> commandFactory)
{
    using var client = new TcpClient();
    await client.ConnectAsync(host, port);
    client.NoDelay = true;

    var stream = client.GetStream();
    var writer = new RespWriter(stream);
    var reader = new RespReader(stream, 16 * 1024 * 1024);

    if (!string.IsNullOrEmpty(password))
    {
        await writer.WriteAsync(
            RespValue.Array(new[]
            {
                RespValue.Bulk(Encoding.UTF8.GetBytes("AUTH")),
                RespValue.Bulk(Encoding.UTF8.GetBytes(password)),
            }),
            default);

        await reader.ReadAsync(default);
    }

    var latencies = new List<double>(requestsPerConn / pipeline + 1);

    for (var i = 0; i < requestsPerConn; i += pipeline)
    {
        var batchSize = Math.Min(pipeline, requestsPerConn - i);
        var batchSw = Stopwatch.StartNew();

        for (var j = 0; j < batchSize; j++)
        {
            var cmdArgs = commandFactory(i + j);

            var items = cmdArgs
                .Select(a => RespValue.Bulk(Encoding.UTF8.GetBytes(a)))
                .ToArray();

            await writer.WriteAsync(RespValue.Array(items), default);
        }

        for (var j = 0; j < batchSize; j++)
            await reader.ReadAsync(default);

        batchSw.Stop();
        latencies.Add(batchSw.Elapsed.TotalMilliseconds / batchSize);
    }

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
