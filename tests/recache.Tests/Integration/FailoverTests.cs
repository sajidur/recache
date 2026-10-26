using recache.Clustering;
using recache.Protocol;
using recache.Tests.Infrastructure;
using System.Collections.Concurrent;
using System.Diagnostics;
using Xunit.Abstractions;

namespace recache.Tests.Integration;

public class FailoverTests
{
    private readonly ITestOutputHelper _output;

    public FailoverTests(ITestOutputHelper output)
    {
        _output = output;
    }

    // Writes unique keys into `master`'s slots as fast as it can and
    // records every write the cluster acknowledged.
    private sealed class Writer
    {
        public readonly ConcurrentDictionary<string, string> Acked = new();
        public readonly ConcurrentQueue<string> Errors = new();
        public double MaxLatencyMs;
        public DateTime MaxLatencyAt;
        public readonly ConcurrentQueue<string> Slow = new();
        public long Count;

        public async Task RunAsync(ClusterTestClient client, int slotStart, int slotEnd, CancellationToken stop)
        {
            var i = 0;

            while (!stop.IsCancellationRequested)
            {
                var key = TestCluster.KeyForSlotRange(slotStart, slotEnd, i);
                var value = $"value-{i}";
                var sw = Stopwatch.StartNew();
                RespValue reply;

                try
                {
                    reply = await client.SendAsync("SET", key, value);
                }
                catch (Exception ex)
                {
                    Errors.Enqueue($"{key}: {ex.Message}");
                    i++;
                    continue;
                }

                if (sw.Elapsed.TotalMilliseconds > MaxLatencyMs)
                {
                    MaxLatencyMs = sw.Elapsed.TotalMilliseconds;
                    MaxLatencyAt = DateTime.Now - sw.Elapsed;
                }

                if (sw.Elapsed.TotalMilliseconds > 50)
                    Slow.Enqueue($"{DateTime.Now - sw.Elapsed:HH:mm:ss.fff} {sw.Elapsed.TotalMilliseconds:F0}ms {key} redirects={client.Redirects} retries={client.Retries}");

                if (reply.IsError)
                    Errors.Enqueue($"{key}: {reply.StringValue}");
                else
                    Acked[key] = value;

                Interlocked.Increment(ref Count);
                i++;
            }
        }
    }

    private static async Task<(int Start, int End)> SlotRangeOfAsync(TestCluster cluster, ServerProcess master)
    {
        var id = await cluster.NodeIdAsync(master);
        var line = (await cluster.NodesViewAsync(master)).Single(l => l.Id == id);
        var first = line.Slots.Split(' ')[0].Split('-');

        return (int.Parse(first[0]), int.Parse(first[^1]));
    }

    private static async Task AssertAllPresentAsync(ClusterTestClient client, IDictionary<string, string> acked)
    {
        var missing = 0;

        foreach (var (key, value) in acked)
        {
            if ((await client.SendAsync("GET", key)).AsString() != value)
                missing++;
        }

        Assert.Equal(0, missing);
    }

    [Fact]
    public async Task Manual_failover_under_load_drops_no_writes_and_loses_none()
    {
        await using var cluster = await TestCluster.StartAsync(masters: 3, replicasPerMaster: 1);

        var master = cluster.Nodes[0];
        var replica = await cluster.ReplicaOfAsync(master);
        var (start, end) = await SlotRangeOfAsync(cluster, master);

        await using var client = cluster.CreateClient();
        await client.RefreshAsync();

        var writer = new Writer();
        using var stop = new CancellationTokenSource();
        var writing = writer.RunAsync(client, start, end, stop.Token);

        await Task.Delay(1000);

        var sw = Stopwatch.StartNew();

        await using (var r = await replica.ConnectAsync())
            await r.OkAsync("CLUSTER", "FAILOVER");

        await Eventually.WaitAsync(
            async () => (await cluster.NodesViewAsync(replica)).Single(l => l.Myself).Role == "master",
            TimeSpan.FromSeconds(10),
            "the replica to take over");

        _output.WriteLine($"Handover took {sw.ElapsedMilliseconds} ms");

        await Task.Delay(1000);
        stop.Cancel();
        await writing;

        _output.WriteLine(
            $"{writer.Count} writes, {writer.Acked.Count} acked, {writer.Errors.Count} errors, " +
            $"max latency {writer.MaxLatencyMs:F1} ms, {client.Redirects} redirects");

        Assert.Empty(writer.Errors);
        Assert.True(writer.Acked.Count > 100);

        await cluster.WaitForHealthyAsync();

        // The old master follows the new one.
        var oldMaster = (await cluster.NodesViewAsync(master)).Single(l => l.Myself);
        Assert.Equal("replica", oldMaster.Role);
        Assert.Equal(await cluster.NodeIdAsync(replica), oldMaster.MasterId);

        await AssertAllPresentAsync(client, writer.Acked);
    }

    [Fact]
    public async Task Stopping_a_master_hands_its_slots_over_first()
    {
        await using var cluster = await TestCluster.StartAsync(masters: 3, replicasPerMaster: 1);

        var master = cluster.Nodes[1];
        var replica = await cluster.ReplicaOfAsync(master);
        var (start, end) = await SlotRangeOfAsync(cluster, master);

        await using var client = cluster.CreateClient();
        await client.RefreshAsync();

        var writer = new Writer();
        using var stop = new CancellationTokenSource();
        var writing = writer.RunAsync(client, start, end, stop.Token);

        await Task.Delay(1000);

        // A clean stop (service stop / Ctrl+C / SHUTDOWN): a rolling
        // restart or upgrade.
        await master.ShutdownAsync();

        await Eventually.WaitAsync(
            async () => (await cluster.NodesViewAsync(replica)).Single(l => l.Myself).Role == "master",
            TimeSpan.FromSeconds(10),
            "the replica to own the slots");

        await Task.Delay(1000);
        stop.Cancel();
        await writing;

        _output.WriteLine(
            $"{writer.Count} writes, {writer.Acked.Count} acked, {writer.Errors.Count} errors, " +
            $"max latency {writer.MaxLatencyMs:F1} ms (at {writer.MaxLatencyAt:HH:mm:ss.fff})");

        _output.WriteLine("Writes slower than 50 ms:\n" + string.Join("\n", writer.Slow));

        Assert.Empty(writer.Errors);
        await AssertAllPresentAsync(client, writer.Acked);

        // Bring it back: it rejoins as a replica of the new master.
        await master.RestartAsync();
        await cluster.WaitForHealthyAsync();

        var rejoined = (await cluster.NodesViewAsync(master)).Single(l => l.Myself);
        Assert.Equal("replica", rejoined.Role);
    }

    [Fact]
    public async Task Crashed_master_is_replaced_automatically()
    {
        await using var cluster = await TestCluster.StartAsync(masters: 3, replicasPerMaster: 1, nodeTimeoutMs: 1500);

        var master = cluster.Nodes[2];
        var replica = await cluster.ReplicaOfAsync(master);
        var (start, end) = await SlotRangeOfAsync(cluster, master);

        await using var client = cluster.CreateClient();
        await client.RefreshAsync();

        // Writes confirmed on the replica with WAIT survive the crash.
        var confirmed = new Dictionary<string, string>();

        for (var i = 0; i < 200; i++)
        {
            var key = TestCluster.KeyForSlotRange(start, end, i);
            Assert.Equal("OK", (await client.SendAsync("SET", key, $"v{i}")).AsString());
            confirmed[key] = $"v{i}";
        }

        Assert.Equal(1, (await client.SendAsync("WAIT", "1", "5000")).IntegerValue);

        var sw = Stopwatch.StartNew();
        master.Kill();

        await Eventually.WaitAsync(
            async () => (await cluster.NodesViewAsync(replica)).Single(l => l.Myself).Role == "master",
            TimeSpan.FromSeconds(20),
            "automatic failover");

        _output.WriteLine($"Replica promoted {sw.ElapsedMilliseconds} ms after the crash");

        await cluster.WaitForHealthyAsync();
        _output.WriteLine($"Cluster healthy {sw.ElapsedMilliseconds} ms after the crash");

        await AssertAllPresentAsync(client, confirmed);

        // Writes to the failed master's slots work again.
        Assert.Equal("OK", (await client.SendAsync("SET", TestCluster.KeyForSlotRange(start, end, 9999), "after")).AsString());

        // The crashed node restarts, learns it was replaced, and
        // becomes a replica of its replacement.
        await master.RestartAsync();

        await Eventually.WaitAsync(
            async () => (await cluster.NodesViewAsync(master)).Single(l => l.Myself).Role == "replica",
            TimeSpan.FromSeconds(20),
            "the old master to rejoin as a replica");

        await cluster.WaitForHealthyAsync();

        await using var old = await master.ConnectAsync();
        await old.OkAsync("READONLY");

        await Eventually.WaitAsync(
            async () => await old.StringAsync("GET", TestCluster.KeyForSlotRange(start, end, 9999)) == "after",
            TimeSpan.FromSeconds(20),
            "the rejoined node to sync from the new master");
    }
}
