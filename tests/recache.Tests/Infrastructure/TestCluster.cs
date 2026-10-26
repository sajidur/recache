using recache.Clustering;
using System.Text;

namespace recache.Tests.Infrastructure;

// A real multi-process cluster, formed the way an operator would:
// slots assigned with ADDSLOTSRANGE, a single node MEETs the others
// (gossip introduces everyone else), replicas attached with CLUSTER
// REPLICATE.
public sealed class TestCluster : IAsyncDisposable
{
    private TestCluster(List<ServerProcess> nodes, int masters)
    {
        Nodes = nodes;
        MasterCount = masters;
    }

    public List<ServerProcess> Nodes { get; }

    public int MasterCount { get; }

    public IEnumerable<int> Ports => Nodes.Select(n => n.Port);

    public static async Task<TestCluster> StartAsync(
        int masters,
        int replicasPerMaster,
        int nodeTimeoutMs = 1500,
        IDictionary<string, string>? extraSettings = null)
    {
        var settings = new Dictionary<string, string>
        {
            ["ClusterEnabled"] = "true",
            ["ClusterNodeTimeoutMs"] = nodeTimeoutMs.ToString(),
        };

        foreach (var (key, value) in extraSettings ?? new Dictionary<string, string>())
            settings[key] = value;

        var total = masters * (1 + replicasPerMaster);
        var nodes = (await Task.WhenAll(
            Enumerable.Range(0, total).Select(i => ServerProcess.StartAsync($"cluster-{i}", settings))))
            .ToList();

        var cluster = new TestCluster(nodes, masters);

        // Even slot split across the masters.
        for (var m = 0; m < masters; m++)
        {
            var start = HashSlot.SlotCount * m / masters;
            var end = HashSlot.SlotCount * (m + 1) / masters - 1;

            await using var client = await nodes[m].ConnectAsync();
            await client.OkAsync("CLUSTER", "ADDSLOTSRANGE", start.ToString(), end.ToString());
        }

        await using (var first = await nodes[0].ConnectAsync())
        {
            foreach (var node in nodes.Skip(1))
                await first.OkAsync("CLUSTER", "MEET", "127.0.0.1", node.Port.ToString());
        }

        await cluster.WaitForKnownNodesAsync(total);

        for (var r = masters; r < total; r++)
        {
            var master = nodes[(r - masters) % masters];
            var masterId = await cluster.NodeIdAsync(master);

            await using var replica = await nodes[r].ConnectAsync();
            await replica.OkAsync("CLUSTER", "REPLICATE", masterId);
        }

        await cluster.WaitForHealthyAsync();
        return cluster;
    }

    public async Task<string> NodeIdAsync(ServerProcess node)
    {
        await using var client = await node.ConnectAsync();
        return (await client.StringAsync("CLUSTER", "MYID"))!;
    }

    public async Task<List<NodeLine>> NodesViewAsync(ServerProcess node)
    {
        await using var client = await node.ConnectAsync();
        var text = await client.StringAsync("CLUSTER", "NODES") ?? "";

        return text
            .Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Select(NodeLine.Parse)
            .ToList();
    }

    public async Task WaitForKnownNodesAsync(int count)
    {
        await Eventually.WaitAsync(
            async () =>
            {
                foreach (var node in Nodes.Where(n => n.IsRunning))
                {
                    if ((await NodesViewAsync(node)).Count != count)
                        return false;
                }

                return true;
            },
            TimeSpan.FromSeconds(30),
            $"every node to know all {count} nodes");
    }

    // Every running node reports cluster_state:ok and agrees on who
    // owns what and who replicates whom.
    public async Task WaitForHealthyAsync(TimeSpan? timeout = null)
    {
        await Eventually.WaitAsync(
            async () =>
            {
                string? signature = null;

                foreach (var node in Nodes.Where(n => n.IsRunning))
                {
                    await using var client = await node.ConnectAsync();
                    var info = await client.StringAsync("CLUSTER", "INFO") ?? "";

                    if (!info.Contains("cluster_state:ok"))
                        return false;

                    var view = await NodesViewAsync(node);

                    var current = string.Join(
                        "|",
                        view
                            .Where(l => !l.Failed)
                            .OrderBy(l => l.Id)
                            .Select(l => $"{l.Id}:{l.Role}:{l.MasterId}:{l.Slots}"));

                    if (signature is not null && signature != current)
                        return false;

                    signature = current;
                }

                return true;
            },
            timeout ?? TimeSpan.FromSeconds(30),
            "cluster to be healthy and consistent");
    }

    public async Task<ServerProcess> MasterOfSlotAsync(int slot)
    {
        var view = await NodesViewAsync(Nodes.First(n => n.IsRunning));
        var owner = view.Single(l => l.Role == "master" && !l.Failed && l.OwnsSlot(slot));
        return Nodes.Single(n => n.Port == owner.Port);
    }

    public async Task<ServerProcess> ReplicaOfAsync(ServerProcess master)
    {
        var masterId = await NodeIdAsync(master);
        var view = await NodesViewAsync(master);
        var replica = view.First(l => l.MasterId == masterId && !l.Failed);
        return Nodes.Single(n => n.Port == replica.Port);
    }

    public ClusterTestClient CreateClient()
        => new(Nodes.Where(n => n.IsRunning).Select(n => n.Port));

    // A key that hashes into one of `master`'s slots.
    public static string KeyForSlotRange(int start, int end, int index)
    {
        for (var n = 0; ; n++)
        {
            var key = $"k{index}:{n}";
            var slot = HashSlot.Compute(Encoding.UTF8.GetBytes(key));

            if (slot >= start && slot <= end)
                return key;
        }
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var node in Nodes)
            await node.DisposeAsync();
    }

    public sealed record NodeLine(
        string Id,
        string Host,
        int Port,
        string Role,
        string MasterId,
        bool Failed,
        bool Myself,
        string Slots)
    {
        public bool OwnsSlot(int slot)
        {
            foreach (var range in Slots.Split(' ', StringSplitOptions.RemoveEmptyEntries))
            {
                if (range.StartsWith('['))
                    continue;

                var parts = range.Split('-');
                var start = int.Parse(parts[0]);
                var end = parts.Length > 1 ? int.Parse(parts[1]) : start;

                if (slot >= start && slot <= end)
                    return true;
            }

            return false;
        }

        public static NodeLine Parse(string line)
        {
            var fields = line.Split(' ');
            var address = fields[1].Split('@')[0];
            var colon = address.LastIndexOf(':');
            var flags = fields[2].Split(',');

            return new NodeLine(
                fields[0],
                address[..colon],
                int.Parse(address[(colon + 1)..]),
                flags.Contains("master") ? "master" : "replica",
                fields[3],
                flags.Contains("fail") || flags.Contains("fail?"),
                flags.Contains("myself"),
                string.Join(' ', fields.Skip(8).Where(f => !f.StartsWith('['))));
        }
    }
}
