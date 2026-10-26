using recache.Clustering;
using recache.Protocol;
using recache.Tests.Infrastructure;
using System.Text;

namespace recache.Tests.Integration;

public class ClusterTests
{
    [Fact]
    public async Task Forms_through_gossip_and_routes_by_slot()
    {
        await using var cluster = await TestCluster.StartAsync(masters: 3, replicasPerMaster: 0);

        // Only node 0 ran MEET, and only toward each node once; every
        // node still knows every other one.
        foreach (var node in cluster.Nodes)
        {
            await using var c = await node.ConnectAsync();
            var info = await c.StringAsync("CLUSTER", "INFO");

            Assert.Contains("cluster_known_nodes:3", info);
            Assert.Contains("cluster_size:3", info);
            Assert.Contains("cluster_slots_assigned:16384", info);
        }

        await using var client = cluster.CreateClient();

        for (var i = 0; i < 2000; i++)
            Assert.Equal("OK", (await client.SendAsync("SET", $"key:{i}", $"v{i}")).AsString());

        for (var i = 0; i < 2000; i++)
            Assert.Equal($"v{i}", (await client.SendAsync("GET", $"key:{i}")).AsString());

        // A node that doesn't own a key redirects.
        var slot = HashSlot.Compute("key:0"u8);
        var owner = await cluster.MasterOfSlotAsync(slot);
        var other = cluster.Nodes.First(n => n != owner);

        await using var direct = await other.ConnectAsync();
        var moved = await direct.SendAsync("GET", "key:0");
        Assert.Equal($"MOVED {slot} 127.0.0.1:{owner.Port}", moved.StringValue);

        Assert.StartsWith("CROSSSLOT", (await direct.SendAsync("MGET", "key:0", "key:1")).StringValue);

        // CLUSTER SLOTS / SHARDS describe all three masters.
        var slots = await direct.SendAsync("CLUSTER", "SLOTS");
        Assert.Equal(3, slots.Items!.Count);
        var shards = await direct.SendAsync("CLUSTER", "SHARDS");
        Assert.Equal(3, shards.Items!.Count);
    }

    [Fact]
    public async Task Replicas_serve_reads_after_readonly()
    {
        await using var cluster = await TestCluster.StartAsync(masters: 3, replicasPerMaster: 1);
        await using var client = cluster.CreateClient();

        Assert.Equal("OK", (await client.SendAsync("SET", "readme", "v")).AsString());
        Assert.Equal(1, (await client.SendAsync("WAIT", "1", "5000")).IntegerValue);

        var master = await cluster.MasterOfSlotAsync(HashSlot.Compute("readme"u8));
        var replica = await cluster.ReplicaOfAsync(master);

        await using var c = await replica.ConnectAsync();

        Assert.StartsWith("MOVED", (await c.SendAsync("GET", "readme")).StringValue);

        await c.OkAsync("READONLY");
        Assert.Equal("v", await c.StringAsync("GET", "readme"));

        // Writes still go to the master.
        Assert.StartsWith("MOVED", (await c.SendAsync("SET", "readme", "x")).StringValue);
    }

    [Fact]
    public async Task Live_resharding_moves_a_slot_with_migrate()
    {
        await using var cluster = await TestCluster.StartAsync(masters: 3, replicasPerMaster: 0);
        await using var client = cluster.CreateClient();

        // Several keys in one slot, via a hash tag.
        for (var i = 0; i < 50; i++)
            await client.SendAsync("SET", $"{{moving}}:{i}", $"v{i}");

        await client.SendAsync("RPUSH", "{moving}:list", "a", "b");

        var slot = HashSlot.Compute("moving"u8);
        var source = await cluster.MasterOfSlotAsync(slot);
        var target = cluster.Nodes.First(n => n != source);
        var sourceId = await cluster.NodeIdAsync(source);
        var targetId = await cluster.NodeIdAsync(target);

        await using var src = await source.ConnectAsync();
        await using var dst = await target.ConnectAsync();

        // The redis-cli --cluster reshard sequence.
        await dst.OkAsync("CLUSTER", "SETSLOT", slot.ToString(), "IMPORTING", sourceId);
        await src.OkAsync("CLUSTER", "SETSLOT", slot.ToString(), "MIGRATING", targetId);

        // Move half first: the rest of the slot is then served via ASK.
        var firstBatch = await src.SendAsync("CLUSTER", "GETKEYSINSLOT", slot.ToString(), "25");
        Assert.Equal(25, firstBatch.Items!.Count);

        var migrate = new List<string> { "MIGRATE", "127.0.0.1", target.Port.ToString(), "", "0", "5000", "KEYS" };
        migrate.AddRange(firstBatch.Items.Select(k => k.AsString()!));
        Assert.Equal("OK", (await src.SendAsync(migrate.ToArray())).AsString());

        var movedKey = firstBatch.Items[0].AsString()!;
        var ask = await src.SendAsync("GET", movedKey);
        Assert.Equal($"ASK {slot} 127.0.0.1:{target.Port}", ask.StringValue);

        // Mid-migration reads work through the cluster client (ASK).
        for (var i = 0; i < 50; i++)
            Assert.Equal($"v{i}", (await client.SendAsync("GET", $"{{moving}}:{i}")).AsString());

        while (true)
        {
            var batch = await src.SendAsync("CLUSTER", "GETKEYSINSLOT", slot.ToString(), "10");

            if (batch.Items!.Count == 0)
                break;

            var rest = new List<string> { "MIGRATE", "127.0.0.1", target.Port.ToString(), "", "0", "5000", "KEYS" };
            rest.AddRange(batch.Items.Select(k => k.AsString()!));
            Assert.Equal("OK", (await src.SendAsync(rest.ToArray())).AsString());
        }

        await dst.OkAsync("CLUSTER", "SETSLOT", slot.ToString(), "NODE", targetId);
        await src.OkAsync("CLUSTER", "SETSLOT", slot.ToString(), "NODE", targetId);

        // The third node learns the new owner through gossip alone.
        var third = cluster.Nodes.Single(n => n != source && n != target);

        await Eventually.WaitAsync(
            async () => (await cluster.NodesViewAsync(third))
                .Single(l => l.Id == targetId)
                .OwnsSlot(slot),
            TimeSpan.FromSeconds(10),
            "the third node to learn the slot's new owner");

        await using var thirdClient = await third.ConnectAsync();
        Assert.Equal($"MOVED {slot} 127.0.0.1:{target.Port}", (await thirdClient.SendAsync("GET", "{moving}:0")).StringValue);

        Assert.Equal(0, await src.IntAsync("CLUSTER", "COUNTKEYSINSLOT", slot.ToString()));
        Assert.Equal(51, await dst.IntAsync("CLUSTER", "COUNTKEYSINSLOT", slot.ToString()));
        Assert.Equal(new[] { "a", "b" }, (await dst.SendAsync("LRANGE", "{moving}:list", "0", "-1")).Items!.Select(i => i.AsString()));
    }
}
