using recache.Protocol;
using recache.Tests.Infrastructure;

namespace recache.Tests.Integration;

public class ReplicationTests
{
    private static async Task WaitForSyncAsync(TestClient master, TestClient replica, string key, string? value)
    {
        await Eventually.WaitAsync(
            async () => await replica.StringAsync("GET", key) == value,
            TimeSpan.FromSeconds(20),
            $"replica to see {key}={value ?? "(nil)"}");
    }

    private static async Task WaitForLinkAsync(TestClient replica)
    {
        await Eventually.WaitAsync(
            async () => (await replica.InfoAsync("replication")).GetValueOrDefault("master_link_status") == "up",
            TimeSpan.FromSeconds(20),
            "replication link up");
    }

    [Fact]
    public async Task Replica_full_syncs_then_follows_the_live_stream()
    {
        await using var masterServer = await ServerProcess.StartAsync("repl-master");
        await using var replicaServer = await ServerProcess.StartAsync("repl-replica");

        await using var master = await masterServer.ConnectAsync();
        await using var replica = await replicaServer.ConnectAsync();

        // Existing data arrives through the snapshot...
        await master.PipelineAsync(Enumerable.Range(0, 20_000).Select(i => new[] { "SET", $"k{i}", $"v{i}" }));
        await master.SendAsync("HSET", "h", "f", "v");
        await master.SendAsync("RPUSH", "l", "1", "2", "3");
        await master.OkAsync("SET", "ttl", "x", "EX", "1000");

        await replica.OkAsync("REPLICAOF", "127.0.0.1", masterServer.Port.ToString());
        await WaitForLinkAsync(replica);
        await WaitForSyncAsync(master, replica, "k19999", "v19999");

        // ...and new writes through the stream.
        await master.SendAsync("INCR", "counter");
        await master.SendAsync("LPOP", "l");
        await master.SendAsync("DEL", "k0");

        await WaitForSyncAsync(master, replica, "k0", null);
        Assert.Equal("1", await replica.StringAsync("GET", "counter"));
        Assert.Equal(new[] { "2", "3" }, (await replica.SendAsync("LRANGE", "l", "0", "-1")).Items!.Select(i => i.AsString()));
        Assert.Equal("v", await replica.StringAsync("HGET", "h", "f"));
        Assert.Equal(await master.IntAsync("PEXPIRETIME", "ttl"), await replica.IntAsync("PEXPIRETIME", "ttl"));

        // Replicas are read-only.
        Assert.StartsWith("READONLY", (await replica.SendAsync("SET", "x", "1")).StringValue);

        // Byte-identical streams: equal offsets once idle.
        await Eventually.WaitAsync(
            async () =>
                (await master.InfoAsync("replication"))["master_repl_offset"] ==
                (await replica.InfoAsync("replication"))["master_repl_offset"],
            TimeSpan.FromSeconds(10),
            "offsets to match");

        var role = await replica.SendAsync("ROLE");
        Assert.Equal("slave", role.Items![0].AsString());
    }

    [Fact]
    public async Task Wait_confirms_replication_of_a_clients_writes()
    {
        await using var masterServer = await ServerProcess.StartAsync("wait-master");
        await using var replicaServer = await ServerProcess.StartAsync(
            "wait-replica",
            new Dictionary<string, string> { ["ReplicaOf"] = $"127.0.0.1 {masterServer.Port}" });

        // Configured as a replica from startup (Recache:ReplicaOf).
        await using var replica = await replicaServer.ConnectAsync();
        await WaitForLinkAsync(replica);

        await using var master = await masterServer.ConnectAsync();

        for (var i = 0; i < 50; i++)
        {
            await master.OkAsync("SET", $"w{i}", "v");
            Assert.Equal(1, await master.IntAsync("WAIT", "1", "5000"));

            // Acknowledged by WAIT means it's on the replica now.
            Assert.Equal("v", await replica.StringAsync("GET", $"w{i}"));
        }

        Assert.Equal(1, await master.IntAsync("WAIT", "1", "100"));

        // Asking for more replicas than exist times out with the count.
        Assert.Equal(1, await master.IntAsync("WAIT", "2", "200"));
    }

    [Fact]
    public async Task A_dropped_link_resumes_with_a_partial_resync()
    {
        await using var masterServer = await ServerProcess.StartAsync("psync-master");
        await using var replicaServer = await ServerProcess.StartAsync("psync-replica");

        await using var master = await masterServer.ConnectAsync();
        await using var replica = await replicaServer.ConnectAsync();

        await master.PipelineAsync(Enumerable.Range(0, 1000).Select(i => new[] { "SET", $"k{i}", "v" }));
        await replica.OkAsync("REPLICAOF", "127.0.0.1", masterServer.Port.ToString());
        await WaitForSyncAsync(master, replica, "k999", "v");

        Assert.Equal("1", (await master.InfoAsync("stats"))["sync_full"]);

        for (var round = 0; round < 3; round++)
        {
            Assert.Equal(1, await replica.IntAsync("CLIENT", "KILL", "TYPE", "master"));

            // Writes while the link is down are caught up from the
            // backlog after reconnecting.
            await master.PipelineAsync(Enumerable.Range(0, 500).Select(i => new[] { "INCR", $"r{round}" }));
            await WaitForSyncAsync(master, replica, $"r{round}", "500");
        }

        var stats = await master.InfoAsync("stats");
        Assert.Equal("1", stats["sync_full"]);
        Assert.True(int.Parse(stats["sync_partial_ok"]) >= 3);
    }

    [Fact]
    public async Task Promoted_replica_lets_its_sibling_continue_without_a_full_resync()
    {
        await using var aServer = await ServerProcess.StartAsync("psync2-a");
        await using var bServer = await ServerProcess.StartAsync("psync2-b");
        await using var cServer = await ServerProcess.StartAsync("psync2-c");

        await using var a = await aServer.ConnectAsync();
        await using var b = await bServer.ConnectAsync();
        await using var c = await cServer.ConnectAsync();

        await b.OkAsync("REPLICAOF", "127.0.0.1", aServer.Port.ToString());
        await c.OkAsync("REPLICAOF", "127.0.0.1", aServer.Port.ToString());

        await a.PipelineAsync(Enumerable.Range(0, 1000).Select(i => new[] { "SET", $"k{i}", "v" }));
        await WaitForSyncAsync(a, b, "k999", "v");
        await WaitForSyncAsync(a, c, "k999", "v");

        // Fail over by hand: B becomes the master, C follows B.
        await b.OkAsync("REPLICAOF", "NO", "ONE");
        await c.OkAsync("REPLICAOF", "127.0.0.1", bServer.Port.ToString());
        await b.OkAsync("SET", "after", "failover");
        await WaitForSyncAsync(b, c, "after", "failover");

        var stats = await b.InfoAsync("stats");
        Assert.Equal("0", stats["sync_full"]);
        Assert.Equal("1", stats["sync_partial_ok"]);
    }

    [Fact]
    public async Task Chained_replicas_receive_the_masters_writes()
    {
        await using var masterServer = await ServerProcess.StartAsync("chain-master");
        await using var midServer = await ServerProcess.StartAsync("chain-mid");
        await using var leafServer = await ServerProcess.StartAsync("chain-leaf");

        await using var master = await masterServer.ConnectAsync();
        await using var mid = await midServer.ConnectAsync();
        await using var leaf = await leafServer.ConnectAsync();

        await mid.OkAsync("REPLICAOF", "127.0.0.1", masterServer.Port.ToString());
        await WaitForLinkAsync(mid);
        await leaf.OkAsync("REPLICAOF", "127.0.0.1", midServer.Port.ToString());
        await WaitForLinkAsync(leaf);

        await master.OkAsync("MULTI");
        await master.SendAsync("SET", "chained", "1");
        await master.SendAsync("INCR", "chained");
        await master.SendAsync("EXEC");

        await WaitForSyncAsync(master, leaf, "chained", "2");
    }

    [Fact]
    public async Task Expiry_on_the_master_reaches_replicas_as_a_delete()
    {
        await using var masterServer = await ServerProcess.StartAsync("expire-master");
        await using var replicaServer = await ServerProcess.StartAsync("expire-replica");

        await using var master = await masterServer.ConnectAsync();
        await using var replica = await replicaServer.ConnectAsync();

        await replica.OkAsync("REPLICAOF", "127.0.0.1", masterServer.Port.ToString());
        await WaitForLinkAsync(replica);

        await master.OkAsync("SET", "short", "v", "PX", "300");
        await WaitForSyncAsync(master, replica, "short", "v");

        // The replica hides it once expired, and the master's active
        // expiry cycle deletes it (and propagates the DEL) without
        // anyone reading it on the master.
        await Eventually.WaitAsync(
            async () => await replica.IntAsync("DBSIZE") == 0,
            TimeSpan.FromSeconds(10),
            "the expired key to be deleted on the replica");
    }
}
