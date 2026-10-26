using recache.Protocol;
using recache.Tests.Infrastructure;
using System.Text;

namespace recache.Tests.Integration;

public class AofTests
{
    private static Dictionary<string, string> Aof(string fsync = "always") => new()
    {
        ["EnableAof"] = "true",
        ["AofFsync"] = fsync,
    };

    private static async Task WriteMixedDataAsync(TestClient c)
    {
        await c.OkAsync("SET", "str", "hello");
        await c.SendAsync("INCR", "counter");
        await c.SendAsync("INCRBY", "counter", "41");
        await c.SendAsync("HSET", "hash", "f1", "v1", "f2", "v2");
        await c.SendAsync("RPUSH", "list", "a", "b", "c");
        await c.SendAsync("LPOP", "list");
        await c.SendAsync("SADD", "set", "x", "y");
        await c.SendAsync("ZADD", "zset", "1", "one", "2.5", "two");
        await c.SendAsync("JSON.SET", "doc", "$", "{\"n\":1,\"tags\":[\"a\"]}");
        await c.SendAsync("JSON.NUMINCRBY", "doc", "$.n", "5");
        await c.OkAsync("SET", "ttl", "v", "EX", "1000");
        await c.OkAsync("SET", "gone", "v", "PX", "300");
        await c.OkAsync("MULTI");
        await c.SendAsync("SET", "tx", "1");
        await c.SendAsync("APPEND", "tx", "23");
        await c.SendAsync("EXEC");
        await c.SendAsync("DEL", "str2");
    }

    private static async Task AssertMixedDataAsync(TestClient c, long expectedTtlAt)
    {
        Assert.Equal("hello", await c.StringAsync("GET", "str"));
        Assert.Equal("42", await c.StringAsync("GET", "counter"));
        Assert.Equal("v2", await c.StringAsync("HGET", "hash", "f2"));
        Assert.Equal(new[] { "b", "c" }, (await c.SendAsync("LRANGE", "list", "0", "-1")).Items!.Select(i => i.AsString()));
        Assert.Equal(1, await c.IntAsync("SISMEMBER", "set", "y"));
        Assert.Equal("2.5", await c.StringAsync("ZSCORE", "zset", "two"));
        Assert.Equal("6", await c.StringAsync("JSON.GET", "doc", "$.n"));
        Assert.Equal("123", await c.StringAsync("GET", "tx"));

        // The TTL survived as the same absolute instant, not "1000s
        // from whenever the AOF was replayed".
        Assert.Equal(expectedTtlAt, await c.IntAsync("PEXPIRETIME", "ttl"));

        // Expired while the server was down.
        Assert.Equal(0, await c.IntAsync("EXISTS", "gone"));
    }

    [Fact]
    public async Task Survives_a_hard_kill_with_every_data_type()
    {
        await using var server = await ServerProcess.StartAsync("aof-kill", Aof());

        long ttlAt;

        await using (var c = await server.ConnectAsync())
        {
            await WriteMixedDataAsync(c);
            ttlAt = await c.IntAsync("PEXPIRETIME", "ttl");
        }

        server.Kill();
        await Task.Delay(400);
        await server.RestartAsync();

        await using var check = await server.ConnectAsync();
        await AssertMixedDataAsync(check, ttlAt);
    }

    [Fact]
    public async Task Everysec_keeps_writes_older_than_a_second()
    {
        await using var server = await ServerProcess.StartAsync("aof-everysec", Aof("everysec"));

        await using (var c = await server.ConnectAsync())
        {
            await c.PipelineAsync(Enumerable.Range(0, 10_000).Select(i => new[] { "SET", $"k{i}", $"v{i}" }));
        }

        await Task.Delay(2000);
        server.Kill();
        await server.RestartAsync();

        await using var check = await server.ConnectAsync();
        Assert.Equal(10_000, await check.IntAsync("DBSIZE"));
        Assert.Equal("v9999", await check.StringAsync("GET", "k9999"));
    }

    [Fact]
    public async Task Repairs_a_torn_final_write()
    {
        await using var server = await ServerProcess.StartAsync("aof-torn", Aof());

        await using (var c = await server.ConnectAsync())
        {
            await c.OkAsync("SET", "a", "1");
            await c.OkAsync("SET", "b", "2");
        }

        server.Kill();

        var intact = new FileInfo(server.AofPath).Length;

        // A command cut off mid-value, and an unterminated MULTI: what
        // a crash in the middle of an append leaves behind.
        await File.AppendAllTextAsync(server.AofPath, "*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1\r\n3\r\n*3\r\n$3\r\nSET\r\n$1\r\nd\r\n$10\r\nabc");

        await server.RestartAsync();

        await using var check = await server.ConnectAsync();
        Assert.Equal("1", await check.StringAsync("GET", "a"));
        Assert.Equal("2", await check.StringAsync("GET", "b"));
        Assert.Equal(0, await check.IntAsync("EXISTS", "c"));
        Assert.Equal(0, await check.IntAsync("EXISTS", "d"));

        // The torn tail was cut off, and new appends work after it.
        await check.OkAsync("SET", "e", "5");
        server.Kill();
        Assert.True(new FileInfo(server.AofPath).Length > intact);

        await server.RestartAsync();
        await using var again = await server.ConnectAsync();
        Assert.Equal("5", await again.StringAsync("GET", "e"));
    }

    [Fact]
    public async Task Rewrite_under_concurrent_writes_loses_nothing()
    {
        await using var server = await ServerProcess.StartAsync("aof-rewrite", Aof());

        await using (var c = await server.ConnectAsync())
        {
            // Lots of history for the rewrite to compact.
            for (var round = 0; round < 5; round++)
                await c.PipelineAsync(Enumerable.Range(0, 5000).Select(i => new[] { "INCR", $"n{i}" }));
        }

        var before = new FileInfo(server.AofPath).Length;

        using var stop = new CancellationTokenSource();

        var writers = Enumerable.Range(0, 4).Select(w => Task.Run(async () =>
        {
            await using var c = await server.ConnectAsync();
            var i = 0;

            while (!stop.IsCancellationRequested)
            {
                await c.SendAsync("INCR", $"n{i % 5000}");
                await c.SendAsync("RPUSH", $"log{w}", (i++).ToString());
            }

            return i;
        })).ToArray();

        await Task.Delay(300);

        await using (var admin = await server.ConnectAsync())
        {
            Assert.StartsWith("Background", await admin.StringAsync("BGREWRITEAOF"));

            await Eventually.WaitAsync(
                async () => (await admin.InfoAsync("persistence"))["aof_rewrite_in_progress"] == "0",
                TimeSpan.FromSeconds(30),
                "rewrite to finish");

            Assert.Equal("ok", (await admin.InfoAsync("persistence"))["aof_last_bgrewrite_status"]);
        }

        await Task.Delay(300);
        stop.Cancel();
        var counts = await Task.WhenAll(writers);

        Dictionary<string, string> expected;

        await using (var c = await server.ConnectAsync())
            expected = await DumpAllAsync(c);

        server.Kill();
        await server.RestartAsync();

        await using var check = await server.ConnectAsync();
        var actual = await DumpAllAsync(check);

        Assert.Equal(expected.Count, actual.Count);
        Assert.Equal(expected, actual);

        for (var w = 0; w < counts.Length; w++)
            Assert.Equal(counts[w].ToString(), actual[$"log{w}"].Split(',').Length.ToString());

        Assert.True(new FileInfo(server.AofPath).Length < before * 2, "rewrite should have compacted the file");
    }

    private static async Task<Dictionary<string, string>> DumpAllAsync(TestClient c)
    {
        var result = new Dictionary<string, string>();
        var keys = await c.SendAsync("KEYS", "*");

        foreach (var key in keys.Items!.Select(k => k.AsString()!))
        {
            var type = await c.StringAsync("TYPE", key);

            result[key] = type switch
            {
                "string" => (await c.StringAsync("GET", key))!,
                "list" => string.Join(",", (await c.SendAsync("LRANGE", key, "0", "-1")).Items!.Select(i => i.AsString())),
                _ => type!,
            };
        }

        return result;
    }
}
