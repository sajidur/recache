using recache.Protocol;
using recache.Tests.Infrastructure;
using System.Text;

namespace recache.Tests.Integration;

public class BasicCommandTests : IAsyncLifetime
{
    private ServerProcess _server = null!;

    public async Task InitializeAsync()
        => _server = await ServerProcess.StartAsync("basic");

    public async Task DisposeAsync()
        => await _server.DisposeAsync();

    [Fact]
    public async Task Answers_a_large_pipeline_in_order()
    {
        await using var client = await _server.ConnectAsync();

        var commands = Enumerable.Range(0, 5000)
            .Select(i => i % 2 == 0
                ? new[] { "SET", $"p:{i}", $"v{i}" }
                : new[] { "GET", $"p:{i - 1}" })
            .ToList();

        var replies = await client.PipelineAsync(commands);

        for (var i = 0; i < replies.Length; i++)
        {
            if (i % 2 == 0)
                Assert.Equal("OK", replies[i].AsString());
            else
                Assert.Equal($"v{i - 1}", replies[i].AsString());
        }
    }

    [Fact]
    public async Task Accepts_inline_commands()
    {
        await using var client = await _server.ConnectAsync();

        await client.SendRawAsync("PING\r\nSET inline \"a b\"\r\nGET inline\r\n"u8.ToArray());

        Assert.Equal("PONG", (await client.ReadAsync()).AsString());
        Assert.Equal("OK", (await client.ReadAsync()).AsString());
        Assert.Equal("a b", (await client.ReadAsync()).AsString());
    }

    [Fact]
    public async Task String_and_keyspace_commands()
    {
        await using var c = await _server.ConnectAsync();

        await c.OkAsync("MSET", "m1", "1", "m2", "2");
        var mget = await c.SendAsync("MGET", "m1", "missing", "m2");
        Assert.Equal(new[] { "1", null, "2" }, mget.Items!.Select(i => i.Type == RespType.Null ? null : i.AsString()));

        Assert.Equal(11, await c.IntAsync("INCRBY", "m1", "10"));
        Assert.Equal(9, await c.IntAsync("DECRBY", "m1", "2"));

        await c.OkAsync("SET", "big", long.MaxValue.ToString());
        Assert.Contains("overflow", (await c.SendAsync("INCR", "big")).StringValue);

        await c.OkAsync("SET", "notnum", "abc");
        Assert.Contains("not an integer", (await c.SendAsync("INCR", "notnum")).StringValue);

        // APPEND and SETRANGE keep the exact expiry.
        await c.OkAsync("SET", "ttl", "a", "PX", "100000");
        var before = await c.IntAsync("PEXPIRETIME", "ttl");
        Assert.Equal(2, await c.IntAsync("APPEND", "ttl", "b"));
        Assert.Equal(before, await c.IntAsync("PEXPIRETIME", "ttl"));

        var at = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 60_000;
        await c.OkAsync("SET", "abs", "v", "PXAT", at.ToString());
        Assert.Equal(at, await c.IntAsync("PEXPIRETIME", "abs"));
        Assert.Equal(1, await c.IntAsync("PEXPIREAT", "abs", (at + 1000).ToString()));
        Assert.Equal(at + 1000, await c.IntAsync("PEXPIRETIME", "abs"));
        Assert.Equal(-1, await c.IntAsync("EXPIRETIME", "m2"));
        Assert.Equal(-2, await c.IntAsync("EXPIRETIME", "nope"));

        // An expiry in the past deletes.
        Assert.Equal(1, await c.IntAsync("EXPIREAT", "abs", "1"));
        Assert.Equal(0, await c.IntAsync("EXISTS", "abs"));

        var keys = await c.SendAsync("KEYS", "m*");
        Assert.Equal(new[] { "m1", "m2" }, keys.Items!.Select(k => k.AsString()).OrderBy(k => k));

        var time = await c.SendAsync("TIME");
        Assert.Equal(2, time.Items!.Count);
    }

    [Fact]
    public async Task Scan_visits_every_key_exactly_once()
    {
        await using var c = await _server.ConnectAsync();

        await c.PipelineAsync(Enumerable.Range(0, 3000).Select(i => new[] { "SET", $"scan:{i}", "v" }));
        await c.SendAsync("HSET", "scan:hash", "f", "v");

        var seen = new List<string>();
        var cursor = "0";
        var calls = 0;

        do
        {
            var reply = await c.SendAsync("SCAN", cursor, "MATCH", "scan:*", "COUNT", "100");
            cursor = reply.Items![0].AsString()!;
            seen.AddRange(reply.Items[1].Items!.Select(k => k.AsString()!));
            calls++;
        }
        while (cursor != "0");

        Assert.Equal(3001, seen.Count);
        Assert.Equal(3001, seen.Distinct().Count());
        Assert.True(calls > 1);

        var hashes = new List<string>();
        cursor = "0";

        do
        {
            var reply = await c.SendAsync("SCAN", cursor, "TYPE", "hash", "COUNT", "1000");
            cursor = reply.Items![0].AsString()!;
            hashes.AddRange(reply.Items[1].Items!.Select(k => k.AsString()!));
        }
        while (cursor != "0");

        Assert.Contains("scan:hash", hashes);
        Assert.DoesNotContain("scan:1", hashes);
    }

    [Fact]
    public async Task Lists_are_efficient_at_both_ends()
    {
        await using var c = await _server.ConnectAsync();

        // 100k pushes on one key: quadratic with the old List.Insert(0).
        var commands = Enumerable.Range(0, 100_000).Select(i => new[] { "LPUSH", "biglist", i.ToString() });
        var replies = await c.PipelineAsync(commands);

        Assert.Equal(100_000, replies[^1].IntegerValue);
        Assert.Equal("99999", await c.StringAsync("LINDEX", "biglist", "0"));
        Assert.Equal("0", await c.StringAsync("RPOP", "biglist"));
        Assert.Equal("99999", await c.StringAsync("LPOP", "biglist"));
        Assert.Equal(99_998, await c.IntAsync("LLEN", "biglist"));
    }

    [Fact]
    public async Task Transactions_are_atomic_and_watch_detects_conflicts()
    {
        await using var a = await _server.ConnectAsync();
        await using var b = await _server.ConnectAsync();

        await a.OkAsync("MULTI");
        Assert.Equal("QUEUED", (await a.SendAsync("SET", "tx1", "1")).AsString());
        Assert.Equal("QUEUED", (await a.SendAsync("INCR", "tx1")).AsString());
        var exec = await a.SendAsync("EXEC");
        Assert.Equal(2, exec.Items![1].IntegerValue);

        await a.OkAsync("WATCH", "watched");
        await b.OkAsync("SET", "watched", "changed");
        await a.OkAsync("MULTI");
        await a.SendAsync("SET", "watched", "mine");
        Assert.Equal(RespType.NullArray, (await a.SendAsync("EXEC")).Type);
        Assert.Equal("changed", await b.StringAsync("GET", "watched"));

        await a.OkAsync("MULTI");
        await a.SendAsync("NOSUCH");
        Assert.StartsWith("EXECABORT", (await a.SendAsync("EXEC")).StringValue);

        // Deleting and re-creating a watched key is a change too, even
        // though the new key's write count starts over.
        await b.OkAsync("SET", "recreated", "1");
        await a.OkAsync("WATCH", "recreated");
        await b.IntAsync("DEL", "recreated");
        await b.OkAsync("SET", "recreated", "1");
        await a.OkAsync("MULTI");
        await a.SendAsync("SET", "recreated", "mine");
        Assert.Equal(RespType.NullArray, (await a.SendAsync("EXEC")).Type);
    }

    [Fact]
    public async Task Concurrent_appends_never_lose_an_update()
    {
        // Read-modify-write commands used to race (read, then write
        // back); key locks make them atomic.
        var tasks = Enumerable.Range(0, 8).Select(async _ =>
        {
            await using var c = await _server.ConnectAsync();
            await c.PipelineAsync(Enumerable.Range(0, 500).Select(_ => new[] { "APPEND", "race", "x" }));
        });

        await Task.WhenAll(tasks);

        await using var check = await _server.ConnectAsync();
        Assert.Equal(4000, await check.IntAsync("STRLEN", "race"));
    }

    [Fact]
    public async Task Dump_and_restore_move_values_intact()
    {
        await using var c = await _server.ConnectAsync();

        await c.SendAsync("ZADD", "z", "1.5", "a", "2", "b");
        await c.SendAsync("PEXPIRE", "z", "100000");

        var dump = await c.SendAsync("DUMP", "z");
        Assert.Equal(RespType.BulkString, dump.Type);

        var restore = await c.SendAsync(new[]
        {
            RespValue.Bulk("RESTORE"), RespValue.Bulk("z2"), RespValue.Bulk("0"), dump,
        });

        Assert.Equal("OK", restore.AsString());

        var range = await c.SendAsync("ZRANGE", "z2", "0", "-1", "WITHSCORES");
        Assert.Equal(new[] { "a", "1.5", "b", "2" }, range.Items!.Select(i => i.AsString()));

        var busy = await c.SendAsync(new[] { RespValue.Bulk("RESTORE"), RespValue.Bulk("z2"), RespValue.Bulk("0"), dump });
        Assert.StartsWith("BUSYKEY", busy.StringValue);
    }

    [Fact]
    public async Task Reports_commands_with_key_positions()
    {
        await using var c = await _server.ConnectAsync();

        var info = await c.SendAsync("COMMAND", "INFO", "mset", "nosuch");
        var mset = info.Items![0].Items!;

        Assert.Equal("mset", mset[0].AsString());
        Assert.Equal(1, mset[3].IntegerValue);
        Assert.Equal(-1, mset[4].IntegerValue);
        Assert.Equal(2, mset[5].IntegerValue);
        Assert.Equal(RespType.NullArray, info.Items![1].Type);

        Assert.True(await c.IntAsync("COMMAND", "COUNT") > 80);
        Assert.Contains("wrong number of arguments", (await c.SendAsync("GET")).StringValue);
    }

    [Fact]
    public async Task Client_pause_holds_writes_without_failing_them()
    {
        await using var admin = await _server.ConnectAsync();
        await using var writer = await _server.ConnectAsync();

        await admin.OkAsync("CLIENT", "PAUSE", "500", "WRITE");

        // Reads are unaffected.
        Assert.Null(await admin.StringAsync("GET", "paused"));

        var started = DateTime.UtcNow;
        await writer.OkAsync("SET", "paused", "1");

        Assert.True(DateTime.UtcNow - started > TimeSpan.FromMilliseconds(300));
        Assert.Equal("1", await admin.StringAsync("GET", "paused"));
    }
}
