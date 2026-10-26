using Microsoft.Extensions.Logging.Abstractions;
using recache.Commands;
using recache.Configuration;
using recache.Persistence;
using recache.Protocol;
using recache.Replication;
using recache.Security;
using recache.Storage;
using System.Collections.Concurrent;
using System.Text;

namespace recache.Tests.Unit;

public class ReplicationBacklogTests
{
    [Fact]
    public void Reads_back_exactly_what_was_appended_across_chunks()
    {
        var backlog = new ReplicationBacklog(64L << 20, 128L << 20)
        {
            RetainForPartialResync = true,
        };

        var expected = new MemoryStream();
        var random = new Random(1);

        for (var i = 0; i < 2000; i++)
        {
            var data = new byte[random.Next(1, 2000)];
            random.NextBytes(data);
            backlog.Append(data);
            expected.Write(data);
        }

        // A single append larger than a chunk.
        var big = new byte[ReplicationBacklog.ChunkSize * 2 + 17];
        random.NextBytes(big);
        backlog.Append(big);
        expected.Write(big);

        var actual = new MemoryStream();
        long position = 0;

        while (true)
        {
            var data = backlog.Read(position, 7777);

            if (data.IsEmpty)
                break;

            actual.Write(data.Span);
            position += data.Length;
        }

        Assert.Equal(expected.Length, backlog.EndOffset);
        Assert.Equal(expected.ToArray(), actual.ToArray());
    }

    [Fact]
    public void A_cursor_pins_data_until_it_moves_on()
    {
        var backlog = new ReplicationBacklog(ReplicationBacklog.ChunkSize, 1L << 30);
        var cursor = backlog.Register("consumer", 0, evictable: false);
        var chunk = new byte[ReplicationBacklog.ChunkSize];

        for (var i = 0; i < 8; i++)
            backlog.Append(chunk);

        // Nothing is dropped while the cursor still needs offset 0.
        Assert.Equal(0, backlog.StartOffset);

        cursor.Position = backlog.EndOffset;
        backlog.Trim();

        Assert.True(backlog.StartOffset > 0);
        Assert.Throws<InvalidOperationException>(() => backlog.Read(0));
    }

    [Fact]
    public void An_evictable_cursor_that_falls_too_far_behind_is_evicted()
    {
        var backlog = new ReplicationBacklog(ReplicationBacklog.ChunkSize, ReplicationBacklog.ChunkSize * 2L);
        var cursor = backlog.Register("slow-replica", 0, evictable: true);
        var chunk = new byte[ReplicationBacklog.ChunkSize];

        for (var i = 0; i < 6; i++)
            backlog.Append(chunk);

        backlog.Trim();

        Assert.True(cursor.Evicted);
    }

    [Fact]
    public void Reset_restarts_the_stream_and_bumps_the_generation()
    {
        var backlog = new ReplicationBacklog(1 << 20, 1 << 30);
        backlog.AppendCommand("SET", "a", "1");
        var generation = backlog.Generation;

        backlog.Reset(5000);

        Assert.Equal(5000, backlog.StartOffset);
        Assert.Equal(5000, backlog.EndOffset);
        Assert.NotEqual(generation, backlog.Generation);
    }
}

public class SegmentedCollectionTests
{
    [Fact]
    public void Large_collections_keep_every_member_across_the_split()
    {
        var hash = new RedisHash();
        var set = new RedisSet();
        var zset = new RedisSortedSet();

        // Well past the point where they split into segments.
        for (var i = 0; i < 20_000; i++)
        {
            Assert.True(hash.Set($"f{i}", Encoding.UTF8.GetBytes($"v{i}"), out _));
            Assert.True(set.Add(Encoding.UTF8.GetBytes($"m{i}"), out _));
            Assert.True(zset.Add($"z{i}", i, out _));
        }

        Assert.False(hash.Set("f7", "new"u8.ToArray(), out _));
        Assert.False(set.Add("m7"u8.ToArray(), out _));

        for (var i = 0; i < 20_000; i += 2)
        {
            Assert.True(hash.Remove($"f{i}", out _));
            Assert.True(set.Remove(Encoding.UTF8.GetBytes($"m{i}"), out _));
            Assert.True(zset.Remove($"z{i}", out _));
        }

        Assert.Equal(10_000, hash.Count);
        Assert.Equal(10_000, set.Count);
        Assert.Equal(10_000, zset.Count);
        Assert.Equal(10_000, hash.Snapshot().Length);
        Assert.Equal(10_000, set.Snapshot().Length);

        Assert.True(hash.TryGet("f7", out var value));
        Assert.Equal("new", Encoding.UTF8.GetString(value!));
        Assert.False(hash.TryGet("f8", out _));
        Assert.True(set.Contains("m19999"u8.ToArray()));
        Assert.True(zset.TryGetScore("z19999", out var score) && score == 19999);
        Assert.Equal(0, zset.Rank("z1"));
    }
}

public class DumpSerializerTests
{
    // The entry's type is derived from its payload.
    private static CacheEntry Entry(CacheValueType type, object payload)
        => new(payload, expiresAtMs: null);

    [Fact]
    public void Round_trips_every_type()
    {
        var hash = new RedisHash();
        hash.Set("f", "v"u8.ToArray(), out _);

        var list = new RedisList();
        list.PushRight("a"u8.ToArray());
        list.PushRight("b"u8.ToArray());

        var set = new RedisSet();
        set.Add("m"u8.ToArray(), out _);

        var zset = new RedisSortedSet();
        zset.Add("z", 1.5, out _);
        zset.Add("y", double.NegativeInfinity, out _);

        var json = new RedisJson();
        json.TrySet("$", System.Text.Json.Nodes.JsonNode.Parse("{\"a\":[1,2]}"), out _);

        var entries = new[]
        {
            Entry(CacheValueType.String, "hello"u8.ToArray()),
            Entry(CacheValueType.Hash, hash),
            Entry(CacheValueType.List, list),
            Entry(CacheValueType.Set, set),
            Entry(CacheValueType.SortedSet, zset),
            Entry(CacheValueType.Json, json),
        };

        foreach (var entry in entries)
        {
            var payload = DumpSerializer.Serialize(entry);

            Assert.True(DumpSerializer.TryDeserialize(payload, out var type, out var value, out _));
            Assert.Equal(entry.Type, type);

            // Serializing the restored value must reproduce the bytes.
            Assert.Equal(payload, DumpSerializer.Serialize(Entry(type, value!)));
        }
    }

    [Fact]
    public void Rejects_a_damaged_payload()
    {
        var payload = DumpSerializer.Serialize(Entry(CacheValueType.String, "hello"u8.ToArray()));
        payload[1] ^= 0xFF;

        Assert.False(DumpSerializer.TryDeserialize(payload, out _, out _, out _));
        Assert.False(DumpSerializer.TryDeserialize(new byte[] { 1, 2, 3 }, out _, out _, out _));
    }
}

// The property everything else rests on: a snapshot taken while
// writers hammer the keyspace, plus the replication stream from the
// snapshot's offset, rebuilds exactly the final dataset — even for
// non-idempotent commands (INCR, LPUSH, APPEND) that would visibly
// double-apply if a write landed in both.
public class SnapshotConsistencyTests
{
    [Fact]
    public async Task Snapshot_plus_stream_reproduces_the_live_dataset()
    {
        var options = new RecacheOptions();
        var auth = new AuthenticationManager(options);
        var store = new CacheStore();
        var locks = new KeyLocks();
        var backlog = new ReplicationBacklog(1L << 30, 1L << 31);
        var snapshots = new SnapshotService(store, locks, backlog);
        var directory = Directory.CreateTempSubdirectory("recache-snapshot-test").FullName;

        var replication = new ReplicationManager(
            options, store, locks, backlog, snapshots, directory, NullLogger.Instance);

        var dispatcher = new CommandDispatcher(store, auth, options, cluster: null, locks)
        {
            Replication = replication,
            Snapshots = snapshots,
        };

        replication.EnablePropagation(retainForPartialResync: true);

        var session = new ClientSession();

        async Task Run(params string[] command)
        {
            var reply = await dispatcher.ExecuteAsync(RespValue.Command(command), session, default);
            Assert.False(reply.IsError, $"{string.Join(' ', command)} -> {reply.StringValue}");
        }

        for (var i = 0; i < 2000; i++)
        {
            await Run("SET", $"s:{i}", "0");
            await Run("RPUSH", $"l:{i % 100}", "seed");
            await Run("HSET", $"h:{i % 100}", $"f{i}", "v");
        }

        using var stop = new CancellationTokenSource();

        var writers = Enumerable.Range(0, 4).Select(w => Task.Run(async () =>
        {
            var random = new Random(w);
            var writerSession = new ClientSession();

            while (!stop.IsCancellationRequested)
            {
                var i = random.Next(2000);

                var command = random.Next(6) switch
                {
                    0 => new[] { "INCR", $"s:{i}" },
                    1 => new[] { "APPEND", $"s:{i}", "x" },
                    2 => new[] { "LPUSH", $"l:{i % 100}", $"w{w}" },
                    3 => new[] { "HSET", $"h:{i % 100}", $"f{i}", $"w{w}" },
                    4 => new[] { "DEL", $"s:{i}" },
                    _ => new[] { "SET", $"s:{i}", $"{i}" },
                };

                await dispatcher.ExecuteAsync(RespValue.Command(command), writerSession, default);
            }
        })).ToArray();

        await Task.Delay(100);

        var path = Path.Combine(directory, "snapshot.aof");
        var snapshot = await snapshots.CreateAsync(path, "test", default);

        await Task.Delay(200);
        stop.Cancel();
        await Task.WhenAll(writers);

        // Rebuild: snapshot, then the stream from its offset.
        var rebuilt = new CacheStore();
        var loader = new CommandDispatcher(rebuilt, auth, options);
        var loadSession = new ClientSession { IsMasterLink = true };

        AofLoader.Load(
            path,
            (args, _) => loader.ExecuteReplicated(args, loadSession, ReadOnlySpan<byte>.Empty),
            allowTruncatedTail: false,
            truncateFile: false);

        var tail = new MemoryStream();

        for (var position = snapshot.Offset; ;)
        {
            var data = backlog.Read(position);

            if (data.IsEmpty)
                break;

            tail.Write(data.Span);
            position += data.Length;
        }

        var tailPath = Path.Combine(directory, "tail.aof");
        File.WriteAllBytes(tailPath, tail.ToArray());

        AofLoader.Load(
            tailPath,
            (args, _) => loader.ExecuteReplicated(args, loadSession, ReadOnlySpan<byte>.Empty),
            allowTruncatedTail: false,
            truncateFile: false);

        Assert.True(snapshot.Keys > 0);
        Assert.Equal(Describe(store), Describe(rebuilt));

        Directory.Delete(directory, recursive: true);
    }

    private static SortedDictionary<string, string> Describe(CacheStore store)
    {
        var result = new SortedDictionary<string, string>(StringComparer.Ordinal);

        foreach (var key in store.Keys)
        {
            var entry = store.Peek(key)!;

            result[key] = entry.Type switch
            {
                CacheValueType.String => Encoding.UTF8.GetString((byte[])entry.Payload),
                CacheValueType.List => string.Join(",", ((RedisList)entry.Payload).Range(0, -1).Select(Encoding.UTF8.GetString)),
                CacheValueType.Hash => string.Join(",", ((RedisHash)entry.Payload).Snapshot()
                    .OrderBy(p => p.Key, StringComparer.Ordinal)
                    .Select(p => $"{p.Key}={Encoding.UTF8.GetString(p.Value)}")),
                _ => entry.Type.ToString(),
            };
        }

        return result;
    }
}
