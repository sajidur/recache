using recache.Clustering;
using recache.Protocol;
using System.Net.Sockets;
using System.Text;

namespace recache.Tests.Infrastructure;

// A small cluster-aware client that behaves like the real ones
// (StackExchange.Redis, redis-py, Lettuce): it routes by slot,
// follows MOVED (and re-learns the slot map), handles ASK, and retries
// through transient CLUSTERDOWN/TRYAGAIN and connection loss — which
// is exactly what a failover looks like from the outside.
public sealed class ClusterTestClient : IAsyncDisposable
{
    private readonly List<int> _seeds;
    private readonly Dictionary<string, TestClient> _connections = new();
    private readonly string?[] _slots = new string?[HashSlot.SlotCount];
    private readonly SemaphoreSlim _gate = new(1, 1);

    public ClusterTestClient(IEnumerable<int> seedPorts)
    {
        _seeds = seedPorts.ToList();
    }

    public long Redirects;
    public long Retries;

    public async Task<RespValue> SendAsync(params string[] args)
    {
        await _gate.WaitAsync();

        try
        {
            return await SendCoreAsync(args);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<RespValue> SendCoreAsync(string[] args)
    {
        var slot = args.Length > 1 ? HashSlot.Compute(Encoding.UTF8.GetBytes(args[1])) : -1;
        var target = slot >= 0 ? _slots[slot] : null;
        var asking = false;

        for (var attempt = 0; attempt < 200; attempt++)
        {
            target ??= $"127.0.0.1:{_seeds[attempt % _seeds.Count]}";

            RespValue reply;

            try
            {
                var connection = await ConnectionAsync(target);

                if (asking)
                {
                    await connection.SendAsync("ASKING");
                    asking = false;
                }

                reply = await connection.SendAsync(args);
            }
            catch (Exception ex) when (ex is IOException or SocketException or OperationCanceledException or InvalidOperationException)
            {
                Drop(target);
                Interlocked.Increment(ref Retries);
                await Task.Delay(50);
                await TryRefreshAsync();
                target = slot >= 0 ? _slots[slot] : null;
                continue;
            }

            if (reply.IsError)
            {
                var message = reply.StringValue ?? "";

                if (message.StartsWith("MOVED ", StringComparison.Ordinal))
                {
                    // Like real clients: a MOVED means the topology
                    // changed, so re-learn the whole slot map.
                    var parts = message.Split(' ');
                    Interlocked.Increment(ref Redirects);
                    await TryRefreshAsync();
                    _slots[int.Parse(parts[1])] = parts[2];
                    target = parts[2];
                    continue;
                }

                if (message.StartsWith("ASK ", StringComparison.Ordinal))
                {
                    target = message.Split(' ')[2];
                    asking = true;
                    continue;
                }

                if (message.StartsWith("CLUSTERDOWN", StringComparison.Ordinal) ||
                    message.StartsWith("TRYAGAIN", StringComparison.Ordinal))
                {
                    Interlocked.Increment(ref Retries);
                    await Task.Delay(50);
                    await TryRefreshAsync();
                    target = slot >= 0 ? _slots[slot] : null;
                    continue;
                }
            }

            return reply;
        }

        throw new TimeoutException($"Gave up on {string.Join(' ', args)}");
    }

    public async Task RefreshAsync()
    {
        await _gate.WaitAsync();

        try
        {
            await TryRefreshAsync();
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task TryRefreshAsync()
    {
        foreach (var port in _seeds.OrderBy(_ => Random.Shared.Next()))
        {
            var endpoint = $"127.0.0.1:{port}";

            try
            {
                var connection = await ConnectionAsync(endpoint);
                var reply = await connection.SendAsync("CLUSTER", "SLOTS");

                if (reply.Type != RespType.Array)
                    continue;

                foreach (var range in reply.Items!)
                {
                    var items = range.Items!;
                    var start = (int)items[0].IntegerValue;
                    var end = (int)items[1].IntegerValue;
                    var master = items[2].Items!;
                    var address = $"{master[0].AsString()}:{master[1].IntegerValue}";

                    for (var s = start; s <= end; s++)
                        _slots[s] = address;
                }

                return;
            }
            catch (Exception ex) when (ex is IOException or SocketException or OperationCanceledException or InvalidOperationException)
            {
                Drop(endpoint);
            }
        }
    }

    private async Task<TestClient> ConnectionAsync(string endpoint)
    {
        if (_connections.TryGetValue(endpoint, out var existing))
            return existing;

        var colon = endpoint.LastIndexOf(':');
        var client = await TestClient.ConnectAsync(int.Parse(endpoint[(colon + 1)..]), endpoint[..colon]);
        _connections[endpoint] = client;
        return client;
    }

    private void Drop(string endpoint)
    {
        if (_connections.Remove(endpoint, out var client))
            _ = client.DisposeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var client in _connections.Values)
            await client.DisposeAsync();

        _connections.Clear();
    }
}

public static class Eventually
{
    public static async Task WaitAsync(
        Func<Task<bool>> condition,
        TimeSpan timeout,
        string description)
    {
        var deadline = DateTime.UtcNow + timeout;
        Exception? last = null;

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                if (await condition())
                    return;
            }
            catch (Exception ex)
            {
                last = ex;
            }

            await Task.Delay(100);
        }

        throw new TimeoutException($"Timed out waiting for: {description}", last);
    }
}
