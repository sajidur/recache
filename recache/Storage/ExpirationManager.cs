using Microsoft.Extensions.Logging;
using recache.Replication;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace recache.Storage
{
    // Active expiry. Keys are also expired lazily when touched, but a
    // key that expires and is never read again would otherwise sit in
    // memory forever. Every 100 ms this walks the keys that carry a
    // TTL — resuming where it left off — for at most 25 ms (Redis's
    // 25% CPU budget for the same job), deleting expired ones under
    // their key lock so each deletion is propagated as a DEL in order.
    //
    // Masters only: a replica waits for its master's DELs.
    public sealed class ExpirationManager : IDisposable
    {
        private static readonly TimeSpan Budget = TimeSpan.FromMilliseconds(25);

        private readonly CacheStore _store;
        private readonly KeyLocks _locks;
        private readonly ReplicationManager _replication;
        private readonly ILogger _logger;
        private readonly CancellationTokenSource _cts = new();

        private IEnumerator<string>? _cursor;

        public ExpirationManager(
            CacheStore store,
            KeyLocks locks,
            ReplicationManager replication,
            ILogger logger)
        {
            _store = store;
            _locks = locks;
            _replication = replication;
            _logger = logger;
        }

        public long ExpiredKeys;

        public void Start()
            => _ = RunAsync(_cts.Token);

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));

            try
            {
                while (await timer.WaitForNextTickAsync(cancellationToken))
                {
                    if (_replication.IsReplica || _replication.Loading)
                        continue;

                    try
                    {
                        Cycle();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Active expiry cycle failed");
                        _cursor = null;
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private void Cycle()
        {
            if (_store.ExpiringCount == 0)
                return;

            var started = Stopwatch.GetTimestamp();
            var now = CacheStore.NowMs;
            var restarted = false;

            while (Stopwatch.GetElapsedTime(started) < Budget)
            {
                _cursor ??= _store.ExpiringKeys.GetEnumerator();

                if (!_cursor.MoveNext())
                {
                    _cursor.Dispose();
                    _cursor = null;

                    // One full pass per cycle is plenty.
                    if (restarted)
                        return;

                    restarted = true;
                    continue;
                }

                var key = _cursor.Current;

                // Cheap unlocked peek first; only lock what looks due.
                if (_store.Peek(key) is { ExpiresAtMs: { } expiresAt } && expiresAt > now)
                    continue;

                var stripe = KeyLocks.StripeOf(key);

                _locks.EnterWrite(stripe);

                try
                {
                    if (_store.ExpireIfDue(key))
                        Interlocked.Increment(ref ExpiredKeys);
                }
                finally
                {
                    _locks.ExitWrite(stripe);
                }
            }
        }

        public void Dispose()
            => _cts.Cancel();
    }
}
