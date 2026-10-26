using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace recache.Monitoring
{
    // Counters for INFO. Incremented on every command from every core,
    // so they're striped per processor (each stripe on its own cache
    // line) and summed on read — a single shared counter would be the
    // most contended memory location in the server.
    public sealed class RedisMetrics
    {
        private const int Stripes = 64;

        [StructLayout(LayoutKind.Explicit, Size = 64)]
        private struct Slot
        {
            [FieldOffset(0)]
            public long Value;
        }

        private readonly Slot[] _commands = new Slot[Stripes];

        private long _connectionsReceived;
        private long _connectedClients;
        private long _rejectedConnections;

        public DateTimeOffset StartedAt { get; } = DateTimeOffset.UtcNow;

        public void CommandProcessed()
        {
            var slot = Thread.GetCurrentProcessorId() & (Stripes - 1);
            Interlocked.Increment(ref _commands[slot].Value);
        }

        public long CommandsProcessed
        {
            get
            {
                long total = 0;

                for (var i = 0; i < Stripes; i++)
                    total += Interlocked.Read(ref _commands[i].Value);

                return total;
            }
        }

        public void ClientConnected()
        {
            Interlocked.Increment(ref _connectionsReceived);
            Interlocked.Increment(ref _connectedClients);
        }

        public void ClientDisconnected()
            => Interlocked.Decrement(ref _connectedClients);

        public void ConnectionRejected()
            => Interlocked.Increment(ref _rejectedConnections);

        public long ConnectionsReceived => Interlocked.Read(ref _connectionsReceived);

        public long ConnectedClients => Interlocked.Read(ref _connectedClients);

        public long RejectedConnections => Interlocked.Read(ref _rejectedConnections);

        // ---- Garbage collection ----
        //
        // A managed cache's tail latency is its GC pauses, so INFO
        // reports them: how many collections of each kind, the total
        // and the longest pause, and which kind of collection that
        // longest pause was.

        private readonly object _gcLock = new();
        private long _lastGcIndex;
        private double _maxPauseMs;
        private string _maxPauseKind = "none";
        private Timer? _gcSampler;

        public void StartGcSampling()
        {
            _gcSampler ??= new Timer(_ => SampleGc(), null, 0, 50);
        }

        private void SampleGc()
        {
            foreach (var kind in new[] { GCKind.Ephemeral, GCKind.FullBlocking, GCKind.Background })
            {
                var info = GC.GetGCMemoryInfo(kind);

                if (info.Index == 0)
                    continue;

                double pause = 0;

                foreach (var duration in info.PauseDurations)
                    pause += duration.TotalMilliseconds;

                lock (_gcLock)
                {
                    if (pause > _maxPauseMs)
                    {
                        _maxPauseMs = pause;
                        _maxPauseKind = $"{kind} (gen{info.Generation}, {info.PromotedBytes / 1048576} MB promoted)";
                    }

                    _lastGcIndex = Math.Max(_lastGcIndex, info.Index);
                }
            }
        }

        public void AppendGcInfo(System.Text.StringBuilder sb)
        {
            var memory = GC.GetGCMemoryInfo();

            sb.Append($"gc_server:{(System.Runtime.GCSettings.IsServerGC ? 1 : 0)}\r\n");
            sb.Append($"gc_latency_mode:{System.Runtime.GCSettings.LatencyMode}\r\n");
            sb.Append($"gc_gen0_collections:{GC.CollectionCount(0)}\r\n");
            sb.Append($"gc_gen1_collections:{GC.CollectionCount(1)}\r\n");
            sb.Append($"gc_gen2_collections:{GC.CollectionCount(2)}\r\n");
            sb.Append($"gc_total_pause_ms:{(long)GC.GetTotalPauseDuration().TotalMilliseconds}\r\n");
            sb.Append($"gc_pause_time_percentage:{memory.PauseTimePercentage:F1}\r\n");
            sb.Append($"gc_heap_bytes:{memory.HeapSizeBytes}\r\n");
            sb.Append($"gc_committed_bytes:{memory.TotalCommittedBytes}\r\n");

            lock (_gcLock)
            {
                sb.Append($"gc_max_pause_ms:{_maxPauseMs:F1}\r\n");
                sb.Append($"gc_max_pause_kind:{_maxPauseKind}\r\n");
            }
        }
    }
}
