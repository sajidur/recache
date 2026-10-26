using recache.Protocol;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace recache.Replication
{
    // The ordered, offset-addressed stream of every write this node
    // has applied, in RESP form — the single source that the AOF
    // writer, each replica's sender, partial resyncs and AOF
    // rewrites all read from. An offset is a byte position in this
    // stream since the node's replication history began, exactly
    // like Redis's master_repl_offset.
    //
    // Storage is a list of fixed chunks. Appends (under one short
    // lock, holding just a memcpy) fill the tail chunk and then
    // publish the new end offset; readers find chunks by offset and
    // read everything below the published end with no lock at all —
    // those bytes are never written again. Chunks fall off the front
    // once nothing needs them: no cursor still behind them, and (when
    // partial resync matters) older than the retention window.
    public sealed class ReplicationBacklog
    {
        public const int ChunkSize = 256 * 1024;

        private sealed class Chunk
        {
            public Chunk(long start, int capacity)
            {
                Start = start;
                Data = GC.AllocateUninitializedArray<byte>(capacity);
            }

            public readonly long Start;
            public readonly byte[] Data;

            // Final once a newer chunk exists; for the tail, only
            // bytes below the published end offset are readable.
            public int Used;
        }

        // A consumer's read position. While registered it pins
        // everything from Position onward in memory — unless it falls
        // further behind than the hard output limit, in which case
        // it's evicted (a replica must then resync) rather than
        // letting the backlog grow without bound.
        public sealed class Cursor
        {
            internal Cursor(string name, long position, bool evictable)
            {
                Name = name;
                _position = position;
                Evictable = evictable;
            }

            private long _position;
            private volatile bool _evicted;

            public string Name { get; }

            public bool Evictable { get; }

            public long Position
            {
                get => Interlocked.Read(ref _position);
                set => Interlocked.Exchange(ref _position, value);
            }

            public bool Evicted
            {
                get => _evicted;
                internal set => _evicted = value;
            }
        }

        private readonly object _lock = new();
        private readonly List<Cursor> _cursors = new();
        private readonly long _retentionBytes;
        private readonly long _hardLimitBytes;

        private volatile Chunk[] _chunks;
        private Chunk _tail;
        private long _end;

        private TaskCompletionSource _signal =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private int _waiters;

        [ThreadStatic]
        private static ArrayBufferWriter<byte>? t_scratch;

        public ReplicationBacklog(long retentionBytes, long hardLimitBytes)
        {
            _retentionBytes = Math.Max(retentionBytes, ChunkSize);
            _hardLimitBytes = Math.Max(hardLimitBytes, _retentionBytes);
            _tail = new Chunk(0, ChunkSize);
            _chunks = new[] { _tail };
        }

        // When false, only registered cursors keep data alive (an
        // AOF-only standalone node needs nothing it has already
        // written). When true, the most recent ReplicationBacklogBytes
        // are also kept, so a replica that reconnects — or that
        // follows a failover to a new master — can catch up with a
        // partial resync instead of a full transfer.
        public volatile bool RetainForPartialResync;

        public long EndOffset => Interlocked.Read(ref _end);

        // Incremented by Reset. A consumer that sees it change knows
        // the stream it was following has been replaced wholesale (a
        // replica just loaded a full snapshot) and must start over.
        public int Generation => Volatile.Read(ref _generation);

        private int _generation;

        public long StartOffset
        {
            get
            {
                var chunks = _chunks;
                return chunks[0].Start;
            }
        }

        public long RetainedBytes => EndOffset - StartOffset;

        public bool Contains(long offset)
            => offset >= StartOffset && offset <= EndOffset;

        // Serializes one command and appends it. Returns the end
        // offset just after it — what WAIT and fsync=always need a
        // replica ACK / fsync to reach.
        public long AppendCommand(IReadOnlyList<RespValue> args)
        {
            var scratch = t_scratch ??= new ArrayBufferWriter<byte>(256);
            scratch.Clear();

            RespSerializer.WriteCommand(scratch, args);

            var end = Append(scratch.WrittenSpan);

            if (scratch.Capacity > 1024 * 1024)
                t_scratch = null;

            return end;
        }

        public long AppendCommand(params string[] args)
        {
            var scratch = t_scratch ??= new ArrayBufferWriter<byte>(256);
            scratch.Clear();

            RespSerializer.WriteCommand(scratch, args);

            return Append(scratch.WrittenSpan);
        }

        public long Append(ReadOnlySpan<byte> data)
        {
            long end;

            lock (_lock)
            {
                var remaining = data;

                while (!remaining.IsEmpty)
                {
                    var free = _tail.Data.Length - _tail.Used;

                    if (free == 0)
                    {
                        AddChunk(ChunkSize);
                        free = _tail.Data.Length;
                    }

                    var take = Math.Min(free, remaining.Length);

                    remaining[..take].CopyTo(_tail.Data.AsSpan(_tail.Used));
                    _tail.Used += take;
                    remaining = remaining[take..];
                }

                end = _end + data.Length;

                // Full fence: publishes the bytes above before the new
                // end, and orders it before the _waiters read below.
                Interlocked.Exchange(ref _end, end);
            }

            if (Volatile.Read(ref _waiters) > 0)
                Signal();

            return end;
        }

        // Starts the stream over at `offset` with nothing retained —
        // a replica that just loaded a full snapshot taken at that
        // master offset. Every cursor is moved there; evictable ones
        // (sub-replicas) are evicted since their history is gone.
        public void Reset(long offset)
        {
            lock (_lock)
            {
                _tail = new Chunk(offset, ChunkSize);
                _chunks = new[] { _tail };
                Interlocked.Exchange(ref _end, offset);
                Interlocked.Increment(ref _generation);

                foreach (var cursor in _cursors)
                {
                    if (cursor.Evictable)
                        cursor.Evicted = true;

                    cursor.Position = offset;
                }
            }

            Signal();
        }

        public Cursor Register(string name, long position, bool evictable)
        {
            lock (_lock)
            {
                if (position < _chunks[0].Start || position > _end)
                    throw new InvalidOperationException(
                        $"Offset {position} is outside the retained " +
                        $"backlog [{_chunks[0].Start}, {_end}].");

                var cursor = new Cursor(name, position, evictable);
                _cursors.Add(cursor);
                return cursor;
            }
        }

        // Registers a cursor at the current end — the snapshot code
        // calls this while it holds every key lock, so no write can
        // land between "state as of now" and "stream from now".
        public Cursor RegisterAtEnd(string name, bool evictable)
        {
            lock (_lock)
            {
                var cursor = new Cursor(name, _end, evictable);
                _cursors.Add(cursor);
                return cursor;
            }
        }

        public void Unregister(Cursor cursor)
        {
            lock (_lock)
                _cursors.Remove(cursor);

            Trim();
        }

        // Returns the next run of contiguous bytes at `position`, up
        // to the current end. Empty = caught up. Throws if `position`
        // has already been trimmed away.
        public ReadOnlyMemory<byte> Read(long position, int maxBytes = int.MaxValue)
        {
            var end = EndOffset;

            if (position >= end)
                return ReadOnlyMemory<byte>.Empty;

            var chunks = _chunks;

            if (position < chunks[0].Start)
                throw new InvalidOperationException(
                    $"Backlog offset {position} has been trimmed.");

            // Binary search for the chunk holding `position`.
            int lo = 0, hi = chunks.Length - 1;

            while (lo < hi)
            {
                var mid = (lo + hi + 1) / 2;

                if (chunks[mid].Start <= position)
                    lo = mid;
                else
                    hi = mid - 1;
            }

            var chunk = chunks[lo];
            var chunkEnd = lo == chunks.Length - 1
                ? end
                : Math.Min(end, chunk.Start + chunk.Used);

            var length = (int)Math.Min(chunkEnd - position, maxBytes);
            var offsetInChunk = (int)(position - chunk.Start);

            return chunk.Data.AsMemory(offsetInChunk, length);
        }

        public async Task WaitForDataAsync(
            long position,
            CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref _waiters);

            try
            {
                while (true)
                {
                    var signal = Volatile.Read(ref _signal);

                    if (EndOffset > position)
                        return;

                    await signal.Task.WaitAsync(cancellationToken)
                        .ConfigureAwait(false);
                }
            }
            finally
            {
                Interlocked.Decrement(ref _waiters);
            }
        }

        // Blocking flavour for the AOF writer's dedicated thread.
        public bool WaitForData(long position, int timeoutMs)
        {
            Interlocked.Increment(ref _waiters);

            try
            {
                var signal = Volatile.Read(ref _signal);

                if (EndOffset > position)
                    return true;

                signal.Task.Wait(timeoutMs);
                return EndOffset > position;
            }
            finally
            {
                Interlocked.Decrement(ref _waiters);
            }
        }

        // Wakes every waiter without new data (shutdown, Reset).
        public void Signal()
        {
            var previous = Interlocked.Exchange(
                ref _signal,
                new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously));

            previous.TrySetResult();
        }

        // Drops chunks nothing needs any more. Called as chunks fill
        // and as consumers advance.
        public void Trim()
        {
            lock (_lock)
            {
                TrimLocked();
            }
        }

        private void AddChunk(int capacity)
        {
            var chunk = new Chunk(_tail.Start + _tail.Used, capacity);
            var chunks = _chunks;
            var grown = new Chunk[chunks.Length + 1];

            Array.Copy(chunks, grown, chunks.Length);
            grown[^1] = chunk;

            _tail = chunk;
            _chunks = grown;

            TrimLocked();
        }

        private void TrimLocked()
        {
            var end = _end;

            var floor = RetainForPartialResync
                ? end - _retentionBytes
                : end;

            foreach (var cursor in _cursors)
            {
                if (cursor.Evicted)
                    continue;

                var position = cursor.Position;

                if (cursor.Evictable && end - position > _hardLimitBytes)
                {
                    cursor.Evicted = true;
                    continue;
                }

                floor = Math.Min(floor, position);
            }

            var chunks = _chunks;
            var drop = 0;

            // Never drop the tail; drop leading chunks that end at or
            // below the floor.
            while (drop < chunks.Length - 1 &&
                   chunks[drop].Start + chunks[drop].Used <= floor)
            {
                drop++;
            }

            if (drop == 0)
                return;

            var kept = new Chunk[chunks.Length - drop];
            Array.Copy(chunks, drop, kept, 0, kept.Length);
            _chunks = kept;
        }
    }
}
