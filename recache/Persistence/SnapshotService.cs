using recache.Protocol;
using recache.Replication;
using recache.Storage;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace recache.Persistence
{
    public sealed record SnapshotResult(
        string Path,
        long Offset,
        ReplicationBacklog.Cursor Cursor,
        long Keys,
        long Bytes);

    // Point-in-time dataset snapshots without fork() and without
    // stopping the world — the base of an AOF rewrite, and what a
    // replica's full resync transfers.
    //
    // Starting a snapshot takes every key lock for an instant (no
    // write is mid-flight then), bumps the snapshot epoch, and pins
    // the replication stream at its current end offset O. From then
    // on each key that existed at O is captured exactly once, by
    // whichever comes first:
    //   - the background iterator walking the keyspace, or
    //   - the first write about to modify that key (BeforeWrite runs
    //     under the key's exclusive lock, before the handler) — a
    //     per-key copy-on-write, capturing the pre-image.
    // Keys created after O carry the new epoch and are skipped. The
    // result is exactly the state at O; replaying the stream from O
    // on top of it reproduces the live dataset.
    //
    // Output is plain RESP commands (SET/HSET/RPUSH/SADD/ZADD/
    // JSON.SET + PEXPIREAT), so a snapshot is itself a valid AOF.
    public sealed class SnapshotService
    {
        // Same batching as Redis's AOF rewrite: big collections become
        // several commands rather than one enormous one.
        private const int ItemsPerCommand = 64;

        private readonly CacheStore _store;
        private readonly KeyLocks _locks;
        private readonly ReplicationBacklog _backlog;
        private readonly SemaphoreSlim _gate = new(1, 1);

        private volatile ActiveSnapshot? _active;
        private int _epoch;

        public SnapshotService(
            CacheStore store,
            KeyLocks locks,
            ReplicationBacklog backlog)
        {
            _store = store;
            _locks = locks;
            _backlog = backlog;
        }

        public bool InProgress => _active != null;

        // The caller owns the returned cursor (it pins the stream
        // from Offset onward) and must Unregister it when done.
        public async Task<SnapshotResult> CreateAsync(
            string path,
            string cursorName,
            CancellationToken cancellationToken)
        {
            await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);

            try
            {
                using var output = new SnapshotOutput(path);

                ActiveSnapshot snapshot;
                ReplicationBacklog.Cursor cursor;

                _locks.EnterAll();

                try
                {
                    var epoch = ++_epoch;

                    Volatile.Write(ref _store.CurrentSnapshotEpoch, epoch);
                    cursor = _backlog.RegisterAtEnd(cursorName, evictable: false);
                    snapshot = new ActiveSnapshot(epoch, output);
                    _active = snapshot;
                }
                finally
                {
                    _locks.ExitAll();
                }

                try
                {
                    await Task.Run(() => Iterate(snapshot), cancellationToken)
                        .ConfigureAwait(false);
                }
                catch
                {
                    _backlog.Unregister(cursor);
                    throw;
                }
                finally
                {
                    _active = null;

                    // A writer may still be inside BeforeWrite with a
                    // reference to this snapshot; wait it out before
                    // the output is finalized.
                    _locks.Drain();
                }

                var bytes = output.Complete();

                return new SnapshotResult(
                    path,
                    cursor.Position,
                    cursor,
                    snapshot.Keys,
                    bytes);
            }
            finally
            {
                _gate.Release();
            }
        }

        // Writers call this holding the key's exclusive lock, before
        // they mutate it.
        public void BeforeWrite(string key)
        {
            var snapshot = _active;

            if (snapshot != null)
                Capture(snapshot, key);
        }

        // FLUSHALL during a snapshot: capture everything still
        // uncaptured first. Called with every key lock held.
        public void BeforeFlushAll()
        {
            var snapshot = _active;

            if (snapshot == null)
                return;

            foreach (var pair in _store.Entries)
                Capture(snapshot, pair.Key);
        }

        private void Iterate(ActiveSnapshot snapshot)
        {
            foreach (var pair in _store.Entries)
            {
                var stripe = KeyLocks.StripeOf(pair.Key);

                _locks.EnterRead(stripe);

                try
                {
                    Capture(snapshot, pair.Key);
                }
                finally
                {
                    _locks.ExitRead(stripe);
                }
            }
        }

        private void Capture(ActiveSnapshot snapshot, string key)
        {
            var entry = _store.Peek(key);

            if (entry == null)
                return;

            var seen = Volatile.Read(ref entry.SnapshotEpoch);

            if (seen >= snapshot.Epoch ||
                Interlocked.CompareExchange(
                    ref entry.SnapshotEpoch,
                    snapshot.Epoch,
                    seen) != seen)
            {
                return;
            }

            if (entry.ExpiresAtMs is { } expiresAt &&
                expiresAt <= CacheStore.NowMs)
            {
                return;
            }

            snapshot.Output.Write(key, entry);
            Interlocked.Increment(ref snapshot.Keys);
        }

        // Serializes one key as the commands that rebuild it.
        public static void WriteEntry(
            IBufferWriter<byte> writer,
            string key,
            CacheEntry entry)
        {
            switch (entry.Type)
            {
                case CacheValueType.String:
                {
                    var value = (byte[])entry.Payload;

                    if (entry.ExpiresAtMs is { } expiresAt)
                    {
                        RespSerializer.WriteArrayHeader(writer, 5);
                        RespSerializer.WriteBulk(writer, "SET"u8);
                        RespSerializer.WriteBulk(writer, key);
                        RespSerializer.WriteBulk(writer, value);
                        RespSerializer.WriteBulk(writer, "PXAT"u8);
                        RespSerializer.WriteBulk(writer, expiresAt);
                    }
                    else
                    {
                        RespSerializer.WriteArrayHeader(writer, 3);
                        RespSerializer.WriteBulk(writer, "SET"u8);
                        RespSerializer.WriteBulk(writer, key);
                        RespSerializer.WriteBulk(writer, value);
                    }

                    return;
                }

                case CacheValueType.Hash:
                {
                    var fields = ((RedisHash)entry.Payload).Snapshot();

                    for (var i = 0; i < fields.Length; i += ItemsPerCommand)
                    {
                        var count = Math.Min(ItemsPerCommand, fields.Length - i);

                        RespSerializer.WriteArrayHeader(writer, 2 + count * 2);
                        RespSerializer.WriteBulk(writer, "HSET"u8);
                        RespSerializer.WriteBulk(writer, key);

                        for (var j = i; j < i + count; j++)
                        {
                            RespSerializer.WriteBulk(writer, fields[j].Key);
                            RespSerializer.WriteBulk(writer, fields[j].Value);
                        }
                    }

                    break;
                }

                case CacheValueType.List:
                {
                    var items = ((RedisList)entry.Payload).Range(0, -1);

                    WriteBatched(writer, "RPUSH"u8, key, items);
                    break;
                }

                case CacheValueType.Set:
                {
                    var members = ((RedisSet)entry.Payload).Snapshot();

                    WriteBatched(writer, "SADD"u8, key, members);
                    break;
                }

                case CacheValueType.SortedSet:
                {
                    var items = ((RedisSortedSet)entry.Payload).Range(0, -1);

                    for (var i = 0; i < items.Length; i += ItemsPerCommand)
                    {
                        var count = Math.Min(ItemsPerCommand, items.Length - i);

                        RespSerializer.WriteArrayHeader(writer, 2 + count * 2);
                        RespSerializer.WriteBulk(writer, "ZADD"u8);
                        RespSerializer.WriteBulk(writer, key);

                        for (var j = i; j < i + count; j++)
                        {
                            RespSerializer.WriteBulk(
                                writer,
                                items[j].Score.ToString("R", CultureInfo.InvariantCulture));

                            RespSerializer.WriteBulk(writer, items[j].Member);
                        }
                    }

                    break;
                }

                case CacheValueType.Json:
                {
                    var json = (RedisJson)entry.Payload;

                    if (!json.TryGet(Array.Empty<string>(), out var text))
                        return;

                    RespSerializer.WriteArrayHeader(writer, 4);
                    RespSerializer.WriteBulk(writer, "JSON.SET"u8);
                    RespSerializer.WriteBulk(writer, key);
                    RespSerializer.WriteBulk(writer, "$"u8);
                    RespSerializer.WriteBulk(writer, text!);
                    break;
                }

                default:
                    return;
            }

            if (entry.ExpiresAtMs is { } collectionExpiresAt)
            {
                RespSerializer.WriteArrayHeader(writer, 3);
                RespSerializer.WriteBulk(writer, "PEXPIREAT"u8);
                RespSerializer.WriteBulk(writer, key);
                RespSerializer.WriteBulk(writer, collectionExpiresAt);
            }
        }

        private static void WriteBatched(
            IBufferWriter<byte> writer,
            ReadOnlySpan<byte> command,
            string key,
            byte[][] items)
        {
            for (var i = 0; i < items.Length; i += ItemsPerCommand)
            {
                var count = Math.Min(ItemsPerCommand, items.Length - i);

                RespSerializer.WriteArrayHeader(writer, 2 + count);
                RespSerializer.WriteBulk(writer, command);
                RespSerializer.WriteBulk(writer, key);

                for (var j = i; j < i + count; j++)
                    RespSerializer.WriteBulk(writer, items[j]);
            }
        }

        private sealed class ActiveSnapshot
        {
            public ActiveSnapshot(int epoch, SnapshotOutput output)
            {
                Epoch = epoch;
                Output = output;
            }

            public readonly int Epoch;
            public readonly SnapshotOutput Output;
            public long Keys;
        }

        // Shared by the iterator and any writer capturing a pre-image,
        // so writes to the file are serialized behind one lock. Each
        // key is serialized into a scratch buffer first and appended
        // to a large BufferedStream, so the lock is held for a memcpy
        // in the common case.
        private sealed class SnapshotOutput : IDisposable
        {
            private readonly object _lock = new();
            private readonly FileStream _file;
            private readonly BufferedStream _stream;
            private readonly ArrayBufferWriter<byte> _scratch = new(64 * 1024);
            private long _bytes;
            private bool _completed;

            public SnapshotOutput(string path)
            {
                var directory = Path.GetDirectoryName(path);

                if (!string.IsNullOrEmpty(directory))
                    Directory.CreateDirectory(directory);

                _file = new FileStream(
                    path,
                    FileMode.Create,
                    FileAccess.Write,
                    FileShare.Read,
                    bufferSize: 1);

                _stream = new BufferedStream(_file, 1024 * 1024);
            }

            public void Write(string key, CacheEntry entry)
            {
                lock (_lock)
                {
                    _scratch.Clear();
                    WriteEntry(_scratch, key, entry);
                    _stream.Write(_scratch.WrittenSpan);
                    _bytes += _scratch.WrittenCount;
                }
            }

            public long Complete()
            {
                lock (_lock)
                {
                    _stream.Flush();
                    _file.Flush(flushToDisk: true);
                    _completed = true;
                    return _bytes;
                }
            }

            public void Dispose()
            {
                lock (_lock)
                {
                    if (!_completed)
                        _stream.Flush();

                    _stream.Dispose();
                }
            }
        }
    }
}
