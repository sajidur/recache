using Microsoft.Extensions.Logging;
using recache.Configuration;
using recache.Replication;
using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace recache.Persistence
{
    public enum AofFsyncPolicy
    {
        Always,
        EverySec,
        No
    }

    // The append-only file. A dedicated thread consumes the
    // replication stream (which already holds every write, in apply
    // order, in deterministic form) and appends it to disk; client
    // commands never touch the file themselves.
    //
    //  - fsync "always": a writing client's reply waits until an fsync
    //    covers its write. Concurrent writers share fsyncs (group
    //    commit), so this costs latency, not one fsync per command.
    //  - fsync "everysec": fsync at most once a second in the
    //    background; no client ever waits on the disk.
    //  - fsync "no": the OS decides.
    //
    // Rewrite (BGREWRITEAOF, or automatic on growth) compacts the file
    // without blocking anything: a point-in-time snapshot at stream
    // offset O becomes the new file's base, the stream from O onward
    // is appended to it, and this thread then swaps it in with an
    // atomic rename — the old file stays complete and valid until the
    // instant the new one replaces it.
    public sealed class AofManager : IDisposable
    {
        private readonly RecacheOptions _options;
        private readonly ReplicationBacklog _backlog;
        private readonly SnapshotService _snapshots;
        private readonly ILogger _logger;

        private readonly object _durableLock = new();

        // The file is only ever touched by the writer thread; this just
        // makes Dispose wait out an in-progress write or rewrite swap.
        private readonly object _fileLock = new();
        private TaskCompletionSource _durableSignal =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private Thread? _thread;
        private volatile bool _running;
        private FileStream? _file;
        private ReplicationBacklog.Cursor? _cursor;

        private long _writtenOffset;
        private long _durableOffset;
        private int _streamGeneration;
        private volatile bool _rewriteAfterReset;
        private long _fileSize;
        private long _baseSize;
        private long _lastFsyncTimestamp;

        private volatile bool _rewriteInProgress;
        private volatile PendingSwitch? _pendingSwitch;
        private long _rewriteStartedTimestamp;
        private long _lastRewriteDurationMs = -1;
        private long _rewrites;
        private volatile bool _lastRewriteOk = true;
        private volatile bool _lastWriteOk = true;
        private long _delayedFsyncs;

        public AofManager(
            RecacheOptions options,
            string contentRoot,
            ReplicationBacklog backlog,
            SnapshotService snapshots,
            ILogger logger)
        {
            _options = options;
            _backlog = backlog;
            _snapshots = snapshots;
            _logger = logger;

            Path = System.IO.Path.IsPathRooted(options.AofPath)
                ? options.AofPath
                : System.IO.Path.Combine(contentRoot, options.AofPath);

            Policy = options.AofFsync?.Trim().ToLowerInvariant() switch
            {
                "always" => AofFsyncPolicy.Always,
                "no" => AofFsyncPolicy.No,
                _ => AofFsyncPolicy.EverySec,
            };
        }

        public string Path { get; }

        public string TempRewritePath => Path + ".rewrite.tmp";

        public AofFsyncPolicy Policy { get; }

        public bool FsyncAlways => Policy == AofFsyncPolicy.Always;

        public bool RewriteInProgress => _rewriteInProgress;

        public long CurrentSize => Interlocked.Read(ref _fileSize);

        // Opens the file for appending and starts consuming the stream
        // from `startOffset` (the stream position the file's contents
        // already correspond to — normally the current end, right
        // after the file was replayed).
        public void Start(long startOffset)
        {
            var directory = System.IO.Path.GetDirectoryName(Path);

            if (!string.IsNullOrEmpty(directory))
                Directory.CreateDirectory(directory);

            // A crash mid-rewrite leaves a stale temp file; the real
            // AOF was never touched, so the temp file is just garbage.
            TryDelete(TempRewritePath);

            _file = OpenForAppend();
            _fileSize = _file.Length;
            _baseSize = _fileSize;
            _writtenOffset = startOffset;
            _durableOffset = startOffset;
            _streamGeneration = _backlog.Generation;
            _lastFsyncTimestamp = Stopwatch.GetTimestamp();
            _cursor = _backlog.Register("aof", startOffset, evictable: false);

            _running = true;

            _thread = new Thread(WriterLoop)
            {
                IsBackground = true,
                Name = "recache-aof-writer",
            };

            _thread.Start();

            _logger.LogInformation(
                "AOF enabled: {Path} (fsync {Policy}, {Size} bytes)",
                Path,
                Policy,
                _fileSize);
        }

        // What a client must wait for before its write is acknowledged,
        // matching Redis: with "always", an fsync covering it; with
        // "everysec"/"no", the write() that hands it to the OS — so a
        // crash of this process never loses an acknowledged write, and
        // only "everysec"'s last second is at risk on a power cut.
        // Concurrent waiters share the same write/fsync.
        public ValueTask WaitAcknowledgeableAsync(long offset)
        {
            var reached = Policy == AofFsyncPolicy.Always
                ? Interlocked.Read(ref _durableOffset)
                : Interlocked.Read(ref _writtenOffset);

            if (reached >= offset || !_running)
                return ValueTask.CompletedTask;

            // The writer thread does the write: it naturally batches
            // every client's pending writes into one write() per wakeup.
            // (Clients writing inline instead convoyed on the file and
            // issued a tiny write() each — several times slower.)
            return WaitSlowAsync(offset);
        }

        private async ValueTask WaitSlowAsync(long offset)
        {
            while (true)
            {
                Task signal;

                lock (_durableLock)
                {
                    var reached = Policy == AofFsyncPolicy.Always
                        ? Interlocked.Read(ref _durableOffset)
                        : Interlocked.Read(ref _writtenOffset);

                    if (reached >= offset || !_running)
                        return;

                    signal = _durableSignal.Task;
                }

                await signal.ConfigureAwait(false);
            }
        }

        // Starts a background rewrite. Returns false if one is already
        // running.
        public bool StartRewrite()
        {
            if (_rewriteInProgress)
                return false;

            _rewriteInProgress = true;
            _rewriteStartedTimestamp = Stopwatch.GetTimestamp();

            _ = Task.Run(RewriteAsync);
            return true;
        }

        public void AppendInfo(StringBuilder sb)
        {
            sb.Append("aof_enabled:1\r\n");
            sb.Append($"aof_rewrite_in_progress:{(_rewriteInProgress ? 1 : 0)}\r\n");
            sb.Append("aof_rewrite_scheduled:0\r\n");
            sb.Append($"aof_last_rewrite_time_sec:{(_lastRewriteDurationMs < 0 ? -1 : _lastRewriteDurationMs / 1000)}\r\n");

            sb.Append(
                "aof_current_rewrite_time_sec:" +
                (_rewriteInProgress
                    ? (long)Stopwatch.GetElapsedTime(_rewriteStartedTimestamp).TotalSeconds
                    : -1) +
                "\r\n");

            sb.Append($"aof_last_bgrewrite_status:{(_lastRewriteOk ? "ok" : "err")}\r\n");
            sb.Append($"aof_rewrites:{Interlocked.Read(ref _rewrites)}\r\n");
            sb.Append($"aof_last_write_status:{(_lastWriteOk ? "ok" : "err")}\r\n");
            sb.Append($"aof_current_size:{CurrentSize}\r\n");
            sb.Append($"aof_base_size:{Interlocked.Read(ref _baseSize)}\r\n");
            sb.Append($"aof_fsync_policy:{Policy.ToString().ToLowerInvariant()}\r\n");
            sb.Append($"aof_buffer_length:{Math.Max(0, _backlog.EndOffset - Interlocked.Read(ref _writtenOffset))}\r\n");
            sb.Append($"aof_pending_bio_fsync:0\r\n");
            sb.Append($"aof_delayed_fsync:{Interlocked.Read(ref _delayedFsyncs)}\r\n");
        }

        private void WriterLoop()
        {
            while (_running)
            {
                try
                {
                    _backlog.WaitForData(Interlocked.Read(ref _writtenOffset), 100);

                    WriteAvailable();
                    MaybeFsync();
                    ProcessPendingSwitch();
                    MaybeStartAutoRewrite();

                    _lastWriteOk = true;
                }
                catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
                {
                    _lastWriteOk = false;

                    _logger.LogError(
                        ex,
                        "Error writing the AOF; retrying (writes are " +
                        "held in memory until the disk accepts them)");

                    Thread.Sleep(1000);
                }
            }

            try
            {
                lock (_fileLock)
                {
                    WriteAvailableLocked();
                    _file?.Flush(flushToDisk: true);
                }

                MarkDurable(Interlocked.Read(ref _writtenOffset));
            }
            catch (Exception ex) when (ex is IOException or ObjectDisposedException)
            {
                _logger.LogError(ex, "Error flushing the AOF on shutdown");
            }
        }

        private void WriteAvailable()
        {
            lock (_fileLock)
                WriteAvailableLocked();
        }

        // Appends everything the stream has beyond what's already in
        // the file, as one write() to the OS.
        private void WriteAvailableLocked()
        {
            var file = _file!;

            if (_backlog.Generation != _streamGeneration)
            {
                // A replica just replaced its whole dataset with a
                // snapshot from its master; the stream restarted at
                // that snapshot's offset. Follow it from there and
                // rewrite the file, whose old contents describe a
                // dataset that no longer exists.
                _streamGeneration = _backlog.Generation;

                var restart = _backlog.StartOffset;

                Interlocked.Exchange(ref _writtenOffset, restart);
                MarkDurable(restart);
                _cursor!.Position = restart;
                _rewriteAfterReset = true;
            }

            var position = Interlocked.Read(ref _writtenOffset);
            var wrote = false;

            while (true)
            {
                var data = _backlog.Read(position, 1024 * 1024);

                if (data.IsEmpty)
                    break;

                file.Write(data.Span);
                position += data.Length;
                Interlocked.Add(ref _fileSize, data.Length);
                wrote = true;
            }

            if (!wrote)
                return;

            // Out of the stream's buffer into the OS: from here a crash
            // of this process can't lose it.
            file.Flush(flushToDisk: false);
            Interlocked.Exchange(ref _writtenOffset, position);
            _cursor!.Position = position;

            if (Policy == AofFsyncPolicy.No)
            {
                MarkDurable(position);
            }
            else if (Policy == AofFsyncPolicy.EverySec)
            {
                // Clients waiting for their write to reach the OS.
                Signal();
            }
        }

        private void Signal()
        {
            TaskCompletionSource signal;

            lock (_durableLock)
            {
                signal = _durableSignal;
                _durableSignal = new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously);
            }

            signal.TrySetResult();
        }

        private void MaybeFsync()
        {
            var written = Interlocked.Read(ref _writtenOffset);

            if (written <= Interlocked.Read(ref _durableOffset))
                return;

            switch (Policy)
            {
                case AofFsyncPolicy.Always:
                    _file!.Flush(flushToDisk: true);
                    MarkDurable(written);
                    break;

                case AofFsyncPolicy.EverySec:
                    var elapsed = Stopwatch.GetElapsedTime(_lastFsyncTimestamp);

                    if (elapsed < TimeSpan.FromSeconds(1))
                        return;

                    if (elapsed > TimeSpan.FromSeconds(2))
                        Interlocked.Increment(ref _delayedFsyncs);

                    _file!.Flush(flushToDisk: true);
                    _lastFsyncTimestamp = Stopwatch.GetTimestamp();
                    MarkDurable(written);
                    break;
            }
        }

        private void MarkDurable(long offset)
        {
            TaskCompletionSource signal;

            lock (_durableLock)
            {
                if (offset > Interlocked.Read(ref _durableOffset))
                    Interlocked.Exchange(ref _durableOffset, offset);

                signal = _durableSignal;
                _durableSignal = new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously);
            }

            signal.TrySetResult();
        }

        private void MaybeStartAutoRewrite()
        {
            if (_rewriteAfterReset && !_rewriteInProgress)
            {
                _rewriteAfterReset = false;

                _logger.LogInformation(
                    "Rewriting the AOF after a full resync replaced the dataset");

                StartRewrite();
                return;
            }

            var percentage = _options.AofRewritePercentage;

            if (percentage <= 0 || _rewriteInProgress)
                return;

            var size = CurrentSize;
            var baseSize = Math.Max(Interlocked.Read(ref _baseSize), 1);

            if (size < _options.AofRewriteMinBytes)
                return;

            if (size * 100 < baseSize * (100 + percentage))
                return;

            _logger.LogInformation(
                "Starting automatic AOF rewrite ({Size} bytes, " +
                "{Growth}% larger than after the last rewrite)",
                size,
                (size - baseSize) * 100 / baseSize);

            StartRewrite();
        }

        private async Task RewriteAsync()
        {
            SnapshotResult? snapshot = null;
            FileStream? temp = null;
            var generation = _backlog.Generation;

            try
            {
                snapshot = await _snapshots.CreateAsync(
                    TempRewritePath,
                    "aof-rewrite",
                    CancellationToken.None).ConfigureAwait(false);

                temp = new FileStream(
                    TempRewritePath,
                    FileMode.Append,
                    FileAccess.Write,
                    FileShare.Read,
                    bufferSize: 1024 * 1024);

                // Catch the new file up with everything written since
                // the snapshot, off the writer thread, so the final
                // hand-off only has a little left to copy.
                var position = snapshot.Offset;

                for (var pass = 0; pass < 8; pass++)
                {
                    position = CopyStream(temp, position);
                    snapshot.Cursor.Position = position;

                    if (_backlog.EndOffset - position < 1024 * 1024)
                        break;
                }

                var pending = new PendingSwitch(temp, position, snapshot, generation);
                _pendingSwitch = pending;
                _backlog.Signal();

                await pending.Completion.Task.ConfigureAwait(false);
                temp = null;

                _lastRewriteOk = true;
                Interlocked.Increment(ref _rewrites);

                _logger.LogInformation(
                    "AOF rewrite complete: {Keys} keys, {Size} bytes " +
                    "(was {OldSize} bytes before the rewrite started)",
                    snapshot.Keys,
                    CurrentSize,
                    pending.PreviousSize);
            }
            catch (Exception ex)
            {
                _lastRewriteOk = false;
                _logger.LogError(ex, "AOF rewrite failed; the existing AOF is unchanged");

                temp?.Dispose();
                TryDelete(TempRewritePath);
            }
            finally
            {
                if (snapshot is not null)
                    _backlog.Unregister(snapshot.Cursor);

                _lastRewriteDurationMs =
                    (long)Stopwatch.GetElapsedTime(_rewriteStartedTimestamp).TotalMilliseconds;

                _rewriteInProgress = false;
            }
        }

        private long CopyStream(FileStream destination, long position)
        {
            while (true)
            {
                var data = _backlog.Read(position, 1024 * 1024);

                if (data.IsEmpty)
                    return position;

                destination.Write(data.Span);
                position += data.Length;
            }
        }

        // Runs on the writer thread, so nothing is appending to the old
        // file while it's replaced.
        private void ProcessPendingSwitch()
        {
            var pending = _pendingSwitch;

            if (pending is null)
                return;

            _pendingSwitch = null;

            // Held for the whole swap: no inline client write may touch
            // the old file once it's been superseded.
            lock (_fileLock)
                SwitchLocked(pending);
        }

        private void SwitchLocked(PendingSwitch pending)
        {
            try
            {
                WriteAvailableLocked();

                if (_backlog.Generation != pending.Generation)
                {
                    // The dataset was replaced (full resync) while this
                    // rewrite was running; its snapshot is of data that
                    // no longer exists. Throw it away and start over.
                    pending.Temp.Dispose();
                    _rewriteAfterReset = true;

                    throw new InvalidOperationException(
                        "Dataset replaced by a full resync during the rewrite");
                }

                var written = Interlocked.Read(ref _writtenOffset);
                var position = CopyStream(pending.Temp, pending.Position);

                // The temp file may already be ahead of the old file
                // (its catch-up read the stream independently); either
                // way it now covers everything up to `position`.
                position = Math.Max(position, written);

                pending.Temp.Flush(flushToDisk: true);
                pending.Temp.Dispose();

                pending.PreviousSize = CurrentSize;

                _file!.Dispose();
                _file = null;

                MoveWithRetry(TempRewritePath, Path);

                _file = OpenForAppend();

                var size = _file.Length;

                Interlocked.Exchange(ref _fileSize, size);
                Interlocked.Exchange(ref _baseSize, size);
                Interlocked.Exchange(ref _writtenOffset, position);
                _cursor!.Position = position;
                MarkDurable(position);

                pending.Completion.TrySetResult();
            }
            catch (Exception ex)
            {
                // Make sure appends continue to *some* file.
                _file ??= OpenForAppend();
                pending.Completion.TrySetException(ex);
            }
        }

        private FileStream OpenForAppend()
            => new(
                Path,
                FileMode.Append,
                FileAccess.Write,
                FileShare.Read,
                bufferSize: 64 * 1024);

        private static void MoveWithRetry(string source, string destination)
        {
            // Antivirus/indexers on Windows can hold a fresh file open
            // for a moment; the rename is retried briefly.
            for (var attempt = 0; ; attempt++)
            {
                try
                {
                    File.Move(source, destination, overwrite: true);
                    return;
                }
                catch (IOException) when (attempt < 20)
                {
                    Thread.Sleep(50);
                }
                catch (UnauthorizedAccessException) when (attempt < 20)
                {
                    Thread.Sleep(50);
                }
            }
        }

        private static void TryDelete(string path)
        {
            try
            {
                if (File.Exists(path))
                    File.Delete(path);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        public void Dispose()
        {
            if (!_running)
                return;

            _running = false;
            _backlog.Signal();
            _thread?.Join(TimeSpan.FromSeconds(10));

            lock (_fileLock)
                _file?.Dispose();

            if (_cursor is not null)
                _backlog.Unregister(_cursor);

            // Anyone still waiting on an fsync gets released (the
            // final flush above already ran).
            MarkDurable(long.MaxValue);
        }

        private sealed class PendingSwitch
        {
            public PendingSwitch(
                FileStream temp,
                long position,
                SnapshotResult snapshot,
                int generation)
            {
                Temp = temp;
                Position = position;
                Snapshot = snapshot;
                Generation = generation;
            }

            public int Generation { get; }

            public FileStream Temp { get; }

            public long Position { get; }

            public SnapshotResult Snapshot { get; }

            public long PreviousSize { get; set; }

            public TaskCompletionSource Completion { get; } =
                new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }
}
