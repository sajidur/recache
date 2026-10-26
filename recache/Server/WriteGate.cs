using recache.Storage;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace recache.Server
{
    // Pauses client writes (or all commands) without failing them:
    // a paused command simply waits, then goes through every check
    // again — including cluster slot ownership. That second check is
    // what makes a coordinated failover invisible to clients: the old
    // master pauses writes, its replica catches up and takes over the
    // slots, and each waiting write then gets MOVED to the new owner
    // instead of an error.
    //
    // Backs CLIENT PAUSE / UNPAUSE as well.
    public sealed class WriteGate
    {
        private readonly KeyLocks _locks;
        private readonly object _lock = new();

        private TaskCompletionSource? _released;
        private volatile bool _paused;
        private volatile bool _pauseAll;
        private long _generation;

        public WriteGate(KeyLocks locks)
        {
            _locks = locks;
        }

        public bool IsPaused => _paused;

        public bool PausesReads => _paused && _pauseAll;

        public bool Blocks(bool isWrite) => _paused && (isWrite || _pauseAll);

        public Task WaitAsync()
        {
            lock (_lock)
                return _released?.Task ?? Task.CompletedTask;
        }

        // Pauses for at most `duration`. Returns once every write that
        // was already executing has finished, so the replication
        // offset is stable from here until Resume.
        public void Pause(TimeSpan duration, bool all)
        {
            long generation;

            lock (_lock)
            {
                _released ??= new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously);

                _pauseAll |= all;
                _paused = true;
                generation = ++_generation;
            }

            // Commands re-check the flag after taking their key locks,
            // so once every stripe has been free at least once, no
            // write can still slip through.
            _locks.Drain();

            _ = ResumeLaterAsync(duration, generation);
        }

        public void Resume()
        {
            TaskCompletionSource? released;

            lock (_lock)
            {
                released = _released;
                _released = null;
                _paused = false;
                _pauseAll = false;
                _generation++;
            }

            released?.TrySetResult();
        }

        private async Task ResumeLaterAsync(TimeSpan duration, long generation)
        {
            await Task.Delay(duration).ConfigureAwait(false);

            lock (_lock)
            {
                // A newer Pause (or a Resume) superseded this one.
                if (_generation != generation)
                    return;
            }

            Resume();
        }
    }
}
