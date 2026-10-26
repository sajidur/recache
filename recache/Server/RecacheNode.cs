using Microsoft.Extensions.Logging;
using recache.Clustering;
using recache.Commands;
using recache.Configuration;
using recache.Monitoring;
using recache.Persistence;
using recache.Replication;
using recache.Security;
using recache.Storage;
using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace recache.Server
{
    // Composition root for one node: builds the store, locks,
    // replication stream, persistence, replication and cluster
    // components in dependency order and wires the cycles between
    // them (the dispatcher needs replication to propagate writes;
    // replication needs the dispatcher to apply its master's).
    //
    // Start() runs the startup sequence before any client is accepted:
    // replay the AOF, start the AOF writer, active expiry, the cluster
    // bus, and replication.
    public sealed class RecacheNode : IDisposable
    {
        private readonly ILogger _logger;
        private readonly string _contentRoot;
        private bool _disposed;

        public RecacheNode(
            RecacheOptions options,
            AuthenticationManager auth,
            ILoggerFactory loggerFactory,
            string contentRoot)
        {
            Options = options;
            _contentRoot = contentRoot;
            _logger = loggerFactory.CreateLogger("recache.Node");

            CommandTable.Initialize();

            Metrics = new RedisMetrics();
            Metrics.StartGcSampling();
            Locks = new KeyLocks();
            Store = new CacheStore(trackSlots: options.ClusterEnabled);

            Backlog = new ReplicationBacklog(
                options.ReplicationBacklogBytes,
                options.ReplicaOutputLimitBytes);

            Snapshots = new SnapshotService(Store, Locks, Backlog);

            var aofPath = Path.IsPathRooted(options.AofPath)
                ? options.AofPath
                : Path.Combine(contentRoot, options.AofPath);

            DataDirectory = Path.GetDirectoryName(aofPath) ?? contentRoot;

            Replication = new ReplicationManager(
                options,
                Store,
                Locks,
                Backlog,
                Snapshots,
                DataDirectory,
                loggerFactory.CreateLogger("recache.Replication"));

            WriteGate = new WriteGate(Locks);

            Cluster = options.ClusterEnabled
                ? new ClusterState(options, contentRoot)
                : null;

            Aof = options.EnableAof
                ? new AofManager(
                    options,
                    contentRoot,
                    Backlog,
                    Snapshots,
                    loggerFactory.CreateLogger("recache.Aof"))
                : null;

            Dispatcher = new CommandDispatcher(Store, auth, options, Cluster, Locks)
            {
                Replication = Replication,
                Snapshots = Snapshots,
                Aof = Aof,
                WriteGate = WriteGate,
                Metrics = Metrics,
            };

            Replication.Dispatcher = Dispatcher;
            Replication.LoaderFactory = side => new CommandDispatcher(side, auth, options);

            if (Cluster is not null)
            {
                ClusterManager = new ClusterManager(
                    options,
                    Cluster,
                    Replication,
                    WriteGate,
                    loggerFactory.CreateLogger("recache.Cluster"))
                {
                    DeleteKeysInSlots = Dispatcher.DeleteKeysInSlots,
                };

                Dispatcher.ClusterManager = ClusterManager;
            }

            Expiry = new ExpirationManager(
                Store,
                Locks,
                Replication,
                loggerFactory.CreateLogger("recache.Expiry"));

            // The stream only costs anything once something consumes
            // it; a plain standalone cache with no AOF and no replicas
            // never pays for it.
            if (Aof is not null)
                Replication.EnablePropagation(retainForPartialResync: false);

            if (Cluster is not null)
                Replication.EnablePropagation(retainForPartialResync: true);
        }

        public RecacheOptions Options { get; }

        public string DataDirectory { get; }

        public RedisMetrics Metrics { get; }

        public KeyLocks Locks { get; }

        public CacheStore Store { get; }

        public ReplicationBacklog Backlog { get; }

        public SnapshotService Snapshots { get; }

        public ReplicationManager Replication { get; }

        public WriteGate WriteGate { get; }

        public ClusterState? Cluster { get; }

        public ClusterManager? ClusterManager { get; }

        public AofManager? Aof { get; }

        public CommandDispatcher Dispatcher { get; }

        public ExpirationManager Expiry { get; }

        public void Start()
        {
            if (Aof is not null)
            {
                LoadAof(Aof);
                Aof.Start(Backlog.EndOffset);
            }

            Expiry.Start();
            ClusterManager?.Start();

            if (Cluster is null && !string.IsNullOrWhiteSpace(Options.ReplicaOf))
            {
                var parts = Options.ReplicaOf.Split(
                    new[] { ' ', ':' },
                    StringSplitOptions.RemoveEmptyEntries);

                if (parts.Length == 2 &&
                    int.TryParse(parts[1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var port))
                {
                    Replication.BecomeReplicaOf(parts[0], port);
                }
                else
                {
                    _logger.LogError(
                        "Recache:ReplicaOf must be \"host port\"; got {Value}",
                        Options.ReplicaOf);
                }
            }

            if (Cluster is not null && !string.IsNullOrWhiteSpace(Options.ReplicaOf))
            {
                _logger.LogWarning(
                    "Recache:ReplicaOf is ignored in cluster mode; " +
                    "use CLUSTER REPLICATE instead");
            }
        }

        private void LoadAof(AofManager aof)
        {
            if (!File.Exists(aof.Path))
                return;

            _logger.LogInformation("Loading AOF {Path}...", aof.Path);

            var stopwatch = Stopwatch.StartNew();
            var session = new ClientSession { IsMasterLink = true, IsAuthenticated = true };

            Replication.Loading = true;

            try
            {
                var result = AofLoader.Load(
                    aof.Path,
                    (args, _) => Dispatcher.ExecuteReplicated(args, session, ReadOnlySpan<byte>.Empty),
                    Options.AofLoadTruncated,
                    truncateFile: true);

                if (result.Truncated)
                {
                    _logger.LogWarning(
                        "AOF ended with an incomplete command or transaction " +
                        "(likely a crash mid-write): dropped the last {Bytes} " +
                        "byte(s) and truncated the file to the last complete command",
                        result.DiscardedBytes);
                }

                _logger.LogInformation(
                    "AOF loaded: {Commands} commands, {Keys} keys in {Elapsed} ms",
                    result.Commands,
                    Store.Count,
                    stopwatch.ElapsedMilliseconds);
            }
            finally
            {
                Replication.Loading = false;
            }
        }

        // Called while clients are still being served, so requests
        // during the handover are redirected rather than refused.
        public Task PrepareShutdownAsync()
        {
            if (ClusterManager is null || !Options.ClusterFailoverOnShutdown)
                return Task.CompletedTask;

            return ClusterManager.HandoffOnShutdownAsync(
                TimeSpan.FromMilliseconds(Options.ClusterFailoverOnShutdownTimeoutMs));
        }

        public void Dispose()
        {
            if (_disposed)
                return;

            _disposed = true;

            ClusterManager?.Dispose();
            Replication.Dispose();
            Expiry.Dispose();

            // Last: flushes and fsyncs everything still buffered.
            Aof?.Dispose();
        }
    }
}
