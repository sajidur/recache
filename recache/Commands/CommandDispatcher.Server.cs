using recache.Clustering;
using recache.Protocol;
using recache.Replication;
using recache.Security;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;

namespace recache.Commands
{
    public sealed partial class CommandDispatcher
    {
        private static readonly string[] InfoSections =
        {
            "server", "clients", "memory", "persistence", "stats",
            "replication", "cpu", "gc", "cluster", "keyspace",
        };

        private ValueTask<RespValue> Info(
            IReadOnlyList<RespValue> args)
        {
            var requested = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            for (var i = 1; i < args.Count; i++)
                requested.Add(GetString(args[i]));

            var all = requested.Count == 0 ||
                      requested.Contains("all") ||
                      requested.Contains("everything") ||
                      requested.Contains("default");

            var sb = new StringBuilder();

            foreach (var section in InfoSections)
            {
                if (!all && !requested.Contains(section))
                    continue;

                if (sb.Length > 0)
                    sb.Append("\r\n");

                AppendInfoSection(sb, section);
            }

            return ValueTask.FromResult(
                RespValue.Bulk(
                    Encoding.UTF8.GetBytes(sb.ToString())));
        }

        private void AppendInfoSection(StringBuilder sb, string section)
        {
            var uptime =
                (long)(DateTimeOffset.UtcNow - _startedAt)
                    .TotalSeconds;

            switch (section)
            {
                case "server":
                    sb.Append("# Server\r\n");
                    sb.Append("redis_version:7.4.0\r\n");
                    sb.Append($"recache_version:{typeof(CommandDispatcher).Assembly.GetName().Version}\r\n");

                    sb.Append(
                        $"redis_mode:{(_cluster is null ? "standalone" : "cluster")}\r\n");

                    sb.Append($"os:{Environment.OSVersion}\r\n");
                    sb.Append("arch_bits:64\r\n");
                    sb.Append($"process_id:{Environment.ProcessId}\r\n");
                    sb.Append($"run_id:{_runId}\r\n");
                    sb.Append($"tcp_port:{_options.Port}\r\n");
                    sb.Append($"uptime_in_seconds:{uptime}\r\n");
                    sb.Append($"uptime_in_days:{uptime / 86400}\r\n");
                    sb.Append($"io_threads_active:1\r\n");
                    break;

                case "clients":
                    sb.Append("# Clients\r\n");
                    sb.Append($"connected_clients:{Metrics?.ConnectedClients ?? 1}\r\n");
                    sb.Append("blocked_clients:0\r\n");
                    sb.Append($"maxclients:{_options.MaxConnections}\r\n");
                    sb.Append($"pubsub_clients:0\r\n");
                    break;

                case "memory":
                {
                    var used = _store.MemoryBytes;
                    using var process = Process.GetCurrentProcess();

                    sb.Append("# Memory\r\n");
                    sb.Append($"used_memory:{used}\r\n");
                    sb.Append($"used_memory_human:{HumanBytes(used)}\r\n");
                    sb.Append($"used_memory_rss:{process.WorkingSet64}\r\n");
                    sb.Append($"used_memory_rss_human:{HumanBytes(process.WorkingSet64)}\r\n");
                    sb.Append($"used_memory_peak:{process.PeakWorkingSet64}\r\n");
                    sb.Append($"maxmemory:{_options.MaxMemoryBytes}\r\n");
                    sb.Append($"maxmemory_human:{HumanBytes(_options.MaxMemoryBytes)}\r\n");
                    sb.Append("maxmemory_policy:noeviction\r\n");
                    sb.Append($"mem_replication_backlog:{Replication?.Backlog.RetainedBytes ?? 0}\r\n");
                    break;
                }

                case "persistence":
                    sb.Append("# Persistence\r\n");
                    sb.Append($"loading:{(Replication?.Loading == true ? 1 : 0)}\r\n");
                    sb.Append($"async_loading:0\r\n");
                    sb.Append("rdb_changes_since_last_save:0\r\n");
                    sb.Append("rdb_bgsave_in_progress:0\r\n");
                    sb.Append("rdb_last_bgsave_status:ok\r\n");

                    if (Aof is { } aof)
                        aof.AppendInfo(sb);
                    else
                        sb.Append("aof_enabled:0\r\naof_rewrite_in_progress:0\r\n");

                    break;

                case "stats":
                    sb.Append("# Stats\r\n");
                    sb.Append($"total_connections_received:{Metrics?.ConnectionsReceived ?? 0}\r\n");
                    sb.Append($"total_commands_processed:{Metrics?.CommandsProcessed ?? 0}\r\n");
                    sb.Append($"rejected_connections:{Metrics?.RejectedConnections ?? 0}\r\n");
                    sb.Append($"sync_full:{Replication?.FullSyncs ?? 0}\r\n");
                    sb.Append($"sync_partial_ok:{Replication?.PartialSyncs ?? 0}\r\n");
                    sb.Append($"expired_keys:0\r\n");
                    break;

                case "replication":
                    sb.Append("# Replication\r\n");

                    if (Replication is { } replication)
                    {
                        replication.AppendInfo(sb);
                    }
                    else
                    {
                        sb.Append("role:master\r\nconnected_slaves:0\r\n");
                    }

                    break;

                case "cpu":
                {
                    using var process = Process.GetCurrentProcess();

                    sb.Append("# CPU\r\n");

                    sb.Append(
                        $"used_cpu_sys:" +
                        $"{process.PrivilegedProcessorTime.TotalSeconds.ToString("F6", CultureInfo.InvariantCulture)}\r\n");

                    sb.Append(
                        $"used_cpu_user:" +
                        $"{process.UserProcessorTime.TotalSeconds.ToString("F6", CultureInfo.InvariantCulture)}\r\n");

                    break;
                }

                case "gc":
                    sb.Append("# GC\r\n");
                    Metrics?.AppendGcInfo(sb);
                    break;

                case "cluster":
                    sb.Append("# Cluster\r\n");
                    sb.Append($"cluster_enabled:{(_cluster is null ? 0 : 1)}\r\n");
                    break;

                case "keyspace":
                    sb.Append("# Keyspace\r\n");

                    if (_store.Count > 0)
                    {
                        sb.Append(
                            $"db0:keys={_store.Count},expires={_store.ExpiringCount},avg_ttl=0\r\n");
                    }

                    break;
            }
        }

        private static string HumanBytes(long bytes)
        {
            string[] units = { "B", "K", "M", "G", "T" };
            double value = bytes;
            var unit = 0;

            while (value >= 1024 && unit < units.Length - 1)
            {
                value /= 1024;
                unit++;
            }

            return unit == 0
                ? $"{bytes}B"
                : value.ToString("0.00", CultureInfo.InvariantCulture) + units[unit];
        }

        private ValueTask<RespValue> Client(
            IReadOnlyList<RespValue> args,
            ClientSession? session)
        {
            var sub = GetString(args[1]).ToUpperInvariant();

            switch (sub)
            {
                case "SETNAME":
                    if (args.Count != 3)
                        return Error("ERR wrong number of arguments for 'client|setname' command");

                    if (session is not null)
                        session.Name = GetString(args[2]);

                    return ValueTask.FromResult(RespValue.Ok);

                case "GETNAME":
                    return ValueTask.FromResult(
                        session?.Name is { } name
                            ? RespValue.Bulk(name)
                            : RespValue.Null());

                case "ID":
                    return ValueTask.FromResult(RespValue.Integer(session?.Id ?? 0));

                case "SETINFO":
                case "NO-EVICT":
                case "NO-TOUCH":
                case "REPLY":
                    return ValueTask.FromResult(RespValue.Ok);

                case "INFO":
                case "LIST":
                    return ValueTask.FromResult(
                        RespValue.Bulk(
                            session is null
                                ? ""
                                : $"id={session.Id} addr={session.RemoteAddress} " +
                                  $"name={session.Name} db=0 " +
                                  $"multi={(session.InTransaction ? session.QueuedCommands.Count : -1)} " +
                                  $"watch={session.WatchedKeys.Count} cmd=client|{sub.ToLowerInvariant()}\n"));

                // CLIENT PAUSE timeout [WRITE|ALL]: clients' commands
                // wait (they are not failed) until the timeout passes
                // or CLIENT UNPAUSE.
                case "PAUSE":
                {
                    if (args.Count < 3 ||
                        !long.TryParse(GetString(args[2]), out var timeoutMs) ||
                        timeoutMs < 0)
                    {
                        return Error("ERR timeout is not an integer or out of range");
                    }

                    var all = true;

                    if (args.Count >= 4)
                    {
                        var mode = GetString(args[3]).ToUpperInvariant();

                        if (mode == "WRITE")
                            all = false;
                        else if (mode != "ALL")
                            return Error("ERR syntax error");
                    }

                    if (WriteGate is null)
                        return Error("ERR CLIENT PAUSE is not available");

                    WriteGate.Pause(TimeSpan.FromMilliseconds(timeoutMs), all);
                    return ValueTask.FromResult(RespValue.Ok);
                }

                case "UNPAUSE":
                    WriteGate?.Resume();
                    return ValueTask.FromResult(RespValue.Ok);

                // CLIENT KILL TYPE master|replica|slave — drops
                // replication links (they reconnect on their own,
                // normally with a partial resync).
                case "KILL":
                {
                    if (args.Count != 4 ||
                        !GetString(args[2]).Equals("TYPE", StringComparison.OrdinalIgnoreCase))
                    {
                        return Error("ERR syntax error (supported: CLIENT KILL TYPE master|replica)");
                    }

                    var type = GetString(args[3]).ToLowerInvariant();

                    switch (type)
                    {
                        case "master":
                            return ValueTask.FromResult(
                                RespValue.Integer(Replication?.DropMasterLink() == true ? 1 : 0));

                        case "replica":
                        case "slave":
                            return ValueTask.FromResult(
                                RespValue.Integer(Replication?.DisconnectAllReplicas() ?? 0));

                        default:
                            return Error($"ERR Unknown client type '{type}'");
                    }
                }

                default:
                    return Error(
                        $"ERR Unknown CLIENT subcommand '{sub}'");
            }
        }

        // The settings CONFIG GET reports, read live from the options
        // actually in effect.
        private IEnumerable<KeyValuePair<string, string>> ConfigValues()
        {
            yield return new("port", _options.Port.ToString(CultureInfo.InvariantCulture));
            yield return new("bind", _options.Host);
            yield return new("databases", _options.DatabaseCount.ToString(CultureInfo.InvariantCulture));
            yield return new("maxclients", _options.MaxConnections.ToString(CultureInfo.InvariantCulture));
            yield return new("maxmemory", _options.MaxMemoryBytes.ToString(CultureInfo.InvariantCulture));
            yield return new("maxmemory-policy", "noeviction");
            yield return new("proto-max-bulk-len", _options.MaxRequestBytes.ToString(CultureInfo.InvariantCulture));
            yield return new("appendonly", _options.EnableAof ? "yes" : "no");
            yield return new("appendfsync", (Aof?.Policy.ToString() ?? _options.AofFsync).ToLowerInvariant());
            yield return new("appendfilename", System.IO.Path.GetFileName(_options.AofPath));
            yield return new("auto-aof-rewrite-percentage", _options.AofRewritePercentage.ToString(CultureInfo.InvariantCulture));
            yield return new("auto-aof-rewrite-min-size", _options.AofRewriteMinBytes.ToString(CultureInfo.InvariantCulture));
            yield return new("aof-load-truncated", _options.AofLoadTruncated ? "yes" : "no");
            yield return new("save", "");
            yield return new("timeout", "0");
            yield return new("repl-backlog-size", _options.ReplicationBacklogBytes.ToString(CultureInfo.InvariantCulture));
            yield return new("repl-ping-replica-period", _options.ReplicaPingPeriodSeconds.ToString(CultureInfo.InvariantCulture));
            yield return new("repl-timeout", _options.ReplicationTimeoutSeconds.ToString(CultureInfo.InvariantCulture));
            yield return new("replica-read-only", "yes");

            yield return new(
                "replicaof",
                Replication is { IsReplica: true } r ? $"{r.MasterHost} {r.MasterPort}" : "");

            yield return new("cluster-enabled", _cluster is null ? "no" : "yes");
            yield return new("cluster-config-file", _options.ClusterConfigFile);
            yield return new("cluster-node-timeout", _options.ClusterNodeTimeoutMs.ToString(CultureInfo.InvariantCulture));
            yield return new("cluster-replica-no-failover", _options.ClusterReplicaNoFailover ? "yes" : "no");
            yield return new("cluster-require-full-coverage", _options.ClusterRequireFullCoverage ? "yes" : "no");
            yield return new("cluster-allow-reads-when-down", _options.ClusterAllowReadsWhenDown ? "yes" : "no");
            yield return new("cluster-announce-ip", _options.ClusterAnnounceIp ?? "");
            yield return new("cluster-announce-port", _options.ClusterAnnouncePort.ToString(CultureInfo.InvariantCulture));
        }

        private ValueTask<RespValue> Config(
            IReadOnlyList<RespValue> args)
        {
            var sub = GetString(args[1]).ToUpperInvariant();

            switch (sub)
            {
                case "GET":
                {
                    if (args.Count < 3)
                    {
                        return Error(
                            "ERR wrong number of arguments " +
                            "for 'config|get' command");
                    }

                    var results = new List<RespValue>();

                    foreach (var (name, value) in ConfigValues())
                    {
                        for (var i = 2; i < args.Count; i++)
                        {
                            if (GlobMatch(GetString(args[i]).ToLowerInvariant(), name))
                            {
                                results.Add(RespValue.Bulk(name));
                                results.Add(RespValue.Bulk(value));
                                break;
                            }
                        }
                    }

                    return ValueTask.FromResult(RespValue.Array(results));
                }

                // Settings come from appsettings.yaml / environment;
                // runtime changes are accepted and ignored so tools
                // that probe with CONFIG SET don't fail.
                case "SET":
                case "RESETSTAT":
                case "REWRITE":
                    return ValueTask.FromResult(RespValue.Ok);

                default:
                    return Error(
                        $"ERR Unknown CONFIG subcommand '{sub}'");
            }
        }

        // COMMAND / COMMAND COUNT / COMMAND INFO / COMMAND LIST /
        // COMMAND GETKEYS. Cluster-aware clients (redis-py's
        // RedisCluster, for one) route by the key positions reported
        // here.
        private static ValueTask<RespValue> Command(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count == 1)
            {
                return ValueTask.FromResult(
                    RespValue.Array(
                        CommandTable.All.Select(DescribeCommand).ToArray()));
            }

            var sub = GetString(args[1]).ToUpperInvariant();

            switch (sub)
            {
                case "COUNT":
                    return ValueTask.FromResult(RespValue.Integer(CommandTable.Count));

                case "LIST":
                    return ValueTask.FromResult(
                        RespValue.Array(
                            CommandTable.All
                                .Select(c => RespValue.Bulk(c.Name.ToLowerInvariant()))
                                .ToArray()));

                case "INFO":
                {
                    var infos = args.Count == 2
                        ? CommandTable.All.Select(DescribeCommand).ToArray()
                        : args.Skip(2)
                            .Select(a => CommandTable.Lookup(GetString(a)) is { } info
                                ? DescribeCommand(info)
                                : RespValue.NullArray())
                            .ToArray();

                    return ValueTask.FromResult(RespValue.Array(infos));
                }

                case "DOCS":
                    return ValueTask.FromResult(RespValue.EmptyArray);

                case "GETKEYS":
                {
                    if (args.Count < 3)
                        return Error("ERR wrong number of arguments for 'command|getkeys' command");

                    var target = args.Skip(2).ToArray();

                    if (CommandTable.Lookup(GetString(target[0])) is not { } info)
                        return Error("ERR Invalid command specified");

                    if (!info.HasKeys)
                        return Error("ERR The command has no key arguments");

                    return ValueTask.FromResult(
                        RespValue.Array(
                            info.GetKeyIndexes(target)
                                .Select(i => target[i])
                                .ToArray()));
                }

                default:
                    return ValueTask.FromResult(RespValue.EmptyArray);
            }
        }

        private static RespValue DescribeCommand(CommandInfo info)
        {
            var flags = new List<RespValue>();

            if (info.IsWrite)
            {
                flags.Add(RespValue.Simple("write"));
                flags.Add(RespValue.Simple("denyoom"));
            }

            if ((info.Flags & CommandFlags.ReadOnly) != 0)
                flags.Add(RespValue.Simple("readonly"));

            if ((info.Flags & CommandFlags.Admin) != 0)
                flags.Add(RespValue.Simple("admin"));

            if ((info.Flags & CommandFlags.Fast) != 0)
                flags.Add(RespValue.Simple("fast"));

            if (info.KeyExtractor is not null)
                flags.Add(RespValue.Simple("movablekeys"));

            return RespValue.Array(new[]
            {
                RespValue.Bulk(info.Name.ToLowerInvariant()),
                RespValue.Integer(info.Arity),
                RespValue.Array(flags),
                RespValue.Integer(info.FirstKey),
                RespValue.Integer(info.LastKey),
                RespValue.Integer(info.Step),
                RespValue.EmptyArray,
                RespValue.EmptyArray,
                RespValue.EmptyArray,
                RespValue.EmptyArray,
            });
        }

        // Set by Program to stop the host gracefully — the same path as
        // a service stop or Ctrl+C: cluster handover first, then the
        // final AOF fsync.
        public Action? ShutdownRequested { get; set; }

        private ValueTask<RespValue> Shutdown()
        {
            if (ShutdownRequested is null)
                return Error("ERR SHUTDOWN is not available");

            ShutdownRequested();
            return ValueTask.FromResult(RespValue.Ok);
        }

        private ValueTask<RespValue> BgRewriteAof()
        {
            if (Aof is null)
            {
                return Error(
                    "ERR AOF is disabled (set Recache:EnableAof: true)");
            }

            return Aof.StartRewrite()
                ? ValueTask.FromResult(
                    RespValue.Simple("Background append only file rewriting started"))
                : Error(
                    "ERR Background append only file rewriting already in progress");
        }
    }
}
