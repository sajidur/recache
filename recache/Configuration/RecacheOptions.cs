using System;
using System.Collections.Generic;
using System.Text;

namespace recache.Configuration
{
        public sealed class RecacheOptions
        {
            public string Host { get; set; } = "0.0.0.0";

            public int Port { get; set; } = 6380;

            public int MaxConnections { get; set; } = 10_000;

            public int MaxRequestBytes { get; set; } = 16 * 1024 * 1024;

            public int MaxValueBytes { get; set; } = 8 * 1024 * 1024;

            public long MaxMemoryBytes { get; set; } =
                4L * 1024 * 1024 * 1024;

            public string? Password { get; set; }

            public bool RequireAuth { get; set; } = false;

            public bool EnableAof { get; set; } = false;

            public string AofPath { get; set; } =
                "/data/appendonly.aof";

            public int DatabaseCount { get; set; } = 16;

            public bool EnableFileLogging { get; set; } = true;

            // Relative paths resolve against the content root (the
            // executable's own folder for a Windows Service or
            // container, the project folder for `dotnet run`).
            public string LogDirectory { get; set; } = "logs";

            // One log file per day; files older than this are
            // deleted automatically.
            public int LogRetentionDays { get; set; } = 7;

            // Turns on hash-slot cluster mode: CLUSTER commands,
            // MOVED/ASK redirection, and CROSSSLOT/CLUSTERDOWN
            // enforcement on every key-bearing command. Off by
            // default — a standalone node behaves exactly as before.
            public bool ClusterEnabled { get; set; } = false;

            // Relative paths resolve against the content root, same
            // as LogDirectory. Stores this node's id, role, and slot
            // ownership so identity survives restarts.
            public string ClusterConfigFile { get; set; } = "nodes.conf";

            // The host:port other nodes should use to reach this one
            // in MOVED/ASK replies and CLUSTER NODES/SLOTS output.
            // Defaults to Host/Port, but those are often "0.0.0.0" or
            // a container-internal address that peers can't dial.
            public string? ClusterAnnounceIp { get; set; }

            public int ClusterAnnouncePort { get; set; } = 0;
        }
    }
