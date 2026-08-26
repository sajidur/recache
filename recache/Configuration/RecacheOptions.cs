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
        }
    }
