using recache.Protocol;
using recache.Security;
using System;
using System.Collections.Generic;
using System.Globalization;

namespace recache.Commands
{
    public sealed partial class CommandDispatcher
    {
        // REPLICAOF host port | REPLICAOF NO ONE (standalone only;
        // cluster nodes use CLUSTER REPLICATE / CLUSTER FAILOVER).
        private ValueTask<RespValue> ReplicaOf(IReadOnlyList<RespValue> args)
        {
            if (Replication is null)
                return Error("ERR replication is not available");

            if (_cluster is not null)
                return Error("ERR REPLICAOF not allowed in cluster mode.");

            var host = GetString(args[1]);
            var portText = GetString(args[2]);

            if (host.Equals("NO", StringComparison.OrdinalIgnoreCase) &&
                portText.Equals("ONE", StringComparison.OrdinalIgnoreCase))
            {
                Replication.PromoteToMaster();
                return ValueTask.FromResult(RespValue.Ok);
            }

            if (!int.TryParse(portText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var port) ||
                port is <= 0 or > 65535)
            {
                return Error("ERR Invalid master port");
            }

            Replication.BecomeReplicaOf(host, port);
            return ValueTask.FromResult(RespValue.Ok);
        }

        private ValueTask<RespValue> Role()
        {
            return ValueTask.FromResult(
                Replication?.RoleReply() ??
                RespValue.Array(new[]
                {
                    RespValue.Bulk("master"),
                    RespValue.Integer(0),
                    RespValue.EmptyArray,
                }));
        }

        // Sent by a replica during its handshake, before PSYNC.
        private static ValueTask<RespValue> ReplConf(
            IReadOnlyList<RespValue> args,
            ClientSession? session)
        {
            for (var i = 1; i + 1 < args.Count; i += 2)
            {
                var option = GetString(args[i]).ToLowerInvariant();
                var value = GetString(args[i + 1]);

                switch (option)
                {
                    case "listening-port":
                        if (session is not null &&
                            int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var port))
                        {
                            session.ReplicaListeningPort = port;
                        }

                        break;

                    case "ip-address":
                        if (session is not null)
                            session.ReplicaAnnouncedIp = value;

                        break;
                }
            }

            return ValueTask.FromResult(RespValue.Ok);
        }

        private ValueTask<RespValue> Wait(
            IReadOnlyList<RespValue> args,
            ClientSession session,
            CancellationToken cancellationToken)
        {
            if (!int.TryParse(GetString(args[1]), out var numReplicas) ||
                !long.TryParse(GetString(args[2]), out var timeoutMs) ||
                numReplicas < 0 ||
                timeoutMs < 0)
            {
                return Error("ERR value is not an integer or out of range");
            }

            if (session.InTransaction)
                return Error("ERR WAIT is not allowed inside a transaction");

            if (Replication is null)
                return ValueTask.FromResult(RespValue.Integer(0));

            if (Replication.IsReplica)
            {
                return Error(
                    "ERR WAIT cannot be used with replica instances.");
            }

            return Replication.WaitForAcksAsync(
                numReplicas,
                timeoutMs,
                session.LastWriteOffset,
                cancellationToken);
        }

        // PSYNC replid offset / SYNC: the connection loop hands the
        // socket to ReplicationManager after this returns.
        private ValueTask<RespValue> HandlePsync(
            IReadOnlyList<RespValue> args,
            ClientSession session)
        {
            if (Replication is null)
                return Error("ERR replication is not available");

            if (session.InTransaction)
                return Error("ERR Command not allowed inside a transaction");

            session.Handoff = SessionHandoff.Replication;
            session.HandoffArgs = args;

            return ValueTask.FromResult(NoReply);
        }
    }
}
