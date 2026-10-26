using recache.Clustering;
using recache.Protocol;
using recache.Security;
using recache.Storage;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace recache.Commands
{
    public sealed partial class CommandDispatcher
    {
        // Computes the slot for a key-bearing command's key(s) and
        // decides whether this node may serve it. Returns null to let
        // the command proceed normally.
        private RespValue? CheckClusterRedirect(
            CommandInfo info,
            IReadOnlyList<RespValue> args,
            ClientSession session)
        {
            if (!info.HasKeys)
                return null;

            var keyIndexes = info.GetKeyIndexes(args);

            if (keyIndexes.Count == 0)
                return null;

            var asking = session.Asking || info.Name == "RESTORE-ASKING";
            session.Asking = false;

            var slot = -1;

            foreach (var index in keyIndexes)
            {
                var keySlot = HashSlot.Compute(KeyBytes(args[index]));

                if (slot < 0)
                {
                    slot = keySlot;
                }
                else if (slot != keySlot)
                {
                    return RespValue.Error(
                        "CROSSSLOT Keys in request don't hash to " +
                        "the same slot");
                }
            }

            var cluster = _cluster!;

            if (!cluster.IsStateOk)
            {
                if (!_options.ClusterAllowReadsWhenDown)
                    return RespValue.Error("CLUSTERDOWN The cluster is down");

                if (info.IsWrite)
                {
                    return RespValue.Error(
                        "CLUSTERDOWN The cluster is down and only accepts read commands");
                }
            }

            var owner = cluster.GetOwner(slot);

            if (owner is null)
                return RespValue.Error("CLUSTERDOWN Hash slot not served");

            var self = cluster.Self;
            var migration = cluster.GetMigration(slot);

            if (ReferenceEquals(owner, self))
            {
                // Keys of a MIGRATING slot that already moved are served
                // by the target: send the client there for this one
                // request (ASK, not MOVED — the slot isn't theirs yet).
                if (migration is { State: SlotMigrationState.Migrating } m)
                {
                    // Routing runs before key locks are taken: check
                    // without the lazy-expiry delete.
                    var missing = keyIndexes.Count(i => !_store.IsLive(GetString(args[i]), out _));

                    if (missing > 0)
                    {
                        if (missing < keyIndexes.Count)
                        {
                            return RespValue.Error(
                                "TRYAGAIN Multiple keys request during rehashing of slot");
                        }

                        if (cluster.GetNode(m.PeerNodeId) is { } target)
                            return RespValue.Error($"ASK {slot} {target.Host}:{target.Port}");
                    }
                }

                return null;
            }

            if (asking && migration is { State: SlotMigrationState.Importing })
                return null;

            // After READONLY, a replica serves reads for its master's
            // slots itself (possibly a few ms stale) instead of
            // redirecting them — read scaling, like Redis.
            if (!info.IsWrite &&
                session.ReadOnlyMode &&
                self.IsReplica &&
                self.MasterId == owner.Id)
            {
                return null;
            }

            return RespValue.Error(
                $"MOVED {slot} {owner.Host}:{owner.Port}");
        }

        private ValueTask<RespValue> Cluster(
            IReadOnlyList<RespValue> args,
            ClientSession? session)
        {
            if (_cluster is null)
            {
                return Error(
                    "ERR This instance has cluster support disabled");
            }

            var cluster = _cluster;
            var manager = ClusterManager;
            var sub = GetString(args[1]).ToUpperInvariant();

            switch (sub)
            {
                case "MYID":
                    return ValueTask.FromResult(RespValue.Bulk(cluster.MyId));

                case "MYSHARDID":
                    return ValueTask.FromResult(
                        RespValue.Bulk(cluster.Self.MasterId ?? cluster.MyId));

                case "INFO":
                    return ValueTask.FromResult(ClusterInfo());

                case "NODES":
                    return ValueTask.FromResult(RespValue.Bulk(cluster.ToNodesText()));

                case "SLOTS":
                    return ValueTask.FromResult(ClusterSlotsReply());

                case "SHARDS":
                    return ValueTask.FromResult(ClusterShardsReply());

                case "KEYSLOT":
                    if (args.Count != 3)
                    {
                        return Error(
                            "ERR wrong number of arguments " +
                            "for 'cluster|keyslot' command");
                    }

                    return ValueTask.FromResult(
                        RespValue.Integer(HashSlot.Compute(KeyBytes(args[2]))));

                case "COUNTKEYSINSLOT":
                    if (args.Count != 3 || !TryParseSlot(GetString(args[2]), out var countSlot))
                        return Error("ERR Invalid slot");

                    return ValueTask.FromResult(
                        RespValue.Integer(_store.CountKeysInSlot(countSlot)));

                case "GETKEYSINSLOT":
                {
                    if (args.Count != 4 ||
                        !TryParseSlot(GetString(args[2]), out var keySlot) ||
                        !int.TryParse(GetString(args[3]), out var count) ||
                        count < 0)
                    {
                        return Error("ERR Invalid slot or number of keys");
                    }

                    return ValueTask.FromResult(
                        RespValue.Array(
                            _store.KeysInSlot(keySlot)
                                .Where(k => _store.IsLive(k, out _))
                                .Take(count)
                                .Select(k => RespValue.Bulk(k))
                                .ToArray()));
                }

                case "ADDSLOTS":
                    return ClusterAddSlots(args, ranges: false);

                case "ADDSLOTSRANGE":
                    return ClusterAddSlots(args, ranges: true);

                case "DELSLOTS":
                    return ClusterDelSlots(args, ranges: false);

                case "DELSLOTSRANGE":
                    return ClusterDelSlots(args, ranges: true);

                case "FLUSHSLOTS":
                    if (_store.Count > 0)
                        return Error("ERR DB must be empty to perform CLUSTER FLUSHSLOTS.");

                    cluster.FlushSlots();
                    manager?.BroadcastPong();
                    return ValueTask.FromResult(RespValue.Ok);

                case "SETSLOT":
                    return ClusterSetSlot(args);

                case "MEET":
                {
                    if (args.Count < 4 ||
                        !int.TryParse(GetString(args[3]), out var port) ||
                        port is <= 0 or > 65535)
                    {
                        return Error("ERR Invalid node address specified");
                    }

                    var busPort = 0;

                    if (args.Count >= 5 &&
                        (!int.TryParse(GetString(args[4]), out busPort) || busPort is <= 0 or > 65535))
                    {
                        return Error("ERR Invalid bus port specified");
                    }

                    if (manager is null)
                        return Error("ERR cluster bus is not running");

                    manager.Meet(GetString(args[2]), port, busPort);
                    return ValueTask.FromResult(RespValue.Ok);
                }

                case "FORGET":
                {
                    if (args.Count != 3)
                    {
                        return Error(
                            "ERR wrong number of arguments " +
                            "for 'cluster|forget' command");
                    }

                    var forgetId = GetString(args[2]);

                    if (forgetId == cluster.MyId)
                        return Error("ERR I tried hard but I can't forget myself...");

                    if (forgetId == cluster.Self.MasterId)
                        return Error("ERR Can't forget my master!");

                    var forgotten = manager?.Forget(forgetId) ?? cluster.Forget(forgetId);

                    return forgotten
                        ? ValueTask.FromResult(RespValue.Ok)
                        : Error($"ERR Unknown node {forgetId}");
                }

                case "REPLICATE":
                {
                    if (args.Count != 3)
                    {
                        return Error(
                            "ERR wrong number of arguments " +
                            "for 'cluster|replicate' command");
                    }

                    if (manager is null)
                        return Error("ERR cluster bus is not running");

                    var error = manager.Replicate(GetString(args[2]), _store.Count > 0);

                    return error is null
                        ? ValueTask.FromResult(RespValue.Ok)
                        : Error(error);
                }

                case "REPLICAS":
                case "SLAVES":
                {
                    if (args.Count != 3)
                        return Error("ERR wrong number of arguments for 'cluster|replicas' command");

                    var masterId = GetString(args[2]);

                    if (cluster.GetNode(masterId) is not { } master)
                        return Error($"ERR Unknown node {masterId}");

                    if (!master.IsMaster)
                        return Error("ERR The specified node is not a master");

                    var lines = cluster.ToNodesText()
                        .Split('\n', StringSplitOptions.RemoveEmptyEntries)
                        .Where(line => line.Split(' ') is { Length: > 3 } fields && fields[3] == masterId)
                        .Select(line => RespValue.Bulk(line))
                        .ToArray();

                    return ValueTask.FromResult(RespValue.Array(lines));
                }

                case "RESET":
                {
                    var hard = args.Count >= 3 &&
                        string.Equals(
                            GetString(args[2]),
                            "HARD",
                            StringComparison.OrdinalIgnoreCase);

                    if (cluster.Self.IsMaster && _store.Count > 0)
                    {
                        return Error(
                            "ERR CLUSTER RESET can't be called with master nodes containing keys");
                    }

                    if (cluster.Self.IsReplica)
                    {
                        Replication?.PromoteToMaster();
                        ClearAllKeys();
                    }

                    cluster.Reset(hard);
                    return ValueTask.FromResult(RespValue.Ok);
                }

                case "SET-CONFIG-EPOCH":
                {
                    if (args.Count != 3 ||
                        !long.TryParse(GetString(args[2]), out var epoch) ||
                        epoch < 0)
                    {
                        return Error(
                            "ERR Invalid config epoch specified: " +
                            "must be a non-negative integer");
                    }

                    // Redis's safety rules: only on a fresh node, so
                    // hand-assigned epochs can't collide with ones the
                    // cluster already handed out.
                    if (cluster.Nodes.Count > 1)
                    {
                        return Error(
                            "ERR The user can assign a config epoch only " +
                            "when the node does not know any other node.");
                    }

                    if (cluster.Self.ConfigEpoch != 0)
                        return Error("ERR Node config epoch is already non-zero");

                    cluster.SetConfigEpoch(epoch);
                    return ValueTask.FromResult(RespValue.Ok);
                }

                case "BUMPEPOCH":
                {
                    var newEpoch = cluster.BumpEpoch();
                    manager?.BroadcastPong();

                    return ValueTask.FromResult(
                        RespValue.Simple($"BUMPED {newEpoch}"));
                }

                case "SAVECONFIG":
                    // nodes.conf is written on every topology change
                    // already.
                    return ValueTask.FromResult(RespValue.Ok);

                case "FAILOVER":
                {
                    var mode = ManualFailoverMode.Default;

                    if (args.Count >= 3)
                    {
                        var option = GetString(args[2]).ToUpperInvariant();

                        mode = option switch
                        {
                            "FORCE" => ManualFailoverMode.Force,
                            "TAKEOVER" => ManualFailoverMode.Takeover,
                            _ => (ManualFailoverMode)(-1),
                        };

                        if ((int)mode < 0)
                            return Error("ERR syntax error");
                    }

                    if (manager is null)
                        return Error("ERR cluster bus is not running");

                    var error = manager.StartManualFailover(mode);

                    return error is null
                        ? ValueTask.FromResult(RespValue.Ok)
                        : Error(error);
                }

                case "COUNT-FAILURE-REPORTS":
                {
                    if (args.Count != 3)
                        return Error("ERR wrong number of arguments for 'cluster|count-failure-reports' command");

                    var reports = manager?.CountFailureReports(GetString(args[2])) ?? -1;

                    return reports < 0
                        ? Error($"ERR Unknown node {GetString(args[2])}")
                        : ValueTask.FromResult(RespValue.Integer(reports));
                }

                case "LINKS":
                    return ValueTask.FromResult(RespValue.EmptyArray);

                default:
                    return Error(
                        $"ERR Unknown CLUSTER subcommand '{sub}'");
            }
        }

        private ValueTask<RespValue> ClusterAddSlots(
            IReadOnlyList<RespValue> args,
            bool ranges)
        {
            if (!TryParseSlotArgs(args, ranges, out var slots, out var err))
                return Error(err!);

            var busy = _cluster!.AddSlots(slots);

            if (busy.Count > 0)
            {
                return Error(
                    $"ERR Slot {busy[0]} is already busy");
            }

            ClusterManager?.BroadcastPong();
            return ValueTask.FromResult(RespValue.Simple("OK"));
        }

        private ValueTask<RespValue> ClusterDelSlots(
            IReadOnlyList<RespValue> args,
            bool ranges)
        {
            if (!TryParseSlotArgs(args, ranges, out var slots, out var err))
                return Error(err!);

            var free = _cluster!.RemoveSlots(slots);

            if (free.Count > 0)
            {
                return Error(
                    $"ERR Slot {free[0]} is already unassigned");
            }

            ClusterManager?.BroadcastPong();
            return ValueTask.FromResult(RespValue.Simple("OK"));
        }

        private static bool TryParseSlotArgs(
            IReadOnlyList<RespValue> args,
            bool ranges,
            out List<int> slots,
            out string? error)
        {
            slots = new List<int>();
            error = null;

            if (args.Count < 3 || (ranges && args.Count % 2 != 0))
            {
                error =
                    "ERR wrong number of arguments for 'cluster' command";

                return false;
            }

            if (ranges)
            {
                for (var i = 2; i < args.Count; i += 2)
                {
                    if (!TryParseSlot(GetString(args[i]), out var start) ||
                        !TryParseSlot(
                            GetString(args[i + 1]),
                            out var end) ||
                        start > end)
                    {
                        error = "ERR Invalid slot range";
                        return false;
                    }

                    for (var slot = start; slot <= end; slot++)
                        slots.Add(slot);
                }
            }
            else
            {
                for (var i = 2; i < args.Count; i++)
                {
                    if (!TryParseSlot(GetString(args[i]), out var slot))
                    {
                        error = "ERR Invalid or out of range slot";
                        return false;
                    }

                    slots.Add(slot);
                }
            }

            return true;
        }

        private static bool TryParseSlot(string text, out int slot)
        {
            return int.TryParse(text, out slot) &&
                slot >= 0 &&
                slot < HashSlot.SlotCount;
        }

        private ValueTask<RespValue> ClusterSetSlot(
            IReadOnlyList<RespValue> args)
        {
            if (args.Count < 4 ||
                !TryParseSlot(GetString(args[2]), out var slot))
            {
                return Error(
                    "ERR wrong number of arguments " +
                    "for 'cluster|setslot' command");
            }

            var cluster = _cluster!;
            var mode = GetString(args[3]).ToUpperInvariant();

            switch (mode)
            {
                case "MIGRATING":
                {
                    if (args.Count != 5)
                        return Error("ERR wrong number of arguments");

                    var migrateTarget = GetString(args[4]);

                    if (cluster.GetNode(migrateTarget) is not { } target)
                        return Error($"ERR I don't know about node {migrateTarget}");

                    if (target.IsReplica)
                        return Error("ERR Target node is not a master");

                    if (!ReferenceEquals(cluster.GetOwner(slot), cluster.Self))
                    {
                        return Error(
                            $"ERR I'm not the owner of hash slot {slot}");
                    }

                    cluster.SetMigrating(slot, migrateTarget);
                    return ValueTask.FromResult(RespValue.Simple("OK"));
                }

                case "IMPORTING":
                {
                    if (args.Count != 5)
                        return Error("ERR wrong number of arguments");

                    var importSource = GetString(args[4]);

                    if (cluster.GetNode(importSource) is null)
                        return Error($"ERR I don't know about node {importSource}");

                    if (ReferenceEquals(cluster.GetOwner(slot), cluster.Self))
                    {
                        return Error(
                            $"ERR I'm already the owner of hash " +
                            $"slot {slot}");
                    }

                    cluster.SetImporting(slot, importSource);
                    return ValueTask.FromResult(RespValue.Simple("OK"));
                }

                case "STABLE":
                    cluster.ClearMigration(slot);
                    return ValueTask.FromResult(RespValue.Simple("OK"));

                case "NODE":
                {
                    if (args.Count != 5)
                        return Error("ERR wrong number of arguments");

                    var ownerId = GetString(args[4]);

                    if (cluster.GetNode(ownerId) is not { } newOwner)
                        return Error($"ERR Unknown node {ownerId}");

                    if (newOwner.IsReplica)
                        return Error("ERR Target node is not a master");

                    var self = cluster.Self;
                    var wasImporting = cluster.GetMigration(slot) is { State: SlotMigrationState.Importing };

                    if (ReferenceEquals(cluster.GetOwner(slot), self) &&
                        !ReferenceEquals(newOwner, self) &&
                        _store.CountKeysInSlot(slot) > 0)
                    {
                        return Error(
                            $"ERR Can't assign hashslot {slot} to a different " +
                            "node while I still hold keys for this hash slot.");
                    }

                    cluster.AssignSlot(slot, ownerId);

                    // Finishing an import: claim the slot with a fresh
                    // epoch so this node's ownership beats the old
                    // owner's everywhere, without needing an election.
                    if (ReferenceEquals(newOwner, self) && wasImporting)
                        cluster.BumpEpoch();

                    ClusterManager?.BroadcastPong();
                    return ValueTask.FromResult(RespValue.Simple("OK"));
                }

                default:
                    return Error(
                        $"ERR Invalid CLUSTER SETSLOT action or number of arguments");
            }
        }

        private RespValue ClusterInfo()
        {
            var cluster = _cluster!;
            var self = cluster.Self;

            long assigned = 0, ok = 0, pfail = 0, fail = 0;

            foreach (var (start, end, node) in cluster.SlotRanges())
            {
                var count = end - start + 1;
                assigned += count;

                if (node.IsFailed)
                    fail += count;
                else if (node.IsPFail)
                    pfail += count;
                else
                    ok += count;
            }

            var sb = new StringBuilder();

            sb.Append("cluster_enabled:1\r\n");
            sb.Append($"cluster_state:{(cluster.IsStateOk ? "ok" : "fail")}\r\n");
            sb.Append($"cluster_slots_assigned:{assigned}\r\n");
            sb.Append($"cluster_slots_ok:{ok}\r\n");
            sb.Append($"cluster_slots_pfail:{pfail}\r\n");
            sb.Append($"cluster_slots_fail:{fail}\r\n");
            sb.Append($"cluster_known_nodes:{cluster.Nodes.Count}\r\n");
            sb.Append($"cluster_size:{cluster.Size}\r\n");
            sb.Append($"cluster_current_epoch:{cluster.CurrentEpoch}\r\n");
            sb.Append($"cluster_my_epoch:{self.ConfigEpoch}\r\n");
            sb.Append("cluster_stats_messages_sent:0\r\n");
            sb.Append("cluster_stats_messages_received:0\r\n");
            sb.Append("total_cluster_links_buffer_limit_exceeded:0\r\n");

            return RespValue.Bulk(Encoding.UTF8.GetBytes(sb.ToString()));
        }

        private static RespValue DescribeEndpoint(ClusterNode node)
            => RespValue.Array(new[]
            {
                RespValue.Bulk(node.Host),
                RespValue.Integer(node.Port),
                RespValue.Bulk(node.Id),
                RespValue.EmptyArray,
            });

        private RespValue ClusterSlotsReply()
        {
            var cluster = _cluster!;
            var result = new List<RespValue>();

            foreach (var (start, end, node) in cluster.SlotRanges())
            {
                var entry = new List<RespValue>
                {
                    RespValue.Integer(start),
                    RespValue.Integer(end),
                    DescribeEndpoint(node),
                };

                foreach (var replica in cluster.ReplicasOf(node.Id))
                {
                    if (!replica.IsFailed)
                        entry.Add(DescribeEndpoint(replica));
                }

                result.Add(RespValue.Array(entry));
            }

            return RespValue.Array(result);
        }

        private RespValue ClusterShardsReply()
        {
            var cluster = _cluster!;
            var result = new List<RespValue>();

            foreach (var master in cluster.Nodes.Where(n => n.IsMaster))
            {
                var ranges = cluster.SlotRanges()
                    .Where(r => ReferenceEquals(r.Node, master))
                    .SelectMany(r => new[] { RespValue.Integer(r.Start), RespValue.Integer(r.End) })
                    .ToArray();

                if (ranges.Length == 0)
                    continue;

                var members = new List<RespValue> { DescribeShardNode(master) };

                members.AddRange(
                    cluster.ReplicasOf(master.Id).Select(DescribeShardNode));

                result.Add(RespValue.Array(new[]
                {
                    RespValue.Bulk("slots"),
                    RespValue.Array(ranges),
                    RespValue.Bulk("nodes"),
                    RespValue.Array(members),
                }));
            }

            return RespValue.Array(result);
        }

        private RespValue DescribeShardNode(ClusterNode node)
        {
            var offset = node.IsMyself ? Replication?.Offset ?? 0 : node.ReplOffset;

            return RespValue.Array(new[]
            {
                RespValue.Bulk("id"),
                RespValue.Bulk(node.Id),
                RespValue.Bulk("port"),
                RespValue.Integer(node.Port),
                RespValue.Bulk("ip"),
                RespValue.Bulk(node.Host),
                RespValue.Bulk("endpoint"),
                RespValue.Bulk(node.Host),
                RespValue.Bulk("role"),
                RespValue.Bulk(node.IsMaster ? "master" : "replica"),
                RespValue.Bulk("replication-offset"),
                RespValue.Integer(offset),
                RespValue.Bulk("health"),
                RespValue.Bulk(node.IsFailing ? "fail" : "online"),
            });
        }

        // Drops every key in slots this node no longer owns (after
        // another master claimed them with a newer configuration).
        internal void DeleteKeysInSlots(IReadOnlyList<int> slots)
        {
            var backlog = Replication is { PropagationEnabled: true } replication
                ? replication.Backlog
                : null;

            foreach (var slot in slots)
            {
                foreach (var key in _store.KeysInSlot(slot).ToArray())
                {
                    WithKeyLock(key, write: true, () =>
                    {
                        Snapshots?.BeforeWrite(key);

                        if (_store.Remove(key))
                            backlog?.AppendCommand("DEL", key);
                    });
                }
            }
        }

        private void ClearAllKeys()
        {
            if (_locks is null)
            {
                _store.Clear();
                return;
            }

            _locks.EnterAll();

            try
            {
                Snapshots?.BeforeFlushAll();
                _store.Clear();
            }
            finally
            {
                _locks.ExitAll();
            }
        }
    }
}
