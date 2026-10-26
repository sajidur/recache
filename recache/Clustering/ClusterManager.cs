using Microsoft.Extensions.Logging;
using recache.Configuration;
using recache.Replication;
using recache.Server;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace recache.Clustering
{
    public enum ManualFailoverMode
    {
        Default,
        Force,
        Takeover
    }

    // The cluster bus: gossip, failure detection, and failover.
    //
    // Every node PINGs every peer (at least every second) and gossips
    // what it knows. A peer that misses PONGs for ClusterNodeTimeoutMs
    // is suspected (PFAIL); once a majority of slot-owning masters
    // report it, it's FAILed cluster-wide. A replica of a FAILed master
    // then asks the masters for votes in a new epoch; with a majority
    // it promotes itself, claims its old master's slots with that
    // higher configEpoch, and every other node — including the old
    // master if it comes back, and its sibling replicas — converges on
    // the new owner because a higher configEpoch always wins.
    //
    // Planned handovers (CLUSTER FAILOVER, or a master being stopped)
    // lose nothing and drop no requests: the master pauses writes,
    // the replica catches up to the master's exact final offset, takes
    // over, and the paused writes are re-routed (MOVED) to it.
    //
    // The design follows Redis Cluster's; the wire format is
    // recache's own (RESP-encoded), so recache and Redis nodes can't
    // share a cluster.
    public sealed class ClusterManager : IDisposable
    {
        private const int ManualFailoverTimeoutMs = 5000;
        private const int ForgetBlacklistMs = 60_000;

        private readonly RecacheOptions _options;
        private readonly ClusterState _state;
        private readonly ReplicationManager _replication;
        private readonly WriteGate _writeGate;
        private readonly ILogger _logger;
        private readonly int _nodeTimeout;
        private readonly CancellationTokenSource _shutdown = new();
        private readonly ConcurrentDictionary<string, ClusterLink> _outbound = new(StringComparer.Ordinal);
        private readonly ConcurrentDictionary<ClusterLink, byte> _links = new();
        private readonly ConcurrentDictionary<string, long> _connecting = new(StringComparer.Ordinal);
        private readonly Dictionary<string, long> _blacklist = new(StringComparer.Ordinal);
        private readonly HashSet<string> _votes = new(StringComparer.Ordinal);
        private readonly Random _random = new();

        private TcpListener? _listener;
        private Task? _cron;
        private Task? _accept;

        // Replica-side election state (guarded by the state lock).
        private long _failoverAuthTime;
        private long _failoverAuthEpoch;
        private bool _failoverAuthSent;
        private bool _failoverScheduled;
        private int _failoverAuthRank;
        private bool _electionLogged;

        // Manual failover. On a replica: _mfEnd > 0 while running,
        // _mfMasterOffset is the paused master's final offset, and
        // _mfCanStart once caught up. On a master: _mfEnd > 0 while
        // writes are paused for _mfReplica.
        private long _mfEnd;
        private long _mfMasterOffset = -1;
        private bool _mfCanStart;
        private string? _mfReplica;

        private bool _lastStateOk = true;
        private long _startedMs;

        public ClusterManager(
            RecacheOptions options,
            ClusterState state,
            ReplicationManager replication,
            WriteGate writeGate,
            ILogger logger)
        {
            _options = options;
            _state = state;
            _replication = replication;
            _writeGate = writeGate;
            _logger = logger;
            _nodeTimeout = Math.Max(500, options.ClusterNodeTimeoutMs);
        }

        // Deletes the keys of slots this node just lost to another
        // master (wired to the dispatcher, which knows locking and
        // propagation).
        public Action<IReadOnlyList<int>>? DeleteKeysInSlots { get; set; }

        public int NodeTimeoutMs => _nodeTimeout;

        public void Start()
        {
            var self = _state.Self;
            var busPort = _options.ClusterBusPort > 0
                ? _options.ClusterBusPort
                : _options.Port + 10000;

            var bindAll = _options.Host == "0.0.0.0";

            _listener = new TcpListener(
                bindAll ? IPAddress.IPv6Any : IPAddress.Parse(_options.Host),
                busPort);

            if (bindAll)
                _listener.Server.DualMode = true;

            _listener.Start(512);
            _startedMs = NowMs;

            _logger.LogInformation(
                "Cluster bus listening on port {BusPort} as node {NodeId} " +
                "({Role}, epoch {Epoch}, node timeout {Timeout} ms)",
                busPort,
                self.Id,
                self.IsMaster ? "master" : $"replica of {self.MasterId}",
                _state.CurrentEpoch,
                _nodeTimeout);

            _accept = AcceptLoopAsync(_shutdown.Token);
            _cron = CronLoopAsync(_shutdown.Token);

            // Resume following our master after a restart.
            if (self.IsReplica && self.MasterId is { } masterId &&
                _state.GetNode(masterId) is { } master)
            {
                _replication.BecomeReplicaOf(master.Host, master.Port);
            }
            else
            {
                _replication.EnablePropagation(retainForPartialResync: true);
            }
        }

        // ---- Public operations (CLUSTER subcommands) ----

        // CLUSTER MEET ip port [bus-port]: asynchronous like Redis —
        // the handshake completes in the background and the node shows
        // up in CLUSTER NODES once it answers.
        public void Meet(string host, int port, int busPort)
        {
            _ = ConnectAsync(
                nodeId: null,
                host,
                busPort > 0 ? busPort : port + 10000,
                handshake: true,
                dialedPort: port);
        }

        public bool Forget(string nodeId)
        {
            lock (_state.SyncRoot)
            {
                if (!_state.Forget(nodeId))
                    return false;

                _blacklist[nodeId] = NowMs + ForgetBlacklistMs;
            }

            if (_outbound.TryRemove(nodeId, out var link))
                link.Close();

            return true;
        }

        // CLUSTER REPLICATE <master-id>. Returns an error message, or
        // null on success.
        public string? Replicate(string masterId, bool hasKeys)
        {
            ClusterNode master;

            lock (_state.SyncRoot)
            {
                var self = _state.Self;

                if (_state.GetNode(masterId) is not { } node)
                    return $"ERR Unknown node {masterId}";

                if (node.Id == self.Id)
                    return "ERR Can't replicate myself";

                if (node.IsReplica)
                    return "ERR I can only replicate a master, not a replica.";

                if (self.IsMaster && (_state.CountSlots(self) > 0 || hasKeys))
                {
                    return
                        "ERR To set a master the node must be empty and " +
                        "without assigned slots.";
                }

                _state.SetRole(ClusterNodeRole.Replica, node.Id);
                master = node;
            }

            _replication.BecomeReplicaOf(master.Host, master.Port);
            BroadcastPong();
            return null;
        }

        // CLUSTER FAILOVER [FORCE|TAKEOVER] on a replica. Returns an
        // error message, or null if the failover was started.
        public string? StartManualFailover(ManualFailoverMode mode)
        {
            lock (_state.SyncRoot)
            {
                var self = _state.Self;

                if (!self.IsReplica)
                    return "ERR You should send CLUSTER FAILOVER to a replica";

                if (self.MasterId is null || _state.GetNode(self.MasterId) is not { } master)
                    return "ERR I'm a replica but my master is unknown to me";

                if (mode == ManualFailoverMode.Default &&
                    (master.IsFailed || !master.LinkConnected))
                {
                    return
                        "ERR Master is down or failed, please use " +
                        "CLUSTER FAILOVER FORCE";
                }

                ResetManualFailover();
                _mfEnd = NowMs + ManualFailoverTimeoutMs;

                switch (mode)
                {
                    case ManualFailoverMode.Takeover:
                        break;

                    case ManualFailoverMode.Force:
                        _mfCanStart = true;
                        _failoverAuthTime = 0;
                        break;

                    default:
                        _failoverAuthTime = 0;

                        _logger.LogInformation(
                            "Manual failover requested: asking master {Master} to pause writes",
                            master.Id);

                        SendTo(master.Id, BuildHeader(ClusterMessageType.MfStart));
                        break;
                }
            }

            if (mode == ManualFailoverMode.Takeover)
            {
                _logger.LogWarning(
                    "Taking over my master's slots without an election (CLUSTER FAILOVER TAKEOVER)");

                PerformFailover(_state.BumpEpoch());
            }

            return null;
        }

        public int CountFailureReports(string nodeId)
        {
            lock (_state.SyncRoot)
            {
                if (_state.GetNode(nodeId) is not { } node)
                    return -1;

                var cutoff = NowMs - _nodeTimeout * 2L;
                return node.FailureReports.Count(r => r.Value >= cutoff);
            }
        }

        // Tells everyone about a local topology change right away
        // instead of waiting for the next PING round.
        public void BroadcastPong()
        {
            var message = BuildHeader(ClusterMessageType.Pong);

            foreach (var link in _outbound.Values)
                link.Send(message);
        }

        // On a clean shutdown: if this is a master with slots and a
        // healthy replica, hand the slots over with a coordinated
        // (zero data loss) failover before going away.
        public async Task HandoffOnShutdownAsync(TimeSpan timeout)
        {
            ClusterNode? candidate;

            lock (_state.SyncRoot)
            {
                var self = _state.Self;

                if (!self.IsMaster || _state.CountSlots(self) == 0)
                    return;

                var onlineReplicas = _replication.Replicas
                    .Where(r => r.Online)
                    .ToArray();

                candidate = _state.ReplicasOf(self.Id)
                    .Where(r => !r.IsFailing && r.LinkConnected)
                    .Where(r => onlineReplicas.Any(o => o.ListeningPort == r.Port))
                    .OrderByDescending(r => r.ReplOffset)
                    .FirstOrDefault();

                if (candidate is null)
                {
                    _logger.LogWarning(
                        "Shutting down a master with no healthy replica: " +
                        "its slots stay unavailable until it restarts");

                    return;
                }

                _logger.LogInformation(
                    "Shutting down: handing my slots to replica {Replica} " +
                    "({Host}:{Port}) with a coordinated failover",
                    candidate.Id,
                    candidate.Host,
                    candidate.Port);

                SendTo(candidate.Id, BuildHeader(ClusterMessageType.FailoverRequest));
            }

            var deadline = Environment.TickCount64 + (long)timeout.TotalMilliseconds;

            while (Environment.TickCount64 < deadline)
            {
                if (_state.Self.IsReplica)
                {
                    _logger.LogInformation(
                        "Handover complete: {Replica} now owns my former slots",
                        candidate.Id);

                    // Let the new topology reach the other masters.
                    BroadcastPong();
                    await Task.Delay(200);
                    return;
                }

                await Task.Delay(10);
            }

            _logger.LogWarning(
                "Handover to {Replica} did not complete within {Timeout} ms; shutting down anyway",
                candidate.Id,
                (long)timeout.TotalMilliseconds);
        }

        // ---- Links ----

        private async Task AcceptLoopAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                Socket socket;

                try
                {
                    socket = await _listener!.AcceptSocketAsync(cancellationToken);
                }
                catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException)
                {
                    return;
                }
                catch (SocketException)
                {
                    continue;
                }

                socket.NoDelay = true;

                var link = new ClusterLink(socket, outbound: false, nodeId: null, handshake: false);
                _links[link] = 0;
                _ = link.RunAsync(HandleMessage, OnLinkClosed);
            }
        }

        private async Task ConnectAsync(
            string? nodeId,
            string host,
            int busPort,
            bool handshake,
            int dialedPort = 0)
        {
            var key = nodeId ?? $"{host}:{busPort}";

            if (!_connecting.TryAdd(key, NowMs))
                return;

            try
            {
                var socket = new Socket(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };

                try
                {
                    using var timeout = CancellationTokenSource.CreateLinkedTokenSource(_shutdown.Token);
                    timeout.CancelAfter(Math.Max(1000, _nodeTimeout / 2));

                    await socket.ConnectAsync(host, busPort, timeout.Token);
                }
                catch
                {
                    socket.Dispose();
                    return;
                }

                var link = new ClusterLink(socket, outbound: true, nodeId, handshake)
                {
                    DialedHost = host,
                    DialedPort = dialedPort,
                    DialedBusPort = busPort,
                };

                _links[link] = 0;

                if (nodeId is not null)
                {
                    if (_outbound.TryGetValue(nodeId, out var existing) && !existing.Closed)
                    {
                        link.Close();
                        return;
                    }

                    _outbound[nodeId] = link;

                    lock (_state.SyncRoot)
                    {
                        if (_state.GetNode(nodeId) is { } node)
                            node.LinkConnected = true;
                    }
                }

                _ = link.RunAsync(HandleMessage, OnLinkClosed);

                // MEET first: a peer that doesn't know us yet (or forgot
                // us) adds us instead of ignoring our PINGs.
                var hello = BuildHeader(ClusterMessageType.Meet);
                link.Send(hello);

                if (nodeId is not null)
                {
                    lock (_state.SyncRoot)
                    {
                        if (_state.GetNode(nodeId) is { } node && node.PingSentMs == 0)
                            node.PingSentMs = NowMs;
                    }
                }
            }
            finally
            {
                _connecting.TryRemove(key, out _);
            }
        }

        private void OnLinkClosed(ClusterLink link)
        {
            _links.TryRemove(link, out _);

            if (link.Outbound && link.NodeId is { } nodeId &&
                _outbound.TryGetValue(nodeId, out var current) &&
                ReferenceEquals(current, link))
            {
                _outbound.TryRemove(nodeId, out _);

                lock (_state.SyncRoot)
                {
                    if (_state.GetNode(nodeId) is { } node)
                        node.LinkConnected = false;
                }
            }
        }

        private void SendTo(string nodeId, ClusterMessage message)
        {
            if (_outbound.TryGetValue(nodeId, out var link))
                link.Send(message);
        }

        private void Broadcast(ClusterMessage message)
        {
            foreach (var link in _outbound.Values)
                link.Send(message);
        }

        // ---- Messages ----

        private void HandleMessage(ClusterMessage message, ClusterLink link)
        {
            var actions = new List<Action>();

            try
            {
                lock (_state.SyncRoot)
                    HandleMessageLocked(message, link, actions);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing cluster bus message {Type}", message.Type);
            }

            foreach (var action in actions)
            {
                try
                {
                    action();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error applying cluster change");
                }
            }
        }

        private void HandleMessageLocked(
            ClusterMessage message,
            ClusterLink link,
            List<Action> actions)
        {
            var now = NowMs;
            var self = _state.Self;

            if (message.SenderId == self.Id)
            {
                // We dialled ourselves (e.g. MEET to our own address).
                link.Close();
                return;
            }

            var sender = _state.GetNode(message.SenderId);
            var senderHost = string.IsNullOrEmpty(message.Host)
                ? (link.Outbound ? link.DialedHost ?? link.RemoteHost : link.RemoteHost)
                : message.Host;

            if (sender is null &&
                (message.Type == ClusterMessageType.Meet ||
                 (message.Type == ClusterMessageType.Pong && link.Handshake)))
            {
                if (IsBlacklisted(message.SenderId, now))
                    return;

                sender = new ClusterNode
                {
                    Id = message.SenderId,
                    Host = senderHost,
                    Port = message.Port,
                    ClusterBusPort = message.BusPort > 0 ? message.BusPort : message.Port + 10000,
                    Role = message.SenderIsMaster ? ClusterNodeRole.Master : ClusterNodeRole.Replica,
                    MasterId = message.SenderIsMaster ? null : message.MasterId,
                    ConfigEpoch = message.SenderIsMaster ? message.ConfigEpoch : 0,
                    PongReceivedMs = now,
                };

                _state.AddOrUpdateNode(sender);

                _logger.LogInformation(
                    "Node {NodeId} ({Host}:{Port}) joined the cluster",
                    sender.Id,
                    sender.Host,
                    sender.Port);

                // A node that wasn't configured with its own address
                // learns it from the address a peer reached it on.
                if (message.Type == ClusterMessageType.Meet && !link.Outbound)
                    _state.LearnSelfHost(link.LocalHost);
            }

            if (link.Handshake && sender is not null)
            {
                link.Handshake = false;
                link.NodeId = sender.Id;

                if (!_outbound.TryAdd(sender.Id, link))
                {
                    // Already linked: this MEET link was redundant.
                    link.Close();
                }
                else
                {
                    sender.LinkConnected = true;
                }
            }

            if (!link.Outbound && link.NodeId is null && sender is not null)
                link.NodeId = sender.Id;

            if (sender is null)
            {
                // Redis answers a PING even from an unknown sender.
                if (message.Type == ClusterMessageType.Ping)
                    link.Send(BuildHeader(ClusterMessageType.Pong));

                return;
            }

            if (message.CurrentEpoch > _state.CurrentEpoch)
                _state.SetCurrentEpoch(message.CurrentEpoch);

            if (!string.IsNullOrEmpty(message.Host) &&
                (sender.Host != message.Host || sender.Port != message.Port) &&
                message.Port > 0)
            {
                _logger.LogInformation(
                    "Node {NodeId} address changed to {Host}:{Port}",
                    sender.Id,
                    message.Host,
                    message.Port);

                sender.Host = message.Host;
                sender.Port = message.Port;
                sender.ClusterBusPort = message.BusPort;

                if (_outbound.TryRemove(sender.Id, out var stale))
                    stale.Close();

                _state.Save();
            }

            sender.ReplOffset = message.ReplOffset;
            sender.LastDataReceivedMs = now;

            if (message.Type == ClusterMessageType.Pong && link.Outbound)
            {
                sender.PongReceivedMs = now;
                sender.PingSentMs = 0;

                if (sender.IsPFail)
                {
                    sender.Flags &= ~ClusterNodeFlags.PFail;

                    _logger.LogInformation(
                        "Node {NodeId} is reachable again (cleared PFAIL)",
                        sender.Id);
                }

                if (sender.IsFailed)
                    MaybeClearFail(sender, now);
            }

            UpdateRole(sender, message, actions);

            // Our master told us it has paused writes for our manual
            // failover; ReplOffset is the exact offset to reach.
            if (self.IsReplica &&
                self.MasterId == sender.Id &&
                _mfEnd > 0 &&
                (message.Flags & ClusterMessageFlags.Paused) != 0)
            {
                _mfMasterOffset = message.ReplOffset;

                // Don't wait for the next cron tick: every millisecond
                // here is a millisecond of paused writes.
                HandleManualFailoverTimers(now, actions);
                HandleReplicaFailover(now, actions);
            }

            if (message.Type is ClusterMessageType.Ping or ClusterMessageType.Pong or ClusterMessageType.Meet)
            {
                if (message.SenderIsMaster)
                {
                    if (sender.ConfigEpoch != message.ConfigEpoch)
                    {
                        sender.ConfigEpoch = message.ConfigEpoch;
                        _state.Save();
                    }

                    UpdateSlotsConfig(sender, message.ConfigEpoch, message.Slots, actions);
                    SendUpdateIfStale(sender, message, link);
                    HandleConfigEpochCollision(sender);
                }

                ProcessGossip(sender, message, now, actions);
            }

            switch (message.Type)
            {
                case ClusterMessageType.Ping:
                case ClusterMessageType.Meet:
                    link.Send(BuildHeader(ClusterMessageType.Pong));
                    break;

                case ClusterMessageType.Fail:
                    if (message.FailNodeId is { } failId &&
                        _state.GetNode(failId) is { } failed &&
                        !failed.IsMyself &&
                        !failed.IsFailed)
                    {
                        _logger.LogWarning(
                            "FAIL message from {Sender}: node {NodeId} is failing",
                            sender.Id,
                            failId);

                        failed.Flags = (failed.Flags & ~ClusterNodeFlags.PFail) | ClusterNodeFlags.Fail;
                        failed.FailTimeMs = now;
                        _state.Save();
                    }

                    break;

                case ClusterMessageType.AuthRequest:
                    HandleAuthRequest(sender, message, link, now);
                    break;

                case ClusterMessageType.AuthAck:
                    if (_state.Self.IsReplica &&
                        sender.IsMaster &&
                        _state.CountSlots(sender) > 0 &&
                        message.CurrentEpoch >= _failoverAuthEpoch &&
                        _failoverAuthSent &&
                        _votes.Add(sender.Id))
                    {
                        _logger.LogInformation(
                            "Failover vote received from {Master} for epoch {Epoch} ({Votes} so far)",
                            sender.Id,
                            _failoverAuthEpoch,
                            _votes.Count);

                        // Promote the moment the deciding vote lands.
                        HandleReplicaFailover(now, actions);
                    }

                    break;

                case ClusterMessageType.Update:
                    if (message.UpdateNodeId is { } updatedId &&
                        message.UpdateSlots is { } updatedSlots &&
                        _state.GetNode(updatedId) is { } updated &&
                        updated.ConfigEpoch < message.UpdateConfigEpoch)
                    {
                        updated.ConfigEpoch = message.UpdateConfigEpoch;

                        if (updated.IsReplica)
                        {
                            updated.Role = ClusterNodeRole.Master;
                            updated.MasterId = null;
                        }

                        UpdateSlotsConfig(updated, message.UpdateConfigEpoch, updatedSlots, actions);
                    }

                    break;

                case ClusterMessageType.MfStart:
                    if (sender.IsReplica && sender.MasterId == _state.Self.Id && _state.Self.IsMaster)
                    {
                        var replicaId = sender.Id;
                        actions.Add(() => BeginMasterSideManualFailover(replicaId));
                    }

                    break;

                case ClusterMessageType.FailoverRequest:
                    if (_state.Self.IsReplica && _state.Self.MasterId == sender.Id)
                    {
                        _logger.LogInformation(
                            "Master {Master} is shutting down and asked me to take over",
                            sender.Id);

                        actions.Add(() =>
                        {
                            if (StartManualFailover(ManualFailoverMode.Default) is { } error)
                                _logger.LogWarning("Could not start the requested failover: {Error}", error);
                        });
                    }

                    break;
            }
        }

        private void UpdateRole(ClusterNode sender, ClusterMessage message, List<Action> actions)
        {
            var role = message.SenderIsMaster ? ClusterNodeRole.Master : ClusterNodeRole.Replica;
            var masterId = role == ClusterNodeRole.Master ? null : message.MasterId;

            if (sender.Role == role && sender.MasterId == masterId)
                return;

            if (role == ClusterNodeRole.Master && sender.IsReplica)
            {
                _logger.LogInformation("Node {NodeId} is now a master", sender.Id);
            }
            else if (role == ClusterNodeRole.Replica)
            {
                _logger.LogInformation(
                    "Node {NodeId} is now a replica of {Master}",
                    sender.Id,
                    masterId);

                // A replica owns no slots; whoever claims them next
                // (with a higher epoch) gets them.
                if (sender.IsMaster)
                {
                    foreach (var slot in _state.SlotsOf(sender))
                        _state.SetSlotOwner(slot, null);
                }

                // A newly attached replica of ours: make it a candidate
                // right away.
                if (masterId == _state.Self.Id)
                    sender.ReplOffset = message.ReplOffset;
            }

            sender.Role = role;
            sender.MasterId = masterId;
            _state.TouchNodes();
            _state.Save();
        }

        // The core convergence rule: a slot belongs to whichever master
        // claims it with the highest configEpoch.
        private void UpdateSlotsConfig(
            ClusterNode sender,
            long senderEpoch,
            byte[] bitmap,
            List<Action> actions)
        {
            var self = _state.Self;
            var myMaster = self.IsMaster ? self : self.MasterId is { } id ? _state.GetNode(id) : null;
            var myMasterHadSlots = myMaster is not null && _state.CountSlots(myMaster) > 0;
            var lostToSender = false;
            var changed = false;
            var dirty = new List<int>();

            for (var slot = 0; slot < HashSlot.SlotCount; slot++)
            {
                if ((bitmap[slot >> 3] & (1 << (slot & 7))) == 0)
                    continue;

                var owner = _state.GetOwner(slot);

                if (ReferenceEquals(owner, sender))
                    continue;

                // A slot we're importing is under manual control until
                // CLUSTER SETSLOT NODE finalizes it.
                if (_state.GetMigration(slot) is { State: SlotMigrationState.Importing })
                    continue;

                if (owner is not null && owner.ConfigEpoch >= senderEpoch)
                    continue;

                if (ReferenceEquals(owner, self) && self.IsMaster)
                    dirty.Add(slot);

                if (owner is not null && ReferenceEquals(owner, myMaster))
                    lostToSender = true;

                _state.SetSlotOwner(slot, sender);
                changed = true;
            }

            if (!changed)
                return;

            _state.Save();

            if (myMaster is not null &&
                myMasterHadSlots &&
                lostToSender &&
                _state.CountSlots(myMaster) == 0)
            {
                // Either we were the master and a replica took over our
                // slots (a failover happened, perhaps while we were
                // partitioned away), or our master was replaced: either
                // way, follow the new owner.
                _logger.LogWarning(
                    "{Who} lost its last slot to {NodeId} (epoch {Epoch}); " +
                    "reconfiguring as a replica of it",
                    self.IsMaster ? "This master" : "My master",
                    sender.Id,
                    senderEpoch);

                var wasMaster = self.IsMaster;

                _state.SetRole(ClusterNodeRole.Replica, sender.Id);

                var host = sender.Host;
                var port = sender.Port;

                actions.Add(() =>
                {
                    _replication.BecomeReplicaOf(host, port);

                    // Writes paused for a manual failover now see the
                    // new owner and are redirected there.
                    if (wasMaster)
                        EndMasterSideManualFailover();

                    BroadcastPong();
                });

                return;
            }

            if (dirty.Count > 0)
            {
                _logger.LogWarning(
                    "Lost {Count} slot(s) to {NodeId}; deleting the keys left in them",
                    dirty.Count,
                    sender.Id);

                var slots = dirty.ToArray();
                actions.Add(() => DeleteKeysInSlots?.Invoke(slots));
            }
        }

        // A sender claiming slots we know belong to a node with a newer
        // config is stale (e.g. a master returning from a partition):
        // tell it, so it converges without waiting for that node.
        private void SendUpdateIfStale(ClusterNode sender, ClusterMessage message, ClusterLink link)
        {
            for (var slot = 0; slot < HashSlot.SlotCount; slot++)
            {
                if (!message.OwnsSlot(slot))
                    continue;

                var owner = _state.GetOwner(slot);

                if (owner is null || ReferenceEquals(owner, sender) || owner.ConfigEpoch <= message.ConfigEpoch)
                    continue;

                link.Send(new ClusterMessage
                {
                    Type = ClusterMessageType.Update,
                    SenderId = _state.MyId,
                    CurrentEpoch = _state.CurrentEpoch,
                    ConfigEpoch = _state.Self.ConfigEpoch,
                    Flags = _state.Self.IsMaster ? ClusterMessageFlags.Master : ClusterMessageFlags.Replica,
                    Host = HostToAnnounce(),
                    Port = _state.Self.Port,
                    BusPort = _state.Self.ClusterBusPort,
                    UpdateNodeId = owner.Id,
                    UpdateConfigEpoch = owner.ConfigEpoch,
                    UpdateSlots = _state.SlotBitmap(owner),
                });

                return;
            }
        }

        // Two masters with the same configEpoch (e.g. both assigned
        // slots by hand) would make slot conflicts undecidable; the one
        // with the lexicographically smaller id moves to a fresh epoch.
        private void HandleConfigEpochCollision(ClusterNode sender)
        {
            var self = _state.Self;

            if (!self.IsMaster ||
                sender.ConfigEpoch != self.ConfigEpoch ||
                string.CompareOrdinal(sender.Id, self.Id) <= 0)
            {
                return;
            }

            var epoch = _state.BumpEpoch();

            _logger.LogInformation(
                "configEpoch collision with {NodeId}: moved to epoch {Epoch}",
                sender.Id,
                epoch);
        }

        private void ProcessGossip(ClusterNode sender, ClusterMessage message, long now, List<Action> actions)
        {
            foreach (var entry in message.Gossip)
            {
                if (entry.Id == _state.MyId)
                    continue;

                if (_state.GetNode(entry.Id) is { } node)
                {
                    if (message.SenderIsMaster &&
                        (entry.Flags & (GossipFlags.PFail | GossipFlags.Fail)) != 0)
                    {
                        if (!node.FailureReports.ContainsKey(sender.Id))
                        {
                            _logger.LogInformation(
                                "Node {Reporter} reports node {NodeId} as not reachable",
                                sender.Id,
                                node.Id);
                        }

                        node.FailureReports[sender.Id] = now;
                        MarkFailingIfQuorum(node, now);
                    }
                    else
                    {
                        node.FailureReports.Remove(sender.Id);
                    }

                    continue;
                }

                if (IsBlacklisted(entry.Id, now) ||
                    string.IsNullOrEmpty(entry.Host) ||
                    entry.Port <= 0 ||
                    (entry.Flags & GossipFlags.Fail) != 0)
                {
                    continue;
                }

                // A node we've never met, vouched for by a known peer.
                var discovered = new ClusterNode
                {
                    Id = entry.Id,
                    Host = entry.Host,
                    Port = entry.Port,
                    ClusterBusPort = entry.BusPort > 0 ? entry.BusPort : entry.Port + 10000,
                    Role = (entry.Flags & GossipFlags.Replica) != 0
                        ? ClusterNodeRole.Replica
                        : ClusterNodeRole.Master,
                    PongReceivedMs = now,
                };

                _state.AddOrUpdateNode(discovered);

                _logger.LogInformation(
                    "Discovered node {NodeId} ({Host}:{Port}) via gossip from {Sender}",
                    discovered.Id,
                    discovered.Host,
                    discovered.Port,
                    sender.Id);

                var host = discovered.Host;
                var busPort = discovered.ClusterBusPort;
                var id = discovered.Id;
                actions.Add(() => _ = ConnectAsync(id, host, busPort, handshake: false));
            }
        }

        private void MarkFailingIfQuorum(ClusterNode node, long now)
        {
            if (!node.IsPFail || node.IsFailed)
                return;

            var quorum = _state.Size / 2 + 1;
            var cutoff = now - _nodeTimeout * 2L;

            var reports = node.FailureReports.Count(r =>
                r.Value >= cutoff &&
                _state.GetNode(r.Key) is { IsMaster: true });

            if (_state.Self.IsMaster)
                reports++;

            if (reports < quorum)
                return;

            _logger.LogWarning(
                "*** Marking node {NodeId} as failing (quorum {Reports}/{Quorum} reached)",
                node.Id,
                reports,
                quorum);

            node.Flags = (node.Flags & ~ClusterNodeFlags.PFail) | ClusterNodeFlags.Fail;
            node.FailTimeMs = now;
            _state.Save();

            var header = BuildHeader(ClusterMessageType.Fail);

            Broadcast(new ClusterMessage
            {
                Type = ClusterMessageType.Fail,
                SenderId = header.SenderId,
                CurrentEpoch = header.CurrentEpoch,
                ConfigEpoch = header.ConfigEpoch,
                Flags = header.Flags,
                MasterId = header.MasterId,
                Host = header.Host,
                Port = header.Port,
                BusPort = header.BusPort,
                ReplOffset = header.ReplOffset,
                Slots = header.Slots,
                FailNodeId = node.Id,
            });
        }

        private void MaybeClearFail(ClusterNode node, long now)
        {
            if (node.IsReplica || _state.CountSlots(node) == 0)
            {
                _logger.LogInformation(
                    "Clear FAIL state for node {NodeId}: {Kind} is reachable again",
                    node.Id,
                    node.IsReplica ? "replica" : "master without slots");
            }
            else if (now - node.FailTimeMs > _nodeTimeout * 2L)
            {
                _logger.LogInformation(
                    "Clear FAIL state for node {NodeId}: reachable again and " +
                    "nobody took over its slots",
                    node.Id);
            }
            else
            {
                return;
            }

            node.Flags &= ~ClusterNodeFlags.Fail;
            node.FailureReports.Clear();
            _state.Save();
        }

        // A replica of `requester.MasterId` asks for our vote in a new
        // epoch. Only slot-owning masters vote, at most once per epoch
        // and once per failed master per 2x node timeout — so two
        // replicas can never both win.
        private void HandleAuthRequest(ClusterNode requester, ClusterMessage message, ClusterLink link, long now)
        {
            var self = _state.Self;

            if (!self.IsMaster || _state.CountSlots(self) == 0)
                return;

            var requestEpoch = message.CurrentEpoch;
            var force = (message.Flags & ClusterMessageFlags.ForceAck) != 0;

            if (requestEpoch < _state.CurrentEpoch)
                return;

            if (_state.LastVoteEpoch >= _state.CurrentEpoch)
            {
                _logger.LogInformation(
                    "Failover vote denied to {NodeId}: already voted in epoch {Epoch}",
                    requester.Id,
                    _state.CurrentEpoch);

                return;
            }

            if (message.SenderIsMaster ||
                message.MasterId is null ||
                _state.GetNode(message.MasterId) is not { } master)
            {
                return;
            }

            if (!master.IsFailed && !force)
            {
                _logger.LogInformation(
                    "Failover vote denied to {NodeId}: its master {Master} is not failing",
                    requester.Id,
                    master.Id);

                return;
            }

            if (now - master.VotedTimeMs < _nodeTimeout * 2L)
            {
                _logger.LogInformation(
                    "Failover vote denied to {NodeId}: already voted for a replica of {Master} recently",
                    requester.Id,
                    master.Id);

                return;
            }

            // Refuse a stale replica claiming slots that have since
            // moved to a node with a newer configuration.
            for (var slot = 0; slot < HashSlot.SlotCount; slot++)
            {
                if (!message.OwnsSlot(slot))
                    continue;

                if (_state.GetOwner(slot) is { } owner && owner.ConfigEpoch > message.ConfigEpoch)
                {
                    _logger.LogInformation(
                        "Failover vote denied to {NodeId}: slot {Slot} has a newer owner",
                        requester.Id,
                        slot);

                    return;
                }
            }

            _state.SetLastVoteEpoch(_state.CurrentEpoch);
            master.VotedTimeMs = now;

            _logger.LogInformation(
                "Failover vote granted to {NodeId} for epoch {Epoch}",
                requester.Id,
                _state.CurrentEpoch);

            var ack = BuildHeader(ClusterMessageType.AuthAck);

            if (_outbound.TryGetValue(requester.Id, out var outboundLink))
                outboundLink.Send(ack);
            else
                link.Send(ack);
        }

        // ---- Periodic work ----

        private async Task CronLoopAsync(CancellationToken cancellationToken)
        {
            using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));

            try
            {
                while (await timer.WaitForNextTickAsync(cancellationToken))
                {
                    var actions = new List<Action>();

                    try
                    {
                        lock (_state.SyncRoot)
                            CronLocked(actions);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Cluster cron failed");
                    }

                    foreach (var action in actions)
                    {
                        try
                        {
                            action();
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error applying cluster change");
                        }
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
        }

        private void CronLocked(List<Action> actions)
        {
            var now = NowMs;
            var pingInterval = Math.Min(1000, _nodeTimeout / 4);
            var cutoff = now - _nodeTimeout * 2L;

            foreach (var expired in _blacklist.Where(b => b.Value < now).Select(b => b.Key).ToArray())
                _blacklist.Remove(expired);

            ClusterMessage? ping = null;

            foreach (var node in _state.Nodes)
            {
                if (node.IsMyself)
                    continue;

                foreach (var stale in node.FailureReports.Where(r => r.Value < cutoff).Select(r => r.Key).ToArray())
                    node.FailureReports.Remove(stale);

                var connected = _outbound.TryGetValue(node.Id, out var link) && !link.Closed;
                node.LinkConnected = connected;

                if (!connected)
                {
                    // Counted as an unanswered ping from the first
                    // attempt, so an unreachable node still goes PFAIL.
                    if (node.PingSentMs == 0)
                        node.PingSentMs = now;

                    if (!_connecting.ContainsKey(node.Id) &&
                        now - node.LastPingSentAttemptMs >= Math.Min(1000, _nodeTimeout / 2))
                    {
                        node.LastPingSentAttemptMs = now;

                        var id = node.Id;
                        var host = node.Host;
                        var busPort = node.ClusterBusPort;
                        actions.Add(() => _ = ConnectAsync(id, host, busPort, handshake: false));
                    }
                }
                else if (node.PingSentMs == 0 && now - node.LastPingSentAttemptMs >= pingInterval)
                {
                    ping ??= BuildHeader(ClusterMessageType.Ping);
                    link!.Send(ping);
                    node.PingSentMs = now;
                    node.LastPingSentAttemptMs = now;
                }
                else if (node.PingSentMs != 0 &&
                         now - node.PingSentMs > _nodeTimeout / 2 &&
                         Environment.TickCount64 - link!.CreatedMs > _nodeTimeout / 2)
                {
                    // Half the timeout without a reply: the connection
                    // may be dead even if the node isn't. Reconnect;
                    // PingSentMs keeps counting toward PFAIL.
                    link.Close();
                }

                if (node.PingSentMs != 0 &&
                    now - node.PingSentMs > _nodeTimeout &&
                    now - node.LastDataReceivedMs > _nodeTimeout &&
                    !node.IsFailing)
                {
                    node.Flags |= ClusterNodeFlags.PFail;

                    _logger.LogWarning(
                        "*** Node {NodeId} ({Host}:{Port}) possibly failing (PFAIL)",
                        node.Id,
                        node.Host,
                        node.Port);
                }

                if (node.IsPFail)
                    MarkFailingIfQuorum(node, now);
            }

            HandleManualFailoverTimers(now, actions);
            HandleReplicaFailover(now, actions);
            UpdateClusterState();
        }

        private void HandleManualFailoverTimers(long now, List<Action> actions)
        {
            if (_mfEnd == 0)
                return;

            var self = _state.Self;

            if (now > _mfEnd)
            {
                _logger.LogWarning("Manual failover timed out");

                var wasMaster = self.IsMaster;
                ResetManualFailover();

                if (wasMaster)
                    actions.Add(() => _writeGate.Resume());

                return;
            }

            if (self.IsMaster && _mfReplica is { } replicaId)
            {
                // Keep telling the replica our (paused, now final)
                // offset until it takes over.
                SendTo(replicaId, BuildHeader(ClusterMessageType.Ping));
                return;
            }

            if (self.IsReplica &&
                !_mfCanStart &&
                _mfMasterOffset >= 0 &&
                _replication.Offset >= _mfMasterOffset)
            {
                _mfCanStart = true;
                _failoverAuthTime = 0;

                _logger.LogInformation(
                    "All of the master's replication stream processed " +
                    "(offset {Offset}); manual failover can start",
                    _mfMasterOffset);
            }
        }

        private void HandleReplicaFailover(long now, List<Action> actions)
        {
            var self = _state.Self;

            if (!self.IsReplica || self.MasterId is null)
                return;

            if (_state.GetNode(self.MasterId) is not { } master)
                return;

            var manual = _mfEnd > 0 && _mfCanStart;

            if (!manual && (!master.IsFailed || _options.ClusterReplicaNoFailover))
                return;

            if (_state.CountSlots(master) == 0)
                return;

            var authTimeout = Math.Max(_nodeTimeout * 2L, 2000);
            var retryTime = authTimeout * 2;

            if (now - _failoverAuthTime > retryTime)
            {
                _failoverAuthRank = ComputeRank(self);
                _failoverAuthTime = manual
                    ? now
                    : now + 500 + _random.Next(500) + _failoverAuthRank * 1000L;

                _failoverAuthSent = false;
                _failoverScheduled = false;
                _votes.Clear();
                _electionLogged = false;

                if (!manual)
                {
                    _logger.LogWarning(
                        "Master {Master} failed: starting an election in {Delay} ms " +
                        "(rank #{Rank}, offset {Offset})",
                        master.Id,
                        _failoverAuthTime - now,
                        _failoverAuthRank,
                        _replication.Offset);

                    return;
                }

                // A manual failover has no reason to wait: fall through
                // and ask for votes right away.
            }

            if (now < _failoverAuthTime)
                return;

            if (now - _failoverAuthTime > authTimeout)
            {
                if (!_electionLogged)
                {
                    _electionLogged = true;

                    _logger.LogWarning(
                        "Failover election for epoch {Epoch} expired with {Votes} vote(s); " +
                        "retrying in {Retry} ms",
                        _failoverAuthEpoch,
                        _votes.Count,
                        retryTime - authTimeout);
                }

                return;
            }

            if (!_failoverAuthSent)
            {
                _failoverAuthEpoch = _state.IncrementCurrentEpoch();
                _failoverAuthSent = true;

                _logger.LogWarning(
                    "Requesting failover votes for epoch {Epoch}{Manual}",
                    _failoverAuthEpoch,
                    manual ? " (manual failover)" : "");

                var request = BuildHeader(ClusterMessageType.AuthRequest);

                Broadcast(manual
                    ? Rebuild(request, flags: request.Flags | ClusterMessageFlags.ForceAck)
                    : request);

                return;
            }

            var quorum = _state.Size / 2 + 1;

            if (_votes.Count >= quorum && !_failoverScheduled)
            {
                var epoch = _failoverAuthEpoch;

                _logger.LogWarning(
                    "Failover election won with {Votes}/{Quorum} votes for epoch {Epoch}",
                    _votes.Count,
                    quorum,
                    epoch);

                // Stays set until the next attempt, so neither a late
                // vote nor a cron tick racing the promotion can start a
                // second election or a second promotion.
                _failoverScheduled = true;
                actions.Add(() => PerformFailover(epoch));
            }
        }

        // How many sibling replicas have more data than us: the most
        // up-to-date replica starts its election first and usually wins.
        private int ComputeRank(ClusterNode self)
        {
            var offset = _replication.Offset;

            return _state.Nodes.Count(n =>
                !n.IsMyself &&
                n.IsReplica &&
                n.MasterId == self.MasterId &&
                !n.IsFailing &&
                n.ReplOffset > offset);
        }

        // Promote this replica: claim the old master's slots with the
        // won epoch, become a master in the replication sense too, and
        // tell everyone.
        private void PerformFailover(long epoch)
        {
            lock (_state.SyncRoot)
            {
                var self = _state.Self;

                if (!self.IsReplica || self.MasterId is null)
                    return;

                var oldMaster = _state.GetNode(self.MasterId);

                self.Role = ClusterNodeRole.Master;
                self.MasterId = null;

                if (self.ConfigEpoch < epoch)
                    self.ConfigEpoch = epoch;

                if (oldMaster is not null)
                {
                    foreach (var slot in _state.SlotsOf(oldMaster))
                        _state.SetSlotOwner(slot, self);
                }

                _state.TouchNodes();
                _state.Save();
                ResetManualFailover();

                _logger.LogWarning(
                    "Failover complete: this node is now the master for its " +
                    "former master's slots (configEpoch {Epoch})",
                    self.ConfigEpoch);
            }

            _replication.PromoteToMaster();
            BroadcastPong();
            UpdateClusterStateLocked();
        }

        private void BeginMasterSideManualFailover(string replicaId)
        {
            _logger.LogInformation(
                "Manual failover requested by replica {Replica}: pausing writes",
                replicaId);

            // Pause and drain first, so the offset we report is final.
            _writeGate.Pause(
                TimeSpan.FromMilliseconds(ManualFailoverTimeoutMs + 1000),
                all: false);

            lock (_state.SyncRoot)
            {
                _mfEnd = NowMs + ManualFailoverTimeoutMs;
                _mfReplica = replicaId;
                SendTo(replicaId, BuildHeader(ClusterMessageType.Ping));
            }
        }

        private void EndMasterSideManualFailover()
        {
            lock (_state.SyncRoot)
                ResetManualFailover();

            _writeGate.Resume();
        }

        private void ResetManualFailover()
        {
            _mfEnd = 0;
            _mfMasterOffset = -1;
            _mfCanStart = false;
            _mfReplica = null;
        }

        private void UpdateClusterStateLocked()
        {
            lock (_state.SyncRoot)
                UpdateClusterState();
        }

        private void UpdateClusterState()
        {
            var ok = true;

            if (_options.ClusterRequireFullCoverage)
            {
                for (var slot = 0; slot < HashSlot.SlotCount; slot++)
                {
                    if (_state.GetOwner(slot) is not { IsFailed: false })
                    {
                        ok = false;
                        break;
                    }
                }
            }

            var self = _state.Self;

            // A master that can't reach a majority of masters stops
            // serving: it may be on the minority side of a partition,
            // where a replica on the majority side is about to replace
            // it. Writes it accepted there would be lost.
            if (ok && self.IsMaster)
            {
                var masters = _state.Nodes
                    .Where(n => n.IsMaster && _state.CountSlots(n) > 0)
                    .ToArray();

                if (masters.Length > 0)
                {
                    var reachable = masters.Count(n => n.IsMyself || !n.IsFailing);

                    if (reachable < masters.Length / 2 + 1)
                        ok = false;
                }

                // A master that just (re)started may have been replaced
                // while it was down; give gossip a moment to tell it
                // before it accepts writes it would later have to drop.
                if (masters.Length > 1 && NowMs - _startedMs < 2000)
                    ok = false;
            }

            _state.IsStateOk = ok;

            if (ok != _lastStateOk)
            {
                _lastStateOk = ok;

                _logger.LogWarning(
                    ok ? "Cluster state changed: ok" : "Cluster state changed: fail");
            }
        }

        // ---- Message construction ----

        private ClusterMessage BuildHeader(ClusterMessageType type)
        {
            var self = _state.Self;
            var master = self.IsReplica && self.MasterId is { } masterId
                ? _state.GetNode(masterId)
                : self;

            var flags = self.IsMaster ? ClusterMessageFlags.Master : ClusterMessageFlags.Replica;

            if (self.IsMaster && _mfEnd > 0 && _writeGate.IsPaused)
                flags |= ClusterMessageFlags.Paused;

            return new ClusterMessage
            {
                Type = type,
                SenderId = self.Id,
                CurrentEpoch = type == ClusterMessageType.AuthRequest
                    ? _failoverAuthEpoch
                    : _state.CurrentEpoch,
                ConfigEpoch = master?.ConfigEpoch ?? self.ConfigEpoch,
                Flags = flags,
                MasterId = self.IsReplica ? self.MasterId : null,
                Host = HostToAnnounce(),
                Port = self.Port,
                BusPort = self.ClusterBusPort,
                ReplOffset = _replication.Offset,
                Slots = master is not null
                    ? _state.SlotBitmap(master)
                    : new byte[HashSlot.SlotCount / 8],
                Gossip = type is ClusterMessageType.Ping or ClusterMessageType.Pong or ClusterMessageType.Meet
                    ? BuildGossip()
                    : new List<GossipEntry>(),
            };
        }

        private static ClusterMessage Rebuild(ClusterMessage source, ClusterMessageFlags flags)
            => new()
            {
                Type = source.Type,
                SenderId = source.SenderId,
                CurrentEpoch = source.CurrentEpoch,
                ConfigEpoch = source.ConfigEpoch,
                Flags = flags,
                MasterId = source.MasterId,
                Host = source.Host,
                Port = source.Port,
                BusPort = source.BusPort,
                ReplOffset = source.ReplOffset,
                Slots = source.Slots,
                Gossip = source.Gossip,
            };

        private List<GossipEntry> BuildGossip()
        {
            var nodes = _state.Nodes.Where(n => !n.IsMyself).ToList();

            // Small clusters: tell everyone about everyone. Large ones:
            // a random sample, plus every node currently suspected, so
            // failure reports still reach a quorum quickly.
            if (nodes.Count > 16)
            {
                var failing = nodes.Where(n => n.IsFailing);
                var sample = nodes.OrderBy(_ => _random.Next()).Take(16);
                nodes = sample.Concat(failing).Distinct().ToList();
            }

            return nodes
                .Select(n => new GossipEntry(
                    n.Id,
                    n.Host,
                    n.Port,
                    n.ClusterBusPort,
                    (n.IsMaster ? GossipFlags.Master : GossipFlags.Replica) |
                    (n.IsPFail ? GossipFlags.PFail : GossipFlags.None) |
                    (n.IsFailed ? GossipFlags.Fail : GossipFlags.None)))
                .ToList();
        }

        private string HostToAnnounce()
            => _state.AnnounceIpConfigured || _state.Self.Host != "127.0.0.1"
                ? _state.Self.Host
                : "";

        private bool IsBlacklisted(string nodeId, long now)
            => _blacklist.TryGetValue(nodeId, out var until) && until > now;

        private static long NowMs => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        public void Dispose()
        {
            _shutdown.Cancel();

            try
            {
                _listener?.Stop();
            }
            catch (SocketException)
            {
            }

            foreach (var link in _links.Keys)
                link.Close();
        }
    }
}
