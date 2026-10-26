using recache.Configuration;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace recache.Clustering
{
    public enum SlotMigrationState
    {
        Migrating,
        Importing
    }

    public sealed record SlotMigration(
        SlotMigrationState State,
        string PeerNodeId);

    // This node's view of the cluster: which node owns each of the
    // 16384 hash slots, every known node, epochs, and this node's
    // vote history — persisted to nodes.conf on every change so
    // identity, topology and election safety survive restarts.
    //
    // Changes come from local CLUSTER commands and from ClusterManager
    // applying gossip/failover, always under _lock. Reads on the
    // command hot path (slot owner, migration state, myself) are
    // lock-free: the slot tables are arrays of references, written
    // under the lock and read with plain atomic reference loads.
    public sealed class ClusterState
    {
        private readonly object _lock = new();
        private readonly string _configPath;
        private readonly bool _announceIpConfigured;
        private readonly ConcurrentDictionary<string, ClusterNode> _nodes =
            new(StringComparer.Ordinal);
        private readonly ClusterNode?[] _slots =
            new ClusterNode?[HashSlot.SlotCount];
        private readonly SlotMigration?[] _migrations =
            new SlotMigration?[HashSlot.SlotCount];

        private volatile ClusterNode _self = null!;
        private volatile ClusterNode[] _nodeSnapshot = Array.Empty<ClusterNode>();
        private long _currentEpoch;
        private long _lastVoteEpoch;
        private volatile bool _stateOk = true;

        public ClusterState(RecacheOptions options, string contentRoot)
        {
            _configPath = ResolvePath(
                contentRoot,
                options.ClusterConfigFile);

            _announceIpConfigured = !string.IsNullOrWhiteSpace(options.ClusterAnnounceIp);

            var announceHost = _announceIpConfigured
                ? options.ClusterAnnounceIp!
                : "127.0.0.1";

            var announcePort =
                options.ClusterAnnouncePort > 0
                    ? options.ClusterAnnouncePort
                    : options.Port;

            var busPort = options.ClusterAnnounceBusPort > 0
                ? options.ClusterAnnounceBusPort
                : options.ClusterBusPort > 0
                    ? options.ClusterBusPort
                    : announcePort + 10000;

            lock (_lock)
            {
                if (File.Exists(_configPath) &&
                    TryLoad(_configPath, announceHost, announcePort))
                {
                    // Configured addresses win over whatever was saved
                    // (the node may have moved, e.g. a new container
                    // IP).
                    var self = _self;

                    if (_announceIpConfigured)
                        self.Host = announceHost;

                    self.Port = announcePort;
                    self.ClusterBusPort = busPort;

                    Save();
                    return;
                }

                var id = GenerateNodeId();

                _self = new ClusterNode
                {
                    Id = id,
                    Host = announceHost,
                    Port = announcePort,
                    ClusterBusPort = busPort,
                    Role = ClusterNodeRole.Master,
                    Flags = ClusterNodeFlags.Myself,
                };

                _nodes[id] = _self;
                RefreshSnapshot();
                Save();
            }
        }

        // Callers that make several related changes atomically
        // (gossip processing, failover) hold this; every mutator below
        // also takes it, and Monitor locks are re-entrant.
        internal object SyncRoot => _lock;

        public string MyId => _self.Id;

        public ClusterNode Self => _self;

        public bool AnnounceIpConfigured => _announceIpConfigured;

        public long CurrentEpoch => Interlocked.Read(ref _currentEpoch);

        public long LastVoteEpoch => Interlocked.Read(ref _lastVoteEpoch);

        // ok/fail as last computed by ClusterManager: all slots served
        // by reachable nodes, and (for a master) a majority of masters
        // reachable. Key commands are refused with CLUSTERDOWN while
        // this is false.
        public bool IsStateOk
        {
            get => _stateOk;
            internal set => _stateOk = value;
        }

        public IReadOnlyList<ClusterNode> Nodes => _nodeSnapshot;

        public ClusterNode? GetNode(string id)
            => _nodes.TryGetValue(id, out var node) ? node : null;

        public ClusterNode? GetOwner(int slot)
            => Volatile.Read(ref _slots[slot]);

        public SlotMigration? GetMigration(int slot)
            => Volatile.Read(ref _migrations[slot]);

        // Masters that own at least one slot — the electorate for
        // failure detection and failover votes.
        public int Size
        {
            get
            {
                var owners = new HashSet<ClusterNode>();

                for (var i = 0; i < _slots.Length; i++)
                {
                    if (Volatile.Read(ref _slots[i]) is { } owner)
                        owners.Add(owner);
                }

                return owners.Count;
            }
        }

        public int CountSlots(ClusterNode node)
        {
            var count = 0;

            for (var i = 0; i < _slots.Length; i++)
            {
                if (ReferenceEquals(Volatile.Read(ref _slots[i]), node))
                    count++;
            }

            return count;
        }

        public List<int> SlotsOf(ClusterNode node)
        {
            var result = new List<int>();

            for (var i = 0; i < _slots.Length; i++)
            {
                if (ReferenceEquals(Volatile.Read(ref _slots[i]), node))
                    result.Add(i);
            }

            return result;
        }

        // 2048-byte bitmap of the slots `node` owns, as carried in
        // every gossip message header.
        public byte[] SlotBitmap(ClusterNode node)
        {
            var bitmap = new byte[HashSlot.SlotCount / 8];

            for (var i = 0; i < _slots.Length; i++)
            {
                if (ReferenceEquals(Volatile.Read(ref _slots[i]), node))
                    bitmap[i >> 3] |= (byte)(1 << (i & 7));
            }

            return bitmap;
        }

        public IReadOnlyList<ClusterNode> ReplicasOf(string masterId)
            => _nodeSnapshot
                .Where(n => n.IsReplica && n.MasterId == masterId)
                .ToArray();

        // Returns the slots that were already owned by someone
        // (including self) — real Redis rejects the whole command
        // in that case rather than partially applying it.
        public IReadOnlyList<int> AddSlots(IEnumerable<int> slots)
        {
            lock (_lock)
            {
                var requested = slots.Distinct().ToArray();
                var busy = requested
                    .Where(s => _slots[s] is not null)
                    .ToArray();

                if (busy.Length > 0)
                    return busy;

                foreach (var slot in requested)
                    Volatile.Write(ref _slots[slot], _self);

                Save();
                return Array.Empty<int>();
            }
        }

        // Returns the slots that were already unassigned.
        public IReadOnlyList<int> RemoveSlots(IEnumerable<int> slots)
        {
            lock (_lock)
            {
                var requested = slots.Distinct().ToArray();
                var unassigned = requested
                    .Where(s => _slots[s] is null)
                    .ToArray();

                if (unassigned.Length > 0)
                    return unassigned;

                foreach (var slot in requested)
                {
                    Volatile.Write(ref _slots[slot], null);
                    Volatile.Write(ref _migrations[slot], null);
                }

                Save();
                return Array.Empty<int>();
            }
        }

        public void FlushSlots()
        {
            lock (_lock)
            {
                for (var i = 0; i < _slots.Length; i++)
                {
                    if (ReferenceEquals(_slots[i], _self))
                        Volatile.Write(ref _slots[i], null);
                }

                Save();
            }
        }

        public void SetMigrating(int slot, string targetNodeId)
        {
            lock (_lock)
            {
                Volatile.Write(
                    ref _migrations[slot],
                    new SlotMigration(SlotMigrationState.Migrating, targetNodeId));

                Save();
            }
        }

        public void SetImporting(int slot, string sourceNodeId)
        {
            lock (_lock)
            {
                Volatile.Write(
                    ref _migrations[slot],
                    new SlotMigration(SlotMigrationState.Importing, sourceNodeId));

                Save();
            }
        }

        public void ClearMigration(int slot)
        {
            lock (_lock)
            {
                Volatile.Write(ref _migrations[slot], null);
                Save();
            }
        }

        // Sets `slot`'s owner without touching migration state —
        // gossip/failover use this; CLUSTER SETSLOT NODE uses
        // AssignSlot below.
        internal void SetSlotOwner(int slot, ClusterNode? owner)
        {
            lock (_lock)
                Volatile.Write(ref _slots[slot], owner);
        }

        // CLUSTER SETSLOT <slot> NODE <id> — finalizes ownership
        // after a key migration.
        public void AssignSlot(int slot, string ownerId)
        {
            lock (_lock)
            {
                Volatile.Write(ref _slots[slot], GetNode(ownerId));
                Volatile.Write(ref _migrations[slot], null);
                Save();
            }
        }

        // CLUSTER RESET — clears slot ownership and forgets every
        // other node. HARD also discards this node's own identity
        // (fresh node id, epochs back to 0), matching real Redis.
        public void Reset(bool hard)
        {
            lock (_lock)
            {
                for (var i = 0; i < _slots.Length; i++)
                {
                    Volatile.Write(ref _slots[i], null);
                    Volatile.Write(ref _migrations[i], null);
                }

                var old = _self;

                _nodes.Clear();

                var id = hard ? GenerateNodeId() : old.Id;

                if (hard)
                {
                    Interlocked.Exchange(ref _currentEpoch, 0);
                    Interlocked.Exchange(ref _lastVoteEpoch, 0);
                }

                _self = new ClusterNode
                {
                    Id = id,
                    Host = old.Host,
                    Port = old.Port,
                    ClusterBusPort = old.ClusterBusPort,
                    Role = ClusterNodeRole.Master,
                    ConfigEpoch = hard ? 0 : old.ConfigEpoch,
                    Flags = ClusterNodeFlags.Myself,
                };

                _nodes[id] = _self;
                RefreshSnapshot();
                Save();
            }
        }

        public bool AddOrUpdateNode(ClusterNode node)
        {
            lock (_lock)
            {
                var isNew = !_nodes.ContainsKey(node.Id);
                _nodes[node.Id] = node;
                RefreshSnapshot();
                Save();
                return isNew;
            }
        }

        public bool Forget(string id)
        {
            lock (_lock)
            {
                if (id == _self.Id)
                    return false;

                if (!_nodes.TryRemove(id, out var removed))
                    return false;

                for (var i = 0; i < _slots.Length; i++)
                {
                    if (ReferenceEquals(_slots[i], removed))
                        Volatile.Write(ref _slots[i], null);
                }

                foreach (var node in _nodes.Values)
                    node.FailureReports.Remove(id);

                RefreshSnapshot();
                Save();
                return true;
            }
        }

        public void SetRole(ClusterNodeRole role, string? masterId)
        {
            lock (_lock)
            {
                var self = _self;
                self.Role = role;
                self.MasterId = masterId;
                Save();
            }
        }

        public void SetConfigEpoch(long epoch)
        {
            lock (_lock)
            {
                _self.ConfigEpoch = epoch;

                if (epoch > CurrentEpoch)
                    Interlocked.Exchange(ref _currentEpoch, epoch);

                Save();
            }
        }

        // Raises this node's configEpoch to a fresh, cluster-unique
        // value without an election — what CLUSTER BUMPEPOCH, a
        // finished slot import, and FAILOVER TAKEOVER use.
        public long BumpEpoch()
        {
            lock (_lock)
            {
                var epoch = Interlocked.Increment(ref _currentEpoch);
                _self.ConfigEpoch = epoch;
                Save();
                return epoch;
            }
        }

        internal void SetCurrentEpoch(long epoch)
        {
            lock (_lock)
            {
                if (epoch <= CurrentEpoch)
                    return;

                Interlocked.Exchange(ref _currentEpoch, epoch);
                Save();
            }
        }

        internal long IncrementCurrentEpoch()
        {
            lock (_lock)
            {
                var epoch = Interlocked.Increment(ref _currentEpoch);
                Save();
                return epoch;
            }
        }

        internal void SetLastVoteEpoch(long epoch)
        {
            lock (_lock)
            {
                Interlocked.Exchange(ref _lastVoteEpoch, epoch);
                Save();
            }
        }

        // A node that wasn't told its own address learns it from the
        // first peer that reaches it (the address that peer dialled).
        internal void LearnSelfHost(string host)
        {
            if (_announceIpConfigured || string.IsNullOrEmpty(host))
                return;

            lock (_lock)
            {
                if (_self.Host == host)
                    return;

                _self.Host = host;
                Save();
            }
        }

        internal void RemoveNode(string id)
            => Forget(id);

        internal void TouchNodes()
        {
            lock (_lock)
                RefreshSnapshot();
        }

        public string ToNodesText()
        {
            lock (_lock)
                return ToNodesTextUnlocked(includeVars: false);
        }

        private string FormatNodeLine(ClusterNode node)
        {
            var flagParts = new List<string>();

            if (node.IsMyself)
                flagParts.Add("myself");

            flagParts.Add(
                node.Role == ClusterNodeRole.Master ? "master" : "slave");

            if (node.IsFailed)
                flagParts.Add("fail");
            else if (node.IsPFail)
                flagParts.Add("fail?");

            if ((node.Flags & ClusterNodeFlags.Handshake) != 0)
                flagParts.Add("handshake");

            if ((node.Flags & ClusterNodeFlags.NoAddr) != 0)
                flagParts.Add("noaddr");

            var master = node.MasterId ?? "-";

            var slots = node.Role == ClusterNodeRole.Master
                ? FormatSlotRanges(node)
                : "";

            if (node.IsMyself)
            {
                var markers = new StringBuilder();

                for (var slot = 0; slot < _migrations.Length; slot++)
                {
                    if (_migrations[slot] is not { } migration)
                        continue;

                    markers.Append(' ');

                    markers.Append(
                        migration.State == SlotMigrationState.Migrating
                            ? $"[{slot}->-{migration.PeerNodeId}]"
                            : $"[{slot}-<-{migration.PeerNodeId}]");
                }

                slots += markers.ToString();
            }

            var link = node.IsMyself || node.LinkConnected
                ? "connected"
                : "disconnected";

            return string.Join(
                ' ',
                node.Id,
                $"{node.Host}:{node.Port}@{node.ClusterBusPort}",
                string.Join(',', flagParts),
                master,
                node.PingSentMs.ToString(CultureInfo.InvariantCulture),
                node.PongReceivedMs.ToString(CultureInfo.InvariantCulture),
                node.ConfigEpoch.ToString(CultureInfo.InvariantCulture),
                link,
                slots.Trim()).TrimEnd();
        }

        private string FormatSlotRanges(ClusterNode node)
        {
            var ranges = new List<string>();
            var start = -1;

            for (var slot = 0; slot < _slots.Length; slot++)
            {
                var owned = ReferenceEquals(_slots[slot], node);

                if (owned && start < 0)
                {
                    start = slot;
                }
                else if (!owned && start >= 0)
                {
                    ranges.Add(FormatRange(start, slot - 1));
                    start = -1;
                }
            }

            if (start >= 0)
                ranges.Add(FormatRange(start, _slots.Length - 1));

            return string.Join(' ', ranges);
        }

        private static string FormatRange(int start, int end)
            => start == end ? start.ToString(CultureInfo.InvariantCulture) : $"{start}-{end}";

        public IReadOnlyList<(int Start, int End, ClusterNode Node)>
            SlotRanges()
        {
            var result = new List<(int, int, ClusterNode)>();
            var start = -1;
            ClusterNode? owner = null;

            for (var slot = 0; slot <= _slots.Length; slot++)
            {
                var current = slot < _slots.Length
                    ? Volatile.Read(ref _slots[slot])
                    : null;

                if (ReferenceEquals(current, owner) && current is not null)
                    continue;

                if (owner is not null)
                    result.Add((start, slot - 1, owner));

                owner = current;
                start = slot;
            }

            return result;
        }

        private void RefreshSnapshot()
            => _nodeSnapshot = _nodes.Values.ToArray();

        private sealed record ParsedNode(
            string Id,
            string Host,
            int Port,
            int ClusterBusPort,
            string Flags,
            string? MasterId,
            long ConfigEpoch,
            IReadOnlyList<int> Slots,
            IReadOnlyList<(int Slot, SlotMigration Migration)> Migrations);

        private static List<ParsedNode> ParseNodesText(
            string text,
            out long currentEpoch,
            out long lastVoteEpoch)
        {
            var result = new List<ParsedNode>();
            currentEpoch = 0;
            lastVoteEpoch = 0;

            foreach (var rawLine in text.Split(
                '\n',
                StringSplitOptions.RemoveEmptyEntries))
            {
                var line = rawLine.Trim();

                if (line.Length == 0)
                    continue;

                var fields = line.Split(
                    ' ',
                    StringSplitOptions.RemoveEmptyEntries);

                if (fields[0] == "vars")
                {
                    for (var i = 1; i + 1 < fields.Length; i += 2)
                    {
                        if (fields[i] == "currentEpoch")
                            long.TryParse(fields[i + 1], NumberStyles.Integer, CultureInfo.InvariantCulture, out currentEpoch);
                        else if (fields[i] == "lastVoteEpoch")
                            long.TryParse(fields[i + 1], NumberStyles.Integer, CultureInfo.InvariantCulture, out lastVoteEpoch);
                    }

                    continue;
                }

                if (fields.Length < 8)
                    continue;

                var id = fields[0];
                var addr = fields[1];
                var flags = fields[2];
                var master = fields[3];
                var configEpoch = long.TryParse(
                    fields[6],
                    NumberStyles.Integer,
                    CultureInfo.InvariantCulture,
                    out var epoch)
                    ? epoch
                    : 0;

                var atIndex = addr.IndexOf('@');
                var hostPort = atIndex >= 0
                    ? addr[..atIndex]
                    : addr;

                var busPortText = atIndex >= 0 ? addr[(atIndex + 1)..] : "";
                var comma = busPortText.IndexOf(',');

                if (comma >= 0)
                    busPortText = busPortText[..comma];

                var busPort = int.TryParse(busPortText, out var bp) ? bp : 0;

                var colonIndex = hostPort.LastIndexOf(':');

                if (colonIndex < 0)
                    continue;

                var host = hostPort[..colonIndex];

                if (!int.TryParse(hostPort[(colonIndex + 1)..], out var port))
                    continue;

                if (busPort == 0)
                    busPort = port + 10000;

                var slots = new List<int>();
                var migrations = new List<(int, SlotMigration)>();

                for (var i = 8; i < fields.Length; i++)
                {
                    var token = fields[i];

                    if (token.StartsWith('['))
                    {
                        // [slot->-nodeid] migrating / [slot-<-nodeid] importing
                        var inner = token.Trim('[', ']');
                        var migrating = inner.IndexOf("->-", StringComparison.Ordinal);
                        var importing = inner.IndexOf("-<-", StringComparison.Ordinal);

                        if (migrating > 0 && int.TryParse(inner[..migrating], out var ms))
                            migrations.Add((ms, new SlotMigration(SlotMigrationState.Migrating, inner[(migrating + 3)..])));
                        else if (importing > 0 && int.TryParse(inner[..importing], out var isl))
                            migrations.Add((isl, new SlotMigration(SlotMigrationState.Importing, inner[(importing + 3)..])));

                        continue;
                    }

                    var dash = token.IndexOf('-');

                    if (dash > 0)
                    {
                        if (int.TryParse(token[..dash], out var s) &&
                            int.TryParse(token[(dash + 1)..], out var e))
                        {
                            for (var slot = s; slot <= e && slot < HashSlot.SlotCount; slot++)
                                slots.Add(slot);
                        }
                    }
                    else if (int.TryParse(token, out var single) && single < HashSlot.SlotCount)
                    {
                        slots.Add(single);
                    }
                }

                result.Add(new ParsedNode(
                    id,
                    host,
                    port,
                    busPort,
                    flags,
                    master == "-" ? null : master,
                    configEpoch,
                    slots,
                    migrations));
            }

            return result;
        }

        private bool TryLoad(
            string path,
            string fallbackHost,
            int fallbackPort)
        {
            try
            {
                var text = File.ReadAllText(path);
                var parsed = ParseNodesText(text, out var currentEpoch, out var lastVoteEpoch);

                if (parsed.Count == 0)
                    return false;

                var selfParsed =
                    parsed.FirstOrDefault(p => p.Flags.Split(',').Contains("myself")) ??
                    parsed.FirstOrDefault(p => p.Host == fallbackHost && p.Port == fallbackPort) ??
                    parsed[0];

                foreach (var p in parsed)
                {
                    var isSelf = ReferenceEquals(p, selfParsed);
                    var flagSet = p.Flags.Split(',');

                    var node = new ClusterNode
                    {
                        Id = p.Id,
                        Host = p.Host,
                        Port = p.Port,
                        ClusterBusPort = p.ClusterBusPort,
                        Role = flagSet.Contains("master")
                            ? ClusterNodeRole.Master
                            : ClusterNodeRole.Replica,
                        MasterId = p.MasterId,
                        ConfigEpoch = p.ConfigEpoch,
                        Flags = isSelf
                            ? ClusterNodeFlags.Myself
                            : flagSet.Contains("fail")
                                ? ClusterNodeFlags.Fail
                                : ClusterNodeFlags.None,
                    };

                    if (node.IsFailed)
                        node.FailTimeMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

                    _nodes[node.Id] = node;

                    if (isSelf)
                        _self = node;

                    currentEpoch = Math.Max(currentEpoch, p.ConfigEpoch);
                }

                foreach (var p in parsed)
                {
                    var node = _nodes[p.Id];

                    if (node.IsMaster)
                    {
                        foreach (var slot in p.Slots)
                            _slots[slot] = node;
                    }

                    if (node.IsMyself)
                    {
                        foreach (var (slot, migration) in p.Migrations)
                            _migrations[slot] = migration;
                    }
                }

                Interlocked.Exchange(ref _currentEpoch, currentEpoch);
                Interlocked.Exchange(ref _lastVoteEpoch, lastVoteEpoch);
                RefreshSnapshot();
                return true;
            }
            catch (IOException)
            {
                return false;
            }
        }

        // Called with _lock held. Written to a temp file and renamed
        // over the real one, so a crash mid-write can never leave a
        // truncated nodes.conf (which would cost this node its
        // identity and its vote history).
        internal void Save()
        {
            try
            {
                var directory = Path.GetDirectoryName(_configPath);

                if (!string.IsNullOrEmpty(directory))
                    Directory.CreateDirectory(directory);

                var temp = _configPath + ".tmp";

                File.WriteAllText(temp, ToNodesTextUnlocked(includeVars: true));
                File.Move(temp, _configPath, overwrite: true);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // Best-effort persistence — an unwritable cluster
                // config file shouldn't take the whole node down.
            }
        }

        private string ToNodesTextUnlocked(bool includeVars)
        {
            var sb = new StringBuilder();

            foreach (var node in _nodes.Values)
                sb.Append(FormatNodeLine(node)).Append('\n');

            if (includeVars)
            {
                sb.Append(
                    $"vars currentEpoch {CurrentEpoch} " +
                    $"lastVoteEpoch {LastVoteEpoch}\n");
            }

            return sb.ToString();
        }

        private static string ResolvePath(string contentRoot, string path)
            => Path.IsPathRooted(path)
                ? path
                : Path.Combine(contentRoot, path);

        internal static string GenerateNodeId()
            => Convert.ToHexString(RandomNumberGenerator.GetBytes(20))
                .ToLowerInvariant();
    }
}
