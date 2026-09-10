using recache.Configuration;
using System;
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

    // In-memory (plus flat-file persisted) cluster topology for this
    // node: which node owns which of the 16384 hash slots, and what
    // other nodes are known. There is no gossip yet (Phase 2) — the
    // topology here is only ever changed by local CLUSTER commands
    // or by importing a peer's CLUSTER NODES text via CLUSTER MEET.
    //
    // Guarded by its own lock, deliberately separate from
    // CommandDispatcher's _executionLock: topology changes are rare
    // admin operations, unrelated to the read/write locking that
    // protects key-space consistency.
    public sealed class ClusterState
    {
        private readonly object _lock = new();
        private readonly string _configPath;
        private readonly Dictionary<string, ClusterNode> _nodes =
            new(StringComparer.Ordinal);
        private readonly string?[] _slotOwners =
            new string?[HashSlot.SlotCount];
        private readonly Dictionary<int, SlotMigration> _migrations =
            new();

        public string MyId { get; private set; } = "";

        public long CurrentEpoch { get; private set; }

        public ClusterState(RecacheOptions options, string contentRoot)
        {
            _configPath = ResolvePath(
                contentRoot,
                options.ClusterConfigFile);

            var announceHost =
                string.IsNullOrWhiteSpace(options.ClusterAnnounceIp)
                    ? "127.0.0.1"
                    : options.ClusterAnnounceIp!;

            var announcePort =
                options.ClusterAnnouncePort > 0
                    ? options.ClusterAnnouncePort
                    : options.Port;

            if (File.Exists(_configPath) &&
                TryLoad(_configPath, announceHost, announcePort))
            {
                return;
            }

            MyId = GenerateNodeId();

            _nodes[MyId] = new ClusterNode
            {
                Id = MyId,
                Host = announceHost,
                Port = announcePort,
                ClusterBusPort = announcePort + 10000,
                Role = ClusterNodeRole.Master,
                Flags = ClusterNodeFlags.Myself,
            };

            Save();
        }

        public ClusterNode Self
        {
            get { lock (_lock) return _nodes[MyId]; }
        }

        public IReadOnlyList<ClusterNode> Nodes
        {
            get { lock (_lock) return _nodes.Values.ToArray(); }
        }

        public ClusterNode? GetNode(string id)
        {
            lock (_lock)
                return _nodes.TryGetValue(id, out var node) ? node : null;
        }

        public ClusterNode? GetOwner(int slot)
        {
            lock (_lock)
            {
                var ownerId = _slotOwners[slot];

                return ownerId is null
                    ? null
                    : _nodes.GetValueOrDefault(ownerId);
            }
        }

        public SlotMigration? GetMigration(int slot)
        {
            lock (_lock)
                return _migrations.TryGetValue(slot, out var m) ? m : null;
        }

        // Returns the slots that were already owned by someone
        // (including self) — real Redis rejects the whole command
        // in that case rather than partially applying it.
        public IReadOnlyList<int> AddSlots(IEnumerable<int> slots)
        {
            lock (_lock)
            {
                var requested = slots.Distinct().ToArray();
                var busy = requested
                    .Where(s => _slotOwners[s] is not null)
                    .ToArray();

                if (busy.Length > 0)
                    return busy;

                foreach (var slot in requested)
                    _slotOwners[slot] = MyId;

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
                    .Where(s => _slotOwners[s] is null)
                    .ToArray();

                if (unassigned.Length > 0)
                    return unassigned;

                foreach (var slot in requested)
                {
                    _slotOwners[slot] = null;
                    _migrations.Remove(slot);
                }

                Save();
                return Array.Empty<int>();
            }
        }

        public void SetMigrating(int slot, string targetNodeId)
        {
            lock (_lock)
                _migrations[slot] =
                    new SlotMigration(SlotMigrationState.Migrating, targetNodeId);
        }

        public void SetImporting(int slot, string sourceNodeId)
        {
            lock (_lock)
                _migrations[slot] =
                    new SlotMigration(SlotMigrationState.Importing, sourceNodeId);
        }

        public void ClearMigration(int slot)
        {
            lock (_lock)
                _migrations.Remove(slot);
        }

        // CLUSTER SETSLOT <slot> NODE <id> — finalizes ownership
        // after a (manually driven, in Phase 1) key migration.
        public void AssignSlot(int slot, string ownerId)
        {
            lock (_lock)
            {
                _slotOwners[slot] = ownerId;
                _migrations.Remove(slot);
                Save();
            }
        }

        // CLUSTER RESET — clears slot ownership and forgets every
        // other node. HARD also discards this node's own identity
        // (fresh node id, epoch back to 0), matching real Redis.
        public void Reset(bool hard)
        {
            lock (_lock)
            {
                for (var i = 0; i < _slotOwners.Length; i++)
                    _slotOwners[i] = null;

                _migrations.Clear();

                foreach (var id in _nodes.Keys.Where(id => id != MyId).ToArray())
                    _nodes.Remove(id);

                var host = _nodes[MyId].Host;
                var port = _nodes[MyId].Port;
                var busPort = _nodes[MyId].ClusterBusPort;

                if (hard)
                {
                    _nodes.Remove(MyId);
                    MyId = GenerateNodeId();
                    CurrentEpoch = 0;
                }

                _nodes[MyId] = new ClusterNode
                {
                    Id = MyId,
                    Host = host,
                    Port = port,
                    ClusterBusPort = busPort,
                    Role = ClusterNodeRole.Master,
                    Flags = ClusterNodeFlags.Myself,
                };

                Save();
            }
        }

        public bool AddOrUpdateNode(ClusterNode node)
        {
            lock (_lock)
            {
                var isNew = !_nodes.ContainsKey(node.Id);
                _nodes[node.Id] = node;
                Save();
                return isNew;
            }
        }

        public bool Forget(string id)
        {
            lock (_lock)
            {
                if (id == MyId)
                    return false;

                if (!_nodes.Remove(id))
                    return false;

                for (var i = 0; i < _slotOwners.Length; i++)
                {
                    if (_slotOwners[i] == id)
                        _slotOwners[i] = null;
                }

                Save();
                return true;
            }
        }

        public void SetRole(ClusterNodeRole role, string? masterId)
        {
            lock (_lock)
            {
                var self = _nodes[MyId];
                self.Role = role;
                self.MasterId = masterId;
                Save();
            }
        }

        public void SetConfigEpoch(long epoch)
        {
            lock (_lock)
            {
                _nodes[MyId].ConfigEpoch = epoch;

                if (epoch > CurrentEpoch)
                    CurrentEpoch = epoch;

                Save();
            }
        }

        public long BumpEpoch()
        {
            lock (_lock)
            {
                CurrentEpoch++;
                _nodes[MyId].ConfigEpoch = CurrentEpoch;
                Save();
                return CurrentEpoch;
            }
        }

        // Merges a peer's CLUSTER NODES snapshot into our own view —
        // the Phase-1 stand-in for gossip. One-shot, not kept in
        // sync afterwards: re-run CLUSTER MEET to refresh.
        public void MergeNodesText(string text)
        {
            var parsed = ParseNodesText(text);

            lock (_lock)
            {
                foreach (var p in parsed)
                {
                    if (p.Id == MyId)
                        continue;

                    var flags = ClusterNodeFlags.None;

                    _nodes[p.Id] = new ClusterNode
                    {
                        Id = p.Id,
                        Host = p.Host,
                        Port = p.Port,
                        ClusterBusPort = p.ClusterBusPort,
                        Role = p.Role,
                        MasterId = p.MasterId,
                        ConfigEpoch = p.ConfigEpoch,
                        Flags = flags,
                    };

                    if (p.Role != ClusterNodeRole.Master)
                        continue;

                    foreach (var slot in p.Slots)
                    {
                        // Don't let an imported snapshot override
                        // slots we already believe we own.
                        if (_slotOwners[slot] != MyId)
                            _slotOwners[slot] = p.Id;
                    }
                }

                Save();
            }
        }

        public string ToNodesText()
        {
            lock (_lock)
            {
                var sb = new StringBuilder();

                foreach (var node in _nodes.Values)
                    sb.Append(FormatNodeLine(node)).Append('\n');

                return sb.ToString();
            }
        }

        private string FormatNodeLine(ClusterNode node)
        {
            var flagParts = new List<string>();

            if (node.Id == MyId)
                flagParts.Add("myself");

            flagParts.Add(
                node.Role == ClusterNodeRole.Master ? "master" : "slave");

            if ((node.Flags & ClusterNodeFlags.Fail) != 0)
                flagParts.Add("fail");
            else if ((node.Flags & ClusterNodeFlags.PFail) != 0)
                flagParts.Add("fail?");

            var master = node.MasterId ?? "-";

            var slots = node.Role == ClusterNodeRole.Master
                ? FormatSlotRanges(node.Id)
                : "";

            return string.Join(
                ' ',
                node.Id,
                $"{node.Host}:{node.Port}@{node.ClusterBusPort}",
                string.Join(',', flagParts),
                master,
                "0",
                "0",
                node.ConfigEpoch.ToString(CultureInfo.InvariantCulture),
                "connected",
                slots).TrimEnd();
        }

        private string FormatSlotRanges(string nodeId)
        {
            var ranges = new List<string>();
            var start = -1;

            for (var slot = 0; slot < _slotOwners.Length; slot++)
            {
                var owned = _slotOwners[slot] == nodeId;

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
                ranges.Add(FormatRange(start, _slotOwners.Length - 1));

            return string.Join(' ', ranges);
        }

        private static string FormatRange(int start, int end)
            => start == end ? start.ToString() : $"{start}-{end}";

        public IReadOnlyList<(int Start, int End, string NodeId)>
            SlotRanges()
        {
            lock (_lock)
            {
                var result = new List<(int, int, string)>();
                var start = -1;
                string? owner = null;

                for (var slot = 0; slot <= _slotOwners.Length; slot++)
                {
                    var current = slot < _slotOwners.Length
                        ? _slotOwners[slot]
                        : null;

                    if (current == owner && current is not null)
                        continue;

                    if (owner is not null)
                        result.Add((start, slot - 1, owner));

                    owner = current;
                    start = slot;
                }

                return result;
            }
        }

        private sealed record ParsedNode(
            string Id,
            string Host,
            int Port,
            int ClusterBusPort,
            ClusterNodeRole Role,
            string? MasterId,
            long ConfigEpoch,
            IReadOnlyList<int> Slots);

        private static List<ParsedNode> ParseNodesText(string text)
        {
            var result = new List<ParsedNode>();

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

                var busPort = atIndex >= 0 &&
                    int.TryParse(
                        addr[(atIndex + 1)..],
                        out var bp)
                    ? bp
                    : 0;

                var colonIndex = hostPort.LastIndexOf(':');

                if (colonIndex < 0)
                    continue;

                var host = hostPort[..colonIndex];

                if (!int.TryParse(hostPort[(colonIndex + 1)..], out var port))
                    continue;

                var role = flags.Contains("master")
                    ? ClusterNodeRole.Master
                    : ClusterNodeRole.Replica;

                var slots = new List<int>();

                for (var i = 8; i < fields.Length; i++)
                {
                    var token = fields[i];
                    var dash = token.IndexOf('-');

                    if (dash > 0)
                    {
                        if (int.TryParse(token[..dash], out var s) &&
                            int.TryParse(token[(dash + 1)..], out var e))
                        {
                            for (var slot = s; slot <= e; slot++)
                                slots.Add(slot);
                        }
                    }
                    else if (int.TryParse(token, out var single))
                    {
                        slots.Add(single);
                    }
                }

                result.Add(new ParsedNode(
                    id,
                    host,
                    port,
                    busPort,
                    role,
                    master == "-" ? null : master,
                    configEpoch,
                    slots));
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
                var parsed = ParseNodesText(text);

                if (parsed.Count == 0)
                    return false;

                var self = parsed.FirstOrDefault(
                    p => p.Host == fallbackHost && p.Port == fallbackPort)
                    ?? parsed[0];

                MyId = self.Id;

                foreach (var p in parsed)
                {
                    _nodes[p.Id] = new ClusterNode
                    {
                        Id = p.Id,
                        Host = p.Host,
                        Port = p.Port,
                        ClusterBusPort = p.ClusterBusPort,
                        Role = p.Role,
                        MasterId = p.MasterId,
                        ConfigEpoch = p.ConfigEpoch,
                        Flags = p.Id == MyId
                            ? ClusterNodeFlags.Myself
                            : ClusterNodeFlags.None,
                    };

                    CurrentEpoch = Math.Max(CurrentEpoch, p.ConfigEpoch);

                    if (p.Role != ClusterNodeRole.Master)
                        continue;

                    foreach (var slot in p.Slots)
                        _slotOwners[slot] = p.Id;
                }

                return true;
            }
            catch (IOException)
            {
                return false;
            }
        }

        private void Save()
        {
            try
            {
                var directory = Path.GetDirectoryName(_configPath);

                if (!string.IsNullOrEmpty(directory))
                    Directory.CreateDirectory(directory);

                File.WriteAllText(_configPath, ToNodesTextUnlocked());
            }
            catch (IOException)
            {
                // Best-effort persistence — an unwritable cluster
                // config file shouldn't take the whole node down.
            }
        }

        // Save() is always called with _lock already held.
        private string ToNodesTextUnlocked()
        {
            var sb = new StringBuilder();

            foreach (var node in _nodes.Values)
                sb.Append(FormatNodeLine(node)).Append('\n');

            return sb.ToString();
        }

        private static string ResolvePath(string contentRoot, string path)
            => Path.IsPathRooted(path)
                ? path
                : Path.Combine(contentRoot, path);

        private static string GenerateNodeId()
            => Convert.ToHexString(RandomNumberGenerator.GetBytes(20))
                .ToLowerInvariant();
    }
}
