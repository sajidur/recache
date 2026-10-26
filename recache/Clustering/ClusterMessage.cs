using recache.Protocol;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace recache.Clustering
{
    internal enum ClusterMessageType
    {
        Ping,
        Pong,
        Meet,
        Fail,
        AuthRequest,
        AuthAck,
        Update,
        MfStart,

        // Sent by a master that's shutting down to its best replica:
        // "take over my slots now" (a coordinated, zero-loss failover).
        FailoverRequest,
    }

    [Flags]
    internal enum ClusterMessageFlags
    {
        None = 0,
        Master = 1 << 0,
        Replica = 1 << 1,

        // The sender (a master in a manual failover) has paused
        // writes; ReplOffset is final.
        Paused = 1 << 2,

        // An AuthRequest for a manual failover: vote even though the
        // master isn't failing.
        ForceAck = 1 << 3,
    }

    [Flags]
    internal enum GossipFlags
    {
        None = 0,
        Master = 1 << 0,
        Replica = 1 << 1,
        PFail = 1 << 2,
        Fail = 1 << 3,
    }

    internal sealed record GossipEntry(
        string Id,
        string Host,
        int Port,
        int BusPort,
        GossipFlags Flags);

    // One cluster-bus message. Every message carries the sender's full
    // header — identity, address, role, epochs, replication offset and
    // slot bitmap — so any message (not just PING/PONG) refreshes the
    // receiver's view of the sender; PING/PONG/MEET also gossip about a
    // few other nodes, which is how failure reports and new nodes
    // spread without anyone needing a full mesh of MEETs.
    //
    // Encoded as a RESP array of bulk strings so the bus reuses the
    // same parser as client traffic.
    internal sealed class ClusterMessage
    {
        private const string Magic = "RCBUS1";
        private const int HeaderFields = 13;

        public ClusterMessageType Type { get; init; }

        public required string SenderId { get; init; }

        public long CurrentEpoch { get; init; }

        // The sender's configEpoch — or, for a replica, its master's
        // (a replica advertises the slots and epoch of its master).
        public long ConfigEpoch { get; init; }

        public ClusterMessageFlags Flags { get; init; }

        public string? MasterId { get; init; }

        // Empty when the sender doesn't know its own address; the
        // receiver then uses the connection's remote address.
        public string Host { get; init; } = "";

        public int Port { get; init; }

        public int BusPort { get; init; }

        public long ReplOffset { get; init; }

        public byte[] Slots { get; init; } = new byte[HashSlot.SlotCount / 8];

        public List<GossipEntry> Gossip { get; init; } = new();

        // FAIL: the node now agreed to be failing.
        public string? FailNodeId { get; init; }

        // UPDATE: a node's newer slot configuration.
        public string? UpdateNodeId { get; init; }

        public long UpdateConfigEpoch { get; init; }

        public byte[]? UpdateSlots { get; init; }

        public bool SenderIsMaster => (Flags & ClusterMessageFlags.Master) != 0;

        public bool OwnsSlot(int slot)
            => (Slots[slot >> 3] & (1 << (slot & 7))) != 0;

        public static IEnumerable<int> SlotsIn(byte[] bitmap)
        {
            for (var slot = 0; slot < HashSlot.SlotCount; slot++)
            {
                if ((bitmap[slot >> 3] & (1 << (slot & 7))) != 0)
                    yield return slot;
            }
        }

        public RespValue Encode()
        {
            var items = new List<RespValue>(HeaderFields + Gossip.Count * 5 + 3)
            {
                RespValue.Bulk(Magic),
                RespValue.Bulk(Type.ToString()),
                RespValue.Bulk(SenderId),
                RespValue.Bulk(CurrentEpoch.ToString(CultureInfo.InvariantCulture)),
                RespValue.Bulk(ConfigEpoch.ToString(CultureInfo.InvariantCulture)),
                RespValue.Bulk(((int)Flags).ToString(CultureInfo.InvariantCulture)),
                RespValue.Bulk(MasterId ?? ""),
                RespValue.Bulk(Host),
                RespValue.Bulk(Port.ToString(CultureInfo.InvariantCulture)),
                RespValue.Bulk(BusPort.ToString(CultureInfo.InvariantCulture)),
                RespValue.Bulk(ReplOffset.ToString(CultureInfo.InvariantCulture)),
                RespValue.Bulk(Slots),
                RespValue.Bulk(Gossip.Count.ToString(CultureInfo.InvariantCulture)),
            };

            foreach (var entry in Gossip)
            {
                items.Add(RespValue.Bulk(entry.Id));
                items.Add(RespValue.Bulk(entry.Host));
                items.Add(RespValue.Bulk(entry.Port.ToString(CultureInfo.InvariantCulture)));
                items.Add(RespValue.Bulk(entry.BusPort.ToString(CultureInfo.InvariantCulture)));
                items.Add(RespValue.Bulk(((int)entry.Flags).ToString(CultureInfo.InvariantCulture)));
            }

            switch (Type)
            {
                case ClusterMessageType.Fail:
                    items.Add(RespValue.Bulk(FailNodeId ?? ""));
                    break;

                case ClusterMessageType.Update:
                    items.Add(RespValue.Bulk(UpdateNodeId ?? ""));
                    items.Add(RespValue.Bulk(UpdateConfigEpoch.ToString(CultureInfo.InvariantCulture)));
                    items.Add(RespValue.Bulk(UpdateSlots ?? new byte[HashSlot.SlotCount / 8]));
                    break;
            }

            return RespValue.Array(items);
        }

        public static ClusterMessage? Decode(RespValue value)
        {
            if (value.Type != RespType.Array ||
                value.Items is not { Count: >= HeaderFields } items ||
                items[0].AsString() != Magic ||
                !Enum.TryParse<ClusterMessageType>(items[1].AsString(), out var type))
            {
                return null;
            }

            var slots = items[11].BinaryValue;

            if (slots is null || slots.Length != HashSlot.SlotCount / 8)
                return null;

            var gossipCount = ParseInt(items[12]);
            var position = HeaderFields;

            if (gossipCount < 0 || items.Count < position + gossipCount * 5)
                return null;

            var gossip = new List<GossipEntry>(gossipCount);

            for (var i = 0; i < gossipCount; i++)
            {
                gossip.Add(new GossipEntry(
                    items[position].AsString() ?? "",
                    items[position + 1].AsString() ?? "",
                    ParseInt(items[position + 2]),
                    ParseInt(items[position + 3]),
                    (GossipFlags)ParseInt(items[position + 4])));

                position += 5;
            }

            string? failNode = null;
            string? updateNode = null;
            long updateEpoch = 0;
            byte[]? updateSlots = null;

            if (type == ClusterMessageType.Fail && items.Count > position)
            {
                failNode = items[position].AsString();
            }
            else if (type == ClusterMessageType.Update && items.Count >= position + 3)
            {
                updateNode = items[position].AsString();
                updateEpoch = ParseLong(items[position + 1]);
                updateSlots = items[position + 2].BinaryValue;

                if (updateSlots is null || updateSlots.Length != HashSlot.SlotCount / 8)
                    return null;
            }

            var masterId = items[6].AsString();

            return new ClusterMessage
            {
                Type = type,
                SenderId = items[2].AsString() ?? "",
                CurrentEpoch = ParseLong(items[3]),
                ConfigEpoch = ParseLong(items[4]),
                Flags = (ClusterMessageFlags)ParseInt(items[5]),
                MasterId = string.IsNullOrEmpty(masterId) ? null : masterId,
                Host = items[7].AsString() ?? "",
                Port = ParseInt(items[8]),
                BusPort = ParseInt(items[9]),
                ReplOffset = ParseLong(items[10]),
                Slots = slots,
                Gossip = gossip,
                FailNodeId = failNode,
                UpdateNodeId = updateNode,
                UpdateConfigEpoch = updateEpoch,
                UpdateSlots = updateSlots,
            };
        }

        private static int ParseInt(RespValue value)
            => int.TryParse(value.AsString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var result)
                ? result
                : -1;

        private static long ParseLong(RespValue value)
            => long.TryParse(value.AsString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var result)
                ? result
                : 0;
    }
}
