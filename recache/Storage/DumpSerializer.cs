using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json.Nodes;

namespace recache.Storage
{
    // The DUMP/RESTORE payload format (what MIGRATE ships between
    // nodes while resharding). Recache's own format, not Redis RDB:
    //
    //   [type:1] [body] [format version:2 LE] [CRC-32 of everything
    //   before it:4 LE]
    //
    // Same trailer layout as Redis (version then checksum), so a
    // payload from one product is cleanly rejected by the other
    // rather than misread.
    public static class DumpSerializer
    {
        private const ushort FormatVersion = 0x5243; // "RC"

        public static byte[] Serialize(CacheEntry entry)
        {
            var buffer = new ArrayBufferWriter<byte>(64);

            buffer.Write(new[] { (byte)entry.Type });

            switch (entry.Type)
            {
                case CacheValueType.String:
                    WriteBytes(buffer, (byte[])entry.Payload);
                    break;

                case CacheValueType.Hash:
                {
                    var fields = ((RedisHash)entry.Payload).Snapshot();
                    WriteLength(buffer, fields.Length);

                    foreach (var field in fields)
                    {
                        WriteBytes(buffer, Encoding.UTF8.GetBytes(field.Key));
                        WriteBytes(buffer, field.Value);
                    }

                    break;
                }

                case CacheValueType.List:
                {
                    var items = ((RedisList)entry.Payload).Range(0, -1);
                    WriteLength(buffer, items.Length);

                    foreach (var item in items)
                        WriteBytes(buffer, item);

                    break;
                }

                case CacheValueType.Set:
                {
                    var members = ((RedisSet)entry.Payload).Snapshot();
                    WriteLength(buffer, members.Length);

                    foreach (var member in members)
                        WriteBytes(buffer, member);

                    break;
                }

                case CacheValueType.SortedSet:
                {
                    var items = ((RedisSortedSet)entry.Payload).Range(0, -1);
                    WriteLength(buffer, items.Length);

                    foreach (var (member, score) in items)
                    {
                        WriteBytes(buffer, Encoding.UTF8.GetBytes(member));

                        var span = buffer.GetSpan(8);
                        BinaryPrimitives.WriteDoubleLittleEndian(span, score);
                        buffer.Advance(8);
                    }

                    break;
                }

                case CacheValueType.Json:
                {
                    ((RedisJson)entry.Payload).TryGet(Array.Empty<string>(), out var json);
                    WriteBytes(buffer, Encoding.UTF8.GetBytes(json ?? "null"));
                    break;
                }

                default:
                    throw new InvalidOperationException($"Cannot DUMP type {entry.Type}");
            }

            var trailer = buffer.GetSpan(2);
            BinaryPrimitives.WriteUInt16LittleEndian(trailer, FormatVersion);
            buffer.Advance(2);

            var crc = Crc32.Compute(buffer.WrittenSpan);
            var crcSpan = buffer.GetSpan(4);
            BinaryPrimitives.WriteUInt32LittleEndian(crcSpan, crc);
            buffer.Advance(4);

            return buffer.WrittenSpan.ToArray();
        }

        // Returns false for a payload that isn't ours, or is damaged.
        public static bool TryDeserialize(
            byte[] payload,
            out CacheValueType type,
            out object? value,
            out long bytes)
        {
            type = default;
            value = null;
            bytes = 0;

            if (payload.Length < 7)
                return false;

            var body = payload.AsSpan(0, payload.Length - 4);
            var expectedCrc = BinaryPrimitives.ReadUInt32LittleEndian(payload.AsSpan(payload.Length - 4));

            if (Crc32.Compute(body) != expectedCrc)
                return false;

            if (BinaryPrimitives.ReadUInt16LittleEndian(payload.AsSpan(payload.Length - 6, 2)) != FormatVersion)
                return false;

            var reader = new Reader(payload.AsSpan(0, payload.Length - 6));

            try
            {
                type = (CacheValueType)reader.ReadByte();

                switch (type)
                {
                    case CacheValueType.String:
                    {
                        var data = reader.ReadBytes();
                        value = data;
                        bytes = data.Length;
                        break;
                    }

                    case CacheValueType.Hash:
                    {
                        var hash = new RedisHash();
                        var count = reader.ReadLength();

                        for (var i = 0; i < count; i++)
                        {
                            var field = Encoding.UTF8.GetString(reader.ReadBytes());
                            hash.Set(field, reader.ReadBytes(), out var delta);
                            bytes += delta;
                        }

                        value = hash;
                        break;
                    }

                    case CacheValueType.List:
                    {
                        var list = new RedisList();
                        var count = reader.ReadLength();

                        for (var i = 0; i < count; i++)
                            bytes += list.PushRight(reader.ReadBytes());

                        value = list;
                        break;
                    }

                    case CacheValueType.Set:
                    {
                        var set = new RedisSet();
                        var count = reader.ReadLength();

                        for (var i = 0; i < count; i++)
                        {
                            set.Add(reader.ReadBytes(), out var delta);
                            bytes += delta;
                        }

                        value = set;
                        break;
                    }

                    case CacheValueType.SortedSet:
                    {
                        var zset = new RedisSortedSet();
                        var count = reader.ReadLength();

                        for (var i = 0; i < count; i++)
                        {
                            var member = Encoding.UTF8.GetString(reader.ReadBytes());
                            zset.Add(member, reader.ReadDouble(), out var delta);
                            bytes += delta;
                        }

                        value = zset;
                        break;
                    }

                    case CacheValueType.Json:
                    {
                        var json = new RedisJson();
                        var text = Encoding.UTF8.GetString(reader.ReadBytes());

                        json.TrySet("$", JsonNode.Parse(text), out var delta);
                        bytes = delta;
                        value = json;
                        break;
                    }

                    default:
                        return false;
                }

                return reader.AtEnd;
            }
            catch (Exception ex) when (ex is IndexOutOfRangeException or ArgumentOutOfRangeException or System.Text.Json.JsonException or InvalidDataException)
            {
                return false;
            }
        }

        private static void WriteLength(IBufferWriter<byte> buffer, int length)
        {
            var span = buffer.GetSpan(5);
            var value = (uint)length;
            var i = 0;

            while (value >= 0x80)
            {
                span[i++] = (byte)(value | 0x80);
                value >>= 7;
            }

            span[i++] = (byte)value;
            buffer.Advance(i);
        }

        private static void WriteBytes(IBufferWriter<byte> buffer, byte[] data)
        {
            WriteLength(buffer, data.Length);
            buffer.Write(data);
        }

        private ref struct Reader
        {
            private readonly ReadOnlySpan<byte> _data;
            private int _position;

            public Reader(ReadOnlySpan<byte> data)
            {
                _data = data;
                _position = 0;
            }

            public bool AtEnd => _position == _data.Length;

            public byte ReadByte() => _data[_position++];

            public int ReadLength()
            {
                uint value = 0;
                var shift = 0;

                while (true)
                {
                    if (shift > 28)
                        throw new InvalidDataException("bad length");

                    var b = _data[_position++];
                    value |= (uint)(b & 0x7F) << shift;

                    if ((b & 0x80) == 0)
                        break;

                    shift += 7;
                }

                // Every byte string and every collection element takes
                // at least one byte, so nothing valid can claim more
                // than what's left.
                if (value > (uint)(_data.Length - _position))
                    throw new InvalidDataException("bad length");

                return (int)value;
            }

            public byte[] ReadBytes()
            {
                var length = ReadLength();
                var bytes = _data.Slice(_position, length).ToArray();
                _position += length;
                return bytes;
            }

            public double ReadDouble()
            {
                var value = BinaryPrimitives.ReadDoubleLittleEndian(_data.Slice(_position, 8));
                _position += 8;
                return value;
            }
        }

        private static class Crc32
        {
            private static readonly uint[] Table = BuildTable();

            public static uint Compute(ReadOnlySpan<byte> data)
            {
                var crc = 0xFFFFFFFFu;

                foreach (var b in data)
                    crc = Table[(crc ^ b) & 0xFF] ^ (crc >> 8);

                return ~crc;
            }

            private static uint[] BuildTable()
            {
                var table = new uint[256];

                for (uint i = 0; i < 256; i++)
                {
                    var c = i;

                    for (var k = 0; k < 8; k++)
                        c = (c & 1) != 0 ? 0xEDB88320u ^ (c >> 1) : c >> 1;

                    table[i] = c;
                }

                return table;
            }
        }
    }
}
