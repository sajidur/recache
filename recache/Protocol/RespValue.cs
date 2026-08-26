using System;
using System.Collections.Generic;
using System.Text;

namespace recache.Protocol
{
    public enum RespType
    {
        SimpleString,
        Error,
        Integer,
        BulkString,
        Null,
        Array,
        NullArray
    }

    public sealed class RespValue
    {
        public RespType Type { get; }

        public string? StringValue { get; }

        public byte[]? BinaryValue { get; }

        public long IntegerValue { get; }

        public IReadOnlyList<RespValue>? Items { get; }

        private RespValue(
            RespType type,
            string? stringValue = null,
            byte[]? binaryValue = null,
            long integerValue = 0,
            IReadOnlyList<RespValue>? items = null)
        {
            Type = type;
            StringValue = stringValue;
            BinaryValue = binaryValue;
            IntegerValue = integerValue;
            Items = items;
        }

        public static RespValue Simple(string value)
            => new(RespType.SimpleString, stringValue: value);

        public static RespValue Error(string value)
            => new(RespType.Error, stringValue: value);

        public static RespValue Integer(long value)
            => new(RespType.Integer, integerValue: value);

        public static RespValue Bulk(byte[] value)
            => new(RespType.BulkString, binaryValue: value);

        public static RespValue Null()
            => new(RespType.Null);

        // The RESP2 "null array" (*-1\r\n) — distinct from a null
        // bulk string. Real Redis sends this for EXEC aborted by a
        // failed WATCH, so a client waiting for an array-or-nil
        // reply sees the correct wire form.
        public static RespValue NullArray()
            => new(RespType.NullArray);

        public static RespValue Array(IReadOnlyList<RespValue> items)
            => new(RespType.Array, items: items);
    }
}
