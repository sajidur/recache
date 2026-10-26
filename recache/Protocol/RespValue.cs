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
        // RespValue is immutable, so the replies that make up the
        // bulk of all traffic (OK, PONG, nil, small integers) are
        // shared instances rather than a fresh allocation per reply.
        private static readonly RespValue OkValue =
            new(RespType.SimpleString, "OK");

        private static readonly RespValue PongValue =
            new(RespType.SimpleString, "PONG");

        private static readonly RespValue QueuedValue =
            new(RespType.SimpleString, "QUEUED");

        private static readonly RespValue NullValue =
            new(RespType.Null, null);

        private static readonly RespValue NullArrayValue =
            new(RespType.NullArray, null);

        private static readonly RespValue EmptyArrayValue =
            new(RespType.Array, System.Array.Empty<RespValue>());

        private const int SmallIntegerMin = -1;
        private const int SmallIntegerMax = 1024;

        private static readonly RespValue[] SmallIntegers =
            CreateSmallIntegers();

        // One payload slot (string / byte[] / item list, by type)
        // rather than a field per kind: every request argument is a
        // RespValue, so its size is paid several times per command.
        private readonly object? _payload;
        private readonly long _integer;

        private RespValue(RespType type, object? payload, long integer = 0)
        {
            Type = type;
            _payload = payload;
            _integer = integer;
        }

        public RespType Type { get; }

        public string? StringValue => _payload as string;

        public byte[]? BinaryValue => _payload as byte[];

        public long IntegerValue => _integer;

        public IReadOnlyList<RespValue>? Items => _payload switch
        {
            IReadOnlyList<RespValue> items => items,
            byte[][] bulks => System.Array.ConvertAll(bulks, Bulk),
            _ => null,
        };

        // The compact form of an array of bulk strings (see BulkArray);
        // null for any other value.
        internal byte[][]? BulkItems => _payload as byte[][];

        public static RespValue Ok => OkValue;

        public static RespValue EmptyArray => EmptyArrayValue;

        // String literals are interned, so a reference comparison
        // catches every Simple("OK")-style call site for free.
        public static RespValue Simple(string value)
        {
            if ((object)value == "OK")
                return OkValue;

            if ((object)value == "PONG")
                return PongValue;

            if ((object)value == "QUEUED")
                return QueuedValue;

            return new(RespType.SimpleString, value);
        }

        public static RespValue Error(string value)
            => new(RespType.Error, value);

        public static RespValue Integer(long value)
        {
            if (value >= SmallIntegerMin && value <= SmallIntegerMax)
                return SmallIntegers[value - SmallIntegerMin];

            return new(RespType.Integer, null, value);
        }

        public static RespValue Bulk(byte[] value)
            => new(RespType.BulkString, value);

        public static RespValue Bulk(string value)
            => new(RespType.BulkString, Encoding.UTF8.GetBytes(value));

        public static RespValue Null()
            => NullValue;

        // The RESP2 "null array" (*-1\r\n) — distinct from a null
        // bulk string. Real Redis sends this for EXEC aborted by a
        // failed WATCH, so a client waiting for an array-or-nil
        // reply sees the correct wire form.
        public static RespValue NullArray()
            => NullArrayValue;

        public static RespValue Array(IReadOnlyList<RespValue> items)
            => items.Count == 0
                ? EmptyArrayValue
                : new(RespType.Array, items);

        // An array reply of bulk strings without a RespValue per element
        // — LRANGE 0 99 is one object instead of a hundred and one.
        // Callers must not modify the arrays afterwards.
        public static RespValue BulkArray(byte[][] items)
            => items.Length == 0
                ? EmptyArrayValue
                : new(RespType.Array, items);

        // Builds a command (an array of bulk strings) — the shape
        // every request and every propagated write takes.
        public static RespValue Command(params string[] parts)
        {
            var items = new RespValue[parts.Length];

            for (var i = 0; i < parts.Length; i++)
                items[i] = Bulk(parts[i]);

            return new(RespType.Array, items);
        }

        public bool IsError => Type == RespType.Error;

        // Text of a simple string, error, or bulk string.
        public string? AsString()
            => _payload switch
            {
                byte[] bytes => Encoding.UTF8.GetString(bytes),
                string text => text,
                _ => null,
            };

        private static RespValue[] CreateSmallIntegers()
        {
            var values =
                new RespValue[SmallIntegerMax - SmallIntegerMin + 1];

            for (var i = 0; i < values.Length; i++)
            {
                values[i] = new RespValue(
                    RespType.Integer,
                    null,
                    i + SmallIntegerMin);
            }

            return values;
        }
    }
}
