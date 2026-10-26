using recache.Clustering;
using recache.Commands;
using recache.Protocol;
using recache.Storage;
using System.Text;

namespace recache.Tests.Unit;

public class RespParserTests
{
    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    [Fact]
    public void Parses_a_complete_command()
    {
        var input = Bytes("*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n");

        var status = RespParser.TryParseRequest(input, 1024, out var request, out var consumed, out _, out _);

        Assert.Equal(RespParseStatus.Complete, status);
        Assert.Equal(input.Length, consumed);
        Assert.Equal(new[] { "SET", "k", "hello" }, request!.Items!.Select(i => i.AsString()));
    }

    [Fact]
    public void Every_prefix_of_a_command_is_incomplete_and_consumes_nothing()
    {
        var input = Bytes("*2\r\n$4\r\nECHO\r\n$11\r\nhello world\r\n");

        for (var length = 0; length < input.Length; length++)
        {
            var status = RespParser.TryParseRequest(input.AsSpan(0, length), 1024, out _, out var consumed, out _, out _);

            Assert.Equal(RespParseStatus.Incomplete, status);
            Assert.Equal(0, consumed);
        }
    }

    [Fact]
    public void Reports_how_many_bytes_a_large_value_needs()
    {
        var header = Bytes("*2\r\n$3\r\nSET\r\n$100000\r\n");

        var status = RespParser.TryParseRequest(header, 1 << 20, out _, out _, out var needed, out _);

        Assert.Equal(RespParseStatus.Incomplete, status);
        Assert.Equal(header.Length + 100000 + 2, needed);
    }

    [Fact]
    public void Parses_pipelined_commands_one_at_a_time()
    {
        var input = Bytes("*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n");

        RespParser.TryParseRequest(input, 1024, out var first, out var consumed, out _, out _);
        RespParser.TryParseRequest(input.AsSpan(consumed), 1024, out var second, out var consumed2, out _, out _);

        Assert.Equal("PING", first!.Items![0].AsString());
        Assert.Equal("GET", second!.Items![0].AsString());
        Assert.Equal(input.Length, consumed + consumed2);
    }

    [Theory]
    [InlineData("PING\r\n", new[] { "PING" })]
    [InlineData("SET k v\n", new[] { "SET", "k", "v" })]
    [InlineData("SET \"a b\" 'c d'\r\n", new[] { "SET", "a b", "c d" })]
    [InlineData("ECHO \"x\\ny\"\r\n", new[] { "ECHO", "x\ny" })]
    public void Parses_inline_commands(string line, string[] expected)
    {
        var status = RespParser.TryParseRequest(Bytes(line), 1024, out var request, out var consumed, out _, out _);

        Assert.Equal(RespParseStatus.Complete, status);
        Assert.Equal(Bytes(line).Length, consumed);
        Assert.Equal(expected, request!.Items!.Select(i => i.AsString()));
    }

    [Fact]
    public void Rejects_malformed_input()
    {
        Assert.Equal(
            RespParseStatus.Invalid,
            RespParser.TryParseRequest(Bytes("*1\r\n+PING\r\n"), 1024, out _, out _, out _, out _));

        Assert.Equal(
            RespParseStatus.Invalid,
            RespParser.TryParseRequest(Bytes("*x\r\n"), 1024, out _, out _, out _, out _));

        Assert.Equal(
            RespParseStatus.Invalid,
            RespParser.TryParseRequest(Bytes("*1\r\n$5000\r\n"), 1024, out _, out _, out _, out _));
    }

    [Fact]
    public void Serializer_round_trips_through_the_value_parser()
    {
        var value = RespValue.Array(new[]
        {
            RespValue.Simple("OK"),
            RespValue.Error("ERR nope"),
            RespValue.Integer(-42),
            RespValue.Integer(1L << 40),
            RespValue.Bulk("héllo"),
            RespValue.Null(),
            RespValue.NullArray(),
            RespValue.Array(new[] { RespValue.Bulk("nested") }),
        });

        var bytes = RespSerializer.ToBytes(value);
        var status = RespParser.TryParseValue(bytes, 1024, out var parsed, out var consumed, out _, out _);

        Assert.Equal(RespParseStatus.Complete, status);
        Assert.Equal(bytes.Length, consumed);
        Assert.Equal(bytes, RespSerializer.ToBytes(parsed!));
    }
}

public class HashSlotTests
{
    [Theory]
    [InlineData("foo", 12182)]
    [InlineData("bar", 5061)]
    [InlineData("somekey", 11058)]
    public void Matches_real_redis_slot_numbers(string key, int slot)
        => Assert.Equal(slot, HashSlot.Compute(key));

    [Fact]
    public void Crc16_matches_the_xmodem_check_value()
        => Assert.Equal(0x31C3, Crc16.Compute("123456789"u8));

    [Fact]
    public void Hash_tags_colocate_keys()
    {
        Assert.Equal(HashSlot.Compute("user1000"), HashSlot.Compute("{user1000}.following"));
        Assert.Equal(HashSlot.Compute("{user1000}.followers"), HashSlot.Compute("{user1000}.following"));

        // Empty braces don't count as a tag.
        Assert.NotEqual(HashSlot.Compute(""), HashSlot.Compute("{}abc"));
    }

    [Fact]
    public void Byte_and_string_forms_agree()
    {
        foreach (var key in new[] { "a", "key:123", "ünïcödé", "{tag}x" })
            Assert.Equal(HashSlot.Compute(key), HashSlot.Compute(Encoding.UTF8.GetBytes(key)));
    }
}

public class CommandTableTests
{
    private static IReadOnlyList<RespValue> Args(params string[] parts)
        => RespValue.Command(parts).Items!;

    [Fact]
    public void Lookup_is_case_insensitive_from_bytes()
    {
        Assert.Same(CommandTable.Lookup("GET"), CommandTable.Lookup("get"u8));
        Assert.Null(CommandTable.Lookup("NOSUCHCOMMAND"u8));
    }

    [Fact]
    public void Key_positions_follow_redis_conventions()
    {
        Assert.Equal(new[] { 1, 3 }, CommandTable.Lookup("MSET")!.GetKeyIndexes(Args("MSET", "a", "1", "b", "2")));
        Assert.Equal(new[] { 1, 2, 3 }, CommandTable.Lookup("DEL")!.GetKeyIndexes(Args("DEL", "a", "b", "c")));
        Assert.Equal(new[] { 3 }, CommandTable.Lookup("MIGRATE")!.GetKeyIndexes(Args("MIGRATE", "h", "1", "k", "0", "5")));

        Assert.Equal(
            new[] { 10, 11 },
            CommandTable.Lookup("MIGRATE")!.GetKeyIndexes(
                Args("MIGRATE", "h", "1", "", "0", "5", "REPLACE", "AUTH", "pw", "KEYS", "a", "b")));
    }

    [Fact]
    public void Stripe_of_bytes_matches_stripe_of_string()
    {
        foreach (var key in new[] { "a", "key:1", "ünïcödé", new string('x', 300) })
            Assert.Equal(KeyLocks.StripeOf(key), KeyLocks.StripeOf(Encoding.UTF8.GetBytes(key)));
    }
}
