using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace recache.Storage
{
    // Striped reader/writer locks over the keyspace. Every
    // key-bearing command holds the stripe(s) of its key(s) for its
    // whole execution — shared for reads, exclusive for writes.
    //
    // That single rule is what makes the rest of the server correct
    // without being single-threaded like Redis:
    //  - read-modify-write commands (APPEND, SETRANGE, HDEL emptying
    //    a hash) can't interleave with another writer of the same key
    //  - a write's entry in the replication stream / AOF is appended
    //    while the stripe is still held, so per-key order in the log
    //    always matches the order the store applied them (writes to
    //    different keys commute, so their relative order is free)
    //  - EXEC locks every key its transaction touches up front, so no
    //    other client — reader or writer — can observe it half-done
    //
    // Critical sections are a few microseconds of in-memory work and
    // never await, so these are spin locks: an uncontended acquire is
    // one interlocked operation on a cache line that, with 4096
    // padded stripes, is almost never shared with another key.
    public sealed class KeyLocks
    {
        public const int StripeCount = 4096;

        private const int Mask = StripeCount - 1;

        private const int WriterHeld = unchecked((int)0x80000000);
        private const int WriterWaiting = 0x40000000;
        private const int ReaderMask = 0x3FFFFFFF;

        [StructLayout(LayoutKind.Explicit, Size = 64)]
        private struct Stripe
        {
            [FieldOffset(0)]
            public int State;
        }

        private readonly Stripe[] _stripes = new Stripe[StripeCount];

        // Stripes hash the key's UTF-8 bytes (the hot path has bytes, not
        // a string). HashCode is seeded randomly per process, so nobody
        // can aim a flood of keys at one stripe.
        public static int StripeOf(ReadOnlySpan<byte> utf8Key)
        {
            // Keys are stored as decoded strings, and invalid UTF-8
            // doesn't survive the round trip byte-for-byte; hash such
            // a key the way StripeOf(string) would see it.
            if (!System.Text.Unicode.Utf8.IsValid(utf8Key))
                return StripeOf(Encoding.UTF8.GetString(utf8Key));

            return HashBytes(utf8Key);
        }

        public static int StripeOf(string key)
        {
            if (key.Length <= 256)
            {
                Span<byte> bytes = stackalloc byte[key.Length * 3];
                return HashBytes(bytes[..Encoding.UTF8.GetBytes(key, bytes)]);
            }

            return HashBytes(Encoding.UTF8.GetBytes(key));
        }

        private static int HashBytes(ReadOnlySpan<byte> bytes)
        {
            var hash = new HashCode();
            hash.AddBytes(bytes);
            return hash.ToHashCode() & Mask;
        }

        public void EnterRead(int stripe)
        {
            ref var state = ref _stripes[stripe].State;
            var current = Volatile.Read(ref state);

            if ((current & (WriterHeld | WriterWaiting)) == 0 &&
                Interlocked.CompareExchange(ref state, current + 1, current) == current)
            {
                return;
            }

            EnterReadSlow(ref state);
        }

        public void ExitRead(int stripe)
            => Interlocked.Decrement(ref _stripes[stripe].State);

        public void EnterWrite(int stripe)
        {
            ref var state = ref _stripes[stripe].State;

            if (Interlocked.CompareExchange(ref state, WriterHeld, 0) == 0)
                return;

            EnterWriteSlow(ref state);
        }

        public void ExitWrite(int stripe)
            => Interlocked.And(ref _stripes[stripe].State, ~WriterHeld);

        public void Enter(int stripe, bool write)
        {
            if (write)
                EnterWrite(stripe);
            else
                EnterRead(stripe);
        }

        public void Exit(int stripe, bool write)
        {
            if (write)
                ExitWrite(stripe);
            else
                ExitRead(stripe);
        }

        // `stripes` must be sorted ascending and free of duplicates —
        // one global acquisition order is what rules out deadlock
        // between two multi-key commands.
        public void EnterMany(ReadOnlySpan<int> stripes, bool write)
        {
            foreach (var stripe in stripes)
                Enter(stripe, write);
        }

        public void ExitMany(ReadOnlySpan<int> stripes, bool write)
        {
            for (var i = stripes.Length - 1; i >= 0; i--)
                Exit(stripes[i], write);
        }

        // Exclusive hold on the entire keyspace — FLUSHALL, and the
        // instant a snapshot pins its starting offset.
        public void EnterAll()
        {
            for (var i = 0; i < StripeCount; i++)
                EnterWrite(i);
        }

        public void ExitAll()
        {
            for (var i = StripeCount - 1; i >= 0; i--)
                ExitWrite(i);
        }

        // Waits until every command that was already holding a stripe
        // has finished. Used after raising a flag (write pause, end of
        // a snapshot) that new commands check once they hold their
        // stripe: afterwards, every command either completed before
        // the flag or is guaranteed to see it.
        public void Drain()
        {
            for (var i = 0; i < StripeCount; i++)
            {
                EnterWrite(i);
                ExitWrite(i);
            }
        }

        // Sorts and de-duplicates in place; returns the unique count.
        public static int Normalize(Span<int> stripes)
        {
            if (stripes.Length <= 1)
                return stripes.Length;

            stripes.Sort();

            var unique = 1;

            for (var i = 1; i < stripes.Length; i++)
            {
                if (stripes[i] != stripes[unique - 1])
                    stripes[unique++] = stripes[i];
            }

            return unique;
        }

        private static void EnterReadSlow(ref int state)
        {
            var spinner = new SpinWait();

            while (true)
            {
                var current = Volatile.Read(ref state);

                // Back off while a writer holds or is waiting for the
                // stripe, so a steady stream of readers on a hot key
                // can't starve writers forever.
                if ((current & (WriterHeld | WriterWaiting)) == 0 &&
                    (current & ReaderMask) != ReaderMask &&
                    Interlocked.CompareExchange(ref state, current + 1, current) == current)
                {
                    return;
                }

                spinner.SpinOnce(sleep1Threshold: -1);
            }
        }

        private static void EnterWriteSlow(ref int state)
        {
            var spinner = new SpinWait();

            while (true)
            {
                var current = Volatile.Read(ref state);

                if ((current & ~WriterWaiting) == 0)
                {
                    // Free (perhaps with our own waiting hint set):
                    // take it, clearing the hint.
                    if (Interlocked.CompareExchange(ref state, WriterHeld, current) == current)
                        return;
                }
                else if ((current & WriterWaiting) == 0)
                {
                    Interlocked.CompareExchange(
                        ref state,
                        current | WriterWaiting,
                        current);
                }

                spinner.SpinOnce(sleep1Threshold: -1);
            }
        }
    }
}
