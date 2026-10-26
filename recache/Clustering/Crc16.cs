using System;

namespace recache.Clustering
{
    // CRC16/XMODEM (poly 0x1021, init 0x0000, no reflection, no
    // final xor) — the same algorithm real Redis's crc16.c table is
    // generated from, so ComputeSlot below matches real Redis slot
    // numbers for any given key. Table-driven: one lookup per byte
    // instead of eight shift/xor steps, since every key-bearing
    // command in cluster mode hashes its key.
    public static class Crc16
    {
        private static readonly ushort[] Table = BuildTable();

        public static ushort Compute(ReadOnlySpan<byte> data)
        {
            ushort crc = 0;

            foreach (var b in data)
                crc = (ushort)((crc << 8) ^ Table[((crc >> 8) ^ b) & 0xFF]);

            return crc;
        }

        private static ushort[] BuildTable()
        {
            var table = new ushort[256];

            for (var i = 0; i < 256; i++)
            {
                var crc = (ushort)(i << 8);

                for (var bit = 0; bit < 8; bit++)
                {
                    crc = (crc & 0x8000) != 0
                        ? (ushort)((crc << 1) ^ 0x1021)
                        : (ushort)(crc << 1);
                }

                table[i] = crc;
            }

            return table;
        }
    }
}
