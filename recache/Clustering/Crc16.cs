using System;

namespace recache.Clustering
{
    // CRC16/XMODEM (poly 0x1021, init 0x0000, no reflection, no
    // final xor) — the same algorithm real Redis's crc16.c table is
    // generated from, so ComputeSlot below matches real Redis slot
    // numbers for any given key.
    public static class Crc16
    {
        public static ushort Compute(ReadOnlySpan<byte> data)
        {
            ushort crc = 0;

            foreach (var b in data)
            {
                crc ^= (ushort)(b << 8);

                for (var i = 0; i < 8; i++)
                {
                    crc = (crc & 0x8000) != 0
                        ? (ushort)((crc << 1) ^ 0x1021)
                        : (ushort)(crc << 1);
                }
            }

            return crc;
        }
    }
}
