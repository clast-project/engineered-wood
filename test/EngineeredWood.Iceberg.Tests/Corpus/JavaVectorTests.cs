// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using EngineeredWood.Compression;

namespace EngineeredWood.Iceberg.Tests.Corpus;

/// <summary>
/// Iceberg Java's deletion-vector and Puffin test vectors, vendored in <c>TestData/java-vectors</c> (see its
/// README), decoded here independently of Java and of any EngineeredWood reader: the vectors' own check, and
/// the reference the Phase 1 DV and Puffin readers (#954 item 10) will be held to.
/// </summary>
public class JavaVectorTests
{
    private static readonly string Root = Path.Combine(AppContext.BaseDirectory, "TestData", "java-vectors");

    private static readonly JsonElement Vectors =
        JsonDocument.Parse(File.ReadAllText(Path.Combine(Root, "vectors.json"))).RootElement.GetProperty("files");

    public static TheoryData<string> Files()
    {
        var data = new TheoryData<string>();
        foreach (var f in Vectors.EnumerateObject())
            data.Add(f.Name);
        return data;
    }

    private static byte[] Read(string name) => File.ReadAllBytes(Path.Combine(Root, name.Replace('/', Path.DirectorySeparatorChar)));

    [Theory]
    [MemberData(nameof(Files))]
    public void VendoredUnchanged(string name)
    {
        using var sha = SHA256.Create();
        string hash = string.Concat(sha.ComputeHash(Read(name)).Select(b => b.ToString("x2", System.Globalization.CultureInfo.InvariantCulture)));
        Assert.Equal(Vectors.GetProperty(name).GetProperty("sha256").GetString(), hash);
    }

    /// <summary>A deletion-vector-v1 blob: length, magic, portable 64-bit roaring, CRC-32 of magic and bitmap.</summary>
    [Theory]
    [MemberData(nameof(Files))]
    public void DeletionVectorBlobs(string name)
    {
        var spec = Vectors.GetProperty(name);
        if (spec.GetProperty("kind").GetString() != "dv")
            return;
        byte[] blob = Read(name);

        int length = BinaryPrimitives.ReadInt32BigEndian(blob);
        Assert.Equal(blob.Length - 8, length); // the length counts the magic and the bitmap
        Assert.Equal(new byte[] { 0xD1, 0xD3, 0x39, 0x64 }, blob.AsSpan(4, 4).ToArray());
        Assert.Equal(BinaryPrimitives.ReadUInt32BigEndian(blob.AsSpan(blob.Length - 4)), Crc32(blob.AsSpan(4, length)));

        var positions = PortableRoaring64.Read(blob.AsSpan(8, length - 4), out int consumed);
        Assert.Equal(length - 4, consumed);
        Assert.Equal(ExpectedPositions(spec.GetProperty("positions")), positions);
    }

    /// <summary>
    /// Upstream's <c>64maphighvals.bin</c> is not CRoaring's file: every byte from 0x80 up became the UTF-8
    /// replacement character. Java's refusal of it rests on that, so it is pinned here as what it is.
    /// </summary>
    [Fact]
    public void HighValsExampleIsMangledUpstream()
    {
        byte[] bytes = Read("deletes/64maphighvals.bin");
        for (int i = 0; i < bytes.Length; i++)
        {
            if (bytes[i] < 0x80)
                continue;
            Assert.Equal(new byte[] { 0xEF, 0xBF, 0xBD }, bytes.AsSpan(i, 3).ToArray());
            i += 2;
        }

        Assert.Equal(11, BinaryPrimitives.ReadInt64LittleEndian(bytes));
        Assert.Equal(0xEFBDBFEFu, BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(8)));
    }

    /// <summary>CRoaring's portable 64-bit examples that are intact: each reads whole.</summary>
    [Theory]
    [MemberData(nameof(Files))]
    public void RoaringExamples(string name)
    {
        var spec = Vectors.GetProperty(name);
        if (spec.GetProperty("kind").GetString() != "roaring64")
            return;
        byte[] bytes = Read(name);
        var positions = PortableRoaring64.Read(bytes, out int consumed);
        Assert.Equal(bytes.Length, consumed);

        // Java's RoaringPositionBitmap takes keys 0 .. int.MaxValue - 1 (positions below 2^63 - 2^32).
        bool javaReads = spec.GetProperty("java").GetString() == "reads";
        Assert.Equal(javaReads, positions.All(p => p >= 0 && (p >> 32) < int.MaxValue));
        if (spec.TryGetProperty("positions", out var expected))
            Assert.Equal(ExpectedPositions(expected), positions);
        if (javaReads && name != "deletes/64mapempty.bin")
            Assert.NotEmpty(positions);
    }

    /// <summary>Puffin: magic, blobs, footer (magic, JSON payload, payload size, flags, magic).</summary>
    [Theory]
    [MemberData(nameof(Files))]
    public void PuffinFiles(string name)
    {
        var spec = Vectors.GetProperty(name);
        if (spec.GetProperty("kind").GetString() != "puffin")
            return;
        byte[] file = Read(name);
        byte[] magic = "PFA1"u8.ToArray();

        Assert.Equal(magic, file.AsSpan(0, 4).ToArray());
        Assert.Equal(magic, file.AsSpan(file.Length - 4).ToArray());
        int flags = BinaryPrimitives.ReadInt32LittleEndian(file.AsSpan(file.Length - 8));
        Assert.Equal(0, flags & 1); // footer payload not LZ4-compressed in these files
        int payloadSize = BinaryPrimitives.ReadInt32LittleEndian(file.AsSpan(file.Length - 12));
        int footerSize = payloadSize + 16; // magic + payload + size + flags + magic
        if (spec.TryGetProperty("footer_size", out var expectedFooter))
            Assert.Equal(expectedFooter.GetInt32(), footerSize);
        int footerStart = file.Length - footerSize;
        Assert.Equal(magic, file.AsSpan(footerStart, 4).ToArray());

        using var footer = JsonDocument.Parse(file.AsMemory(footerStart + 4, payloadSize));
        var properties = footer.RootElement.TryGetProperty("properties", out var p)
            ? p.EnumerateObject().ToDictionary(x => x.Name, x => x.Value.GetString())
            : new Dictionary<string, string?>();
        Assert.Equal(spec.GetProperty("properties").EnumerateObject().ToDictionary(x => x.Name, x => x.Value.GetString()), properties);

        var blobs = footer.RootElement.GetProperty("blobs").EnumerateArray().ToList();
        var expectedBlobs = spec.GetProperty("blobs").EnumerateArray().ToList();
        Assert.Equal(expectedBlobs.Count, blobs.Count);
        long nextOffset = 4;
        foreach (var (blob, expected) in blobs.Zip(expectedBlobs, (a, b) => (a, b)))
        {
            Assert.Equal(expected.GetProperty("type").GetString(), blob.GetProperty("type").GetString());
            Assert.Equal(expected.GetProperty("fields").EnumerateArray().Select(f => f.GetInt32()),
                blob.GetProperty("fields").EnumerateArray().Select(f => f.GetInt32()));
            long offset = blob.GetProperty("offset").GetInt64();
            int length = blob.GetProperty("length").GetInt32();
            Assert.Equal(nextOffset, offset); // blobs sit back to back after the magic
            nextOffset = offset + length;

            string? codec = blob.TryGetProperty("compression-codec", out var c) ? c.GetString() : null;
            Assert.Equal(spec.GetProperty("codec").ValueKind == JsonValueKind.Null ? null : spec.GetProperty("codec").GetString(), codec);
            var stored = file.AsSpan((int)offset, length);
            byte[] data;
            if (codec == "zstd")
            {
                data = new byte[Decompressor.GetDecompressedLength(CompressionCodec.Zstd, stored)];
                Assert.Equal(data.Length, Decompressor.Decompress(CompressionCodec.Zstd, stored, data));
            }
            else
            {
                data = stored.ToArray();
            }

            Assert.Equal(expected.GetProperty("data").GetString(), Encoding.UTF8.GetString(data));
        }

        Assert.Equal(footerStart, nextOffset);
    }

    private static List<long> ExpectedPositions(JsonElement spec)
    {
        var positions = new List<long>();
        foreach (var p in spec.EnumerateArray())
        {
            if (p.ValueKind == JsonValueKind.Number)
            {
                positions.Add(p.GetInt64());
                continue;
            }

            // [bitmap, container, value] or [bitmap, container, from, to), to exclusive
            var parts = p.EnumerateArray().Select(x => x.GetInt64()).ToArray();
            long Position(long value) => (parts[0] << 32) | (parts[1] << 16) | value;
            long end = parts.Length == 4 ? parts[3] : parts[2] + 1;
            for (long v = parts[2]; v < end; v++)
                positions.Add(Position(v));
        }

        return positions.OrderBy(x => x).ToList();
    }

    private static uint Crc32(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFF;
        foreach (byte b in data)
        {
            crc ^= b;
            for (int k = 0; k < 8; k++)
                crc = (crc & 1) != 0 ? (crc >> 1) ^ 0xEDB88320 : crc >> 1;
        }

        return ~crc;
    }

    /// <summary>
    /// The portable 64-bit roaring format (RoaringFormatSpec): a count of 32-bit bitmaps, then each as its
    /// 32-bit key and a portable 32-bit roaring bitmap. Test-only; just enough to check the vectors.
    /// </summary>
    private static class PortableRoaring64
    {
        public static List<long> Read(ReadOnlySpan<byte> data, out int consumed)
        {
            var positions = new List<long>();
            int pos = 0;
            long count = BinaryPrimitives.ReadInt64LittleEndian(data);
            pos += 8;
            for (long b = 0; b < count; b++)
            {
                long high = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(pos));
                pos += 4;
                foreach (int low in Read32(data, ref pos))
                    positions.Add((high << 32) | (uint)low);
            }

            consumed = pos;
            return positions;
        }

        private static List<int> Read32(ReadOnlySpan<byte> data, ref int pos)
        {
            const int SerialCookieNoRun = 12346, SerialCookie = 12347, NoOffsetThreshold = 4;
            uint cookie = BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(pos));
            pos += 4;
            int size;
            byte[]? runFlags = null;
            if ((cookie & 0xFFFF) == SerialCookie)
            {
                size = (int)(cookie >> 16) + 1;
                runFlags = data.Slice(pos, (size + 7) / 8).ToArray();
                pos += (size + 7) / 8;
            }
            else
            {
                Assert.Equal((uint)SerialCookieNoRun, cookie);
                size = (int)BinaryPrimitives.ReadUInt32LittleEndian(data.Slice(pos));
                pos += 4;
            }

            var keys = new int[size];
            var cardinalities = new int[size];
            for (int i = 0; i < size; i++)
            {
                keys[i] = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos));
                cardinalities[i] = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos + 2)) + 1;
                pos += 4;
            }

            if (runFlags is null || size >= NoOffsetThreshold)
                pos += 4 * size; // container offsets

            var values = new List<int>();
            for (int i = 0; i < size; i++)
            {
                int high = keys[i] << 16;
                bool isRun = runFlags is not null && (runFlags[i / 8] & (1 << (i % 8))) != 0;
                if (isRun)
                {
                    int runs = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos));
                    pos += 2;
                    for (int r = 0; r < runs; r++)
                    {
                        int start = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos));
                        int len = BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos + 2));
                        pos += 4;
                        for (int v = start; v <= start + len; v++)
                            values.Add(high | v);
                    }
                }
                else if (cardinalities[i] > 4096)
                {
                    for (int w = 0; w < 1024; w++)
                    {
                        ulong word = BinaryPrimitives.ReadUInt64LittleEndian(data.Slice(pos + (w * 8)));
                        for (int bit = 0; bit < 64; bit++)
                            if ((word & (1UL << bit)) != 0)
                                values.Add(high | ((w * 64) + bit));
                    }

                    pos += 8192;
                }
                else
                {
                    for (int k = 0; k < cardinalities[i]; k++)
                        values.Add(high | BinaryPrimitives.ReadUInt16LittleEndian(data.Slice(pos + (2 * k))));
                    pos += 2 * cardinalities[i];
                }
            }

            return values;
        }
    }
}
