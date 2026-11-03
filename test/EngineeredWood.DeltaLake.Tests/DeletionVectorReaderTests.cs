// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.DeletionVectors;
using EngineeredWood.IO;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Tests;

/// <summary>
/// Spark packs the deletion vectors of many data files into ONE <c>.bin</c>, each at its own offset. A reader
/// that fetches the whole file per descriptor reads that file once per data file, so the reader must request
/// only <c>[offset, offset + 4 + sizeInBytes + 4)</c>: the size field, the bitmap and its CRC-32 (#614).
/// </summary>
public class DeletionVectorReaderTests : IDisposable
{
    private const string PackedPath = "packed_dvs.bin";

    private readonly string _tempDir;

    public DeletionVectorReaderTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"ew_dvreader_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly long[][] Vectors =
    [
        [0, 2, 5],
        [7],
        Enumerable.Range(0, 3000).Select(i => 3L * i).ToArray(), // large enough to need a bitmap container
        [1L << 33, (1L << 33) + 1],                              // a second 32-bit sub-bitmap
    ];

    /// <summary>
    /// Writes <see cref="Vectors"/> into one file in the spec layout: a version byte, then per DV
    /// <c>&lt;size BE&gt;&lt;bitmap&gt;&lt;CRC-32 BE&gt;</c>. Returns each DV's descriptor.
    /// </summary>
    private async Task<DeletionVector[]> WritePackedFileAsync(ITableFileSystem fs)
    {
        using var file = new MemoryStream();
        file.WriteByte(1); // format version
        var descriptors = new DeletionVector[Vectors.Length];
        for (int i = 0; i < Vectors.Length; i++)
        {
            byte[] bitmap = RoaringBitmapWriter.Serialize(Vectors[i]);
            descriptors[i] = new DeletionVector
            {
                StorageType = "p",
                PathOrInlineDv = PackedPath,
                Offset = checked((int)file.Position),
                SizeInBytes = bitmap.Length,
                Cardinality = Vectors[i].Length,
            };
            WriteFramed(file, bitmap);
        }

        await fs.WriteAllBytesAsync(PackedPath, file.ToArray());
        return descriptors;
    }

    private static void WriteFramed(Stream file, byte[] bitmap)
    {
        var word = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(word, bitmap.Length);
        file.Write(word, 0, 4);
        file.Write(bitmap, 0, bitmap.Length);
        BinaryPrimitives.WriteUInt32BigEndian(word, Crc32.HashToUInt32(bitmap));
        file.Write(word, 0, 4);
    }

    [Fact]
    public async Task PackedFile_EachDescriptorReadsItsOwnVector_FromOnlyItsOwnRange()
    {
        var fs = new RecordingFileSystem(new LocalTableFileSystem(_tempDir));
        DeletionVector[] descriptors = await WritePackedFileAsync(fs);
        var reader = new DeletionVectorReader(fs);

        for (int i = 0; i < descriptors.Length; i++)
        {
            fs.Ranges.Clear();
            DeletionVectorBitmap bitmap = await reader.ReadAsync(descriptors[i]);

            Assert.Equal(Vectors[i], bitmap.ToArray());
            var expected = new FileRange(descriptors[i].Offset!.Value, 4 + descriptors[i].SizeInBytes + 4);
            Assert.Equal([(PackedPath, expected)], fs.Ranges);
        }

        Assert.Equal(0, fs.ReadAllBytesCalls);
        Assert.Equal(0, fs.LengthCalls); // the success path asks for no length
    }

    /// <summary>The DV this library writes itself ("u" storage, one per file) reads back the same way.</summary>
    [Fact]
    public async Task WriterOutput_ReadsBackThroughOneRangedRead()
    {
        var fs = new RecordingFileSystem(new LocalTableFileSystem(_tempDir));
        long[] positions = Enumerable.Range(0, 5000).Select(i => 2L * i).ToArray(); // too large to inline

        DeletionVector dv = await new DeletionVectorWriter(fs).CreateAsync(positions);
        Assert.Equal("u", dv.StorageType);

        DeletionVectorBitmap bitmap = await new DeletionVectorReader(fs).ReadAsync(dv);

        Assert.Equal(positions, bitmap.ToArray());
        Assert.Single(fs.Ranges);
        Assert.Equal(0, fs.ReadAllBytesCalls);
    }

    [Fact]
    public async Task SizeFieldDisagreeingWithDescriptor_IsRefused()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        DeletionVector[] descriptors = await WritePackedFileAsync(fs);
        // Point the first descriptor one byte short: the stored size field no longer matches.
        var dv = new DeletionVector
        {
            StorageType = "p",
            PathOrInlineDv = PackedPath,
            Offset = descriptors[0].Offset,
            SizeInBytes = descriptors[0].SizeInBytes - 1,
            Cardinality = descriptors[0].Cardinality,
        };

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await new DeletionVectorReader(fs).ReadAsync(dv));

        Assert.Equal(DeltaErrorCodes.InvalidDeletionVector, ex.ErrorCode);
        Assert.Contains("does not match", ex.Message);
    }

    /// <summary>
    /// A bare bitmap at the offset (no size field, no checksum) is not a layout any writer produces, and
    /// Spark refuses it; so does this reader, rather than guessing.
    /// </summary>
    [Fact]
    public async Task BareBitmapWithoutFraming_IsRefused()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        byte[] bitmap = RoaringBitmapWriter.Serialize([0L, 2L]);
        await fs.WriteAllBytesAsync(PackedPath, bitmap.Concat(new byte[8]).ToArray());
        var dv = new DeletionVector
        {
            StorageType = "p",
            PathOrInlineDv = PackedPath,
            Offset = 0,
            SizeInBytes = bitmap.Length,
            Cardinality = 2,
        };

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await new DeletionVectorReader(fs).ReadAsync(dv));

        Assert.Equal(DeltaErrorCodes.InvalidDeletionVector, ex.ErrorCode);
    }

    [Fact]
    public async Task CorruptBitmapByte_FailsTheChecksum()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        DeletionVector[] descriptors = await WritePackedFileAsync(fs);
        byte[] bytes = await fs.ReadAllBytesAsync(PackedPath);
        int bitmapStart = descriptors[1].Offset!.Value + 4;
        bytes[bitmapStart + descriptors[1].SizeInBytes - 1] ^= 0x01;
        await fs.WriteAllBytesAsync(PackedPath, bytes);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await new DeletionVectorReader(fs).ReadAsync(descriptors[1]));

        Assert.Equal(DeltaErrorCodes.InvalidDeletionVector, ex.ErrorCode);
        Assert.Contains("checksum", ex.Message);

        // Its neighbours are untouched and still read.
        Assert.Equal(Vectors[0], (await new DeletionVectorReader(fs).ReadAsync(descriptors[0])).ToArray());
        Assert.Equal(Vectors[2], (await new DeletionVectorReader(fs).ReadAsync(descriptors[2])).ToArray());
    }

    /// <summary>
    /// A range that runs past the end, and one that STARTS at or past it. Object stores answer the second with
    /// HTTP 416 rather than a short read; the backends report both as <see cref="IOException"/> (held for every
    /// backend by the storage conformance suite), and the reader reports both as an invalid vector.
    /// </summary>
    [Theory]
    [InlineData("straddles the end")]
    [InlineData("starts at the end")]
    [InlineData("starts beyond the end")]
    public async Task RangePastTheEndOfTheFile_IsAnInvalidVector(string shape)
    {
        var fs = new RecordingFileSystem(new LocalTableFileSystem(_tempDir));
        DeletionVector[] descriptors = await WritePackedFileAsync(fs);
        int fileLength = checked((int)new FileInfo(Path.Combine(_tempDir, PackedPath)).Length);
        DeletionVector last = descriptors[^1];
        var dv = new DeletionVector
        {
            StorageType = "p",
            PathOrInlineDv = PackedPath,
            Offset = shape switch
            {
                "straddles the end" => last.Offset,
                "starts at the end" => fileLength,
                _ => fileLength + 100,
            },
            SizeInBytes = shape == "straddles the end" ? last.SizeInBytes + 100 : last.SizeInBytes,
            Cardinality = last.Cardinality,
        };

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await new DeletionVectorReader(fs).ReadAsync(dv));

        Assert.Equal(DeltaErrorCodes.InvalidDeletionVector, ex.ErrorCode);
        Assert.Contains($"exceeds file length {fileLength}", ex.Message);
        Assert.IsAssignableFrom<IOException>(ex.InnerException);
    }

    /// <summary>
    /// An I/O failure INSIDE the file is a storage problem, not a malformed vector: it surfaces as itself.
    /// </summary>
    [Fact]
    public async Task IOFailureWithinTheFile_IsNotRelabelled()
    {
        var inner = new LocalTableFileSystem(_tempDir);
        DeletionVector[] descriptors = await WritePackedFileAsync(inner);
        var fs = new RecordingFileSystem(inner) { FailReads = true };

        var ex = await Assert.ThrowsAsync<IOException>(
            async () => await new DeletionVectorReader(fs).ReadAsync(descriptors[0]));

        Assert.Contains("injected", ex.Message);
        Assert.Equal(1, fs.LengthCalls);
    }

    [Theory]
    [InlineData(-1, 10)]
    [InlineData(1, -1)]
    [InlineData(1, int.MaxValue - 7)] // + 8 framing bytes overflows one buffer
    [InlineData(1, int.MaxValue)]
    public async Task OutOfRangeOffsetOrSize_IsRefused(int offset, int size)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await WritePackedFileAsync(fs);
        var dv = new DeletionVector
        {
            StorageType = "p",
            PathOrInlineDv = PackedPath,
            Offset = offset,
            SizeInBytes = size,
            Cardinality = 1,
        };

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await new DeletionVectorReader(fs).ReadAsync(dv));

        Assert.Equal(DeltaErrorCodes.InvalidDeletionVector, ex.ErrorCode);
    }

    /// <summary>Records every ranged read and whole-file read made through it.</summary>
    private sealed class RecordingFileSystem(ITableFileSystem inner) : ITableFileSystem
    {
        public List<(string Path, FileRange Range)> Ranges { get; } = [];
        public int ReadAllBytesCalls { get; private set; }
        public int LengthCalls { get; set; }
        public bool FailReads { get; init; }

        public PathNameConstraints PathConstraints => inner.PathConstraints;

        public IAsyncEnumerable<TableFileInfo> ListAsync(
            string prefix, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, cancellationToken);

        public async ValueTask<IRandomAccessFile> OpenReadAsync(
            string path, CancellationToken cancellationToken = default) =>
            new RecordingFile(this, path, await inner.OpenReadAsync(path, cancellationToken));

        public ValueTask<ISequentialFile> CreateAsync(
            string path, bool overwrite = false, CancellationToken cancellationToken = default) =>
            inner.CreateAsync(path, overwrite, cancellationToken);

        public ValueTask DeleteAsync(string path, CancellationToken cancellationToken = default) =>
            inner.DeleteAsync(path, cancellationToken);

        public ValueTask<bool> ExistsAsync(string path, CancellationToken cancellationToken = default) =>
            inner.ExistsAsync(path, cancellationToken);

        public ValueTask<byte[]> ReadAllBytesAsync(
            string path, CancellationToken cancellationToken = default)
        {
            ReadAllBytesCalls++;
            return inner.ReadAllBytesAsync(path, cancellationToken);
        }

        public ValueTask<bool> TryWriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
            inner.TryWriteAllBytesAsync(path, data, cancellationToken);

        public ValueTask WriteAllBytesAsync(
            string path, ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default) =>
            inner.WriteAllBytesAsync(path, data, cancellationToken);

        private sealed class RecordingFile(RecordingFileSystem owner, string path, IRandomAccessFile file)
            : IRandomAccessFile
        {
            public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
            {
                owner.LengthCalls++;
                return file.GetLengthAsync(cancellationToken);
            }

            public ValueTask<IMemoryOwner<byte>> ReadAsync(
                FileRange range, CancellationToken cancellationToken = default)
            {
                owner.Ranges.Add((path, range));
                if (owner.FailReads)
                    throw new IOException("read failed (injected)");
                return file.ReadAsync(range, cancellationToken);
            }

            public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
                IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
            {
                foreach (FileRange range in ranges)
                    owner.Ranges.Add((path, range));
                return file.ReadRangesAsync(ranges, cancellationToken);
            }

            public ValueTask DisposeAsync() => file.DisposeAsync();

            public void Dispose() => file.Dispose();
        }
    }
}
