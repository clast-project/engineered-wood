// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.Vortex.Tests.TestData;
using Xunit.Abstractions;

namespace EngineeredWood.Vortex.Tests;

public class VortexFileReaderTests
{
    private readonly ITestOutputHelper _output;

    public VortexFileReaderTests(ITestOutputHelper output) { _output = output; }

    [Fact]
    public async Task OpensStructIntFixture()
    {
        var path = TestDataPath.Resolve("struct_int_3rows.vortex");
        await using var reader = await VortexFileReader.OpenAsync(path);

        Assert.Equal(1, reader.FormatVersion);
        Assert.Equal(new FileInfo(path).Length, reader.FileLength);

        // Registries are non-empty and ids are namespaced strings.
        Assert.NotEmpty(reader.LayoutSpecs);
        Assert.NotEmpty(reader.ArraySpecs);
        Assert.NotEmpty(reader.SegmentSpecs);

        Assert.All(reader.LayoutSpecs, id => Assert.False(string.IsNullOrEmpty(id)));
        Assert.All(reader.ArraySpecs, id => Assert.False(string.IsNullOrEmpty(id)));

        Assert.False(reader.DTypeBytes.IsEmpty);
        Assert.False(reader.LayoutBytes.IsEmpty);

        // Every segment_specs entry lies inside the file.
        foreach (var seg in reader.SegmentSpecs)
        {
            Assert.InRange((long)seg.Offset, 0L, reader.FileLength);
            Assert.True((long)seg.Offset + seg.Length <= reader.FileLength,
                $"segment_specs entry {seg} extends past file");
        }

        // Schema: Struct { a: i32 not null }, exactly matching what the Rust fixture emits.
        Assert.Single(reader.Schema.FieldsList);
        var field = reader.Schema.FieldsList[0];
        Assert.Equal("a", field.Name);
        Assert.IsType<Int32Type>(field.DataType);
        Assert.False(field.IsNullable);
    }

    /// <summary>
    /// Records the registry contents the Rust impl emits for our reference
    /// fixture. Not strictly an invariant test — if these strings change in a
    /// future vortex release we'll learn about it here and can update the
    /// chunk-5 layout / encoding dispatch tables to match.
    /// </summary>
    [Fact]
    public async Task DumpsRegistryForReference()
    {
        await using var reader = await VortexFileReader.OpenAsync(
            TestDataPath.Resolve("struct_int_3rows.vortex"));

        _output.WriteLine($"FormatVersion = {reader.FormatVersion}");
        _output.WriteLine($"FileLength    = {reader.FileLength}");
        _output.WriteLine($"DTypeBytes    = {reader.DTypeBytes.Length}");
        _output.WriteLine($"LayoutBytes   = {reader.LayoutBytes.Length}");
        _output.WriteLine($"layout_specs ({reader.LayoutSpecs.Count}):");
        for (int i = 0; i < reader.LayoutSpecs.Count; i++)
            _output.WriteLine($"  [{i}] {reader.LayoutSpecs[i]}");
        _output.WriteLine($"array_specs  ({reader.ArraySpecs.Count}):");
        for (int i = 0; i < reader.ArraySpecs.Count; i++)
            _output.WriteLine($"  [{i}] {reader.ArraySpecs[i]}");
        _output.WriteLine($"segment_specs ({reader.SegmentSpecs.Count}):");
        foreach (var s in reader.SegmentSpecs)
            _output.WriteLine($"  off={s.Offset} len={s.Length} align={s.AlignmentExponent} codec={s.Codec}");
    }

    [Fact]
    public async Task RejectsTooSmallFile()
    {
        using var stream = new ByteArrayRandomAccessFile(new byte[8]);
        await Assert.ThrowsAsync<VortexFormatException>(async () =>
            await VortexFileReader.OpenAsync(stream));
    }

    [Fact]
    public async Task RejectsMissingTrailingMagic()
    {
        using var stream = new ByteArrayRandomAccessFile(new byte[16]);
        var ex = await Assert.ThrowsAsync<VortexFormatException>(async () =>
            await VortexFileReader.OpenAsync(stream));
        Assert.Contains("'VTXF'", ex.Message);
    }

    [Fact]
    public async Task RejectsMissingLeadingMagic()
    {
        var bytes = await File.ReadAllBytesAsync(
            TestDataPath.Resolve("struct_int_3rows.vortex"));
        bytes[0] = (byte)'X';

        using var stream = new ByteArrayRandomAccessFile(bytes);
        var ex = await Assert.ThrowsAsync<VortexFormatException>(async () =>
            await VortexFileReader.OpenAsync(stream));
        Assert.Contains("start of file", ex.Message);
    }

    private sealed class ByteArrayRandomAccessFile : IRandomAccessFile
    {
        private readonly byte[] _bytes;

        public ByteArrayRandomAccessFile(byte[] bytes) { _bytes = bytes; }

        public ValueTask<long> GetLengthAsync(CancellationToken cancellationToken = default)
            => new(_bytes.LongLength);

        public ValueTask<IMemoryOwner<byte>> ReadAsync(
            FileRange range, CancellationToken cancellationToken = default)
        {
            if (range.Offset < 0 || range.Offset + range.Length > _bytes.LongLength)
                throw new IOException(
                    $"Range {range.Offset}..+{range.Length} is out of bounds for {_bytes.LongLength}-byte file.");
            var copy = new byte[range.Length];
            Array.Copy(_bytes, range.Offset, copy, 0, range.Length);
            return new(new ArrayMemoryOwner(copy));
        }

        public ValueTask<IReadOnlyList<IMemoryOwner<byte>>> ReadRangesAsync(
            IReadOnlyList<FileRange> ranges, CancellationToken cancellationToken = default)
        {
            throw new NotImplementedException();
        }

        public ValueTask DisposeAsync() => default;
        public void Dispose() { }

        private sealed class ArrayMemoryOwner : IMemoryOwner<byte>
        {
            public ArrayMemoryOwner(byte[] bytes) { Memory = bytes; }
            public Memory<byte> Memory { get; }
            public void Dispose() { }
        }
    }
}
