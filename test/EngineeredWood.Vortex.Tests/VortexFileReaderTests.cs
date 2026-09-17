// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using Apache.Arrow.Types;
using EngineeredWood.IO;
using EngineeredWood.Vortex.Tests.TestData;
using EngineeredWood.Vortex.Tests.TestHelpers;
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
    public async Task OpensAFileWhosePostscriptSegmentStartsBeforeTheTailRead()
    {
        // The reader fetches the file's last 64 KiB up front and slices the
        // footer, layout and dtype segments from it when they fit. Many small
        // batches under zone maps and a dict layout make those segments large
        // enough that one begins before that window and ends inside it.
        var schema = new Apache.Arrow.Schema(new[]
        {
            new Apache.Arrow.Field("id", Int32Type.Default, nullable: false),
            new Apache.Arrow.Field("tag", StringType.Default, nullable: false),
        }, metadata: null);
        const int batches = 1500, rowsPerBatch = 8;

        using var ms = new MemoryStream();
        using (var writer = new EngineeredWood.Vortex.Writer.VortexFileWriter(
            ms, schema, compress: true, preserveStats: true, preferDictLayout: true))
        {
            for (int b = 0; b < batches; b++)
            {
                var ids = new Apache.Arrow.Int32Array.Builder();
                var tags = new Apache.Arrow.StringArray.Builder();
                for (int r = 0; r < rowsPerBatch; r++)
                {
                    ids.Append(b * rowsPerBatch + r);
                    tags.Append((r % 3).ToString());
                }
                writer.WriteBatch(new Apache.Arrow.RecordBatch(
                    schema, new Apache.Arrow.IArrowArray[] { ids.Build(), tags.Build() }, rowsPerBatch));
            }
            writer.Close();
        }
        var bytes = ms.ToArray();

        Assert.True(PostscriptSegmentStraddlesTail(bytes),
            "the file no longer has a postscript segment straddling the tail read; grow it");

        using var stream = new ByteArrayRandomAccessFile(bytes);
        await using var reader = await VortexFileReader.OpenAsync(stream);
        Assert.Equal(batches * rowsPerBatch, reader.NumberOfRows);
        var read = Assert.IsType<Apache.Arrow.Int32Array>(await reader.ReadColumnAsync(0));
        Assert.Equal(Enumerable.Range(0, batches * rowsPerBatch), read.Values.ToArray());
    }

    private static bool PostscriptSegmentStraddlesTail(byte[] file)
    {
        long tailOffset = Math.Max(0, file.Length - 64 * 1024);
        int postscriptLen = System.Buffers.Binary.BinaryPrimitives.ReadUInt16LittleEndian(file.AsSpan(file.Length - 6));
        var postscript = EngineeredWood.Vortex.Format.Postscript.ReadRoot(
            file.AsSpan(file.Length - 8 - postscriptLen, postscriptLen));
        return Straddles(postscript.DType, tailOffset)
            || Straddles(postscript.Layout, tailOffset)
            || Straddles(postscript.Footer, tailOffset);
    }

    private static bool Straddles(EngineeredWood.Vortex.Format.PostscriptSegment segment, long tailOffset) =>
        segment.IsPresent
        && (long)segment.Offset < tailOffset
        && (long)segment.Offset + segment.Length > tailOffset;

    [Fact]
    public async Task RejectsTooSmallFile()
    {
        using var stream = new ByteArrayRandomAccessFile(new byte[8]);
        await Assert.ThrowsAsync<VortexFormatException>(async () =>
            await VortexFileReader.OpenAsync(stream));
    }

    /// <summary>
    /// Upstream's limit is <c>u16::MAX - 8</c> = 65527 bytes. A longer postscript is
    /// rejected by the length check, before anything tries to parse it.
    /// </summary>
    [Theory]
    [InlineData(65528)]
    [InlineData(ushort.MaxValue)]
    public async Task RejectsPostscriptLongerThanTheFormatAllows(int postscriptLen)
    {
        using var stream = new ByteArrayRandomAccessFile(FileWithPostscriptLength(postscriptLen));
        var ex = await Assert.ThrowsAsync<VortexFormatException>(async () =>
            await VortexFileReader.OpenAsync(stream));
        Assert.Contains("out of range", ex.Message);
    }

    [Fact]
    public async Task AcceptsTheLongestPostscriptLength()
    {
        // The postscript is all zeros, so the file is still rejected, but not for its length.
        using var stream = new ByteArrayRandomAccessFile(FileWithPostscriptLength(65527));
        var ex = await Assert.ThrowsAsync<VortexFormatException>(async () =>
            await VortexFileReader.OpenAsync(stream));
        Assert.DoesNotContain("out of range", ex.Message);
    }

    /// <summary>Leading magic, a zeroed postscript, and an EndOfFile naming its length.</summary>
    private static byte[] FileWithPostscriptLength(int postscriptLen)
    {
        var bytes = new byte[4 + postscriptLen + 8];
        "VTXF"u8.CopyTo(bytes);
        var eof = bytes.AsSpan(bytes.Length - 8);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(eof, 1);
        System.Buffers.Binary.BinaryPrimitives.WriteUInt16LittleEndian(eof.Slice(2), (ushort)postscriptLen);
        "VTXF"u8.CopyTo(eof.Slice(4));
        return bytes;
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
#if NETCOREAPP2_0_OR_GREATER
        var bytes = await File.ReadAllBytesAsync(
            TestDataPath.Resolve("struct_int_3rows.vortex"));
#else
        var bytes = File.ReadAllBytes(TestDataPath.Resolve("struct_int_3rows.vortex"));
        await Task.CompletedTask;
#endif
        bytes[0] = (byte)'X';

        using var stream = new ByteArrayRandomAccessFile(bytes);
        var ex = await Assert.ThrowsAsync<VortexFormatException>(async () =>
            await VortexFileReader.OpenAsync(stream));
        Assert.Contains("start of file", ex.Message);
    }
}
