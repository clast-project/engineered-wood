// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Round-trip tests for <see cref="GuidArray"/> through the Parquet writer and reader.
/// Verifies that <c>arrow.uuid</c> extension columns are encoded as Parquet
/// <c>UUID</c>-annotated FLBA(16), and that the reader produces <see cref="GuidArray"/>
/// when the caller registers the extension via <see cref="ParquetReadOptions.ExtensionRegistry"/>.
/// </summary>
public class GuidArrayRoundTripTests : IDisposable
{
    private readonly string _tempDir;

    public GuidArrayRoundTripTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-guid-test-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
        GC.SuppressFinalize(this);
    }

    private static ExtensionTypeRegistry GuidRegistry()
    {
        var registry = new ExtensionTypeRegistry();
        registry.Register(GuidExtensionDefinition.Instance);
        return registry;
    }

    private static RecordBatch MakeGuidBatch(Guid[] values)
    {
        var builder = new GuidArray.Builder();
        foreach (var g in values) builder.Append(g);
        var arr = builder.Build(allocator: null);

        // GuidType is the storage's IArrowType; build a Field with it.
        var field = new Field("g", arr.Data.DataType, nullable: false);
        var schema = new Apache.Arrow.Schema(new[] { field }, metadata: null);
        return new RecordBatch(schema, new IArrowArray[] { arr }, values.Length);
    }

    [Fact]
    public async Task GuidColumn_EmitsUuidLogicalTypeAnnotation()
    {
        string path = Path.Combine(_tempDir, "guid_annot.parquet");
        var batch = MakeGuidBatch([Guid.NewGuid(), Guid.NewGuid(), Guid.Empty]);

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed }))
        {
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var schema = await reader.GetSchemaAsync();

        // The leaf column should be annotated as UUID with FLBA(16) physical layout.
        var leaf = schema.Columns[0];
        Assert.IsType<LogicalType.UuidType>(leaf.SchemaElement.LogicalType);
        Assert.Equal(PhysicalType.FixedLenByteArray, leaf.PhysicalType);
        Assert.Equal(16, leaf.TypeLength);
    }

    [Fact]
    public async Task ReadWithoutRegistry_ProducesFixedSizeBinary()
    {
        string path = Path.Combine(_tempDir, "guid_no_reg.parquet");
        var values = new[] { Guid.NewGuid(), Guid.NewGuid(), Guid.Empty };
        var batch = MakeGuidBatch(values);

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed }))
        {
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        // No registry → reader produces FixedSizeBinaryArray(16) (historical behaviour).
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var read = await reader.ReadRowGroupAsync(0);

        var col = read.Column(0);
        var fsb = Assert.IsType<FixedSizeBinaryArray>(col);
        Assert.Equal(16, ((FixedSizeBinaryType)fsb.Data.DataType).ByteWidth);
        Assert.Equal(values.Length, fsb.Length);

        // Each row's bytes should round-trip through GuidArray.RFC4122ToGuid.
        for (int i = 0; i < values.Length; i++)
        {
            var bytes = fsb.GetBytes(i);
            Guid decoded = GuidArray.RFC4122ToGuid(bytes);
            Assert.Equal(values[i], decoded);
        }
    }

    [Fact]
    public async Task ReadWithRegistry_ProducesGuidArray()
    {
        string path = Path.Combine(_tempDir, "guid_with_reg.parquet");
        var values = new[]
        {
            Guid.Parse("12345678-1234-5678-1234-567812345678"),
            Guid.NewGuid(),
            Guid.Empty,
            Guid.NewGuid(),
        };
        var batch = MakeGuidBatch(values);

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed }))
        {
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        // With registry → reader produces GuidArray.
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false,
            new ParquetReadOptions { ExtensionRegistry = GuidRegistry() });
        var read = await reader.ReadRowGroupAsync(0);

        var col = read.Column(0);
        var ga = Assert.IsType<GuidArray>(col);
        Assert.Equal(values.Length, ga.Length);

        for (int i = 0; i < values.Length; i++)
        {
            Assert.Equal(values[i], ga.GetGuid(i));
        }
    }

    [Fact]
    public async Task ToggleRegistry_SameFile_GivesDifferentArrayTypes()
    {
        // One file, two readers: same bytes, different output types depending
        // on whether the caller opted into the GuidType extension.
        string path = Path.Combine(_tempDir, "guid_toggle.parquet");
        var values = new[] { Guid.NewGuid(), Guid.NewGuid() };
        var batch = MakeGuidBatch(values);

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new BufferedParquetWriter(file, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed }))
        {
            await writer.AppendAsync(batch);
            await writer.CloseAsync();
        }

        // Open twice with different options.
        await using (var f1 = new LocalRandomAccessFile(path))
        await using (var r1 = new ParquetFileReader(f1, ownsFile: false))
        {
            var b1 = await r1.ReadRowGroupAsync(0);
            Assert.IsType<FixedSizeBinaryArray>(b1.Column(0));
        }

        await using (var f2 = new LocalRandomAccessFile(path))
        await using (var r2 = new ParquetFileReader(f2, ownsFile: false,
            new ParquetReadOptions { ExtensionRegistry = GuidRegistry() }))
        {
            var b2 = await r2.ReadRowGroupAsync(0);
            Assert.IsType<GuidArray>(b2.Column(0));
        }
    }
}
