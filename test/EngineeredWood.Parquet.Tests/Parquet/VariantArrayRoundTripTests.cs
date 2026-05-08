// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Round-trip tests for <see cref="VariantArray"/> through the Parquet writer/reader.
/// Verifies that a VariantArray column emits a Parquet group annotated with the
/// VARIANT logical type, and that the reader produces VariantArray when the
/// caller registers the extension via
/// <see cref="ParquetReadOptions.ExtensionRegistry"/>.
/// </summary>
public class VariantArrayRoundTripTests : IDisposable
{
    private readonly string _tempDir;

    public VariantArrayRoundTripTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-variant-test-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
        GC.SuppressFinalize(this);
    }

    private static ExtensionTypeRegistry VariantRegistry()
    {
        var registry = new ExtensionTypeRegistry();
        registry.Register(VariantExtensionDefinition.Instance);
        return registry;
    }

    /// <summary>
    /// Builds a tiny RecordBatch with a single nullable VARIANT column.
    /// Each row uses canonical empty metadata (version=1, no dict entries) and
    /// the value bytes provided by the caller.
    /// </summary>
    private static (RecordBatch batch, byte[][] metas, byte[][] values) MakeVariantBatch(byte[][] valueBytes)
    {
        // Canonical empty metadata: version byte 0x01, dict_size=0 (varint), offset=0.
        byte[] meta = [0x01, 0x00, 0x00];
        var metas = new byte[valueBytes.Length][];

        var builder = new VariantArray.Builder();
        for (int i = 0; i < valueBytes.Length; i++)
        {
            metas[i] = meta;
            builder.Append(meta, valueBytes[i]);
        }
        var arr = builder.Build(allocator: null);

        var field = new Field("v", arr.Data.DataType, nullable: true);
        var schema = new Apache.Arrow.Schema(new[] { field }, metadata: null);
        return (new RecordBatch(schema, new IArrowArray[] { arr }, valueBytes.Length), metas, valueBytes);
    }

    [Fact]
    public async Task VariantColumn_EmitsVariantLogicalTypeAnnotation()
    {
        string path = Path.Combine(_tempDir, "variant_annot.parquet");
        var (batch, _, _) = MakeVariantBatch(new[]
        {
            new byte[] { 0x00 }, // primitive null
            new byte[] { 0x0C }, // primitive boolean true (per Variant spec basic_type=primitive, type_id=2 -> 0b00001100)
        });

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();

        // The top-level group element (index 1, since index 0 is the synthetic
        // root) should carry the VARIANT logical type.
        var schema = meta.Schema;
        var groupElement = schema.First(s => s.Name == "v");
        Assert.IsType<LogicalType.VariantType>(groupElement.LogicalType);
        Assert.Equal(2, groupElement.NumChildren);

        // Storage children: metadata (BYTE_ARRAY) and value (BYTE_ARRAY).
        var metaChild = schema.First(s => s.Name == "metadata");
        var valueChild = schema.First(s => s.Name == "value");
        Assert.Equal(PhysicalType.ByteArray, metaChild.Type);
        Assert.Equal(PhysicalType.ByteArray, valueChild.Type);
    }

    [Fact]
    public async Task ReadWithoutRegistry_ProducesStructArray()
    {
        string path = Path.Combine(_tempDir, "variant_no_reg.parquet");
        var (batch, _, _) = MakeVariantBatch(new[]
        {
            new byte[] { 0x00 },
            new byte[] { 0x0C },
            new byte[] { 0x00 },
        });

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        // No registry → reader returns a bare StructArray with metadata+value
        // BinaryArray children.
        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false);
        var read = await reader.ReadRowGroupAsync(0);

        var col = read.Column(0);
        var sa = Assert.IsType<StructArray>(col);
        Assert.Equal(3, sa.Length);
        Assert.Equal(2, sa.Fields.Count);

        var metaCol = Assert.IsAssignableFrom<BinaryArray>(sa.Fields[0]);
        var valueCol = Assert.IsAssignableFrom<BinaryArray>(sa.Fields[1]);
        Assert.Equal(3, metaCol.Length);
        Assert.Equal(3, valueCol.Length);
    }

    [Fact]
    public async Task ReadWithRegistry_ProducesVariantArray()
    {
        string path = Path.Combine(_tempDir, "variant_with_reg.parquet");
        var (batch, _, values) = MakeVariantBatch(new[]
        {
            new byte[] { 0x00 }, // primitive null
            new byte[] { 0x0C }, // primitive boolean true
            new byte[] { 0x08 }, // primitive boolean false
            new byte[] { 0x00 },
        });

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var readFile = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(readFile, ownsFile: false,
            new ParquetReadOptions { ExtensionRegistry = VariantRegistry() });
        var read = await reader.ReadRowGroupAsync(0);

        var col = read.Column(0);
        var va = Assert.IsType<VariantArray>(col);
        Assert.Equal(values.Length, va.Length);
        Assert.False(va.IsShredded);

        // Round-trip the value bytes for each row.
        for (int i = 0; i < values.Length; i++)
        {
            var got = va.GetValueBytes(i);
            Assert.Equal(values[i], got.ToArray());
        }
    }

    [Fact]
    public async Task ToggleRegistry_SameFile_GivesDifferentArrayTypes()
    {
        string path = Path.Combine(_tempDir, "variant_toggle.parquet");
        var (batch, _, _) = MakeVariantBatch(new[]
        {
            new byte[] { 0x00 },
            new byte[] { 0x0C },
        });

        await using (var file = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(file, ownsFile: false))
        {
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        // No registry: StructArray.
        await using (var f1 = new LocalRandomAccessFile(path))
        await using (var r1 = new ParquetFileReader(f1, ownsFile: false))
        {
            var b1 = await r1.ReadRowGroupAsync(0);
            Assert.IsType<StructArray>(b1.Column(0));
        }

        // With registry: VariantArray.
        await using (var f2 = new LocalRandomAccessFile(path))
        await using (var r2 = new ParquetFileReader(f2, ownsFile: false,
            new ParquetReadOptions { ExtensionRegistry = VariantRegistry() }))
        {
            var b2 = await r2.ReadRowGroupAsync(0);
            Assert.IsType<VariantArray>(b2.Column(0));
        }
    }
}
