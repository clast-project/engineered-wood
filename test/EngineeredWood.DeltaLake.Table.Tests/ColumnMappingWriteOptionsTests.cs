// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using ArrowStructType = Apache.Arrow.Types.StructType;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Per-column <see cref="ParquetWriteOptions"/> name LOGICAL columns and reach the PHYSICAL leaves of a
/// column-mapped table's data files (#416). Before, every one of them silently matched nothing.
/// </summary>
public class ColumnMappingWriteOptionsTests : IDisposable
{
    private readonly string _tempDir;

    public ColumnMappingWriteOptionsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_cmwo_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static readonly Apache.Arrow.Schema IdSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch Ids(int count) => new(IdSchema,
        [new Int64Array.Builder().AppendRange(Enumerable.Range(0, count).Select(v => (long)v)).Build()], count);

    /// <summary>The metadata of every column chunk in every data file, by the file's dotted leaf path.</summary>
    private async Task<List<(string Path, ColumnMetaData Meta)>> Chunks()
    {
        var chunks = new List<(string, ColumnMetaData)>();
        foreach (string path in Directory.EnumerateFiles(_tempDir, "*.parquet", SearchOption.AllDirectories)
                     .Where(p => !p.Contains("_delta_log")))
        {
            await using var file = new LocalRandomAccessFile(path);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            foreach (var chunk in (await reader.ReadMetadataAsync()).RowGroups.SelectMany(rg => rg.Columns))
                chunks.Add((string.Join(".", chunk.MetaData!.PathInSchema!), chunk.MetaData));
        }
        return chunks;
    }

    /// <summary>The issue's repro: a Bloom filter asked for by logical name, in every mapping mode.</summary>
    [Theory]
    [InlineData(ColumnMappingMode.None)]
    [InlineData(ColumnMappingMode.Name)]
    [InlineData(ColumnMappingMode.Id)]
    public async Task BloomFilterColumns_ReachThePhysicalLeaf(ColumnMappingMode mode)
    {
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { BloomFilterColumns = new HashSet<string> { "id" } },
        };
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, options, columnMappingMode: mode);
        await table.WriteAsync([Ids(100)]);

        var (path, meta) = Assert.Single(await Chunks());
        Assert.Equal(mode == ColumnMappingMode.None, path == "id");
        Assert.NotNull(meta.BloomFilterOffset);
    }

    [Fact]
    public async Task ColumnWriteStatistics_Off_IsHonoured()
    {
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with
            {
                ColumnWriteStatistics = new Dictionary<string, bool> { ["id"] = false },
            },
        };
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, options, columnMappingMode: ColumnMappingMode.Name);
        await table.WriteAsync([Ids(100)]);

        var (_, meta) = Assert.Single(await Chunks());
        Assert.Null(meta.Statistics?.MinValue);
        Assert.Null(meta.Statistics?.MaxValue);
    }

    [Fact]
    public async Task NestedFieldsAndListElements_AreTranslated()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("s", new ArrowStructType([new Field("x", Int64Type.Default, false)]), false))
            .Field(new Field("tags", new ListType(new Field("element", StringType.Default, false)), false))
            .Build();
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with
            {
                BloomFilterColumns = new HashSet<string> { "s.x", "tags.list.element" },
            },
        };
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), schema, options, columnMappingMode: ColumnMappingMode.Name);

        var x = new Int64Array.Builder().Append(1).Append(2).Build();
        var s = new StructArray((ArrowStructType)schema.FieldsList[0].DataType, 2, [x], ArrowBuffer.Empty, nullCount: 0);
        var tags = new ListArray.Builder(StringType.Default);
        var tagValues = (StringArray.Builder)tags.ValueBuilder;
        tags.Append(); tagValues.Append("a");
        tags.Append(); tagValues.Append("b");
        await table.WriteAsync([new RecordBatch(schema, [s, tags.Build()], 2)]);

        var chunks = await Chunks();
        Assert.Equal(2, chunks.Count);
        Assert.All(chunks, c => Assert.NotNull(c.Meta.BloomFilterOffset));
        Assert.All(chunks, c => Assert.StartsWith("col-", c.Path));
    }

    /// <summary>
    /// The translation follows the schema at each write: an option naming "renamed" matches nothing until
    /// the column is renamed to it, and then reaches the same physical leaf.
    /// </summary>
    [Fact]
    public async Task ARename_IsPickedUpAtTheNextWrite()
    {
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { BloomFilterColumns = new HashSet<string> { "renamed" } },
        };
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, options, columnMappingMode: ColumnMappingMode.Name);

        await table.WriteAsync([Ids(10)]);
        Assert.All(await Chunks(), c => Assert.Null(c.Meta.BloomFilterOffset));

        await table.RenameColumnAsync("id", "renamed");
        var renamedSchema = new Apache.Arrow.Schema.Builder().Field(new Field("renamed", Int64Type.Default, false)).Build();
        await table.WriteAsync([new RecordBatch(renamedSchema, [Ids(10).Column(0)], 10)]);

        var chunks = await Chunks();
        Assert.Equal(2, chunks.Count);
        Assert.Single(chunks, c => c.Meta.BloomFilterOffset is not null);
    }

    /// <summary>
    /// What the fix is for: a column-mapped table's scan can now be pruned by the Bloom filter it asked
    /// for. Even ids only, so every row group's min/max spans an odd literal and statistics cannot decide.
    /// </summary>
    [Fact]
    public async Task BloomPruning_WorksOnAColumnMappedTable()
    {
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with
            {
                RowGroupMaxRows = 10,
                BloomFilterColumns = new HashSet<string> { "id" },
            },
            ParquetReadOptions = ParquetReadOptions.Default with
            {
                FilterUseBloomFilters = true,
                FilterUseDictionaries = false,
            },
        };
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, options, columnMappingMode: ColumnMappingMode.Name);
        var ids = new Int64Array.Builder().AppendRange(Enumerable.Range(0, 30).Select(v => 2L * v)).Build();
        await table.WriteAsync([new RecordBatch(IdSchema, [ids], 30)]);

        int rows = 0;
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Filter = Ex.Equal("id", 13L) }))
            rows += batch.Length;

        Assert.Equal(0, rows);
    }

    /// <summary>
    /// A buffered transaction writes under its PENDING schema, whose added column has a physical name the
    /// committed snapshot does not know yet; an option naming that column must still reach it.
    /// </summary>
    [Fact]
    public async Task AStagedAddColumn_IsTranslatedAgainstThePendingSchema()
    {
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { BloomFilterColumns = new HashSet<string> { "extra" } },
        };
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, options, columnMappingMode: ColumnMappingMode.Name);

        var change = table.ComputeAddColumn(new Field("extra", Int32Type.Default, true));
        var widened = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("extra", Int32Type.Default, true))
            .Build();
        await table.WriteDataFilesAsync(
            [new RecordBatch(widened, [Ids(3).Column(0), new Int32Array.Builder().Append(1).Append(2).Append(3).Build()], 3)],
            schemaOverride: change.NewSchema);

        var chunks = await Chunks();
        Assert.Equal(2, chunks.Count);
        Assert.Single(chunks, c => c.Meta.BloomFilterOffset is not null);
    }

    // ───── The translator ─────

    private static StructField Field(string name, DeltaDataType type, string physical) => new()
    {
        Name = name,
        Type = type,
        Nullable = true,
        Metadata = new Dictionary<string, string>
        {
            [ColumnMapping.PhysicalNameKey] = physical,
            [ColumnMapping.FieldIdKey] = "1",
        },
    };

    private static readonly PrimitiveType Long = new() { TypeName = "long" };

    private static readonly Schema.StructType Mapped = new()
    {
        Fields =
        [
            Field("id", Long, "col-id"),
            Field("a.b", Long, "col-dotted"),
            Field("s", new Schema.StructType { Fields = [Field("x", Long, "col-x")] }, "col-s"),
            Field("tags", new ArrayType { ElementType = Long, ContainsNull = true }, "col-tags"),
            Field("m", new Schema.MapType { KeyType = Long, ValueType = Long, ValueContainsNull = true }, "col-m"),
            Field("amb", new Schema.StructType { Fields = [Field("c", Long, "col-amb-c")] }, "col-amb"),
            Field("amb.c", Long, "col-amb-dotted"),
            Field("p", Long, "col-p"),
            Field("p.q", Long, "col-pq"),
            Field("v", new PrimitiveType { TypeName = "variant" }, "col-v"),
        ],
    };

    [Theory]
    [InlineData("id", "col-id")]
    [InlineData("a.b", "col-dotted")] // a field whose name has a dot
    [InlineData("s.x", "col-s.col-x")]
    [InlineData("tags.list.element", "col-tags.list.element")]
    [InlineData("m.key_value.key", "col-m.key_value.key")]
    [InlineData("m.key_value.value", "col-m.key_value.value")]
    [InlineData("missing", "missing")] // unresolved: left as it was
    [InlineData("col-id", "col-id")] // already physical: still works
    [InlineData("s.nope", "s.nope")]
    [InlineData("tags.element", "tags.element")] // not the Parquet list layout
    [InlineData("amb.c", "amb.c")] // a struct leaf and a dotted field both spell it: ambiguous
    [InlineData("p.q", "col-pq")] // a primitive p cannot hold q, so the dotted field is the only reading
    [InlineData("p.extra", "p.extra")] // nothing follows a non-variant primitive
    [InlineData("v.value", "col-v.value")] // a variant's own children pass through
    public void Translate(string logical, string physical)
    {
        Assert.Equal(physical, ColumnMappingWriteOptions.Translate(logical, Mapped, ColumnMappingMode.Name));
    }

    [Fact]
    public void NoMapping_ReturnsTheOptionsUnchanged()
    {
        var options = ParquetWriteOptions.Default with { BloomFilterColumns = new HashSet<string> { "id" } };

        Assert.Same(options, ColumnMappingWriteOptions.ToPhysical(options, Mapped, ColumnMappingMode.None));
    }

    [Fact]
    public void EveryPerColumnOption_IsTranslated()
    {
#pragma warning disable EWPARQUET0004
        var options = ParquetWriteOptions.Default with
        {
            BloomFilterColumns = new HashSet<string> { "id" },
            ColumnCodecs = new Dictionary<string, EngineeredWood.Compression.CompressionCodec> { ["id"] = EngineeredWood.Compression.CompressionCodec.Zstd },
            ColumnCompressionLevels = new Dictionary<string, EngineeredWood.Compression.BlockCompressionLevel> { ["id"] = EngineeredWood.Compression.BlockCompressionLevel.Fastest },
            ColumnEncodings = new Dictionary<string, ByteArrayEncoding> { ["id"] = default },
            ColumnDictionaryEnabled = new Dictionary<string, bool> { ["id"] = false },
            ColumnWriteStatistics = new Dictionary<string, bool> { ["id"] = false },
            ExtendedTimestampColumns = ["id"],
            VariantShredSchemas = new Dictionary<string, Apache.Arrow.Operations.Shredding.ShredSchema> { ["id"] = null! },
        };

        var physical = ColumnMappingWriteOptions.ToPhysical(options, Mapped, ColumnMappingMode.Name);

        Assert.Equal(["col-id"], physical.BloomFilterColumns!);
        Assert.Equal(["col-id"], physical.ColumnCodecs!.Keys);
        Assert.Equal(["col-id"], physical.ColumnCompressionLevels!.Keys);
        Assert.Equal(["col-id"], physical.ColumnEncodings!.Keys);
        Assert.Equal(["col-id"], physical.ColumnDictionaryEnabled!.Keys);
        Assert.Equal(["col-id"], physical.ColumnWriteStatistics!.Keys);
        Assert.Equal(["col-id"], physical.ExtendedTimestampColumns!);
        Assert.Equal(["col-id"], physical.VariantShredSchemas!.Keys);
#pragma warning restore EWPARQUET0004
    }
}
