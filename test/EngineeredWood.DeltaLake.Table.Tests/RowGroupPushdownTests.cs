// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using ArrowStructType = Apache.Arrow.Types.StructType;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// <see cref="DeltaReadOptions.Filter"/> reaches the Parquet row groups of a surviving file (#55). Every
/// table here is ONE file of 30 rows, <c>id</c> 0..29 in row groups of ten, so file pruning keeps the file
/// for any predicate that some row matches, and only row-group pruning can shrink the read. Row position
/// equals <c>id</c>, which is what makes the position-keyed checks readable.
/// </summary>
public class RowGroupPushdownTests : IDisposable
{
    private const int RowsPerGroup = 10;
    private const int Rows = 30;

    private readonly string _tempDir;

    public RowGroupPushdownTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_rgpd_{Guid.NewGuid():N}");
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

    private static DeltaTableOptions SmallRowGroups(bool bloom = false) => new()
    {
        ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = RowsPerGroup },
        ParquetReadOptions = ParquetReadOptions.Default with { FilterUseBloomFilters = bloom },
    };

    private async Task<DeltaTable> CreateOneFileTable(
        ColumnMappingMode mode = ColumnMappingMode.None, bool dv = false, bool rowTracking = false)
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, SmallRowGroups(),
            columnMappingMode: mode, enableDeletionVectors: dv, enableRowTracking: rowTracking);
        var ids = new Int64Array.Builder().AppendRange(Enumerable.Range(0, Rows).Select(v => (long)v)).Build();
        await table.WriteAsync([new RecordBatch(IdSchema, [ids], Rows)]);
        Assert.Single(table.CurrentSnapshot.ActiveFiles);
        return table;
    }

    private static async Task<List<long>> ReadIds(DeltaTable table, Predicate? filter)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Filter = filter }))
        {
            var column = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(column.GetValue(i)!.Value);
        }
        return ids;
    }

    private static List<long> Range(int start, int count) =>
        Enumerable.Range(start, count).Select(v => (long)v).ToList();

    [Fact]
    public async Task Filter_SkipsRowGroupsTheFooterRulesOut()
    {
        await using var table = await CreateOneFileTable();

        Assert.Equal(Range(0, Rows), await ReadIds(table, null));
        Assert.Equal(Range(20, RowsPerGroup), await ReadIds(table, Ex.GreaterThanOrEqual("id", 25L)));
        Assert.Equal(Range(10, RowsPerGroup), await ReadIds(table, Ex.Equal("id", 15L)));
    }

    [Fact]
    public async Task Filter_NothingProvable_ReadsEveryRow()
    {
        await using var table = await CreateOneFileTable();

        // A NOT over an unresolvable reference must stay Unknown; a constant substitute would be inverted.
        Assert.Equal(Range(0, Rows), await ReadIds(table, new NotPredicate(Ex.Equal("nope", 1L))));
        Assert.Equal(Range(0, Rows), await ReadIds(table, Ex.Or(Ex.Equal("id", 5L), Ex.IsNull("nope"))));
    }

    /// <summary>
    /// The deletion vector is keyed by file position. Positions used to be counted from the batches read,
    /// so after a skipped row group row 27 would have been looked for at position 7 and survived.
    /// </summary>
    [Fact]
    public async Task DeletionVector_StillAppliesAtTheRowsFilePosition()
    {
        await using var table = await CreateOneFileTable(dv: true);
        await table.DeleteAsync(Ex.Equal("id", 27L));
        Assert.NotNull(table.CurrentSnapshot.ActiveFiles.Values.Single().DeletionVector);

        var expected = Range(20, RowsPerGroup);
        expected.Remove(27);
        Assert.Equal(expected, await ReadIds(table, Ex.GreaterThanOrEqual("id", 25L)));
    }

    [Fact]
    public async Task MetadataColumns_CarryTheFilePosition_AfterASkippedRowGroup()
    {
        await using var table = await CreateOneFileTable(rowTracking: true);

        var options = new DeltaReadOptions
        {
            Filter = Ex.GreaterThanOrEqual("id", 25L),
            Metadata = DeltaRowMetadata.Locator | DeltaRowMetadata.RowTracking,
        };
        int rows = 0;
        await foreach (var batch in table.ReadAsync(options))
        {
            var id = (Int64Array)batch.Column("id");
            var rowIndex = (Int64Array)batch.Column(DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIndexSuffix);
            var rowId = (Int64Array)batch.Column(DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIdSuffix);
            for (int i = 0; i < batch.Length; i++)
            {
                Assert.Equal(id.GetValue(i), rowIndex.GetValue(i));
                Assert.Equal(id.GetValue(i), rowId.GetValue(i)); // baseRowId 0 + position
            }
            rows += batch.Length;
        }
        Assert.Equal(RowsPerGroup, rows);
    }

    [Theory]
    [InlineData(ColumnMappingMode.Name)]
    [InlineData(ColumnMappingMode.Id)]
    public async Task ColumnMapping_TranslatesLogicalNames(ColumnMappingMode mode)
    {
        await using var table = await CreateOneFileTable(mode);

        // Untranslated, "id" would miss the file's col-<uuid> leaf and every row group would be kept.
        Assert.Equal(Range(20, RowsPerGroup), await ReadIds(table, Ex.GreaterThanOrEqual("id", 25L)));
    }

    /// <summary>
    /// Dictionary pages (#57) prune through the same per-read path, by default, unless the table turns
    /// them off. The three row groups span "a".."z" but hold disjoint names, so statistics cannot rule
    /// any out. Null leaves the table's read options at their default.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Dictionaries_ApplyUnlessTheTableOptsOut(bool? useDictionaries)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("name", StringType.Default, false))
            .Build();
        var options = new DeltaTableOptions
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = RowsPerGroup },
            ParquetReadOptions = useDictionaries is { } use
                ? ParquetReadOptions.Default with { FilterUseDictionaries = use }
                : ParquetReadOptions.Default,
        };
        await using var table = await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), schema, options);

        // Each group of ten repeats its two names, so the writer builds a dictionary for it.
        string[][] groups = [["alpha", "zebra"], ["cherry", "zinnia"], ["avocado", "zucchini"]];
        var names = new StringArray.Builder();
        foreach (var group in groups)
            for (int i = 0; i < RowsPerGroup; i++)
                names.Append(group[i % 2]);
        await table.WriteAsync([new RecordBatch(schema, [names.Build()], Rows)]);
        Assert.Single(table.CurrentSnapshot.ActiveFiles);

        int rows = 0;
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Filter = Ex.Equal("name", "cherry") }))
            rows += batch.Length;

        Assert.Equal(useDictionaries != false ? RowsPerGroup : Rows, rows);
    }

    [Fact]
    public async Task BloomFilters_ApplyWhenTheTableOptsIn()
    {
        var bloomWrite = ParquetWriteOptions.Default with
        {
            RowGroupMaxRows = RowsPerGroup,
            BloomFilterColumns = new HashSet<string> { "id" },
        };
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema,
            SmallRowGroups(bloom: true) with { ParquetWriteOptions = bloomWrite });

        // Even ids only, so the gaps sit INSIDE each row group's min/max: statistics cannot rule a group out.
        var ids = new Int64Array.Builder().AppendRange(Enumerable.Range(0, Rows).Select(v => 2L * v)).Build();
        await table.WriteAsync([new RecordBatch(IdSchema, [ids], Rows)]);

        Assert.Empty(await ReadIds(table, Ex.Equal("id", 13L)));
    }

    [Fact]
    public async Task ReaderLevelFilter_IsRefused()
    {
        var options = new DeltaTableOptions
        {
            ParquetReadOptions = ParquetReadOptions.Default with { Filter = Ex.Equal("id", 1L) },
        };

        var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await DeltaTable.CreateAsync(new LocalTableFileSystem(_tempDir), IdSchema, options));
        Assert.Contains("DeltaReadOptions.Filter", ex.Message);

        // Refused BEFORE commit 0: a refusal must not leave a table behind.
        Assert.False(Directory.Exists(Path.Combine(_tempDir, "_delta_log"))
            && Directory.EnumerateFiles(Path.Combine(_tempDir, "_delta_log")).Any());
    }

    // ── The logical-to-file name map ──

    private static StructField Long(string name, IReadOnlyDictionary<string, string>? metadata = null) => new()
    {
        Name = name, Type = new PrimitiveType { TypeName = "long" }, Nullable = true, Metadata = metadata,
    };

    private async Task<EngineeredWood.Parquet.Schema.SchemaDescriptor> FileSchema(Apache.Arrow.Schema schema)
    {
        string path = Path.Combine(_tempDir, $"{Guid.NewGuid():N}.parquet");
        await using (var file = new EngineeredWood.IO.Local.LocalSequentialFile(path))
        {
            await using var writer = new ParquetFileWriter(file, ownsFile: false);
            var columns = schema.FieldsList.Select(f => ArrowNulls(f.DataType)).ToList();
            await writer.WriteRowGroupAsync(new RecordBatch(schema, columns, 1));
        }
        await using var read = new EngineeredWood.IO.Local.LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(read, ownsFile: false);
        return await reader.GetSchemaAsync();
    }

    private static IArrowArray ArrowNulls(IArrowType type) => type switch
    {
        ArrowStructType st => new StructArray(st, 1,
            st.Fields.Select(f => ArrowNulls(f.DataType)).ToList(), ArrowBuffer.Empty, nullCount: 0),
        ListType lt => new ListArray(lt, 1,
            new ArrowBuffer.Builder<int>().Append(0).Append(0).Build(), new Int64Array.Builder().Build(),
            ArrowBuffer.Empty, nullCount: 0),
        _ => new Int64Array.Builder().AppendNull().Build(),
    };

    [Fact]
    public async Task NameMap_KeepsOnlyUnambiguousUnchangedStructLeaves()
    {
        var file = await FileSchema(new Apache.Arrow.Schema.Builder()
            .Field(new Field("plain", Int64Type.Default, true))
            .Field(new Field("widened", Int64Type.Default, true))
            .Field(new Field("s", new ArrowStructType([new Field("x", Int64Type.Default, true)]), true))
            .Field(new Field("a.b", Int64Type.Default, true))
            .Field(new Field("a", new ArrowStructType([new Field("b", Int64Type.Default, true)]), true))
            .Field(new Field("l", new ListType(new Field("element", Int64Type.Default, true)), true))
            .Build());

        var table = new Schema.StructType
        {
            Fields =
            [
                Long("plain"),
                Long("widened", new Dictionary<string, string>
                {
                    [Schema.TypeWidening.TypeChangesKey] = """[{"fromType":"integer","toType":"long"}]""",
                }),
                new StructField
                {
                    Name = "s", Nullable = true, Type = new Schema.StructType { Fields = [Long("x")] },
                },
                Long("a.b"),
                new StructField
                {
                    Name = "a", Nullable = true, Type = new Schema.StructType { Fields = [Long("b")] },
                },
                new StructField
                {
                    Name = "l", Nullable = true,
                    Type = new ArrayType { ElementType = new PrimitiveType { TypeName = "long" }, ContainsNull = true },
                },
                Long("added_later"),
            ],
        };

        var map = RowGroupPushdown.LogicalToFileLeaf(table, ColumnMappingMode.None, file);

        // widened: the file's bounds are of the old type. a.b: two table columns claim the path. l: a list
        // element's bounds do not bound the row. added_later: not in this file.
        Assert.Equal(
            new Dictionary<string, string> { ["plain"] = "plain", ["s.x"] = "s.x" },
            map);
    }

    [Fact]
    public async Task NameMap_AmbiguousLogicalPath_IsUnresolved_EvenWhenTheFileHoldsOnlyOneOfThem()
    {
        // The file predates the literal "a.b" column, so its leaf path "a.b" is unique; the predicate's
        // "a.b" still names two table columns, and pruning must not pick one.
        var file = await FileSchema(new Apache.Arrow.Schema.Builder()
            .Field(new Field("a", new ArrowStructType([new Field("b", Int64Type.Default, true)]), true))
            .Build());
        var table = new Schema.StructType
        {
            Fields =
            [
                Long("a.b"),
                new StructField
                {
                    Name = "a", Nullable = true, Type = new Schema.StructType { Fields = [Long("b")] },
                },
            ],
        };

        Assert.Empty(RowGroupPushdown.LogicalToFileLeaf(table, ColumnMappingMode.None, file));
    }

    [Fact]
    public void Rewrite_ReachesEveryReference()
    {
        var predicate = Ex.And(
            new NotPredicate(Ex.Equal("a", 1L)),
            Ex.Or(Ex.IsNull("b"), new SetPredicate(new UnboundReference("c"), [LiteralValue.Of(1L)], SetOperator.In)),
            new ComparisonPredicate(
                new FunctionCall("lower", [new BoundReference(7, "d")]), ComparisonOperator.Equal,
                new UnboundReference("e")));

        var rewritten = RowGroupPushdown.Rewrite(predicate, name => "p_" + name);

        Assert.Equal(
            "(NOT p_a = 1 AND (p_b IS NULL OR p_c IN (1)) AND lower(p_d#7) = p_e)",
            rewritten.ToString());
    }

    [Fact]
    public async Task Compaction_ReadsEveryRow_AfterAPrunedScan()
    {
        await using var table = await CreateOneFileTable();
        var ids = new Int64Array.Builder().AppendRange(Enumerable.Range(Rows, Rows).Select(v => (long)v)).Build();
        await table.WriteAsync([new RecordBatch(IdSchema, [ids], Rows)]);

        Assert.Equal(Range(20, RowsPerGroup), await ReadIds(table,
            Ex.And(Ex.GreaterThanOrEqual("id", 25L), Ex.LessThan("id", 30L))));

        await table.CompactAsync();
        Assert.Equal(Range(0, 2 * Rows), (await ReadIds(table, null)).OrderBy(v => v).ToList());
    }
}
