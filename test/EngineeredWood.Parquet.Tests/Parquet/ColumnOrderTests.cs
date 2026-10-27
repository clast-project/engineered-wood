// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using EngineeredWood.Parquet.Metadata;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// The file's <c>column_orders</c> say how each column's min/max are ordered (#546). The reader used to
/// ignore them and compare every bound as if it were TYPE_ORDER; a bound in an order it does not know
/// can be anything, so pruning on it can skip rows that match.
/// </summary>
public sealed class ColumnOrderTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ew-colorder-" + Guid.NewGuid().ToString("N")[..8]);

    public ColumnOrderTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        if (Directory.Exists(_dir))
            Directory.Delete(_dir, recursive: true);
    }

    // ── Reading ──

    [Fact]
    public async Task TypeDefinedOrder_Prunes()
    {
        string path = await WriteTwoRowGroupsAsync();

        Assert.Equal(new[] { false, true }, await CandidatesAsync(path, Ex.GreaterThan("d", 150.0)));
        Assert.Equal(new[] { false, true }, await CandidatesAsync(path, Ex.GreaterThan("i", 150L)));
        Assert.All((await SchemaAsync(path)).Columns, c => Assert.Equal(ColumnOrder.TypeDefined, c.ColumnOrder));
    }

    /// <summary>
    /// An order this reader does not recognize (decoded as <see cref="ColumnOrder.Undefined"/>) says
    /// nothing a literal can be compared against: no row group or page is skipped on that column, and
    /// the accessor reports no bounds. Other columns are unaffected.
    /// </summary>
    [Fact]
    public async Task UnrecognizedOrder_BoundsAreNotUsed()
    {
        string path = await WriteTwoRowGroupsAsync();
        SetColumnOrders(path, [ColumnOrder.Undefined, ColumnOrder.TypeDefined]);

        var schema = await SchemaAsync(path);
        Assert.Equal(ColumnOrder.Undefined, schema.Columns[0].ColumnOrder);
        Assert.Equal(ColumnOrder.TypeDefined, schema.Columns[1].ColumnOrder);

        Assert.Equal(new[] { true, true }, await CandidatesAsync(path, Ex.GreaterThan("d", 150.0)));
        Assert.Equal(new[] { false, true }, await CandidatesAsync(path, Ex.GreaterThan("i", 150L)));

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var accessor = new ParquetStatisticsAccessor(await reader.GetSchemaAsync());
        Assert.NotNull(metadata.RowGroups[0].Columns[0].MetaData!.Statistics!.MinValue);
        Assert.Null(accessor.GetMinValue(metadata.RowGroups[0], "d"));
        Assert.Null(accessor.GetMaxValue(metadata.RowGroups[0], "d"));

        // The page index is ordered the same way, so it cannot narrow the row group either.
        var ranges = await reader.GetCandidateRowRangesAsync(1, Ex.GreaterThan("d", 190.0));
        Assert.Equal(new[] { new RowRange(0, 100) }, ranges);
    }

    /// <summary>A <c>column_orders</c> list that does not match the leaves cannot be attributed to them.</summary>
    [Fact]
    public async Task MismatchedColumnOrdersList_NoColumnHasAKnownOrder()
    {
        string path = await WriteTwoRowGroupsAsync();
        SetColumnOrders(path, [ColumnOrder.TypeDefined]);

        Assert.All((await SchemaAsync(path)).Columns, c => Assert.Equal(ColumnOrder.Undefined, c.ColumnOrder));
        Assert.Equal(new[] { true, true }, await CandidatesAsync(path, Ex.GreaterThan("i", 150L)));
    }

    /// <summary>
    /// No <c>column_orders</c> at all: files from before the field existed, which readers have always
    /// read as TYPE_ORDER.
    /// </summary>
    [Fact]
    public async Task NoColumnOrdersList_IsTypeDefined()
    {
        string path = await WriteTwoRowGroupsAsync();
        SetColumnOrders(path, null);

        Assert.All((await SchemaAsync(path)).Columns, c => Assert.Equal(ColumnOrder.TypeDefined, c.ColumnOrder));
        Assert.Equal(new[] { false, true }, await CandidatesAsync(path, Ex.GreaterThan("d", 150.0)));
    }

    /// <summary>IEEE 754 total order is only for FLOAT, DOUBLE and FLOAT16; on an integer it is invalid.</summary>
    [Fact]
    public async Task TotalOrderOnAnInteger_IsUndefined()
    {
        string path = await WriteTwoRowGroupsAsync();
        SetColumnOrders(path, [ColumnOrder.Ieee754TotalOrder, ColumnOrder.Ieee754TotalOrder]);

        var schema = await SchemaAsync(path);
        Assert.Equal(ColumnOrder.Ieee754TotalOrder, schema.Columns[0].ColumnOrder);
        Assert.Equal(ColumnOrder.Undefined, schema.Columns[1].ColumnOrder);
        Assert.Equal(new[] { false, true }, await CandidatesAsync(path, Ex.GreaterThan("d", 150.0)));
        Assert.Equal(new[] { true, true }, await CandidatesAsync(path, Ex.GreaterThan("i", 150L)));
    }

    /// <summary>INT96 timestamp order is only for INT96.</summary>
    [Fact]
    public async Task Int96OrderOnAnotherType_IsUndefined()
    {
        string path = await WriteTwoRowGroupsAsync();
        SetColumnOrders(path, [ColumnOrder.TypeDefined, ColumnOrder.Int96TimestampOrder]);

        Assert.Equal(ColumnOrder.Undefined, (await SchemaAsync(path)).Columns[1].ColumnOrder);
        Assert.Equal(new[] { true, true }, await CandidatesAsync(path, Ex.GreaterThan("i", 150L)));
    }

    [Fact]
    public async Task TotalOrderFile_Prunes()
    {
        string path = await WriteTwoRowGroupsAsync(FloatingPointColumnOrder.Ieee754TotalOrder);

        Assert.Equal(ColumnOrder.Ieee754TotalOrder, (await SchemaAsync(path)).Columns[0].ColumnOrder);
        Assert.Equal(new[] { false, true }, await CandidatesAsync(path, Ex.GreaterThan("d", 150.0)));
    }

    // ── Writing: TYPE_ORDER's signed zeros ──

    public static TheoryData<bool, string, bool, bool> ZeroCases()
    {
        var data = new TheoryData<bool, string, bool, bool>();
        foreach (bool isFloat in new[] { true, false })
        foreach (string writer in new[] { "file", "buffered" })
        foreach (bool dictionary in new[] { true, false })
        foreach (bool negativeZeros in new[] { true, false })
            data.Add(isFloat, writer, dictionary, negativeZeros);
        return data;
    }

    /// <summary>
    /// Under TYPE_ORDER a zero minimum is written as -0.0 and a zero maximum as +0.0, whichever zero the
    /// values held, in the chunk statistics and the column index alike: TYPE_ORDER does not say whether
    /// -0 sorts below +0, so a bound must hold under either reading. A chunk of only +0.0 used to record
    /// a minimum of +0.0, which a reader that orders the zeros takes as excluding -0.0.
    /// </summary>
    [Theory]
    [MemberData(nameof(ZeroCases))]
    public async Task TypeOrder_ZeroBoundsAreSignedPerTheSpec(bool isFloat, string writer, bool dictionary, bool negativeZeros)
    {
        double zero = negativeZeros ? -0.0 : 0.0;
        string path = await WriteZerosAsync(isFloat, writer, dictionary, FloatingPointColumnOrder.TypeDefined, zero);

        var (stats, columnIndex) = await ReadBoundsAsync(path);
        AssertZero(isFloat, negative: true, stats.MinValue!);
        AssertZero(isFloat, negative: false, stats.MaxValue!);
        AssertZero(isFloat, negative: true, columnIndex.MinValues[0]);
        AssertZero(isFloat, negative: false, columnIndex.MaxValues[0]);
    }

    /// <summary>IEEE 754 total order orders the zeros, so its bounds are the exact ones.</summary>
    [Theory]
    [MemberData(nameof(ZeroCases))]
    public async Task TotalOrder_ZeroBoundsAreExact(bool isFloat, string writer, bool dictionary, bool negativeZeros)
    {
        double zero = negativeZeros ? -0.0 : 0.0;
        string path = await WriteZerosAsync(isFloat, writer, dictionary, FloatingPointColumnOrder.Ieee754TotalOrder, zero);

        var (stats, columnIndex) = await ReadBoundsAsync(path);
        AssertZero(isFloat, negativeZeros, stats.MinValue!);
        AssertZero(isFloat, negativeZeros, stats.MaxValue!);
        AssertZero(isFloat, negativeZeros, columnIndex.MinValues[0]);
        AssertZero(isFloat, negativeZeros, columnIndex.MaxValues[0]);
    }

    /// <summary>Only a zero bound changes; the other bound keeps its value.</summary>
    [Fact]
    public async Task TypeOrder_OnlyTheZeroBoundChanges()
    {
        string path = Path.Combine(_dir, "mixed.parquet");
        await WriteAsync(path, "file", Batch(new DoubleArray.Builder().AppendRange([-0.0, 2.5, -0.0]).Build()),
            new ParquetWriteOptions { DictionaryEnabled = false });

        var (stats, _) = await ReadBoundsAsync(path);
        AssertZero(isFloat: false, negative: true, stats.MinValue!);
        Assert.Equal(2.5, BitConverter.ToDouble(stats.MaxValue!, 0));
    }

    // ── Helpers ──

    /// <summary>DOUBLE <c>d</c> and INT64 <c>i</c>, both 1..200, in two row groups of 100.</summary>
    private async Task<string> WriteTwoRowGroupsAsync(
        FloatingPointColumnOrder order = FloatingPointColumnOrder.TypeDefined)
    {
        var d = new DoubleArray.Builder();
        var i = new Int64Array.Builder();
        for (int v = 1; v <= 200; v++)
        {
            d.Append(v);
            i.Append(v);
        }
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("d", DoubleType.Default, nullable: false))
            .Field(new Field("i", Int64Type.Default, nullable: false))
            .Build();

        string path = Path.Combine(_dir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await WriteAsync(path, "file", new RecordBatch(schema, [d.Build(), i.Build()], 200), new ParquetWriteOptions
        {
            RowGroupMaxRows = 100,
            DataPageSize = 256,   // several pages per chunk, so the page index has something to prune
            DictionaryEnabled = false,
            FloatingPointOrder = order,
        });
        return path;
    }

    private async Task<string> WriteZerosAsync(
        bool isFloat, string writer, bool dictionary, FloatingPointColumnOrder order, double zero)
    {
        IArrowArray column = isFloat
            ? new FloatArray.Builder().AppendRange(Enumerable.Repeat((float)zero, 50)).Build()
            : new DoubleArray.Builder().AppendRange(Enumerable.Repeat(zero, 50)).Build();
        string path = Path.Combine(_dir, Guid.NewGuid().ToString("N")[..8] + ".parquet");
        await WriteAsync(path, writer, Batch(column),
            new ParquetWriteOptions { DictionaryEnabled = dictionary, FloatingPointOrder = order });
        return path;
    }

    private static RecordBatch Batch(IArrowArray column) => new(
        new Apache.Arrow.Schema.Builder().Field(new Field("x", column.Data.DataType, nullable: false)).Build(),
        [column], column.Length);

    private static async Task WriteAsync(string path, string writer, RecordBatch batch, ParquetWriteOptions options)
    {
        await using var file = new LocalSequentialFile(path);
        if (writer == "file")
        {
            await using var w = new ParquetFileWriter(file, ownsFile: false, options);
            await w.WriteRowGroupAsync(batch);
            await w.CloseAsync();
        }
        else
        {
            await using var w = new BufferedParquetWriter(file, ownsFile: false, options);
            await w.AppendAsync(batch);
            await w.CloseAsync();
        }
    }

    private static void SetColumnOrders(string path, IReadOnlyList<ColumnOrder>? orders) =>
        FooterRewrite.Rewrite(path, metadata => FooterRewrite.With(metadata, nameof(FileMetaData.ColumnOrders), orders));

    private static async Task<EngineeredWood.Parquet.Schema.SchemaDescriptor> SchemaAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        return await reader.GetSchemaAsync();
    }

    private static async Task<bool[]> CandidatesAsync(string path, EngineeredWood.Expressions.Predicate filter)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var candidates = await reader.GetCandidateRowGroupsAsync(filter);
        return Enumerable.Range(0, candidates.Count).Select(i => candidates[i]).ToArray();
    }

    private static async Task<(Statistics Stats, ColumnIndex ColumnIndex)> ReadBoundsAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var index = (await reader.ReadPageIndexAsync(0))[0];
        return (metadata.RowGroups[0].Columns[0].MetaData!.Statistics!, index.ColumnIndex!);
    }

    private static void AssertZero(bool isFloat, bool negative, byte[] bound)
    {
        byte[] expected = isFloat
            ? BitConverter.GetBytes(negative ? -0.0f : 0.0f)
            : BitConverter.GetBytes(negative ? -0.0 : 0.0);
        Assert.Equal(expected, bound);
    }
}
