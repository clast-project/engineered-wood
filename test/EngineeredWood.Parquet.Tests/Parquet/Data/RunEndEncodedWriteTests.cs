// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// The writer accepts run-end encoded columns, and the contract is that accepting one changes NOTHING
/// about the file: run-end encoding is an in-memory layout, no Parquet file holds runs, and a reader must
/// not be able to tell which form the writer was handed.
///
/// <para>So nearly every case here asserts the bytes of the file written from a run-encoded column against
/// the bytes written from the equivalent plain one. That single assertion covers the dictionary page, the
/// index stream, the definition levels, the statistics, the Bloom filter and the page headers at once —
/// and unlike a round-trip through our own reader, it cannot be satisfied by a writer and reader that
/// agree with each other and with nobody else.</para>
/// </summary>
public class RunEndEncodedWriteTests : IDisposable
{
    private readonly string _tempDir;
    private int _fileCounter;

    public RunEndEncodedWriteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-ree-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    // ── Building the two forms of the same column ──

    /// <summary>A run-encoded column and its plain twin, from the same (value, row count) runs.</summary>
    private static (RunEndEncodedArray Encoded, IArrowArray Plain) StringRuns(
        params (string? Value, int Length)[] runs)
    {
        var values = new StringArray.Builder();
        var ends = new Int32Array.Builder();
        var plain = new StringArray.Builder();
        int end = 0;

        foreach (var (value, length) in runs)
        {
            if (value is null) values.AppendNull();
            else values.Append(value);

            end += length;
            ends.Append(end);

            for (int i = 0; i < length; i++)
            {
                if (value is null) plain.AppendNull();
                else plain.Append(value);
            }
        }

        return (new RunEndEncodedArray(ends.Build(), values.Build()), plain.Build());
    }

    private static (RunEndEncodedArray Encoded, IArrowArray Plain) Int64Runs(
        params (long? Value, int Length)[] runs)
    {
        var values = new Int64Array.Builder();
        var ends = new Int32Array.Builder();
        var plain = new Int64Array.Builder();
        int end = 0;

        foreach (var (value, length) in runs)
        {
            if (value is null) values.AppendNull();
            else values.Append(value.Value);

            end += length;
            ends.Append(end);

            for (int i = 0; i < length; i++)
            {
                if (value is null) plain.AppendNull();
                else plain.Append(value.Value);
            }
        }

        return (new RunEndEncodedArray(ends.Build(), values.Build()), plain.Build());
    }

    private static RecordBatch Batch(IArrowArray column, bool nullable)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("c", column.Data.DataType, nullable))
            .Build();

        return new RecordBatch(schema, [column], column.Length);
    }

    private async Task<byte[]> WriteAsync(RecordBatch batch, ParquetWriteOptions options)
    {
        string path = Path.Combine(_tempDir, $"f{_fileCounter++}.parquet");

        await using (var file = new LocalSequentialFile(path))
        {
            await using var writer = new ParquetFileWriter(file, ownsFile: false, options);
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        return File.ReadAllBytes(path);
    }

    /// <summary>
    /// Writes both forms of a column and asserts the files are byte-identical, returning the bytes so a
    /// caller can go on to read them back.
    /// </summary>
    private async Task<byte[]> AssertIdenticalFilesAsync(
        (RunEndEncodedArray Encoded, IArrowArray Plain) column,
        bool nullable,
        ParquetWriteOptions? options = null)
    {
        options ??= new ParquetWriteOptions();

        byte[] encoded = await WriteAsync(Batch(column.Encoded, nullable), options);
        byte[] plain = await WriteAsync(Batch(column.Plain, nullable), options);

        Assert.Equal(plain, encoded);
        return encoded;
    }

    private static async Task<RecordBatch> ReadAsync(byte[] bytes, string name)
    {
        string path = Path.Combine(Path.GetTempPath(), "ew-ree-read-" + Guid.NewGuid().ToString("N")[..8]);
        File.WriteAllBytes(path, bytes);
        try
        {
            await using var file = new LocalRandomAccessFile(path);
            using var reader = new ParquetFileReader(file, ownsFile: false);
            return await reader.ReadRowGroupAsync(0);
        }
        finally
        {
            File.Delete(path);
        }
    }

    // ── The shape the write path actually produces ──

    [Fact]
    public async Task ConstantColumn_WritesTheSameFileAsTheTiledForm()
    {
        // The CDF _change_type shape: one value in every row, non-nullable.
        var bytes = await AssertIdenticalFilesAsync(
            StringRuns(("update_postimage", 10_000)), nullable: false);

        var read = await ReadAsync(bytes, "c");
        var column = (StringArray)read.Column(0);

        Assert.Equal(10_000, column.Length);
        Assert.Equal("update_postimage", column.GetString(0));
        Assert.Equal("update_postimage", column.GetString(9_999));
    }

    [Fact]
    public async Task ConstantColumn_ReadsBackAsItsValueType_NotAsRuns()
    {
        // Nothing in the file records that the writer was handed runs, so the reader cannot reproduce
        // them — the schema it recovers is the plain value type.
        var bytes = await AssertIdenticalFilesAsync(StringRuns(("x", 64)), nullable: false);
        var read = await ReadAsync(bytes, "c");

        Assert.IsType<Apache.Arrow.Types.StringType>(read.Schema.FieldsList[0].DataType);
        Assert.False(read.Schema.FieldsList[0].IsNullable);
    }

    [Theory]
    [InlineData(DataPageVersion.V1)]
    [InlineData(DataPageVersion.V2)]
    public async Task MultiRunColumn_WritesTheSameFileAsTheExpandedForm(DataPageVersion version)
    {
        var options = new ParquetWriteOptions { DataPageVersion = version };

        await AssertIdenticalFilesAsync(
            StringRuns(("alpha", 300), ("beta", 1), ("gamma", 700), ("alpha", 250)),
            nullable: false,
            options);
    }

    [Fact]
    public async Task Int64Runs_WriteTheSameFileAsTheExpandedForm()
    {
        await AssertIdenticalFilesAsync(
            Int64Runs((7L, 500), (-3L, 500), (7L, 1000)), nullable: false);
    }

    // ── Nulls, which do not live where the rest of Arrow puts them ──

    [Fact]
    public async Task NullRuns_BecomeNullRows_NotWhateverSatInTheValueSlot()
    {
        // A run-end encoded array's own IsNull answers false for every row, so a writer that derived
        // definition levels from it would write these rows as present. The file must say otherwise.
        var bytes = await AssertIdenticalFilesAsync(
            StringRuns(("a", 100), (null, 50), ("b", 100), (null, 25)), nullable: true);

        var read = await ReadAsync(bytes, "c");
        var column = (StringArray)read.Column(0);

        Assert.Equal(275, column.Length);
        Assert.Equal(75, column.NullCount);
        Assert.Equal("a", column.GetString(0));
        Assert.True(column.IsNull(100));
        Assert.Equal("b", column.GetString(150));
        Assert.True(column.IsNull(274));
    }

    [Fact]
    public async Task AnEntirelyNullColumn_WritesEveryRowNull()
    {
        var bytes = await AssertIdenticalFilesAsync(StringRuns((null, 500)), nullable: true);

        var read = await ReadAsync(bytes, "c");
        var column = (StringArray)read.Column(0);

        Assert.Equal(500, column.Length);
        Assert.Equal(500, column.NullCount);
    }

    [Fact]
    public async Task TwoRunsSeparatedByNulls_MergeIntoOneIndexRun()
    {
        // The runs are adjacent in the INDEX stream once the null run between them contributes nothing,
        // and the RLE encoder cannot merge what it is handed as two. Byte equality with the expanded form
        // is what catches a failure to merge.
        await AssertIdenticalFilesAsync(
            StringRuns(("a", 40), (null, 40), ("a", 40)), nullable: true);
    }

    // ── Page and row-group boundaries ──

    [Fact]
    public async Task RunsStraddlingPageBoundaries_WriteTheSameFileAsTheExpandedForm()
    {
        // A page size this small puts several page boundaries inside single runs, which is what the run
        // cursor in the page loop exists for.
        var options = new ParquetWriteOptions { DataPageSize = 64 };

        await AssertIdenticalFilesAsync(
            StringRuns(("alpha", 500), ("beta", 500)), nullable: false, options);
    }

    [Fact]
    public async Task RunsStraddlingRowGroupBoundaries_WriteTheSameFileAsTheExpandedForm()
    {
        // Auto-split slices every column; a run-encoded one splits by run rather than by row.
        var options = new ParquetWriteOptions { RowGroupMaxRows = 100 };

        await AssertIdenticalFilesAsync(
            StringRuns(("alpha", 250), ("beta", 250)), nullable: false, options);
    }

    // ── Paths that decline the run-aware route ──

    [Fact]
    public async Task WithTheDictionaryDisabled_TheColumnIsExpandedAndWrittenPlain()
    {
        var options = new ParquetWriteOptions { DictionaryEnabled = false };

        await AssertIdenticalFilesAsync(StringRuns(("a", 100), ("b", 100)), nullable: false, options);
    }

    [Fact]
    public async Task HighCardinalityRuns_FallBackToTheExpandedForm()
    {
        // One row per run and every value distinct: past the cardinality threshold, so the dictionary
        // declines and the column takes the expansion path.
        var runs = new (string?, int)[200];
        for (int i = 0; i < runs.Length; i++)
            runs[i] = ($"value-{i}", 1);

        await AssertIdenticalFilesAsync(StringRuns(runs), nullable: false);
    }

    [Fact]
    public async Task DoubleRuns_AreExpandedForTheNanScanAndStillMatch()
    {
        // FLOAT/DOUBLE columns are always full-scanned for the NaN count, which has no run-aware form —
        // such a column is expanded up front and takes the plain path in its entirety.
        var values = new DoubleArray.Builder().Append(1.5).Append(double.NaN).Build();
        var ends = new Int32Array.Builder().Append(400).Append(600).Build();
        var plain = new DoubleArray.Builder();
        for (int i = 0; i < 400; i++) plain.Append(1.5);
        for (int i = 0; i < 200; i++) plain.Append(double.NaN);

        await AssertIdenticalFilesAsync(
            (new RunEndEncodedArray(ends, values), plain.Build()), nullable: false);
    }

    // ── Value types that are normalized before encoding ──

    [Fact]
    public async Task Int16Runs_AreWidenedThroughTheValuesChild()
    {
        // Int16 is written as the 4-byte INT32 physical type. The widening has to reach the VALUES child;
        // applied to the runs it would reinterpret run ends as data.
        var values = new Int16Array.Builder().Append((short)-7).Append((short)9).Build();
        var ends = new Int32Array.Builder().Append(50).Append(120).Build();
        var plain = new Int16Array.Builder();
        for (int i = 0; i < 50; i++) plain.Append((short)-7);
        for (int i = 0; i < 70; i++) plain.Append((short)9);

        var bytes = await AssertIdenticalFilesAsync(
            (new RunEndEncodedArray(ends, values), plain.Build()), nullable: false);

        var read = await ReadAsync(bytes, "c");
        var column = (Int16Array)read.Column(0);
        Assert.Equal((short)-7, column.GetValue(0));
        Assert.Equal((short)9, column.GetValue(119));
    }

    [Fact]
    public async Task Decimal128Runs_HaveTheirBytesReversedThroughTheValuesChild()
    {
        // Precision above 18 so the reader hands back a Decimal128 rather than narrowing to Decimal64.
        var type = new Decimal128Type(25, 4);
        var values = new Decimal128Array.Builder(type).Append(12.3456m).Append(-99.9999m).Build();
        var ends = new Int32Array.Builder().Append(60).Append(150).Build();

        var plain = new Decimal128Array.Builder(type);
        for (int i = 0; i < 60; i++) plain.Append(12.3456m);
        for (int i = 0; i < 90; i++) plain.Append(-99.9999m);

        var bytes = await AssertIdenticalFilesAsync(
            (new RunEndEncodedArray(ends, values), plain.Build()), nullable: false);

        var read = await ReadAsync(bytes, "c");
        var column = (Decimal128Array)read.Column(0);
        Assert.Equal(12.3456m, column.GetValue(0));
        Assert.Equal(-99.9999m, column.GetValue(149));
    }

    // ── Everything the writer derives from the column ──

    [Fact]
    public async Task StatisticsAndBloomFilter_MatchTheExpandedForm()
    {
        var options = new ParquetWriteOptions { BloomFilterColumns = ["c"] };

        await AssertIdenticalFilesAsync(
            StringRuns(("mango", 200), ("apple", 300), (null, 100), ("zebra", 200)),
            nullable: true,
            options);
    }

    [Fact]
    public async Task ASingleRowColumn_WritesTheSameFileAsTheExpandedForm()
    {
        await AssertIdenticalFilesAsync(StringRuns(("only", 1)), nullable: false);
    }
}
