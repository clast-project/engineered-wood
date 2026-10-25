// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Delta <c>string</c> and <c>binary</c> accept Arrow's view layouts, and a write converts them to the plain
/// forms the table reads back as (#515). The partition split formats only the plain forms, so a StringView
/// partition column could not be written at all; a view data column wrote, but under its own
/// <c>ARROW:schema</c> type, so the files of one column disagreed about it.
/// </summary>
public class StringViewWriteTests : IDisposable
{
    private readonly string _tempDir;

    public StringViewWriteTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_strview_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema Declared = new Apache.Arrow.Schema.Builder()
        .Field(new Field("part", StringType.Default, true))
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("v", StringType.Default, true))
        .Build();

    private static readonly Apache.Arrow.Schema ViewSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("part", StringViewType.Default, true))
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("v", StringViewType.Default, true))
        .Build();

    /// <summary>A value longer than 12 bytes, so a view holds it out of line rather than inside the view.</summary>
    private const string Long = "a value too long to be held inline";

    /// <summary>Rows (part, id, v): an empty partition value and a null one, which are stored alike.</summary>
    private static RecordBatch ViewRows(long firstId = 1)
    {
        var parts = new StringViewArray.Builder().Append("a").Append("").AppendNull().Append(Long).Build();
        var ids = new Int64Array.Builder().AppendRange([firstId, firstId + 1, firstId + 2, firstId + 3]).Build();
        var values = new StringViewArray.Builder().Append(Long).AppendNull().Append("x").Append("").Build();
        return new RecordBatch(ViewSchema, [parts, ids, values], 4);
    }

    private ValueTask<DeltaTable> CreateAsync(DeltaTableOptions? options = null) => DeltaTable.CreateAsync(
        new LocalTableFileSystem(_tempDir), Declared, options ?? DeltaTableOptions.Default, partitionColumns: ["part"]);

    private static async Task<List<string>> RowsAsync(DeltaTable table)
    {
        var rows = new List<string>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var part = Assert.IsType<StringArray>(batch.Column(batch.Schema.GetFieldIndex("part")));
            var id = (Int64Array)batch.Column(batch.Schema.GetFieldIndex("id"));
            var v = Assert.IsType<StringArray>(batch.Column(batch.Schema.GetFieldIndex("v")));
            for (int i = 0; i < batch.Length; i++)
            {
                rows.Add($"{id.GetValue(i)}|{(part.IsNull(i) ? "null" : part.GetString(i))}|"
                    + (v.IsNull(i) ? "null" : v.GetString(i)));
            }
        }
        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private static List<string?> PartitionValues(DeltaTable table) => table.CurrentSnapshot.ActiveFiles.Values
        .Select(f => f.PartitionValues["part"])
        .OrderBy(p => p, StringComparer.Ordinal)
        .ToList();

    [Fact]
    public async Task Write_StringViewPartition_IsSplitByItsValues()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([ViewRows()]);

        Assert.Equal([null, "a", Long], PartitionValues(table));
        Assert.Equal(
            ["1|a|" + Long, "2|null|null", "3|null|x", "4|" + Long + "|"],
            await RowsAsync(table));
    }

    [Fact]
    public async Task Write_SlicedStringViewPartition_KeepsTheSlicesRows()
    {
        await using var table = await CreateAsync();
        await table.WriteAsync([ViewRows().Slice(2, 2)]);

        Assert.Equal([null, Long], PartitionValues(table));
        Assert.Equal(["3|null|x", "4|" + Long + "|"], await RowsAsync(table));
    }

    [Fact]
    public async Task WriteDataFiles_StringViewPartition_IsSplitByItsValues()
    {
        await using var table = await CreateAsync();
        await table.CommitDataFilesAsync(await table.WriteDataFilesAsync([ViewRows()]));

        Assert.Equal([null, "a", Long], PartitionValues(table));
        Assert.Equal(
            ["1|a|" + Long, "2|null|null", "3|null|x", "4|" + Long + "|"],
            await RowsAsync(table));
    }

    [Fact]
    public async Task Transaction_StringViewPartition_IsSplitByItsValues()
    {
        await using var table = await CreateAsync();
        await using (var transaction = table.StartTransaction())
        {
            await transaction.WriteAsync([ViewRows()]);
            await transaction.CommitAsync();
        }

        Assert.Equal([null, "a", Long], PartitionValues(table));
    }

    /// <summary>
    /// An UPDATE's post-image is held to the write path's partition rule, so an empty string set through string
    /// views into a NOT NULL partition column is refused as the plain-string one is.
    /// </summary>
    [Fact]
    public async Task Update_EmptyStringViewInNonNullablePartition_IsRefused()
    {
        var declared = new Apache.Arrow.Schema.Builder()
            .Field(new Field("part", StringType.Default, false))
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), declared, partitionColumns: ["part"]);
        await table.WriteAsync([new RecordBatch(declared,
            [new StringArray.Builder().Append("a").Build(), new Int64Array.Builder().Append(1).Build()], 1)]);
        long version = table.CurrentSnapshot.Version;

        await Assert.ThrowsAsync<ArgumentException>(() => table.UpdateAsync(
            Expressions.Expressions.Equal("id", 1L),
            batch =>
            {
                var parts = new StringViewArray.Builder();
                for (int i = 0; i < batch.Length; i++)
                    parts.Append("");
                int index = batch.Schema.GetFieldIndex("part");
                var fields = batch.Schema.FieldsList.ToArray();
                fields[index] = new Field("part", StringViewType.Default, false);
                var columns = batch.Arrays.ToArray();
                columns[index] = parts.Build();
                return new RecordBatch(new Apache.Arrow.Schema(fields, null), columns, batch.Length);
            }).AsTask());
        Assert.Equal(version, table.CurrentSnapshot.Version);
    }

    /// <summary>Under a host writer the data columns are the host's to encode, but the partition values are the
    /// library's to format, so only those are converted.</summary>
    [Fact]
    public async Task HostWriter_StringViewPartition_IsConvertedAndTheDataColumnIsLeftAlone()
    {
        var writer = new CapturingWriter();
        await using var table = await CreateAsync(DeltaTableOptions.Default with { DataFileWriter = writer });
        await table.WriteAsync([ViewRows()]);

        Assert.Equal([null, "a", Long], PartitionValues(table));
        Assert.NotEmpty(writer.Received);
        Assert.All(writer.Received, b => Assert.IsType<StringViewArray>(b.Column(b.Schema.GetFieldIndex("v"))));
    }

    private sealed class CapturingWriter : IDataFileWriter
    {
        public List<RecordBatch> Received { get; } = [];

        public async ValueTask<long> WriteAsync(
            IAsyncEnumerable<RecordBatch> batches, string relativePath, CancellationToken cancellationToken)
        {
            await foreach (var b in batches.WithCancellation(cancellationToken))
                Received.Add(b);
            return 4096;
        }
    }

    [Fact]
    public void Normalize_StringView_BecomesAStringArrayOfTheSameValues()
    {
        var batch = ViewRows().Slice(1, 3);
        var normalized = WriteTypeNormalization.Normalize(batch);

        Assert.IsType<StringType>(normalized.Schema.GetFieldByName("v").DataType);
        var v = Assert.IsType<StringArray>(normalized.Column(normalized.Schema.GetFieldIndex("v")));
        Assert.Equal((0, 3, 1), (v.Offset, v.Length, v.NullCount));
        Assert.True(v.IsNull(0));
        Assert.Equal("x", v.GetString(1));
        Assert.Equal("", v.GetString(2));

        var part = Assert.IsType<StringArray>(normalized.Column(0));
        Assert.Equal(["", null, Long], Enumerable.Range(0, 3).Select(i => part.IsNull(i) ? null : part.GetString(i)));
    }

    [Fact]
    public void Normalize_BinaryView_BecomesABinaryArrayOfTheSameValues()
    {
        byte[] longBytes = Enumerable.Range(0, 40).Select(i => (byte)i).ToArray();
        var views = new BinaryViewArray.Builder().Append([1, 2]).AppendNull().Append(longBytes).Build();
        var batch = new RecordBatch(
            new Apache.Arrow.Schema([new Field("b", BinaryViewType.Default, true)], null), [views], 3);

        var normalized = WriteTypeNormalization.Normalize(batch);

        Assert.IsType<BinaryType>(normalized.Schema.FieldsList[0].DataType);
        var b = Assert.IsType<BinaryArray>(normalized.Column(0));
        Assert.Equal(new byte[] { 1, 2 }, b.GetBytes(0).ToArray());
        Assert.True(b.IsNull(1));
        Assert.Equal(longBytes, b.GetBytes(2).ToArray());
    }

    [Fact]
    public void Normalize_StringViewInsideAStruct_IsConverted()
    {
        var child = new StringViewArray.Builder().Append(Long).AppendNull().Build();
        var structType = new Apache.Arrow.Types.StructType([new Field("s", StringViewType.Default, true)]);
        var structArray = new StructArray(structType, 2, [child], ArrowBuffer.Empty, 0);
        var batch = new RecordBatch(new Apache.Arrow.Schema([new Field("st", structType, true)], null), [structArray], 2);

        var normalized = WriteTypeNormalization.Normalize(batch);

        var converted = Assert.IsType<StructArray>(normalized.Column(0));
        var s = Assert.IsType<StringArray>(converted.Fields[0]);
        Assert.Equal(Long, s.GetString(0));
        Assert.True(s.IsNull(1));
    }
}
