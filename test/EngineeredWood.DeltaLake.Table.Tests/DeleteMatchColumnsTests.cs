// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.Table;
using EngineeredWood.Expressions;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A predicate DELETE finds its rows by reading only the columns the predicate references (#713). Before, it read
/// every column to match, and a copy-on-write delete then read each file it partly matched again, whole, to
/// rewrite it: every byte of it twice. Every table here is ONE file of 600 rows, in row groups of 200: an
/// <c>id</c>, a <c>tag</c> of "a" or "c" by row parity (so neither statistics, dictionaries nor page indexes
/// can narrow <c>tag = 'a'</c>), and a large <c>payload</c>. Outcomes are checked against a twin deleted through
/// the functional overload, which reads every column.
/// </summary>
public class DeleteMatchColumnsTests : IDisposable
{
    private const int Rows = 600;

    private readonly string _tempDir;

    public DeleteMatchColumnsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_delmc_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema TableSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("tag", StringType.Default, false))
        .Field(new Field("payload", StringType.Default, false))
        .Build();

    private static readonly Apache.Arrow.Schema PartitionedSchema = new Apache.Arrow.Schema.Builder()
        .Field(new Field("part", StringType.Default, false))
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("tag", StringType.Default, false))
        .Field(new Field("payload", StringType.Default, false))
        .Build();

    private static readonly Predicate TagA = Ex.Equal("tag", "a");

    private static DeltaTableOptions Options() => new()
    {
        ParquetWriteOptions = ParquetWriteOptions.Default with { RowGroupMaxRows = 200 },
    };

    private async Task<(DeltaTable Table, DataFileCountingFileSystem Fs)> CreateAsync(
        string name, bool dv, ColumnMappingMode mode = ColumnMappingMode.None, bool partitioned = false)
    {
        var fs = new DataFileCountingFileSystem(new LocalTableFileSystem(Path.Combine(_tempDir, name)));
        var table = await DeltaTable.CreateAsync(
            fs, partitioned ? PartitionedSchema : TableSchema, Options(),
            partitionColumns: partitioned ? ["part"] : null, columnMappingMode: mode, enableDeletionVectors: dv);

        var ids = new Int64Array.Builder();
        var tags = new StringArray.Builder();
        var payloads = new StringArray.Builder();
        for (int i = 0; i < Rows; i++)
        {
            ids.Append(i);
            tags.Append(i % 2 == 0 ? "a" : "c");
            payloads.Append(string.Concat(Enumerable.Range(0, 6).Select(_ => Guid.NewGuid().ToString("N"))));
        }
        IArrowArray[] columns = partitioned
            ? [new StringArray.Builder().AppendRange(Enumerable.Repeat("p", Rows)).Build(), ids.Build(), tags.Build(), payloads.Build()]
            : [ids.Build(), tags.Build(), payloads.Build()];
        await table.WriteAsync([new RecordBatch(partitioned ? PartitionedSchema : TableSchema, columns, Rows)]);
        Assert.Single(table.CurrentSnapshot.ActiveFiles);
        return (table, fs);
    }

    /// <summary>The byte spans of <paramref name="column"/>'s chunks in the table's one data file, and its
    /// length.</summary>
    private static async Task<(string Path, List<(long Start, long End)> Spans, long Length)> ChunkSpansAsync(
        DeltaTable table, string root, string column)
    {
        var path = Assert.Single(table.CurrentSnapshot.ActiveFiles.Values).Path;
        await using var file = await new LocalTableFileSystem(root).OpenReadAsync(DeltaPath.Decode(path));
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var footer = await reader.ReadMetadataAsync();
        var spans = footer.RowGroups
            .SelectMany(g => g.Columns.Select(c => c.MetaData!))
            .Where(m => m.PathInSchema![0] == column)
            .Select(m =>
            {
                long start = m.DictionaryPageOffset is long d && d > 0 && d < m.DataPageOffset ? d : m.DataPageOffset;
                return (start, start + m.TotalCompressedSize);
            })
            .ToList();
        Assert.NotEmpty(spans);
        return (path, spans, await file.GetLengthAsync());
    }

    /// <summary>The bytes of <paramref name="spans"/> the recorded reads of <paramref name="path"/> covered,
    /// counting a byte once per read that covered it. The footer read is left out: it reads the file's tail
    /// speculatively, and the tail can reach back into the last column chunk.</summary>
    private static long BytesReadWithin(
        DataFileCountingFileSystem fs, string path, List<(long Start, long End)> spans, long length) =>
        fs.DataReads
            .Where(r => r.Path == DeltaPath.Decode(path) && r.Range.End != length)
            .Sum(r => spans.Sum(s => Math.Max(0, Math.Min(s.End, r.Range.End) - Math.Max(s.Start, r.Range.Offset))));

    private static async Task<List<long>> IdsAsync(DeltaTable table)
    {
        var ids = new List<long>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions()))
        {
            var column = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(column.GetValue(i)!.Value);
            batch.Dispose();
        }
        ids.Sort();
        return ids;
    }

    /// <summary>
    /// Copy-on-write: the match reads <c>tag</c> alone, so the rewrite is the only read of <c>payload</c>.
    /// Matching on every column read all of <c>payload</c> twice.
    /// </summary>
    [Fact]
    public async Task CopyOnWriteDelete_ReadsTheColumnsOutsideThePredicateOnce()
    {
        var root = Path.Combine(_tempDir, "cow");
        var (table, fs) = await CreateAsync("cow", dv: false);
        await using var _ = table;
        var (path, payload, length) = await ChunkSpansAsync(table, root, "payload");
        long payloadBytes = payload.Sum(s => s.End - s.Start);

        fs.Reset();
        var (deleted, _) = await table.DeleteAsync(TagA);

        Assert.Equal(Rows / 2, deleted);
        long read = BytesReadWithin(fs, path, payload, length);
        Assert.True(read > 0, "the rewrite never read the payload");
        Assert.True(read < payloadBytes * 3 / 2, $"payload is {payloadBytes} bytes; the delete read {read} of them");
        Assert.Equal(Enumerable.Range(0, Rows).Where(i => i % 2 == 1).Select(i => (long)i), await IdsAsync(table));
    }

    /// <summary>A deletion-vector delete rewrites nothing, so it never reads a column outside its predicate.</summary>
    [Fact]
    public async Task DeletionVectorDelete_NeverReadsTheColumnsOutsideThePredicate()
    {
        var root = Path.Combine(_tempDir, "dv");
        var (table, fs) = await CreateAsync("dv", dv: true);
        await using var _ = table;
        var (path, payload, length) = await ChunkSpansAsync(table, root, "payload");

        fs.Reset();
        var (deleted, _) = await table.DeleteAsync(TagA);

        Assert.Equal(Rows / 2, deleted);
        // The one exception: a dictionary-encoded chunk is read with 100 bytes to spare past its end, in case its
        // writer left the dictionary page header out of its size (PARQUET-816), and payload follows tag.
        long read = BytesReadWithin(fs, path, payload, length);
        Assert.True(read <= 100 * payload.Count,
            $"payload is {payload.Sum(s => s.End - s.Start)} bytes; the delete read {read} of them");
        Assert.NotNull(Assert.Single(table.CurrentSnapshot.ActiveFiles.Values).DeletionVector);
        Assert.Equal(Enumerable.Range(0, Rows).Where(i => i % 2 == 1).Select(i => (long)i), await IdsAsync(table));
    }

    public static TheoryData<ColumnMappingMode, bool, bool> Shapes()
    {
        var data = new TheoryData<ColumnMappingMode, bool, bool>();
        foreach (var mode in new[] { ColumnMappingMode.None, ColumnMappingMode.Name, ColumnMappingMode.Id })
        {
            foreach (bool dv in new[] { false, true })
            {
                foreach (bool partitioned in new[] { false, true })
                    data.Add(mode, dv, partitioned);
            }
        }
        return data;
    }

    /// <summary>
    /// The narrowed match deletes what the whole-row match does, under every column mapping, with and without
    /// deletion vectors and partitions: a reference spelled in another case (<c>TAG</c>), resolved as the
    /// evaluator resolves it, and, on a partitioned table, a partition column beside a data column.
    /// </summary>
    [Theory]
    [MemberData(nameof(Shapes))]
    public async Task NarrowedMatch_DeletesWhatTheWholeRowMatchDoes(ColumnMappingMode mode, bool dv, bool partitioned)
    {
        var predicate = partitioned
            ? Ex.And(Ex.Equal("part", "p"), Ex.Equal("TAG", "a"), Ex.LessThan("id", 450L))
            : Ex.And(Ex.Equal("TAG", "a"), Ex.LessThan("id", 450L));
        var (narrowed, _) = await CreateAsync("narrowed", dv, mode, partitioned);
        var (oracle, _) = await CreateAsync("oracle", dv, mode, partitioned);
        await using var __ = narrowed;
        await using var ___ = oracle;

        var (narrowedDeleted, _) = await narrowed.DeleteAsync(predicate);
        var (oracleDeleted, _) = await oracle.DeleteAsync(DeltaTable.MaskFor(predicate));

        Assert.Equal(225, narrowedDeleted);
        Assert.Equal(oracleDeleted, narrowedDeleted);
        Assert.Equal(await IdsAsync(oracle), await IdsAsync(narrowed));
    }

    /// <summary>
    /// A column added after the file was written is not in it: the match reads what the file has and the read
    /// backfills the rest as NULL, as a scan does, so <c>extra IS NULL</c> holds for every old row.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task NarrowedMatch_OnAColumnAddedAfterTheFile_SeesItAsNull(bool dv)
    {
        var predicate = Ex.And(Ex.IsNull("extra"), TagA);
        var (narrowed, _) = await CreateAsync("narrowed", dv, ColumnMappingMode.Name);
        var (oracle, _) = await CreateAsync("oracle", dv, ColumnMappingMode.Name);
        await using var __ = narrowed;
        await using var ___ = oracle;
        foreach (var table in new[] { narrowed, oracle })
            await table.AddColumnAsync(new Field("extra", Int32Type.Default, true));

        var (narrowedDeleted, _) = await narrowed.DeleteAsync(predicate);
        var (oracleDeleted, _) = await oracle.DeleteAsync(DeltaTable.MaskFor(predicate));

        Assert.Equal(Rows / 2, narrowedDeleted);
        Assert.Equal(oracleDeleted, narrowedDeleted);
        Assert.Equal(await IdsAsync(oracle), await IdsAsync(narrowed));
    }

    /// <summary>
    /// A top-level column literally named <c>s.a</c> beside a struct <c>s</c> with a field <c>a</c>, in either
    /// order, without column mapping (so the file's names are the table's). A projected read of <c>s.a</c>, and a
    /// DELETE on it, must take the top-level column, not the struct's leaf, whose dotted path is the same text.
    /// Struct leaf values are 0..599, the top-level column's 1000..1599.
    /// </summary>
    [Theory]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public async Task ATopLevelColumnNamedLikeAStructField_IsTheColumnRead(bool structFirst, bool dv)
    {
        var structField = new Field("s", new Apache.Arrow.Types.StructType([new Field("a", Int64Type.Default, false)]), false);
        var dotted = new Field("s.a", Int64Type.Default, false);
        var id = new Field("id", Int64Type.Default, false);
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(id).Field(structFirst ? structField : dotted).Field(structFirst ? dotted : structField).Build();

        async Task<DeltaTable> CreateDottedAsync(string name)
        {
            var table = await DeltaTable.CreateAsync(
                new LocalTableFileSystem(Path.Combine(_tempDir, name)), schema, Options(), enableDeletionVectors: dv);
            var ids = new Int64Array.Builder();
            var leaves = new Int64Array.Builder();
            var literal = new Int64Array.Builder();
            for (int i = 0; i < Rows; i++)
            {
                ids.Append(i);
                leaves.Append(i);
                literal.Append(1000 + i);
            }
            var s = new StructArray(structField.DataType, Rows, [leaves.Build()], ArrowBuffer.Empty, 0);
            var dottedArray = literal.Build();
            await table.WriteAsync([new RecordBatch(schema,
                [ids.Build(), structFirst ? s : dottedArray, structFirst ? dottedArray : s], Rows)]);
            return table;
        }

        await using var narrowed = await CreateDottedAsync("narrowed");
        await using var oracle = await CreateDottedAsync("oracle");

        // The projected scan returns the top-level column's values.
        var projected = new List<long>();
        await foreach (var batch in narrowed.ReadAsync(new DeltaReadOptions { Columns = ["s.a"] }))
        {
            var column = (Int64Array)batch.Column("s.a");
            for (int i = 0; i < batch.Length; i++)
                projected.Add(column.GetValue(i)!.Value);
            batch.Dispose();
        }
        Assert.Equal(Enumerable.Range(1000, Rows).Select(v => (long)v), projected.OrderBy(v => v));

        // Ex.Equal("s.a", ...) is a one-part reference: the top-level column. Row 5 holds 1005 there.
        var predicate = Ex.Equal("s.a", 1005L);
        var (narrowedDeleted, _) = await narrowed.DeleteAsync(predicate);
        var (oracleDeleted, _) = await oracle.DeleteAsync(DeltaTable.MaskFor(predicate));

        Assert.Equal(1, oracleDeleted);
        Assert.Equal(oracleDeleted, narrowedDeleted);
        Assert.Equal(await IdsAsync(oracle), await IdsAsync(narrowed));
    }

    [Fact]
    public void PredicateColumns_AreTheTopLevelColumnsReferenced_InSchemaOrder()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("s", new Apache.Arrow.Types.StructType([new Field("a", Int32Type.Default, true)]), true))
            .Field(new Field("tag", StringType.Default, true))
            .Build();

        // Case-insensitive, deduplicated, in schema order; a struct field needs its whole column.
        var predicate = Ex.And(
            Ex.Equal("TAG", "a"),
            Ex.Equal(UnboundReference.FromParts(["S", "a"]), new LiteralExpression(LiteralValue.Of(1))),
            Ex.IsNull("tag"));
        Assert.Equal(["s", "tag"], DeltaTable.PredicateColumns(predicate, schema));

        // A function's arguments count.
        var call = Ex.Equal(new FunctionCall("lower", [Ex.Ref("tag")]), new LiteralExpression(LiteralValue.Of("a")));
        Assert.Equal(["tag"], DeltaTable.PredicateColumns(call, schema));

        // Nothing to narrow to, or a reference this cannot place: read every column.
        Assert.Null(DeltaTable.PredicateColumns(Ex.Equal(new LiteralExpression(LiteralValue.Of(1)),
            new LiteralExpression(LiteralValue.Of(1))), schema));
        Assert.Null(DeltaTable.PredicateColumns(Ex.Equal("nope", 1L), schema));
    }
}
