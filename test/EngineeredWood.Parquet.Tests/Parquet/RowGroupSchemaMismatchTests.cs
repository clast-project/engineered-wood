// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using ArrowSchema = Apache.Arrow.Schema;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// #457: a Parquet file has one schema, fixed by the first batch. A later batch whose shape differs must be refused,
/// not encoded against the first batch's column types (an Int64 5e9 used to come back as Int32 705032704).
/// </summary>
public class RowGroupSchemaMismatchTests : IDisposable
{
    private readonly string _tempDir;

    public RowGroupSchemaMismatchTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-rgschema-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    public enum Writer { File, Buffered }

    private static ArrowSchema SchemaOf(params Field[] fields)
    {
        var b = new ArrowSchema.Builder();
        foreach (var f in fields)
            b.Field(f);
        return b.Build();
    }

    private static IArrowArray ArrayOfNulls(IArrowType type, int length)
    {
        var data = new ArrayData(type, length, length,
            0, [new ArrowBuffer(new byte[(length + 7) / 8]), .. ValueBuffers(type, length)],
            Children(type, length));
        return ArrowArrayFactory.BuildArray(data);
    }

    private static ArrowBuffer[] ValueBuffers(IArrowType type, int length) => type switch
    {
        StructType => [],
        ListType or MapType => [new ArrowBuffer(new byte[(length + 1) * 4])],
        StringType or BinaryType => [new ArrowBuffer(new byte[(length + 1) * 4]), ArrowBuffer.Empty],
        FixedWidthType fw => [new ArrowBuffer(new byte[Math.Max(1, (fw.BitWidth * length + 7) / 8)])],
        _ => throw new NotSupportedException(type.Name),
    };

    private static ArrayData[]? Children(IArrowType type, int length) => type switch
    {
        StructType st => st.Fields.Select(f => ArrayOfNulls(f.DataType, length).Data).ToArray(),
        ListType lt => [ArrayOfNulls(lt.ValueDataType, 0).Data],
        MapType mt => [ArrayOfNulls(new StructType([mt.KeyField, mt.ValueField]), 0).Data],
        _ => null,
    };

    private static RecordBatch BatchOf(ArrowSchema schema, int length = 2) =>
        new(schema, schema.FieldsList.Select(f => ArrayOfNulls(f.DataType, length)), length);

    private async Task WriteAsync(Writer kind, string path, params RecordBatch[] batches)
    {
        await using var file = new LocalSequentialFile(path);
        if (kind == Writer.File)
        {
            await using var w = new ParquetFileWriter(file, ownsFile: false);
            foreach (var b in batches)
                await w.WriteRowGroupAsync(b);
            await w.CloseAsync();
        }
        else
        {
            await using var w = new BufferedParquetWriter(file, ownsFile: false);
            foreach (var b in batches)
                await w.AppendAsync(b);
            await w.CloseAsync();
        }
    }

    private async Task<ArgumentException> RefusedAsync(Writer kind, ArrowSchema first, ArrowSchema second)
    {
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N") + ".parquet");
        return await Assert.ThrowsAsync<ArgumentException>(
            () => WriteAsync(kind, path, BatchOf(first), BatchOf(second)));
    }

    // The issue's repro, with real values: the second batch must not be truncated into the first one's Int32.
    [Theory]
    [InlineData(Writer.File)]
    [InlineData(Writer.Buffered)]
    public async Task Int64AfterInt32_IsRefused(Writer kind)
    {
        var s32 = SchemaOf(new Field("a", Int32Type.Default, true));
        var s64 = SchemaOf(new Field("a", Int64Type.Default, true));
        string path = Path.Combine(_tempDir, "narrow.parquet");

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => WriteAsync(kind, path,
            new RecordBatch(s32, [new Int32Array.Builder().Append(1).Build()], 1),
            new RecordBatch(s64, [new Int64Array.Builder().Append(5_000_000_000L).Build()], 1)));

        Assert.Contains("'a'", ex.Message);
    }

    public static TheoryData<Writer, string, string> Mismatches()
    {
        var data = new TheoryData<Writer, string, string>();
        foreach (var w in new[] { Writer.File, Writer.Buffered })
        {
            data.Add(w, "column count", "b");
            data.Add(w, "column order", "a");
            data.Add(w, "column name", "c");
            data.Add(w, "decimal scale", "d");
            data.Add(w, "timestamp unit", "t");
        }
        // BufferedParquetWriter takes flat columns only, so the nested cases are ParquetFileWriter's.
        data.Add(Writer.File, "struct child type", "s.x");
        data.Add(Writer.File, "struct child name", "s.y");
        data.Add(Writer.File, "list element type", "l.element");
        data.Add(Writer.File, "map value type", "m.value");
        return data;
    }

    [Theory]
    [MemberData(nameof(Mismatches))]
    public async Task ShapeMismatch_IsRefused_NamingThePath(Writer kind, string mismatch, string path)
    {
        var a = new Field("a", Int32Type.Default, true);
        var b = new Field("b", StringType.Default, true);
        (ArrowSchema first, ArrowSchema second) = mismatch switch
        {
            "column count" => (SchemaOf(a, b), SchemaOf(a)),
            "column order" => (SchemaOf(a, b), SchemaOf(b, a)),
            "column name" => (SchemaOf(a), SchemaOf(new Field("c", Int32Type.Default, true))),
            "decimal scale" => (SchemaOf(new Field("d", new Decimal128Type(10, 2), true)),
                                SchemaOf(new Field("d", new Decimal128Type(10, 3), true))),
            "timestamp unit" => (SchemaOf(new Field("t", new TimestampType(TimeUnit.Microsecond, "UTC"), true)),
                                 SchemaOf(new Field("t", new TimestampType(TimeUnit.Nanosecond, "UTC"), true))),
            "struct child type" => (
                SchemaOf(new Field("s", new StructType([new Field("x", Int32Type.Default, true)]), true)),
                SchemaOf(new Field("s", new StructType([new Field("x", Int64Type.Default, true)]), true))),
            "struct child name" => (
                SchemaOf(new Field("s", new StructType([new Field("x", Int32Type.Default, true)]), true)),
                SchemaOf(new Field("s", new StructType([new Field("x", Int32Type.Default, true),
                                                         new Field("y", Int32Type.Default, true)]), true))),
            "list element type" => (
                SchemaOf(new Field("l", new ListType(new Field("element", Int32Type.Default, true)), true)),
                SchemaOf(new Field("l", new ListType(new Field("element", Int64Type.Default, true)), true))),
            "map value type" => (
                SchemaOf(new Field("m", new MapType(new Field("key", StringType.Default, false),
                                                    new Field("value", Int32Type.Default, true)), true)),
                SchemaOf(new Field("m", new MapType(new Field("key", StringType.Default, false),
                                                    new Field("value", Int64Type.Default, true)), true))),
            _ => throw new ArgumentOutOfRangeException(nameof(mismatch)),
        };

        var ex = await RefusedAsync(kind, first, second);

        Assert.Contains($"'{path}'", ex.Message);
    }

    // Differences that do not change what is encoded are accepted: field metadata, a timestamp's zone name, and a
    // nullable flag on a column with no nulls in it.
    [Theory]
    [InlineData(Writer.File)]
    [InlineData(Writer.Buffered)]
    public async Task MetadataZoneNameAndNullFreeNullableFlag_AreAccepted(Writer kind)
    {
        var first = SchemaOf(
            new Field("a", Int32Type.Default, true, [new("k", "1")]),
            new Field("t", new TimestampType(TimeUnit.Microsecond, "UTC"), true),
            new Field("r", Int32Type.Default, false));
        var second = SchemaOf(
            new Field("a", Int32Type.Default, true, [new("k", "2")]),
            new Field("t", new TimestampType(TimeUnit.Microsecond, "+00:00"), true),
            new Field("r", Int32Type.Default, true));
        string path = Path.Combine(_tempDir, "ok.parquet");

        RecordBatch Batch(ArrowSchema schema, int r0, int r1) => new(schema,
            [ArrayOfNulls(Int32Type.Default, 2), ArrayOfNulls(schema.FieldsList[1].DataType, 2),
             new Int32Array.Builder().Append(r0).Append(r1).Build()], 2);

        await WriteAsync(kind, path, Batch(first, 1, 2), Batch(second, 3, 4));

        int[] r = await ReadColumnAsync(path, "r");
        Assert.Equal(new[] { 1, 2, 3, 4 }, r);
    }

    // Parquet writes its own names for list and map children, so Arrow's may differ.
    [Fact]
    public async Task ListChildName_IsAccepted()
    {
        var first = SchemaOf(new Field("l", new ListType(new Field("element", Int32Type.Default, true)), true));
        var second = SchemaOf(new Field("l", new ListType(new Field("item", Int32Type.Default, true)), true));

        await WriteAsync(Writer.File, Path.Combine(_tempDir, "list.parquet"), BatchOf(first), BatchOf(second));
    }

    // A column the first batch made required cannot hold a later batch's nulls: both writers used to encode each
    // null as 0.
    [Theory]
    [InlineData(Writer.File)]
    [InlineData(Writer.Buffered)]
    public async Task NullsInAColumnTheFileMadeRequired_AreRefused(Writer kind)
    {
        var required = SchemaOf(new Field("a", Int32Type.Default, false));
        var nullable = SchemaOf(new Field("a", Int32Type.Default, true));

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => WriteAsync(kind,
            Path.Combine(_tempDir, "nulls.parquet"),
            new RecordBatch(required, [new Int32Array.Builder().Append(1).Build()], 1),
            new RecordBatch(nullable, [new Int32Array.Builder().Append(3).AppendNull().Build()], 2)));

        Assert.Contains("'a'", ex.Message);
        Assert.Contains("required", ex.Message);
    }

    // The same inside a struct.
    [Fact]
    public async Task NullsInARequiredStructChild_AreRefused()
    {
        static ArrowSchema Of(bool childNullable) => SchemaOf(new Field("s",
            new StructType([new Field("x", Int32Type.Default, childNullable)]), true));
        static RecordBatch Batch(ArrowSchema schema, Int32Array child) => new(schema,
            [new StructArray(schema.FieldsList[0].DataType, child.Length, [child],
                ArrowBuffer.Empty, nullCount: 0)], child.Length);

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => WriteAsync(Writer.File,
            Path.Combine(_tempDir, "struct-nulls.parquet"),
            Batch(Of(childNullable: false), new Int32Array.Builder().Append(1).Build()),
            Batch(Of(childNullable: true), new Int32Array.Builder().Append(2).AppendNull().Build())));

        Assert.Contains("'s.x'", ex.Message);
    }

    private static async Task<int[]> ReadColumnAsync(string path, string column)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var values = new List<int>();
        for (int g = 0; g < metadata.RowGroups.Count; g++)
        {
            var batch = await reader.ReadRowGroupAsync(g);
            var array = (Int32Array)batch.Column(batch.Schema.GetFieldIndex(column));
            for (int i = 0; i < array.Length; i++)
                values.Add(array.GetValue(i) ?? -1);
        }
        return values.ToArray();
    }
}
