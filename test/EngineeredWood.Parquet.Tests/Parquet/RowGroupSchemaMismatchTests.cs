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
        StructType or FixedSizeListType => [],
        ListType or MapType => [new ArrowBuffer(new byte[(length + 1) * 4])],
        StringType or BinaryType => [new ArrowBuffer(new byte[(length + 1) * 4]), ArrowBuffer.Empty],
        FixedWidthType fw => [new ArrowBuffer(new byte[Math.Max(1, (fw.BitWidth * length + 7) / 8)])],
        _ => throw new NotSupportedException(type.Name),
    };

    private static ArrayData[]? Children(IArrowType type, int length) => type switch
    {
        StructType st => st.Fields.Select(f => ArrayOfNulls(f.DataType, length).Data).ToArray(),
        ListType lt => [ArrayOfNulls(lt.ValueDataType, 0).Data],
        FixedSizeListType ft => [ArrayOfNulls(ft.ValueDataType, length * ft.ListSize).Data],
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

    private static RunEndEncodedArray Ree(IArrowArray values)
    {
        var ends = new Int32Array.Builder();
        for (int i = 1; i <= values.Length; i++)
            ends.Append(i * 2);
        return new RunEndEncodedArray(ends.Build(), values);
    }

    private static RecordBatch ReeBatch(RunEndEncodedArray ree, bool nullable) => new(
        SchemaOf(new Field("r", ree.Data.DataType, nullable)), [ree], ree.Length);

    // Review of #464: two run-end encoded columns share a TypeId whatever their values are, so the values type
    // must be compared, or REE<Int64> is narrowed into the file's REE<Int32> column.
    // ParquetFileWriter only: BufferedParquetWriter refuses run-end encoded columns outright.
    [Fact]
    public async Task RunEndEncodedValuesType_IsCompared()
    {
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => WriteAsync(Writer.File,
            Path.Combine(_tempDir, "ree-type.parquet"),
            ReeBatch(Ree(new Int32Array.Builder().Append(1).Build()), nullable: true),
            ReeBatch(Ree(new Int64Array.Builder().Append(5_000_000_000L).Build()), nullable: true)));

        Assert.Contains("'r'", ex.Message);
        Assert.Contains("int64", ex.Message);
    }

    // Review of #464: a run-end encoded array has no validity bitmap; its nulls are null runs in Values.
    // ParquetFileWriter only: BufferedParquetWriter refuses run-end encoded columns outright.
    [Fact]
    public async Task NullRunsInAColumnTheFileMadeRequired_AreRefused()
    {
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => WriteAsync(Writer.File,
            Path.Combine(_tempDir, "ree-nulls.parquet"),
            ReeBatch(Ree(new Int32Array.Builder().Append(1).Build()), nullable: false),
            ReeBatch(Ree(new Int32Array.Builder().Append(2).AppendNull().Build()), nullable: true)));

        Assert.Contains("required", ex.Message);
    }

    // Review of #464: an empty zone is still a zone to ArrowToSchemaConverter (isAdjustedToUTC = Timezone != null),
    // so it does not match a file whose timestamps have none.
    [Theory]
    [InlineData(Writer.File)]
    [InlineData(Writer.Buffered)]
    public async Task EmptyTimezoneAgainstNone_IsRefused(Writer kind)
    {
        var ex = await RefusedAsync(kind,
            SchemaOf(new Field("t", new TimestampType(TimeUnit.Microsecond, (string?)null), true)),
            SchemaOf(new Field("t", new TimestampType(TimeUnit.Microsecond, ""), true)));

        Assert.Contains("'t'", ex.Message);
    }

    private static ArrowBuffer Validity(params bool[] valid)
    {
        var b = new ArrowBuffer.BitmapBuilder();
        foreach (bool v in valid)
            b.Append(v);
        return b.Build();
    }

    private static ArrowBuffer Offsets(params int[] offsets)
    {
        var b = new ArrowBuffer.Builder<int>();
        foreach (int o in offsets)
            b.Append(o);
        return b.Build();
    }

    private static Int32Array Ints(params int?[] values)
    {
        var b = new Int32Array.Builder();
        foreach (var v in values)
        {
            if (v is null)
                b.AppendNull();
            else
                b.Append(v.Value);
        }
        return b.Build();
    }

    private static ArrowSchema StructOfX(bool xNullable) =>
        SchemaOf(new Field("s", new StructType([new Field("x", Int32Type.Default, xNullable)]), true));

    private static ArrowSchema ListOfInts(bool elementNullable) =>
        SchemaOf(new Field("l", new ListType(new Field("element", Int32Type.Default, elementNullable)), true));

    private static RecordBatch ListBatch(bool elementNullable, bool[] valid, int[] offsets, Int32Array values)
    {
        var schema = ListOfInts(elementNullable);
        var list = new ListArray(schema.FieldsList[0].DataType, valid.Length, Offsets(offsets), values,
            Validity(valid), valid.Count(v => !v));
        return new RecordBatch(schema, [list], valid.Length);
    }

    // Review of #464: Parquet map keys are always required (ArrowToSchemaConverter, NestedLevelWriter), whatever
    // the Arrow key field says, so a null key is refused even when the first batch declared keys nullable.
    [Fact]
    public async Task NullMapKey_IsRefused_EvenWhenTheFileDeclaredKeysNullable()
    {
        var mapType = new MapType(new Field("key", Int32Type.Default, true), new Field("value", Int32Type.Default, true));
        var schema = SchemaOf(new Field("m", mapType, true));
        var entries = new StructArray(new StructType([mapType.KeyField, mapType.ValueField]), 2,
            [Ints(1, null), Ints(10, 20)], ArrowBuffer.Empty, nullCount: 0);
        var map = new MapArray(mapType, 1, Offsets(0, 2), entries, Validity(true), nullCount: 0);

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => WriteAsync(Writer.File,
            Path.Combine(_tempDir, "map-key.parquet"), BatchOf(schema), new RecordBatch(schema, [map], 1)));

        Assert.Contains("'m.key'", ex.Message);
    }

    // Review of #464: a struct child's slot under a null struct row is never written (NestedLevelWriter maps it
    // to -1), so a null there is fine even when the file made the child required.
    [Fact]
    public async Task NullChildUnderANullStruct_IsAccepted()
    {
        var second = StructOfX(xNullable: true);
        var structArray = new StructArray(second.FieldsList[0].DataType, 2, [Ints(2, null)],
            Validity(true, false), nullCount: 1);
        string path = Path.Combine(_tempDir, "struct-null-parent.parquet");

        await WriteAsync(Writer.File, path, BatchOf(StructOfX(xNullable: false)),
            new RecordBatch(second, [structArray], 2));

        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        var read = (StructArray)(await reader.ReadRowGroupAsync(1)).Column(0);
        Assert.Equal(2, ((Int32Array)read.Fields[0]).GetValue(0));
        Assert.True(read.IsNull(1));
    }

    // The same for a list: an element under a null list slot is never written.
    [Fact]
    public async Task NullElementUnderANullListSlot_IsAccepted()
    {
        await WriteAsync(Writer.File, Path.Combine(_tempDir, "list-null-slot.parquet"),
            BatchOf(ListOfInts(elementNullable: false)),
            ListBatch(elementNullable: true, valid: [true, false], offsets: [0, 1, 2], Ints(1, null)));
    }

    // ...but one in a present slot is written, and is refused.
    [Fact]
    public async Task NullElementInAPresentListSlot_IsRefused()
    {
        var ex = await Assert.ThrowsAsync<ArgumentException>(() => WriteAsync(Writer.File,
            Path.Combine(_tempDir, "list-null-element.parquet"),
            BatchOf(ListOfInts(elementNullable: false)),
            ListBatch(elementNullable: true, valid: [true, true], offsets: [0, 1, 2], Ints(1, null))));

        Assert.Contains("'l.element'", ex.Message);
    }

    // A fixed-size list writes no elements for a null slot either.
    [Fact]
    public async Task NullElementsUnderANullFixedSizeListSlot_AreAccepted()
    {
        static ArrowSchema Of(bool elementNullable) => SchemaOf(new Field("f",
            new FixedSizeListType(new Field("element", Int32Type.Default, elementNullable), 2), true));
        var second = Of(elementNullable: true);
        var fixedList = new FixedSizeListArray(second.FieldsList[0].DataType, 2, Ints(1, 2, null, null),
            Validity(true, false), nullCount: 1);

        await WriteAsync(Writer.File, Path.Combine(_tempDir, "fsl-null-slot.parquet"),
            BatchOf(Of(elementNullable: false)), new RecordBatch(second, [fixedList], 2));
    }

    // Review of #464: ArrayData.NullCount may be -1 ("not computed"), but the check reads IArrowArray.NullCount,
    // which computes it. These pin that: an unknown count must neither hide a null nor invent one.
    private static Int32Array WithUnknownNullCount(Int32Array array) =>
        new(new ArrayData(Int32Type.Default, array.Length, -1, 0, array.Data.Buffers));

    [Fact]
    public async Task UnknownNullCount_InRunEndEncodedValues_StillFindsTheNullRun()
    {
        var values = WithUnknownNullCount(Ints(2, null));
        Assert.Equal(-1, values.Data.NullCount);

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => WriteAsync(Writer.File,
            Path.Combine(_tempDir, "ree-unknown.parquet"),
            ReeBatch(Ree(Ints(1)), nullable: false),
            ReeBatch(Ree(values), nullable: true)));

        Assert.Contains("required", ex.Message);
    }

    [Theory]
    [InlineData(Writer.File)]
    [InlineData(Writer.Buffered)]
    public async Task UnknownNullCount_WithNoNulls_IsAccepted(Writer kind)
    {
        var schema = SchemaOf(new Field("a", Int32Type.Default, false));
        var values = WithUnknownNullCount(Ints(3, 4));
        Assert.Equal(-1, values.Data.NullCount);
        string path = Path.Combine(_tempDir, "unknown-none.parquet");

        await WriteAsync(kind, path,
            new RecordBatch(schema, [Ints(1, 2)], 2), new RecordBatch(schema, [values], 2));

        int[] read = await ReadColumnAsync(path, "a");
        Assert.Equal(new[] { 1, 2, 3, 4 }, read);
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
