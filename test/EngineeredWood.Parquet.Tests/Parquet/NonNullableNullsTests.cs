// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using ArrowSchema = Apache.Arrow.Schema;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// #463: Arrow does not enforce a field's nullable flag, and both writers encode a non-nullable field as a required
/// column with no definition levels, so a null in the first (or only) batch was written as a value: an Int32 null
/// read back as 0. Such a batch must be refused, naming the column, at every depth where the null would be written.
/// #457 already refused it in a later batch, as a mismatch with the file's schema.
/// </summary>
public class NonNullableNullsTests : IDisposable
{
    private readonly string _tempDir;

    public NonNullableNullsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-nonnull-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }

    public enum Writer { File, Buffered }

    private string NewPath() => Path.Combine(_tempDir, Guid.NewGuid().ToString("N") + ".parquet");

    private static async Task WriteAsync(Writer kind, string path, params RecordBatch[] batches)
    {
        await using var file = new LocalSequentialFile(path);
        if (kind == Writer.File)
        {
            await using var w = new ParquetFileWriter(file, ownsFile: false);
            foreach (var b in batches)
            {
                await w.WriteRowGroupAsync(b);
            }
            await w.CloseAsync();
        }
        else
        {
            await using var w = new BufferedParquetWriter(file, ownsFile: false);
            foreach (var b in batches)
            {
                await w.AppendAsync(b);
            }
            await w.CloseAsync();
        }
    }

    private async Task<ArgumentException> RefusedAsync(Writer kind, RecordBatch batch) =>
        await Assert.ThrowsAsync<ArgumentException>(() => WriteAsync(kind, NewPath(), batch));

    private static async Task<RecordBatch> ReadAsync(string path)
    {
        await using var file = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        return await reader.ReadRowGroupAsync(0);
    }

    private static RecordBatch Single(Field field, IArrowArray array) =>
        new(new ArrowSchema.Builder().Field(field).Build(), [array], array.Length);

    private static ArrowBuffer Validity(params bool[] valid)
    {
        var b = new ArrowBuffer.BitmapBuilder();
        foreach (bool v in valid)
        {
            b.Append(v);
        }
        return b.Build();
    }

    private static ArrowBuffer Offsets(params int[] offsets)
    {
        var b = new ArrowBuffer.Builder<int>();
        foreach (int o in offsets)
        {
            b.Append(o);
        }
        return b.Build();
    }

    private static Int32Array Ints(params int?[] values)
    {
        var b = new Int32Array.Builder();
        foreach (var v in values)
        {
            if (v is null)
            {
                b.AppendNull();
            }
            else
            {
                b.Append(v.Value);
            }
        }
        return b.Build();
    }

    // The issue's repro: a single batch, so no later-batch comparison ever ran.
    [Theory]
    [InlineData(Writer.File)]
    [InlineData(Writer.Buffered)]
    public async Task NullInANonNullableInt32_IsRefused(Writer kind)
    {
        var ex = await RefusedAsync(kind, Single(new Field("a", Int32Type.Default, false), Ints(1, null, 3)));

        Assert.Contains("'a'", ex.Message);
        Assert.Contains("non-nullable", ex.Message);
    }

    [Theory]
    [InlineData(Writer.File)]
    [InlineData(Writer.Buffered)]
    public async Task NullInANonNullableString_IsRefused(Writer kind)
    {
        var strings = new StringArray.Builder().Append("x").AppendNull().Build();

        var ex = await RefusedAsync(kind, Single(new Field("s", StringType.Default, false), strings));

        Assert.Contains("'s'", ex.Message);
    }

    // A dictionary column's nulls are its null indices. BufferedParquetWriter only: ParquetFileWriter does not
    // write dictionary arrays.
    [Fact]
    public async Task NullIndexInANonNullableDictionary_IsRefused()
    {
        var dictType = new DictionaryType(Int32Type.Default, StringType.Default, false);
        var dict = new DictionaryArray(dictType, Ints(0, null, 1),
            new StringArray.Builder().Append("x").Append("y").Build());

        var ex = await RefusedAsync(Writer.Buffered, Single(new Field("d", dictType, false), dict));

        Assert.Contains("'d'", ex.Message);
    }

    // The refusal must leave the writer usable: the schema, including the one the footer declares in ARROW:schema,
    // is captured only from a batch that is written.
    [Theory]
    [InlineData(Writer.File)]
    [InlineData(Writer.Buffered)]
    public async Task ARefusedFirstBatch_LeavesTheWriterUsable(Writer kind)
    {
        string path = NewPath();
        await using (var file = new LocalSequentialFile(path))
        {
            var bad = Single(new Field("a", Int32Type.Default, false), Ints(1, null));
            var good = Single(new Field("a", Int64Type.Default, true), new Int64Array.Builder().Append(7).Build());
            if (kind == Writer.File)
            {
                await using var w = new ParquetFileWriter(file, ownsFile: false);
                await Assert.ThrowsAsync<ArgumentException>(() => w.WriteRowGroupAsync(bad).AsTask());
                await w.WriteRowGroupAsync(good);
                await w.CloseAsync();
            }
            else
            {
                await using var w = new BufferedParquetWriter(file, ownsFile: false);
                await Assert.ThrowsAsync<ArgumentException>(() => w.AppendAsync(bad).AsTask());
                await w.AppendAsync(good);
                await w.CloseAsync();
            }
        }

        var read = await ReadAsync(path);
        Assert.Equal(7L, ((Int64Array)read.Column(0)).GetValue(0));
        Assert.True(read.Schema.FieldsList[0].IsNullable);
    }

    // Controls: the check must not refuse what the writers handle correctly.
    [Theory]
    [InlineData(Writer.File)]
    [InlineData(Writer.Buffered)]
    public async Task NonNullableWithoutNulls_AndNullableWithNulls_RoundTrip(Writer kind)
    {
        var schema = new ArrowSchema.Builder()
            .Field(new Field("req", Int32Type.Default, false))
            .Field(new Field("opt", Int32Type.Default, true))
            .Build();
        string path = NewPath();

        await WriteAsync(kind, path, new RecordBatch(schema, [Ints(1, 2), Ints(null, 4)], 2));

        var read = await ReadAsync(path);
        Assert.Equal(1, ((Int32Array)read.Column(0)).GetValue(0));
        Assert.True(read.Column(1).IsNull(0));
    }

    // A null outside the batch's slice is never written.
    [Theory]
    [InlineData(Writer.File)]
    [InlineData(Writer.Buffered)]
    public async Task NullOutsideASlice_IsAccepted(Writer kind)
    {
        var sliced = (Int32Array)Ints(null, 5, 6).Slice(1, 2);
        string path = NewPath();

        await WriteAsync(kind, path, Single(new Field("a", Int32Type.Default, false), sliced));

        var read = (Int32Array)(await ReadAsync(path)).Column(0);
        Assert.Equal([5, 6], new[] { read.GetValue(0)!.Value, read.GetValue(1)!.Value });
    }

    // Nested columns: ParquetFileWriter only, since BufferedParquetWriter has no nested support (#462).

    private static StructType StructOfX(bool xNullable) => new([new Field("x", Int32Type.Default, xNullable)]);

    [Fact]
    public async Task NullSlotInANonNullableStruct_IsRefused()
    {
        var type = StructOfX(xNullable: true);
        var structArray = new StructArray(type, 2, [Ints(1, 2)], Validity(true, false), nullCount: 1);

        var ex = await RefusedAsync(Writer.File, Single(new Field("s", type, false), structArray));

        Assert.Contains("'s'", ex.Message);
    }

    [Fact]
    public async Task NullInANonNullableStructChild_IsRefused()
    {
        var type = StructOfX(xNullable: false);
        var structArray = new StructArray(type, 2, [Ints(1, null)], ArrowBuffer.Empty, nullCount: 0);

        var ex = await RefusedAsync(Writer.File, Single(new Field("s", type, true), structArray));

        Assert.Contains("'s.x'", ex.Message);
    }

    // A struct child's slot under a null struct row is never written.
    [Fact]
    public async Task NullInANonNullableStructChild_UnderANullStructSlot_IsAccepted()
    {
        var type = StructOfX(xNullable: false);
        var structArray = new StructArray(type, 2, [Ints(2, null)], Validity(true, false), nullCount: 1);
        string path = NewPath();

        await WriteAsync(Writer.File, path, Single(new Field("s", type, true), structArray));

        var read = (StructArray)(await ReadAsync(path)).Column(0);
        Assert.Equal(2, ((Int32Array)read.Fields[0]).GetValue(0));
        Assert.True(read.IsNull(1));
    }

    private static ListType ListOfInts(bool elementNullable) =>
        new(new Field("element", Int32Type.Default, elementNullable));

    private static ListArray List(ListType type, bool[] valid, int[] offsets, Int32Array values) =>
        new(type, valid.Length, Offsets(offsets), values, Validity(valid), valid.Count(v => !v));

    [Fact]
    public async Task NullSlotInANonNullableList_IsRefused()
    {
        var type = ListOfInts(elementNullable: true);
        var list = List(type, [true, false], [0, 1, 1], Ints(1));

        var ex = await RefusedAsync(Writer.File, Single(new Field("l", type, false), list));

        Assert.Contains("'l'", ex.Message);
    }

    [Fact]
    public async Task NullElementInANonNullableList_IsRefused()
    {
        var type = ListOfInts(elementNullable: false);
        var list = List(type, [true, true], [0, 1, 2], Ints(1, null));

        var ex = await RefusedAsync(Writer.File, Single(new Field("l", type, true), list));

        Assert.Contains("'l.element'", ex.Message);
    }

    [Fact]
    public async Task NullElementUnderANullListSlot_IsAccepted()
    {
        var type = ListOfInts(elementNullable: false);
        var list = List(type, [true, false], [0, 1, 2], Ints(1, null));
        string path = NewPath();

        await WriteAsync(Writer.File, path, Single(new Field("l", type, true), list));

        var read = (ListArray)(await ReadAsync(path)).Column(0);
        Assert.Equal(1, ((Int32Array)read.GetSlicedValues(0)).GetValue(0));
        Assert.True(read.IsNull(1));
    }

    [Fact]
    public async Task NullElementInANonNullableFixedSizeList_IsRefused()
    {
        var type = new FixedSizeListType(new Field("element", Int32Type.Default, false), 2);
        var fixedList = new FixedSizeListArray(type, 2, Ints(1, 2, 3, null), ArrowBuffer.Empty, nullCount: 0);

        var ex = await RefusedAsync(Writer.File, Single(new Field("f", type, true), fixedList));

        Assert.Contains("'f.element'", ex.Message);
    }

    private static MapArray Map(MapType type, Int32Array keys, Int32Array values) =>
        new(type, 1, Offsets(0, keys.Length),
            new StructArray(new StructType([type.KeyField, type.ValueField]), keys.Length, [keys, values],
                ArrowBuffer.Empty, nullCount: 0),
            Validity(true), nullCount: 0);

    // Parquet map keys are always required, whatever the Arrow key field says.
    [Fact]
    public async Task NullMapKey_IsRefused_EvenWhenTheKeyFieldIsNullable()
    {
        var type = new MapType(new Field("key", Int32Type.Default, true), new Field("value", Int32Type.Default, true));

        var ex = await RefusedAsync(Writer.File, Single(new Field("m", type, true), Map(type, Ints(1, null), Ints(10, 20))));

        Assert.Contains("'m.key'", ex.Message);
        Assert.Contains("map key", ex.Message);
    }

    [Fact]
    public async Task NullInANonNullableMapValue_IsRefused()
    {
        var type = new MapType(new Field("key", Int32Type.Default, false), new Field("value", Int32Type.Default, false));

        var ex = await RefusedAsync(Writer.File, Single(new Field("m", type, true), Map(type, Ints(1, 2), Ints(10, null))));

        Assert.Contains("'m.value'", ex.Message);
    }

    // A run-end encoded array's nulls are null runs in its values.
    [Fact]
    public async Task NullRunInANonNullableRunEndEncodedColumn_IsRefused()
    {
        var ree = new RunEndEncodedArray(Ints(2, 4), Ints(1, null));

        var ex = await RefusedAsync(Writer.File, Single(new Field("r", ree.Data.DataType, false), ree));

        Assert.Contains("'r'", ex.Message);
    }
}
