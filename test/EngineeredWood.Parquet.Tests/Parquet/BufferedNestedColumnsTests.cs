// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using ArrowSchema = Apache.Arrow.Schema;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// #462: BufferedParquetWriter handled only flat columns; a struct, list or map failed on the first append with
/// "Nullable object must have a value". Each case here writes the same batches through both writers and compares
/// what reads back row by row, with ParquetFileWriter (which already wrote nested columns) as the oracle.
/// </summary>
public class BufferedNestedColumnsTests : IDisposable
{
    private readonly string _tempDir;

    public BufferedNestedColumnsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-buffered-nested-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            Directory.Delete(_tempDir, recursive: true);
        }
    }

    // ───── Batch builders: (first row index, row count) → one column ─────

    private static ArrowBuffer Offsets(List<int> offsets)
    {
        var b = new ArrowBuffer.Builder<int>();
        foreach (int o in offsets)
            b.Append(o);
        return b.Build();
    }

    private static ArrowBuffer Validity(List<bool> valid)
    {
        var b = new ArrowBuffer.BitmapBuilder();
        foreach (bool v in valid)
            b.Append(v);
        return b.Build();
    }

    // Row r: null when r % 7 == 3; otherwise r % 4 elements, each null when (r + j) % 5 == 0.
    private static IArrowArray ListOfLongs(int start, int count)
    {
        var values = new Int64Array.Builder();
        var offsets = new List<int> { 0 };
        var valid = new List<bool>();
        for (int r = start; r < start + count; r++)
        {
            bool isNull = r % 7 == 3;
            valid.Add(!isNull);
            if (!isNull)
            {
                for (int j = 0; j < r % 4; j++)
                {
                    if ((r + j) % 5 == 0) values.AppendNull(); else values.Append(r * 10L + j);
                }
            }
            offsets.Add(values.Length);
        }
        var type = new ListType(new Field("element", Int64Type.Default, true));
        return new ListArray(type, count, Offsets(offsets), values.Build(), Validity(valid), valid.Count(v => !v));
    }

    // {a: int32 (null when r % 3 == 0), b: string}, the struct null when r % 5 == 1.
    private static IArrowArray StructOfIntAndString(int start, int count)
    {
        var a = new Int32Array.Builder();
        var b = new StringArray.Builder();
        var valid = new List<bool>();
        for (int r = start; r < start + count; r++)
        {
            if (r % 3 == 0) a.AppendNull(); else a.Append(r);
            b.Append("s" + (r % 6));
            valid.Add(r % 5 != 1);
        }
        var type = new StructType([new Field("a", Int32Type.Default, true), new Field("b", StringType.Default, true)]);
        return new StructArray(type, count, [a.Build(), b.Build()], Validity(valid), valid.Count(v => !v));
    }

    // Row r: r % 3 entries, key "k{j}", value null when (r + j) % 4 == 0; the map null when r % 6 == 5.
    private static IArrowArray MapOfStringToInt(int start, int count)
    {
        var keys = new StringArray.Builder();
        var values = new Int32Array.Builder();
        var offsets = new List<int> { 0 };
        var valid = new List<bool>();
        for (int r = start; r < start + count; r++)
        {
            bool isNull = r % 6 == 5;
            valid.Add(!isNull);
            if (!isNull)
            {
                for (int j = 0; j < r % 3; j++)
                {
                    keys.Append("k" + j);
                    if ((r + j) % 4 == 0) values.AppendNull(); else values.Append(r + j);
                }
            }
            offsets.Add(keys.Length);
        }
        var mapType = new MapType(new Field("key", StringType.Default, false), new Field("value", Int32Type.Default, true));
        var entries = new StructArray(new StructType([mapType.KeyField, mapType.ValueField]), keys.Length,
            [keys.Build(), values.Build()], ArrowBuffer.Empty, nullCount: 0);
        return new MapArray(mapType, count, Offsets(offsets), entries, Validity(valid), valid.Count(v => !v));
    }

    // list<struct<x: double, tags: list<string>>>: row r has r % 3 structs; struct j's tags hold (r + j) % 3 strings.
    private static IArrowArray ListOfStructsOfLists(int start, int count)
    {
        var x = new DoubleArray.Builder();
        var tagValues = new StringArray.Builder();
        var tagOffsets = new List<int> { 0 };
        var tagValid = new List<bool>();
        var listOffsets = new List<int> { 0 };
        int structs = 0;
        for (int r = start; r < start + count; r++)
        {
            for (int j = 0; j < r % 3; j++)
            {
                x.Append(r + j / 10.0);
                bool tagsNull = (r + j) % 5 == 4;
                tagValid.Add(!tagsNull);
                if (!tagsNull)
                {
                    for (int t = 0; t < (r + j) % 3; t++)
                        tagValues.Append("t" + ((r + t) % 4));
                }
                tagOffsets.Add(tagValues.Length);
                structs++;
            }
            listOffsets.Add(structs);
        }
        var tagType = new ListType(new Field("element", StringType.Default, true));
        var tags = new ListArray(tagType, structs, Offsets(tagOffsets), tagValues.Build(), Validity(tagValid),
            tagValid.Count(v => !v));
        var structType = new StructType([new Field("x", DoubleType.Default, true), new Field("tags", tagType, true)]);
        var items = new StructArray(structType, structs, [x.Build(), tags], ArrowBuffer.Empty, nullCount: 0);
        var type = new ListType(new Field("element", structType, true));
        return new ListArray(type, count, Offsets(listOffsets), items, ArrowBuffer.Empty, nullCount: 0);
    }

    // list<bool>: row r has r % 3 elements, null when j == 1.
    private static IArrowArray ListOfBools(int start, int count)
    {
        var values = new BooleanArray.Builder();
        var offsets = new List<int> { 0 };
        for (int r = start; r < start + count; r++)
        {
            for (int j = 0; j < r % 3; j++)
            {
                if (j == 1) values.AppendNull(); else values.Append((r + j) % 2 == 0);
            }
            offsets.Add(values.Length);
        }
        var type = new ListType(new Field("element", BooleanType.Default, true));
        return new ListArray(type, count, Offsets(offsets), values.Build(), ArrowBuffer.Empty, nullCount: 0);
    }

    // list<string> where every element is distinct, so the dictionary is past the cardinality threshold.
    private static IArrowArray ListOfUniqueStrings(int start, int count)
    {
        var values = new StringArray.Builder();
        var offsets = new List<int> { 0 };
        var valid = new List<bool>();
        for (int r = start; r < start + count; r++)
        {
            bool isNull = r % 4 == 2;
            valid.Add(!isNull);
            if (!isNull)
            {
                for (int j = 0; j < 1 + r % 3; j++)
                    values.Append("unique-" + r + "-" + j);
            }
            offsets.Add(values.Length);
        }
        var type = new ListType(new Field("element", StringType.Default, true));
        return new ListArray(type, count, Offsets(offsets), values.Build(), Validity(valid), valid.Count(v => !v));
    }

    // struct<a: int32> whose a is null in every row: a leaf with no value at all.
    private static IArrowArray StructOfAllNullInt(int start, int count)
    {
        var a = new Int32Array.Builder();
        var valid = new List<bool>();
        for (int r = start; r < start + count; r++)
        {
            a.AppendNull();
            valid.Add(r % 2 == 0);
        }
        var type = new StructType([new Field("a", Int32Type.Default, true)]);
        return new StructArray(type, count, [a.Build()], Validity(valid), valid.Count(v => !v));
    }

    // A required struct with a required child: no definition levels at all.
    private static IArrowArray RequiredStruct(int start, int count)
    {
        var a = new Int64Array.Builder();
        for (int r = start; r < start + count; r++)
            a.Append(r % 9);
        var type = new StructType([new Field("a", Int64Type.Default, false)]);
        return new StructArray(type, count, [a.Build()], ArrowBuffer.Empty, nullCount: 0);
    }

    // fixed_size_list<int32, 3>: the list null when r % 5 == 2, element j null when (r + j) % 4 == 3.
    private static IArrowArray FixedSizeListOfInts(int start, int count)
    {
        var values = new Int32Array.Builder();
        var valid = new List<bool>();
        for (int r = start; r < start + count; r++)
        {
            valid.Add(r % 5 != 2);
            for (int j = 0; j < 3; j++)
            {
                if ((r + j) % 4 == 3) values.AppendNull(); else values.Append(r * 3 + j);
            }
        }
        var type = new FixedSizeListType(new Field("element", Int32Type.Default, true), 3);
        return new FixedSizeListArray(type, count, values.Build(), Validity(valid), valid.Count(v => !v));
    }

    // The same, as a slice starting two rows into a longer array: a fixed-size list's values are not sliced with it,
    // so the leaf's first element is at (offset + row) * 3.
    private static IArrowArray SlicedFixedSizeListOfInts(int start, int count) =>
        ArrowArrayFactory.Slice(FixedSizeListOfInts(start - 2, count + 3), 2, count);

    private static IArrowArray FlatInts(int start, int count)
    {
        var b = new Int32Array.Builder();
        for (int r = start; r < start + count; r++)
        {
            if (r % 4 == 1) b.AppendNull(); else b.Append(r % 5);
        }
        return b.Build();
    }

    private static readonly Dictionary<string, (Func<int, int, IArrowArray> Build, bool Nullable)> Columns = new()
    {
        ["list_of_longs"] = (ListOfLongs, true),
        ["struct_of_int_and_string"] = (StructOfIntAndString, true),
        ["map_of_string_to_int"] = (MapOfStringToInt, true),
        ["list_of_structs_of_lists"] = (ListOfStructsOfLists, true),
        ["list_of_bools"] = (ListOfBools, true),
        ["list_of_unique_strings"] = (ListOfUniqueStrings, true),
        ["struct_of_all_null_int"] = (StructOfAllNullInt, true),
        ["required_struct"] = (RequiredStruct, false),
        ["fixed_size_list_of_ints"] = (FixedSizeListOfInts, true),
        ["sliced_fixed_size_list_of_ints"] = (SlicedFixedSizeListOfInts, true),
        ["flat_ints"] = (FlatInts, true),
    };

    public static TheoryData<string> ColumnNames()
    {
        var data = new TheoryData<string>();
        foreach (var name in Columns.Keys)
            data.Add(name);
        return data;
    }

    private static RecordBatch Batch(string[] names, int start, int count)
    {
        var fields = new List<Field>();
        var arrays = new List<IArrowArray>();
        foreach (var name in names)
        {
            var (build, nullable) = Columns[name];
            var array = build(start, count);
            fields.Add(new Field(name, array.Data.DataType, nullable));
            arrays.Add(array);
        }
        return new RecordBatch(new ArrowSchema(fields, null), arrays, count);
    }

    // ───── Writing and reading ─────

    private async Task<string> WriteFileAsync(RecordBatch[] batches)
    {
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N") + ".parquet");
        await using var file = new LocalSequentialFile(path);
        await using var w = new ParquetFileWriter(file, ownsFile: false);
        foreach (var b in batches)
            await w.WriteRowGroupAsync(b);
        await w.CloseAsync();
        return path;
    }

    private async Task<string> WriteBufferedAsync(RecordBatch[] batches, ParquetWriteOptions? options = null)
    {
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N") + ".parquet");
        await using var file = new LocalSequentialFile(path);
        await using var w = new BufferedParquetWriter(file, ownsFile: false, options);
        foreach (var b in batches)
            await w.AppendAsync(b);
        await w.CloseAsync();
        return path;
    }

    private static async Task<(List<string> Rows, int RowGroups)> ReadRowsAsync(string path)
    {
        await using var input = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(input, ownsFile: false);
        var metadata = await reader.ReadMetadataAsync();
        var rows = new List<string>();
        for (int g = 0; g < metadata.RowGroups.Count; g++)
        {
            var batch = await reader.ReadRowGroupAsync(g);
            for (int r = 0; r < batch.Length; r++)
                rows.Add(ArrowValues.RenderRow(batch, r));
        }
        return (rows, metadata.RowGroups.Count);
    }

    private static RecordBatch[] Batches(string[] names, params int[] sizes)
    {
        var batches = new RecordBatch[sizes.Length];
        int start = 0;
        for (int i = 0; i < sizes.Length; i++)
        {
            batches[i] = Batch(names, start, sizes[i]);
            start += sizes[i];
        }
        return batches;
    }

    // ───── Tests ─────

    [Theory]
    [MemberData(nameof(ColumnNames))]
    public async Task AColumn_ReadsBackAsParquetFileWriterWritesIt(string name)
    {
        var batches = Batches([name], 13, 1, 20);

        var expected = (await ReadRowsAsync(await WriteFileAsync(batches))).Rows;
        var (actual, rowGroups) = await ReadRowsAsync(await WriteBufferedAsync(batches));

        Assert.Equal(34, expected.Count);
        Assert.Equal(expected, actual);
        Assert.Equal(1, rowGroups);
    }

    [Fact]
    public async Task EveryColumnTogether_AcrossAutomaticRowGroups_ReadsBackTheSame()
    {
        var names = Columns.Keys.ToArray();
        var batches = Batches(names, 4, 4, 4, 4, 3);

        var expected = (await ReadRowsAsync(await WriteFileAsync(batches))).Rows;
        var (actual, rowGroups) = await ReadRowsAsync(
            await WriteBufferedAsync(batches, new ParquetWriteOptions { RowGroupMaxRows = 8 }));

        Assert.Equal(expected, actual);
        Assert.Equal(3, rowGroups);
    }

    [Fact]
    public async Task AListColumn_HoldsTheValuesItWasGiven()
    {
        // A direct check, independent of the oracle: rows 0..4 of ListOfLongs.
        // Row 0: [], row 1: [10], row 2: [20, 21], row 3: null, row 4: [].
        var batches = Batches(["list_of_longs"], 5);

        var (rows, _) = await ReadRowsAsync(await WriteBufferedAsync(batches));

        var expected = Enumerable.Range(0, 5).Select(r => ArrowValues.Render(ListOfLongs(0, 5), r)).ToList();
        Assert.Equal(expected, rows);
        Assert.Equal("null", rows[3]);
        Assert.Equal("[]", rows[0]);
    }

    // A run-end encoded list element is rebuilt as a plain array when the batch has a null or empty row (the leaf
    // then needs nulls the runs can't carry, #480) and passed through as runs when it has none. The leaf's layout
    // therefore changes from batch to batch, and every batch must be buffered by its values, not its layout.
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task AListOfRunEndEncodedValues_ReadsBackTheSame_WhicheverLayoutEachBatchHas(bool nullRowFirst)
    {
        static RecordBatch Runs(bool withNullRow, int seed)
        {
            // Runs: seed x2, seed + 1 x1. Rows: [seed, seed], then a null row if asked for, then [seed + 1].
            var values = new RunEndEncodedArray(
                new Int32Array.Builder().Append(2).Append(3).Build(),
                new Int32Array.Builder().Append(seed).Append(seed + 1).Build());
            var type = new ListType(new Field("element", values.Data.DataType, true));
            var list = withNullRow
                ? new ListArray(type, 3, Offsets([0, 2, 2, 3]), values, Validity([true, false, true]), 1)
                : new ListArray(type, 2, Offsets([0, 2, 3]), values, ArrowBuffer.Empty, 0);
            return new RecordBatch(new ArrowSchema([new Field("l", type, true)], null), [list], list.Length);
        }

        var batches = new[] { Runs(nullRowFirst, 10), Runs(!nullRowFirst, 20), Runs(nullRowFirst, 30) };

        var expected = (await ReadRowsAsync(await WriteFileAsync(batches))).Rows;
        var (actual, _) = await ReadRowsAsync(await WriteBufferedAsync(batches));

        Assert.Equal(nullRowFirst ? 8 : 7, expected.Count);
        Assert.Equal(expected, actual);
    }
}
