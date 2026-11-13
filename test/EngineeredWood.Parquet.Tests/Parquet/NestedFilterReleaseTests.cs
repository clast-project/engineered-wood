// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;
using ArrowSchema = Apache.Arrow.Schema;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// #856: assembling a list or map drops the level entries that mark a null or empty one by gathering its leaf, and
/// never released the leaf it gathered from, so its native buffers stayed allocated until the collector finalized them.
/// Each test reads a row group, disposes it and requires the native memory back where it started, with no collection
/// in between.
/// </summary>
[Collection(NativeMemoryCollection.Name)]
public class NestedFilterReleaseTests : IDisposable
{
    private const int Rows = 4_000;
    private readonly string _tempDir;

    public NestedFilterReleaseTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-nested-release-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch { }
    }

    public static TheoryData<ByteArrayOutputKind> Representations =>
        [ByteArrayOutputKind.Default, ByteArrayOutputKind.ViewType];

    [Fact]
    public async Task AListOfLongsWithNullAndEmptyRows_ReleasesItsLeaf()
    {
        var values = new Int64Array.Builder();
        var column = List(new Field("element", Int64Type.Default, true), i => values.Append(i), () => values.Build());

        var (read, left) = await ReadAndMeasureAsync(column, ByteArrayOutputKind.Default);

        Assert.Equal(Expected(i => i.ToString()), read);
        Assert.Equal(0, left);
    }

    [Theory]
    [MemberData(nameof(Representations))]
    public async Task AListOfStringsWithNullAndEmptyRows_ReleasesItsLeaf(ByteArrayOutputKind representation)
    {
        // A view leaf's gather shares its out-of-line data: that must survive, the rest must not.
        var values = new StringArray.Builder();
        var column = List(new Field("element", StringType.Default, true), i => values.Append(Text(i)), () => values.Build());

        var (read, left) = await ReadAndMeasureAsync(column, representation);

        Assert.Equal(Expected(Text), read);
        Assert.Equal(0, left);
    }

    [Fact]
    public async Task AListOfStructsWithNullAndEmptyRows_ReleasesItsLeaves()
    {
        var structType = new StructType([
            new Field("a", Int64Type.Default, true), new Field("b", StringType.Default, true)]);
        var a = new Int64Array.Builder();
        var b = new StringArray.Builder();
        int count = 0;
        var column = List(
            new Field("element", structType, true),
            i => { a.Append(i); b.Append(Text(i)); count++; },
            () => new StructArray(structType, count, [a.Build(), b.Build()], ArrowBuffer.Empty, 0));

        var (read, left) = await ReadAndMeasureAsync(column, ByteArrayOutputKind.Default);

        Assert.Equal(Expected(i => i + ":" + Text(i)), read);
        Assert.Equal(0, left);
    }

    [Fact]
    public async Task AMapWithNullAndEmptyRows_ReleasesItsKeysAndValues()
    {
        var keyField = new Field("key", StringType.Default, false);
        var valueField = new Field("value", Int64Type.Default, true);
        var keys = new StringArray.Builder();
        var values = new Int64Array.Builder();
        int count = 0;
        var (offsets, validity, nulls) = Layout(i => { keys.Append(Text(i)); values.Append(i); count++; });
        var entries = new StructArray(
            new StructType([keyField, valueField]), count, [keys.Build(), values.Build()], ArrowBuffer.Empty, 0);
        var column = new MapArray(new MapType(keyField, valueField), Rows, offsets, entries, validity, nulls);

        var (read, left) = await ReadAndMeasureAsync(column, ByteArrayOutputKind.Default);

        Assert.Equal(Expected(i => Text(i) + "=" + i), read);
        Assert.Equal(0, left);
    }

    [Fact]
    public async Task BareRepeatedLeaves_ReleaseTheirDenseLeaves()
    {
        // A repeated primitive with no LIST annotation is gathered by the element filter, not the subtree one.
        for (int i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        long before = NativeMemoryTracker.LiveBytes;
        await using (var input = new LocalRandomAccessFile(TestData.GetPath("repeated_primitive_no_list.parquet")))
        await using (var reader = new ParquetFileReader(input, ownsFile: false))
        {
            var metadata = await reader.ReadMetadataAsync();
            for (int rg = 0; rg < metadata.RowGroups.Count; rg++)
            {
                using var batch = await reader.ReadRowGroupAsync(rg);
                Assert.True(batch.Length > 0);
            }
        }

        Assert.Equal(0, NativeMemoryTracker.LiveBytes - before);
    }

    private static string Text(int i) => "element value number " + i;

    private static ListArray List(Field element, Action<int> append, Func<IArrowArray> build)
    {
        var (offsets, validity, nulls) = Layout(append);
        return new ListArray(new ListType(element), Rows, offsets, build(), validity, nulls);
    }

    // Rows cycle null, empty, two elements; element k of row r is r * 2 + k.
    private static (ArrowBuffer Offsets, ArrowBuffer Validity, int Nulls) Layout(Action<int> append)
    {
        var offsets = new ArrowBuffer.Builder<int>();
        var validity = new ArrowBuffer.BitmapBuilder();
        int count = 0, nulls = 0;
        offsets.Append(0);
        for (int row = 0; row < Rows; row++)
        {
            int kind = row % 3;
            if (kind == 2)
            {
                for (int k = 0; k < 2; k++, count++)
                    append(row * 2 + k);
            }
            validity.Append(kind != 0);
            nulls += kind == 0 ? 1 : 0;
            offsets.Append(count);
        }
        return (offsets.Build(), validity.Build(), nulls);
    }

    private static List<string> Expected(Func<int, string> element)
    {
        var rows = new List<string>(Rows);
        for (int row = 0; row < Rows; row++)
        {
            rows.Add((row % 3) switch
            {
                0 => "null",
                1 => "[]",
                _ => "[" + element(row * 2) + "," + element(row * 2 + 1) + "]",
            });
        }
        return rows;
    }

    // Writes the column, reads it back, renders it, disposes it, and reports the native bytes still allocated.
    private async Task<(List<string> Rows, long Left)> ReadAndMeasureAsync(
        IArrowArray column, ByteArrayOutputKind representation)
    {
        var schema = new ArrowSchema.Builder().Field(new Field("c", column.Data.DataType, true)).Build();
        string path = Path.Combine(_tempDir, Guid.NewGuid().ToString("N") + ".parquet");
        await using (var file = new LocalSequentialFile(path))
        {
            await using var writer = new ParquetFileWriter(file, ownsFile: false);
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [column], column.Length));
            await writer.CloseAsync();
        }

        for (int i = 0; i < 3; i++)
        {
            GC.Collect();
            GC.WaitForPendingFinalizers();
        }

        long before = NativeMemoryTracker.LiveBytes;
        List<string> rendered;
        await using (var input = new LocalRandomAccessFile(path))
        await using (var reader = new ParquetFileReader(
            input, ownsFile: false, new ParquetReadOptions { ByteArrayOutput = representation }))
        {
            using var batch = await reader.ReadRowGroupAsync(0);
            rendered = Render(batch.Column(0));
        }
        return (rendered, NativeMemoryTracker.LiveBytes - before);
    }

    private static List<string> Render(IArrowArray column)
    {
        var rows = new List<string>(column.Length);
        var list = (ListArray)column; // MapArray is a ListArray
        for (int row = 0; row < column.Length; row++)
        {
            if (column.IsNull(row))
            {
                rows.Add("null");
                continue;
            }

            var items = new List<string>();
            for (int i = list.ValueOffsets[row]; i < list.ValueOffsets[row + 1]; i++)
                items.Add(Element(list.Values, i, column is MapArray));
            rows.Add("[" + string.Join(",", items) + "]");
        }
        return rows;
    }

    private static string Element(IArrowArray values, int i, bool entry = false) => values switch
    {
        Int64Array longs => longs.GetValue(i)!.Value.ToString(),
        StringArray strings => strings.GetString(i),
        StringViewArray views => views.GetString(i),
        StructArray s => Element(s.Fields[0], i) + (entry ? "=" : ":") + Element(s.Fields[1], i),
        _ => throw new NotSupportedException(values.GetType().Name),
    };
}
