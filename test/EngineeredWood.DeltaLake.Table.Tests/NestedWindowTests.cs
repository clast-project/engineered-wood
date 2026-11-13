// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Arrow;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #853: reshaping a nested column (a field added inside a struct, list element or map value, or widened inside one)
/// works on the rows the batch covers. A batch sliced from a larger decode shares children spanning the whole decode;
/// the rebuilt column must hold the same values as the same rows reshaped on their own, at any offset, nulls at every
/// level included, and be built over those rows alone rather than the whole decode.
/// </summary>
public class NestedWindowTests : IDisposable
{
    private const int Decoded = 203;
    private readonly string _tempDir;
    private readonly LocalTableFileSystem _fs;

    public NestedWindowTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-nested-window-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _fs = new LocalTableFileSystem(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, true); } catch { }
    }

    public static TheoryData<string, int, int> Cases()
    {
        var data = new TheoryData<string, int, int>();
        foreach (var scenario in new[]
                 {
                     "struct add", "struct drop", "struct in struct add", "list element add", "large list element add",
                     "map value add", "struct widen", "list element widen", "map value widen", "list in struct add",
                 })
        {
            foreach (var (offset, length) in new[] { (0, Decoded), (0, 50), (8, 50), (13, 61), (150, 53) })
                data.Add(scenario, offset, length);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task ASlice_ReshapesToItsOwnRows(string scenario, int offset, int length)
    {
        bool widen = scenario.EndsWith("widen", StringComparison.Ordinal);
        IArrowType Element(bool evolved) => new StructType(
            widen ? [new Field("a", evolved ? Int64Type.Default : Int32Type.Default, true), new Field("s", StringType.Default, true)]
            : scenario == "struct drop" ? (evolved
                ? [new Field("a", Int32Type.Default, true)]
                : [new Field("a", Int32Type.Default, true), new Field("gone", StringType.Default, true)])
            : evolved
                ? [new Field("a", Int32Type.Default, true), new Field("b", StringType.Default, true), new Field("c", Int64Type.Default, true)]
                : [new Field("a", Int32Type.Default, true)]);
        IArrowType Column(bool evolved) => scenario switch
        {
            "struct add" or "struct drop" or "struct widen" => Element(evolved),
            "struct in struct add" => new StructType(
                [new Field("x", Int32Type.Default, true), new Field("inner", Element(evolved), true)]),
            "large list element add" => new LargeListType(new Field("element", Element(evolved), true)),
            "map value add" or "map value widen" => new MapType(
                new Field("key", StringType.Default, false), new Field("value", Element(evolved), true)),
            "list in struct add" => new StructType(
                [new Field("l", new ListType(new Field("element", Element(evolved), true)), true)]),
            _ => new ListType(new Field("element", Element(evolved), true)),
        };

        var tableSchema = new Apache.Arrow.Schema.Builder().Field(new Field("c", Column(true), true)).Build();
        var configuration = widen
            ? new Dictionary<string, string> { [EngineeredWood.DeltaLake.Schema.TypeWidening.EnableKey] = "true" }
            : null;
        // A large list is no Delta type: reconciled against the expected fields directly.
        bool large = scenario.StartsWith("large", StringComparison.Ordinal);
        await using var table = await DeltaTable.CreateAsync(
            _fs, large ? new Apache.Arrow.Schema.Builder().Field(new Field("x", Int32Type.Default, true)).Build() : tableSchema,
            configuration: configuration);
        var snapshot = table.CurrentSnapshot;
        var fileSchema = new Apache.Arrow.Schema.Builder().Field(new Field("c", Column(false), true)).Build();

        int seed = 0;
        using var whole = new RecordBatch(fileSchema, [Build(Column(false), Decoded, ref seed)], Decoded);
        RecordBatch Shape(RecordBatch raw) => large
            ? SchemaEvolution.BackfillMissingColumns(raw, tableSchema.FieldsList)
            : DeltaTable.ReconcileToTableShape(raw, snapshot, null, null, [], false, new Dictionary<string, string>());

        var shapedWhole = Shape(whole);
        var raw = whole.Slice(offset, length);
        var shaped = Shape(raw);
        Assert.NotSame(raw, shaped); // something was reshaped
        Assert.Equal(shapedWhole.Schema.FieldsList[0].DataType.ToString(), shaped.Schema.FieldsList[0].DataType.ToString());
        for (int row = 0; row < length; row++)
            Assert.Equal(Render(shapedWhole.Column(0), offset + row), Render(shaped.Column(0), row));

        // Built over the slice's rows: no child holds more entries than the slice's own.
        Assert.True(LargestChild(shaped.Column(0).Data) <= MaxEntries(raw.Column(0), length),
            $"a child of the reshaped slice holds {LargestChild(shaped.Column(0).Data)} entries");
    }

    /// <summary>
    /// #854 review round 3: the reshaping end to end, through the Parquet reader's own (native, reference-counted)
    /// buffers. Old files hold a struct with a struct inside it and a list of structs; a merge-schema write then adds a
    /// field at every level and widens the ints inside them. Read in batches of a quarter of a row group, each batch
    /// sliced from one decode, every batch disposed (and a collection run) before the next is read, the values are an
    /// unbatched read's: the rebuilt children a batch hands on outlive the batches before it.
    /// </summary>
    [Fact]
    public async Task ABatchedRead_OfANestedEvolvedTable_SurvivesDisposingEachBatch()
    {
        const int rows = 4_000, rowGroup = 1_000, batch = rowGroup / 4;
        IArrowType Inner(bool evolved) => new StructType(evolved
            ? [new Field("a", Int64Type.Default, true), new Field("c", Int64Type.Default, true)]
            : [new Field("a", Int32Type.Default, true)]);
        IArrowType Point(bool evolved) => new StructType(evolved
            ? [new Field("a", Int64Type.Default, true), new Field("inner", Inner(true), true), new Field("b", StringType.Default, true)]
            : [new Field("a", Int32Type.Default, true), new Field("inner", Inner(false), true)]);
        Apache.Arrow.Schema SchemaOf(bool evolved) => new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int32Type.Default, true))
            .Field(new Field("s", Point(evolved), true))
            .Field(new Field("l", new ListType(new Field("element", Inner(evolved), true)), true))
            .Build();

        await using (var create = await DeltaTable.CreateAsync(
                         _fs, SchemaOf(false),
                         new DeltaTableOptions { ParquetWriteOptions = new ParquetWriteOptions { RowGroupMaxRows = rowGroup } },
                         configuration: new Dictionary<string, string> { [EngineeredWood.DeltaLake.Schema.TypeWidening.EnableKey] = "true" }))
        {
            int seed = 0;
            var old = SchemaOf(false);
            var ids = new Int32Array.Builder();
            for (int i = 0; i < rows; i++)
                ids.Append(i);
            await create.WriteAsync([new RecordBatch(old,
                [ids.Build(), Build(old.FieldsList[1].DataType, rows, ref seed), Build(old.FieldsList[2].DataType, rows, ref seed)], rows)]);
            var evolved = SchemaOf(true);
            await create.WriteAsync(
                [new RecordBatch(evolved, evolved.FieldsList.Select(f => ArrowCompute.MakeNullArray(f.DataType, 1)), 1)],
                mergeSchema: true);
        }

        async Task<List<string>> ReadAsync(int? batchSize)
        {
            await using var table = await DeltaTable.OpenAsync(_fs, new DeltaTableOptions
            {
                ParquetReadOptions = new ParquetReadOptions { BatchSize = batchSize },
                MaxConcurrentReads = 1,
            });
            Assert.Equal(SchemaOf(true).ToString(), table.CurrentSnapshot.ArrowSchema.ToString());
            var rendered = new List<string>();
            await foreach (var b in table.ReadAsync())
            {
                for (int r = 0; r < b.Length; r++)
                    rendered.Add(string.Join("|", Enumerable.Range(0, b.ColumnCount).Select(c => Render(b.Column(c), r))));
                b.Dispose();
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
            rendered.Sort(StringComparer.Ordinal);
            return rendered;
        }

        var whole = await ReadAsync(null);
        var batched = await ReadAsync(batch);
        Assert.Equal(rows + 1, whole.Count);
        Assert.Contains(whole, r => r.Contains("c:null", StringComparison.Ordinal) && r.Contains("b:null", StringComparison.Ordinal));
        Assert.Equal(whole, batched);
    }

    // The most entries any array under `data` holds (its own length included).
    private static int LargestChild(ArrayData data) =>
        Math.Max(data.Length, (data.Children ?? []).Select(LargestChild).DefaultIfEmpty(0).Max());

    // What a column's own rows can hold at any depth: here at most three entries a row at each list or map level.
    private static int MaxEntries(IArrowArray column, int rows)
    {
        int depth = 0;
        for (var type = column.Data.DataType; ;)
        {
            switch (type)
            {
                case ListType l: depth++; type = l.ValueDataType; continue;
                case LargeListType l: depth++; type = l.ValueDataType; continue;
                case MapType m: depth++; type = m.ValueField.DataType; continue;
                case StructType s:
                    type = s.Fields.Select(f => f.DataType).FirstOrDefault(t => t is NestedType and not StructType)
                        ?? s.Fields.Select(f => f.DataType).OfType<StructType>().FirstOrDefault() as IArrowType;
                    if (type is null)
                        return rows * (int)Math.Pow(3, depth);
                    continue;
                default:
                    return rows * (int)Math.Pow(3, depth);
            }
        }
    }

    // A column of `type`, `length` rows, with nulls at every level and 0-3 entries a list or map row.
    private static IArrowArray Build(IArrowType type, int length, ref int seed)
    {
        var validity = new ArrowBuffer.BitmapBuilder(length);
        int nulls = 0;
        for (int i = 0; i < length; i++)
        {
            bool valid = (seed++ * 7 + i) % 5 != 0;
            validity.Append(valid);
            nulls += valid ? 0 : 1;
        }
        var bitmap = validity.Build();

        switch (type)
        {
            case StructType st:
            {
                var children = new List<IArrowArray>();
                foreach (var f in st.Fields)
                    children.Add(Build(f.DataType, length, ref seed));
                return new StructArray(st, length, children, bitmap, nulls);
            }
            case ListType or LargeListType or MapType:
            {
                var counts = new int[length];
                for (int i = 0; i < length; i++)
                    counts[i] = (seed + i * 3) % 4;
                int total = counts.Sum();
                var child = type switch
                {
                    ListType l => Build(l.ValueDataType, total, ref seed),
                    LargeListType l => Build(l.ValueDataType, total, ref seed),
                    _ => null,
                };
                if (type is LargeListType large)
                {
                    var offsets = new ArrowBuffer.Builder<long>(length + 1);
                    long at = 0;
                    offsets.Append(0);
                    foreach (int c in counts)
                        offsets.Append(at += c);
                    return new LargeListArray(large, length, offsets.Build(), child!, bitmap, nulls);
                }
                var offsets32 = new ArrowBuffer.Builder<int>(length + 1);
                int at32 = 0;
                offsets32.Append(0);
                foreach (int c in counts)
                    offsets32.Append(at32 += c);
                if (type is ListType list)
                    return new ListArray(list, length, offsets32.Build(), child!, bitmap, nulls);
                var map = (MapType)type;
                var keys = new StringArray.Builder();
                for (int i = 0; i < total; i++)
                    keys.Append($"k{i}");
                var entries = new StructArray(map.KeyValueType, total,
                    [keys.Build(), Build(map.ValueField.DataType, total, ref seed)], ArrowBuffer.Empty, 0);
                return new MapArray(map, length, offsets32.Build(), entries, bitmap, nulls);
            }
            case StringType:
            {
                var b = new StringArray.Builder();
                for (int i = 0; i < length; i++)
                {
                    if ((seed++ + i) % 6 == 0)
                        b.AppendNull();
                    else
                        b.Append($"s{seed}");
                }
                return b.Build();
            }
            default:
            {
                var b = new Int32Array.Builder();
                for (int i = 0; i < length; i++)
                {
                    if ((seed++ + i) % 6 == 0)
                        b.AppendNull();
                    else
                        b.Append(seed * 1000 + i);
                }
                return b.Build();
            }
        }
    }

    private static string Render(IArrowArray array, int row)
    {
        if (array.IsNull(row))
            return "null";
        switch (array)
        {
            case StructArray s:
            {
                var type = (StructType)s.Data.DataType;
                var sb = new StringBuilder("{");
                for (int i = 0; i < type.Fields.Count; i++)
                    sb.Append(type.Fields[i].Name).Append(':').Append(Render(s.Fields[i], row)).Append(',');
                return sb.Append('}').ToString();
            }
            case ListArray l:
                return Elements(l.GetSlicedValues(row));
            case LargeListArray l:
                return Elements(l.GetSlicedValues(row));
            case Int32Array i:
                return i.GetValue(row)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
            case Int64Array i:
                return i.GetValue(row)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
            case StringArray s:
                return "'" + s.GetString(row) + "'";
            default:
                throw new NotSupportedException(array.GetType().Name);
        }

        static string Elements(IArrowArray values) =>
            "[" + string.Join(",", Enumerable.Range(0, values.Length).Select(i => Render(values, i))) + "]";
    }
}
