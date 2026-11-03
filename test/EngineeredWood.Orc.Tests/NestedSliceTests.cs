// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;

namespace EngineeredWood.Orc.Tests;

/// <summary>
/// Nested columns whose child arrays hold more than their rows reach: sliced arrays, and null list or map entries
/// with a non-empty child range (which Arrow permits). Only the children of the rows written may reach the file
/// (#585).
/// </summary>
public class NestedSliceTests
{
    private static string TempPath() => Path.Combine(Path.GetTempPath(), $"ew-nested-{Guid.NewGuid():N}.orc");

    /// <summary>Row <paramref name="i"/> of <paramref name="array"/>, as text, through Arrow's own accessors.</summary>
    internal static string Render(IArrowArray array, int i)
    {
        if (!array.IsValid(i))
            return "null";
        switch (array)
        {
            case Int32Array ints:
                return ints.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
            case StringArray strings:
                return "\"" + strings.GetString(i) + "\"";
            // A union row renders as the value it selects.
            case DenseUnionArray union:
                return Render(union.Fields[union.TypeIds[i]], union.ValueOffsets[i]);
            case MapArray map:
            {
                var kv = map.KeyValues;
                var parts = new List<string>();
                for (int j = map.ValueOffsets[i]; j < map.ValueOffsets[i + 1]; j++)
                    parts.Add(Render(kv.Fields[0], j) + ":" + Render(kv.Fields[1], j));
                return "{" + string.Join(",", parts) + "}";
            }
            case ListArray list:
            {
                var parts = new List<string>();
                for (int j = list.ValueOffsets[i]; j < list.ValueOffsets[i + 1]; j++)
                    parts.Add(Render(list.Values, j));
                return "[" + string.Join(",", parts) + "]";
            }
            case StructArray s:
            {
                var type = (StructType)s.Data.DataType;
                var parts = new List<string>();
                for (int f = 0; f < s.Fields.Count; f++)
                    parts.Add(type.Fields[f].Name + "=" + Render(s.Fields[f], i));
                return "(" + string.Join(",", parts) + ")";
            }
            default:
                throw new NotSupportedException(array.GetType().Name);
        }
    }

    private static List<string> Rows(IArrowArray array) =>
        Enumerable.Range(0, array.Length).Select(i => Render(array, i)).ToList();

    /// <summary>The slice, after checking that the rendering sees it as the original's rows.</summary>
    private static IArrowArray Slice(IArrowArray array, int offset, int length)
    {
        var slice = ArrowArrayFactory.Slice(array, offset, length);
        Assert.Equal(Rows(array).Skip(offset).Take(length), Rows(slice));
        return slice;
    }

    private static ArrowBuffer Offsets(params int[] offsets) => new ArrowBuffer.Builder<int>().AppendRange(offsets).Build();

    private static ArrowBuffer Validity(params bool[] valid)
    {
        var builder = new ArrowBuffer.BitmapBuilder();
        foreach (var v in valid)
            builder.Append(v);
        return builder.Build();
    }

    private static Int32Array Ints(int count, int first = 1) =>
        new Int32Array.Builder().AppendRange(Enumerable.Range(first, count)).Build();

    private static readonly ListType IntList = new(new Field("item", Int32Type.Default, true));

    /// <summary>[[1,2],[3],[],[4,5,6],[7],[8,9]]</summary>
    private static ListArray SixLists() =>
        new(IntList, 6, Offsets(0, 2, 3, 3, 6, 7, 9), Ints(9), ArrowBuffer.Empty, 0);

    private static readonly StructType Pair = new([
        new Field("a", Int32Type.Default, true),
        new Field("b", StringType.Default, true),
    ]);

    private static StructArray Pairs(int count) => new(Pair, count,
        [Ints(count), new StringArray.Builder().AppendRange(Enumerable.Range(0, count).Select(i => "s" + i)).Build()],
        ArrowBuffer.Empty, 0);

    private static readonly MapType IntToString = new(
        new Field("key", Int32Type.Default, false), new Field("value", StringType.Default, true));

    private static StructArray Entries(int count) => new(
        new StructType([IntToString.KeyField, IntToString.ValueField]), count,
        [Ints(count), new StringArray.Builder().AppendRange(Enumerable.Range(0, count).Select(i => "v" + i)).Build()],
        ArrowBuffer.Empty, 0);

    private static readonly UnionType IntOrString = new(
        [new Field("i", Int32Type.Default, true), new Field("s", StringType.Default, true)], [0, 1], UnionMode.Dense);

    /// <summary>A dense union of 3 rows: 7, "x", 8.</summary>
    private static DenseUnionArray IntsAndStrings() => new(IntOrString, 3,
        [new Int32Array.Builder().Append(7).Append(8).Build(), new StringArray.Builder().Append("x").Build()],
        new ArrowBuffer.Builder<byte>().AppendRange([0, 1, 0]).Build(), Offsets(0, 0, 1));

    // ── The cases ────────────────────────────────────────────────────────────────────────────────────────

    private static readonly Dictionary<string, Func<IArrowArray>> Cases = new()
    {
        ["SlicedList"] = () => Slice(SixLists(), 2, 3),
        // [[1,2], null (over 3,4), [5]]
        ["NullListEntryWithChildren"] = () =>
            new ListArray(IntList, 3, Offsets(0, 2, 4, 5), Ints(5), Validity(true, false, true), 1),
        // [[1], null (2,3), [4,5], null (6), [], [7,8], null (9)], sliced to its middle five.
        ["SlicedListWithNullEntriesOverChildren"] = () => Slice(
            new ListArray(IntList, 7, Offsets(0, 1, 3, 5, 6, 6, 8, 9), Ints(9),
                Validity(true, false, true, false, true, true, false), 3), 1, 5),
        // [[[1,2],[3]], [[]], [[4,5,6]], [[7],[8,9]]]
        ["SlicedListOfLists"] = () => Slice(
            new ListArray(new ListType(new Field("item", IntList, true)), 4, Offsets(0, 2, 3, 4, 6), SixLists(),
                ArrowBuffer.Empty, 0), 1, 3),
        ["SlicedListOfStructs"] = () => Slice(
            new ListArray(new ListType(new Field("item", Pair, true)), 4, Offsets(0, 2, 3, 3, 6), Pairs(6),
                ArrowBuffer.Empty, 0), 1, 3),
        ["SlicedStruct"] = () => Slice(Pairs(6), 2, 3),
        ["NullStructRow"] = () => new StructArray(Pair, 4, Pairs(4).Fields, Validity(true, false, true, true), 1),
        ["SlicedStructWithNullRows"] = () => Slice(
            new StructArray(Pair, 6, Pairs(6).Fields, Validity(true, false, true, false, true, true), 2), 1, 4),
        ["SlicedStructOfLists"] = () => Slice(
            new StructArray(new StructType([new Field("l", IntList, true), new Field("x", Int32Type.Default, true)]),
                6, [SixLists(), Ints(6, first: 100)], ArrowBuffer.Empty, 0), 2, 3),
        // A null struct row over a list: the list has no entry for it.
        ["NullStructRowOverAList"] = () => new StructArray(
            new StructType([new Field("l", IntList, true), new Field("x", Int32Type.Default, true)]),
            4, [new ListArray(IntList, 4, Offsets(0, 2, 3, 3, 6), Ints(6), ArrowBuffer.Empty, 0), Ints(4, first: 100)],
            Validity(true, false, true, true), 1),
        ["SlicedMap"] = () => Slice(
            new MapArray(IntToString, 5, Offsets(0, 2, 3, 3, 5, 6), Entries(6), ArrowBuffer.Empty, 0), 1, 3),
        // Tags [0,1,0,0,1,0] over ints [10..13] and strings ["a","b"], sliced to [1,0,0,1]: the slice points at
        // ints 11, 12 and strings "a", "b", but the children hold all six values.
        ["SlicedDenseUnion"] = () => Slice(new DenseUnionArray(
            new UnionType([new Field("i", Int32Type.Default, true), new Field("s", StringType.Default, true)], [0, 1],
                UnionMode.Dense),
            6, [Ints(4, first: 10), new StringArray.Builder().Append("a").Append("b").Build()],
            new ArrowBuffer.Builder<byte>().AppendRange([0, 1, 0, 0, 1, 0]).Build(), Offsets(0, 0, 1, 2, 1, 3)), 1, 4),
        ["NullMapEntryWithChildren"] = () =>
            new MapArray(IntToString, 3, Offsets(0, 1, 3, 4), Entries(4), Validity(true, false, true), 1),
        // The child is written in parts: [10] has no nulls, [null] does. Both parts need PRESENT bits.
        ["StructNullRowThenNullField"] = () => new StructArray(
            new StructType([new Field("x", Int32Type.Default, true)]), 3,
            [new Int32Array.Builder().Append(10).Append(11).AppendNull().Build()], Validity(true, false, true), 1),
        // [[1,2], null (3), [null]]: the element parts are [1,2] and [null].
        ["ListPartsWithChildNullsLater"] = () => new ListArray(IntList, 3, Offsets(0, 2, 3, 4),
            new Int32Array.Builder().Append(1).Append(2).Append(3).AppendNull().Build(), Validity(true, false, true), 1),
        // A union under a nullable struct: nothing to gather, whatever the field's type.
        ["NullStructRowOverAUnion"] = () => new StructArray(
            new StructType([new Field("u", IntOrString, true)]), 3, [IntsAndStrings()], Validity(true, false, true), 1),
        // A sliced dense union whose second branch is a union: a branch is written in parts, never gathered.
        ["SlicedDenseUnionOfAUnion"] = () => Slice(new DenseUnionArray(
            new UnionType([new Field("i", Int32Type.Default, true), new Field("u", IntOrString, true)], [0, 1],
                UnionMode.Dense),
            5, [Ints(3, first: 10), IntsAndStrings()],
            new ArrowBuffer.Builder<byte>().AppendRange([0, 1, 1, 0, 1]).Build(), Offsets(0, 0, 1, 1, 2)), 1, 3),
    };

    public static TheoryData<string> CaseNames()
    {
        var data = new TheoryData<string>();
        foreach (var name in Cases.Keys)
            data.Add(name);
        return data;
    }

    [Theory]
    [MemberData(nameof(CaseNames))]
    public async Task RoundTrips(string name)
    {
        var column = Cases[name]();
        var read = await ReadRows(await Write(column));
        Assert.Equal(Rows(column), read);
    }

    [Fact]
    public async Task AFileEngineeredWoodWroteBeforeTheFix_KeepsItsStructRows()
    {
        // Written by EngineeredWood.Orc 0.2, which wrote a struct's fields for its null rows too.
        var rows = await ReadRows(TestHelpers.GetTestFilePath("engineeredwood-0.2-struct-nulls.orc"), delete: false);
        Assert.Equal(["(a=1,b=\"s0\")", "null", "(a=3,b=\"s2\")", "(a=4,b=\"s3\")"], rows);
    }

    /// <summary>pyarrow reads EngineeredWood's file, and EngineeredWood reads pyarrow's file of the same rows.</summary>
    [SkippableTheory]
    [MemberData(nameof(CaseNames))]
    public async Task CrossValidates_WithPyArrow(string name)
    {
        Skip.IfNot(CrossValidationTests.IsPyArrowAvailable(), "PyArrow is not installed.");
        var column = Cases[name]();
        var path = await Write(column);
        try
        {
            using (var doc = System.Text.Json.JsonDocument.Parse(CrossValidationTests.RunPython(PyRender, path)))
                Assert.Equal(Rows(column), doc.RootElement.EnumerateArray().Select(e => e.GetString()!));

            if (PyArrowCannotWrite.Contains(name))
                return;
            CrossValidationTests.RunPython(PyRewrite, path);
            Assert.Equal(Rows(column), await ReadRows(path + ".py.orc", delete: false));
        }
        finally
        {
            File.Delete(path);
            File.Delete(path + ".py.orc");
        }
    }

    /// <summary>
    /// Cases pyarrow reads, but cannot write back: it reads an ORC union as a sparse union, and its ORC writer
    /// aborts on a C++ check (<c>buffers[0] == nullptr</c> in <c>array_nested.cc</c>) writing one under a struct
    /// with null rows, or nested in another union. It does so for such a table of its own, too (pyarrow 2.2).
    /// </summary>
    private static readonly HashSet<string> PyArrowCannotWrite = ["NullStructRowOverAUnion", "SlicedDenseUnionOfAUnion"];

    /// <summary>Writes the table in <c>argv[1]</c> back out with pyarrow, to <c>argv[1] + ".py.orc"</c>.</summary>
    private const string PyRewrite =
        "import sys, pyarrow.orc as orc\n" +
        "orc.write_table(orc.read_table(sys.argv[1]), sys.argv[1] + '.py.orc')\n";

    /// <summary>Renders column <c>c</c> as <see cref="Render"/> does.</summary>
    private const string PyRender =
        "import sys, json, pyarrow as pa, pyarrow.orc as orc\n" +
        "def r(v, t):\n" +
        "    if v is None: return 'null'\n" +
        "    if pa.types.is_map(t): return '{' + ','.join(r(k, t.key_type) + ':' + r(x, t.item_type) for k, x in v) + '}'\n" +
        "    if pa.types.is_list(t): return '[' + ','.join(r(x, t.value_type) for x in v) + ']'\n" +
        "    if pa.types.is_struct(t): return '(' + ','.join(t.field(i).name + '=' + r(v[t.field(i).name], t.field(i).type) for i in range(t.num_fields)) + ')'\n" +
        "    if pa.types.is_union(t): return '\"' + v + '\"' if isinstance(v, str) else str(v)\n" +
        "    if pa.types.is_string(t): return '\"' + v + '\"'\n" +
        "    return str(v)\n" +
        "t = orc.read_table(sys.argv[1])\n" +
        "col = t.column('c')\n" +
        "rows = [r(v, col.type) for v in col.to_pylist()]\n" +
        "print(json.dumps(rows))\n";

    private static async Task<string> Write(IArrowArray column)
    {
        var schema = new Schema([new Field("c", column.Data.DataType, true)], null);
        var path = TempPath();
        await using var writer = OrcWriter.Create(path, schema);
        await writer.WriteBatchAsync(new RecordBatch(schema, [column], column.Length));
        await writer.CloseAsync();
        return path;
    }

    private static async Task<List<string>> ReadRows(string path, bool delete = true)
    {
        try
        {
            await using var reader = await OrcReader.OpenAsync(path);
            var read = new List<string>();
            await foreach (var batch in reader.CreateRowReader())
                read.AddRange(Rows(batch.Column(0)));
            return read;
        }
        finally
        {
            if (delete)
                File.Delete(path);
        }
    }
}
