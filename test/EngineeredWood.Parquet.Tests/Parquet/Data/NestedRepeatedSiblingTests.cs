// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet.Data;

/// <summary>
/// Reading nested columns whose leaves do not all repeat to the same depth. Two separate places in
/// the assembler assumed one level entry per slot, which holds only while every leaf under the node
/// repeats exactly as often as the node itself. See issue #167.
/// </summary>
/// <remarks>
/// <para>A leaf under an inner list emits one level entry per ELEMENT of that list, so it carries
/// more entries than a sibling leaf without one. That broke the phantom filter — which derived a
/// single keep-list from the subtree's first leaf and applied it to every leaf in the subtree,
/// dropping the wrong entries from the deeper ones — and it broke a struct's validity walk, which
/// read <c>defLevels[i]</c> for slot <c>i</c>.</para>
/// <para>Every expectation below is what PyArrow and DuckDB both return for the same file.</para>
/// </remarks>
public class NestedRepeatedSiblingTests : IDisposable
{
    private readonly string _tempDir;

    public NestedRepeatedSiblingTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-nested-sibling-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
        GC.SuppressFinalize(this);
    }

    // ─── The shapes ────────────────────────────────────────────────────────────

    /// <summary>
    /// The shape from the issue: <c>map&lt;string, struct&lt;H: struct&lt;i: list&lt;double&gt;&gt;&gt;&gt;</c>.
    /// The first entry's two-element list pushes the level cursor two entries ahead, so the second
    /// entry's <c>H</c> took its validity from inside that list and came back present holding an
    /// empty list instead of null.
    /// </summary>
    [Fact]
    public async Task MapValueStruct_AfterAMultiElementList_KeepsItsOwnNullity()
    {
        var i = ListOf(Doubles(1.0, 2.0), Field("element", DoubleType.Default), [0, 2, 2], true, true);
        var h = StructOf([Field("i", i.Data.DataType)], [i], true, false);
        var value = StructOf([Field("H", h.Data.DataType)], [h], true, true);
        var map = MapOf(Strings("a", "e"), value, [0, 2, 2], true, true);

        Assert.Equal(
            "[[a={H: {i: [1.0, 2.0]}}, e={H: null}], []]",
            Render(await RoundTripAsync(map, "map_struct_deep")));
    }

    /// <summary>
    /// <c>list&lt;struct&lt;a: int32, b: list&lt;int32&gt;&gt;&gt;</c>. Here what is lost is data rather than
    /// a null: the last row's <c>b</c> came back <c>[]</c> and the 30 in it was simply gone.
    /// </summary>
    [Fact]
    public async Task ListOfStruct_WithARepeatedSibling_KeepsEveryElement()
    {
        // [{a: 1, b: [10, 20]}, {a: null, b: null}] | null | [] | [{a: 3, b: [30]}]
        var a = Ints(1, null, 3);
        var b = ListOf(Ints(10, 20, 30), Field("element", Int32Type.Default), [0, 2, 2, 3], true, false, true);
        var element = StructOf([Field("a", Int32Type.Default), Field("b", b.Data.DataType)], [a, b], true, true, true);
        var list = ListOf(element, Field("element", element.Data.DataType), [0, 2, 2, 2, 3], true, false, true, true);

        Assert.Equal(
            "[[{a: 1, b: [10, 20]}, {a: null, b: null}], null, [], [{a: 3, b: [30]}]]",
            Render(await RoundTripAsync(list, "list_struct_sibling")));
    }

    /// <summary>
    /// The same sibling shape under a map: <c>map&lt;string, struct&lt;a: int32, b: list&lt;int32&gt;&gt;&gt;</c>.
    /// <c>k2</c>'s empty list read back null, and <c>k4</c>'s <c>[40]</c> read back empty.
    /// </summary>
    [Fact]
    public async Task MapOfStruct_WithARepeatedSibling_KeepsEveryElement()
    {
        // {k1: {a: 1, b: [10, 20, 30]}, k2: {a: 2, b: []}, k3: null} | {} | {k4: {a: null, b: [40]}}
        var a = Ints(1, 2, null, null);
        var b = ListOf(Ints(10, 20, 30, 40), Field("element", Int32Type.Default), [0, 3, 3, 3, 4],
            true, true, false, true);
        var value = StructOf([Field("a", Int32Type.Default), Field("b", b.Data.DataType)], [a, b],
            true, true, false, true);
        var map = MapOf(Strings("k1", "k2", "k3", "k4"), value, [0, 3, 3, 4], true, true, true);

        Assert.Equal(
            "[[k1={a: 1, b: [10, 20, 30]}, k2={a: 2, b: []}, k3=null], [], [k4={a: null, b: [40]}]]",
            Render(await RoundTripAsync(map, "map_struct_sibling")));
    }

    /// <summary>
    /// No list or map above the struct at all: <c>struct&lt;a: int32, s: struct&lt;b: list&lt;int32&gt;&gt;&gt;</c>.
    /// Row 0's three-element list shifts everything after it, so rows 1 and 3 traded nullity — row 1's
    /// null inner struct read as present, and row 3's present one read as null.
    /// </summary>
    [Fact]
    public async Task StructInStruct_WithAListBelow_TakesNullityFromItsOwnSlot()
    {
        // {a: 1, s: {b: [1, 2, 3]}} | {a: 2, s: null} | null | {a: 3, s: {b: null}}
        var b = ListOf(Ints(1, 2, 3), Field("element", Int32Type.Default), [0, 3, 3, 3, 3],
            true, false, false, false);
        var s = StructOf([Field("b", b.Data.DataType)], [b], true, false, false, true);
        var outer = StructOf([Field("a", Int32Type.Default), Field("s", s.Data.DataType)],
            [Ints(1, 2, null, 3), s], true, true, false, true);

        Assert.Equal(
            "[{a: 1, s: {b: [1, 2, 3]}}, {a: 2, s: null}, null, {a: 3, s: {b: null}}]",
            Render(await RoundTripAsync(outer, "struct_in_struct")));
    }

    /// <summary>
    /// The corpus file the issue was found on. <c>nested_struct.g</c> is the
    /// <c>map&lt;string, struct&lt;H: struct&lt;i: list&lt;double&gt;&gt;&gt;&gt;</c> column, and row 1 exercises the
    /// whole range at once: a list with a null element, an empty list, a null entry value, a null
    /// list, and a null <c>H</c> — the last of which only surfaces once the two-element list ahead of
    /// it has moved the cursor.
    /// </summary>
    [Fact]
    public async Task NullableImpala_NestedStructG_MatchesTheOtherReaders()
    {
        await using var file = new LocalRandomAccessFile(TestData.GetPath("nullable.impala.parquet"));
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);

        var nested = (StructArray)batch.Column(batch.Schema.GetFieldIndex("nested_struct"));
        var g = nested.Fields[((StructType)nested.Data.DataType).GetFieldIndex("g")];

        Assert.Equal(
            "[g1={H: {i: [2.2, null]}}, g2={H: {i: []}}, g3=null, g4={H: {i: null}}, g5={H: null}]",
            RenderValue(g, 1));
        Assert.Equal("[foo={H: {i: [2.2, 3.3]}}]", RenderValue(g, 4));
    }

    // ─── Round trip ────────────────────────────────────────────────────────────

    private async Task<IArrowArray> RoundTripAsync(IArrowArray column, string name)
    {
        var schema = new Apache.Arrow.Schema([new Field("c", column.Data.DataType, nullable: true)], null);
        string path = Path.Combine(_tempDir, name + ".parquet");

        await using (var sink = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(sink, ownsFile: false,
            new ParquetWriteOptions { Compression = CompressionCodec.Uncompressed }))
        {
            await writer.WriteRowGroupAsync(new RecordBatch(schema, [column], column.Length));
            await writer.CloseAsync();
        }

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var batch = await reader.ReadRowGroupAsync(0);
        return batch.Column(0);
    }

    // ─── Building ──────────────────────────────────────────────────────────────

    private static Field Field(string name, IArrowType type) => new(name, type, nullable: true);

    private static (ArrowBuffer Bitmap, int NullCount) Validity(bool[] valid)
    {
        var builder = new ArrowBuffer.BitmapBuilder();
        int nullCount = 0;
        foreach (bool v in valid)
        {
            builder.Append(v);
            if (!v) nullCount++;
        }
        return (builder.Build(), nullCount);
    }

    private static ArrowBuffer Offsets(int[] offsets)
    {
        var builder = new ArrowBuffer.Builder<int>();
        foreach (int o in offsets) builder.Append(o);
        return builder.Build();
    }

    private static Int32Array Ints(params int?[] values)
    {
        var builder = new Int32Array.Builder();
        foreach (int? v in values) builder.Append(v);
        return builder.Build();
    }

    private static DoubleArray Doubles(params double?[] values)
    {
        var builder = new DoubleArray.Builder();
        foreach (double? v in values) builder.Append(v);
        return builder.Build();
    }

    private static StringArray Strings(params string[] values)
    {
        var builder = new StringArray.Builder();
        foreach (string v in values) builder.Append(v);
        return builder.Build();
    }

    private static ListArray ListOf(IArrowArray values, Field itemField, int[] offsets, params bool[] valid)
    {
        var (bitmap, nullCount) = Validity(valid);
        return new ListArray(new ListType(itemField), valid.Length, Offsets(offsets), values, bitmap, nullCount);
    }

    private static StructArray StructOf(Field[] fields, IArrowArray[] children, params bool[] valid)
    {
        var (bitmap, nullCount) = Validity(valid);
        return new StructArray(new StructType(fields), valid.Length, children, bitmap, nullCount);
    }

    private static MapArray MapOf(StringArray keys, IArrowArray values, int[] offsets, params bool[] valid)
    {
        var keyField = new Field("key", StringType.Default, nullable: false);
        var valueField = Field("value", values.Data.DataType);
        var entries = new StructArray(new StructType([keyField, valueField]), keys.Length,
            [keys, values], ArrowBuffer.Empty, nullCount: 0);
        var (bitmap, nullCount) = Validity(valid);
        return new MapArray(new MapType(keyField, valueField), valid.Length,
            Offsets(offsets), entries, bitmap, nullCount);
    }

    // ─── Rendering ─────────────────────────────────────────────────────────────

    /// <summary>
    /// Renders a column as <c>[slot, slot, ...]</c>: a list as <c>[a, b]</c>, a map as
    /// <c>[key=value, ...]</c>, a struct as <c>{name: value, ...}</c>, and a null as <c>null</c>.
    /// Deep nesting is the whole subject here, and asserting value by value would bury the one
    /// difference each test exists to show.
    /// </summary>
    private static string Render(IArrowArray array) =>
        "[" + string.Join(", ", Enumerable.Range(0, array.Length).Select(i => RenderValue(array, i))) + "]";

    private static string RenderValue(IArrowArray array, int index)
    {
        if (array.IsNull(index)) return "null";

        switch (array)
        {
            case MapArray map:
                return "[" + string.Join(", ", Span(map, index)
                    .Select(j => $"{RenderValue(map.Keys, j)}={RenderValue(map.Values, j)}")) + "]";
            case ListArray list:
                return "[" + string.Join(", ", Span(list, index)
                    .Select(j => RenderValue(list.Values, j))) + "]";
            case StructArray structArray:
                var type = (StructType)structArray.Data.DataType;
                return "{" + string.Join(", ", Enumerable.Range(0, structArray.Fields.Count)
                    .Select(f => $"{type.Fields[f].Name}: {RenderValue(structArray.Fields[f], index)}")) + "}";
            case StringArray strings:
                return strings.GetString(index)!;
            case Int32Array ints:
                return ints.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture);
            case DoubleArray doubles:
                return doubles.GetValue(index)!.Value.ToString("0.0###############", CultureInfo.InvariantCulture);
        }

        throw new NotSupportedException($"No rendering for {array.GetType().Name}.");
    }

    private static IEnumerable<int> Span(ListArray list, int index) =>
        Enumerable.Range(list.ValueOffsets[index], list.ValueOffsets[index + 1] - list.ValueOffsets[index]);
}
