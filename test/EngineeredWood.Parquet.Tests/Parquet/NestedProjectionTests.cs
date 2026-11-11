// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Operations.VariantJson;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// #783: a projection that names leaves of a nested column by dotted path. A struct comes back holding only the
/// leaves named (in schema order, whatever order they were named in), as Spark and arrow-rs return a nested projection.
/// A leaf under a list, a map or another repeated group brings back that whole group. Before the fix,
/// <c>["pt.x"]</c> threw <see cref="IndexOutOfRangeException"/> in the assembler, and <c>["pt.y", "pt.x"]</c> handed
/// one leaf's values to the other.
/// </summary>
/// <remarks>
/// The oracle is the whole column read as it is, rendered with only the selected fields kept. Every read path is
/// asked: a row group, all row groups, all row groups in small batches, and with positions.
/// </remarks>
public class NestedProjectionTests : IDisposable
{
    private const int Rows = 50;
    private const int RowsPerGroup = 25;

    private readonly string _tempDir;

    public NestedProjectionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "ew-nested-proj-" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { Directory.Delete(_tempDir, recursive: true); } catch (IOException) { }
    }

    public static IEnumerable<object[]> Projections() =>
    [
        [new[] { "pt.x" }],
        [new[] { "pt.y" }],
        [new[] { "id", "pt.x" }],
        [new[] { "pt.y", "pt.x" }],             // named out of schema order
        [new[] { "pt.inner.v", "pt.inner.u", "pt.y", "pt.x" }], // every leaf, reversed: nothing pruned, all reordered
        [new[] { "pt.inner.v" }],               // two structs deep
        [new[] { "pt.x", "pt.inner.u", "id" }],
        [new[] { "pt", "pt.x" }],               // the whole struct and one of its leaves
        [new[] { "pt.x", "pt.x" }],             // a leaf named twice
        [new[] { "l.list.element.a" }],         // under a list: the whole list
        [new[] { "m.key_value.value" }],        // under a map: the whole map
        [new[] { "m", "pt.y", "l" }],
        [new[] { "id", "pt", "id" }],           // a flat column twice beside a nested one: once (review round 1)
        [new[] { "id", "pt.x", "id", "l" }],
    ];

    public static IEnumerable<object[]> Cases()
    {
        foreach (var projection in Projections())
        {
            foreach (string read in new[] { "row group", "all", "all, batched", "positions" })
                yield return [projection[0], read];
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task ADottedProjectionReturnsTheSelectedLeavesOfTheirColumns(string[] projection, string read)
    {
        string path = await WriteFileAsync();
        var expected = await ExpectedAsync(path, projection);
        var actual = await ReadAsync(path, projection, read);
        Assert.Equal(expected.Schema, actual.Schema);
        Assert.Equal(expected.Rows, actual.Rows);
    }

    /// <summary>The struct holds only the leaves named, in schema order.</summary>
    [Fact]
    public async Task APrunedStructHoldsOnlyTheNamedLeavesInSchemaOrder()
    {
        string path = await WriteFileAsync();
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        using var batch = await reader.ReadRowGroupAsync(0, ["pt.inner.v", "pt.x"]);

        var pt = Assert.Single(batch.Schema.FieldsList);
        Assert.Equal("pt", pt.Name);
        var fields = ((StructType)pt.DataType).Fields;
        Assert.Equal(["x", "inner"], fields.Select(f => f.Name));
        Assert.Equal(["v"], ((StructType)fields[1].DataType).Fields.Select(f => f.Name));
    }

    /// <summary>
    /// A pruned struct's leaves keep the time zone the writer declared for them in <c>ARROW:schema</c>, matched by
    /// field name: projecting <c>pt.b</c> alone restores b's zone, not a's, and not none (review round 1).
    /// </summary>
    [Theory]
    [InlineData("pt.a", "America/New_York")]
    [InlineData("pt.b", "Asia/Tokyo")]
    public async Task APrunedStructsTimestampKeepsItsOwnDeclaredZone(string leaf, string zone)
    {
        string path = Path.Combine(_tempDir, "zones.parquet");
        var ptType = new StructType([
            new Field("a", new TimestampType(TimeUnit.Microsecond, "America/New_York"), true),
            new Field("b", new TimestampType(TimeUnit.Microsecond, "Asia/Tokyo"), true)]);
        var schema = new Apache.Arrow.Schema([new Field("pt", ptType, true)], null);
        var a = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "America/New_York"));
        var b = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "Asia/Tokyo"));
        for (int r = 0; r < 3; r++)
        {
            a.Append(new DateTimeOffset(2026, 1, 1 + r, 0, 0, 0, TimeSpan.Zero));
            b.Append(new DateTimeOffset(2026, 2, 1 + r, 0, 0, 0, TimeSpan.Zero));
        }

        await using (var output = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(output, ownsFile: false))
        {
            using var batch = new RecordBatch(schema,
                [new StructArray(ptType, 3, [a.Build(), b.Build()], ArrowBuffer.Empty, 0)], 3);
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);

        // The whole struct, as a premise: the zones round-trip at all.
        using (var whole = await reader.ReadRowGroupAsync(0, ["pt"]))
        {
            var wholeFields = ((StructType)whole.Schema.FieldsList[0].DataType).Fields;
            Assert.Equal("America/New_York", ((TimestampType)wholeFields[0].DataType).Timezone);
            Assert.Equal("Asia/Tokyo", ((TimestampType)wholeFields[1].DataType).Timezone);
        }

        using var pruned = await reader.ReadRowGroupAsync(0, [leaf]);
        var field = Assert.Single(((StructType)pruned.Schema.FieldsList[0].DataType).Fields);
        Assert.Equal(leaf[3..], field.Name);
        Assert.Equal(zone, ((TimestampType)field.DataType).Timezone);
        var column = (StructArray)pruned.Column(0);
        Assert.Equal(zone, ((TimestampType)column.Fields[0].Data.DataType).Timezone);
    }

    public static IEnumerable<object[]> RepeatedCases()
    {
        foreach (var projection in new[]
        {
            new[] { "Int32_list", "Int32_list", "String_list" },
            new[] { "String_list", "Int32_list", "String_list" },
        })
        {
            foreach (string read in new[] { "row group", "all", "all, batched", "positions" })
                yield return [projection, read];
        }
    }

    /// <summary>
    /// A bare repeated top-level column (a list with no LIST annotation) has a one-segment path but is assembled as
    /// nested, so naming a column twice beside one reads it once, as beside a struct (review round 2: the second copy
    /// was assembled as the next column).
    /// </summary>
    [Theory]
    [MemberData(nameof(RepeatedCases))]
    public async Task ABareRepeatedColumnNamedTwiceIsReadOnce(string[] projection, string read)
    {
        string path = TestData.GetPath("repeated_primitive_no_list.parquet");
        var expected = await ExpectedAsync(path, projection);
        var actual = await ReadAsync(path, projection, read);
        Assert.Equal(expected.Schema, actual.Schema);
        Assert.Equal(expected.Rows, actual.Rows);
    }

    /// <summary>
    /// A variant written without the VARIANT annotation (a binary <c>metadata</c> beside a binary <c>value</c>, as Spark
    /// 4.0 writes it) is kept whole like an annotated one: a leaf under it selects the whole group, since a variant's
    /// parts mean nothing apart (review round 2: <c>v.metadata</c> came back without <c>value</c>).
    /// </summary>
    [Theory]
    [InlineData("v.metadata")]
    [InlineData("v.value")]
    public async Task AnUnannotatedVariantIsKeptWhole(string leaf)
    {
        string path = Path.Combine(_tempDir, "variant.parquet");
        var vType = new StructType([
            new Field("metadata", BinaryType.Default, false),
            new Field("value", BinaryType.Default, true)]);
        var schema = new Apache.Arrow.Schema([new Field("id", Int32Type.Default, false), new Field("v", vType, true)], null);
        var metadata = new BinaryArray.Builder();
        var value = new BinaryArray.Builder();
        var ids = new Int32Array.Builder();
        for (int r = 0; r < 3; r++)
        {
            ids.Append(r);
            metadata.Append(new byte[] { 1, 0, 0 });
            value.Append(new byte[] { 0x0C, (byte)r }); // int8 r
        }

        await using (var output = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(output, ownsFile: false))
        {
            using var batch = new RecordBatch(schema,
                [ids.Build(), new StructArray(vType, 3, [metadata.Build(), value.Build()], ArrowBuffer.Empty, 0)], 3);
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        using var whole = await reader.ReadRowGroupAsync(0, ["v"]);
        using var projected = await reader.ReadRowGroupAsync(0, [leaf]);
        Assert.Equal(RenderSchema(whole.Schema, null), RenderSchema(projected.Schema, null));
        for (int r = 0; r < 3; r++)
            Assert.Equal(RenderRow(whole, r, null), RenderRow(projected, r, null));
    }

    /// <summary>
    /// An annotated, shredded variant inside a struct, read with the extension registry so the variant is wrapped and
    /// reassembled (<c>VariantNestedWrapper</c>): any leaf under it selects the whole variant, which reads back as it
    /// does in a whole read; the struct's other field is pruned; and a projection of that other field leaves the
    /// variant out (review round 3).
    /// </summary>
    [Fact]
    public async Task AVariantInsideAStructIsKeptWholeAndStillWrapped()
    {
        VariantValue Obj(int a, string b) => VariantValue.FromObject(
            new Dictionary<string, VariantValue> { ["a"] = VariantValue.FromInt32(a), ["b"] = VariantValue.FromString(b) });
        var values = new[] { Obj(1, "x"), VariantValue.Null, Obj(3, "z") };
        Assert.True(VariantShredding.TryShred(values, [false, true, false], out var shredded));
        Assert.True(shredded.IsShredded);

        var sType = new StructType([new Field("id", Int32Type.Default, false), new Field("v", shredded.Data.DataType, true)]);
        var ids = new Int32Array.Builder().Append(10).Append(11).Append(12).Build();
        var schema = new Apache.Arrow.Schema([new Field("s", sType, true)], null);
        string path = Path.Combine(_tempDir, "nested_variant.parquet");
        await using (var output = new LocalSequentialFile(path))
        await using (var writer = new ParquetFileWriter(output, ownsFile: false))
        {
            using var batch = new RecordBatch(schema, [new StructArray(sType, 3, [ids, shredded], ArrowBuffer.Empty, 0)], 3);
            await writer.WriteRowGroupAsync(batch);
            await writer.CloseAsync();
        }

        var registry = new ExtensionTypeRegistry();
        registry.Register(VariantExtensionDefinition.Instance);
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false, new ParquetReadOptions { ExtensionRegistry = registry });

        static string Json(VariantArray array, int row) =>
            array.IsNull(row) ? "null" : VariantJsonWriter.ToJson(array.GetLogicalVariantValue(row), indented: false);

        using var whole = await reader.ReadRowGroupAsync(0, ["s"]);
        var wholeVariant = Assert.IsType<VariantArray>(((StructArray)whole.Column(0)).Fields[1]);
        var expected = Enumerable.Range(0, 3).Select(r => Json(wholeVariant, r)).ToList();
        Assert.Equal(VariantJsonWriter.ToJson(values[0], indented: false), expected[0]); // premise: the whole read is right

        var leaves = (await reader.GetSchemaAsync()).Columns
            .Select(c => c.DottedPath).Where(p => p.StartsWith("s.v.", StringComparison.Ordinal)).ToList();
        Assert.True(leaves.Count >= 4, string.Join(", ", leaves)); // metadata, value, and the shredded a and b
        foreach (string leaf in leaves)
        {
            using var projected = await reader.ReadRowGroupAsync(0, [leaf]);
            var sField = (StructType)projected.Schema.FieldsList[0].DataType;
            var vField = Assert.Single(sField.Fields);
            Assert.Equal("v", vField.Name);
            Assert.IsType<VariantType>(vField.DataType);
            var variant = Assert.IsType<VariantArray>(Assert.Single(((StructArray)projected.Column(0)).Fields));
            Assert.Equal(expected, Enumerable.Range(0, 3).Select(r => Json(variant, r)).ToList());
        }

        using var idOnly = await reader.ReadRowGroupAsync(0, ["s.id"]);
        Assert.Equal("id", Assert.Single(((StructType)idOnly.Schema.FieldsList[0].DataType).Fields).Name);
        Assert.Equal([10, 11, 12], ((Int32Array)((StructArray)idOnly.Column(0)).Fields[0]).Values.ToArray());
    }

    // ---- The oracle and the reads ------------------------------------------------------------------------------

    private sealed record Rendered(string Schema, List<string> Rows);

    /// <summary>The whole columns, read as they are, rendered keeping only the selected fields.</summary>
    private static async Task<Rendered> ExpectedAsync(string path, string[] projection)
    {
        var tops = new List<string>();
        foreach (var name in projection)
        {
            string top = name.Split('.')[0];
            if (!tops.Contains(top))
                tops.Add(top);
        }

        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false);
        var rows = new List<string>();
        string? schema = null;
        await foreach (var batch in reader.ReadAllAsync(tops))
        {
            using (batch)
            {
                schema ??= RenderSchema(batch.Schema, projection);
                for (int r = 0; r < batch.Length; r++)
                    rows.Add(RenderRow(batch, r, projection));
            }
        }

        return new Rendered(schema!, rows);
    }

    private static async Task<Rendered> ReadAsync(string path, string[] projection, string read)
    {
        await using var file = new LocalRandomAccessFile(path);
        using var reader = new ParquetFileReader(file, ownsFile: false,
            read == "all, batched" ? new ParquetReadOptions { BatchSize = 7 } : null);
        var rows = new List<string>();
        string? schema = null;
        void Add(RecordBatch batch)
        {
            schema ??= RenderSchema(batch.Schema, null);
            for (int r = 0; r < batch.Length; r++)
                rows.Add(RenderRow(batch, r, null));
        }

        switch (read)
        {
            case "row group":
                int groups = (await reader.ReadMetadataAsync()).RowGroups.Count;
                for (int g = 0; g < groups; g++)
                {
                    using var batch = await reader.ReadRowGroupAsync(g, projection);
                    Add(batch);
                }

                break;
            case "positions":
                await foreach (var positioned in reader.ReadWithPositionsAsync(null, projection))
                {
                    using var batch = positioned.Batch;
                    Add(batch);
                }

                break;
            default:
                await foreach (var batch in reader.ReadAllAsync(projection))
                {
                    using (batch)
                        Add(batch);
                }

                break;
        }

        return new Rendered(schema!, rows);
    }

    // Whether the field at `path` survives the projection: it is named, inside something named, or holds something
    // named. A leaf under a list or map keeps the whole list or map, whose fields render under its own path.
    private static bool Keeps(string[]? projection, string path) =>
        projection is null || projection.Any(p =>
            p == path || p.StartsWith(path + ".", StringComparison.Ordinal) || path.StartsWith(p + ".", StringComparison.Ordinal)
            || path.StartsWith("l.", StringComparison.Ordinal) || path.StartsWith("m.", StringComparison.Ordinal));

    private static string RenderSchema(Apache.Arrow.Schema schema, string[]? projection) =>
        string.Join(", ", schema.FieldsList.Select(f => RenderType(f, f.Name, projection)));

    private static string RenderType(Field field, string path, string[]? projection) => field.DataType switch
    {
        StructType s => $"{field.Name}{{{string.Join(", ", s.Fields.Where(c => Keeps(projection, path + "." + c.Name)).Select(c => RenderType(c, path + "." + c.Name, projection)))}}}",
        _ => $"{field.Name}:{field.DataType.Name}",
    };

    private static string RenderRow(RecordBatch batch, int row, string[]? projection)
    {
        var cells = new List<string>();
        for (int c = 0; c < batch.ColumnCount; c++)
        {
            var field = batch.Schema.GetFieldByIndex(c);
            cells.Add(Render(batch.Column(c), row, field.Name, projection));
        }

        return string.Join(" | ", cells);
    }

    private static string Render(IArrowArray array, int row, string path, string[]? projection)
    {
        if (array.IsNull(row))
            return "null";
        switch (array)
        {
            case StructArray s:
            {
                var type = (StructType)s.Data.DataType;
                var parts = new List<string>();
                for (int i = 0; i < type.Fields.Count; i++)
                {
                    string child = path + "." + type.Fields[i].Name;
                    if (Keeps(projection, child))
                        parts.Add(type.Fields[i].Name + "=" + Render(s.Fields[i], row, child, projection));
                }

                return "{" + string.Join(", ", parts) + "}";
            }

            case MapArray m:
            {
                int start = m.ValueOffsets[row], end = m.ValueOffsets[row + 1];
                var sb = new StringBuilder("map[");
                for (int i = start; i < end; i++)
                    sb.Append(Render(m.Keys, i, path, null)).Append("->").Append(Render(m.Values, i, path, null)).Append(';');
                return sb.Append(']').ToString();
            }

            case ListArray l:
            {
                int start = l.ValueOffsets[row], end = l.ValueOffsets[row + 1];
                var items = new List<string>();
                for (int i = start; i < end; i++)
                    items.Add(Render(l.Values, i, path, null));
                return "[" + string.Join(", ", items) + "]";
            }

            case Int32Array a:
                return a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture);
            case Int64Array a:
                return a.GetValue(row)!.Value.ToString(CultureInfo.InvariantCulture);
            case StringArray a:
                return a.GetString(row);
            default:
                return array.Data.DataType.Name;
        }
    }

    // ---- The file ----------------------------------------------------------------------------------------------

    /// <summary>
    /// id; pt, a nullable struct of x, y and inner { u, v }, NULL every 7th row; l, a list of structs { a, b }; m, a map
    /// from string to int. Two row groups.
    /// </summary>
    private async Task<string> WriteFileAsync()
    {
        string path = Path.Combine(_tempDir, "nested.parquet");
        if (File.Exists(path))
            return path;

        var innerType = new StructType([new Field("u", Int64Type.Default, true), new Field("v", StringType.Default, true)]);
        var ptType = new StructType([
            new Field("x", Int32Type.Default, true), new Field("y", StringType.Default, true), new Field("inner", innerType, true)]);
        var elementType = new StructType([new Field("a", Int32Type.Default, true), new Field("b", StringType.Default, true)]);
        var listType = new ListType(new Field("element", elementType, true));
        var mapType = new MapType(new Field("key", StringType.Default, false), new Field("value", Int32Type.Default, true));
        var schema = new Apache.Arrow.Schema([
            new Field("id", Int32Type.Default, false), new Field("pt", ptType, true),
            new Field("l", listType, true), new Field("m", mapType, true)], null);

        await using var output = new LocalSequentialFile(path);
        await using var writer = new ParquetFileWriter(output, ownsFile: false,
            ParquetWriteOptions.Default with { RowGroupMaxRows = RowsPerGroup });
        for (int g = 0; g < Rows / RowsPerGroup; g++)
        {
            int first = g * RowsPerGroup;
            using var batch = new RecordBatch(schema,
                [Ids(first), Pt(first, ptType, innerType), List(first, listType, elementType), Map(first, mapType)],
                RowsPerGroup);
            await writer.WriteRowGroupAsync(batch);
        }

        await writer.CloseAsync();
        return path;
    }

    private static Int32Array Ids(int first)
    {
        var b = new Int32Array.Builder();
        for (int r = first; r < first + RowsPerGroup; r++)
            b.Append(r);
        return b.Build();
    }

    private static StructArray Pt(int first, StructType ptType, StructType innerType)
    {
        var x = new Int32Array.Builder();
        var y = new StringArray.Builder();
        var u = new Int64Array.Builder();
        var v = new StringArray.Builder();
        var valid = new List<bool>();
        for (int r = first; r < first + RowsPerGroup; r++)
        {
            valid.Add(r % 7 != 0);
            if (r % 5 == 0) x.AppendNull(); else x.Append(r * 10);
            y.Append("y" + r.ToString(CultureInfo.InvariantCulture));
            u.Append(r * 100L);
            if (r % 3 == 0) v.AppendNull(); else v.Append("v" + r.ToString(CultureInfo.InvariantCulture));
        }

        var inner = new StructArray(innerType, RowsPerGroup, [u.Build(), v.Build()], ArrowBuffer.Empty, 0);
        return new StructArray(ptType, RowsPerGroup, [x.Build(), y.Build(), inner], Validity(valid), valid.Count(b => !b));
    }

    private static ListArray List(int first, ListType listType, StructType elementType)
    {
        var a = new Int32Array.Builder();
        var b = new StringArray.Builder();
        var offsets = new List<int> { 0 };
        int items = 0;
        for (int r = first; r < first + RowsPerGroup; r++)
        {
            for (int i = 0; i < r % 3; i++, items++)
            {
                a.Append(r + i);
                b.Append("b" + (r + i).ToString(CultureInfo.InvariantCulture));
            }

            offsets.Add(items);
        }

        var values = new StructArray(elementType, items, [a.Build(), b.Build()], ArrowBuffer.Empty, 0);
        return new ListArray(listType, RowsPerGroup, Offsets(offsets), values, ArrowBuffer.Empty, 0);
    }

    private static MapArray Map(int first, MapType mapType)
    {
        var keys = new StringArray.Builder();
        var values = new Int32Array.Builder();
        var offsets = new List<int> { 0 };
        int entries = 0;
        for (int r = first; r < first + RowsPerGroup; r++)
        {
            for (int i = 0; i < r % 2 + 1; i++, entries++)
            {
                keys.Append("k" + i.ToString(CultureInfo.InvariantCulture));
                values.Append(r * 2 + i);
            }

            offsets.Add(entries);
        }

        var entryStruct = new StructArray(new StructType([mapType.KeyField, mapType.ValueField]), entries,
            [keys.Build(), values.Build()], ArrowBuffer.Empty, 0);
        return new MapArray(mapType, RowsPerGroup, Offsets(offsets), entryStruct, ArrowBuffer.Empty, 0);
    }

    private static ArrowBuffer Offsets(List<int> offsets)
    {
        var b = new ArrowBuffer.Builder<int>();
        foreach (int o in offsets)
            b.Append(o);
        return b.Build();
    }

    private static ArrowBuffer Validity(List<bool> valid)
    {
        var bytes = new byte[(valid.Count + 7) / 8];
        for (int i = 0; i < valid.Count; i++)
        {
            if (valid[i])
                bytes[i >> 3] |= (byte)(1 << (i & 7));
        }

        return new ArrowBuffer(bytes);
    }
}
