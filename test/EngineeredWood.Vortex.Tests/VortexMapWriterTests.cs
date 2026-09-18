// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Tests.TestHelpers;
using EngineeredWood.Vortex.Writer;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// The writer emits an Arrow map as the <c>Map</c> dtype over <c>vortex.map</c>, whose one child is
/// a <c>vortex.listview</c> of <c>{key, value}</c> entry structs (#383). Both the dtype and the
/// encoding need a vortex 0.85+ reader, so they appear only in a file whose schema has a map
/// (<see cref="VortexWriterEditionTests"/>); <see cref="VortexCrossValidationTests"/> checks that
/// vortex reads what is written here.
/// </summary>
public class VortexMapWriterTests
{
    private static readonly MapType Unsorted = new(
        new Field("key", StringType.Default, nullable: false),
        new Field("value", Int64Type.Default, nullable: true),
        keySorted: false);

    private static readonly MapType Sorted = new(
        new Field("key", Int32Type.Default, nullable: false),
        new Field("value", StringType.Default, nullable: false),
        keySorted: true);

    private static readonly Apache.Arrow.Schema Schema = new(new[]
    {
        new Field("attrs", Unsorted, nullable: true),
        new Field("sorted", Sorted, nullable: false),
    }, metadata: null);

    /// <summary>
    /// Row <c>i</c> of <c>attrs</c>: null every seventh row, empty every fifth, otherwise one to
    /// four entries, some with a null value and one row with a duplicate key.
    /// </summary>
    internal static MapArray BuildAttrs(int start, int rows)
    {
        var b = new MapArray.Builder(Unsorted);
        var keys = (StringArray.Builder)b.KeyBuilder;
        var values = (Int64Array.Builder)b.ValueBuilder;
        for (int r = 0; r < rows; r++)
        {
            int i = start + r;
            if (i % 7 == 3) { b.AppendNull(); continue; }
            b.Append();
            if (i % 5 == 0) continue;
            for (int j = 0; j < i % 4 + 1; j++)
            {
                keys.Append(i % 11 == 1 ? "dup" : $"k{i}.{j}");
                if ((i + j) % 6 == 0) values.AppendNull(); else values.Append(i * 10L + j);
            }
        }
        return b.Build();
    }

    private static MapArray BuildSorted(int start, int rows)
    {
        var b = new MapArray.Builder(Sorted);
        var keys = (Int32Array.Builder)b.KeyBuilder;
        var values = (StringArray.Builder)b.ValueBuilder;
        for (int r = 0; r < rows; r++)
        {
            int i = start + r;
            b.Append();
            for (int j = 0; j < i % 3; j++)
            {
                keys.Append(j * 2);
                values.Append($"v{i}");
            }
        }
        return b.Build();
    }

    private static RecordBatch Batch(int start, int rows) =>
        new(Schema, new IArrowArray[] { BuildAttrs(start, rows), BuildSorted(start, rows) }, rows);

    public static IEnumerable<object[]> Options() => new[]
    {
        new object[] { "plain", false, false, false },
        new object[] { "compress", true, false, false },
        new object[] { "compress+varbinview", true, true, false },
        new object[] { "compress+stats", true, false, true },
    };

    [Theory]
    [MemberData(nameof(Options))]
    public async Task MapColumns_RoundTrip(string name, bool compress, bool preferVarBinView, bool preserveStats)
    {
        _ = name;
        var batches = new[] { Batch(0, 300), Batch(300, 200) };
        var path = Write(batches, compress, preferVarBinView, preserveStats);
        try
        {
            await using var reader = await VortexFileReader.OpenAsync(path);
            var attrs = reader.Schema.FieldsList[0];
            var attrsType = Assert.IsType<MapType>(attrs.DataType);
            Assert.True(attrs.IsNullable);
            Assert.False(attrsType.KeySorted);
            Assert.IsType<StringType>(attrsType.KeyField.DataType);
            Assert.False(attrsType.KeyField.IsNullable);
            Assert.IsType<Int64Type>(attrsType.ValueField.DataType);
            Assert.True(attrsType.ValueField.IsNullable);

            var sorted = reader.Schema.FieldsList[1];
            var sortedType = Assert.IsType<MapType>(sorted.DataType);
            Assert.False(sorted.IsNullable);
            Assert.True(sortedType.KeySorted);
            Assert.False(sortedType.ValueField.IsNullable);

            for (int c = 0; c < 2; c++)
                Assert.Equal(Rows(batches.Select(b => b.Column(c))), Rows(new[] { await reader.ReadColumnAsync(c) }));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task SlicedMap_WritesOnlyTheVisibleRows()
    {
        var full = BuildAttrs(0, 100);
        var sliced = (MapArray)ArrowArrayFactory.Slice(full, 13, 50);
        var schema = new Apache.Arrow.Schema(new[] { new Field("attrs", Unsorted, nullable: true) }, null);
        var path = Write(new[] { new RecordBatch(schema, new IArrowArray[] { sliced }, 50) });
        try
        {
            await using var reader = await VortexFileReader.OpenAsync(path);
            Assert.Equal(Rows(new[] { sliced }), Rows(new[] { await reader.ReadColumnAsync(0) }));

            // The entries child holds the visible rows' entries only, not the whole source array.
            var listView = (await FixtureArrayNodes.ReadAsync(path)).Single(n => n.Encoding == "vortex.listview");
            int entries = Enumerable.Range(0, 50).Sum(i => sliced.GetValueLength(i));
            Assert.Equal(new byte[] { 0x08, (byte)entries, 0x10, 6, 0x18, 6 }, listView.Metadata);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task MapsNestedInStructAndList_RoundTrip()
    {
        var attrs = BuildAttrs(0, 40);
        var structType = new StructType(new[] { new Field("id", Int32Type.Default, false), new Field("attrs", Unsorted, true) });
        var ids = new Int32Array.Builder().AppendRange(Enumerable.Range(0, 40)).Build();
        var st = new StructArray(structType, 40, new IArrowArray[] { ids, attrs }, ArrowBuffer.Empty, nullCount: 0);

        // A list of maps: row i holds the maps attrs[2i] and attrs[2i + 1].
        var listType = new ListType(new Field("item", Unsorted, nullable: true));
        var listOffsets = new ArrowBuffer.Builder<int>().AppendRange(Enumerable.Range(0, 21).Select(i => i * 2)).Build();
        var list = new ListArray(listType, 20, listOffsets, attrs, ArrowBuffer.Empty, nullCount: 0);

        var schema = new Apache.Arrow.Schema(new[]
        {
            new Field("s", structType, nullable: false),
            new Field("l", listType, nullable: false),
        }, null);
        var path = Write(new[]
        {
            new RecordBatch(schema, new IArrowArray[] { ArrowArrayFactory.Slice(st, 0, 20), list }, 20),
        });
        try
        {
            await using var reader = await VortexFileReader.OpenAsync(path);
            Assert.Equal(Rows(new[] { ArrowArrayFactory.Slice(st, 0, 20) }), Rows(new[] { await reader.ReadColumnAsync(0) }));
            Assert.Equal(Rows(new IArrowArray[] { list }), Rows(new[] { await reader.ReadColumnAsync(1) }));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public async Task Map_WireShapeIsAListViewOfEntryStructs()
    {
        var path = Write(new[] { Batch(0, 64) });
        try
        {
            var nodes = await FixtureArrayNodes.ReadAsync(path);
            var maps = nodes.Where(n => n.Encoding == "vortex.map").ToList();
            Assert.Equal(2, maps.Count);
            Assert.All(maps, m =>
            {
                Assert.Equal(0, m.BufferCount);
                Assert.Empty(m.Metadata);
                Assert.Equal(new[] { "vortex.listview" }, m.Children);
            });

            // elements, offsets and sizes, plus validity for the nullable column's null maps.
            var views = nodes.Where(n => n.Encoding == "vortex.listview").ToList();
            Assert.Equal(2, views.Count);
            Assert.Contains(views, v => v.Children.SequenceEqual(
                new[] { "vortex.struct", "vortex.primitive", "vortex.primitive", "vortex.bool" }));
            Assert.Contains(views, v => v.Children.SequenceEqual(
                new[] { "vortex.struct", "vortex.primitive", "vortex.primitive" }));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    [Fact]
    public void NullableKey_IsRefused()
    {
        var type = new MapType(
            new Field("key", StringType.Default, nullable: true),
            new Field("value", Int32Type.Default, nullable: true));
        var schema = new Apache.Arrow.Schema(new[] { new Field("m", type, nullable: true) }, null);

        using var ms = new MemoryStream();
        var e = Assert.Throws<NotSupportedException>(() => new VortexFileWriter(ms, schema).Close());
        Assert.Contains("'m'", e.Message);
        Assert.Contains("nullable key", e.Message);
    }

    internal static string Write(
        IReadOnlyList<RecordBatch> batches, bool compress = false, bool preferVarBinView = false, bool preserveStats = false)
    {
        var path = Path.GetTempFileName();
        using (var fs = File.Create(path))
        using (var writer = new VortexFileWriter(
            fs, batches[0].Schema, compress, preferVarBinView, preserveStats))
        {
            foreach (var batch in batches)
                writer.WriteBatch(batch);
            writer.Close();
        }
        return path;
    }

    private static List<string> Rows(IEnumerable<IArrowArray> arrays) =>
        arrays.SelectMany(a => Enumerable.Range(0, a.Length).Select(i => ArrowValues.Render(a, i))).ToList();
}
