// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Vortex.Layouts;
using EngineeredWood.Vortex.Tests.TestHelpers;
using EngineeredWood.Vortex.Writer;

namespace EngineeredWood.Vortex.Tests;

/// <summary>
/// Upstream vortex only promises that later readers accept a file whose encodings all belong to
/// a frozen edition (<c>docs/specs/editions.md</c>). Unless a caller opts into
/// <c>preferDelta</c>, everything this writer emits must be in a frozen <c>core</c> edition, and
/// only a map column reaches past the 2025 editions.
/// </summary>
public class VortexWriterEditionTests
{
    /// <summary>
    /// Array encodings of the frozen core editions this writer draws on: <c>core2025.05.0</c>
    /// (vortex 0.36), <c>core2025.06.0</c> (0.40) and <c>core2025.10.0</c> (0.54).
    /// </summary>
    private static readonly HashSet<string> CoreArrays = new(StringComparer.Ordinal)
    {
        "fastlanes.bitpacked", "fastlanes.for", "vortex.alp", "vortex.alprd", "vortex.bool",
        "vortex.bytebool", "vortex.chunked", "vortex.constant", "vortex.datetimeparts",
        "vortex.decimal", "vortex.decimal_byte_parts", "vortex.dict", "vortex.ext", "vortex.fsst",
        "vortex.list", "vortex.null", "vortex.primitive", "vortex.runend", "vortex.sparse",
        "vortex.struct", "vortex.varbin", "vortex.varbinview", "vortex.zigzag",
        "vortex.pco", "vortex.sequence", "vortex.zstd",
        "fastlanes.rle", "vortex.fixed_size_list", "vortex.listview", "vortex.masked",
    };

    /// <summary>Layouts of <c>core2025.05.0</c>, which every reader since vortex 0.36 knows.</summary>
    private static readonly HashSet<string> CoreLayouts = new(StringComparer.Ordinal)
    {
        "vortex.chunked", "vortex.dict", "vortex.flat", "vortex.stats", "vortex.struct",
    };

    public static IEnumerable<object[]> Options() => new[]
    {
        new object[] { "compress", new WriterOptions(Compress: true) },
        new object[] { "compress+pco", new WriterOptions(Compress: true, PreferPco: true) },
        new object[] { "compress+datetimeparts", new WriterOptions(Compress: true, PreferDateTimeParts: true) },
        new object[] { "compress+varbinview", new WriterOptions(Compress: true, PreferVarBinView: true) },
        new object[] { "compress+stats+dictlayout", new WriterOptions(Compress: true, PreserveStats: true, PreferDictLayout: true) },
        new object[] { "plain", new WriterOptions() },
    };

    [Theory]
    [MemberData(nameof(Options))]
    public async Task WritesOnlyCoreEditionComponents(string name, WriterOptions options)
    {
        var path = Path.GetTempFileName();
        try
        {
            using (var fs = File.Create(path))
            using (var writer = new VortexFileWriter(
                fs, Schema, options.Compress, options.PreferVarBinView, options.PreserveStats,
                options.PreferPco, options.PreferDateTimeParts, options.PreferDictLayout))
            {
                // Two equal batches, so zoned and dict layouts apply.
                writer.WriteBatch(Batch(0));
                writer.WriteBatch(Batch(Rows));
                writer.Close();
            }

            var nodes = await FixtureArrayNodes.ReadAsync(path);
            var arrays = nodes.Select(n => n.Encoding).Distinct().ToList();
            Assert.All(arrays, e => Assert.True(CoreArrays.Contains(e), $"{name}: {e} is in no frozen core edition"));
            // The data is meant to reach the compressing encoders, not just plain ones.
            if (options.Compress)
                Assert.True(arrays.Count > 6, $"{name}: only {string.Join(", ", arrays)}");

            await using var reader = await VortexFileReader.OpenAsync(path);
            foreach (var layout in Layouts(reader.RootLayout))
                Assert.True(CoreLayouts.Contains(layout), $"{name}: layout {layout} is not in core2025.05.0");
            // Registered only when a map needs them, so a file without one lists what it always has.
            Assert.DoesNotContain("vortex.map", reader.ArraySpecs);
            Assert.DoesNotContain("vortex.listview", reader.ArraySpecs);
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    /// <summary>
    /// A map is the one thing this writer emits from a 2026 edition: the <c>Map</c> dtype and
    /// <c>vortex.map</c> joined <c>core2026.08.2</c> (vortex 0.85), and no earlier encoding can
    /// carry that dtype. Everything under the map still comes from the 2025 editions.
    /// </summary>
    [Fact]
    public async Task MapColumn_AddsOnlyVortexMap()
    {
        var path = VortexMapWriterTests.Write(
            new[] { new RecordBatch(MapSchema, new IArrowArray[] { VortexMapWriterTests.BuildAttrs(0, Rows) }, Rows) },
            compress: true);
        try
        {
            var arrays = (await FixtureArrayNodes.ReadAsync(path)).Select(n => n.Encoding).Distinct().ToList();
            Assert.Contains("vortex.map", arrays);
            Assert.All(arrays.Where(e => e != "vortex.map"),
                e => Assert.True(CoreArrays.Contains(e), $"{e} is in no frozen core edition"));
        }
        finally
        {
            try { File.Delete(path); } catch { }
        }
    }

    private static readonly Apache.Arrow.Schema MapSchema = new(new[]
    {
        new Field("attrs", new MapType(
            new Field("key", StringType.Default, nullable: false),
            new Field("value", Int64Type.Default, nullable: true)), nullable: true),
    }, metadata: null);

    public sealed record WriterOptions(
        bool Compress = false, bool PreferVarBinView = false, bool PreserveStats = false,
        bool PreferPco = false, bool PreferDateTimeParts = false, bool PreferDictLayout = false);

    private const int Rows = 4096;

    private static readonly Apache.Arrow.Schema Schema = new(new[]
    {
        new Field("constant", Int32Type.Default, nullable: false),
        new Field("small", Int64Type.Default, nullable: false),
        new Field("offset", Int32Type.Default, nullable: true),
        new Field("runs", UInt16Type.Default, nullable: false),
        new Field("sparse", Int32Type.Default, nullable: true),
        new Field("delta_friendly", UInt32Type.Default, nullable: false),
        new Field("decimals", DoubleType.Default, nullable: false),
        new Field("noise", FloatType.Default, nullable: false),
        new Field("low_card", StringType.Default, nullable: false),
        new Field("text", StringType.Default, nullable: true),
        new Field("flag", BooleanType.Default, nullable: true),
        new Field("ts", new TimestampType(TimeUnit.Microsecond, "UTC"), nullable: false),
        new Field("day", Date32Type.Default, nullable: false),
        new Field("list", new ListType(Int32Type.Default), nullable: false),
    }, metadata: null);

    private static RecordBatch Batch(int start)
    {
        var constant = new Int32Array.Builder();
        var small = new Int64Array.Builder();
        var offset = new Int32Array.Builder();
        var runs = new UInt16Array.Builder();
        var sparse = new Int32Array.Builder();
        var delta = new UInt32Array.Builder();
        var decimals = new DoubleArray.Builder();
        var noise = new FloatArray.Builder();
        var lowCard = new StringArray.Builder();
        var text = new StringArray.Builder();
        var flag = new BooleanArray.Builder();
        var ts = new TimestampArray.Builder(new TimestampType(TimeUnit.Microsecond, "UTC"));
        var day = new Date32Array.Builder();
        var list = new ListArray.Builder(Int32Type.Default);
        var listValues = (Int32Array.Builder)list.ValueBuilder;

        for (int r = 0; r < Rows; r++)
        {
            int i = start + r;
            constant.Append(7);
            small.Append(i % 100);
            if (i % 9 == 0) offset.AppendNull(); else offset.Append(1_000_000 + i % 500);
            runs.Append((ushort)(i / 200));
            if (i % 50 == 0) sparse.Append(i); else sparse.AppendNull();
            delta.Append((uint)(i / 64) * 100_000u + (uint)(i % 8));
            decimals.Append((i % 1000) / 100.0);
            noise.Append((float)Math.Sin(i * 0.37) * 1e6f);
            lowCard.Append(new[] { "red", "green", "blue" }[i % 3]);
            if (i % 11 == 0) text.AppendNull(); else text.Append($"https://example.com/items/{i % 700}/detail");
            if (i % 13 == 0) flag.AppendNull(); else flag.Append(i % 3 == 0);
            ts.Append(DateTimeOffset.FromUnixTimeMilliseconds(1_700_000_000_000L + i * 1000L));
            day.Append(new DateTime(2024, 1, 1).AddDays(i % 400));
            list.Append();
            for (int k = 0; k < i % 4; k++) listValues.Append(i + k);
        }

        return new RecordBatch(Schema, new IArrowArray[]
        {
            constant.Build(), small.Build(), offset.Build(), runs.Build(), sparse.Build(),
            delta.Build(), decimals.Build(), noise.Build(), lowCard.Build(), text.Build(),
            flag.Build(), ts.Build(), day.Build(), list.Build(),
        }, Rows);
    }

    private static IEnumerable<string> Layouts(VortexLayout layout) =>
        new[] { layout.EncodingId }.Concat(layout.Children.SelectMany(Layouts));
}
