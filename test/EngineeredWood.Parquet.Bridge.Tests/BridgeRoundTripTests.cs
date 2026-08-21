// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Compression;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.Tests.Parquet.Bridge;

/// <summary>
/// What survives a table crossing the boundary twice. The bridge is part of the system under test
/// when it is driven from Parquity, so a fidelity fault here would be reported as a Parquet defect;
/// these pin the transport itself.
/// </summary>
public class BridgeRoundTripTests : BridgeHarness
{
    private async Task<(Apache.Arrow.Schema Schema, List<RecordBatch> Batches)> RoundTripAsync(
        RecordBatch batch, string name, string? profile = null)
    {
        string source = Path_($"{name}.in.arrow");
        string parquet = Path_($"{name}.parquet");
        string target = Path_($"{name}.out.arrow");
        await WriteArrowAsync(batch, source);

        string[] write = ["write", "--arrow", source, "--parquet", parquet];
        var written = await RunAsync(profile is null ? write : [.. write, "--profile", profile]);
        Assert.Equal(0, written.ExitCode);
        Assert.True(File.Exists(parquet), "the bridge reported success without writing a file");

        var read = await RunAsync("read", "--parquet", parquet, "--arrow", target);
        Assert.Equal(0, read.ExitCode);
        return await ReadArrowAsync(target);
    }

    [Fact]
    public async Task EveryScalarTypeAndItsNullsSurviveTheRoundTrip()
    {
        var batch = MixedBatch();
        var (schema, batches) = await RoundTripAsync(batch, "mixed");

        Assert.Equal(batch.Schema.FieldsList.Count, schema.FieldsList.Count);
        var read = Assert.Single(batches);
        Assert.Equal(batch.Length, read.Length);

        Assert.Equal([1, null, 3], Values<int?>((Int32Array)read.Column("id"), (a, i) => a.GetValue(i)));
        Assert.Equal(
            ["first", null, "third"],
            Values<string?>((StringArray)read.Column("label"), (a, i) => a.GetString(i)));
        Assert.Equal(
            [true, null, false],
            Values<bool?>((BooleanArray)read.Column("flag"), (a, i) => a.GetValue(i)));
    }

    [Fact]
    public async Task ATableWithNoRowsKeepsItsColumns()
    {
        // No row group is written at all, so the schema reaches the footer only because the bridge
        // declares it. Without that the file comes back with no columns.
        var declared = new Apache.Arrow.Schema(
            [
                new Field("id", Int64Type.Default, nullable: false),
                new Field("label", StringType.Default, nullable: true),
            ],
            null);
        string source = Path_("empty.in.arrow");
        string parquet = Path_("empty.parquet");
        string target = Path_("empty.out.arrow");
        await WriteArrowSchemaAsync(declared, source);

        Assert.Equal(0, (await RunAsync("write", "--arrow", source, "--parquet", parquet)).ExitCode);
        Assert.Equal(0, (await RunAsync("read", "--parquet", parquet, "--arrow", target)).ExitCode);

        var (schema, batches) = await ReadArrowAsync(target);
        Assert.Empty(batches);
        Assert.Equal(["id", "label"], schema.FieldsList.Select(field => field.Name));
    }

    [Fact]
    public async Task NestedColumnsAndTheirNullsSurvive()
    {
        var batch = NestedBatch();
        var (_, batches) = await RoundTripAsync(batch, "nested");
        var read = Assert.Single(batches);

        var groups = (StructArray)read.Column("grp");
        Assert.Equal(3, groups.Length);
        Assert.False(groups.IsNull(0));
        Assert.True(groups.IsNull(1));
        Assert.False(groups.IsNull(2));

        var items = (ListArray)read.Column("items");
        Assert.Equal(2, items.GetValueLength(0));
        Assert.True(items.IsNull(1));
        Assert.Equal(0, items.GetValueLength(2));
    }

    [Fact]
    public async Task ADecimalComesBackAsTheWidthTheArrowEcosystemExpects()
    {
        // EngineeredWood narrows a decimal to the smallest Arrow width that fits, which no other
        // Parquet-to-Arrow implementation does. The bridge crosses into that ecosystem, so it opts
        // out; without that a decimal(6,2) returns as decimal32 and every consumer disagrees.
        var type = new Decimal128Type(6, 2);
        var builder = new Decimal128Array.Builder(type);
        builder.Append(12.30m);
        builder.Append(-0.01m);
        var schema = new Apache.Arrow.Schema([new Field("amount", type, nullable: false)], null);
        var batch = new RecordBatch(schema, [builder.Build()], 2);

        var (read, batches) = await RoundTripAsync(batch, "decimal");

        var actual = Assert.IsType<Decimal128Type>(read.FieldsList[0].DataType);
        Assert.Equal((6, 2), (actual.Precision, actual.Scale));
        var values = (Decimal128Array)Assert.Single(batches).Column(0);
        Assert.Equal(12.30m, values.GetValue(0));
        Assert.Equal(-0.01m, values.GetValue(1));
    }

    [Fact]
    public async Task ATimestampKeepsItsZoneName()
    {
        // Parquet records only isAdjustedToUTC, so the name survives solely through ARROW:schema.
        var type = new TimestampType(TimeUnit.Microsecond, "America/New_York");
        var values = new TimestampArray.Builder(type)
            .Append(DateTimeOffset.FromUnixTimeMilliseconds(1_615_705_200_000)).Build();
        var schema = new Apache.Arrow.Schema([new Field("tick", type, nullable: false)], null);

        var (read, _) = await RoundTripAsync(new RecordBatch(schema, [values], 1), "zone");

        Assert.Equal("America/New_York", ((TimestampType)read.FieldsList[0].DataType).Timezone);
    }

    [Fact]
    public async Task ASecondPrecisionColumnIsRescaledRatherThanRefused()
    {
        // Parquet has no second unit. The values are multiplied rather than relabelled, so the
        // instant is preserved even though the unit it comes back as is milliseconds.
        var type = new TimestampType(TimeUnit.Second, "UTC");
        var values = new TimestampArray.Builder(type)
            .Append(DateTimeOffset.FromUnixTimeSeconds(1_700_000_000)).Build();
        var schema = new Apache.Arrow.Schema([new Field("tick", type, nullable: false)], null);

        var (read, batches) = await RoundTripAsync(new RecordBatch(schema, [values], 1), "seconds");

        Assert.Equal(TimeUnit.Millisecond, ((TimestampType)read.FieldsList[0].DataType).Unit);
        var actual = (TimestampArray)Assert.Single(batches).Column(0);
        Assert.Equal(
            DateTimeOffset.FromUnixTimeSeconds(1_700_000_000), actual.GetTimestamp(0)!.Value);
    }

    [Fact]
    public async Task AFixedSizeListIsWrittenAsAnOrdinaryList()
    {
        // Parquet has no fixed-size list, so it writes as a LIST -- what PyArrow, Polars and DuckDB
        // all put on disk. A null slot still occupies its full width in the child array, which is
        // where the values after it are at risk.
        var values = new Int32Array.Builder()
            .Append(1).Append(2).Append(3).Append(4).Append(5).Append(6).Build();
        var validity = new ArrowBuffer.BitmapBuilder().Append(true).Append(false).Append(true).Build();
        var type = new FixedSizeListType(new Field("item", Int32Type.Default, nullable: false), 2);
        var array = new FixedSizeListArray(type, 3, values, validity, nullCount: 1);
        var schema = new Apache.Arrow.Schema([new Field("pair", type, nullable: true)], null);

        var (_, batches) = await RoundTripAsync(new RecordBatch(schema, [array], 3), "fixed");

        var read = (ListArray)Assert.Single(batches).Column(0);
        Assert.False(read.IsNull(0));
        Assert.True(read.IsNull(1));
        Assert.False(read.IsNull(2));
        var items = (Int32Array)read.Values;
        Assert.Equal(1, items.GetValue(read.ValueOffsets[0]));
        Assert.Equal(5, items.GetValue(read.ValueOffsets[2]));
    }

    [Theory]
    [InlineData("compression-gzip", CompressionCodec.Gzip)]
    [InlineData("compression-brotli", CompressionCodec.Brotli)]
    public async Task ACompressionProfileReachesEveryColumnChunk(string profile, CompressionCodec expected)
    {
        // Parquity verifies the physical artifact, so a profile that is declared but not applied is
        // a contract violation rather than a quiet no-op.
        string parquet = Path_($"{profile}.parquet");
        await WriteAndVerifyAsync(MixedBatch(), parquet, profile, async reader =>
        {
            var metadata = await reader.ReadMetadataAsync();
            foreach (var group in metadata.RowGroups)
            {
                foreach (var column in group.Columns)
                    Assert.Equal(expected, column.MetaData!.Codec);
            }
        });
    }

    [Fact]
    public async Task TheRowGroupProfileSplitsTheFileAsDeclared()
    {
        string parquet = Path_("rowgroups.parquet");
        await WriteAndVerifyAsync(MixedBatch(), parquet, "row-group-2", async reader =>
        {
            var metadata = await reader.ReadMetadataAsync();
            Assert.Equal(3, metadata.NumRows);
            Assert.Equal(2, metadata.RowGroups.Count);
            Assert.Equal([2, 1], metadata.RowGroups.Select(group => group.NumRows));
        });
    }

    private async Task WriteAndVerifyAsync(
        RecordBatch batch,
        string parquet,
        string profile,
        Func<ParquetFileReader, Task> verify)
    {
        string source = Path_(Path.GetFileNameWithoutExtension(parquet) + ".arrow");
        await WriteArrowAsync(batch, source);
        var written = await RunAsync(
            "write", "--arrow", source, "--parquet", parquet, "--profile", profile);
        Assert.Equal(0, written.ExitCode);

        await using var file = new LocalRandomAccessFile(parquet);
        await using var reader = new ParquetFileReader(file, ownsFile: false);
        await verify(reader);
    }

    private static T[] Values<T>(IArrowArray array, Func<dynamic, int, T> read) =>
        [.. Enumerable.Range(0, array.Length).Select(index => read((dynamic)array, index))];

    private static RecordBatch MixedBatch()
    {
        var ids = new Int32Array.Builder().Append(1).AppendNull().Append(3).Build();
        var labels = new StringArray.Builder().Append("first").AppendNull().Append("third").Build();
        var flags = new BooleanArray.Builder().Append(true).AppendNull().Append(false).Build();
        var ratios = new DoubleArray.Builder().Append(1.5).Append(double.NaN).Append(-0.0).Build();
        var schema = new Apache.Arrow.Schema(
            [
                new Field("id", Int32Type.Default, nullable: true),
                new Field("label", StringType.Default, nullable: true),
                new Field("flag", BooleanType.Default, nullable: true),
                new Field("ratio", DoubleType.Default, nullable: false),
            ],
            null);
        return new RecordBatch(schema, [ids, labels, flags, ratios], 3);
    }

    private static RecordBatch NestedBatch()
    {
        var inner = new Int32Array.Builder().Append(10).Append(20).Build();
        var offsets = new ArrowBuffer.Builder<int>().Append(0).Append(2).Append(2).Append(2).Build();
        var listValidity = new ArrowBuffer.BitmapBuilder()
            .Append(true).Append(false).Append(true).Build();
        var itemField = new Field("item", Int32Type.Default, nullable: true);
        var items = new ListArray(
            new ListType(itemField), 3, offsets, inner, listValidity, nullCount: 1);

        var child = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var structType = new StructType([new Field("x", Int64Type.Default, nullable: true)]);
        var structValidity = new ArrowBuffer.BitmapBuilder()
            .Append(true).Append(false).Append(true).Build();
        var groups = new StructArray(structType, 3, [child], structValidity, nullCount: 1);

        var schema = new Apache.Arrow.Schema(
            [
                new Field("grp", structType, nullable: true),
                new Field("items", items.Data.DataType, nullable: true),
            ],
            null);
        return new RecordBatch(schema, [groups, items], 3);
    }
}
