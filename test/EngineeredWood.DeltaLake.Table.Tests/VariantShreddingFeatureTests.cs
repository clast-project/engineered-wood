// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Operations.Shredding;
using Apache.Arrow.Operations.VariantJson;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.IO.Local;
using EngineeredWood.Parquet;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The <c>variantShredding</c> / <c>variantShredding-preview</c> table features (#671): a table declaring either
/// one opens and reads, <c>delta.enableVariantShredding</c> declares the GA feature, and a shredded write is
/// refused unless the table enables it.
/// </summary>
public class VariantShreddingFeatureTests : IDisposable
{
    private const string EnableKey = "delta.enableVariantShredding";

    private readonly string _tempDir;

    public VariantShreddingFeatureTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_variant_shred_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private static Apache.Arrow.Schema VariantSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("v", VariantType.Default, true))
            .Build();

    private static Apache.Arrow.Schema PlainSchema() =>
        new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();

    private static VariantValue Obj(int a, string b) => VariantValue.FromObject(
        new Dictionary<string, VariantValue>
        {
            ["a"] = VariantValue.FromInt32(a),
            ["b"] = VariantValue.FromString(b),
        });

    /// <summary>Uniform objects, so inference hoists both fields; row 3 is SQL NULL.</summary>
    private static RecordBatch ObjectBatch()
    {
        var ids = new Int64Array.Builder().Append(1).Append(2).Append(3).Build();
        var vb = new VariantArray.Builder();
        vb.Append(Obj(1, "x"));
        vb.Append(Obj(2, "y"));
        vb.AppendNull();
        return new RecordBatch(VariantSchema(), [ids, vb.Build(allocator: null)], 3);
    }

    private static readonly DeltaTableOptions Shredding =
        DeltaTableOptions.Default with
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with { ShredVariants = ShredOptions.Default },
        };

    private static Dictionary<string, string> Enabled() => new() { [EnableKey] = "true" };

    private async Task<ProtocolAction> LatestProtocolAsync(LocalTableFileSystem fs)
    {
        await using var table = await DeltaTable.OpenAsync(fs);
        return table.CurrentSnapshot.Protocol;
    }

    private string[] DataFiles() =>
        Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories)
            .Where(f => !f.Contains("_delta_log", StringComparison.Ordinal))
            .ToArray();

    private static async Task<bool> IsShreddedAsync(string path)
    {
        await using var raf = new LocalRandomAccessFile(path);
        await using var reader = new ParquetFileReader(raf, ownsFile: false);
        var meta = await reader.ReadMetadataAsync();
        return meta.Schema.Any(s => s.Name == "typed_value");
    }

    private static async Task<List<string?>> ReadJsonAsync(DeltaTable table)
    {
        var values = new List<string?>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var v = Assert.IsType<VariantArray>(batch.Column("v"));
            for (int i = 0; i < v.Length; i++)
                values.Add(v.IsNull(i) ? null : VariantJsonWriter.ToJson(v.GetLogicalVariantValue(i), indented: false));
        }
        return values;
    }

    // ── Reading a table that declares the feature ──

    [Theory]
    [InlineData("variantShredding")]
    [InlineData("variantShredding-preview")]
    public async Task TableDeclaringTheFeature_OpensAndReadsShreddedFiles(string feature)
    {
        // The issue's reproduction, with the feature arriving in a later protocol action: a table written with a
        // variant column whose protocol then declares shredding. Before #671 the open refused it as an unsupported
        // reader feature.
        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(fs, VariantSchema()))
            await table.WriteAsync([ObjectBatch()]);

        var log = new TransactionLog(fs);
        await log.WriteCommitAsync(2,
        [
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["variantType", feature],
                WriterFeatures = ["variantType", feature],
            },
        ]);

        await using var reopened = await DeltaTable.OpenAsync(fs);
        Assert.Contains(feature, reopened.CurrentSnapshot.Protocol.ReaderFeatures!);
        Assert.Equal(["{\"a\":1,\"b\":\"x\"}", "{\"a\":2,\"b\":\"y\"}", null], await ReadJsonAsync(reopened));

        // ...and it is writable: the feature is a writer feature this library honours by writing unshredded.
        await reopened.WriteAsync([ObjectBatch()]);
        Assert.Equal(6, (await ReadJsonAsync(reopened)).Count);
    }

    // ── Enabling it ──

    [Fact]
    public async Task EnableProperty_DeclaresTheGaFeatureAndVariantType()
    {
        // As delta-spark 4.1+ does: the property enables the GA feature from metadata, and the GA feature requires
        // variantType -- declared here even without a variant column, as Spark's requiredFeatures would.
        var fs = new LocalTableFileSystem(_tempDir);
        await using (await DeltaTable.CreateAsync(fs, PlainSchema(), configuration: Enabled())) { }

        var protocol = await LatestProtocolAsync(fs);
        Assert.Equal(3, protocol.MinReaderVersion);
        Assert.Equal(7, protocol.MinWriterVersion);
        foreach (var list in new[] { protocol.ReaderFeatures!, protocol.WriterFeatures! })
        {
            Assert.Contains("variantShredding", list);
            Assert.Contains("variantType", list);
            Assert.DoesNotContain("variantShredding-preview", list);
        }
    }

    [Fact]
    public async Task EnableProperty_WithAVariantColumn_ListsEachFeatureOnce()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using (await DeltaTable.CreateAsync(fs, VariantSchema(), configuration: Enabled())) { }

        var protocol = await LatestProtocolAsync(fs);
        Assert.Single(protocol.ReaderFeatures!, f => f == "variantType");
        Assert.Single(protocol.WriterFeatures!, f => f == "variantType");
        Assert.Single(protocol.ReaderFeatures!, f => f == "variantShredding");
        Assert.Single(protocol.WriterFeatures!, f => f == "variantShredding");
    }

    [Fact]
    public async Task EnablePropertyFalse_DeclaresNothing()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using (await DeltaTable.CreateAsync(
            fs, VariantSchema(), configuration: new Dictionary<string, string> { [EnableKey] = "false" })) { }

        var protocol = await LatestProtocolAsync(fs);
        Assert.DoesNotContain("variantShredding", protocol.WriterFeatures!);
    }

    [Fact]
    public async Task FeatureSupportedProperty_GaName_PullsInVariantType()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using (await DeltaTable.CreateAsync(
            fs, PlainSchema(),
            configuration: new Dictionary<string, string> { ["delta.feature.variantShredding"] = "supported" })) { }

        var protocol = await LatestProtocolAsync(fs);
        Assert.Contains("variantShredding", protocol.ReaderFeatures!);
        Assert.Contains("variantType", protocol.ReaderFeatures!);
        Assert.Contains("variantType", protocol.WriterFeatures!);
    }

    [Fact]
    public async Task FeatureSupportedProperty_PreviewName_RequiresNothing()
    {
        // The preview feature declares no requiredFeatures in delta-spark 4.3.1.
        var fs = new LocalTableFileSystem(_tempDir);
        await using (await DeltaTable.CreateAsync(
            fs, PlainSchema(),
            configuration: new Dictionary<string, string> { ["delta.feature.variantShredding-preview"] = "supported" })) { }

        var protocol = await LatestProtocolAsync(fs);
        Assert.Contains("variantShredding-preview", protocol.ReaderFeatures!);
        Assert.Contains("variantShredding-preview", protocol.WriterFeatures!);
        Assert.DoesNotContain("variantType", protocol.WriterFeatures!);
    }

    [Fact]
    public async Task Replace_OfAPreviewTable_KeepsThePreviewNameAlone()
    {
        // Spark skips the GA feature when the table already supports the preview one; the replaced protocol's
        // features carry over, so the preview name is what stands for shredding afterwards.
        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(fs, VariantSchema()))
            await table.WriteAsync([ObjectBatch()]);
        await new TransactionLog(fs).WriteCommitAsync(2,
        [
            new ProtocolAction
            {
                MinReaderVersion = 3,
                MinWriterVersion = 7,
                ReaderFeatures = ["variantType", "variantShredding-preview"],
                WriterFeatures = ["variantType", "variantShredding-preview"],
            },
        ]);

        await using (await DeltaTable.CreateOrReplaceAsync(fs, VariantSchema(), [ObjectBatch()], configuration: Enabled())) { }

        var protocol = await LatestProtocolAsync(fs);
        Assert.Contains("variantShredding-preview", protocol.WriterFeatures!);
        Assert.DoesNotContain("variantShredding", protocol.WriterFeatures!);
    }

    // ── Writing ──

    [Fact]
    public async Task ShreddedWrite_OnATableWithoutTheProperty_IsRefusedAndWritesNothing()
    {
        // PROTOCOL.md: without the property, a variant column "must not be written as a shredded Variant".
        // delta-spark refuses it with the same code.
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, VariantSchema(), options: Shredding);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.WriteAsync([ObjectBatch()]));
        Assert.Equal(DeltaTableErrorCodes.ShreddingTablePropertyDisabled, ex.ErrorCode);
        Assert.Empty(DataFiles());
        Assert.Equal(0, table.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task ShreddedWrite_AsTheCreationData_IsRefused()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await DeltaTable.CreateOrReplaceAsync(fs, VariantSchema(), [ObjectBatch()], options: Shredding));
        Assert.Equal(DeltaTableErrorCodes.ShreddingTablePropertyDisabled, ex.ErrorCode);
        Assert.Empty(DataFiles());
    }

    [Fact]
    public async Task VariantShredSchemas_OnATableWithoutTheProperty_IsRefused()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        var options = DeltaTableOptions.Default with
        {
            ParquetWriteOptions = ParquetWriteOptions.Default with
            {
                VariantShredSchemas = new Dictionary<string, ShredSchema>
                {
                    ["v"] = ShredSchema.ForObject(new Dictionary<string, ShredSchema>
                    {
                        ["a"] = ShredSchema.Primitive(ShredType.Int32),
                    }),
                },
            },
        };
        await using var table = await DeltaTable.CreateAsync(fs, VariantSchema(), options: options);

        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.WriteAsync([ObjectBatch()]));
        Assert.Equal(DeltaTableErrorCodes.ShreddingTablePropertyDisabled, ex.ErrorCode);
    }

    [Fact]
    public async Task ShreddedWrite_OnATableWithTheProperty_ShredsAndReadsBack()
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(
            fs, VariantSchema(), options: Shredding, configuration: Enabled());

        await table.WriteAsync([ObjectBatch()]);

        Assert.True(await IsShreddedAsync(DataFiles().Single()));
        Assert.Equal(["{\"a\":1,\"b\":\"x\"}", "{\"a\":2,\"b\":\"y\"}", null], await ReadJsonAsync(table));
    }

    [Fact]
    public async Task ShreddingOptions_OnATableWithNoVariantColumn_WriteNormally()
    {
        // The writer shreds only top-level variant columns, so on this table the options shred nothing and
        // there is nothing to refuse.
        var fs = new LocalTableFileSystem(_tempDir);
        await using var table = await DeltaTable.CreateAsync(fs, PlainSchema(), options: Shredding);

        var ids = new Int64Array.Builder().Append(1).Append(2).Build();
        await table.WriteAsync([new RecordBatch(PlainSchema(), [ids], 2)]);

        Assert.Equal(1, table.CurrentSnapshot.Version);
    }

    [Fact]
    public async Task ShreddedWrite_PropertyWithoutTheFeature_IsRefused()
    {
        // A table another engine wrote with the property but no feature: a reader honouring only the protocol
        // would not expect a shredded file, so the property alone does not permit one.
        var fs = new LocalTableFileSystem(_tempDir);
        await using (await DeltaTable.CreateAsync(fs, VariantSchema())) { }
        var log = new TransactionLog(fs);
        var metadata = (await log.ReadCommitAsync(0)).OfType<MetadataAction>().Single();
        await log.WriteCommitAsync(1,
        [
            metadata with { Configuration = new Dictionary<string, string> { [EnableKey] = "true" } },
        ]);

        await using var table = await DeltaTable.OpenAsync(fs, Shredding);
        var ex = await Assert.ThrowsAsync<DeltaFormatException>(
            async () => await table.WriteAsync([ObjectBatch()]));
        Assert.Equal(DeltaTableErrorCodes.ShreddingTablePropertyDisabled, ex.ErrorCode);
    }

    [Fact]
    public async Task Checkpoint_OfAShreddingTable_IsWrittenAndReadable()
    {
        // Checkpoints never shred (CheckpointParquetOptions clears the options); a shredding table's checkpoint
        // must still write and serve the snapshot.
        var fs = new LocalTableFileSystem(_tempDir);
        await using (var table = await DeltaTable.CreateAsync(
            fs, VariantSchema(), options: Shredding, configuration: Enabled()))
        {
            await table.WriteAsync([ObjectBatch()]);
            await table.CheckpointAsync();
        }

        await using var reopened = await DeltaTable.OpenAsync(fs, Shredding);
        Assert.Equal(3, (await ReadJsonAsync(reopened)).Count);
        Assert.Contains("variantShredding", reopened.CurrentSnapshot.Protocol.WriterFeatures!);
    }
}
