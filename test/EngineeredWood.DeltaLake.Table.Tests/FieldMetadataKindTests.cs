// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Log;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;
using DeltaStructType = EngineeredWood.DeltaLake.Schema.StructType;
using SchemaTypeWidening = EngineeredWood.DeltaLake.Schema.TypeWidening;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// #455: a metadata commit must keep the JSON kind of every field-metadata value. Spark reads the identity keys
/// with Metadata.getLong/getBoolean and delta.typeChanges with getMetadataArray, which all throw on a string.
/// </summary>
public class FieldMetadataKindTests : IDisposable
{
    private readonly string _tempDir;

    public FieldMetadataKindTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_fmk_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
            Directory.Delete(_tempDir, recursive: true);
    }

    private async Task<DeltaTable> CreateAsync(string schemaString, ProtocolAction protocol,
        IReadOnlyDictionary<string, string>? configuration = null)
    {
        var fs = new LocalTableFileSystem(_tempDir);
        await new TransactionLog(fs).WriteCommitAsync(0, new List<DeltaAction>
        {
            protocol,
            new MetadataAction
            {
                Id = "t", Format = Format.Parquet, SchemaString = schemaString, PartitionColumns = [],
                Configuration = configuration,
            },
        });
        return await DeltaTable.OpenAsync(fs);
    }

    private string LatestSchemaString()
    {
        var logDir = Path.Combine(_tempDir, "_delta_log");
        var last = Directory.GetFiles(logDir, "*.json").OrderBy(p => p, StringComparer.Ordinal).Last();
        foreach (var line in File.ReadAllLines(last))
        {
            using var doc = JsonDocument.Parse(line);
            if (doc.RootElement.TryGetProperty("metaData", out var md))
                return md.GetProperty("schemaString").GetString()!;
        }
        throw new InvalidOperationException("latest commit has no metaData");
    }

    private static JsonElement FieldMetadataOf(string schemaString, string field)
    {
        using var doc = JsonDocument.Parse(schemaString);
        foreach (var f in doc.RootElement.GetProperty("fields").EnumerateArray())
            if (f.GetProperty("name").GetString() == field)
                return f.GetProperty("metadata").Clone();
        throw new InvalidOperationException(field);
    }

    private static RecordBatch ValueBatch(string value)
    {
        var schema = new Apache.Arrow.Schema.Builder().Field(new Field("value", StringType.Default, true)).Build();
        return new RecordBatch(schema, [new StringArray.Builder().Append(value).Build()], 1);
    }

    private static readonly ProtocolAction IdentityProtocol =
        new() { MinReaderVersion = 1, MinWriterVersion = 7, WriterFeatures = ["identityColumns"] };

    [Fact]
    public async Task IdentityMetadata_SparkNumbers_StayNumbersAfterAppend()
    {
        string schemaString =
            """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{"delta.identity.start":1,"delta.identity.step":1,"delta.identity.allowExplicitInsert":false,"__COLLATIONS":{"x":"y"}}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""";
        await using var table = await CreateAsync(schemaString, IdentityProtocol);

        await table.WriteAsync([ValueBatch("a")]);

        var md = FieldMetadataOf(LatestSchemaString(), "id");
        Assert.Equal(JsonValueKind.Number, md.GetProperty("delta.identity.start").ValueKind);
        Assert.Equal(JsonValueKind.Number, md.GetProperty("delta.identity.step").ValueKind);
        Assert.Equal(1, md.GetProperty("delta.identity.highWaterMark").GetInt64());
        Assert.Equal(JsonValueKind.False, md.GetProperty("delta.identity.allowExplicitInsert").ValueKind);
        // An unknown non-string key rides along through the high-water-mark update.
        Assert.Equal("y", md.GetProperty("__COLLATIONS").GetProperty("x").GetString());
    }

    // Tables EW wrote before the fix carry the identity keys as strings; the next metadata commit repairs them.
    [Fact]
    public async Task IdentityMetadata_LegacyEwStrings_AreWrittenAsNumbers()
    {
        string schemaString =
            """{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{"delta.identity.start":"5","delta.identity.step":"-2","delta.identity.highWaterMark":"5","delta.identity.allowExplicitInsert":"true"}},{"name":"value","type":"string","nullable":true,"metadata":{}}]}""";
        await using var table = await CreateAsync(schemaString, IdentityProtocol);

        await table.WriteAsync([ValueBatch("a")]);

        var md = FieldMetadataOf(LatestSchemaString(), "id");
        Assert.Equal(5, md.GetProperty("delta.identity.start").GetInt64());
        Assert.Equal(-2, md.GetProperty("delta.identity.step").GetInt64());
        Assert.Equal(3, md.GetProperty("delta.identity.highWaterMark").GetInt64());
        Assert.Equal(JsonValueKind.True, md.GetProperty("delta.identity.allowExplicitInsert").ValueKind);
    }

    [Fact]
    public async Task TypeChangesArray_SurvivesAddColumnAsArray()
    {
        string schemaString =
            """{"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{"delta.typeChanges":[{"toType":"long","fromType":"integer"}]}}]}""";
        await using var table = await CreateAsync(
            schemaString,
            new ProtocolAction
            {
                MinReaderVersion = 3, MinWriterVersion = 7,
                ReaderFeatures = ["typeWidening"], WriterFeatures = ["typeWidening"],
            },
            new Dictionary<string, string> { ["delta.enableTypeWidening"] = "true" });

        await table.AddColumnAsync(new Field("extra", StringType.Default, true));

        var changes = FieldMetadataOf(LatestSchemaString(), "id").GetProperty("delta.typeChanges");
        Assert.Equal(JsonValueKind.Array, changes.ValueKind);
        Assert.Equal("integer", changes[0].GetProperty("fromType").GetString());
    }

    [Fact]
    public async Task ArbitraryFieldMetadata_JsonKindsPreservedOnAddColumn()
    {
        // "q" and "inv" are STRINGS whose text happens to be valid JSON: they must stay strings.
        string schemaString =
            """{"type":"struct","fields":[{"name":"a","type":"long","nullable":true,"metadata":{"n":42,"d":1.5E3,"b":true,"z":null,"o":{"x":1},"arr":[1,"two"],"s":"he said \"hi\"","q":"42","inv":"{\"expression\":{\"expression\":\"a > 0\"}}"}}]}""";
        await using var table = await CreateAsync(schemaString, new ProtocolAction { MinReaderVersion = 1, MinWriterVersion = 2 });

        await table.AddColumnAsync(new Field("extra", StringType.Default, true));

        var md = FieldMetadataOf(LatestSchemaString(), "a");
        Assert.Equal(42, md.GetProperty("n").GetInt64());
        Assert.Equal(1500.0, md.GetProperty("d").GetDouble());
        Assert.Equal(JsonValueKind.True, md.GetProperty("b").ValueKind);
        Assert.Equal(JsonValueKind.Null, md.GetProperty("z").ValueKind);
        Assert.Equal(1, md.GetProperty("o").GetProperty("x").GetInt32());
        Assert.Equal("two", md.GetProperty("arr")[1].GetString());
        Assert.Equal("he said \"hi\"", md.GetProperty("s").GetString());
        Assert.Equal("42", md.GetProperty("q").GetString());
        Assert.Equal(JsonValueKind.String, md.GetProperty("inv").ValueKind);
    }

    // Enabling column mapping rewrites every field's metadata; the kinds must survive it.
    [Fact]
    public void AssignColumnMapping_KeepsJsonKinds()
    {
        var schema = DeltaSchemaSerializer.Parse(
            """{"type":"struct","fields":[{"name":"a","type":"long","nullable":true,"metadata":{"n":42,"o":{"x":1},"q":"42"}}]}""");

        var (mapped, _) = ColumnMapping.AssignColumnMapping(schema, 0);

        var md = FieldMetadataOf(DeltaSchemaSerializer.Serialize(mapped), "a");
        Assert.Equal(JsonValueKind.Number, md.GetProperty("n").ValueKind);
        Assert.Equal(JsonValueKind.Object, md.GetProperty("o").ValueKind);
        Assert.Equal(JsonValueKind.String, md.GetProperty("q").ValueKind);
        Assert.Equal(JsonValueKind.Number, md.GetProperty(ColumnMapping.FieldIdKey).ValueKind);
        Assert.Equal(JsonValueKind.String, md.GetProperty(ColumnMapping.PhysicalNameKey).ValueKind);
    }

    // EW's own writers produce the spec kinds: a new identity column and a recorded type change.
    [Fact]
    public void EwWrittenIdentityAndTypeChanges_UseSpecKinds()
    {
        var field = new StructField
        {
            Name = "id", Type = new PrimitiveType { TypeName = "long" }, Nullable = false,
            Metadata = IdentityColumn.CreateMetadata(start: 10, step: 3, allowExplicitInsert: true),
        };
        field = SchemaTypeWidening.AddTypeChange(field, "integer", "long");

        var md = FieldMetadataOf(DeltaSchemaSerializer.Serialize(new DeltaStructType { Fields = [field] }), "id");
        Assert.Equal(10, md.GetProperty(IdentityColumn.StartKey).GetInt64());
        Assert.Equal(3, md.GetProperty(IdentityColumn.StepKey).GetInt64());
        Assert.Equal(JsonValueKind.True, md.GetProperty(IdentityColumn.AllowExplicitInsertKey).ValueKind);
        Assert.Equal(JsonValueKind.Array, md.GetProperty(SchemaTypeWidening.TypeChangesKey).ValueKind);
    }

    // A spec-typed key whose text is not that kind stays a string rather than being dropped or thrown on.
    [Fact]
    public void SpecKeyWithMalformedText_StaysString()
    {
        var field = new StructField
        {
            Name = "id", Type = new PrimitiveType { TypeName = "long" }, Nullable = false,
            Metadata = new Dictionary<string, string>
            {
                [IdentityColumn.StartKey] = "one",
                [IdentityColumn.AllowExplicitInsertKey] = "maybe",
                [SchemaTypeWidening.TypeChangesKey] = "{\"not\":\"an array\"}",
            },
        };

        var md = FieldMetadataOf(DeltaSchemaSerializer.Serialize(new DeltaStructType { Fields = [field] }), "id");
        Assert.Equal("one", md.GetProperty(IdentityColumn.StartKey).GetString());
        Assert.Equal("maybe", md.GetProperty(IdentityColumn.AllowExplicitInsertKey).GetString());
        Assert.Equal("{\"not\":\"an array\"}", md.GetProperty(SchemaTypeWidening.TypeChangesKey).GetString());
    }

    // Parse -> Serialize is the identity on the JSON kinds (the .crc validator compares schemas this way).
    [Fact]
    public void ParseSerialize_RoundTripsEveryKind()
    {
        string schemaString =
            """{"type":"struct","fields":[{"name":"a","type":"long","nullable":true,"metadata":{"n":-7,"b":false,"z":null,"o":{"k":[1,{"x":true}]},"s":"str","q":"true"}}]}""";

        string written = DeltaSchemaSerializer.Serialize(DeltaSchemaSerializer.Parse(schemaString));

        Assert.Equal(schemaString, written);
    }
}
