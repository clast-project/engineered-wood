// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Avro;

namespace EngineeredWood.Tests.Avro;

/// <summary>
/// Round-trip tests for <see cref="GuidArray"/> through Avro Object Container Files.
/// Verifies that a <c>GuidType</c> field emits an Avro <c>string</c> with the
/// <c>uuid</c> logical type, and that the reader produces <see cref="GuidArray"/>
/// when the caller registers the extension via
/// <see cref="AvroReaderBuilder.WithExtensionRegistry"/>.
/// </summary>
public class GuidArrayRoundTripTests
{
    private static ExtensionTypeRegistry GuidRegistry()
    {
        var registry = new ExtensionTypeRegistry();
        registry.Register(GuidExtensionDefinition.Instance);
        return registry;
    }

    private static (RecordBatch batch, Guid[] values) MakeGuidBatch(int count = 4)
    {
        var values = new Guid[count];
        var builder = new GuidArray.Builder();
        for (int i = 0; i < count; i++)
        {
            values[i] = Guid.NewGuid();
            builder.Append(values[i]);
        }
        var arr = builder.Build(allocator: null);

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("g", arr.Data.DataType, nullable: false))
            .Build();
        return (new RecordBatch(schema, new IArrowArray[] { arr }, count), values);
    }

    [Fact]
    public void GuidColumn_EmitsAvroUuidLogicalType()
    {
        var (batch, _) = MakeGuidBatch();

        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(batch.Schema).Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }

        ms.Position = 0;
        using var reader = new AvroReaderBuilder().Build(ms);
        var json = reader.WriterSchema.Json;

        // Avro spec for UUID: "type": "string", "logicalType": "uuid"
        Assert.Contains("\"logicalType\":\"uuid\"", json);
        Assert.Contains("\"type\":\"string\"", json);
    }

    [Fact]
    public void ReadWithoutRegistry_ProducesStringArray()
    {
        var (batch, values) = MakeGuidBatch();

        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(batch.Schema).Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }

        ms.Position = 0;
        // No registry → reader produces StringArray with the raw 36-char UUIDs.
        using var reader = new AvroReaderBuilder().Build(ms);
        var read = reader.ReadNextBatch();
        Assert.NotNull(read);

        var col = read!.Column(0);
        var sa = Assert.IsType<StringArray>(col);
        Assert.Equal(values.Length, sa.Length);
        for (int i = 0; i < values.Length; i++)
        {
            Assert.Equal(values[i], Guid.Parse(sa.GetString(i)));
        }
    }

    [Fact]
    public void ReadWithRegistry_ProducesGuidArray()
    {
        var (batch, values) = MakeGuidBatch();

        using var ms = new MemoryStream();
        using (var writer = new AvroWriterBuilder(batch.Schema).Build(ms))
        {
            writer.Write(batch);
            writer.Finish();
        }

        ms.Position = 0;
        using var reader = new AvroReaderBuilder()
            .WithExtensionRegistry(GuidRegistry())
            .Build(ms);
        var read = reader.ReadNextBatch();
        Assert.NotNull(read);

        var col = read!.Column(0);
        var ga = Assert.IsType<GuidArray>(col);
        Assert.Equal(values.Length, ga.Length);
        for (int i = 0; i < values.Length; i++)
        {
            Assert.Equal(values[i], ga.GetGuid(i));
        }
    }

    [Fact]
    public void ToggleRegistry_SameBytes_GivesDifferentArrayTypes()
    {
        // Same OCF bytes, two readers. Only the registry option drives the
        // shape of the returned Arrow array.
        var (batch, _) = MakeGuidBatch();

        var bytes = new MemoryStream();
        using (var writer = new AvroWriterBuilder(batch.Schema).Build(bytes))
        {
            writer.Write(batch);
            writer.Finish();
        }
        byte[] buf = bytes.ToArray();

        using (var ms = new MemoryStream(buf))
        using (var reader = new AvroReaderBuilder().Build(ms))
        {
            Assert.IsType<StringArray>(reader.ReadNextBatch()!.Column(0));
        }

        using (var ms = new MemoryStream(buf))
        using (var reader = new AvroReaderBuilder()
            .WithExtensionRegistry(GuidRegistry())
            .Build(ms))
        {
            Assert.IsType<GuidArray>(reader.ReadNextBatch()!.Column(0));
        }
    }
}
