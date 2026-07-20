// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Compression;

namespace EngineeredWood.Avro;

/// <summary>
/// Fluent builder that configures and constructs Avro writers.
/// </summary>
public sealed class AvroWriterBuilder
{
    private readonly Apache.Arrow.Schema _arrowSchema;
    private AvroCodec _codec = AvroCodec.Null;
    private BlockCompressionLevel? _compressionLevel;
    private int? _customCompressionLevel;
    private string _recordName = "Record";
    private string? _recordNamespace;
    private AvroSchema? _explicitAvroSchema;
    private FingerprintStrategy? _fingerprintStrategy;

    public AvroWriterBuilder(Apache.Arrow.Schema arrowSchema)
    {
        _arrowSchema = arrowSchema ?? throw new ArgumentNullException(nameof(arrowSchema));
    }

    /// <summary>Use an explicit Avro schema instead of auto-converting from Arrow.</summary>
    public AvroWriterBuilder WithAvroSchema(AvroSchema schema)
    {
        _explicitAvroSchema = schema;
        return this;
    }

    /// <summary>Set the Avro record name and optional namespace.</summary>
    public AvroWriterBuilder WithRecordName(string name, string? ns = null)
    {
        _recordName = name;
        _recordNamespace = ns;
        return this;
    }

    /// <summary>OCF block compression codec (default: Null / no compression).</summary>
    public AvroWriterBuilder WithCompression(AvroCodec codec)
    {
        _codec = codec;
        return this;
    }

    /// <summary>
    /// Codec-agnostic compression level for OCF blocks. Codecs without a tunable level
    /// (Null, Snappy) ignore this setting.
    /// </summary>
    public AvroWriterBuilder WithCompressionLevel(BlockCompressionLevel level)
    {
        _compressionLevel = level;
        return this;
    }

    /// <summary>
    /// Explicit native compression level. Overrides <see cref="WithCompressionLevel"/>.
    /// Honored by Zstandard (1..22) and Lz4 (LZ4Level enum value); ignored by Deflate.
    /// </summary>
    public AvroWriterBuilder WithCustomCompressionLevel(int level)
    {
        _customCompressionLevel = level;
        return this;
    }

    /// <summary>Build a synchronous OCF writer targeting the given stream.</summary>
    public AvroWriter Build(Stream output)
    {
        var avroRecord = ResolveAvroSchema();
        var ocf = new OcfWriter(output, _codec, _compressionLevel, _customCompressionLevel);
        return new AvroWriter(ocf, _arrowSchema, avroRecord);
    }

    /// <summary>Build an asynchronous OCF writer targeting the given stream.</summary>
    public async ValueTask<AvroAsyncWriter> BuildAsync(Stream output, CancellationToken ct = default)
    {
        var avroRecord = ResolveAvroSchema();
        var ocf = new OcfWriterAsync(output, _codec, _compressionLevel, _customCompressionLevel);
        await ocf.WriteHeaderAsync(avroRecord, ct).ConfigureAwait(false);
        return new AvroAsyncWriter(ocf, _arrowSchema, avroRecord);
    }

    /// <summary>
    /// Sets a fingerprint strategy for building an <see cref="AvroEncoder"/> (SOE, Confluent, or Apicurio).
    /// </summary>
    public AvroWriterBuilder WithFingerprintStrategy(FingerprintStrategy strategy)
    {
        _fingerprintStrategy = strategy ?? throw new ArgumentNullException(nameof(strategy));
        return this;
    }

    /// <summary>
    /// Build an <see cref="AvroEncoder"/> for row-level encoding with wire format framing.
    /// Requires <see cref="WithFingerprintStrategy"/> to be called first.
    /// </summary>
    public AvroEncoder BuildEncoder()
    {
        if (_fingerprintStrategy is null)
            throw new InvalidOperationException(
                "A fingerprint strategy must be set via WithFingerprintStrategy() before calling BuildEncoder().");

        var avroRecord = ResolveAvroSchema();
        var avroSchema = ResolveAvroSchemaObject(avroRecord);

        return new AvroEncoder(_arrowSchema, avroSchema, avroRecord, _fingerprintStrategy);
    }

    private AvroRecordSchema ResolveAvroSchema()
    {
        if (_explicitAvroSchema != null)
        {
            if (_explicitAvroSchema.Parsed is not AvroRecordSchema r)
                throw new InvalidOperationException("Explicit Avro schema must be a record.");
            return r;
        }

        return ArrowSchemaConverter.FromArrow(_arrowSchema, _recordName, _recordNamespace);
    }

    private AvroSchema ResolveAvroSchemaObject(AvroRecordSchema avroRecord)
    {
        if (_explicitAvroSchema != null)
            return _explicitAvroSchema;

        var json = AvroSchemaWriter.ToJson(avroRecord);
        return new AvroSchema(json);
    }
}
