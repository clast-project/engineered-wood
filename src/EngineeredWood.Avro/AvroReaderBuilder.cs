// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro;

/// <summary>
/// Fluent builder that configures and constructs Avro readers.
/// </summary>
public sealed class AvroReaderBuilder
{
    private int _batchSize = 1024;
    private AvroSchema? _readerSchema;
    private SchemaStore? _writerSchemaStore;
    private SchemaFingerprint? _activeFingerprint;
    private AvroWireFormat _wireFormat = AvroWireFormat.SingleObject;
    private int[]? _projection;
    private string[]? _projectionNames;
    private string[]? _skipFields;
    private ExtensionTypeRegistry? _extensionRegistry;

    /// <summary>Maximum rows per RecordBatch (default: 1024).</summary>
    public AvroReaderBuilder WithBatchSize(int batchSize)
    {
        _batchSize = batchSize;
        return this;
    }

    /// <summary>
    /// Sets a reader schema for schema evolution. When provided, the reader will
    /// resolve the writer schema (from the OCF header) against this reader schema
    /// per the Avro specification: fields matched by name/alias, unmatched writer
    /// fields are skipped, unmatched reader fields use their defaults, and type
    /// promotions (e.g. int to long) are applied.
    /// </summary>
    public AvroReaderBuilder WithReaderSchema(AvroSchema readerSchema)
    {
        _readerSchema = readerSchema;
        return this;
    }

    /// <summary>
    /// Sets a <see cref="SchemaStore"/> containing writer schemas for streaming decoding.
    /// </summary>
    public AvroReaderBuilder WithWriterSchemaStore(SchemaStore store)
    {
        _writerSchemaStore = store ?? throw new ArgumentNullException(nameof(store));
        return this;
    }

    /// <summary>
    /// Sets an active fingerprint for <see cref="AvroWireFormat.RawBinary"/> decoding,
    /// identifying the initial schema to use.
    /// </summary>
    public AvroReaderBuilder WithActiveFingerprint(SchemaFingerprint fingerprint)
    {
        _activeFingerprint = fingerprint ?? throw new ArgumentNullException(nameof(fingerprint));
        return this;
    }

    /// <summary>
    /// Sets the wire format for streaming message decoding (default: <see cref="AvroWireFormat.SingleObject"/>).
    /// </summary>
    public AvroReaderBuilder WithWireFormat(AvroWireFormat wireFormat)
    {
        _wireFormat = wireFormat;
        return this;
    }

    /// <summary>
    /// Select specific top-level fields by index. Only these fields are
    /// materialized; others are skipped during decode.
    /// </summary>
    public AvroReaderBuilder WithProjection(params int[] fieldIndices)
    {
        _projection = fieldIndices;
        return this;
    }

    /// <summary>
    /// Select specific top-level fields by name. Only these fields are
    /// materialized; others are skipped during decode.
    /// </summary>
    public AvroReaderBuilder WithProjection(params string[] fieldNames)
    {
        _projectionNames = fieldNames;
        return this;
    }

    /// <summary>Skip specific fields by name during decode.</summary>
    public AvroReaderBuilder WithSkipFields(params string[] fieldNames)
    {
        _skipFields = fieldNames;
        return this;
    }

    /// <summary>
    /// Registers Arrow extension types for the reader. When supplied and the
    /// registry contains <c>arrow.uuid</c>, Avro <c>uuid</c> logical-type
    /// columns materialise as <see cref="GuidArray"/> rather than the default
    /// <see cref="StringArray"/>. The registered extension's storage type
    /// must be <c>FixedSizeBinaryType(16)</c>.
    /// </summary>
    public AvroReaderBuilder WithExtensionRegistry(ExtensionTypeRegistry registry)
    {
        _extensionRegistry = registry ?? throw new ArgumentNullException(nameof(registry));
        return this;
    }

    /// <summary>Build a synchronous OCF reader.</summary>
    public AvroReader Build(Stream input)
    {
        var ocf = OcfReader.Open(input);
        var effectiveReaderSchema = ResolveReaderSchema(ocf.WriterSchema);
        return new AvroReader(ocf, _batchSize, effectiveReaderSchema, _extensionRegistry);
    }

    /// <summary>Build an asynchronous OCF reader.</summary>
    public async ValueTask<AvroAsyncReader> BuildAsync(Stream input, CancellationToken ct = default)
    {
        var ocf = await OcfReaderAsync.OpenAsync(input, ct).ConfigureAwait(false);
        var effectiveReaderSchema = ResolveReaderSchema(ocf.WriterSchema);
        return new AvroAsyncReader(ocf, _batchSize, effectiveReaderSchema, _extensionRegistry);
    }

    /// <summary>
    /// Build a push-based streaming <see cref="AvroDecoder"/> for framed Avro messages.
    /// Requires <see cref="WithWriterSchemaStore"/> to be called first.
    /// </summary>
    public AvroDecoder BuildDecoder()
    {
        if (_writerSchemaStore is null)
            throw new InvalidOperationException(
                "A writer schema store must be set via WithWriterSchemaStore() before calling BuildDecoder().");

        return new AvroDecoder(_writerSchemaStore, _readerSchema, _batchSize, _wireFormat);
    }

    private AvroSchema? ResolveReaderSchema(AvroRecordSchema writerSchema)
    {
        if (_readerSchema != null)
            return _readerSchema;

        if (_projection != null)
        {
            var fields = new List<AvroFieldNode>();
            foreach (int idx in _projection)
            {
                if (idx < 0 || idx >= writerSchema.Fields.Count)
                    throw new ArgumentOutOfRangeException(nameof(_projection),
                        $"Field index {idx} out of range (0..{writerSchema.Fields.Count - 1}).");
                fields.Add(writerSchema.Fields[idx]);
            }
            var projected = new AvroRecordSchema(writerSchema.Name, writerSchema.Namespace, fields);
            return new AvroSchema(AvroSchemaWriter.ToJson(projected));
        }

        if (_projectionNames != null)
        {
            var lookup = new Dictionary<string, AvroFieldNode>(StringComparer.Ordinal);
            foreach (var f in writerSchema.Fields)
                lookup[f.Name] = f;

            var fields = new List<AvroFieldNode>();
            foreach (string name in _projectionNames)
            {
                if (!lookup.TryGetValue(name, out var field))
                    throw new ArgumentException(
                        $"Field '{name}' not found in writer schema. Available fields: {string.Join(", ", writerSchema.Fields.Select(f => f.Name))}.",
                        nameof(_projectionNames));
                fields.Add(field);
            }
            var projected = new AvroRecordSchema(writerSchema.Name, writerSchema.Namespace, fields);
            return new AvroSchema(AvroSchemaWriter.ToJson(projected));
        }

        if (_skipFields != null)
        {
            var skipSet = new HashSet<string>(_skipFields);
            var fields = writerSchema.Fields.Where(f => !skipSet.Contains(f.Name)).ToList();
            var projected = new AvroRecordSchema(writerSchema.Name, writerSchema.Namespace, fields);
            return new AvroSchema(AvroSchemaWriter.ToJson(projected));
        }

        return null;
    }
}
