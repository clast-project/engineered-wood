// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Data;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro;

/// <summary>
/// Asynchronously reads Avro Object Container Files into Arrow RecordBatches.
/// Block I/O is async; decoding (block data to RecordBatch) is synchronous.
/// </summary>
public sealed class AvroAsyncReader : IAsyncEnumerable<RecordBatch>, IAsyncDisposable
{
    private readonly OcfReaderAsync _ocf;
    private readonly RecordBatchAssembler _assembler;
    private readonly int _batchSize;

    private ReadOnlyMemory<byte> _pendingBlock;
    private int _pendingOffset;
    private int _pendingRemaining;

    /// <summary>The Arrow schema for all batches produced by this reader.</summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>The Avro writer schema from the OCF header.</summary>
    public AvroSchema WriterSchema { get; }

    /// <summary>The compression codec declared in the OCF header.</summary>
    public AvroCodec Codec => _ocf.Codec;

    /// <summary>Arbitrary metadata from the OCF header.</summary>
    public IReadOnlyDictionary<string, byte[]> Metadata => _ocf.Metadata;

    internal AvroAsyncReader(OcfReaderAsync ocf, int batchSize, AvroSchema? readerSchema = null,
        ExtensionTypeRegistry? extensionRegistry = null)
    {
        _ocf = ocf;
        _batchSize = batchSize;

        var writerRecord = ocf.WriterSchema;
        WriterSchema = new AvroSchema(AvroSchemaWriter.ToJson(writerRecord));

        if (readerSchema != null)
        {
            if (readerSchema.Parsed is not AvroRecordSchema readerRecord)
                throw new InvalidOperationException("Reader schema must be a record type.");

            var resolution = SchemaResolver.Resolve(writerRecord, readerRecord);
            Schema = resolution.ArrowSchema;
            _assembler = new RecordBatchAssembler(writerRecord, resolution);
        }
        else
        {
            Schema = ArrowSchemaConverter.ToArrow(writerRecord, extensionRegistry);
            _assembler = new RecordBatchAssembler(writerRecord, Schema);
        }
    }

    /// <summary>Read the next batch asynchronously, or null on EOF.</summary>
    public async ValueTask<RecordBatch?> ReadNextBatchAsync(CancellationToken ct = default)
    {
        if (_pendingRemaining <= 0)
        {
            var block = await _ocf.ReadBlockAsync(ct).ConfigureAwait(false);
            if (block == null) return null;

            var (data, objectCount) = block.Value;
            _pendingBlock = data;
            _pendingOffset = 0;
            _pendingRemaining = checked((int)objectCount);
        }

        int rowsToRead = Math.Min(_batchSize, _pendingRemaining);
        var (batch, bytesConsumed) = _assembler.Decode(
            _pendingBlock.Span.Slice(_pendingOffset), rowsToRead);

        _pendingOffset += bytesConsumed;
        _pendingRemaining -= rowsToRead;

        if (_pendingRemaining <= 0)
            _pendingBlock = default;

        return batch;
    }

    /// <summary>Enumerates all batches in the OCF stream.</summary>
    public async IAsyncEnumerator<RecordBatch> GetAsyncEnumerator(
        CancellationToken ct = default)
    {
        RecordBatch? batch;
        while ((batch = await ReadNextBatchAsync(ct).ConfigureAwait(false)) != null)
            yield return batch;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync() => _ocf.DisposeAsync();
}
