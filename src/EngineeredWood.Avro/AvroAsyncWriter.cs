// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Data;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Buffers;

namespace EngineeredWood.Avro;

/// <summary>
/// Asynchronously writes Arrow RecordBatches as an Avro Object Container File.
/// Encoding is synchronous (CPU-bound); only block I/O is async.
/// </summary>
public sealed class AvroAsyncWriter : IAsyncDisposable
{
    private readonly OcfWriterAsync _ocf;
    private readonly RecordBatchEncoder _encoder;
    private readonly GrowableBuffer _blockBuffer = new(4096);
    private bool _finished;

    /// <summary>The Arrow schema describing the data.</summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>The Avro schema used for encoding.</summary>
    public AvroSchema AvroSchema { get; }

    internal AvroAsyncWriter(OcfWriterAsync ocf, Apache.Arrow.Schema arrowSchema,
        AvroRecordSchema avroRecord)
    {
        _ocf = ocf;
        _encoder = new RecordBatchEncoder(avroRecord);
        Schema = arrowSchema;
        AvroSchema = new AvroSchema(AvroSchemaWriter.ToJson(avroRecord));
    }

    /// <summary>Write all rows from a RecordBatch as one OCF data block.</summary>
    public async ValueTask WriteAsync(RecordBatch batch, CancellationToken ct = default)
    {
        if (_finished) throw new InvalidOperationException("Writer has been finished.");

        _blockBuffer.Reset();
        int rowCount = _encoder.Encode(batch, _blockBuffer);
        await _ocf.WriteBlockAsync(_blockBuffer.WrittenMemory, rowCount, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Finalize the OCF stream. Must be called before disposing.
    /// </summary>
    public async ValueTask FinishAsync(CancellationToken ct = default)
    {
        if (_finished) return;
        _finished = true;
        await _ocf.FinishAsync(ct).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (!_finished) await FinishAsync().ConfigureAwait(false);
    }
}
