// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Data;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Buffers;

namespace EngineeredWood.Avro;

/// <summary>
/// Writes Arrow RecordBatches as an Avro Object Container File.
/// </summary>
public sealed class AvroWriter : IDisposable
{
    private readonly OcfWriter _ocf;
    private readonly RecordBatchEncoder _encoder;
    private readonly GrowableBuffer _blockBuffer = new(4096);
    private bool _finished;

    /// <summary>The Arrow schema describing the data.</summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>The Avro schema used for encoding.</summary>
    public AvroSchema AvroSchema { get; }

    internal AvroWriter(OcfWriter ocf, Apache.Arrow.Schema arrowSchema, AvroRecordSchema avroRecord)
    {
        _ocf = ocf;
        _encoder = new RecordBatchEncoder(avroRecord);
        Schema = arrowSchema;
        AvroSchema = new AvroSchema(AvroSchemaWriter.ToJson(avroRecord));

        _ocf.WriteHeader(avroRecord);
    }

    /// <summary>Write all rows from a RecordBatch as one OCF data block.</summary>
    public void Write(RecordBatch batch)
    {
        if (_finished) throw new InvalidOperationException("Writer has been finished.");

        _blockBuffer.Reset();
        int rowCount = _encoder.Encode(batch, _blockBuffer);
        _ocf.WriteBlock(_blockBuffer.WrittenSpan, rowCount);
    }

    /// <summary>
    /// Finalize the OCF stream. Must be called before disposing.
    /// </summary>
    public void Finish()
    {
        if (_finished) return;
        _finished = true;
        _ocf.Finish();
    }

    public void Dispose()
    {
        if (!_finished) Finish();
    }
}
