// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.Avro.Data;
using EngineeredWood.Avro.Encoding;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Buffers;

namespace EngineeredWood.Avro;

/// <summary>
/// Row-level Avro encoder for message queue integration (Kafka, Pulsar, etc.).
/// Encodes RecordBatch rows individually with SOE, Confluent, or Apicurio wire format framing.
/// </summary>
public sealed class AvroEncoder : IDisposable
{
    private readonly AvroRecordSchema _avroRecord;
    private readonly GrowableBuffer _outputBuffer;
    private readonly List<int> _rowOffsets;
    private readonly byte[] _headerPrefix;
    private bool _disposed;

    /// <summary>The Arrow schema that this encoder expects.</summary>
    public Apache.Arrow.Schema Schema { get; }

    /// <summary>The Avro schema used for encoding.</summary>
    public AvroSchema AvroSchema { get; }

    internal AvroEncoder(Apache.Arrow.Schema arrowSchema, AvroSchema avroSchema, AvroRecordSchema avroRecord, FingerprintStrategy strategy)
    {
        Schema = arrowSchema;
        AvroSchema = avroSchema;
        _avroRecord = avroRecord;
        _outputBuffer = new GrowableBuffer(4096);
        _rowOffsets = new List<int>();
        _headerPrefix = BuildHeaderPrefix(avroSchema, strategy);
    }

    /// <summary>
    /// Encode all rows from a RecordBatch. Each row gets its own fingerprint prefix.
    /// </summary>
    public void Encode(RecordBatch batch)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentNullException.ThrowIfNull(batch);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
        if (batch is null) throw new ArgumentNullException(nameof(batch));
#endif

        var writer = new AvroBinaryWriter(_outputBuffer);

        for (int row = 0; row < batch.Length; row++)
        {
            _rowOffsets.Add(_outputBuffer.Length);
            _outputBuffer.Write(_headerPrefix);
            EncodeRecord(writer, batch, row);
        }
    }

    /// <summary>
    /// Drain all buffered encoded rows. Returns an <see cref="EncodedRows"/> providing
    /// zero-copy per-row access. Resets the encoder for the next batch.
    /// </summary>
    public EncodedRows Flush()
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif

        if (_rowOffsets.Count == 0)
            return new EncodedRows([], [0]);

        // Build the offsets array: start of each row + end sentinel
        var offsets = new int[_rowOffsets.Count + 1];
        _rowOffsets.CopyTo(offsets, 0);
        offsets[^1] = _outputBuffer.Length;

        // Copy the buffer data
        var buffer = _outputBuffer.WrittenSpan.ToArray();

        // Reset internal state
        _outputBuffer.Reset();
        _rowOffsets.Clear();

        return new EncodedRows(buffer, offsets);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _disposed = true;
    }

    private void EncodeRecord(AvroBinaryWriter writer, RecordBatch batch, int row)
    {
        for (int col = 0; col < _avroRecord.Fields.Count; col++)
        {
            var fieldSchema = _avroRecord.Fields[col].Schema;
            var array = batch.Column(col);
            RecordBatchEncoder.EncodeValue(writer, array, row, fieldSchema);
        }
    }

    private static byte[] BuildHeaderPrefix(AvroSchema avroSchema, FingerprintStrategy strategy)
    {
        switch (strategy)
        {
            case FingerprintStrategy.Soe:
            {
                // Compute Rabin fingerprint of the Parsing Canonical Form
                var pcf = ParsingCanonicalForm.ToCanonicalJson(avroSchema.Parsed);
                var pcfBytes = System.Text.Encoding.UTF8.GetBytes(pcf);
                ulong rabin = RabinFingerprint.Compute(pcfBytes);

                // SOE header: [0xC3, 0x01] + 8-byte LE fingerprint = 10 bytes
                var header = new byte[10];
                header[0] = 0xC3;
                header[1] = 0x01;
                BinaryPrimitives.WriteUInt64LittleEndian(header.AsSpan(2), rabin);
                return header;
            }

            case FingerprintStrategy.Confluent confluent:
            {
                // Confluent header: [0x00] + 4-byte BE schema ID = 5 bytes
                var header = new byte[5];
                header[0] = 0x00;
                BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(1), confluent.SchemaId);
                return header;
            }

            case FingerprintStrategy.Apicurio apicurio:
            {
                // Apicurio header: [0x00] + 8-byte BE global ID = 9 bytes
                var header = new byte[9];
                header[0] = 0x00;
                BinaryPrimitives.WriteUInt64BigEndian(header.AsSpan(1), apicurio.GlobalId);
                return header;
            }

            default:
                throw new ArgumentOutOfRangeException(nameof(strategy), $"Unknown fingerprint strategy: {strategy}");
        }
    }
}
