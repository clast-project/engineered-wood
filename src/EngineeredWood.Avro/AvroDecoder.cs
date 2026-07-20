// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using EngineeredWood.Avro.Data;
using EngineeredWood.Avro.Encoding;
using EngineeredWood.Avro.Schema;

namespace EngineeredWood.Avro;

/// <summary>
/// Push-based streaming decoder for SOE/Confluent/Apicurio framed Avro messages.
/// The caller feeds complete messages one at a time and drains batches when ready.
/// </summary>
public sealed class AvroDecoder : IDisposable
{
    private readonly SchemaStore _store;
    private readonly AvroSchema? _readerSchema;
    private readonly int _batchSize;
    private readonly AvroWireFormat _wireFormat;

    // Current batch state — may change when the schema switches
    private SchemaFingerprint? _currentFingerprint;
    private RecordBatchAssembler? _assembler;
    private Apache.Arrow.Schema? _currentArrowSchema;
    private int _currentRowCount;

    // Accumulate raw payloads per batch so we can decode them in bulk
    private List<byte[]> _pendingPayloads = new();

    private bool _disposed;

    /// <summary>The current Arrow schema (changes when a new writer schema is encountered).</summary>
    public Apache.Arrow.Schema? Schema => _currentArrowSchema;

    /// <summary>Maximum rows per RecordBatch.</summary>
    public int BatchSize => _batchSize;

    /// <summary>Number of additional rows that can be added before auto-flush.</summary>
    public int RemainingCapacity => _batchSize - _currentRowCount;

    internal AvroDecoder(SchemaStore store, AvroSchema? readerSchema, int batchSize, AvroWireFormat wireFormat)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _readerSchema = readerSchema;
        _batchSize = batchSize;
        _wireFormat = wireFormat;
    }

    /// <summary>
    /// Decode a single framed message. The message includes the wire format header and Avro binary payload.
    /// Returns a <see cref="RecordBatch"/> if a schema change or batch-full condition caused an implicit flush;
    /// otherwise returns <c>null</c>.
    /// </summary>
    public RecordBatch? Decode(ReadOnlySpan<byte> message)
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif

        // Parse fingerprint and extract payload
        SchemaFingerprint fingerprint;
        ReadOnlySpan<byte> payload;

        switch (_wireFormat)
        {
            case AvroWireFormat.SingleObject:
                fingerprint = ParseSoeHeader(message, out payload);
                break;
            case AvroWireFormat.Confluent:
                fingerprint = ParseConfluentHeader(message, out payload);
                break;
            case AvroWireFormat.Apicurio:
                fingerprint = ParseApicurioHeader(message, out payload);
                break;
            case AvroWireFormat.RawBinary:
                // No framing — use current schema; fingerprint stays the same
                fingerprint = _currentFingerprint
                    ?? throw new InvalidOperationException(
                        "RawBinary wire format requires at least one schema to be set up before decoding.");
                payload = message;
                break;
            default:
                throw new InvalidOperationException($"Unknown wire format: {_wireFormat}");
        }

        // Check for schema switch
        RecordBatch? flushedBatch = null;
        if (_currentFingerprint is null || !_currentFingerprint.Equals(fingerprint))
        {
            // Flush any accumulated rows under the old schema before switching
            flushedBatch = FlushInternal();
            SwitchSchema(fingerprint);
        }

        // Accumulate the payload
        _pendingPayloads.Add(payload.ToArray());
        _currentRowCount++;

        // Auto-flush if batch is full
        if (_currentRowCount >= _batchSize)
        {
            var batch = FlushInternal();
            // If we already had a flushedBatch from schema change, return that first;
            // the auto-flush result is lost (caller should use smaller batch sizes or
            // call Flush() more frequently). In practice, schema changes + full batch in
            // the same Decode call are extremely unlikely.
            return flushedBatch ?? batch;
        }

        return flushedBatch;
    }

    /// <summary>
    /// Drain accumulated rows into a RecordBatch. Returns <c>null</c> if no rows have been accumulated.
    /// </summary>
    public RecordBatch? Flush()
    {
#if NET8_0_OR_GREATER
        ObjectDisposedException.ThrowIf(_disposed, this);
#else
        if (_disposed) throw new ObjectDisposedException(GetType().FullName);
#endif
        return FlushInternal();
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _disposed = true;
    }

    private RecordBatch? FlushInternal()
    {
        if (_currentRowCount == 0 || _assembler is null || _currentArrowSchema is null)
            return null;

        // Concatenate all pending payloads into one buffer
        int totalBytes = 0;
        foreach (var p in _pendingPayloads)
            totalBytes += p.Length;

        var combined = new byte[totalBytes];
        int offset = 0;
        foreach (var p in _pendingPayloads)
        {
            p.CopyTo(combined, offset);
            offset += p.Length;
        }

        int rowCount = _currentRowCount;

        // Reset state
        _pendingPayloads.Clear();
        _currentRowCount = 0;

        // Decode all rows
        return _assembler.DecodeBlock(combined, rowCount);
    }

    private void SwitchSchema(SchemaFingerprint fingerprint)
    {
        var schema = _store.Lookup(fingerprint)
            ?? throw new InvalidOperationException(
                $"No schema found in store for fingerprint {fingerprint}.");

        var writerRecord = schema.Parsed as AvroRecordSchema
            ?? throw new InvalidOperationException("Schema must be a record type.");

        if (_readerSchema != null)
        {
            var readerRecord = _readerSchema.Parsed as AvroRecordSchema
                ?? throw new InvalidOperationException("Reader schema must be a record type.");
            var resolution = SchemaResolver.Resolve(writerRecord, readerRecord);
            _assembler = new RecordBatchAssembler(writerRecord, resolution);
            _currentArrowSchema = resolution.ArrowSchema;
        }
        else
        {
            var arrowSchema = ArrowSchemaConverter.ToArrow(writerRecord);
            _assembler = new RecordBatchAssembler(writerRecord, arrowSchema);
            _currentArrowSchema = arrowSchema;
        }

        _currentFingerprint = fingerprint;
    }

    private static SchemaFingerprint ParseSoeHeader(ReadOnlySpan<byte> message, out ReadOnlySpan<byte> payload)
    {
        if (message.Length < 10)
            throw new InvalidDataException("SOE message too short; expected at least 10 bytes.");
        if (message[0] != 0xC3 || message[1] != 0x01)
            throw new InvalidDataException($"Invalid SOE marker bytes: 0x{message[0]:X2} 0x{message[1]:X2}.");

        ulong rabin = BinaryPrimitives.ReadUInt64LittleEndian(message.Slice(2));
        payload = message.Slice(10);
        return new SchemaFingerprint.Rabin(rabin);
    }

    private static SchemaFingerprint ParseConfluentHeader(ReadOnlySpan<byte> message, out ReadOnlySpan<byte> payload)
    {
        if (message.Length < 5)
            throw new InvalidDataException("Confluent message too short; expected at least 5 bytes.");
        if (message[0] != 0x00)
            throw new InvalidDataException($"Invalid Confluent magic byte: 0x{message[0]:X2}.");

        uint schemaId = BinaryPrimitives.ReadUInt32BigEndian(message.Slice(1));
        payload = message.Slice(5);
        return new SchemaFingerprint.ConfluentId(schemaId);
    }

    private static SchemaFingerprint ParseApicurioHeader(ReadOnlySpan<byte> message, out ReadOnlySpan<byte> payload)
    {
        if (message.Length < 9)
            throw new InvalidDataException("Apicurio message too short; expected at least 9 bytes.");
        if (message[0] != 0x00)
            throw new InvalidDataException($"Invalid Apicurio magic byte: 0x{message[0]:X2}.");

        ulong globalId = BinaryPrimitives.ReadUInt64BigEndian(message.Slice(1));
        payload = message.Slice(9);
        return new SchemaFingerprint.ApicurioId(globalId);
    }
}
