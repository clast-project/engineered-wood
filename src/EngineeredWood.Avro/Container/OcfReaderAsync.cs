// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers;
using System.IO.Pipelines;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Buffers;
using EngineeredWood.Encodings;

namespace EngineeredWood.Avro.Container;

/// <summary>
/// Asynchronously reads Avro Object Container Format (OCF) files: header + data blocks.
/// Uses <see cref="PipeReader"/> to buffer stream data for efficient synchronous varint parsing.
/// </summary>
internal sealed class OcfReaderAsync : IAsyncDisposable
{
    private static readonly byte[] Magic = "Obj\x01"u8.ToArray();

    private readonly PipeReader _pipeReader;
    private readonly byte[] _syncMarker;
    private readonly AvroCodec _codec;
    private readonly AvroRecordSchema _writerSchema;
    private readonly IReadOnlyDictionary<string, byte[]> _metadata;
    private readonly GrowableBuffer _decompressBuffer = new(4096);
    private byte[] _readBuffer = new byte[4096];
    private bool _eof;

    public AvroRecordSchema WriterSchema => _writerSchema;
    public AvroCodec Codec => _codec;
    public IReadOnlyDictionary<string, byte[]> Metadata => _metadata;

    private OcfReaderAsync(PipeReader pipeReader, AvroRecordSchema writerSchema, AvroCodec codec,
        byte[] syncMarker, IReadOnlyDictionary<string, byte[]> metadata)
    {
        _pipeReader = pipeReader;
        _writerSchema = writerSchema;
        _codec = codec;
        _syncMarker = syncMarker;
        _metadata = metadata;
    }

    public static async ValueTask<OcfReaderAsync> OpenAsync(Stream stream, CancellationToken ct = default)
    {
        var pipeReader = PipeReader.Create(stream);

        // Read magic bytes
        var result = await pipeReader.ReadAtLeastAsync(4, ct).ConfigureAwait(false);
        if (result.Buffer.Length < 4)
            throw new InvalidDataException("Not an Avro Object Container File (truncated).");
        Span<byte> magicBuf = stackalloc byte[4];
        result.Buffer.Slice(0, 4).CopyTo(magicBuf);
        pipeReader.AdvanceTo(result.Buffer.GetPosition(4));
        if (!magicBuf.SequenceEqual(Magic))
            throw new InvalidDataException("Not an Avro Object Container File (invalid magic).");

        // Read file metadata (Avro map<bytes>)
        var metadata = await ReadMetadataMapAsync(pipeReader, ct).ConfigureAwait(false);

        // Read 16-byte sync marker
        result = await pipeReader.ReadAtLeastAsync(16, ct).ConfigureAwait(false);
        if (result.Buffer.Length < 16)
            throw new EndOfStreamException("Unexpected end of stream reading sync marker.");
        var syncMarker = result.Buffer.Slice(0, 16).ToArray();
        pipeReader.AdvanceTo(result.Buffer.GetPosition(16));

        // Extract schema
        if (!metadata.TryGetValue("avro.schema", out var schemaBytes))
            throw new InvalidDataException("Missing avro.schema in OCF header.");
        var schemaJson = System.Text.Encoding.UTF8.GetString(schemaBytes);
        var schemaNode = AvroSchemaParser.Parse(schemaJson);
        if (schemaNode is not AvroRecordSchema recordSchema)
            throw new InvalidDataException("Top-level Avro schema must be a record.");

        // Extract codec
        var codec = AvroCodec.Null;
        if (metadata.TryGetValue("avro.codec", out var codecBytes))
        {
            var codecName = System.Text.Encoding.UTF8.GetString(codecBytes);
            codec = AvroCompression.ParseCodecName(codecName);
        }

        return new OcfReaderAsync(pipeReader, recordSchema, codec, syncMarker, metadata);
    }

    /// <summary>
    /// Reads the next data block asynchronously. Returns null on EOF.
    /// The returned memory is valid until the next call to <see cref="ReadBlockAsync"/>.
    /// </summary>
    public async ValueTask<(ReadOnlyMemory<byte> data, long objectCount)?> ReadBlockAsync(CancellationToken ct = default)
    {
        if (_eof) return null;

        // Read object count (long varint)
        long objectCount;
        try
        {
            objectCount = await ReadVarLongAsync(_pipeReader, ct).ConfigureAwait(false);
        }
        catch (EndOfStreamException)
        {
            _eof = true;
            return null;
        }

        // Read block byte size (long varint)
        long blockSize = await ReadVarLongAsync(_pipeReader, ct).ConfigureAwait(false);
        if (blockSize < 0 || blockSize > int.MaxValue)
            throw new InvalidDataException($"Invalid block size: {blockSize}");

        // Read block data + sync marker in a single async call
        int size = (int)blockSize;
        int totalNeeded = size + 16;
        var readResult = await _pipeReader.ReadAtLeastAsync(totalNeeded, ct).ConfigureAwait(false);
        if (readResult.Buffer.Length < totalNeeded)
            throw new EndOfStreamException("Unexpected end of stream reading data block.");

        // Copy block data into reusable buffer
        if (_readBuffer.Length < size)
            _readBuffer = new byte[size];
        readResult.Buffer.Slice(0, size).CopyTo(_readBuffer);

        // Verify sync marker directly from pipe buffer (no allocation)
        var syncSlice = readResult.Buffer.Slice(size, 16);
        if (!SequenceEquals(syncSlice, _syncMarker))
            throw new InvalidDataException("Sync marker mismatch in OCF data block.");

        _pipeReader.AdvanceTo(readResult.Buffer.GetPosition(totalNeeded));

        // Decompress into reusable buffer (null codec uses read buffer directly)
        if (_codec == AvroCodec.Null)
            return (new ReadOnlyMemory<byte>(_readBuffer, 0, size), objectCount);

        _decompressBuffer.Reset();
        AvroCompression.Decompress(_codec, _readBuffer, 0, size, _decompressBuffer);
        return (_decompressBuffer.WrittenMemory, objectCount);
    }

    private static async ValueTask<Dictionary<string, byte[]>> ReadMetadataMapAsync(
        PipeReader reader, CancellationToken ct)
    {
        var metadata = new Dictionary<string, byte[]>();

        // Avro map encoding: blocks of (count, key-value pairs), terminated by 0-count block
        while (true)
        {
            long count = await ReadVarLongAsync(reader, ct).ConfigureAwait(false);
            if (count == 0) break;

            if (count < 0)
            {
                // Negative count: next long is block byte size (skip it)
                count = -count;
                await ReadVarLongAsync(reader, ct).ConfigureAwait(false); // block byte size
            }

            for (long i = 0; i < count; i++)
            {
                var key = await ReadAvroStringAsync(reader, ct).ConfigureAwait(false);
                var value = await ReadAvroBytesAsync(reader, ct).ConfigureAwait(false);
                metadata[key] = value;
            }
        }

        return metadata;
    }

    /// <summary>
    /// Reads a zigzag-encoded varint from the pipe. Uses synchronous parsing over buffered data,
    /// only awaiting when the pipe needs to fetch more data from the underlying stream.
    /// </summary>
    private static async ValueTask<long> ReadVarLongAsync(PipeReader reader, CancellationToken ct)
    {
        while (true)
        {
            var result = await reader.ReadAsync(ct).ConfigureAwait(false);
            if (result.Buffer.IsEmpty && result.IsCompleted)
                throw new EndOfStreamException("Unexpected end of stream reading varint.");

            var seqReader = new SequenceReader<byte>(result.Buffer);
            if (TryReadVarLong(ref seqReader, out long value))
            {
                reader.AdvanceTo(seqReader.Position);
                return value;
            }

            // Not enough bytes buffered — mark all as examined, consume nothing, and wait for more
            reader.AdvanceTo(result.Buffer.Start, result.Buffer.End);

            if (result.IsCompleted)
                throw new EndOfStreamException("Unexpected end of stream reading varint.");
        }
    }

    /// <summary>
    /// Tries to parse a zigzag-encoded varint from the sequence reader.
    /// Returns false if there aren't enough bytes available yet.
    /// </summary>
    private static bool TryReadVarLong(ref SequenceReader<byte> reader, out long value)
    {
        long result = 0;
        int shift = 0;

        while (reader.TryRead(out byte b))
        {
            result |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                value = Varint.ZigzagDecode(result);
                return true;
            }
            shift += 7;
        }

        // Incomplete varint — caller uses buffer start as consumed position, so no rewind needed
        value = 0;
        return false;
    }

    private static async ValueTask<string> ReadAvroStringAsync(PipeReader reader, CancellationToken ct)
    {
        long len = await ReadVarLongAsync(reader, ct).ConfigureAwait(false);
        if (len < 0 || len > int.MaxValue)
            throw new InvalidDataException($"Invalid string length: {len}");
        var buf = await ReadExactAsync(reader, (int)len, ct).ConfigureAwait(false);
        return System.Text.Encoding.UTF8.GetString(buf);
    }

    private static async ValueTask<byte[]> ReadAvroBytesAsync(PipeReader reader, CancellationToken ct)
    {
        long len = await ReadVarLongAsync(reader, ct).ConfigureAwait(false);
        if (len < 0 || len > int.MaxValue)
            throw new InvalidDataException($"Invalid bytes length: {len}");
        return await ReadExactAsync(reader, (int)len, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads exactly <paramref name="count"/> bytes from the pipe. Used for header/metadata parsing only.
    /// </summary>
    private static async ValueTask<byte[]> ReadExactAsync(PipeReader reader, int count, CancellationToken ct)
    {
        if (count == 0) return [];

        var result = await reader.ReadAtLeastAsync(count, ct).ConfigureAwait(false);
        if (result.Buffer.Length < count)
            throw new EndOfStreamException("Unexpected end of stream.");
        var data = result.Buffer.Slice(0, count).ToArray();
        reader.AdvanceTo(result.Buffer.GetPosition(count));
        return data;
    }

    private static bool SequenceEquals(ReadOnlySequence<byte> sequence, ReadOnlySpan<byte> expected)
    {
        if (sequence.IsSingleSegment)
#if NETSTANDARD2_0
            return sequence.First.Span.SequenceEqual(expected);
#else
            return sequence.FirstSpan.SequenceEqual(expected);
#endif

        Span<byte> temp = stackalloc byte[16];
        sequence.CopyTo(temp);
        return temp.SequenceEqual(expected);
    }

    public ValueTask DisposeAsync()
    {
        _pipeReader.Complete();
        return default;
    }
}
