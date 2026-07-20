// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Avro.Schema;
using EngineeredWood.Buffers;
using EngineeredWood.Encodings;

namespace EngineeredWood.Avro.Container;

/// <summary>
/// Reads Avro Object Container Format (OCF) files: header + data blocks.
/// </summary>
internal sealed class OcfReader : IDisposable
{
    private static readonly byte[] Magic = "Obj\x01"u8.ToArray();

    private readonly Stream _stream;
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

    private OcfReader(Stream stream, AvroRecordSchema writerSchema, AvroCodec codec,
        byte[] syncMarker, IReadOnlyDictionary<string, byte[]> metadata)
    {
        _stream = stream;
        _writerSchema = writerSchema;
        _codec = codec;
        _syncMarker = syncMarker;
        _metadata = metadata;
    }

    public static OcfReader Open(Stream stream)
    {
        // Read magic bytes
        Span<byte> magic = stackalloc byte[4];
        ReadExact(stream, magic);
        if (!magic.SequenceEqual(Magic))
            throw new InvalidDataException("Not an Avro Object Container File (invalid magic).");

        // Read file metadata (Avro map<bytes>)
        var metadata = ReadMetadataMap(stream);

        // Read 16-byte sync marker
        var syncMarker = new byte[16];
        ReadExact(stream, syncMarker);

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

        return new OcfReader(stream, recordSchema, codec, syncMarker, metadata);
    }

    /// <summary>
    /// Reads the next data block. Returns null on EOF.
    /// The returned memory is valid until the next call to <see cref="ReadBlock"/>.
    /// </summary>
    public (ReadOnlyMemory<byte> data, long objectCount)? ReadBlock()
    {
        if (_eof) return null;

        // Read object count (long varint)
        long objectCount;
        try
        {
            objectCount = ReadVarLong(_stream);
        }
        catch (EndOfStreamException)
        {
            _eof = true;
            return null;
        }

        // Read block byte size (long varint)
        long blockSize = ReadVarLong(_stream);
        if (blockSize < 0 || blockSize > int.MaxValue)
            throw new InvalidDataException($"Invalid block size: {blockSize}");

        // Read compressed block data into reusable buffer
        int size = (int)blockSize;
        if (_readBuffer.Length < size)
            _readBuffer = new byte[size];
        ReadExact(_stream, _readBuffer.AsSpan(0, size));

        // Read and verify sync marker
        Span<byte> sync = stackalloc byte[16];
        ReadExact(_stream, sync);
        if (!sync.SequenceEqual(_syncMarker))
            throw new InvalidDataException("Sync marker mismatch in OCF data block.");

        // Decompress into reusable buffer (null codec uses read buffer directly)
        if (_codec == AvroCodec.Null)
            return (new ReadOnlyMemory<byte>(_readBuffer, 0, size), objectCount);

        _decompressBuffer.Reset();
        AvroCompression.Decompress(_codec, _readBuffer, 0, size, _decompressBuffer);
        return (_decompressBuffer.WrittenMemory, objectCount);
    }

    private static Dictionary<string, byte[]> ReadMetadataMap(Stream stream)
    {
        var metadata = new Dictionary<string, byte[]>();

        // Avro map encoding: blocks of (count, key-value pairs), terminated by 0-count block
        while (true)
        {
            long count = ReadVarLong(stream);
            if (count == 0) break;

            if (count < 0)
            {
                // Negative count: next long is block byte size (skip it)
                count = -count;
                ReadVarLong(stream); // block byte size — we don't need it
            }

            for (long i = 0; i < count; i++)
            {
                var key = ReadAvroString(stream);
                var value = ReadAvroBytes(stream);
                metadata[key] = value;
            }
        }

        return metadata;
    }

    private static long ReadVarLong(Stream stream)
    {
        // Read zigzag-encoded varint from stream, byte by byte
        long result = 0;
        int shift = 0;
        while (true)
        {
            int b = stream.ReadByte();
            if (b < 0) throw new EndOfStreamException("Unexpected end of stream reading varint.");
            result |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return Varint.ZigzagDecode(result);
            shift += 7;
        }
    }

    private static string ReadAvroString(Stream stream)
    {
        long len = ReadVarLong(stream);
        if (len < 0 || len > int.MaxValue)
            throw new InvalidDataException($"Invalid string length: {len}");
        var buf = new byte[(int)len];
        ReadExact(stream, buf);
        return System.Text.Encoding.UTF8.GetString(buf);
    }

    private static byte[] ReadAvroBytes(Stream stream)
    {
        long len = ReadVarLong(stream);
        if (len < 0 || len > int.MaxValue)
            throw new InvalidDataException($"Invalid bytes length: {len}");
        var buf = new byte[(int)len];
        ReadExact(stream, buf);
        return buf;
    }

    private static void ReadExact(Stream stream, Span<byte> buffer)
    {
        int total = 0;
        while (total < buffer.Length)
        {
#if NETSTANDARD2_0
            var tmp = new byte[buffer.Length - total];
            int read = stream.Read(tmp, 0, tmp.Length);
            if (read == 0) throw new EndOfStreamException("Unexpected end of stream.");
            tmp.AsSpan(0, read).CopyTo(buffer.Slice(total));
#else
            int read = stream.Read(buffer[total..]);
            if (read == 0) throw new EndOfStreamException("Unexpected end of stream.");
#endif
            total += read;
        }
    }

    public void Dispose() { } // We don't own the stream
}
