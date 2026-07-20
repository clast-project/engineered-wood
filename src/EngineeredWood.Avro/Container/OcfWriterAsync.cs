// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Security.Cryptography;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Buffers;
using EngineeredWood.Compression;
using EngineeredWood.Encodings;

namespace EngineeredWood.Avro.Container;

/// <summary>
/// Asynchronously writes Avro Object Container Format (OCF) files: header + data blocks.
/// </summary>
internal sealed class OcfWriterAsync : IAsyncDisposable
{
    private static readonly byte[] Magic = "Obj\x01"u8.ToArray();

    private readonly Stream _stream;
    private readonly AvroCodec _codec;
    private readonly BlockCompressionLevel? _level;
    private readonly int? _customLevel;
    private readonly byte[] _syncMarker;
    private readonly GrowableBuffer _compressBuffer = new(4096);
    private bool _headerWritten;
    private bool _finished;

    public OcfWriterAsync(
        Stream stream, AvroCodec codec,
        BlockCompressionLevel? level = null, int? customLevel = null)
    {
        _stream = stream;
        _codec = codec;
        _level = level;
        _customLevel = customLevel;
#if NET6_0_OR_GREATER
        _syncMarker = RandomNumberGenerator.GetBytes(16);
#else
        _syncMarker = new byte[16];
        using (var rng = RandomNumberGenerator.Create())
            rng.GetBytes(_syncMarker);
#endif
    }

    public async ValueTask WriteHeaderAsync(AvroRecordSchema schema, CancellationToken ct = default)
    {
        if (_headerWritten) throw new InvalidOperationException("Header already written.");
        _headerWritten = true;

        // Magic
        await WriteMemoryAsync(Magic, ct).ConfigureAwait(false);

        // Metadata map
        var schemaJson = AvroSchemaWriter.ToJson(schema);
        var entries = new Dictionary<string, byte[]>
        {
            ["avro.schema"] = System.Text.Encoding.UTF8.GetBytes(schemaJson),
        };

        if (_codec != AvroCodec.Null)
            entries["avro.codec"] = System.Text.Encoding.UTF8.GetBytes(AvroCompression.CodecName(_codec));

        await WriteMetadataMapAsync(entries, ct).ConfigureAwait(false);

        // Sync marker
        await WriteMemoryAsync(_syncMarker, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes a complete block of pre-encoded data with a known object count.
    /// Compression is CPU-bound and performed synchronously; only I/O is async.
    /// </summary>
    public async ValueTask WriteBlockAsync(ReadOnlyMemory<byte> encodedData, int objectCount,
        CancellationToken ct = default)
    {
        await WriteVarLongAsync(objectCount, ct).ConfigureAwait(false);
        if (_codec == AvroCodec.Null)
        {
            await WriteVarLongAsync(encodedData.Length, ct).ConfigureAwait(false);
            await WriteMemoryAsync(encodedData, ct).ConfigureAwait(false);
        }
        else
        {
            _compressBuffer.Reset();
            AvroCompression.Compress(_codec, encodedData.Span, _compressBuffer, _level, _customLevel);
            await WriteVarLongAsync(_compressBuffer.Length, ct).ConfigureAwait(false);
            await WriteMemoryAsync(_compressBuffer.WrittenMemory, ct).ConfigureAwait(false);
        }
        await WriteMemoryAsync(_syncMarker, ct).ConfigureAwait(false);
    }

    public async ValueTask FinishAsync(CancellationToken ct = default)
    {
        if (_finished) return;
        _finished = true;
        await _stream.FlushAsync(ct).ConfigureAwait(false);
    }

    public async ValueTask DisposeAsync()
    {
        if (!_finished) await FinishAsync().ConfigureAwait(false);
    }

    private async ValueTask WriteMetadataMapAsync(Dictionary<string, byte[]> entries,
        CancellationToken ct)
    {
        if (entries.Count > 0)
        {
            // Write block count
            await WriteVarLongAsync(entries.Count, ct).ConfigureAwait(false);
            foreach (var kvp in entries)
            {
                await WriteAvroStringAsync(kvp.Key, ct).ConfigureAwait(false);
                await WriteAvroBytesAsync(kvp.Value, ct).ConfigureAwait(false);
            }
        }
        // Terminate map with 0-count block
        await WriteVarLongAsync(0, ct).ConfigureAwait(false);
    }

    private async ValueTask WriteAvroStringAsync(string value, CancellationToken ct)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        await WriteVarLongAsync(bytes.Length, ct).ConfigureAwait(false);
        await WriteMemoryAsync(bytes, ct).ConfigureAwait(false);
    }

    private async ValueTask WriteAvroBytesAsync(ReadOnlyMemory<byte> value, CancellationToken ct)
    {
        await WriteVarLongAsync(value.Length, ct).ConfigureAwait(false);
        await WriteMemoryAsync(value, ct).ConfigureAwait(false);
    }

    private async ValueTask WriteVarLongAsync(long value, CancellationToken ct)
    {
        // Encode varint into a small stack buffer, then write async
        var buf = new byte[10]; // max varint size
        int written = Varint.WriteSigned(buf, value);
        await WriteMemoryAsync(buf.AsMemory(0, written), ct).ConfigureAwait(false);
    }

    private ValueTask WriteMemoryAsync(ReadOnlyMemory<byte> data, CancellationToken ct)
    {
#if NETSTANDARD2_0
        var array = data.ToArray();
        return new ValueTask(_stream.WriteAsync(array, 0, array.Length, ct));
#else
        return _stream.WriteAsync(data, ct);
#endif
    }
}
