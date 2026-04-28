// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow.Ipc;
using EwCodec = EngineeredWood.Compression.CompressionCodec;
using EwDecompressor = EngineeredWood.Compression.Decompressor;

namespace EngineeredWood.Lance.Table.Deletions;

/// <summary>
/// Adapter that lets <see cref="ArrowFileReader"/> decompress its
/// per-buffer compressed payloads (LZ4-frame, ZSTD) by delegating to
/// <see cref="EwDecompressor"/>. Pylance's deletion <c>.arrow</c> files
/// are ZSTD-compressed Arrow IPC streams, so we have to plug in a
/// factory at reader-construction time or the reader throws "no
/// ICompressionCodecFactory has been configured".
/// </summary>
internal sealed class LanceArrowCompressionFactory : ICompressionCodecFactory
{
    public static LanceArrowCompressionFactory Instance { get; } = new();

    public ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType)
        => CreateCodec(compressionCodecType, compressionLevel: 0);

    public ICompressionCodec CreateCodec(CompressionCodecType compressionCodecType, int compressionLevel)
        => compressionCodecType switch
        {
            CompressionCodecType.Zstd => new ZstdCodec(),
            CompressionCodecType.Lz4Frame => new Lz4FrameCodec(),
            _ => throw new NotSupportedException(
                $"Arrow IPC compression codec '{compressionCodecType}' is not supported."),
        };

    private sealed class ZstdCodec : ICompressionCodec
    {
        public int Decompress(ReadOnlyMemory<byte> source, Memory<byte> destination)
            => EwDecompressor.Decompress(EwCodec.Zstd, source.Span, destination.Span);
        public void Dispose() { }
    }

    private sealed class Lz4FrameCodec : ICompressionCodec
    {
        public int Decompress(ReadOnlyMemory<byte> source, Memory<byte> destination)
            => EwDecompressor.Decompress(EwCodec.Lz4, source.Span, destination.Span);
        public void Dispose() { }
    }
}
