// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Encodings;
using EngineeredWood.IO;

namespace EngineeredWood.Lance.Table.Indices;

/// <summary>
/// Reader for a Lance BITMAP scalar index. A BITMAP index lives in
/// <c>{dataset}/_indices/{uuid}/</c> as a single Lance file
/// <c>bitmap_page_lookup.lance</c> with schema
/// <c>(keys: T, bitmaps: binary)</c>. There is one row per unique
/// indexed value; the <c>bitmaps</c> column carries a Lance-specific
/// "Roaring64-by-fragment" encoding of the rows containing that value.
///
/// <para><b>Wire format of each bitmaps[i] value</b>:</para>
/// <code>
///   [u32 LE: number of (fragment_id, bitmap) entries]
///   For each entry:
///     [u32 LE: fragment_id]
///     [u32 LE: bitmap_size_bytes]
///     [bitmap bytes — CRoaring portable Roaring32Map of row offsets
///                     within that fragment]
/// </code>
///
/// <para>This shape is Lance-specific; standard CRoaring Roaring64Map
/// would interleave high32 bits with each Roaring32Map without an
/// explicit length prefix. Lance prefixes each blob with its byte
/// length so the reader can skip past unfamiliar containers without
/// fully decoding them.</para>
///
/// <para>Only equality queries make sense on a bitmap index — there's
/// no cheap range pruning since rows are bucketed by exact value.
/// <see cref="QueryEqualAsync"/> finds the matching key, parses the
/// blob, and emits <see cref="IndexedRowAddress"/>es.</para>
///
/// <para>This slice supports Int32 indexed values. Other primitive
/// types follow the same wire format and can be added when fixtures
/// exist.</para>
/// </summary>
public sealed class BitmapIndex : IAsyncDisposable
{
    private readonly LanceFileReader _file;
    private (Int32Array Keys, BinaryArray Bitmaps)? _cached;

    private BitmapIndex(LanceFileReader file) { _file = file; }

    /// <summary>The Arrow type of the indexed value column.</summary>
    public IArrowType ValueType =>
        _file.Schema.GetFieldByName("keys").DataType;

    /// <summary>
    /// Open a BITMAP index from an index directory using a local filesystem.
    /// </summary>
    public static ValueTask<BitmapIndex> OpenAsync(
        string indexDirectory, CancellationToken cancellationToken = default)
    {
        var fs = new EngineeredWood.IO.Local.LocalTableFileSystem(indexDirectory);
        return OpenAsync(fs, ".", cancellationToken);
    }

    /// <summary>
    /// Open a BITMAP index from a caller-provided filesystem rooted at the
    /// dataset, given the relative path to the index directory.
    /// </summary>
    public static async ValueTask<BitmapIndex> OpenAsync(
        ITableFileSystem fs, string indexDirectory,
        CancellationToken cancellationToken = default)
    {
        string path = JoinPath(indexDirectory, "bitmap_page_lookup.lance");
        IRandomAccessFile? raf = null;
        LanceFileReader? reader = null;
        try
        {
            raf = await fs.OpenReadAsync(path, cancellationToken).ConfigureAwait(false);
            reader = await LanceFileReader.OpenAsync(raf, ownsReader: true, cancellationToken).ConfigureAwait(false);
            raf = null;  // ownership transferred
            var taken = reader; reader = null;
            return new BitmapIndex(taken);
        }
        finally
        {
            if (reader is not null) await reader.DisposeAsync().ConfigureAwait(false);
            if (raf is not null) await raf.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>
    /// Find every row whose indexed value equals <paramref name="value"/>.
    /// Returns the row addresses; empty list if no such value exists in
    /// the index.
    /// </summary>
    public async Task<IReadOnlyList<IndexedRowAddress>> QueryEqualAsync(
        int value, CancellationToken cancellationToken = default)
    {
        if (ValueType is not Int32Type)
            throw new NotSupportedException(
                $"BitmapIndex.QueryEqualAsync(int) requires an Int32 index; this index is {ValueType}.");

        var (keys, bitmaps) = await EnsureLoadedAsync(cancellationToken).ConfigureAwait(false);

        // Linear scan for the matching key. The keys are stored sorted
        // by lance-rs so a binary search would be a fine micro-opt; for
        // this slice the simpler scan is fine.
        for (int i = 0; i < keys.Length; i++)
        {
            int? k = keys.GetValue(i);
            if (k != value) continue;
            return ParseBitmapBlob(bitmaps.GetBytes(i));
        }
        return System.Array.Empty<IndexedRowAddress>();
    }

    private async Task<(Int32Array Keys, BinaryArray Bitmaps)> EnsureLoadedAsync(
        CancellationToken cancellationToken)
    {
        if (_cached is { } c) return c;

        var schema = _file.Schema;
        if (schema.GetFieldByName("keys") is null || schema.GetFieldByName("bitmaps") is null)
            throw new LanceTableFormatException(
                "BITMAP page-lookup file is missing one of the (keys, bitmaps) columns.");
        if (schema.GetFieldByName("keys").DataType is not Int32Type)
            throw new NotSupportedException(
                $"BitmapIndex with non-Int32 keys ({schema.GetFieldByName("keys").DataType}) is not yet supported.");

        var keys = (Int32Array)await _file.ReadColumnAsync(0, cancellationToken).ConfigureAwait(false);
        var bitmaps = (BinaryArray)await _file.ReadColumnAsync(1, cancellationToken).ConfigureAwait(false);
        _cached = (keys, bitmaps);
        return _cached.Value;
    }

    private static IReadOnlyList<IndexedRowAddress> ParseBitmapBlob(ReadOnlySpan<byte> blob)
    {
        if (blob.Length < 4) return System.Array.Empty<IndexedRowAddress>();
        int cursor = 0;
        uint count = BinaryPrimitives.ReadUInt32LittleEndian(blob.Slice(cursor, 4)); cursor += 4;
        var result = new List<IndexedRowAddress>();
        for (uint e = 0; e < count; e++)
        {
            if (cursor + 8 > blob.Length)
                throw new LanceTableFormatException(
                    $"BITMAP blob entry {e} truncated (cursor={cursor}, len={blob.Length}).");
            uint fragmentId = BinaryPrimitives.ReadUInt32LittleEndian(blob.Slice(cursor, 4)); cursor += 4;
            uint sz = BinaryPrimitives.ReadUInt32LittleEndian(blob.Slice(cursor, 4)); cursor += 4;
            int szInt = checked((int)sz);
            if (cursor + szInt > blob.Length)
                throw new LanceTableFormatException(
                    $"BITMAP blob bitmap {e} of size {sz} extends past end (cursor={cursor}, len={blob.Length}).");

            var roaring = blob.Slice(cursor, szInt);
            cursor += szInt;

            // The inner Roaring32Map encodes row offsets within the
            // fragment (a u32 value space of which we use the low 32 bits).
            // CRoaring portable serialisation matches the Lance flavour.
            uint fragForCallback = fragmentId;
            var local = result;  // captured by lambda
            RoaringBitmap.DeserializePortable(roaring, v =>
            {
                local.Add(new IndexedRowAddress(
                    FragmentId: fragForCallback,
                    RowOffset: checked((uint)v)));
            }, RoaringBitmap.RoaringFormat.CRoaring);
        }
        return result;
    }

    public async ValueTask DisposeAsync()
    {
        await _file.DisposeAsync().ConfigureAwait(false);
    }

    private static string JoinPath(string a, string b) =>
        a == "." || a == string.Empty
            ? b
            : a.EndsWith("/", StringComparison.Ordinal) ? a + b : a + "/" + b;
}
