// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;
using EngineeredWood.Avro;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Encoding;
using EngineeredWood.Buffers;
using EngineeredWood.Iceberg.Serialization;
using EngineeredWood.IO;

namespace EngineeredWood.Iceberg.Manifest;

/// <summary>
/// Reads and writes Iceberg manifest files and manifest lists,
/// dispatching between Avro and JSON formats based on file extension.
/// </summary>
public static class ManifestIO
{
    private static readonly JsonSerializerOptions JsonOptions = CreateJsonOptions();

    private static JsonSerializerOptions CreateJsonOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = KebabCaseNamingPolicy.Instance,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = false,
            // Source-generated context only — no reflection fallback, so the library stays trim/AOT safe.
            TypeInfoResolver = IcebergJsonContext.Default,
        };
        return options;
    }

    // --- Dispatch by format ---

    /// <summary>
    /// Writes manifest entries to the given path, choosing Avro or JSON format
    /// based on the file extension. Returns the number of bytes written.
    /// </summary>
    public static async ValueTask<long> WriteManifestAsync(
        ITableFileSystem fs, string path, IReadOnlyList<ManifestEntry> entries,
        CancellationToken ct = default)
    {
        if (path.EndsWith(".avro", StringComparison.OrdinalIgnoreCase))
            return await WriteManifestAvroAsync(fs, path, entries, ct).ConfigureAwait(false);
        else
            return await WriteManifestJsonAsync(fs, path, entries, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads manifest entries from the given path, choosing Avro or JSON format
    /// based on the file extension.
    /// </summary>
    public static async ValueTask<List<ManifestEntry>> ReadManifestAsync(
        ITableFileSystem fs, string path, CancellationToken ct = default)
    {
        if (path.EndsWith(".avro", StringComparison.OrdinalIgnoreCase))
            return await ReadManifestAvroAsync(fs, path, ct).ConfigureAwait(false);
        else
            return await ReadManifestJsonAsync(fs, path, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Writes manifest list entries to the given path, choosing Avro or JSON format
    /// based on the file extension. Returns the number of bytes written.
    /// </summary>
    public static async ValueTask<long> WriteManifestListAsync(
        ITableFileSystem fs, string path, IReadOnlyList<ManifestListEntry> entries,
        CancellationToken ct = default)
    {
        if (path.EndsWith(".avro", StringComparison.OrdinalIgnoreCase))
            return await WriteManifestListAvroAsync(fs, path, entries, ct).ConfigureAwait(false);
        else
            return await WriteManifestListJsonAsync(fs, path, entries, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads manifest list entries from the given path, choosing Avro or JSON format
    /// based on the file extension.
    /// </summary>
    public static async ValueTask<List<ManifestListEntry>> ReadManifestListAsync(
        ITableFileSystem fs, string path, CancellationToken ct = default)
    {
        if (path.EndsWith(".avro", StringComparison.OrdinalIgnoreCase))
            return await ReadManifestListAvroAsync(fs, path, ct).ConfigureAwait(false);
        else
            return await ReadManifestListJsonAsync(fs, path, ct).ConfigureAwait(false);
    }

    // --- JSON format ---

    private static async ValueTask<long> WriteManifestJsonAsync(
        ITableFileSystem fs, string path, IReadOnlyList<ManifestEntry> entries,
        CancellationToken ct = default)
    {
        var ms = new MemoryStream();
        JsonSerializer.Serialize(ms, entries, JsonOptions.TypeInfo<IReadOnlyList<ManifestEntry>>());
        var data = ms.ToArray();
        await fs.WriteAllBytesAsync(path, data, ct).ConfigureAwait(false);
        return data.Length;
    }

    private static async ValueTask<List<ManifestEntry>> ReadManifestJsonAsync(
        ITableFileSystem fs, string path, CancellationToken ct = default)
    {
        var data = await fs.ReadAllBytesAsync(path, ct).ConfigureAwait(false);
        return JsonSerializer.Deserialize(data, JsonOptions.TypeInfo<List<ManifestEntry>>())
            ?? throw new InvalidOperationException($"Failed to read manifest: {path}");
    }

    private static async ValueTask<long> WriteManifestListJsonAsync(
        ITableFileSystem fs, string path, IReadOnlyList<ManifestListEntry> entries,
        CancellationToken ct = default)
    {
        var ms = new MemoryStream();
        JsonSerializer.Serialize(ms, entries, JsonOptions.TypeInfo<IReadOnlyList<ManifestListEntry>>());
        var data = ms.ToArray();
        await fs.WriteAllBytesAsync(path, data, ct).ConfigureAwait(false);
        return data.Length;
    }

    private static async ValueTask<List<ManifestListEntry>> ReadManifestListJsonAsync(
        ITableFileSystem fs, string path, CancellationToken ct = default)
    {
        var data = await fs.ReadAllBytesAsync(path, ct).ConfigureAwait(false);
        return JsonSerializer.Deserialize(data, JsonOptions.TypeInfo<List<ManifestListEntry>>())
            ?? throw new InvalidOperationException($"Failed to read manifest list: {path}");
    }

    // --- Avro format ---

    private static async ValueTask<long> WriteManifestAvroAsync(
        ITableFileSystem fs, string path, IReadOnlyList<ManifestEntry> entries,
        CancellationToken ct = default)
    {
        var ms = new MemoryStream();
        using (var writer = new OcfWriter(ms, AvroCodec.Null))
        {
            writer.WriteHeader(ManifestAvroSchemas.ManifestEntrySchema);
            var buf = new GrowableBuffer();
            var bw = new AvroBinaryWriter(buf);
            foreach (var entry in entries)
                EncodeManifestEntry(bw, entry);
            writer.WriteBlock(bw.WrittenSpan, entries.Count);
        }
        var data = ms.ToArray();
        await fs.WriteAllBytesAsync(path, data, ct).ConfigureAwait(false);
        return data.Length;
    }

    private static async ValueTask<List<ManifestEntry>> ReadManifestAvroAsync(
        ITableFileSystem fs, string path, CancellationToken ct = default)
    {
        var data = await fs.ReadAllBytesAsync(path, ct).ConfigureAwait(false);
        using var ms = new MemoryStream(data);
        using var reader = OcfReader.Open(ms);
        var results = new List<ManifestEntry>();
        while (reader.ReadBlock() is { } block)
        {
            var br = new AvroBinaryReader(block.data.Span);
            for (long i = 0; i < block.objectCount; i++)
                results.Add(DecodeManifestEntry(ref br));
        }
        return results;
    }

    private static async ValueTask<long> WriteManifestListAvroAsync(
        ITableFileSystem fs, string path, IReadOnlyList<ManifestListEntry> entries,
        CancellationToken ct = default)
    {
        var ms = new MemoryStream();
        using (var writer = new OcfWriter(ms, AvroCodec.Null))
        {
            writer.WriteHeader(ManifestAvroSchemas.ManifestListSchema);
            var buf = new GrowableBuffer();
            var bw = new AvroBinaryWriter(buf);
            foreach (var entry in entries)
                EncodeManifestListEntry(bw, entry);
            writer.WriteBlock(bw.WrittenSpan, entries.Count);
        }
        var data = ms.ToArray();
        await fs.WriteAllBytesAsync(path, data, ct).ConfigureAwait(false);
        return data.Length;
    }

    private static async ValueTask<List<ManifestListEntry>> ReadManifestListAvroAsync(
        ITableFileSystem fs, string path, CancellationToken ct = default)
    {
        var data = await fs.ReadAllBytesAsync(path, ct).ConfigureAwait(false);
        using var ms = new MemoryStream(data);
        using var reader = OcfReader.Open(ms);
        var results = new List<ManifestListEntry>();
        while (reader.ReadBlock() is { } block)
        {
            var br = new AvroBinaryReader(block.data.Span);
            for (long i = 0; i < block.objectCount; i++)
                results.Add(DecodeManifestListEntry(ref br));
        }
        return results;
    }

    // --- Avro encoding: Manifest entries ---

    private static void EncodeManifestEntry(AvroBinaryWriter w, ManifestEntry entry)
    {
        w.WriteInt((int)entry.Status);
        WriteNullableLong(w, entry.SnapshotId);
        WriteNullableLong(w, entry.SequenceNumber);
        WriteNullableLong(w, entry.FileSequenceNumber);
        EncodeDataFile(w, entry.DataFile);
    }

    private static void EncodeDataFile(AvroBinaryWriter w, DataFile df)
    {
        w.WriteInt((int)df.Content);
        w.WriteString(df.FilePath);
        w.WriteString(df.FileFormat.ToString().ToUpperInvariant());
        // partition: empty record (0 fields = 0 bytes)
        w.WriteLong(df.RecordCount);
        w.WriteLong(df.FileSizeInBytes);
        WriteNullableIntLongMap(w, df.ColumnSizes);
        WriteNullableIntLongMap(w, df.ValueCounts);
        WriteNullableIntLongMap(w, df.NullValueCounts);
        WriteNullableLongArray(w, df.SplitOffsets);
        WriteNullableInt(w, df.SortOrderId);
    }

    private static ManifestEntry DecodeManifestEntry(ref AvroBinaryReader r)
    {
        var status = (ManifestEntryStatus)r.ReadInt();
        var snapshotId = ReadNullableLong(ref r);
        var sequenceNumber = ReadNullableLong(ref r);
        var fileSequenceNumber = ReadNullableLong(ref r);
        var dataFile = DecodeDataFile(ref r);

        return new ManifestEntry
        {
            Status = status,
            SnapshotId = snapshotId,
            SequenceNumber = sequenceNumber,
            FileSequenceNumber = fileSequenceNumber,
            DataFile = dataFile,
        };
    }

    private static DataFile DecodeDataFile(ref AvroBinaryReader r)
    {
        var content = (FileContent)r.ReadInt();
        var filePath = r.ReadString();
        var fileFormatStr = r.ReadString();
        // partition: empty record (skip 0 bytes)
        var recordCount = r.ReadLong();
        var fileSizeInBytes = r.ReadLong();
        var columnSizes = ReadNullableIntLongMap(ref r);
        var valueCounts = ReadNullableIntLongMap(ref r);
        var nullValueCounts = ReadNullableIntLongMap(ref r);
        var splitOffsets = ReadNullableLongArray(ref r);
        var sortOrderId = ReadNullableInt(ref r);

        var fileFormat = fileFormatStr.ToUpperInvariant() switch
        {
            "PARQUET" => FileFormat.Parquet,
            "AVRO" => FileFormat.Avro,
            "ORC" => FileFormat.Orc,
            _ => FileFormat.Parquet,
        };

        return new DataFile
        {
            Content = content,
            FilePath = filePath,
            FileFormat = fileFormat,
            RecordCount = recordCount,
            FileSizeInBytes = fileSizeInBytes,
            ColumnSizes = columnSizes,
            ValueCounts = valueCounts,
            NullValueCounts = nullValueCounts,
            SplitOffsets = splitOffsets,
            SortOrderId = sortOrderId,
        };
    }

    // --- Avro encoding: Manifest list entries ---

    private static void EncodeManifestListEntry(AvroBinaryWriter w, ManifestListEntry entry)
    {
        w.WriteString(entry.ManifestPath);
        w.WriteLong(entry.ManifestLength);
        w.WriteInt(entry.PartitionSpecId);
        w.WriteInt((int)entry.Content);
        w.WriteLong(entry.SequenceNumber);
        w.WriteLong(entry.MinSequenceNumber);
        w.WriteLong(entry.AddedSnapshotId);
        w.WriteInt(entry.AddedDataFilesCount);
        w.WriteInt(entry.ExistingDataFilesCount);
        w.WriteInt(entry.DeletedDataFilesCount);
        w.WriteLong(entry.AddedRowsCount);
        w.WriteLong(entry.ExistingRowsCount);
        w.WriteLong(entry.DeletedRowsCount);
        // partitions: null
        w.WriteUnionIndex(0);
        // key_metadata: null
        w.WriteUnionIndex(0);
    }

    private static ManifestListEntry DecodeManifestListEntry(ref AvroBinaryReader r)
    {
        var manifestPath = r.ReadString();
        var manifestLength = r.ReadLong();
        var partitionSpecId = r.ReadInt();
        var content = (ManifestContent)r.ReadInt();
        var sequenceNumber = r.ReadLong();
        var minSequenceNumber = r.ReadLong();
        var addedSnapshotId = r.ReadLong();
        var addedDataFilesCount = r.ReadInt();
        var existingDataFilesCount = r.ReadInt();
        var deletedDataFilesCount = r.ReadInt();
        var addedRowsCount = r.ReadLong();
        var existingRowsCount = r.ReadLong();
        var deletedRowsCount = r.ReadLong();

        // partitions: skip nullable array of records
        var partitionsIndex = r.ReadUnionIndex();
        if (partitionsIndex != 0)
        {
            var count = r.ReadLong();
            while (count != 0)
            {
                for (long i = 0; i < Math.Abs(count); i++)
                {
                    r.ReadBoolean(); // contains_null
                    ReadNullableInt(ref r); // contains_nan (reuse nullable read, it's union)
                    var lbIdx = r.ReadUnionIndex(); // lower_bound
                    if (lbIdx != 0) r.ReadBytes();
                    var ubIdx = r.ReadUnionIndex(); // upper_bound
                    if (ubIdx != 0) r.ReadBytes();
                }
                count = r.ReadLong();
            }
        }

        // key_metadata: skip nullable bytes
        var kmIndex = r.ReadUnionIndex();
        if (kmIndex != 0) r.ReadBytes();

        return new ManifestListEntry
        {
            ManifestPath = manifestPath,
            ManifestLength = manifestLength,
            PartitionSpecId = partitionSpecId,
            Content = content,
            SequenceNumber = sequenceNumber,
            MinSequenceNumber = minSequenceNumber,
            AddedSnapshotId = addedSnapshotId,
            AddedDataFilesCount = addedDataFilesCount,
            ExistingDataFilesCount = existingDataFilesCount,
            DeletedDataFilesCount = deletedDataFilesCount,
            AddedRowsCount = addedRowsCount,
            ExistingRowsCount = existingRowsCount,
            DeletedRowsCount = deletedRowsCount,
        };
    }

    // --- Nullable union helpers ---

    private static void WriteNullableLong(AvroBinaryWriter w, long? value)
    {
        if (value is null) { w.WriteUnionIndex(0); return; }
        w.WriteUnionIndex(1);
        w.WriteLong(value.Value);
    }

    private static void WriteNullableInt(AvroBinaryWriter w, int? value)
    {
        if (value is null) { w.WriteUnionIndex(0); return; }
        w.WriteUnionIndex(1);
        w.WriteInt(value.Value);
    }

    private static void WriteNullableIntLongMap(AvroBinaryWriter w, IReadOnlyDictionary<int, long>? map)
    {
        if (map is null) { w.WriteUnionIndex(0); return; }
        w.WriteUnionIndex(1);
        if (map.Count > 0)
        {
            w.WriteLong(map.Count);
            foreach (var (key, value) in map)
            {
                w.WriteInt(key);
                w.WriteLong(value);
            }
        }
        w.WriteLong(0);
    }

    private static void WriteNullableLongArray(AvroBinaryWriter w, IReadOnlyList<long>? values)
    {
        if (values is null) { w.WriteUnionIndex(0); return; }
        w.WriteUnionIndex(1);
        if (values.Count > 0)
        {
            w.WriteLong(values.Count);
            foreach (var v in values)
                w.WriteLong(v);
        }
        w.WriteLong(0);
    }

    private static long? ReadNullableLong(ref AvroBinaryReader r)
    {
        return r.ReadUnionIndex() == 0 ? null : r.ReadLong();
    }

    private static int? ReadNullableInt(ref AvroBinaryReader r)
    {
        return r.ReadUnionIndex() == 0 ? null : r.ReadInt();
    }

    private static IReadOnlyDictionary<int, long>? ReadNullableIntLongMap(ref AvroBinaryReader r)
    {
        if (r.ReadUnionIndex() == 0) return null;
        var map = new Dictionary<int, long>();
        var count = r.ReadLong();
        while (count != 0)
        {
            for (long i = 0; i < Math.Abs(count); i++)
                map[r.ReadInt()] = r.ReadLong();
            count = r.ReadLong();
        }
        return map;
    }

    private static IReadOnlyList<long>? ReadNullableLongArray(ref AvroBinaryReader r)
    {
        if (r.ReadUnionIndex() == 0) return null;
        var list = new List<long>();
        var count = r.ReadLong();
        while (count != 0)
        {
            for (long i = 0; i < Math.Abs(count); i++)
                list.Add(r.ReadLong());
            count = r.ReadLong();
        }
        return list;
    }
}
