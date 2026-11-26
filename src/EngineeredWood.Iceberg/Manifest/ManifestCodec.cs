// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using EngineeredWood.Avro;
using EngineeredWood.Avro.Container;
using EngineeredWood.Avro.Encoding;
using EngineeredWood.Avro.Schema;
using EngineeredWood.Buffers;
using S = EngineeredWood.Iceberg.Manifest.ManifestAvroSchemas;

namespace EngineeredWood.Iceberg.Manifest;

/// <summary>A manifest as decoded, with what is needed to apply inheritance and to decide whether it can be rewritten.</summary>
internal sealed record DecodedManifest(
    List<ManifestEntry> Entries,
    IReadOnlyDictionary<string, byte[]> Metadata,
    bool HasSequenceNumbers,
    IReadOnlyList<string> UnmodelledFields);

/// <summary>A manifest list as decoded.</summary>
internal sealed record DecodedManifestList(
    List<ManifestListEntry> Entries,
    IReadOnlyDictionary<string, byte[]> Metadata,
    IReadOnlyList<string> UnmodelledFields);

/// <summary>
/// The Avro codec for manifests and manifest lists. Decoding resolves every field by its <c>field-id</c> (or,
/// for a writer that wrote none, its spec name) in the file's own writer schema, so it reads any writer's
/// layout and every format version. Encoding writes the spec's schema for the table's format version.
/// </summary>
internal static class ManifestCodec
{
    private static readonly HashSet<int> EntryIds = [S.Status, S.SnapshotId, S.DataFileId, S.SequenceNumber, S.FileSequenceNumber];

    private static readonly HashSet<int> DataFileIds =
    [
        S.Content, S.FilePath, S.FileFormat, S.Partition, S.RecordCount, S.FileSizeInBytes, S.BlockSizeInBytes,
        S.ColumnSizes, S.ValueCounts, S.NullValueCounts, S.NanValueCounts, S.DistinctCounts, S.LowerBounds,
        S.UpperBounds, S.KeyMetadata, S.SplitOffsets, S.EqualityIds, S.SortOrderId, S.FirstRowId,
        S.ReferencedDataFile, S.ContentOffset, S.ContentSizeInBytes,
    ];

    private static readonly HashSet<int> ManifestFileIds =
    [
        S.ManifestPath, S.ManifestLength, S.PartitionSpecId, S.ManifestContent, S.ListSequenceNumber,
        S.MinSequenceNumber, S.AddedSnapshotId, S.AddedFilesCount, S.ExistingFilesCount, S.DeletedFilesCount,
        S.AddedRowsCount, S.ExistingRowsCount, S.DeletedRowsCount, S.Partitions, S.ManifestKeyMetadata,
        S.ManifestFirstRowId,
    ];

    private static readonly HashSet<int> FieldSummaryIds = [S.ContainsNull, S.ContainsNan, S.LowerBound, S.UpperBound];

    // ---------------------------------------------------------------- manifest: decode

    /// <summary>
    /// Decodes a manifest. <paramref name="partitionType"/> types the partition tuple when the caller knows
    /// the file's spec; otherwise the tuple is typed from the file's Avro schema.
    /// </summary>
    public static DecodedManifest DecodeManifest(byte[] data, string path, IReadOnlyList<PartitionValueType>? partitionType)
    {
        using var ms = new MemoryStream(data);
        using var reader = OcfReader.Open(ms);
        var schema = reader.WriterSchema;
        var lost = new SortedSet<string>(StringComparer.Ordinal);

        var entryFields = FieldIds(schema, S.EntryFieldsByName);
        int dataFileIndex = IndexOf(entryFields, S.DataFileId)
            ?? throw Invalid(path, "has no data_file field");
        var dataFileSchema = Unwrap(schema.Fields[dataFileIndex].Schema) as AvroRecordSchema
            ?? throw Invalid(path, "has a data_file that is not a record");
        var dataFileFields = FieldIds(dataFileSchema, S.DataFileFieldsByName);
        bool hasSequenceNumbers = IndexOf(entryFields, S.SequenceNumber) is not null;

        PartitionReader? partition = null;
        if (IndexOf(dataFileFields, S.Partition) is { } pIndex)
        {
            var partitionSchema = Unwrap(dataFileSchema.Fields[pIndex].Schema) as AvroRecordSchema
                ?? throw Invalid(path, "has a partition that is not a record");
            partition = new PartitionReader(partitionSchema, partitionType, path);
        }

        var entries = new List<ManifestEntry>();
        while (reader.ReadBlock() is { } block)
        {
            var br = new AvroBinaryReader(block.data.Span);
            for (long i = 0; i < block.objectCount; i++)
            {
                var entry = (AvroRecordValue)AvroDatum.Read(ref br, schema)!;
                NoteUnmodelled(entry, entryFields, EntryIds, "manifest_entry", lost);
                var df = (AvroRecordValue)entry.Values[dataFileIndex]!;
                NoteUnmodelled(df, dataFileFields, DataFileIds, "data_file", lost);
                entries.Add(DecodeEntry(entry, entryFields, df, dataFileFields, partition, path));
            }
        }

        return new DecodedManifest(entries, reader.Metadata, hasSequenceNumbers, lost.ToList());
    }

    private static ManifestEntry DecodeEntry(
        AvroRecordValue entry, int?[] entryFields, AvroRecordValue df, int?[] dfFields,
        PartitionReader? partition, string path)
    {
        object? E(int id) => Get(entry, entryFields, id);
        object? D(int id) => Get(df, dfFields, id);

        var formatName = D(S.FileFormat) as string ?? throw Invalid(path, "has a data file with no file_format");
        return new ManifestEntry
        {
            Status = (ManifestEntryStatus)(E(S.Status) as int? ?? throw Invalid(path, "has an entry with no status")),
            SnapshotId = AsLong(E(S.SnapshotId)),
            SequenceNumber = AsLong(E(S.SequenceNumber)),
            FileSequenceNumber = AsLong(E(S.FileSequenceNumber)),
            DataFile = new DataFile
            {
                Content = (FileContent)(D(S.Content) as int? ?? 0),
                FilePath = D(S.FilePath) as string ?? throw Invalid(path, "has a data file with no file_path"),
                FileFormat = ParseFormat(formatName, path),
                Partition = partition?.Read(D(S.Partition) as AvroRecordValue),
                RecordCount = AsLong(D(S.RecordCount)) ?? throw Invalid(path, "has a data file with no record_count"),
                FileSizeInBytes = AsLong(D(S.FileSizeInBytes)) ?? throw Invalid(path, "has a data file with no file_size_in_bytes"),
                BlockSizeInBytes = AsLong(D(S.BlockSizeInBytes)),
                ColumnSizes = IntLongMap(D(S.ColumnSizes)),
                ValueCounts = IntLongMap(D(S.ValueCounts)),
                NullValueCounts = IntLongMap(D(S.NullValueCounts)),
                NanValueCounts = IntLongMap(D(S.NanValueCounts)),
                DistinctCounts = IntLongMap(D(S.DistinctCounts)),
                LowerBounds = IntBytesMap(D(S.LowerBounds)),
                UpperBounds = IntBytesMap(D(S.UpperBounds)),
                KeyMetadata = D(S.KeyMetadata) as byte[],
                SplitOffsets = LongList(D(S.SplitOffsets)),
                EqualityIds = IntList(D(S.EqualityIds)),
                SortOrderId = D(S.SortOrderId) as int?,
                FirstRowId = AsLong(D(S.FirstRowId)),
                ReferencedDataFile = D(S.ReferencedDataFile) as string,
                ContentOffset = AsLong(D(S.ContentOffset)),
                ContentSizeInBytes = AsLong(D(S.ContentSizeInBytes)),
            },
        };
    }

    /// <summary>Reads a manifest's partition record into a typed <see cref="PartitionData"/>.</summary>
    private sealed class PartitionReader
    {
        private readonly PartitionValueType[] _fields;

        public PartitionReader(AvroRecordSchema schema, IReadOnlyList<PartitionValueType>? spec, string path)
        {
            _fields = new PartitionValueType[schema.Fields.Count];
            for (int i = 0; i < schema.Fields.Count; i++)
            {
                var f = schema.Fields[i];
                // v2 requires the partition record's field ids to be the spec's; a v1 writer may have left
                // them out, and then the fields are the spec's, in order.
                int? id = f.FieldId ?? (spec is not null && i < spec.Count ? spec[i].FieldId : null);
                if (id is null)
                    throw Invalid(path, $"has partition field '{f.Name}' with no field-id and no partition spec to place it");

                IcebergType? type = null;
                if (spec is not null)
                {
                    foreach (var s in spec)
                    {
                        if (s.FieldId == id)
                            type = s.Type;
                    }
                    if (type is null)
                        throw Invalid(path, $"has partition field {id}, which its partition spec does not have");
                }

                _fields[i] = new PartitionValueType(id.Value, f.Name, type ?? IcebergAvroTypes.InferType(f.Schema));
            }
        }

        public PartitionData? Read(AvroRecordValue? record)
        {
            if (record is null || _fields.Length == 0)
                return null;
            var values = new PartitionValue[_fields.Length];
            for (int i = 0; i < _fields.Length; i++)
            {
                var f = _fields[i];
                values[i] = new PartitionValue(f.FieldId, f.Name, f.Type, IcebergAvroTypes.ToPhysical(record.Values[i], f.Type));
            }
            return new PartitionData(values);
        }
    }

    // ---------------------------------------------------------------- manifest: encode

    /// <summary>
    /// Encodes a manifest in the spec's schema for <paramref name="formatVersion"/>, with
    /// <paramref name="metadata"/> in the OCF header. Throws when an entry holds something that format
    /// version cannot carry, rather than dropping it.
    /// </summary>
    public static byte[] EncodeManifest(
        IReadOnlyList<ManifestEntry> entries, int formatVersion, IReadOnlyList<PartitionValueType> partitionType,
        IReadOnlyDictionary<string, byte[]> metadata)
    {
        bool withDistinct = formatVersion <= 2 && entries.Any(e => e.DataFile.DistinctCounts is not null);
        var schema = S.ManifestEntry(formatVersion, partitionType, withDistinct);
        var dataFileSchema = (AvroRecordSchema)schema.Fields.Single(f => f.FieldId == S.DataFileId).Schema;
        var partitionSchema = (AvroRecordSchema)dataFileSchema.Fields.Single(f => f.FieldId == S.Partition).Schema;

        var buffer = new GrowableBuffer();
        var w = new AvroBinaryWriter(buffer);
        foreach (var entry in entries)
        {
            Validate(entry, formatVersion);
            var df = entry.DataFile;
            var partition = EncodePartition(partitionSchema, partitionType, df);
            var dfRecord = Build(dataFileSchema, id => id switch
            {
                S.Content => (int)df.Content,
                S.FilePath => df.FilePath,
                S.FileFormat => FormatName(df.FileFormat),
                S.Partition => partition,
                S.RecordCount => df.RecordCount,
                S.FileSizeInBytes => df.FileSizeInBytes,
                // v1 requires it; Java writes its default block size when it has none.
                S.BlockSizeInBytes => df.BlockSizeInBytes ?? 64L * 1024 * 1024,
                S.ColumnSizes => IntMapValue(df.ColumnSizes),
                S.ValueCounts => IntMapValue(df.ValueCounts),
                S.NullValueCounts => IntMapValue(df.NullValueCounts),
                S.NanValueCounts => IntMapValue(df.NanValueCounts),
                S.DistinctCounts => IntMapValue(df.DistinctCounts),
                S.LowerBounds => IntMapValue(df.LowerBounds),
                S.UpperBounds => IntMapValue(df.UpperBounds),
                S.KeyMetadata => df.KeyMetadata,
                S.SplitOffsets => df.SplitOffsets?.Select(o => (object?)o).ToList(),
                S.EqualityIds => df.EqualityIds?.Select(o => (object?)o).ToList(),
                S.SortOrderId => df.SortOrderId,
                S.FirstRowId => df.FirstRowId,
                S.ReferencedDataFile => df.ReferencedDataFile,
                S.ContentOffset => df.ContentOffset,
                S.ContentSizeInBytes => df.ContentSizeInBytes,
                _ => throw new InvalidOperationException($"No value for data_file field {id}."),
            });
            var record = Build(schema, id => id switch
            {
                S.Status => (int)entry.Status,
                S.SnapshotId => entry.SnapshotId,
                S.SequenceNumber => entry.SequenceNumber,
                S.FileSequenceNumber => entry.FileSequenceNumber,
                S.DataFileId => dfRecord,
                _ => throw new InvalidOperationException($"No value for manifest_entry field {id}."),
            });
            AvroDatum.Write(w, schema, record);
        }

        return WriteOcf(schema, metadata, w, entries.Count);
    }

    private static void Validate(ManifestEntry entry, int formatVersion)
    {
        var df = entry.DataFile;
        string where = $"Entry for '{df.FilePath}'";
        if (formatVersion == 1)
        {
            if (df.Content != FileContent.Data)
                throw new ArgumentException($"{where} is a delete file, which format version 1 cannot hold.");
            if (entry.SnapshotId is null)
                throw new ArgumentException($"{where} has no snapshot id, which format version 1 requires.");
            if (df.EqualityIds is not null || df.ReferencedDataFile is not null)
                throw new ArgumentException($"{where} has delete-file fields, which format version 1 cannot hold.");
        }
        if (formatVersion <= 2 && (df.FirstRowId is not null || df.ContentOffset is not null || df.ContentSizeInBytes is not null))
            throw new ArgumentException(
                $"{where} has first_row_id or content_offset/content_size_in_bytes, which need format version 3.");
    }

    private static AvroRecordValue EncodePartition(
        AvroRecordSchema partitionSchema, IReadOnlyList<PartitionValueType> partitionType, DataFile df)
    {
        var values = new object?[partitionType.Count];
        var partition = df.Partition;
        if (partitionType.Count == 0)
        {
            if (partition is { Values.Count: > 0 })
                throw new ArgumentException(
                    $"Data file '{df.FilePath}' has partition values, but the manifest's partition spec is unpartitioned.");
            return new AvroRecordValue(partitionSchema, values);
        }

        if (partition is null)
            throw new ArgumentException(
                $"Data file '{df.FilePath}' has no partition values, but the manifest's partition spec has " +
                $"{partitionType.Count} field(s).");
        if (partition.Values.Count != partitionType.Count)
            throw new ArgumentException(
                $"Data file '{df.FilePath}' has {partition.Values.Count} partition value(s); the spec has {partitionType.Count}.");

        for (int i = 0; i < partitionType.Count; i++)
        {
            var t = partitionType[i];
            if (!partition.TryGetValue(t.FieldId, out var value))
                throw new ArgumentException($"Data file '{df.FilePath}' has no value for partition field {t.FieldId} ({t.Name}).");
            values[i] = IcebergAvroTypes.ToAvro(value, t.Type);
        }
        return new AvroRecordValue(partitionSchema, values);
    }

    // ---------------------------------------------------------------- manifest list

    public static DecodedManifestList DecodeManifestList(byte[] data, string path)
    {
        using var ms = new MemoryStream(data);
        using var reader = OcfReader.Open(ms);
        var schema = reader.WriterSchema;
        var fields = FieldIds(schema, S.ManifestFileFieldsByName);
        var lost = new SortedSet<string>(StringComparer.Ordinal);

        int?[]? summaryFields = null;
        if (IndexOf(fields, S.Partitions) is { } pIndex
            && Unwrap(schema.Fields[pIndex].Schema) is AvroArraySchema { Items: AvroRecordSchema summary })
        {
            summaryFields = FieldIds(summary, S.FieldSummaryFieldsByName);
        }

        var entries = new List<ManifestListEntry>();
        while (reader.ReadBlock() is { } block)
        {
            var br = new AvroBinaryReader(block.data.Span);
            for (long i = 0; i < block.objectCount; i++)
            {
                var record = (AvroRecordValue)AvroDatum.Read(ref br, schema)!;
                NoteUnmodelled(record, fields, ManifestFileIds, "manifest_file", lost);
                object? F(int id) => Get(record, fields, id);

                List<PartitionFieldSummary>? partitions = null;
                if (F(S.Partitions) is List<object?> items)
                {
                    partitions = new List<PartitionFieldSummary>(items.Count);
                    foreach (AvroRecordValue item in items.Cast<AvroRecordValue>())
                    {
                        NoteUnmodelled(item, summaryFields!, FieldSummaryIds, "field_summary", lost);
                        partitions.Add(new PartitionFieldSummary(
                            Get(item, summaryFields!, S.ContainsNull) as bool? ?? throw Invalid(path, "has a partition summary with no contains_null"),
                            Get(item, summaryFields!, S.ContainsNan) as bool?,
                            Get(item, summaryFields!, S.LowerBound) as byte[],
                            Get(item, summaryFields!, S.UpperBound) as byte[]));
                    }
                }

                entries.Add(new ManifestListEntry
                {
                    ManifestPath = F(S.ManifestPath) as string ?? throw Invalid(path, "has an entry with no manifest_path"),
                    ManifestLength = AsLong(F(S.ManifestLength)) ?? throw Invalid(path, "has an entry with no manifest_length"),
                    PartitionSpecId = F(S.PartitionSpecId) as int? ?? throw Invalid(path, "has an entry with no partition_spec_id"),
                    // v1 has no content or sequence numbers: data manifests, sequence number 0.
                    Content = (ManifestContent)(F(S.ManifestContent) as int? ?? 0),
                    SequenceNumber = AsLong(F(S.ListSequenceNumber)) ?? 0,
                    MinSequenceNumber = AsLong(F(S.MinSequenceNumber)) ?? 0,
                    AddedSnapshotId = AsLong(F(S.AddedSnapshotId)) ?? throw Invalid(path, "has an entry with no added_snapshot_id"),
                    AddedDataFilesCount = F(S.AddedFilesCount) as int?,
                    ExistingDataFilesCount = F(S.ExistingFilesCount) as int?,
                    DeletedDataFilesCount = F(S.DeletedFilesCount) as int?,
                    AddedRowsCount = AsLong(F(S.AddedRowsCount)),
                    ExistingRowsCount = AsLong(F(S.ExistingRowsCount)),
                    DeletedRowsCount = AsLong(F(S.DeletedRowsCount)),
                    Partitions = partitions,
                    KeyMetadata = F(S.ManifestKeyMetadata) as byte[],
                    FirstRowId = AsLong(F(S.ManifestFirstRowId)),
                });
            }
        }

        return new DecodedManifestList(entries, reader.Metadata, lost.ToList());
    }

    public static byte[] EncodeManifestList(
        IReadOnlyList<ManifestListEntry> entries, int formatVersion, IReadOnlyDictionary<string, byte[]> metadata)
    {
        var schema = S.ManifestFile(formatVersion);
        var summarySchema = (AvroRecordSchema)((AvroArraySchema)Unwrap(
            schema.Fields.Single(f => f.FieldId == S.Partitions).Schema)).Items;

        var buffer = new GrowableBuffer();
        var w = new AvroBinaryWriter(buffer);
        foreach (var e in entries)
        {
            if (formatVersion <= 2 && e.FirstRowId is not null)
                throw new ArgumentException($"Manifest '{e.ManifestPath}' has a first_row_id, which needs format version 3.");
            if (formatVersion == 1 && e.Content != ManifestContent.Data)
                throw new ArgumentException($"Manifest '{e.ManifestPath}' is a delete manifest, which format version 1 cannot hold.");

            object? Required(object? value, string name) => formatVersion == 1 || value is not null
                ? value
                : throw new NotSupportedException(
                    $"Manifest '{e.ManifestPath}' has no {name} (a v1 manifest list may leave it out), and format " +
                    $"version {formatVersion} requires it.");

            var record = Build(schema, id => id switch
            {
                S.ManifestPath => e.ManifestPath,
                S.ManifestLength => e.ManifestLength,
                S.PartitionSpecId => e.PartitionSpecId,
                S.ManifestContent => (int)e.Content,
                S.ListSequenceNumber => e.SequenceNumber,
                S.MinSequenceNumber => e.MinSequenceNumber,
                S.AddedSnapshotId => e.AddedSnapshotId,
                S.AddedFilesCount => Required(e.AddedDataFilesCount, "added_files_count"),
                S.ExistingFilesCount => Required(e.ExistingDataFilesCount, "existing_files_count"),
                S.DeletedFilesCount => Required(e.DeletedDataFilesCount, "deleted_files_count"),
                S.AddedRowsCount => Required(e.AddedRowsCount, "added_rows_count"),
                S.ExistingRowsCount => Required(e.ExistingRowsCount, "existing_rows_count"),
                S.DeletedRowsCount => Required(e.DeletedRowsCount, "deleted_rows_count"),
                S.Partitions => e.Partitions?.Select(p => (object?)Build(summarySchema, sid => sid switch
                {
                    S.ContainsNull => p.ContainsNull,
                    S.ContainsNan => p.ContainsNan,
                    S.LowerBound => p.LowerBound,
                    S.UpperBound => p.UpperBound,
                    _ => throw new InvalidOperationException($"No value for field_summary field {sid}."),
                })).ToList(),
                S.ManifestKeyMetadata => e.KeyMetadata,
                S.ManifestFirstRowId => e.FirstRowId,
                _ => throw new InvalidOperationException($"No value for manifest_file field {id}."),
            });
            AvroDatum.Write(w, schema, record);
        }

        return WriteOcf(schema, metadata, w, entries.Count);
    }

    // ---------------------------------------------------------------- inheritance

    /// <summary>
    /// Fills in what a manifest's entries inherit from their manifest-list entry (spec "Manifest Entry
    /// Inheritance"): a null snapshot id is the manifest's added snapshot; a null sequence number of an ADDED
    /// entry is the manifest's; a v1 manifest has sequence number 0 throughout; and a data file's null
    /// <c>first_row_id</c> is the manifest's <c>first_row_id</c> plus the record counts of the data files before
    /// it that also had none.
    /// </summary>
    public static List<ManifestEntry> Inherit(DecodedManifest manifest, ManifestListEntry from)
    {
        long? nextRowId = from.Content == ManifestContent.Data ? from.FirstRowId : null;
        var result = new List<ManifestEntry>(manifest.Entries.Count);
        foreach (var entry in manifest.Entries)
        {
            long? seq = entry.SequenceNumber, fileSeq = entry.FileSequenceNumber;
            if (!manifest.HasSequenceNumbers)
            {
                seq ??= 0;
                fileSeq ??= 0;
            }
            else if (entry.Status == ManifestEntryStatus.Added)
            {
                seq ??= from.SequenceNumber;
                fileSeq ??= from.SequenceNumber;
            }

            var df = entry.DataFile;
            if (df.Content == FileContent.Data && df.FirstRowId is null && nextRowId is { } rowId)
            {
                df = df with { FirstRowId = rowId };
                nextRowId = rowId + entry.DataFile.RecordCount;
            }

            result.Add(entry with
            {
                SnapshotId = entry.SnapshotId ?? from.AddedSnapshotId,
                SequenceNumber = seq,
                FileSequenceNumber = fileSeq,
                DataFile = df,
            });
        }
        return result;
    }

    // ---------------------------------------------------------------- helpers

    /// <summary>Each writer field's id: its <c>field-id</c>, or the id its spec name stands for.</summary>
    private static int?[] FieldIds(AvroRecordSchema schema, IReadOnlyDictionary<string, int> byName)
    {
        var ids = new int?[schema.Fields.Count];
        for (int i = 0; i < ids.Length; i++)
        {
            var f = schema.Fields[i];
            ids[i] = f.FieldId ?? (byName.TryGetValue(f.Name, out var id) ? id : null);
        }
        return ids;
    }

    private static int? IndexOf(int?[] ids, int id)
    {
        for (int i = 0; i < ids.Length; i++)
        {
            if (ids[i] == id)
                return i;
        }
        return null;
    }

    private static object? Get(AvroRecordValue record, int?[] ids, int id) =>
        IndexOf(ids, id) is { } i ? record.Values[i] : null;

    /// <summary>Records a field this codec does not model and that holds a value: a rewrite would drop it.</summary>
    private static void NoteUnmodelled(AvroRecordValue record, int?[] ids, HashSet<int> modelled, string where, SortedSet<string> lost)
    {
        for (int i = 0; i < ids.Length; i++)
        {
            if ((ids[i] is not { } id || !modelled.Contains(id)) && record.Values[i] is not null)
                lost.Add($"{where}.{record.Schema.Fields[i].Name}" + (ids[i] is { } fid ? $" (field-id {fid})" : ""));
        }
    }

    private static AvroSchemaNode Unwrap(AvroSchemaNode node) =>
        node is AvroUnionSchema u && u.IsNullable(out var inner, out _) ? inner : node;

    private static AvroRecordValue Build(AvroRecordSchema schema, Func<int, object?> valueFor)
    {
        var values = new object?[schema.Fields.Count];
        for (int i = 0; i < values.Length; i++)
            values[i] = valueFor(schema.Fields[i].FieldId!.Value);
        return new AvroRecordValue(schema, values);
    }

    private static List<object?>? IntMapValue<T>(IReadOnlyDictionary<int, T>? map)
    {
        if (map is null)
            return null;
        var kv = new List<object?>(map.Count);
        // The key/value record schema is supplied when the value is written; any record with two values works.
        foreach (var pair in map.OrderBy(p => p.Key))
            kv.Add(new AvroRecordValue(KeyValueShape, [pair.Key, pair.Value]));
        return kv;
    }

    // AvroDatum.Write reads a record value's Values by position and ignores its Schema, so map entries share one.
    private static readonly AvroRecordSchema KeyValueShape = new("kv", null, []);

    private static Dictionary<int, long>? IntLongMap(object? value) => KeyValues(value, v => AsLong(v) ?? 0);

    private static Dictionary<int, byte[]>? IntBytesMap(object? value) => KeyValues(value, v => (byte[])v!);

    // Iceberg writes int-keyed maps as arrays of key/value records; accept an Avro map with numeric keys too.
    private static Dictionary<int, T>? KeyValues<T>(object? value, Func<object?, T> convert)
    {
        switch (value)
        {
            case null:
                return null;
            case List<object?> records:
            {
                var map = new Dictionary<int, T>(records.Count);
                foreach (AvroRecordValue r in records.Cast<AvroRecordValue>())
                {
                    int ki = FieldIndex(r.Schema, "key"), vi = FieldIndex(r.Schema, "value");
                    map[(int)r.Values[ki]!] = convert(r.Values[vi]);
                }
                return map;
            }
            case List<KeyValuePair<string, object?>> pairs:
            {
                var map = new Dictionary<int, T>(pairs.Count);
                foreach (var p in pairs)
                    map[int.Parse(p.Key, NumberStyles.Integer, CultureInfo.InvariantCulture)] = convert(p.Value);
                return map;
            }
            default:
                throw new InvalidDataException($"Expected a map, found {value.GetType().Name}.");
        }
    }

    private static int FieldIndex(AvroRecordSchema schema, string name)
    {
        for (int i = 0; i < schema.Fields.Count; i++)
        {
            if (schema.Fields[i].Name == name)
                return i;
        }
        throw new InvalidDataException($"Map entry record '{schema.FullName}' has no '{name}' field.");
    }

    private static List<long>? LongList(object? value) =>
        (value as List<object?>)?.Select(v => AsLong(v) ?? 0).ToList();

    private static List<int>? IntList(object? value) =>
        (value as List<object?>)?.Select(v => (int)v!).ToList();

    private static long? AsLong(object? value) => value switch
    {
        null => null,
        long l => l,
        int i => i,
        _ => throw new InvalidDataException($"Expected a long, found {value.GetType().Name}."),
    };

    private static FileFormat ParseFormat(string name, string path) => name.ToUpperInvariant() switch
    {
        "PARQUET" => FileFormat.Parquet,
        "AVRO" => FileFormat.Avro,
        "ORC" => FileFormat.Orc,
        "PUFFIN" => FileFormat.Puffin,
        _ => throw Invalid(path, $"names file format '{name}', which this library does not know"),
    };

    // Java writes the format in upper case; readers compare case-insensitively.
    private static string FormatName(FileFormat format) => format switch
    {
        FileFormat.Parquet => "PARQUET",
        FileFormat.Avro => "AVRO",
        FileFormat.Orc => "ORC",
        FileFormat.Puffin => "PUFFIN",
        _ => throw new ArgumentOutOfRangeException(nameof(format)),
    };

    private static byte[] WriteOcf(AvroRecordSchema schema, IReadOnlyDictionary<string, byte[]> metadata, AvroBinaryWriter w, int count)
    {
        var ms = new MemoryStream();
        using (var writer = new OcfWriter(ms, AvroCodec.Null))
        {
            writer.WriteHeader(schema, metadata);
            if (count > 0)
                writer.WriteBlock(w.WrittenSpan, count);
        }
        return ms.ToArray();
    }

    private static InvalidDataException Invalid(string path, string problem) =>
        new($"Iceberg manifest file '{path}' {problem}.");
}
