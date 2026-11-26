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

        // Iceberg Java (1.8 on) writes a bare fixed partition value with a length prefix, though the schema it
        // writes says fixed (apache/iceberg#17501), and reads it back the same way. Decode as the schema says;
        // only if that does not decode cleanly, and the partition has a bare fixed field, decode as Java writes.
        //
        // A wrong reading need not fail: a value's last byte can join the next varint and leave the rest
        // aligned. So when the partition has a bare fixed field, try both readings, take the one that decodes,
        // and refuse the manifest if both decode to different files: nothing in the file says which writer
        // made it.
        var javaFixed = BareFixedPartitionFields(schema);
        if (javaFixed.Count == 0)
        {
            var (plain, plainLost) = DecodeEntries(lengthPrefixed: null);
            return new DecodedManifest(plain, reader.Metadata, hasSequenceNumbers, plainLost);
        }

        (List<ManifestEntry> Entries, IReadOnlyList<string> Lost)? strict = null, java = null;
        Exception? strictError = null;
        try { strict = DecodeEntries(lengthPrefixed: null); }
        catch (Exception e) when (IsDecodeFailure(e)) { strictError = e; }
        try { java = DecodeEntries(javaFixed); }
        catch (Exception e) when (IsDecodeFailure(e)) { }

        var chosen = (strict, java) switch
        {
            (null, null) => throw strictError!,
            ({ } s, null) => s,
            (null, { } j) => j,
            ({ } s, { } j) when SameFiles(s.Entries, j.Entries) => s,
            _ => throw Invalid(path,
                "decodes both as its schema says and as Iceberg Java writes fixed partition values " +
                "(apache/iceberg#17501), to different files, and nothing in it says which is right"),
        };
        return new DecodedManifest(chosen.Entries, reader.Metadata, hasSequenceNumbers, chosen.Lost);

        (List<ManifestEntry> Entries, IReadOnlyList<string> Lost) DecodeEntries(HashSet<AvroFixedSchema>? lengthPrefixed)
        {
            lost.Clear();
            using var stream = new MemoryStream(data);
            using var blocks = OcfReader.Open(stream);
            // The same schema text parses to new nodes, so map the fixed nodes to this parse by position.
            var decodeSchema = blocks.WriterSchema;
            var prefixed = lengthPrefixed is null ? null : BareFixedPartitionFields(decodeSchema);
            var result = new List<ManifestEntry>();
            while (blocks.ReadBlock() is { } block)
            {
                var br = new AvroBinaryReader(block.data.Span);
                for (long i = 0; i < block.objectCount; i++)
                {
                    var entry = (AvroRecordValue)AvroDatum.Read(ref br, decodeSchema, prefixed)!;
                    NoteUnmodelled(entry, entryFields, EntryIds, "manifest_entry", lost);
                    var df = (AvroRecordValue)entry.Values[dataFileIndex]!;
                    NoteUnmodelled(df, dataFileFields, DataFileIds, "data_file", lost);
                    result.Add(DecodeEntry(entry, entryFields, df, dataFileFields, partition, path));
                }
                // A block that decodes into its record count but leaves bytes over was read misaligned.
                if (!br.IsEmpty)
                    throw Invalid(path, $"has a block with {br.Remaining} bytes left after its {block.objectCount} records");
            }
            return (result, lost.ToList());
        }
    }

    /// <summary>Whether two readings of a manifest describe the same files with the same partition values.</summary>
    private static bool SameFiles(List<ManifestEntry> a, List<ManifestEntry> b)
    {
        if (a.Count != b.Count)
            return false;
        for (int i = 0; i < a.Count; i++)
        {
            DataFile x = a[i].DataFile, y = b[i].DataFile;
            if (a[i].Status != b[i].Status || x.FilePath != y.FilePath || x.RecordCount != y.RecordCount
                || x.FileSizeInBytes != y.FileSizeInBytes || !Equals(x.Partition, y.Partition))
                return false;
        }
        return true;
    }

    /// <summary>
    /// The bare <c>fixed</c> nodes (no logical type) of a manifest's partition record: the fields Iceberg Java
    /// writes with a length prefix. UUIDs and decimals, also fixed, it writes as the schema says.
    /// </summary>
    private static HashSet<AvroFixedSchema> BareFixedPartitionFields(AvroRecordSchema manifestEntrySchema)
    {
        var result = new HashSet<AvroFixedSchema>();
        var entryFields = FieldIds(manifestEntrySchema, S.EntryFieldsByName);
        if (IndexOf(entryFields, S.DataFileId) is not { } dIndex
            || Unwrap(manifestEntrySchema.Fields[dIndex].Schema) is not AvroRecordSchema dataFile)
            return result;
        var dataFileFields = FieldIds(dataFile, S.DataFileFieldsByName);
        if (IndexOf(dataFileFields, S.Partition) is { } pIndex
            && Unwrap(dataFile.Fields[pIndex].Schema) is AvroRecordSchema partition)
        {
            foreach (var field in partition.Fields)
            {
                if (Unwrap(field.Schema) is AvroFixedSchema { LogicalType: null } fixedNode)
                    result.Add(fixedNode);
            }
        }
        return result;
    }

    private static bool IsDecodeFailure(Exception e) =>
        e is InvalidDataException or OverflowException or IndexOutOfRangeException or ArgumentOutOfRangeException
            or EndOfStreamException or InvalidCastException;

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

    /// <summary>
    /// Reads a manifest's partition record into a typed <see cref="PartitionData"/>. Each Avro field is placed
    /// by its <c>field-id</c>; a writer that wrote none (v1) is matched to the spec by name (the Iceberg name
    /// or its Avro-escaped form), and only when no name matches at all, by position. With a spec, the values
    /// come out in spec order, whatever order the writer used.
    /// </summary>
    private sealed class PartitionReader
    {
        private readonly PartitionValueType[] _fields;   // output order
        private readonly int[] _avroIndex;               // the Avro field each output value is read from

        public PartitionReader(AvroRecordSchema schema, IReadOnlyList<PartitionValueType>? spec, string path)
        {
            var avro = schema.Fields;
            if (spec is null)
            {
                _fields = new PartitionValueType[avro.Count];
                _avroIndex = new int[avro.Count];
                for (int i = 0; i < avro.Count; i++)
                {
                    var f = avro[i];
                    if (f.FieldId is not { } id)
                        throw Invalid(path, $"has partition field '{f.Name}' with no field-id and no partition spec to place it");
                    _fields[i] = new PartitionValueType(id, f.Name, IcebergAvroTypes.InferType(f.Schema));
                    _avroIndex[i] = i;
                }
                return;
            }

            // Which spec field each Avro field is.
            var specIndexOf = new int[avro.Count];
            bool anyNamed = false;
            for (int i = 0; i < avro.Count; i++)
            {
                var f = avro[i];
                int k = f.FieldId is { } id
                    ? IndexOf(spec, s => s.FieldId == id)
                    : IndexOf(spec, s => s.Name == f.Name || ManifestAvroSchemas.AvroCompatibleName(s.Name) == f.Name);
                if (f.FieldId is null && k >= 0)
                    anyNamed = true;
                specIndexOf[i] = k;
            }
            for (int i = 0; i < avro.Count; i++)
            {
                if (specIndexOf[i] >= 0)
                    continue;
                var f = avro[i];
                // An id-less record whose names match none of the spec's is taken to be in spec order, as
                // the oldest v1 writers wrote it; one that matches some names but not this one is ambiguous.
                if (f.FieldId is null && !anyNamed && avro.Count == spec.Count)
                    specIndexOf[i] = i;
                else
                    throw Invalid(path, f.FieldId is { } id
                        ? $"has partition field {id}, which its partition spec does not have"
                        : $"has partition field '{f.Name}' with no field-id, which matches no field of its partition spec");
            }

            _fields = spec.ToArray();
            _avroIndex = new int[spec.Count];
            for (int k = 0; k < spec.Count; k++)
            {
                int i = System.Array.IndexOf(specIndexOf, k);
                if (i < 0 || System.Array.LastIndexOf(specIndexOf, k) != i)
                    throw Invalid(path, $"does not hold partition field {spec[k].FieldId} ({spec[k].Name}) exactly once");
                _avroIndex[k] = i;
            }
        }

        private static int IndexOf(IReadOnlyList<PartitionValueType> spec, Func<PartitionValueType, bool> match)
        {
            for (int k = 0; k < spec.Count; k++)
            {
                if (match(spec[k]))
                    return k;
            }
            return -1;
        }

        public PartitionData? Read(AvroRecordValue? record)
        {
            if (record is null || _fields.Length == 0)
                return null;
            var values = new PartitionValue[_fields.Length];
            for (int k = 0; k < _fields.Length; k++)
            {
                var f = _fields[k];
                values[k] = new PartitionValue(f.FieldId, f.Name, f.Type,
                    IcebergAvroTypes.ToPhysical(record.Values[_avroIndex[k]], f.Type));
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
        IReadOnlyList<ManifestEntry> entries, int formatVersion, ManifestContent content,
        IReadOnlyList<PartitionValueType> partitionType, IReadOnlyDictionary<string, byte[]> metadata)
    {
        ValidateContent(entries, content);
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

    /// <summary>
    /// A manifest holds data files or delete files, never both, and its header says which: readers choose
    /// manifests by it.
    /// </summary>
    internal static void ValidateContent(IReadOnlyList<ManifestEntry> entries, ManifestContent content)
    {
        foreach (var entry in entries)
        {
            bool isData = entry.DataFile.Content == FileContent.Data;
            if (isData != (content == ManifestContent.Data))
                throw new ArgumentException(
                    $"Entry for '{entry.DataFile.FilePath}' is {entry.DataFile.Content}, which a " +
                    $"{(content == ManifestContent.Data ? "data" : "delete")} manifest cannot hold.");
        }
    }

    /// <remarks>
    /// Two values are left out on purpose, because the spec says to stop writing them once a table moves on:
    /// <c>block_size_in_bytes</c> (v1 only, "do not write in v2 or v3") and <c>distinct_counts</c> (not
    /// written in v3). Everything else a version cannot hold is refused.
    /// </remarks>
    private static void Validate(ManifestEntry entry, int formatVersion)
    {
        var df = entry.DataFile;
        string where = $"Entry for '{df.FilePath}'";
        if (formatVersion == 1)
        {
            // v1 has no sequence numbers: every file's is 0, which is what a v1 entry reads back as.
            if (entry.SequenceNumber is not (null or 0) || entry.FileSequenceNumber is not (null or 0))
                throw new ArgumentException(
                    $"{where} has sequence number {entry.SequenceNumber}/{entry.FileSequenceNumber}, which format " +
                    "version 1 cannot hold.");
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
            // The physical value means something only in its own type: unscaled 123 is 1.23 as decimal(9, 2) and
            // 0.123 as decimal(9, 3), and a long is micros or nanos by its type. Refuse a value typed otherwise.
            var declared = partition.Values.First(v => v.FieldId == t.FieldId).Type;
            if (!declared.Equals(t.Type))
                throw new ArgumentException(
                    $"Data file '{df.FilePath}' has partition field {t.FieldId} ({t.Name}) typed {declared}; its spec types it {t.Type}.");
            // Iceberg Java (1.8 on) reads a fixed partition value with a length prefix, though the spec and the
            // schema say fixed has none (apache/iceberg#17501). Either encoding would be misread by one group of
            // readers, so refuse to write one until that is settled. A null value is encoded the same either way.
            if (t.Type is FixedType && value is not null)
                throw new NotSupportedException(
                    $"Cannot write data file '{df.FilePath}': its partition field '{t.Name}' is a non-null fixed " +
                    "value, and readers disagree on how a manifest encodes fixed (apache/iceberg#17501).");
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
    /// entry is the manifest's; a v1 manifest has sequence number 0 throughout; and a live data file's null
    /// <c>first_row_id</c> is the manifest's <c>first_row_id</c> plus the record counts of the live data files
    /// before it that also had none. DELETED entries neither take a row id nor use one up, as Iceberg Java's
    /// <c>ManifestReader</c> assigns them.
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
            if (df.Content == FileContent.Data && entry.Status != ManifestEntryStatus.Deleted
                && df.FirstRowId is null && nextRowId is { } rowId)
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
