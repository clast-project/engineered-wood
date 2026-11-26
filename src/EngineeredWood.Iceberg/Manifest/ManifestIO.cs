// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using EngineeredWood.Iceberg.Serialization;
using EngineeredWood.IO;

namespace EngineeredWood.Iceberg.Manifest;

/// <summary>
/// Reads and writes Iceberg manifest files and manifest lists, dispatching between Avro (the Iceberg format)
/// and JSON (an EngineeredWood debugging format) by file extension.
/// </summary>
/// <remarks>
/// Avro manifests are decoded by field id against each file's own writer schema, so manifests from any
/// writer (Java, Spark, pyiceberg, iceberg-rust, iceberg-go) and any format version read. They are written in
/// the spec's schema for the table's format version, with the partition tuple typed by the spec and the
/// header metadata readers expect.
/// </remarks>
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
    /// Writes an unpartitioned format-version-2 data manifest. Returns the number of bytes written. Use the
    /// overload taking <see cref="ManifestWriteOptions"/> for anything else.
    /// </summary>
    public static ValueTask<long> WriteManifestAsync(
        ITableFileSystem fs, string path, IReadOnlyList<ManifestEntry> entries,
        CancellationToken ct = default) =>
        WriteManifestAsync(fs, path, entries, new ManifestWriteOptions(), ct);

    /// <summary>
    /// Writes manifest entries to the given path, choosing Avro or JSON format based on the file extension.
    /// Returns the number of bytes written.
    /// </summary>
    public static async ValueTask<long> WriteManifestAsync(
        ITableFileSystem fs, string path, IReadOnlyList<ManifestEntry> entries, ManifestWriteOptions options,
        CancellationToken ct = default)
    {
        byte[] data;
        if (IsAvro(path))
        {
            data = ManifestCodec.EncodeManifest(
                entries, options.FormatVersion, options.Content, options.ResolvePartitionType(entries),
                options.HeaderMetadata());
        }
        else
        {
            ManifestCodec.ValidateContent(entries, options.Content);
            if (entries.Any(e => e.DataFile.Partition is { Values.Count: > 0 }))
                throw new NotSupportedException(
                    $"Cannot write '{path}': the JSON manifest format does not hold partition values. Use an .avro path.");
            data = JsonSerializer.SerializeToUtf8Bytes(entries, JsonOptions.TypeInfo<IReadOnlyList<ManifestEntry>>());
        }

        await fs.WriteAllBytesAsync(path, data, ct).ConfigureAwait(false);
        return data.Length;
    }

    /// <summary>
    /// Reads manifest entries from the given path, choosing Avro or JSON format based on the file extension.
    /// Entries are as stored: values a reader inherits from the manifest list (a null snapshot id or
    /// sequence number) are left null. The partition tuple is typed from the file's own schema.
    /// </summary>
    public static async ValueTask<List<ManifestEntry>> ReadManifestAsync(
        ITableFileSystem fs, string path, CancellationToken ct = default)
    {
        var data = await fs.ReadAllBytesAsync(path, ct).ConfigureAwait(false);
        return IsAvro(path)
            ? ManifestCodec.DecodeManifest(data, path, partitionType: null).Entries
            : JsonSerializer.Deserialize(data, JsonOptions.TypeInfo<List<ManifestEntry>>())
                ?? throw new InvalidOperationException($"Failed to read manifest: {path}");
    }

    /// <summary>
    /// Writes a format-version-2 manifest list. Returns the number of bytes written. Use the overload
    /// taking <see cref="ManifestListWriteOptions"/> for anything else.
    /// </summary>
    public static ValueTask<long> WriteManifestListAsync(
        ITableFileSystem fs, string path, IReadOnlyList<ManifestListEntry> entries,
        CancellationToken ct = default) =>
        WriteManifestListAsync(fs, path, entries, new ManifestListWriteOptions(), ct);

    /// <summary>
    /// Writes manifest list entries to the given path, choosing Avro or JSON format based on the file
    /// extension. Returns the number of bytes written.
    /// </summary>
    public static async ValueTask<long> WriteManifestListAsync(
        ITableFileSystem fs, string path, IReadOnlyList<ManifestListEntry> entries, ManifestListWriteOptions options,
        CancellationToken ct = default)
    {
        var data = IsAvro(path)
            ? ManifestCodec.EncodeManifestList(entries, options.FormatVersion, options.HeaderMetadata())
            : JsonSerializer.SerializeToUtf8Bytes(entries, JsonOptions.TypeInfo<IReadOnlyList<ManifestListEntry>>());
        await fs.WriteAllBytesAsync(path, data, ct).ConfigureAwait(false);
        return data.Length;
    }

    /// <summary>
    /// Reads manifest list entries from the given path, choosing Avro or JSON format based on the file
    /// extension. A v1 manifest list's entries read as data manifests with sequence number 0.
    /// </summary>
    public static async ValueTask<List<ManifestListEntry>> ReadManifestListAsync(
        ITableFileSystem fs, string path, CancellationToken ct = default)
    {
        var data = await fs.ReadAllBytesAsync(path, ct).ConfigureAwait(false);
        return IsAvro(path)
            ? ManifestCodec.DecodeManifestList(data, path).Entries
            : JsonSerializer.Deserialize(data, JsonOptions.TypeInfo<List<ManifestListEntry>>())
                ?? throw new InvalidOperationException($"Failed to read manifest list: {path}");
    }

    // --- Reads through a snapshot ---

    /// <summary>
    /// Reads a snapshot's manifest list, throwing <see cref="FileNotFoundException"/> when it is missing. A
    /// snapshot whose manifest list is gone cannot be read: treating it as empty would silently drop every
    /// file the snapshot holds, from a scan or from the next commit's carried-forward state. Throws
    /// <see cref="UnsupportedTableFeatureException"/> for an encrypted table or snapshot, whose manifest
    /// list cannot be read as plaintext.
    /// </summary>
    internal static async ValueTask<List<ManifestListEntry>> ReadSnapshotManifestListAsync(
        ITableFileSystem fs, TableMetadata metadata, Snapshot snapshot, CancellationToken ct)
    {
        metadata.ThrowIfDataInaccessible(snapshot);
        if (!await fs.ExistsAsync(snapshot.ManifestList, ct).ConfigureAwait(false))
            throw new FileNotFoundException(
                $"Snapshot {snapshot.SnapshotId}'s manifest list '{snapshot.ManifestList}' does not exist; " +
                "the table's metadata references a missing file.",
                snapshot.ManifestList);
        return await ReadManifestListAsync(fs, snapshot.ManifestList, ct).ConfigureAwait(false);
    }

    /// <summary>
    /// Reads a snapshot's manifest list for a commit that will write it out again. On top of
    /// <see cref="ReadSnapshotManifestListAsync"/>'s checks, refuses a list holding a field this library does not
    /// model (rewriting the list would drop it) or an encrypted manifest (carrying it into a list this library
    /// writes would put a plaintext list over encrypted data).
    /// </summary>
    internal static async ValueTask<List<ManifestListEntry>> ReadSnapshotManifestListForRewriteAsync(
        ITableFileSystem fs, TableMetadata metadata, Snapshot snapshot, CancellationToken ct)
    {
        metadata.ThrowIfDataInaccessible(snapshot);
        if (!await fs.ExistsAsync(snapshot.ManifestList, ct).ConfigureAwait(false))
            throw new FileNotFoundException(
                $"Snapshot {snapshot.SnapshotId}'s manifest list '{snapshot.ManifestList}' does not exist; " +
                "the table's metadata references a missing file.",
                snapshot.ManifestList);

        var data = await fs.ReadAllBytesAsync(snapshot.ManifestList, ct).ConfigureAwait(false);
        List<ManifestListEntry> entries;
        if (IsAvro(snapshot.ManifestList))
        {
            var decoded = ManifestCodec.DecodeManifestList(data, snapshot.ManifestList);
            if (decoded.UnmodelledFields.Count > 0)
                throw new NotSupportedException(
                    $"Cannot rewrite manifest list '{snapshot.ManifestList}': it holds fields this library does not " +
                    $"model ({string.Join(", ", decoded.UnmodelledFields)}), and the next snapshot's list would drop them.");
            entries = decoded.Entries;
        }
        else
        {
            entries = JsonSerializer.Deserialize(data, JsonOptions.TypeInfo<List<ManifestListEntry>>())
                ?? throw new InvalidOperationException($"Failed to read manifest list: {snapshot.ManifestList}");
        }

        ThrowIfEncryptedManifest(entries, snapshot, "the table cannot be written");
        return entries;
    }

    /// <summary>Refuses a manifest-list entry with key metadata: its manifest is encrypted.</summary>
    internal static void ThrowIfEncryptedManifest(IEnumerable<ManifestListEntry> entries, Snapshot snapshot, string consequence)
    {
        if (entries.FirstOrDefault(e => e.KeyMetadata is not null) is { } encrypted)
            throw new UnsupportedTableFeatureException(
                $"Manifest '{encrypted.ManifestPath}' of snapshot {snapshot.SnapshotId} is encrypted (it has key " +
                $"metadata); table encryption is not supported, so {consequence}.");
    }

    /// <summary>
    /// Reads a manifest a snapshot's manifest list names, with the partition tuple typed by the manifest's
    /// spec and inherited values filled in (see <see cref="ManifestCodec.Inherit"/>). Throws
    /// <see cref="FileNotFoundException"/> when it is missing, for the same reason as
    /// <see cref="ReadSnapshotManifestListAsync"/>, and <see cref="UnsupportedTableFeatureException"/> when the
    /// manifest or a file it lists is encrypted.
    /// </summary>
    internal static async ValueTask<List<ManifestEntry>> ReadSnapshotManifestAsync(
        ITableFileSystem fs, TableMetadata metadata, Snapshot snapshot, ManifestListEntry manifest, CancellationToken ct)
    {
        var decoded = await ReadSnapshotManifestDecodedAsync(fs, metadata, snapshot, manifest, ct).ConfigureAwait(false);
        // Bounds are decoded against the current schema, which planning binds filters to.
        var columnTypes = ManifestValues.BoundColumnTypes(metadata.Schemas.First(s => s.SchemaId == metadata.CurrentSchemaId));
        return ManifestCodec.Inherit(decoded, manifest)
            .Select(e => e with { DataFile = ManifestValues.WithDecodedBounds(e.DataFile, columnTypes) })
            .ToList();
    }

    /// <summary>As <see cref="ReadSnapshotManifestAsync"/>, but before inheritance and with what a rewrite needs to know.</summary>
    internal static async ValueTask<DecodedManifest> ReadSnapshotManifestDecodedAsync(
        ITableFileSystem fs, TableMetadata metadata, Snapshot snapshot, ManifestListEntry manifest, CancellationToken ct)
    {
        if (manifest.KeyMetadata is not null)
            throw new UnsupportedTableFeatureException(
                $"Manifest '{manifest.ManifestPath}' of snapshot {snapshot.SnapshotId} is encrypted (it has key " +
                "metadata); table encryption is not supported, so it cannot be read.");
        if (!await fs.ExistsAsync(manifest.ManifestPath, ct).ConfigureAwait(false))
            throw new FileNotFoundException(
                $"Manifest '{manifest.ManifestPath}' of snapshot {snapshot.SnapshotId} does not exist; " +
                "the table's metadata references a missing file.",
                manifest.ManifestPath);

        var data = await fs.ReadAllBytesAsync(manifest.ManifestPath, ct).ConfigureAwait(false);
        DecodedManifest decoded;
        if (IsAvro(manifest.ManifestPath))
        {
            IReadOnlyList<PartitionValueType>? partitionType = null;
            var spec = metadata.PartitionSpecs.FirstOrDefault(s => s.SpecId == manifest.PartitionSpecId);
            if (spec is not null)
            {
                var schemas = metadata.Schemas.Where(s => s.SchemaId == metadata.CurrentSchemaId)
                    .Concat(metadata.Schemas.Where(s => s.SchemaId != metadata.CurrentSchemaId).Reverse());
                // A spec this library cannot type (an unknown transform) is typed from the file instead.
                if (PartitionTypes.TryResolve(spec, schemas, out var resolved, out _))
                    partitionType = resolved;
            }
            decoded = ManifestCodec.DecodeManifest(data, manifest.ManifestPath, partitionType);
        }
        else
        {
            var entries = JsonSerializer.Deserialize(data, JsonOptions.TypeInfo<List<ManifestEntry>>())
                ?? throw new InvalidOperationException($"Failed to read manifest: {manifest.ManifestPath}");
            decoded = new DecodedManifest(entries, new Dictionary<string, byte[]>(), HasSequenceNumbers: true, []);
        }

        foreach (var entry in decoded.Entries)
        {
            if (entry.DataFile.KeyMetadata is not null)
                throw new UnsupportedTableFeatureException(
                    $"Data file '{entry.DataFile.FilePath}' is encrypted (it has key metadata); table encryption " +
                    "is not supported, so it cannot be read.");
        }
        return decoded;
    }

    private static bool IsAvro(string path) => path.EndsWith(".avro", StringComparison.OrdinalIgnoreCase);

    internal static byte[] Utf8(string value) => System.Text.Encoding.UTF8.GetBytes(value);

    internal static byte[] Utf8(long value) => Utf8(value.ToString(CultureInfo.InvariantCulture));
}

/// <summary>What a manifest is written for: the format version, the partition spec its files use, and the table schema.</summary>
public sealed record ManifestWriteOptions
{
    /// <summary>The table's format version, which decides the manifest's schema (default 2).</summary>
    public int FormatVersion { get; init; } = 2;

    /// <summary>The partition spec of the manifest's files (default unpartitioned).</summary>
    public PartitionSpec PartitionSpec { get; init; } = PartitionSpec.Unpartitioned;

    /// <summary>
    /// The table schema, written to the manifest header and used to type the partition tuple. Without it,
    /// the tuple is typed from the entries' <see cref="PartitionData"/>.
    /// </summary>
    public Schema? Schema { get; init; }

    /// <summary>Whether the manifest holds data files or delete files.</summary>
    public ManifestContent Content { get; init; } = ManifestContent.Data;

    /// <summary>Further schemas to resolve partition source fields in (a dropped source column's spec).</summary>
    internal IReadOnlyList<Schema> OtherSchemas { get; init; } = [];

    /// <summary>Options for a manifest of <paramref name="table"/>'s spec <paramref name="specId"/>.</summary>
    public static ManifestWriteOptions For(TableMetadata table, int specId, ManifestContent content)
    {
        var spec = table.PartitionSpecs.FirstOrDefault(s => s.SpecId == specId)
            ?? throw new ArgumentException($"Table has no partition spec {specId}.", nameof(specId));

        // The header's schema must be one the spec binds to: readers rebuild the spec from the header's
        // schema and partition-spec. For an old spec whose source column has since been dropped that is not
        // the current schema, so take the newest schema that still has every source field.
        var candidates = table.Schemas.Where(s => s.SchemaId == table.CurrentSchemaId)
            .Concat(table.Schemas.Where(s => s.SchemaId != table.CurrentSchemaId).Reverse())
            .ToList();
        // Binding needs only the source fields, so this also works for a spec whose transforms this library
        // cannot evaluate.
        var sourceIds = spec.Fields.SelectMany(f => f.SourceIds ?? [f.SourceId]).Distinct().ToList();
        var header = candidates.FirstOrDefault(s => sourceIds.All(id => PartitionTypes.FindField(s.Fields, id) is not null))
            ?? candidates[0];
        return new ManifestWriteOptions
        {
            FormatVersion = table.FormatVersion,
            PartitionSpec = spec,
            Schema = header,
            Content = content,
            OtherSchemas = candidates.Where(s => !ReferenceEquals(s, header)).ToList(),
        };
    }

    /// <summary>
    /// Set for a rewrite of an existing manifest: then a spec this library cannot type itself (an unknown
    /// transform) is typed from the entries' own tuples, which the manifest already holds. New files may not
    /// be written under such a spec.
    /// </summary>
    internal bool RewritesExistingManifest { get; init; }

    internal IReadOnlyList<PartitionValueType> ResolvePartitionType(IReadOnlyList<ManifestEntry> entries)
    {
        if (PartitionSpec.Fields.Count == 0)
            return [];

        if (!RewritesExistingManifest
            && PartitionSpec.Fields.FirstOrDefault(f => f.Transform is UnknownTransform || f.SourceIds is { Count: > 1 }) is { } unknown)
        {
            throw new NotSupportedException(
                $"Cannot write new files under partition spec {PartitionSpec.SpecId}: field '{unknown.Name}' has a " +
                "transform this library does not know, and the spec forbids committing files under one.");
        }

        string? reason = "no table schema was given";
        if (Schema is not null)
        {
            if (PartitionTypes.TryResolve(PartitionSpec, [Schema, .. OtherSchemas], out var type, out reason))
                return type;
        }

        // Fall back to the entries' own typed tuples (a rewrite of a manifest whose spec this library cannot
        // type itself), provided they are the spec's fields.
        var sample = entries.Select(e => e.DataFile.Partition).FirstOrDefault(p => p is { Values.Count: > 0 });
        if (sample is not null
            && sample.Values.Select(v => v.FieldId).SequenceEqual(PartitionSpec.Fields.Select(f => f.FieldId)))
        {
            return sample.Values.Select(v => new PartitionValueType(v.FieldId, v.Name, v.Type)).ToList();
        }

        throw new NotSupportedException(
            $"Cannot type the partition tuple of spec {PartitionSpec.SpecId}: {reason}.");
    }

    // The keys Iceberg Java writes and readers look for.
    internal IReadOnlyDictionary<string, byte[]> HeaderMetadata()
    {
        var md = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["partition-spec"] = ManifestIO.Utf8(IcebergJsonSerializer.Serialize(PartitionSpec.Fields)),
            ["partition-spec-id"] = ManifestIO.Utf8(PartitionSpec.SpecId),
            ["format-version"] = ManifestIO.Utf8(FormatVersion),
        };
        if (Schema is not null)
        {
            md["schema"] = ManifestIO.Utf8(IcebergJsonSerializer.Serialize(Schema));
            md["schema-id"] = ManifestIO.Utf8(Schema.SchemaId);
        }
        if (FormatVersion >= 2)
            md["content"] = ManifestIO.Utf8(Content == ManifestContent.Data ? "data" : "deletes");
        return md;
    }
}

/// <summary>What a manifest list is written for: the format version and the snapshot it belongs to.</summary>
public sealed record ManifestListWriteOptions
{
    /// <summary>The table's format version, which decides the manifest list's schema (default 2).</summary>
    public int FormatVersion { get; init; } = 2;

    /// <summary>The snapshot the list belongs to, written to the header when set.</summary>
    public long? SnapshotId { get; init; }

    /// <summary>The snapshot's parent, written to the header (as <c>null</c> when it has none) with <see cref="SnapshotId"/>.</summary>
    public long? ParentSnapshotId { get; init; }

    /// <summary>The snapshot's sequence number, written to the header when set (format version 2 and later).</summary>
    public long? SequenceNumber { get; init; }

    /// <summary>The snapshot's first row id, written to the header when set (format version 3).</summary>
    public long? FirstRowId { get; init; }

    internal IReadOnlyDictionary<string, byte[]> HeaderMetadata()
    {
        var md = new Dictionary<string, byte[]>(StringComparer.Ordinal)
        {
            ["format-version"] = ManifestIO.Utf8(FormatVersion),
        };
        if (SnapshotId is { } id)
        {
            md["snapshot-id"] = ManifestIO.Utf8(id);
            md["parent-snapshot-id"] = ParentSnapshotId is { } parent ? ManifestIO.Utf8(parent) : ManifestIO.Utf8("null");
        }
        if (FormatVersion >= 2 && SequenceNumber is { } seq)
            md["sequence-number"] = ManifestIO.Utf8(seq);
        if (FormatVersion >= 3 && FirstRowId is { } first)
            md["first-row-id"] = ManifestIO.Utf8(first);
        return md;
    }
}
