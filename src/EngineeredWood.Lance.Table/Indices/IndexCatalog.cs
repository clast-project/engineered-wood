// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Encodings;
using EngineeredWood.Lance.Proto;
using EngineeredWood.Lance.Table.Proto;
using Google.Protobuf;

namespace EngineeredWood.Lance.Table.Indices;

/// <summary>
/// Lists the indices active on a Lance dataset version from its manifest's index section (#991), as lance's own reader
/// does (lance-table 14.0.3 <c>read_manifest_indexes</c>). The section holds one <c>IndexMetadata</c> per index
/// segment; a manifest without one has no indices.
///
/// <para>EW once found indices by walking <c>_transactions/</c> for <c>CreateIndex</c> operations instead. That missed
/// an index whose transaction file was vacuumed and a shallow clone's inherited ones (the clone's only transaction is
/// its <c>Clone</c>), and kept listing an index after a commit that dropped it from the section, which lance then no
/// longer reads. lance has written the index section since before it wrote transaction files, so a manifest without
/// one is not an older format with indices elsewhere, and there is no fallback.</para>
///
/// <para>Each entry's base is checked here, so the dataset refuses to open when an index names a base EW cannot place,
/// as it does for a data file; but its directory is resolved only when the index is opened
/// (<see cref="IndexInfo.ResolveDirectory"/>), so an index whose base is not registered does not fail a read that
/// does not open it (#992).</para>
/// </summary>
internal static class IndexCatalog
{
    public static IReadOnlyList<IndexInfo> FromManifest(
        Proto.Manifest manifest, IndexSection? section, LanceFileLocator locator)
    {
        if (section is null || section.Indices.Count == 0)
            return System.Array.Empty<IndexInfo>();

        // Build a field-id → path lookup from the manifest's flat schema.
        var fieldPaths = BuildFieldPathMap(manifest.Fields);

        var infos = new List<IndexInfo>(section.Indices.Count);
        foreach (var meta in section.Indices)
        {
            if (!TryGetUuid(meta, out var uuid))
                throw new LanceTableFormatException(
                    $"Index '{meta.Name}' in the manifest's index section has no valid 16-byte UUID.");

            var columnPaths = new List<IReadOnlyList<string>>(meta.Fields.Count);
            foreach (int fieldId in meta.Fields)
                columnPaths.Add(fieldPaths.TryGetValue(fieldId, out var path) ? path : [$"<field#{fieldId}>"]);
            var columnNames = columnPaths.Select(path => string.Join(".", path)).ToList();

            infos.Add(new IndexInfo
            {
                Name = meta.Name,
                Uuid = uuid,
                TypeUrl = meta.IndexDetails?.TypeUrl ?? string.Empty,
                ColumnNames = columnNames,
                ColumnPaths = columnPaths,
                FragmentIds = DecodeFragmentBitmap(meta.FragmentBitmap),
                DirectoryPath = locator.IndexDirectoryInBase(meta, uuid),
                BaseId = meta.HasBaseId ? meta.BaseId : null,
                BuiltFromVersion = meta.DatasetVersion,
                Metadata = meta,
                Locator = locator,
            });
        }
        // By name, then by the version each segment was built from, so an index's segments are listed oldest first.
        infos.Sort((a, b) =>
        {
            int byName = string.Compare(a.Name, b.Name, StringComparison.Ordinal);
            return byName != 0 ? byName : a.BuiltFromVersion.CompareTo(b.BuiltFromVersion);
        });
        return infos;
    }

    /// <summary>
    /// Each field's path in the manifest's flat schema: its name, after its parent's path. A field's own name is
    /// not enough to find it by: an index on field <c>x</c> of struct <c>s</c> answered for a top-level column
    /// <c>x</c> (#771). A field whose parent chain does not reach the root (a cycle, or a field id two fields
    /// share) has no path.
    /// </summary>
    private static Dictionary<int, IReadOnlyList<string>> BuildFieldPathMap(
        Google.Protobuf.Collections.RepeatedField<Field> fields)
    {
        var byId = new Dictionary<int, Field>();
        var shared = new HashSet<int>();
        foreach (var field in fields)
        {
            if (byId.ContainsKey(field.Id))
                shared.Add(field.Id);
            else
                byId[field.Id] = field;
        }

        var paths = new Dictionary<int, IReadOnlyList<string>>();
        foreach (var field in fields)
        {
            if (shared.Contains(field.Id) || paths.ContainsKey(field.Id))
                continue;
            var parts = new List<string>();
            var current = field;
            bool rooted = false;
            for (int depth = 0; depth <= fields.Count; depth++)
            {
                parts.Add(current.Name);
                // A parent id outside the schema (-1 by convention) is the root, as LanceSchemaConverter reads it.
                if (!byId.TryGetValue(current.ParentId, out var parent))
                {
                    rooted = true;
                    break;
                }
                if (shared.Contains(parent.Id))
                    break;
                current = parent;
            }
            if (!rooted)
                continue;
            parts.Reverse();
            paths[field.Id] = parts;
        }
        return paths;
    }

    internal static bool TryGetUuid(IndexMetadata idx, out Guid uuid)
    {
        if (idx.Uuid is null || idx.Uuid.Uuid.Length != 16)
        {
            uuid = default;
            return false;
        }
        // Lance writes the UUID as 16 raw bytes in big-endian-ish order.
        // .NET's Guid constructor with a byte[] uses little-endian for the
        // first three fields; we use the byte-ordered form here so the
        // string representation matches the directory name pylance writes.
        var bytes = idx.Uuid.Uuid.ToByteArray();
        uuid = new Guid(
            (bytes[0] << 24) | (bytes[1] << 16) | (bytes[2] << 8) | bytes[3],
            (short)((bytes[4] << 8) | bytes[5]),
            (short)((bytes[6] << 8) | bytes[7]),
            bytes[8], bytes[9], bytes[10], bytes[11],
            bytes[12], bytes[13], bytes[14], bytes[15]);
        return true;
    }

    private static IReadOnlyList<uint> DecodeFragmentBitmap(ByteString bytes)
    {
        if (bytes.IsEmpty) return System.Array.Empty<uint>();
        var bytesArr = bytes.ToByteArray();
        var ids = new List<uint>();
        RoaringBitmap.DeserializePortable(
            bytesArr, v => ids.Add(checked((uint)v)), RoaringBitmap.RoaringFormat.CRoaring);
        ids.Sort();
        return ids;
    }
}
