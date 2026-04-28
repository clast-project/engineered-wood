// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Encodings;
using EngineeredWood.IO;
using EngineeredWood.Lance.Proto;
using EngineeredWood.Lance.Table.Proto;
using Google.Protobuf;

namespace EngineeredWood.Lance.Table.Indices;

/// <summary>
/// Walks a Lance dataset's transaction history to discover the set of
/// indices currently active on the table.
///
/// <para>Lance writes a <c>Transaction</c> proto for every commit, both
/// inline in the manifest (when <c>transaction_section</c> is set) and
/// as a sibling file in <c>_transactions/{read_version}-{uuid}.txn</c>.
/// A <c>CreateIndex</c> operation in any transaction adds entries to
/// <c>new_indices</c> and removes entries listed in <c>removed_indices</c>;
/// the live index set is the running accumulation across all such
/// transactions.</para>
///
/// <para>This walker reads the <c>.txn</c> files (more reliable than
/// the optional inline copy) and only keys on the <c>CreateIndex</c>
/// case of the <c>operation</c> oneof. Other operations (Append,
/// Delete, Overwrite, Rewrite, etc.) are skipped — they don't change
/// the index set.</para>
///
/// <para>Caveat: vacuuming can prune old <c>.txn</c> files. If indices
/// were created in a vacuumed version, this walker won't find them. A
/// manifest-resident catalog of current indices would be needed to
/// handle that case; the current Lance manifest format doesn't expose
/// one consistently.</para>
/// </summary>
internal static class IndexCatalog
{
    public static async ValueTask<IReadOnlyList<IndexInfo>> DiscoverAsync(
        ITableFileSystem fs, Proto.Manifest manifest, CancellationToken cancellationToken = default)
    {
        // Build a field-id → name lookup from the manifest's flat schema.
        var fieldNames = new Dictionary<int, string>();
        BuildFieldNameMap(manifest.Fields, fieldNames);

        // Walk every .txn file in _transactions/ ordered by read_version.
        // Each Lance commit produces a file named "{read_version}-{uuid}.txn".
        var txnEntries = new List<(ulong ReadVersion, string Path)>();
        await foreach (var info in fs.ListAsync("_transactions/", cancellationToken).ConfigureAwait(false))
        {
            string fileName = ExtractFileName(info.Path);
            if (!fileName.EndsWith(".txn", StringComparison.Ordinal))
                continue;
            int dash = fileName.IndexOf('-');
            if (dash <= 0) continue;
            if (!ulong.TryParse(fileName.AsSpan(0, dash), out ulong readVersion))
                continue;
            txnEntries.Add((readVersion, info.Path));
        }
        txnEntries.Sort((a, b) => a.ReadVersion.CompareTo(b.ReadVersion));

        // Accumulate live indices keyed by UUID. Each CreateIndex adds new
        // entries and removes listed ones; later edits supersede earlier
        // ones for the same UUID.
        var live = new Dictionary<Guid, (IndexMetadata Meta, ulong DatasetVersion)>();
        foreach (var (readVersion, path) in txnEntries)
        {
            byte[] bytes = await fs.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
            Transaction txn;
            try { txn = Transaction.Parser.ParseFrom(bytes); }
            catch (InvalidProtocolBufferException) { continue; }
            if (txn.OperationCase != Transaction.OperationOneofCase.CreateIndex) continue;

            var op = txn.CreateIndex;
            ulong producedVersion = readVersion + 1;
            foreach (var idx in op.RemovedIndices)
            {
                if (TryGetUuid(idx, out var uuid))
                    live.Remove(uuid);
            }
            foreach (var idx in op.NewIndices)
            {
                if (!TryGetUuid(idx, out var uuid)) continue;
                live[uuid] = (idx, producedVersion);
            }
        }

        var infos = new List<IndexInfo>(live.Count);
        foreach (var (uuid, (meta, datasetVersion)) in live)
        {
            var columnNames = new List<string>(meta.Fields.Count);
            foreach (int fieldId in meta.Fields)
                columnNames.Add(fieldNames.TryGetValue(fieldId, out var name) ? name : $"<field#{fieldId}>");

            var fragmentIds = DecodeFragmentBitmap(meta.FragmentBitmap);

            infos.Add(new IndexInfo
            {
                Name = meta.Name,
                Uuid = uuid,
                TypeUrl = meta.IndexDetails?.TypeUrl ?? string.Empty,
                ColumnNames = columnNames,
                FragmentIds = fragmentIds,
                DirectoryPath = $"_indices/{uuid:D}",
                BuiltFromVersion = datasetVersion,
            });
        }
        infos.Sort((a, b) => string.Compare(a.Name, b.Name, StringComparison.Ordinal));
        return infos;
    }

    private static void BuildFieldNameMap(
        Google.Protobuf.Collections.RepeatedField<Field> fields, Dictionary<int, string> dest)
    {
        foreach (var field in fields)
        {
            // Field.id may be -1 for proto-default; only index fields with a
            // real id assignment.
            dest[field.Id] = field.Name;
        }
    }

    private static bool TryGetUuid(IndexMetadata idx, out Guid uuid)
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

    private static string ExtractFileName(string path)
    {
        int slash = System.Math.Max(path.LastIndexOf('/'), path.LastIndexOf('\\'));
        return slash >= 0 ? path.Substring(slash + 1) : path;
    }
}
