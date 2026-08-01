// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.DeltaLake.RowTracking;
using EngineeredWood.IO.Local;
using Ex = EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// The identity half of the embedding seam: a host doing its own copy-on-write rewrite must be able to read a
/// row's STABLE id, carry it through its own engine, and write it back into the new file — otherwise every
/// host-side UPDATE silently reassigns identities. Three pieces make that possible:
/// <see cref="RowSelection"/> (name exactly the rows to read back, by a key that cannot go stale in range),
/// <c>ReadRowsAsync</c>' <c>DeltaRowMetadata.RowTracking</c> derivation (an appended row HAS an id, it is
/// just not materialized), and <c>WriteDataFilesAsync</c>' <c>materializedRowIds</c> (bake it into the new
/// file). Plus <c>preAssignedSchema</c>, which lets the files of a CTAS be written before the table exists.
/// </summary>
public class HostRowIdentityTests : IDisposable
{
    private readonly string _tempDir;

    public HostRowIdentityTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_hostid_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }


    private static Apache.Arrow.Schema BuildSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch Batch(long startId, int count, string prefix = "v")
    {
        var ids = new Int64Array.Builder();
        var values = new StringArray.Builder();
        for (int i = 0; i < count; i++)
        {
            ids.Append(startId + i);
            values.Append(prefix + (startId + i));
        }
        return new RecordBatch(BuildSchema(), [ids.Build(), values.Build()], count);
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private Task<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir)).AsTask();

    /// <summary>
    /// Every row's (user id → STABLE row id) resolved the way a spec reader does: read each active file's
    /// parquet directly, take the materialized row-id column where the file has one, else derive
    /// <c>baseRowId + position</c>. Deliberately NOT via <c>ReadAllWithRowIdsAsync</c>, whose
    /// <c>_ew_row_address</c> column is the snapshot-relative TRANSIENT address — same name, different
    /// number (it is documented as "NOT a stable Delta row id").
    /// </summary>
    private async Task<Dictionary<long, long?>> StableIdsAsync(DeltaTable table)
    {
        string? matName = RowTrackingConfig.TryGetMaterializedColumnNames(
            table.CurrentSnapshot.Metadata.Configuration).RowIdColumnName;

        var byId = new Dictionary<long, long?>();
        foreach (var add in table.CurrentSnapshot.ActiveFiles.Values)
        {
            await using var file = new LocalRandomAccessFile(Path.Combine(
                _tempDir, add.Path.Replace('/', Path.DirectorySeparatorChar)));
            using var reader = new Parquet.ParquetFileReader(file, ownsFile: false);
            long position = 0;
            await foreach (var batch in reader.ReadAllAsync())
            {
                var ids = (Int64Array)batch.Column("id");
                int matIndex = matName is null ? -1 : batch.Schema.GetFieldIndex(matName);
                var mat = matIndex >= 0 ? (Int64Array)batch.Column(matIndex) : null;
                for (int i = 0; i < batch.Length; i++, position++)
                {
                    byId[ids.GetValue(i)!.Value] = mat is not null && !mat.IsNull(i)
                        ? mat.GetValue(i)
                        : add.BaseRowId is { } b ? b + position : null;
                }
            }
        }
        return byId;
    }

    /// <summary>Every row's (user id -> its DML locator): the file's <c>add.path</c> plus the row's absolute
    /// in-file position. The packed address is unpacked into a path HERE, against the snapshot it was read
    /// from, which is where a stale address is caught rather than silently mis-addressing a file.</summary>
    private static async Task<Dictionary<long, (string Path, long Position)>> LocateRowsAsync(DeltaTable table)
    {
        var ordered = table.CurrentSnapshot.ActiveFiles.Values
            .Select(a => a.Path).OrderBy(p => p, StringComparer.Ordinal).ToList();
        var located = new Dictionary<long, (string, long)>();
        await foreach (var batch in table.ReadAsync(new DeltaReadOptions { Metadata = DeltaRowMetadata.RowAddress }))
        {
            var ids = (Int64Array)batch.Column("id");
            var rids = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
            for (int i = 0; i < batch.Length; i++)
            {
                long rid = rids.GetValue(i)!.Value;
                located[ids.GetValue(i)!.Value] =
                    (ordered[TransientRowAddress.FileOrdinal(rid)], TransientRowAddress.Position(rid));
            }
        }
        return located;
    }

    /// <summary>The rows at <paramref name="rows"/> as the path-keyed DML boundary key.</summary>
    private static RowSelection Sel(params (string Path, long Position)[] rows)
    {
        var byPath = new Dictionary<string, IReadOnlyCollection<long>>(StringComparer.Ordinal);
        foreach (var (path, position) in rows)
        {
            if (!byPath.TryGetValue(path, out var set))
                byPath[path] = set = new HashSet<long>();
            ((HashSet<long>)set).Add(position);
        }
        return RowSelection.ByPath(byPath);
    }

    // ── reading back exactly what was selected ──

    [Fact]
    public async Task ReadRows_SelectionSpanningTwoFiles_ReturnsExactlyThoseRows()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 5)]);
        await table.WriteAsync([Batch(11, 5)]); // a second file, so the selection spans two paths
        var at = await LocateRowsAsync(table);

        var returned = new List<long>();
        await foreach (var batch in table.ReadRowsAsync(Sel(at[2], at[4], at[13])))
        {
            var ids = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                returned.Add(ids.GetValue(i)!.Value);
        }

        returned.Sort();
        Assert.Equal(new long[] { 2, 4, 13 }, returned);
    }

    [Fact]
    public async Task ReadRows_SkipsDeletionVectorHiddenRows()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 5)]);
        var at = await LocateRowsAsync(table);

        await table.DeleteAsync(Ex.Equal("id", 3L));

        var ids = new List<long>();
        await foreach (var batch in table.ReadRowsAsync(Sel(at[3], at[4])))
        {
            var col = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }

        // The hidden row simply does not come back — a caller pairing results POSITIONALLY with its request
        // would otherwise attribute the survivor's data to the deleted row.
        Assert.Equal([4L], ids);
    }

    // ── metadata columns: the read that could not ask ──

    /// <summary>
    /// None by default — the schema is the table's own, unchanged. Asking for the address costs a sort of the
    /// whole active set for the ordinal, so a caller that does not need it must not pay for it.
    /// </summary>
    [Fact]
    public async Task ReadRowsMetadata_IsOptional()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 5)]);
        var at = await LocateRowsAsync(table);

        int rows = 0;
        await foreach (var batch in table.ReadRowsAsync(Sel(at[2])))
        {
            Assert.Equal(table.CurrentSnapshot.Schema.Fields.Count, batch.ColumnCount);
            rows += batch.Length;
        }
        Assert.Equal(1, rows);
    }

    /// <summary>
    /// A host whose row identifiers ARE the transient address needs it back to pair results with what it
    /// asked for — batching and deletion-vector filtering both break any positional correspondence, and this
    /// read surfaces no absolute position otherwise, so it cannot be reconstructed from outside.
    ///
    /// <para>The file ORDINAL half is what makes this more than bookkeeping: it is a position in the
    /// snapshot's FULL path-sorted active set, not in the selection. So the rows are taken from the
    /// LAST-SORTING file ONLY — an ordinal derived from the selection would then be 0 where the right answer
    /// is 1, whereas a selection naming BOTH files makes the two numberings agree and every assertion below
    /// pass under either.</para>
    /// </summary>
    [Fact]
    public async Task ReadRowsMetadata_RowAddress_OrdinalsIndexTheFullActiveSet()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema());
        await table.WriteAsync([Batch(1, 5)]);
        await table.WriteAsync([Batch(11, 5)]);
        var at = await LocateRowsAsync(table);

        // Data files are GUID-named, so which BATCH sorts last is not knowable up front.
        var ordered = table.CurrentSnapshot.ActiveFiles.Values
            .Select(f => f.Path).OrderBy(p => p, StringComparer.Ordinal).ToList();
        Assert.Equal(2, ordered.Count);
        var wanted = at.Values.Where(a => a.Path == ordered[^1]).Take(2).ToArray();
        Assert.Equal(2, wanted.Length);

        var addresses = new List<long>();
        await foreach (var batch in table.ReadRowsAsync(
            Sel(wanted),
            options: new DeltaRowReadOptions { Metadata = DeltaRowMetadata.RowAddress }))
        {
            // Appended after the user columns, and NOT prefixed — the enum's stated contract.
            Assert.Equal(TransientRowAddress.ColumnName, batch.Schema.FieldsList[^1].Name);
            var addr = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
            for (int i = 0; i < batch.Length; i++)
                addresses.Add(addr.GetValue(i)!.Value);
        }

        var expected = wanted
            .Select(w => TransientRowAddress.Pack(ordered.IndexOf(w.Path), w.Position))
            .OrderBy(a => a).ToList();
        Assert.Equal(expected, addresses.OrderBy(a => a).ToList());
        Assert.All(addresses, a => Assert.Equal(1, TransientRowAddress.FileOrdinal(a)));
    }

    /// <summary>
    /// COMBINABLE, which is the enum's own justification for existing — two kinds, one pass. Also pins that
    /// the two describe the SAME rows: the locator pair must unpack to the packed address beside it, or a
    /// caller correlating on one and rewriting by the other silently targets different rows.
    /// </summary>
    [Fact]
    public async Task ReadRowsMetadata_AddressAndLocator_AgreeInOnePass()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), enableDeletionVectors: true);
        await table.WriteAsync([Batch(1, 5)]);
        var at = await LocateRowsAsync(table);

        // A deletion vector first, so an ABSOLUTE position and a batch offset disagree — without it the two
        // forms would agree for the wrong reason.
        await table.DeleteAsync(Ex.Equal("id", 2L));

        var ordered = table.CurrentSnapshot.ActiveFiles.Values
            .Select(f => f.Path).OrderBy(p => p, StringComparer.Ordinal).ToList();

        int seen = 0;
        await foreach (var batch in table.ReadRowsAsync(
            Sel(at[3], at[5]),
            options: new DeltaRowReadOptions
            {
                Metadata = DeltaRowMetadata.RowAddress | DeltaRowMetadata.Locator,
            }))
        {
            // EVERY column is exactly as long as the batch. The metadata columns are built over the TAKEN
            // rows while the loop still has the SCANNED batch in hand, so sizing one from the wrong count
            // yields a malformed batch whose surplus values are never read — invisible to any assertion about
            // values, and a hazard once the batch crosses the Arrow C interface. (Mutation-testing this suite
            // found the mutant SURVIVED without this.)
            Assert.All(
                Enumerable.Range(0, batch.ColumnCount),
                c => Assert.Equal(batch.Length, batch.Column(c).Length));

            var addr = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
            var path = (StringArray)batch.Column(
                DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.FilePathSuffix);
            var index = (Int64Array)batch.Column(
                DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIndexSuffix);
            for (int i = 0; i < batch.Length; i++)
            {
                long packed = addr.GetValue(i)!.Value;
                Assert.Equal(ordered.IndexOf(path.GetString(i)), TransientRowAddress.FileOrdinal(packed));
                Assert.Equal(index.GetValue(i)!.Value, TransientRowAddress.Position(packed));
                seen++;
            }
        }
        Assert.Equal(2, seen);
    }

    // ── RowTracking: the spec derivation, not just materialized values ──

    [Fact]
    public async Task ReadRowsMetadata_RowTracking_DerivesIdsForRowsFromAPlainAppend()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), enableRowTracking: true);
        await table.WriteAsync([Batch(1, 5)]);
        var at = await LocateRowsAsync(table);
        var stable = await StableIdsAsync(table);

        var seen = new List<long>();
        var reported = new List<long?>();
        var versions = new List<long?>();
        await foreach (var batch in table.ReadRowsAsync(
            Sel(at[2], at[4]),
            options: new DeltaRowReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var col = (Int64Array)batch.Column("id");
            var ids = (Int64Array)batch.Column(
                DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIdSuffix);
            var vers = (Int64Array)batch.Column(
                DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowCommitVersionSuffix);
            for (int i = 0; i < batch.Length; i++)
            {
                seen.Add(col.GetValue(i)!.Value);
                reported.Add(ids.IsNull(i) ? null : ids.GetValue(i));
                versions.Add(vers.IsNull(i) ? null : vers.GetValue(i));
            }
        }

        // A freshly appended file materializes NOTHING — its rows' ids are baseRowId + position, and
        // ReadFileAsync resolves that derivation before the column is built from it. Pinned here because a
        // host rewriting rows that came from a plain APPEND (most rows, most of the time) depends on it: null
        // would mean no identity to carry into the rewrite.
        Assert.Equal(2, reported.Count);
        Assert.All(reported, id => Assert.NotNull(id));
        Assert.Equal(
            seen.Select(id => (long?)stable[id]).OrderBy(x => x),
            reported.OrderBy(x => x));
        Assert.All(versions, v => Assert.NotNull(v));
    }

    /// <summary>
    /// The divergence the retired <c>sourceRowTrackingOut</c> left standing: on a table with no row tracking
    /// it silently handed back an all-null id per row, while asking the SAME question as a metadata column
    /// refuses and says why. Two ways to ask one question that disagree about "you asked wrong" is worse than
    /// either alone — a host reading nulls cannot tell "this row has no id yet" from "this table has no ids at
    /// all", and the second is a configuration mistake it should hear about.
    /// </summary>
    [Fact]
    public async Task ReadRowsMetadata_RowTracking_WithoutRowTracking_RefusesAndNamesTheAlternative()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema()); // no row tracking
        await table.WriteAsync([Batch(1, 3)]);
        var at = await LocateRowsAsync(table);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in table.ReadRowsAsync(
                Sel(at[2]),
                options: new DeltaRowReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
            {
            }
        });
        Assert.Contains("delta.enableRowTracking", ex.Message, StringComparison.Ordinal);
        Assert.Contains("RowAddress", ex.Message, StringComparison.Ordinal);

        // The read itself is unaffected — it is the ASK that is refused, not the table.
        int rows = 0;
        await foreach (var batch in table.ReadRowsAsync(Sel(at[2])))
            rows += batch.Length;
        Assert.Equal(1, rows);
    }

    // ── materializedRowIds: writing the identity back ──

    [Fact]
    public async Task MaterializedRowIds_PreserveIdentityAcrossAHostRewrite()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), enableDeletionVectors: true, enableRowTracking: true);
        await table.WriteAsync([Batch(1, 5)]);
        var before = await StableIdsAsync(table);
        var at = await LocateRowsAsync(table);

        // A host-side UPDATE of id=3: read the row back with its stable identity...
        var target = Sel(at[3]);
        var originalIds = new List<long?>();
        var post = new List<RecordBatch>();
        await foreach (var batch in table.ReadRowsAsync(
            target, new DeltaRowReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            var ids = (Int64Array)batch.Column("id");
            var stableIds = (Int64Array)batch.Column(
                DeltaMetadataColumns.DefaultPrefix + DeltaMetadataColumns.RowIdSuffix);
            var vals = new StringArray.Builder();
            for (int i = 0; i < batch.Length; i++)
            {
                vals.Append("rewritten");
                originalIds.Add(stableIds.IsNull(i) ? null : stableIds.GetValue(i));
            }

            // The post-image is the HOST's batch, built to the table's schema — the metadata columns are an
            // input to that construction, not part of it. Forwarding the read's batch verbatim would carry
            // _metadata.row_id into the data file as a column of its own.
            post.Add(new RecordBatch(BuildSchema(), [ids, vals.Build()], batch.Length));
        }
        Assert.Single(originalIds);

        // ...write the post-image carrying that id, delete the original row, commit both.
        var txn = table.StartTransaction();
        var files = await table.WriteDataFilesAsync(post, materializedRowIds: originalIds);
        txn.StageDataFiles(files);
        await txn.StageRowDeletesAsync(Sel(at[3]));
        await txn.CommitAsync();

        await using var check = await OpenAsync();
        var after = await StableIdsAsync(check);

        // The rewritten row kept its identity; untouched rows kept theirs.
        Assert.Equal(before[3], after[3]);
        foreach (long id in new long[] { 1, 2, 4, 5 })
            Assert.Equal(before[id], after[id]);

        var values = new Dictionary<long, string?>();
        await foreach (var batch in check.ReadAllAsync())
        {
            var ids = (Int64Array)batch.Column("id");
            var vals = (StringArray)batch.Column("value");
            for (int i = 0; i < batch.Length; i++)
                values[ids.GetValue(i)!.Value] = vals.GetString(i);
        }
        Assert.Equal("rewritten", values[3]);
    }

    [Fact]
    public async Task MaterializedRowIds_RideThePartitionSplit()
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("region", StringType.Default, true))
            .Build();
        await using var table = await DeltaTable.CreateAsync(
            Fs, schema, partitionColumns: ["region"], enableRowTracking: true);

        static RecordBatch Rows(Apache.Arrow.Schema s, params (long Id, string Region)[] rows)
        {
            var ids = new Int64Array.Builder();
            var regions = new StringArray.Builder();
            foreach (var (id, region) in rows)
            {
                ids.Append(id);
                regions.Append(region);
            }
            return new RecordBatch(s, [ids.Build(), regions.Build()], rows.Length);
        }

        // One batch spanning two partitions: the split regroups the rows, so a flat side-list would no longer
        // line up — each id has to travel WITH its row.
        var batch = Rows(schema, (1, "us"), (2, "eu"), (3, "us"), (4, "eu"));
        long?[] ids = [100L, 200L, 300L, 400L];

        var files = await table.WriteDataFilesAsync([batch], materializedRowIds: ids);
        Assert.Equal(2, files.Count); // one per partition

        var txn = table.StartTransaction();
        txn.StageDataFiles(files);
        await txn.CommitAsync();

        await using var check = await OpenAsync();
        var stable = await StableIdsAsync(check);

        // Each id followed its own row through the regrouping, across two files.
        Assert.Equal(100L, stable[1]);
        Assert.Equal(200L, stable[2]);
        Assert.Equal(300L, stable[3]);
        Assert.Equal(400L, stable[4]);
    }

    [Fact]
    public async Task MaterializedRowIds_WithoutRowTrackingDeclared_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema()); // no row tracking
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await table.WriteDataFilesAsync([Batch(1, 2)], materializedRowIds: [1L, 2L]));
        Assert.Contains("materializedRowIdColumnName", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MaterializedRowIds_LengthMismatch_Throws()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), enableRowTracking: true);
        var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
            await table.WriteDataFilesAsync([Batch(1, 3)], materializedRowIds: [1L, 2L]));
        Assert.Contains("one entry per row", ex.Message, StringComparison.Ordinal);
    }

    // ── preAssignedSchema: files written before the table exists ──

    [Fact]
    public async Task PreAssignedSchema_KeepsThePhysicalNamesTheFilesWereWrittenUnder()
    {
        // The CTAS shape: the host assigns column mapping FIRST, writes files against those physical names,
        // and only then creates the table. Re-assigning at create would mint fresh GUIDs and orphan the files.
        var (assigned, _) = ColumnMapping.AssignColumnMapping(
            SchemaConverter.FromArrowSchema(BuildSchema()));
        var physicalNames = assigned.Fields.ToDictionary(
            f => f.Name, f => ColumnMapping.GetPhysicalName(f, ColumnMappingMode.Name));

        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), columnMappingMode: ColumnMappingMode.Name, preAssignedSchema: assigned);

        // The committed metadata carries the caller's physical names, not freshly generated ones.
        foreach (var field in table.CurrentSnapshot.Schema.Fields)
        {
            Assert.Equal(
                physicalNames[field.Name],
                ColumnMapping.GetPhysicalName(field, ColumnMappingMode.Name));
        }

        // ...and the table is fully usable through those names.
        await table.WriteAsync([Batch(1, 3)]);
        var read = new List<long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var col = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                read.Add(col.GetValue(i)!.Value);
        }
        read.Sort();
        Assert.Equal([1L, 2L, 3L], read);
    }

    [Fact]
    public async Task PreAssignedSchema_WithoutFieldIdsUnderColumnMapping_Throws()
    {
        // An unmapped schema handed in under a mapping mode would produce a table whose metadata claims
        // name-mapping over columns that have no physical names — unreadable.
        var unmapped = SchemaConverter.FromArrowSchema(BuildSchema());
        await Assert.ThrowsAsync<DeltaFormatException>(async () =>
            await DeltaTable.CreateAsync(
                Fs, BuildSchema(), columnMappingMode: ColumnMappingMode.Name, preAssignedSchema: unmapped));
    }

    [Fact]
    public async Task OpenOrCreate_WithPreAssignedSchema_ReopensAnExistingTable()
    {
        var (assigned, _) = ColumnMapping.AssignColumnMapping(
            SchemaConverter.FromArrowSchema(BuildSchema()));

        await using (var created = await DeltaTable.OpenOrCreateAsync(
            Fs, BuildSchema(), columnMappingMode: ColumnMappingMode.Name, preAssignedSchema: assigned))
        {
            await created.WriteAsync([Batch(1, 2)]);
        }

        // A retried CTAS reopens rather than failing, and the schema it created is untouched.
        await using var reopened = await DeltaTable.OpenOrCreateAsync(
            Fs, BuildSchema(), columnMappingMode: ColumnMappingMode.Name, preAssignedSchema: assigned);
        Assert.Equal(1, reopened.CurrentSnapshot.Version);
        foreach (var field in reopened.CurrentSnapshot.Schema.Fields)
        {
            var original = assigned.Fields.First(f => f.Name == field.Name);
            Assert.Equal(
                ColumnMapping.GetPhysicalName(original, ColumnMappingMode.Name),
                ColumnMapping.GetPhysicalName(field, ColumnMappingMode.Name));
        }
    }
}
