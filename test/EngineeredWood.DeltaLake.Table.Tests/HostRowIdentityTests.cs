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
/// <c>ReadRowsAsync</c>' <c>sourceRowTrackingOut</c> derivation (an appended row HAS an id, it is just not
/// materialized), and <c>WriteDataFilesAsync</c>' <c>materializedRowIds</c> (bake it into the new file). Plus
/// <c>preAssignedSchema</c>, which lets the files of a CTAS be written before the table exists.
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
        await foreach (var batch in table.ReadAllWithRowIdsAsync(null, null))
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

    // ── sourceRowTrackingOut: the spec derivation, not just materialized values ──

    [Fact]
    public async Task SourceRowTracking_DerivesIdsForRowsFromAPlainAppend()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, BuildSchema(), enableRowTracking: true);
        await table.WriteAsync([Batch(1, 5)]);
        var at = await LocateRowsAsync(table);
        var stable = await StableIdsAsync(table);

        var tracking = new List<(long?[] Ids, long?[] Versions)>();
        var seen = new List<long>();
        await foreach (var batch in table.ReadRowsAsync(Sel(at[2], at[4]), sourceRowTrackingOut: tracking))
        {
            var col = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                seen.Add(col.GetValue(i)!.Value);
        }

        // A freshly appended file materializes NOTHING — its rows' ids are baseRowId + position, and
        // ReadFileAsync resolves that derivation before these out-params ever see it. Pinned here because a
        // host rewriting rows that came from a plain APPEND (most rows, most of the time) depends on it: null
        // would mean no identity to carry into the rewrite.
        var reported = tracking.SelectMany(t => t.Ids).ToList();
        Assert.Equal(2, reported.Count);
        Assert.All(reported, id => Assert.NotNull(id));
        Assert.Equal(
            seen.Select(id => (long?)stable[id]).OrderBy(x => x),
            reported.OrderBy(x => x));
        Assert.All(tracking.SelectMany(t => t.Versions), v => Assert.NotNull(v));
    }

    [Fact]
    public async Task SourceRowTracking_IsNullWithoutRowTracking()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, BuildSchema()); // no row tracking
        await table.WriteAsync([Batch(1, 3)]);
        var at = await LocateRowsAsync(table);

        var tracking = new List<(long?[] Ids, long?[] Versions)>();
        await foreach (var _ in table.ReadRowsAsync(Sel(at[2]), sourceRowTrackingOut: tracking))
        {
        }

        // No baseRowId on the add, so there is nothing to derive from — null rather than a fabricated id.
        Assert.All(tracking.SelectMany(t => t.Ids), id => Assert.Null(id));
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
        var tracking = new List<(long?[] Ids, long?[] Versions)>();
        var post = new List<RecordBatch>();
        await foreach (var batch in table.ReadRowsAsync(target, sourceRowTrackingOut: tracking))
        {
            var ids = (Int64Array)batch.Column("id");
            var vals = new StringArray.Builder();
            for (int i = 0; i < batch.Length; i++)
                vals.Append("rewritten");
            post.Add(new RecordBatch(BuildSchema(), [ids, vals.Build()], batch.Length));
        }
        var originalIds = tracking.SelectMany(t => t.Ids).ToList();
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
