// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// A column the table does not declare, handed to a write. Both write paths used to accept it: the parquet
/// writer writes whatever columns the batch has, and the column-mapping rename passes an unmatched column
/// through untouched. The result was a data file with a real extra column that no Delta reader ever shows,
/// because a Delta read projects by the table schema — so it cost bytes in every file written, forever, with
/// nothing reporting that it was there. MEASURED before the guard: a batch of <c>id,value,surprise</c> wrote
/// a parquet file whose schema was <c>id,value,surprise</c> through BOTH entry points, and read back as
/// <c>id,value</c>.
///
/// <para>The motivating case is narrower and easier to hit than a typo. A host's copy-on-write rewrite reads
/// rows with <c>DeltaRowMetadata.RowTracking</c> to carry their stable ids, which arrive as COLUMNS of the
/// batch; forwarding that batch as the post-image is the obvious thing to write, and it buried
/// <c>_metadata.row_id</c> in the data file.</para>
/// </summary>
public class UndeclaredColumnGuardTests : IDisposable
{
    private readonly string _tempDir;

    public UndeclaredColumnGuardTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_undeclared_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private LocalTableFileSystem Fs => new(_tempDir);

    private static Apache.Arrow.Schema TableSchema() => new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Field(new Field("value", StringType.Default, true))
        .Build();

    private static RecordBatch Batch(int count = 3)
    {
        var ids = new Int64Array.Builder();
        var vals = new StringArray.Builder();
        for (int i = 0; i < count; i++) { ids.Append(i); vals.Append("v" + i); }
        return new RecordBatch(TableSchema(), [ids.Build(), vals.Build()], count);
    }

    /// <summary>The same batch plus one column the table has never heard of.</summary>
    private static RecordBatch BatchWith(string extraName, int count = 3)
    {
        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Field(new Field(extraName, Int64Type.Default, true))
            .Build();
        var ids = new Int64Array.Builder();
        var vals = new StringArray.Builder();
        var extra = new Int64Array.Builder();
        for (int i = 0; i < count; i++) { ids.Append(i); vals.Append("v" + i); extra.Append(99 + i); }
        return new RecordBatch(schema, [ids.Build(), vals.Build(), extra.Build()], count);
    }

    [Fact]
    public async Task WriteAsync_UndeclaredColumn_IsRefusedNamingIt()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, TableSchema());

        var ex = await Assert.ThrowsAsync<ArgumentException>(
            async () => await table.WriteAsync([BatchWith("surprise")]));

        Assert.Contains("'surprise'", ex.Message, StringComparison.Ordinal);
        // The table's own columns are listed, so the caller can see WHICH name is the odd one out.
        Assert.Contains("'id', 'value'", ex.Message, StringComparison.Ordinal);

        // Refused BEFORE anything was written: no commit, and no orphan file left behind.
        Assert.Equal(0, table.CurrentSnapshot.Version);
        Assert.Empty(Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories));
    }

    [Fact]
    public async Task WriteDataFilesAsync_UndeclaredColumn_IsRefusedNamingIt()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, TableSchema());

        var ex = await Assert.ThrowsAsync<ArgumentException>(
            async () => await table.WriteDataFilesAsync([BatchWith("surprise")]));

        Assert.Contains("'surprise'", ex.Message, StringComparison.Ordinal);
        Assert.Empty(Directory.GetFiles(_tempDir, "*.parquet", SearchOption.AllDirectories));
    }

    /// <summary>
    /// "You have an extra column" is a poor hint for the case that actually happens. A read's metadata column
    /// is not the caller's data at all, and the message says so rather than suggesting they ALTER the table to
    /// declare <c>_metadata.row_id</c> — which would be a genuinely bad thing to talk someone into.
    /// </summary>
    [Fact]
    public async Task ForwardingAReadsMetadataColumns_IsRefusedWithTheRightAdvice()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, TableSchema(), enableRowTracking: true);
        await table.WriteAsync([Batch()]);

        var addresses = new List<long>();
        await foreach (var b in table.ReadAsync(
            new DeltaReadOptions { Metadata = DeltaRowMetadata.RowAddress }))
        {
            var a = (Int64Array)b.Column(TransientRowAddress.ColumnName);
            for (int i = 0; i < b.Length; i++)
                addresses.Add(a.GetValue(i)!.Value);
        }
        var selection = RowSelection.FromRowAddresses(addresses, table.CurrentSnapshot);

        // The host reads its rows WITH their stable identity, then forwards the batch verbatim.
        var post = new List<RecordBatch>();
        await foreach (var batch in table.ReadRowsAsync(
            selection, new DeltaRowReadOptions { Metadata = DeltaRowMetadata.RowTracking }))
        {
            post.Add(batch);
        }
        Assert.NotEmpty(post);

        var ex = await Assert.ThrowsAsync<ArgumentException>(
            async () => await table.WriteDataFilesAsync(post));

        Assert.Contains("_metadata.row_id", ex.Message, StringComparison.Ordinal);
        Assert.Contains("READ's metadata column", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("ALTER", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The packed address is not prefixed, so it has to be recognised by name rather than by prefix — it is
    /// the one metadata column a prefix test would miss.
    /// </summary>
    [Fact]
    public async Task ForwardingTheRowAddressColumn_IsRefusedWithTheRightAdvice()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, TableSchema());

        var ex = await Assert.ThrowsAsync<ArgumentException>(
            async () => await table.WriteAsync([BatchWith(TransientRowAddress.ColumnName)]));

        Assert.Contains("READ's metadata column", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The guard must never refuse what the write would have accepted. Under column mapping the rename
    /// tolerates a batch ALREADY under physical names — a batch read out of a data file and handed straight
    /// back — so the guard resolves either name, and the row survives the round trip.
    /// </summary>
    [Fact]
    public async Task PhysicalColumnNames_AreAccepted()
    {
        await using var table = await DeltaTable.CreateAsync(
            Fs, TableSchema(), columnMappingMode: ColumnMappingMode.Name);

        var physical = table.CurrentSnapshot.Schema.Fields
            .Select(f => ColumnMapping.GetPhysicalName(f, ColumnMappingMode.Name))
            .ToList();
        Assert.All(physical, p => Assert.StartsWith("col-", p));

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field(physical[0], Int64Type.Default, false))
            .Field(new Field(physical[1], StringType.Default, true))
            .Build();
        var ids = new Int64Array.Builder().Append(7L);
        var vals = new StringArray.Builder().Append("seven");
        await table.WriteAsync([new RecordBatch(schema, [ids.Build(), vals.Build()], 1)]);

        var read = new List<long>();
        await foreach (var b in table.ReadAllAsync())
        {
            var col = (Int64Array)b.Column("id");
            for (int i = 0; i < b.Length; i++)
                read.Add(col.GetValue(i)!.Value);
        }
        Assert.Equal([7L], read);
    }

    /// <summary>
    /// A batch with FEWER columns than the table is not what this guard is about, and stays legal: Delta reads
    /// an absent column as null, and a caller writing a subset is making a choice rather than a mistake.
    /// Pinned so the guard cannot quietly grow into a completeness check.
    /// </summary>
    [Fact]
    public async Task AMissingColumn_IsStillAccepted()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, TableSchema());

        var schema = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Build();
        var ids = new Int64Array.Builder().Append(1L).Append(2L);
        await table.WriteAsync([new RecordBatch(schema, [ids.Build()], 2)]);

        int rows = 0;
        await foreach (var b in table.ReadAllAsync())
            rows += b.Length;
        Assert.Equal(2, rows);
    }

    /// <summary>
    /// A buffered transaction writes its files against the PENDING (ALTERed) schema, which the committed
    /// snapshot does not know yet. The guard has to check the schema the write is actually using, or an
    /// ALTER-then-insert would be refused for declaring exactly the column it just added.
    /// </summary>
    [Fact]
    public async Task SchemaOverride_IsWhatTheGuardChecksAgainst()
    {
        await using var table = await DeltaTable.CreateAsync(Fs, TableSchema());
        await table.WriteAsync([Batch()]);

        var change = table.ComputeAddColumn(new Field("extra", Int64Type.Default, true));

        var evolved = new Apache.Arrow.Schema.Builder()
            .Field(new Field("id", Int64Type.Default, false))
            .Field(new Field("value", StringType.Default, true))
            .Field(new Field("extra", Int64Type.Default, true))
            .Build();
        var ids = new Int64Array.Builder().Append(42L);
        var vals = new StringArray.Builder().Append("forty-two");
        var extra = new Int64Array.Builder().Append(4242L);
        var batch = new RecordBatch(evolved, [ids.Build(), vals.Build(), extra.Build()], 1);

        // Against the pending schema: accepted.
        var files = await table.WriteDataFilesAsync([batch], schemaOverride: change.NewSchema);
        Assert.NotEmpty(files);

        // Against the COMMITTED schema, the very same batch is refused — the override is doing real work.
        var ex = await Assert.ThrowsAsync<ArgumentException>(
            async () => await table.WriteDataFilesAsync([batch]));
        Assert.Contains("'extra'", ex.Message, StringComparison.Ordinal);
    }
}
