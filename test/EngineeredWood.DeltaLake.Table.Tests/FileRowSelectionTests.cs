// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.IO.Local;

namespace EngineeredWood.DeltaLake.Table.Tests;

/// <summary>
/// Why a DML boundary should be keyed by <see cref="FileRowSelection"/> rather than by a file's PATH-SORTED
/// ORDINAL. The argument is NOT that ordinals are a bad address — <c>TransientRowAddress</c> makes them a
/// documented first-class one, and a host whose own row id is a single integer has to mint and decode one
/// regardless. It is narrower:
/// </summary>
/// <remarks>
/// <para>An ordinal is a fine ADDRESS for a host to mint and decode. It is the wrong KEY for a library's DML
/// boundary to accept, because at that boundary a stale address is indistinguishable from a fresh one.</para>
/// <para>Every fixture here has THREE files on purpose. With one file the ordinal is always 0, so a
/// single-file fixture cannot fail these tests at all.</para>
/// </remarks>
public class FileRowSelectionTests : IDisposable
{
    private readonly string _tempDir;

    public FileRowSelectionTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"delta_selection_{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); } catch { }
        }
    }

    private static Apache.Arrow.Schema IdSchema { get; } = new Apache.Arrow.Schema.Builder()
        .Field(new Field("id", Int64Type.Default, false))
        .Build();

    private static RecordBatch Batch(long first, int count)
    {
        var b = new Int64Array.Builder();
        for (int i = 0; i < count; i++)
            b.Append(first + i);
        return new RecordBatch(IdSchema, new IArrowArray[] { b.Build() }, count);
    }

    private ValueTask<DeltaTable> OpenAsync() => DeltaTable.OpenAsync(new LocalTableFileSystem(_tempDir));

    /// <summary>THREE files, one row each — so an ordinal genuinely distinguishes them.</summary>
    private async Task<DeltaTable> CreateThreeFilesAsync()
    {
        var table = await DeltaTable.CreateAsync(
            new LocalTableFileSystem(_tempDir), IdSchema, enableDeletionVectors: true);
        await table.WriteAsync([Batch(10, 1)]);
        await table.WriteAsync([Batch(20, 1)]);
        await table.WriteAsync([Batch(30, 1)]);
        return table;
    }

    private async Task<List<long>> ReadIdsAsync()
    {
        await using var table = await OpenAsync();
        var ids = new List<long>();
        await foreach (var batch in table.ReadAllAsync())
        {
            var col = (Int64Array)batch.Column("id");
            for (int i = 0; i < batch.Length; i++)
                ids.Add(col.GetValue(i)!.Value);
        }
        ids.Sort();
        return ids;
    }

    /// <summary>Path-sorted active files of the current snapshot — the ordering an ordinal indexes.</summary>
    private static List<string> PathSorted(DeltaTable t) =>
        t.CurrentSnapshot.ActiveFiles.Values.Select(a => a.Path)
            .OrderBy(p => p, StringComparer.Ordinal).ToList();

    private static FileRowSelection Select(string path, params long[] positions) =>
        new(new Dictionary<string, IReadOnlyCollection<long>> { [path] = positions });

    /// <summary>
    /// THE CASE THAT DECIDES IT. A concurrent commit that REMOVES an earlier file renumbers the path-sorted
    /// set, so a captured ordinal still EXISTS — nothing can reject it — and names a DIFFERENT file. The
    /// delete then succeeds and removes a row nobody selected: silent WRONG DATA, strictly worse than a
    /// silent no-op, and invisible to a range check or to any type that owns the packing.
    /// </summary>
    [Fact]
    public async Task OrdinalKeyed_AfterAConcurrentRemoveRenumbersTheSet_DeletesTheWRONGRow()
    {
        await using var setup = await CreateThreeFilesAsync();
        var before = PathSorted(setup);
        Assert.Equal(3, before.Count);

        // We intend the row in ordinal 1. Aim at 1, not 2: removing ordinal 0 shifts old ordinal 2 INTO
        // slot 1, so a captured 2 would go out of range (and be skipped) while a captured 1 hits the wrong
        // file — which is the case worth demonstrating.
        long intendedId = await IdInFileAsync(before[1]);
        long addressOfIntended = TransientRowAddress.Pack(1, 0);

        // A concurrent writer removes the FIRST file. Copy-on-write, not a deletion vector: a DV delete is a
        // remove+add of the SAME path, so the file stays active and nothing renumbers. Deleting the only row
        // of a one-row file by rewrite drops the file from the active set — ordinals 1,2 become 0,1.
        await using (var other = await OpenAsync())
        {
            await other.DeleteByRowIdsAsync([TransientRowAddress.Pack(0, 0)]);
        }
        await using (var check = await OpenAsync())
        {
            Assert.Equal(2, check.CurrentSnapshot.ActiveFiles.Count);   // the premise, asserted
        }

        // The captured ordinal is still in range, so the ordinal-keyed form accepts it and deletes.
        await using (var mine = await OpenAsync())
        {
            var (deleted, _) = await mine.DeleteByRowIdsViaVectorsAsync([addressOfIntended]);
            Assert.Equal(1, deleted);   // it "worked"
        }

        // But the row it deleted is NOT the one we selected.
        var remaining = await ReadIdsAsync();
        Assert.Contains(intendedId, remaining);            // the row we selected is STILL THERE...
        Assert.Single(remaining);                          // ...and a row nobody selected is gone instead
    }

    /// <summary>
    /// The same capture keyed by PATH fails loudly instead. The message names the file, so a caller learns
    /// that its selection is stale rather than that its delete did nothing.
    /// </summary>
    [Fact]
    public async Task PathKeyed_AfterTheSameConcurrentRemove_ThrowsInsteadOfActing()
    {
        await using var setup = await CreateThreeFilesAsync();
        var before = PathSorted(setup);
        string capturedPath = before[0];

        // A concurrent copy-on-write delete removes that exact file from the active set.
        await using (var other = await OpenAsync())
        {
            await other.DeleteByRowIdsAsync([TransientRowAddress.Pack(0, 0)]);
        }

        await using var mine = await OpenAsync();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await mine.DeleteBySelectionViaVectorsAsync(Select(capturedPath, 0)));
        Assert.Contains(capturedPath, ex.Message);
        Assert.Contains("not active", ex.Message);
    }

    /// <summary>
    /// The silent-loss case, which is the milder half of the same problem: identifiers resolved against a
    /// snapshot an overwrite shrank leave the ordinal OUT of range, and the ordinal form then reports zero
    /// rows deleted with no error at all.
    /// </summary>
    [Fact]
    public async Task OrdinalKeyed_AgainstAShrunkSnapshot_SilentlyDeletesNothing()
    {
        await using var setup = await CreateThreeFilesAsync();
        long addressInThirdFile = TransientRowAddress.Pack(2, 0);

        // Overwrite down to a single file: ordinal 2 no longer exists.
        await using (var other = await OpenAsync())
        {
            await other.WriteAsync([Batch(99, 1)], DeltaWriteMode.Overwrite);
        }

        await using (var mine = await OpenAsync())
        {
            var (deleted, _) = await mine.DeleteByRowIdsViaVectorsAsync([addressInThirdFile]);
            Assert.Equal(0, deleted);   // no error, no rows — the caller cannot tell it was stale
        }

        Assert.Equal([99L], await ReadIdsAsync());
    }

    /// <summary>A selection naming a path the table never had is a caller error, reported as one.</summary>
    [Fact]
    public async Task PathKeyed_UnknownPath_Throws()
    {
        await using var table = await CreateThreeFilesAsync();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            async () => await table.DeleteBySelectionViaVectorsAsync(Select("part-never-existed.parquet", 0)));
        Assert.Contains("part-never-existed.parquet", ex.Message);
    }

    /// <summary>Reads the single row of one specific data file, to identify rows by content not by ordinal.</summary>
    private async Task<long> IdInFileAsync(string path)
    {
        await using var table = await OpenAsync();
        var target = table.CurrentSnapshot.ActiveFiles.Values.Single(a => a.Path == path);
        long? found = null;
        await foreach (var batch in table.ReadAllWithRowIdsAsync(null, null))
        {
            var ids = (Int64Array)batch.Column("id");
            var addr = (Int64Array)batch.Column(TransientRowAddress.ColumnName);
            var ordered = PathSorted(table);
            for (int i = 0; i < batch.Length; i++)
            {
                int ord = TransientRowAddress.FileOrdinal(addr.GetValue(i)!.Value);
                if (ordered[ord] == target.Path)
                    found = ids.GetValue(i)!.Value;
            }
        }
        Assert.NotNull(found);
        return found!.Value;
    }
}
