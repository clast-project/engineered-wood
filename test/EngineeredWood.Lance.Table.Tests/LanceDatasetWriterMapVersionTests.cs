// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using EngineeredWood.Lance.Format;

namespace EngineeredWood.Lance.Table.Tests;

/// <summary>
/// A Map column needs a Lance v2.2 data file, and v2.1 and v2.2 use different
/// mini-block chunk formats, so a writer can't move to v2.2 once another
/// column is on disk (#997). These datasets put the Map after another column,
/// which only works when the dataset writer picks v2.2 before the first column.
/// </summary>
public class LanceDatasetWriterMapVersionTests
{
    [Fact]
    public async Task MapAfterAnotherColumn_UpdateCompactAndAppend_StayV2_2()
    {
        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-mapver-{Guid.NewGuid():N}");
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path, fileVersion: LanceVersion.V2_2))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("id", new[] { 1, 2, 3 });
                await ds.FileWriter.WriteColumnAsync("kvs", BuildMap(1));
                await ds.NewFragmentAsync();
                await ds.FileWriter.WriteInt32ColumnAsync("id", new[] { 4, 5, 6 });
                await ds.FileWriter.WriteColumnAsync("kvs", BuildMap(4));
                await ds.FinishAsync();
            }

            // Rewrites write the columns in schema order: id, then kvs.
            var (updated, _) = await LanceDatasetWriter.UpdateAsync(path,
                EngineeredWood.Expressions.Expressions.LessThan("id", LiteralValue.Of(3)),
                new Dictionary<string, Expression> { ["id"] = new LiteralExpression(LiteralValue.Of(10)) });
            Assert.Equal(2, updated);
            await LanceDatasetWriter.CompactAsync(path);

            // Append defaults to the dataset's version, so id can come first again.
            await using (var ds = await LanceDatasetWriter.AppendAsync(path))
            {
                Assert.Equal(LanceVersion.V2_2, ds.FileWriter.Version);
                await ds.FileWriter.WriteInt32ColumnAsync("id", new[] { 7, 8, 9 });
                await ds.FileWriter.WriteColumnAsync("kvs", BuildMap(7));
                await ds.FinishAsync();
            }

            var rows = new List<(int Id, string Key)>();
            await using (var table = await LanceTable.OpenAsync(path))
            {
                await foreach (var batch in table.ReadAsync())
                {
                    var ids = (Int32Array)batch.Column("id");
                    var maps = (MapArray)batch.Column("kvs");
                    var keys = (StringArray)maps.Keys;
                    for (int i = 0; i < batch.Length; i++)
                        rows.Add((ids.GetValue(i)!.Value, keys.GetString(maps.ValueOffsets[i])));
                }
            }
            // The update set ids 1 and 2 to 10; each row keeps its own map.
            Assert.Equal(
                new[] { (3, "k3"), (4, "k4"), (5, "k5"), (6, "k6"), (7, "k7"), (8, "k8"), (9, "k9"),
                        (10, "k1"), (10, "k2") },
                rows.OrderBy(r => r.Id).ThenBy(r => r.Key, StringComparer.Ordinal));

            foreach (string file in Directory.GetFiles(Path.Combine(path, "data"), "*.lance"))
            {
                await using var reader = await LanceFileReader.OpenAsync(file);
                Assert.Equal(LanceVersion.V2_2, reader.Version);
            }
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Append_WithADifferentVersion_IsRefused()
    {
        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-mapver-{Guid.NewGuid():N}");
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("id", new[] { 1, 2, 3 });
                await ds.FinishAsync();
            }

            var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
                await LanceDatasetWriter.AppendAsync(path, fileVersion: LanceVersion.V2_2));
            Assert.Equal("fileVersion", ex.ParamName);

            // The matching version is accepted.
            await using (var ds = await LanceDatasetWriter.AppendAsync(path, fileVersion: LanceVersion.V2_1))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("id", new[] { 4 });
                await ds.FinishAsync();
            }
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    // Copilot on #999: a rewrite of a dataset declared 2.1 that has a Map would
    // write 2.2 files under a 2.1 manifest, which lance refuses. Raising the
    // manifest to 2.2 would leave the other fragments' 2.1 files mixed in, so
    // the rewrite is refused before anything is written.
    [Fact]
    public async Task Rewrite_OfA2_1DatasetWithAMap_IsRefused()
    {
        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-mapver-{Guid.NewGuid():N}");
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteColumnAsync("kvs", BuildMap(1));
                await ds.FileWriter.WriteInt32ColumnAsync("id", new[] { 1, 2, 3 });
                await ds.FinishAsync();
            }
            LanceBasePathsTests.RewriteManifests(path, m => m.DataFormat.Version = "2.1");
            // Delete writes only a deletion file, so it is allowed, and it gives Compact work to do.
            await LanceDatasetWriter.DeleteRowsAsync(path,
                new Dictionary<ulong, IReadOnlyList<int>> { [0] = new[] { 0 } });
            string[] versions = Directory.GetFiles(Path.Combine(path, "_versions"));

            var ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
                await LanceDatasetWriter.UpdateAsync(path,
                    EngineeredWood.Expressions.Expressions.LessThan("id", LiteralValue.Of(3)),
                    new Dictionary<string, Expression> { ["id"] = new LiteralExpression(LiteralValue.Of(10)) }));
            Assert.Contains("Map", ex.Message);
            await Assert.ThrowsAsync<NotSupportedException>(async () => await LanceDatasetWriter.CompactAsync(path));
            Assert.Equal(versions, Directory.GetFiles(Path.Combine(path, "_versions")));
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    // Append pins the dataset's version, so a Map written first can't move the
    // new file to v2.2 under a 2.1 manifest. (The schema check would refuse it
    // at commit anyway, but only after writing the file.)
    [Fact]
    public async Task Append_ToA2_1Dataset_CantMoveToV2_2()
    {
        string path = NewDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("id", new[] { 1, 2, 3 });
                await ds.FinishAsync();
            }

            await using var append = await LanceDatasetWriter.AppendAsync(path);
            await Assert.ThrowsAsync<NotSupportedException>(
                () => append.FileWriter.WriteColumnAsync("kvs", BuildMap(1)));
            Assert.Equal(LanceVersion.V2_1, append.FileWriter.Version);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    // lance infers a missing data_format from the data files' own versions
    // (lance-table Fragment::try_infer_version), and so does EW.
    [Fact]
    public async Task MissingDataFormat_IsInferredFromTheDataFiles()
    {
        string path = NewDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path, fileVersion: LanceVersion.V2_2))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("id", new[] { 1, 2, 3 });
                await ds.FinishAsync();
            }
            LanceBasePathsTests.RewriteManifests(path, m => m.DataFormat = null);

            // Inferred 2.2, so Append starts there (not at the 2.1 default) and the
            // appended files match the existing ones.
            await using (var ds = await LanceDatasetWriter.AppendAsync(path))
            {
                Assert.Equal(LanceVersion.V2_2, ds.FileWriter.Version);
                await ds.FileWriter.WriteInt32ColumnAsync("id", new[] { 4 });
                await ds.FinishAsync();
            }
            var (updated, _) = await LanceDatasetWriter.UpdateAsync(path,
                EngineeredWood.Expressions.Expressions.LessThan("id", LiteralValue.Of(2)),
                new Dictionary<string, Expression> { ["id"] = new LiteralExpression(LiteralValue.Of(10)) });
            Assert.Equal(1, updated);

            foreach (string file in Directory.GetFiles(Path.Combine(path, "data"), "*.lance"))
            {
                await using var reader = await LanceFileReader.OpenAsync(file);
                Assert.Equal(LanceVersion.V2_2, reader.Version);
            }
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    private static string NewDatasetPath() =>
        Path.Combine(Path.GetTempPath(), $"ew-lance-mapver-{Guid.NewGuid():N}");

    /// <summary>Three one-entry rows: {"k{first}": first}, {"k{first+1}": first+1}, ...</summary>
    private static MapArray BuildMap(int first)
    {
        var keys = new StringArray.Builder();
        var values = new Int32Array.Builder();
        for (int i = 0; i < 3; i++)
        {
            keys.Append($"k{first + i}");
            values.Append(first + i);
        }
        var entriesType = new StructType(new[]
        {
            new Field("key", StringType.Default, nullable: false),
            new Field("value", Int32Type.Default, nullable: true),
        });
        var entries = new StructArray(new ArrayData(
            entriesType, length: 3, nullCount: 0, offset: 0,
            new[] { ArrowBuffer.Empty },
            new[] { keys.Build().Data, values.Build().Data }));

        var offsets = new byte[4 * sizeof(int)];
        for (int i = 0; i <= 3; i++)
            BinaryPrimitives.WriteInt32LittleEndian(offsets.AsSpan(i * 4, 4), i);

        return new MapArray(new ArrayData(
            new MapType(entriesType), length: 3, nullCount: 0, offset: 0,
            new[] { ArrowBuffer.Empty, new ArrowBuffer(offsets) },
            new[] { entries.Data }));
    }
}
