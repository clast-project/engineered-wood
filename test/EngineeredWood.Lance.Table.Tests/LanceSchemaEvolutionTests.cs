// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using System.Globalization;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
using static EngineeredWood.Expressions.Expressions;

namespace EngineeredWood.Lance.Table.Tests;

/// <summary>
/// Datasets whose data files do not hold the manifest's fields in the manifest's order (#995). drop_columns and
/// add_columns change only the manifest, so a reader must find each field's column through the data file's
/// <c>fields</c>/<c>column_indices</c>, never by position. Fixtures come from <c>generate_test_data.py</c>
/// (pylance 14.0.3); the expected rows are pylance's, and the writer tests check pylance reads EW's result the same
/// way when pylance is installed.
/// </summary>
public class LanceSchemaEvolutionTests
{
    private const string FlatRows =
        "[{\"b\":10,\"c\":100},{\"b\":20,\"c\":200},{\"b\":30,\"c\":300}]";

    [Theory]
    [InlineData("dropped_column_v21")]
    [InlineData("dropped_column_v20")]
    public async Task DroppedColumn_ReadsTheRemainingColumns(string fixture)
    {
        // The file still holds a, b, c as fields [0, 1, 2]; by position b would read a's values.
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve(fixture));
        Assert.Equal(new[] { "b", "c" }, table.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(FlatRows, await ReadJsonAsync(table));
        Assert.Equal("[{\"c\":100},{\"c\":200},{\"c\":300}]", await ReadJsonAsync(table, ["c"]));
        Assert.Equal("[{\"c\":200},{\"c\":300}]",
            await ReadJsonAsync(table, ["c"], GreaterThan("b", LiteralValue.Of(15L))));
    }

    [Fact]
    public async Task DroppedNestedField_V21_ReadsTheRemainingLeaves()
    {
        // Schema [s{y}, l, c]; the 2.1 file lists leaves [a, s.x, s.y, l.item, c] as columns 0-4, so s{y} is
        // column 2 alone.
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("dropped_nested_v21"));
        Assert.Equal(
            "[{\"s\":{\"y\":\"p\"},\"l\":[1],\"c\":100},{\"s\":{\"y\":\"q\"},\"l\":[2,3],\"c\":200},"
            + "{\"s\":{\"y\":\"r\"},\"l\":[],\"c\":300}]",
            await ReadJsonAsync(table));
    }

    [Fact]
    public async Task DroppedNestedField_V20_RefusesTheStructButReadsTheRest()
    {
        // In 2.0 s, s.x and s.y each have a column (1, 2, 3); s{y} is columns 1 and 3, which the decoder cannot
        // read as one field.
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("dropped_nested_v20"));
        Assert.Equal("[{\"l\":[1],\"c\":100},{\"l\":[2,3],\"c\":200},{\"l\":[],\"c\":300}]",
            await ReadJsonAsync(table, ["l", "c"]));
        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => ReadJsonAsync(table, ["s"]));
        Assert.Contains("contiguous", ex.Message);
    }

    [Fact]
    public async Task ColumnAddedWithoutData_IsRefusedButOtherColumnsRead()
    {
        // add_columns with a schema writes no data: n is in no data file, and pylance reads it as nulls.
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("added_null_column_v21"));
        Assert.Equal("[{\"a\":1,\"c\":100},{\"a\":2,\"c\":200},{\"a\":3,\"c\":300}]",
            await ReadJsonAsync(table, ["a", "c"]));
        var ex = await Assert.ThrowsAsync<NotSupportedException>(() => ReadJsonAsync(table));
        Assert.Contains("'n'", ex.Message);
        Assert.Contains("add_columns", ex.Message);
    }

    [Fact]
    public async Task LegacyEwStructLayout_StillReads()
    {
        // Written by EW before #995, which listed a 2.1 file's struct parent with a column of its own (fields
        // [0, 1, 2, 3, 4] as columns [0, 1, 2, 3, 4]). lance-rs refuses that; EW reads it through the file's schema.
        await using var table = await LanceTable.OpenAsync(TestDataPath.Resolve("ew_legacy_struct_v21"));
        Assert.Equal(
            "[{\"a\":10,\"rec\":{\"id\":1,\"name\":\"p\"},\"c\":100},{\"a\":20,\"rec\":{\"id\":2,\"name\":\"q\"},\"c\":200},"
            + "{\"a\":30,\"rec\":{\"id\":3,\"name\":\"r\"},\"c\":300}]",
            await ReadJsonAsync(table));
    }

    [Fact]
    public async Task ManifestOrderDiffersFromFileOrder_ReadsByFieldId()
    {
        // The file holds v (field 0) then id (field 1); the manifest lists id first.
        string path = await MakeReorderedDatasetAsync();
        try
        {
            await using var table = await LanceTable.OpenAsync(path);
            Assert.Equal(new[] { "id", "v" }, table.Schema.FieldsList.Select(f => f.Name));
            Assert.Equal(ReorderedRows, await ReadJsonAsync(table));
            await AssertPylanceReadsAsync(path, ReorderedRows);
        }
        finally
        {
            DeleteDirectory(path);
        }
    }

    [Fact]
    public async Task ManifestOrderDiffersFromFileOrder_DeleteUpdateCompactUseFieldIds()
    {
        string path = await MakeReorderedDatasetAsync();
        try
        {
            // id is [1, 2, 3] and v is [10, 20, 30]; by position, "id = 2" would test v and match nothing.
            await LanceDatasetWriter.DeleteAsync(path, Equal("id", LiteralValue.Of(2L)));
            var (updated, _) = await LanceDatasetWriter.UpdateAsync(path, Equal("id", LiteralValue.Of(3L)),
                new Dictionary<string, Expression> { ["v"] = new LiteralExpression(LiteralValue.Of(33L)) });
            Assert.Equal(1L, updated);
            await LanceDatasetWriter.CompactAsync(path);

            // Compact moves the first fragment's survivor after the updated row's fragment.
            const string expected = "[{\"id\":3,\"v\":33},{\"id\":1,\"v\":10}]";
            await using (var table = await LanceTable.OpenAsync(path))
                Assert.Equal(expected, await ReadJsonAsync(table));
            await AssertPylanceReadsAsync(path, expected);
        }
        finally
        {
            DeleteDirectory(path);
        }
    }

    [Fact]
    public async Task DroppedColumn_DeleteUpdateCompactUseFieldIds()
    {
        string path = CopyFixture("dropped_column_v21");
        try
        {
            // By position, "b = 20" would test the dropped a column and delete nothing.
            await LanceDatasetWriter.DeleteAsync(path, Equal("b", LiteralValue.Of(20L)));
            var (updated, _) = await LanceDatasetWriter.UpdateAsync(path, Equal("c", LiteralValue.Of(300L)),
                new Dictionary<string, Expression> { ["b"] = new LiteralExpression(LiteralValue.Of(99L)) });
            Assert.Equal(1L, updated);

            const string expected = "[{\"b\":10,\"c\":100},{\"b\":99,\"c\":300}]";
            await using (var table = await LanceTable.OpenAsync(path))
                Assert.Equal(expected, await ReadJsonAsync(table));
            await AssertPylanceReadsAsync(path, expected);

            // Compact rewrites the original fragment's survivors into a new file (with field ids [1, 2]), last.
            await LanceDatasetWriter.CompactAsync(path);
            const string compacted = "[{\"b\":99,\"c\":300},{\"b\":10,\"c\":100}]";
            await using (var table = await LanceTable.OpenAsync(path))
                Assert.Equal(compacted, await ReadJsonAsync(table));
            await AssertPylanceReadsAsync(path, compacted);
        }
        finally
        {
            DeleteDirectory(path);
        }
    }

    // EW writes only 2.1 and 2.2 files, which lance refuses in a 2.0 dataset (#996), so it writes no data files
    // there until #1000. Delete writes only a deletion file, and still uses the field ids.
    [Fact]
    public async Task DroppedColumn_V20_DeleteWorks_DataFileWritesAreRefused()
    {
        string path = CopyFixture("dropped_column_v20");
        try
        {
            await LanceDatasetWriter.DeleteAsync(path, Equal("b", LiteralValue.Of(20L)));
            const string expected = "[{\"b\":10,\"c\":100},{\"b\":30,\"c\":300}]";
            await AssertPylanceReadsAsync(path, expected);
            string[] versions = Directory.GetFiles(Path.Combine(path, "_versions"));

            var update = await Assert.ThrowsAsync<NotSupportedException>(async () =>
                await LanceDatasetWriter.UpdateAsync(path, Equal("c", LiteralValue.Of(300L)),
                    new Dictionary<string, Expression> { ["b"] = new LiteralExpression(LiteralValue.Of(99L)) }));
            Assert.Contains("2.0 dataset", update.Message);
            await Assert.ThrowsAsync<NotSupportedException>(async () => await LanceDatasetWriter.CompactAsync(path));
            await Assert.ThrowsAsync<NotSupportedException>(async () => await LanceDatasetWriter.AppendAsync(path));

            // Nothing was committed, and pylance still reads it.
            Assert.Equal(versions, Directory.GetFiles(Path.Combine(path, "_versions")));
            await using (var table = await LanceTable.OpenAsync(path))
                Assert.Equal(expected, await ReadJsonAsync(table));
            await AssertPylanceReadsAsync(path, expected);
        }
        finally
        {
            DeleteDirectory(path);
        }
    }

    // Without data_format, lance infers the version from the data files, so a 2.0 dataset is still refused.
    [Fact]
    public async Task DroppedColumn_V20_WithoutDataFormat_IsStillRefused()
    {
        string path = CopyFixture("dropped_column_v20");
        try
        {
            LanceBasePathsTests.RewriteManifests(path, m => m.DataFormat = null);
            var ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
                await LanceDatasetWriter.AppendAsync(path));
            Assert.Contains("2.0 dataset", ex.Message);

            // Delete is still allowed, and records the files' own version, not a guessed 2.1.
            await LanceDatasetWriter.DeleteAsync(path, Equal("b", LiteralValue.Of(20L)));
            var manifest = await LanceDatasetWriterMapVersionTests.LatestManifestAsync(path);
            Assert.Equal("2.0", manifest.DataFormat.Version);
            await AssertPylanceReadsAsync(path, "[{\"b\":10,\"c\":100},{\"b\":30,\"c\":300}]");
        }
        finally
        {
            DeleteDirectory(path);
        }
    }

    // lance reads a DataFile's (0,3) and (2,0) both as 2.0, and refuses a manifest
    // data_format of "0.3", so EW must neither call them a mix nor record "0.3".
    [Fact]
    public async Task DroppedColumn_V20_LegacyFileNumbers_AreTheSameVersion()
    {
        string path = CopyFixture("dropped_column_v20");
        try
        {
            int files = 0;
            LanceBasePathsTests.RewriteManifests(path, m =>
            {
                m.DataFormat = null;
                int i = 0;
                foreach (var fragment in m.Fragments)
                    foreach (var file in fragment.Files)
                        if (i++ % 2 == 0) { file.FileMajorVersion = 0; file.FileMinorVersion = 3; }
                files = i;
            });
            Assert.True(files > 0);

            var ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
                await LanceDatasetWriter.AppendAsync(path));
            Assert.Contains("2.0 dataset", ex.Message);

            await LanceDatasetWriter.DeleteAsync(path, Equal("b", LiteralValue.Of(20L)));
            var manifest = await LanceDatasetWriterMapVersionTests.LatestManifestAsync(path);
            Assert.Equal("2.0", manifest.DataFormat.Version);
            await AssertPylanceReadsAsync(path, "[{\"b\":10,\"c\":100},{\"b\":30,\"c\":300}]");
        }
        finally
        {
            DeleteDirectory(path);
        }
    }

    [Fact]
    public async Task DroppedNestedField_V21_UpdateRewritesWithTheManifestsIds()
    {
        string path = CopyFixture("dropped_nested_v21");
        try
        {
            var (updated, _) = await LanceDatasetWriter.UpdateAsync(path, Equal("c", LiteralValue.Of(200L)),
                new Dictionary<string, Expression> { ["c"] = new LiteralExpression(LiteralValue.Of(222L)) });
            Assert.Equal(1L, updated);

            const string expected =
                "[{\"s\":{\"y\":\"p\"},\"l\":[1],\"c\":100},{\"s\":{\"y\":\"r\"},\"l\":[],\"c\":300},"
                + "{\"s\":{\"y\":\"q\"},\"l\":[2,3],\"c\":222}]";
            await using (var table = await LanceTable.OpenAsync(path))
                Assert.Equal(expected, await ReadJsonAsync(table));
            await AssertPylanceReadsAsync(path, expected);
        }
        finally
        {
            DeleteDirectory(path);
        }
    }

    [Fact]
    public async Task StructDataset_ListsOnlyLeavesAndIsReadableByPylance()
    {
        // Before #995 EW gave the struct parent a column of its own, which lance-rs refuses in a 2.1+ file
        // ("non-leaf fields should have column_index=-1").
        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-struct-{Guid.NewGuid():N}.lance");
        try
        {
            var type = new StructType(new[]
            {
                new Field("id", Int64Type.Default, true), new Field("name", StringType.Default, true),
            });
            var rec = new StructArray(type, 3,
                new IArrowArray[]
                {
                    new Int64Array.Builder().Append(1).Append(2).Append(3).Build(),
                    new StringArray.Builder().Append("p").Append("q").Append("r").Build(),
                },
                ArrowBuffer.Empty, 0);
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt64ColumnAsync("a", new long[] { 10, 20, 30 });
                await ds.FileWriter.WriteColumnAsync("rec", rec);
                await ds.FileWriter.WriteInt64ColumnAsync("c", new long[] { 100, 200, 300 });
                await ds.FinishAsync();
            }

            var file = ReadOnlyManifest(path).Fragments.Single().Files.Single();
            Assert.Equal(new[] { 0, 2, 3, 4 }, file.Fields);
            Assert.Equal(new[] { 0, 1, 2, 3 }, file.ColumnIndices);

            const string expected =
                "[{\"a\":10,\"rec\":{\"id\":1,\"name\":\"p\"},\"c\":100},{\"a\":20,\"rec\":{\"id\":2,\"name\":\"q\"},\"c\":200},"
                + "{\"a\":30,\"rec\":{\"id\":3,\"name\":\"r\"},\"c\":300}]";
            await using (var table = await LanceTable.OpenAsync(path))
                Assert.Equal(expected, await ReadJsonAsync(table));
            await AssertPylanceReadsAsync(path, expected);
        }
        finally
        {
            DeleteDirectory(path);
        }
    }

    [Fact]
    public async Task DuplicateColumnIndices_AreRefused()
    {
        // table.proto forbids two fields sharing a column; mapped through, b and c would both read column 1.
        string path = CopyFixture("dropped_column_v21");
        try
        {
            LanceBasePathsTests.RewriteManifests(path, m =>
            {
                foreach (var file in m.Fragments.SelectMany(f => f.Files))
                {
                    file.ColumnIndices.Clear();
                    file.ColumnIndices.AddRange(new[] { 0, 1, 1 });
                }
            });

            await using (var table = await LanceTable.OpenAsync(path))
            {
                var ex = await Assert.ThrowsAsync<LanceTableFormatException>(() => ReadJsonAsync(table));
                Assert.Contains("more than one field to column 1", ex.Message);
            }
            await Assert.ThrowsAsync<LanceTableFormatException>(async () =>
                await LanceDatasetWriter.DeleteAsync(path, Equal("b", LiteralValue.Of(20L))));
        }
        finally
        {
            DeleteDirectory(path);
        }
    }

    [Fact]
    public async Task LegacyEwStructLayout_ReadsAndRewritesUnderRenamedFields()
    {
        // A rename changes only the manifest, so the legacy file's own schema still says rec.id and c.
        string path = CopyFixture("ew_legacy_struct_v21");
        try
        {
            LanceBasePathsTests.RewriteManifests(path, m =>
            {
                foreach (var f in m.Fields)
                {
                    if (f.Name == "id") f.Name = "ident";
                    else if (f.Name == "c") f.Name = "c2";
                }
            });

            const string renamed =
                "[{\"a\":10,\"rec\":{\"ident\":1,\"name\":\"p\"},\"c2\":100},"
                + "{\"a\":20,\"rec\":{\"ident\":2,\"name\":\"q\"},\"c2\":200},"
                + "{\"a\":30,\"rec\":{\"ident\":3,\"name\":\"r\"},\"c2\":300}]";
            await using (var table = await LanceTable.OpenAsync(path))
                Assert.Equal(renamed, await ReadJsonAsync(table));

            // The rewrite writes the struct as read; its child names must match the manifest's.
            var (updated, _) = await LanceDatasetWriter.UpdateAsync(path, Equal("a", LiteralValue.Of(20L)),
                new Dictionary<string, Expression> { ["c2"] = new LiteralExpression(LiteralValue.Of(222L)) });
            Assert.Equal(1L, updated);
            await using (var table = await LanceTable.OpenAsync(path))
                Assert.Equal(
                    "[{\"a\":10,\"rec\":{\"ident\":1,\"name\":\"p\"},\"c2\":100},"
                    + "{\"a\":30,\"rec\":{\"ident\":3,\"name\":\"r\"},\"c2\":300},"
                    + "{\"a\":20,\"rec\":{\"ident\":2,\"name\":\"q\"},\"c2\":222}]",
                    await ReadJsonAsync(table));
        }
        finally
        {
            DeleteDirectory(path);
        }
    }

    [Fact]
    public async Task ListChildNamedElement_SurvivesARewrite()
    {
        // pylance list<element: int64>; EW's file writer used to name every list child "item".
        string path = CopyFixture("list_element_v21");
        try
        {
            var (updated, _) = await LanceDatasetWriter.UpdateAsync(path, Equal("k", LiteralValue.Of(2L)),
                new Dictionary<string, Expression> { ["k"] = new LiteralExpression(LiteralValue.Of(22L)) });
            Assert.Equal(1L, updated);

            const string expected = "[{\"k\":1,\"l\":[1]},{\"k\":3,\"l\":[]},{\"k\":22,\"l\":[2,3]}]";
            await using (var table = await LanceTable.OpenAsync(path))
            {
                Assert.Equal("element",
                    ((Apache.Arrow.Types.ListType)table.Schema.GetFieldByName("l").DataType).ValueField.Name);
                Assert.Equal(expected, await ReadJsonAsync(table));
            }
            await AssertPylanceReadsAsync(path, expected);
        }
        finally
        {
            DeleteDirectory(path);
        }
    }

    private const string ReorderedRows = "[{\"id\":1,\"v\":10},{\"id\":2,\"v\":20},{\"id\":3,\"v\":30}]";

    /// <summary>A dataset whose file holds v (field 0) then id (field 1), and whose manifest lists id first.</summary>
    private static async Task<string> MakeReorderedDatasetAsync()
    {
        string path = Path.Combine(Path.GetTempPath(), $"ew-lance-reordered-{Guid.NewGuid():N}.lance");
        await using (var ds = await LanceDatasetWriter.CreateAsync(path))
        {
            await ds.FileWriter.WriteInt64ColumnAsync("v", new long[] { 10, 20, 30 });
            await ds.FileWriter.WriteInt64ColumnAsync("id", new long[] { 1, 2, 3 });
            await ds.FinishAsync();
        }
        LanceBasePathsTests.RewriteManifests(path, m =>
        {
            var fields = m.Fields.Reverse().ToList();
            m.Fields.Clear();
            m.Fields.AddRange(fields);
        });
        return path;
    }

    /// <summary>The manifest of a dataset with a single version.</summary>
    private static Proto.Manifest ReadOnlyManifest(string dataset)
    {
        Proto.Manifest? result = null;
        LanceBasePathsTests.RewriteManifests(dataset, m => result = m.Clone());
        return result!;
    }

    private static string CopyFixture(string fixture)
    {
        string source = TestDataPath.Resolve(fixture);
        string target = Path.Combine(Path.GetTempPath(), $"ew-lance-{fixture}-{Guid.NewGuid():N}");
        foreach (string file in Directory.GetFiles(source, "*", SearchOption.AllDirectories))
        {
            string dest = Path.Combine(target, file.Substring(source.Length + 1));
            Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
            File.Copy(file, dest);
        }
        return target;
    }

    private static void DeleteDirectory(string path)
    {
        if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
    }

    /// <summary>Reads the table and renders its rows as compact JSON, the way Python's
    /// <c>json.dumps(t.to_pylist(), separators=(",", ":"))</c> renders pylance's.</summary>
    private static async Task<string> ReadJsonAsync(
        LanceTable table, IReadOnlyList<string>? columns = null, Predicate? filter = null)
    {
        var sb = new StringBuilder("[");
        bool first = true;
        await foreach (var batch in table.ReadAsync(columns, filter))
        {
            for (int row = 0; row < batch.Length; row++)
            {
                if (!first) sb.Append(',');
                first = false;
                sb.Append('{');
                for (int c = 0; c < batch.ColumnCount; c++)
                {
                    if (c > 0) sb.Append(',');
                    sb.Append('"').Append(batch.Schema.FieldsList[c].Name).Append("\":");
                    AppendValue(sb, batch.Column(c), row);
                }
                sb.Append('}');
            }
        }
        return sb.Append(']').ToString();
    }

    private static void AppendValue(StringBuilder sb, IArrowArray array, int index)
    {
        if (array.IsNull(index)) { sb.Append("null"); return; }
        switch (array)
        {
            case Int64Array a: sb.Append(a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture)); break;
            case Int32Array a: sb.Append(a.GetValue(index)!.Value.ToString(CultureInfo.InvariantCulture)); break;
            case StringArray a: sb.Append('"').Append(a.GetString(index)).Append('"'); break;
            case StructArray a:
                var type = (StructType)a.Data.DataType;
                sb.Append('{');
                for (int f = 0; f < type.Fields.Count; f++)
                {
                    if (f > 0) sb.Append(',');
                    sb.Append('"').Append(type.Fields[f].Name).Append("\":");
                    AppendValue(sb, a.Fields[f], index);
                }
                sb.Append('}');
                break;
            case ListArray a:
                var values = a.GetSlicedValues(index);
                sb.Append('[');
                for (int v = 0; v < values.Length; v++)
                {
                    if (v > 0) sb.Append(',');
                    AppendValue(sb, values, v);
                }
                sb.Append(']');
                break;
            default:
                throw new NotSupportedException($"No JSON rendering for {array.GetType().Name}.");
        }
    }

    /// <summary>When pylance is installed, checks that it reads <paramref name="path"/> as
    /// <paramref name="expectedJson"/>; otherwise does nothing (CI has no pylance).</summary>
    private static async Task AssertPylanceReadsAsync(string path, string expectedJson)
    {
        string script = "import sys, json, lance\n"
            + $"t = lance.dataset(r'{path}').to_table()\n"
            + "sys.stdout.write(json.dumps(t.to_pylist(), separators=(',', ':')))\n";
        var psi = new ProcessStartInfo("python", "-c \"" + script.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        Process? proc;
        try { proc = Process.Start(psi); }
        catch (System.ComponentModel.Win32Exception) { return; } // no python
        if (proc is null) return;
        using (proc)
        {
            string stdout = await proc.StandardOutput.ReadToEndAsync();
            string stderr = await proc.StandardError.ReadToEndAsync();
            proc.WaitForExit();
            if (proc.ExitCode != 0 && stderr.Contains("No module named 'lance'")) return;
            Assert.True(proc.ExitCode == 0, $"pylance exited {proc.ExitCode}; stderr: {stderr}");
            Assert.Equal(expectedJson, stdout);
        }
    }
}
