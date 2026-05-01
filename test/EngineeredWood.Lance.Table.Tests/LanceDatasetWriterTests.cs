// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Lance;

namespace EngineeredWood.Lance.Table.Tests;

/// <summary>
/// Tests for <see cref="LanceDatasetWriter"/>. Two angles:
/// (1) self round-trip — write a dataset then open it via
/// <see cref="LanceTable"/> and confirm rows + schema match;
/// (2) cross-validation — invoke pylance's <c>lance.dataset()</c> on
/// the written directory and confirm it sees the same data.
/// </summary>
public class LanceDatasetWriterTests
{
    private static string MakeTempDatasetPath() =>
        Path.Combine(Path.GetTempPath(), $"ew-lance-ds-{Guid.NewGuid():N}.lance");

    [Fact]
    public async Task SingleColumn_RoundTrip_ViaLanceTable()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 10, 20, 30, 40, 50 });
                await ds.FinishAsync();
            }

            await using var table = await LanceTable.OpenAsync(path);
            Assert.Equal(5L, table.NumberOfRows);
            Assert.Equal(1, table.NumberOfFragments);
            Assert.Single(table.Schema.FieldsList);
            Assert.Equal("x", table.Schema.FieldsList[0].Name);
            Assert.IsType<Int32Type>(table.Schema.FieldsList[0].DataType);

            int batches = 0, rows = 0;
            await foreach (var batch in table.ReadAsync())
            {
                batches++;
                rows += batch.Length;
                var col = (Int32Array)batch.Column(0);
                Assert.Equal(new int?[] { 10, 20, 30, 40, 50 }, col.ToArray());
            }
            Assert.Equal(1, batches);
            Assert.Equal(5, rows);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task MultiColumn_RoundTrip_ViaLanceTable()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("id", new[] { 1, 2, 3 });
                var sb = new StringArray.Builder();
                sb.Append("alpha"); sb.AppendNull(); sb.Append("gamma");
                await ds.FileWriter.WriteColumnAsync("name", sb.Build());
                await ds.FinishAsync();
            }

            await using var table = await LanceTable.OpenAsync(path);
            Assert.Equal(3L, table.NumberOfRows);
            Assert.Equal(2, table.Schema.FieldsList.Count);

            await foreach (var batch in table.ReadAsync())
            {
                var ids = (Int32Array)batch.Column(0);
                var names = (StringArray)batch.Column(1);
                Assert.Equal(new int?[] { 1, 2, 3 }, ids.ToArray());
                Assert.Equal("alpha", names.GetString(0));
                Assert.Null(names.GetString(1));
                Assert.Equal("gamma", names.GetString(2));
            }
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task DatasetLayout_HasExpectedFiles()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3 });
                await ds.FinishAsync();
            }

            // _versions/<u64::MAX-1>.manifest
            string versionsDir = Path.Combine(path, "_versions");
            Assert.True(Directory.Exists(versionsDir));
            var manifests = Directory.GetFiles(versionsDir, "*.manifest");
            Assert.Single(manifests);
            string manifestName = Path.GetFileNameWithoutExtension(manifests[0]);
            Assert.Equal((ulong.MaxValue - 1).ToString(), manifestName);

            // data/<uuid>.lance
            string dataDir = Path.Combine(path, "data");
            Assert.True(Directory.Exists(dataDir));
            var dataFiles = Directory.GetFiles(dataDir, "*.lance");
            Assert.Single(dataFiles);

            // _transactions/0-<uuid>.txn
            string txnDir = Path.Combine(path, "_transactions");
            Assert.True(Directory.Exists(txnDir));
            var txns = Directory.GetFiles(txnDir, "*.txn");
            Assert.Single(txns);
            Assert.StartsWith("0-", Path.GetFileName(txns[0]));
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task RefusesToOverwriteExistingDataset()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1 });
                await ds.FinishAsync();
            }

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await using var ds2 = await LanceDatasetWriter.CreateAsync(path);
            });
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Dataset_Int32Only_CrossValidatedAgainstPylance()
    {
        if (!IsPythonAvailable()) return;

        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("id", new[] { 7, 11, 13, 17 });
                await ds.FinishAsync();
            }

            string script = "import sys, json\n" +
                "import lance\n" +
                $"ds = lance.dataset(r'{path}')\n" +
                "t = ds.to_table()\n" +
                "out = { 'rows': len(t), 'id': t['id'].to_pylist() }\n" +
                "sys.stdout.write(json.dumps(out))\n";

            var psi = new ProcessStartInfo("python", "-c " + EscapeArg(script))
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi)!;
            string stdout = await proc.StandardOutput.ReadToEndAsync();
            string stderr = await proc.StandardError.ReadToEndAsync();
            await proc.WaitForExitAsync();
            Assert.True(proc.ExitCode == 0,
                $"pylance exited {proc.ExitCode}; stderr: {stderr}; stdout: {stdout}");

            var json = System.Text.Json.JsonDocument.Parse(stdout);
            Assert.Equal(4, json.RootElement.GetProperty("rows").GetInt32());
            var ids = json.RootElement.GetProperty("id").EnumerateArray().Select(e => e.GetInt32()).ToArray();
            Assert.Equal(new[] { 7, 11, 13, 17 }, ids);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Dataset_TenStrings_CrossValidatedAgainstPylance()
    {
        if (!IsPythonAvailable()) return;

        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("id", Enumerable.Range(0, 10).ToArray());
                var sb = new StringArray.Builder();
                for (int i = 0; i < 10; i++) sb.Append($"item-{i}");
                await ds.FileWriter.WriteColumnAsync("name", sb.Build());
                await ds.FinishAsync();
            }

            string script = "import sys, json\n" +
                "import lance\n" +
                $"ds = lance.dataset(r'{path}')\n" +
                "t = ds.to_table()\n" +
                "out = { 'rows': len(t), 'name': t['name'].to_pylist() }\n" +
                "sys.stdout.write(json.dumps(out))\n";

            var psi = new ProcessStartInfo("python", "-c " + EscapeArg(script))
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi)!;
            string stdout = await proc.StandardOutput.ReadToEndAsync();
            string stderr = await proc.StandardError.ReadToEndAsync();
            await proc.WaitForExitAsync();
            Assert.True(proc.ExitCode == 0,
                $"pylance exited {proc.ExitCode}; stderr: {stderr}; stdout: {stdout}");
            var json = System.Text.Json.JsonDocument.Parse(stdout);
            Assert.Equal(10, json.RootElement.GetProperty("rows").GetInt32());
            var names = json.RootElement.GetProperty("name").EnumerateArray().Select(e => e.GetString()).ToArray();
            var expected = Enumerable.Range(0, 10).Select(i => $"item-{i}").ToArray();
            Assert.Equal(expected, names);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Dataset_NonNullStrings_CrossValidatedAgainstPylance()
    {
        if (!IsPythonAvailable()) return;

        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("id", new[] { 7, 11, 13, 17 });
                var sb = new StringArray.Builder();
                sb.Append("a"); sb.Append("bb"); sb.Append("ccc"); sb.Append("dddd");
                await ds.FileWriter.WriteColumnAsync("name", sb.Build());
                await ds.FinishAsync();
            }

            string script = "import sys, json\n" +
                "import lance\n" +
                $"ds = lance.dataset(r'{path}')\n" +
                "t = ds.to_table()\n" +
                "out = { 'rows': len(t), 'name': t['name'].to_pylist() }\n" +
                "sys.stdout.write(json.dumps(out))\n";

            var psi = new ProcessStartInfo("python", "-c " + EscapeArg(script))
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi)!;
            string stdout = await proc.StandardOutput.ReadToEndAsync();
            string stderr = await proc.StandardError.ReadToEndAsync();
            await proc.WaitForExitAsync();
            Assert.True(proc.ExitCode == 0,
                $"pylance exited {proc.ExitCode}; stderr: {stderr}; stdout: {stdout}");

            var json = System.Text.Json.JsonDocument.Parse(stdout);
            Assert.Equal(4, json.RootElement.GetProperty("rows").GetInt32());
            var names = json.RootElement.GetProperty("name").EnumerateArray().Select(e => e.GetString()).ToArray();
            Assert.Equal(new[] { "a", "bb", "ccc", "dddd" }, names);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Dataset_CrossValidatedAgainstPylance()
    {
        if (!IsPythonAvailable())
        {
            return;
        }

        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("id", new[] { 7, 11, 13, 17 });
                var sb = new StringArray.Builder();
                sb.Append("a"); sb.Append("bb"); sb.AppendNull(); sb.Append("dddd");
                await ds.FileWriter.WriteColumnAsync("name", sb.Build());
                await ds.FinishAsync();
            }

            // pylance opens via lance.dataset(path).
            string script = "import sys, json\n" +
                "import lance\n" +
                $"ds = lance.dataset(r'{path}')\n" +
                "t = ds.to_table()\n" +
                "out = { 'rows': len(t), 'columns': t.column_names, " +
                "'types': [str(f.type) for f in t.schema], " +
                "'id': t['id'].to_pylist(), 'name': t['name'].to_pylist() }\n" +
                "sys.stdout.write(json.dumps(out))\n";

            var psi = new ProcessStartInfo("python", "-c " + EscapeArg(script))
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi)!;
            string stdout = await proc.StandardOutput.ReadToEndAsync();
            string stderr = await proc.StandardError.ReadToEndAsync();
            await proc.WaitForExitAsync();
            Assert.True(proc.ExitCode == 0,
                $"pylance exited {proc.ExitCode}; stderr: {stderr}; stdout: {stdout}");

            var json = System.Text.Json.JsonDocument.Parse(stdout);
            var root = json.RootElement;
            Assert.Equal(4, root.GetProperty("rows").GetInt32());
            Assert.Equal(new[] { "id", "name" },
                root.GetProperty("columns").EnumerateArray().Select(e => e.GetString()).ToArray());
            Assert.Equal(new[] { "int32", "string" },
                root.GetProperty("types").EnumerateArray().Select(e => e.GetString()).ToArray());

            var ids = root.GetProperty("id").EnumerateArray().Select(e => e.GetInt32()).ToArray();
            Assert.Equal(new[] { 7, 11, 13, 17 }, ids);

            var names = new List<string?>();
            foreach (var v in root.GetProperty("name").EnumerateArray())
                names.Add(v.ValueKind == System.Text.Json.JsonValueKind.Null ? null : v.GetString());
            Assert.Equal(new string?[] { "a", "bb", null, "dddd" }, names);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    private static bool IsPythonAvailable()
    {
        try
        {
            var psi = new ProcessStartInfo("python", "-c \"import lance\"")
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi);
            if (proc is null) return false;
            proc.WaitForExit(5_000);
            return proc.ExitCode == 0;
        }
        catch { return false; }
    }

    private static string EscapeArg(string s)
    {
        return "\"" + s.Replace("\\", "\\\\").Replace("\"", "\\\"") + "\"";
    }

    [Fact]
    public async Task Append_AddsFragmentToExistingDataset()
    {
        string path = MakeTempDatasetPath();
        try
        {
            // Initial dataset: one fragment with [1, 2, 3].
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3 });
                await ds.FinishAsync();
            }

            // Append a second fragment with [4, 5].
            await using (var ds = await LanceDatasetWriter.AppendAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 4, 5 });
                await ds.FinishAsync();
            }

            // Reader should see version 2, two fragments, total 5 rows.
            await using var table = await LanceTable.OpenAsync(path);
            Assert.Equal(2, table.NumberOfFragments);
            Assert.Equal(5L, table.NumberOfRows);
            // ReadAsync streams one RecordBatch per fragment; concatenate.
            var batches = await ToArrayBatchesAsync(table);
            Assert.Equal(2, batches.Count);
            Assert.Equal(new int?[] { 1, 2, 3 }, ((Int32Array)batches[0]).ToArray());
            Assert.Equal(new int?[] { 4, 5 }, ((Int32Array)batches[1]).ToArray());

            // _versions/ should now have two manifests (v1 and v2).
            string versions = Path.Combine(path, "_versions");
            Assert.Equal(2, Directory.GetFiles(versions, "*.manifest").Length);
            // data/ should have two .lance files.
            Assert.Equal(2, Directory.GetFiles(Path.Combine(path, "data"), "*.lance").Length);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Append_RejectsSchemaMismatch()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1 });
                await ds.FinishAsync();
            }

            // Try to append with a different column name → must throw.
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await using var ds = await LanceDatasetWriter.AppendAsync(path);
                await ds.FileWriter.WriteInt32ColumnAsync("y", new[] { 2 });
                await ds.FinishAsync();
            });

            // The bad write left a partial manifest absent — only v1 should remain.
            string versions = Path.Combine(path, "_versions");
            Assert.Single(Directory.GetFiles(versions, "*.manifest"));
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Overwrite_ReplacesContentsAndAllowsSchemaChange()
    {
        string path = MakeTempDatasetPath();
        try
        {
            // Initial: column 'x' int32.
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3 });
                await ds.FinishAsync();
            }

            // Overwrite with a totally different schema: column 'y' float.
            await using (var ds = await LanceDatasetWriter.OverwriteAsync(path))
            {
                await ds.FileWriter.WriteFloatColumnAsync("y", new[] { 1.5f, 2.5f });
                await ds.FinishAsync();
            }

            await using var table = await LanceTable.OpenAsync(path);
            Assert.Equal(1, table.NumberOfFragments);
            Assert.Equal(2L, table.NumberOfRows);
            Assert.Equal("y", table.Schema.FieldsList[0].Name);
            Assert.IsType<FloatType>(table.Schema.FieldsList[0].DataType);

            // _versions/ has 2 manifests (v1 and v2 = overwritten).
            string versions = Path.Combine(path, "_versions");
            Assert.Equal(2, Directory.GetFiles(versions, "*.manifest").Length);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task MultiFragment_InOneTransaction()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3 });
                await ds.NewFragmentAsync();
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 10, 20 });
                await ds.NewFragmentAsync();
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 100 });
                await ds.FinishAsync();
            }

            // One manifest version with three fragments.
            await using var table = await LanceTable.OpenAsync(path);
            Assert.Equal(3, table.NumberOfFragments);
            Assert.Equal(6L, table.NumberOfRows);

            string versions = Path.Combine(path, "_versions");
            Assert.Single(Directory.GetFiles(versions, "*.manifest"));
            Assert.Equal(3, Directory.GetFiles(Path.Combine(path, "data"), "*.lance").Length);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Append_AfterMultiFragment_ChainsVersionsCorrectly()
    {
        // Combines multi-fragment-per-transaction with append: v1 has 2
        // fragments; appending in v2 adds a 3rd.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2 });
                await ds.NewFragmentAsync();
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 3, 4 });
                await ds.FinishAsync();
            }
            await using (var ds = await LanceDatasetWriter.AppendAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 5, 6, 7 });
                await ds.FinishAsync();
            }

            await using var table = await LanceTable.OpenAsync(path);
            Assert.Equal(3, table.NumberOfFragments);
            Assert.Equal(7L, table.NumberOfRows);

            // Sanity: manifest at v2 references three fragments with ids 0, 1, 2.
            await using var v1 = await LanceTable.OpenAsync(path, version: 1);
            Assert.Equal(2, v1.NumberOfFragments);
            await using var v2 = await LanceTable.OpenAsync(path, version: 2);
            Assert.Equal(3, v2.NumberOfFragments);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Append_RefusesIfNoExistingDataset()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await using var ds = await LanceDatasetWriter.AppendAsync(path);
            });
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Append_CrossValidatedAgainstPylance()
    {
        // Write v1 + append v2 with our writer, then have pylance read
        // the dataset and confirm both versions are reachable and the
        // latest version returns the union of both fragments' rows.
        if (!IsPythonAvailable()) return;
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 10, 20, 30 });
                await ds.FinishAsync();
            }
            await using (var ds = await LanceDatasetWriter.AppendAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 40, 50 });
                await ds.FinishAsync();
            }

            string script = "import sys, json, lance\n" +
                $"ds = lance.dataset(r'{path}')\n" +
                "out = { 'version': ds.version, 'rows': ds.count_rows(), " +
                "'data': ds.to_table().to_pydict()['x'], " +
                "'v1_rows': lance.dataset(r'" + path + "', version=1).count_rows() }\n" +
                "sys.stdout.write(json.dumps(out))\n";

            var psi = new ProcessStartInfo("python", "-c " + EscapeArg(script))
            {
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            };
            using var proc = Process.Start(psi)!;
            string stdout = await proc.StandardOutput.ReadToEndAsync();
            string stderr = await proc.StandardError.ReadToEndAsync();
            await proc.WaitForExitAsync();
            Assert.True(proc.ExitCode == 0,
                $"pylance exited {proc.ExitCode}; stderr: {stderr}; stdout: {stdout}");

            var json = System.Text.Json.JsonDocument.Parse(stdout);
            Assert.Equal(2, json.RootElement.GetProperty("version").GetInt32());
            Assert.Equal(5, json.RootElement.GetProperty("rows").GetInt32());
            Assert.Equal(3, json.RootElement.GetProperty("v1_rows").GetInt32());
            int[] expected = { 10, 20, 30, 40, 50 };
            int i = 0;
            foreach (var v in json.RootElement.GetProperty("data").EnumerateArray())
                Assert.Equal(expected[i++], v.GetInt32());
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    private static async Task<List<IArrowArray>> ToArrayBatchesAsync(LanceTable table)
    {
        var batches = new List<IArrowArray>();
        await foreach (var batch in table.ReadAsync())
            batches.Add(batch.Column(0));
        return batches;
    }
}
