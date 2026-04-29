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
}
