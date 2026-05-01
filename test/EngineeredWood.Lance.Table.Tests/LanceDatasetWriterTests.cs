// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Diagnostics;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.Expressions;
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
    public async Task Delete_RemovesRowsFromSingleFragment()
    {
        // Single fragment, 5 rows. Delete rows at offsets 1 and 3.
        // Reading the new version should yield 3 surviving rows.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 10, 20, 30, 40, 50 });
                await ds.FinishAsync();
            }

            long newVersion = await LanceDatasetWriter.DeleteRowsAsync(path,
                new Dictionary<ulong, IReadOnlyList<int>>
                {
                    [0] = new[] { 1, 3 },
                });
            Assert.Equal(2L, newVersion);

            await using var table = await LanceTable.OpenAsync(path);
            // NumberOfRows is logical = physical - deleted, so 5 - 2 = 3.
            Assert.Equal(3L, table.NumberOfRows);
            int[] survivors = await ReadInt32ColumnAsync(table, "x");
            Assert.Equal(new[] { 10, 30, 50 }, survivors);

            // _deletions/ should now have a single .arrow file.
            var delFiles = Directory.GetFiles(
                Path.Combine(path, "_deletions"), "*.arrow");
            Assert.Single(delFiles);
            string delName = Path.GetFileName(delFiles[0]);
            Assert.StartsWith("0-1-2", delName);  // {fragId}-{readVersion}-{newId}
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Delete_AcrossMultipleFragments()
    {
        // Two-fragment dataset; delete rows from each independently.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3 });
                await ds.NewFragmentAsync();
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 100, 200, 300 });
                await ds.FinishAsync();
            }

            await LanceDatasetWriter.DeleteRowsAsync(path,
                new Dictionary<ulong, IReadOnlyList<int>>
                {
                    [0] = new[] { 0 },         // delete the first row of fragment 0
                    [1] = new[] { 1, 2 },      // delete the last two of fragment 1
                });

            await using var table = await LanceTable.OpenAsync(path);
            int[] survivors = await ReadInt32ColumnAsync(table, "x");
            Assert.Equal(new[] { 2, 3, 100 }, survivors);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Delete_MergesWithExistingDeletionFile()
    {
        // First delete rows {1, 3}, then in a second call delete {0, 2}.
        // The second deletion file should reference all 4 rows.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 10, 20, 30, 40, 50 });
                await ds.FinishAsync();
            }

            await LanceDatasetWriter.DeleteRowsAsync(path,
                new Dictionary<ulong, IReadOnlyList<int>>
                {
                    [0] = new[] { 1, 3 },
                });
            await LanceDatasetWriter.DeleteRowsAsync(path,
                new Dictionary<ulong, IReadOnlyList<int>>
                {
                    [0] = new[] { 0, 2 },
                });

            await using var table = await LanceTable.OpenAsync(path);
            int[] survivors = await ReadInt32ColumnAsync(table, "x");
            Assert.Equal(new[] { 50 }, survivors);

            // _deletions/ has both files (each is referenced by its own version).
            // The latest manifest references only the second file.
            var delFiles = Directory.GetFiles(
                Path.Combine(path, "_deletions"), "*.arrow");
            Assert.Equal(2, delFiles.Length);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Delete_PreviousVersionStillReadable()
    {
        // After a delete bumps version 1 → 2, opening v1 explicitly should
        // still see all original rows.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3, 4 });
                await ds.FinishAsync();
            }
            await LanceDatasetWriter.DeleteRowsAsync(path,
                new Dictionary<ulong, IReadOnlyList<int>>
                {
                    [0] = new[] { 0, 1 },
                });

            await using var v1 = await LanceTable.OpenAsync(path, version: 1);
            int[] before = await ReadInt32ColumnAsync(v1, "x");
            Assert.Equal(new[] { 1, 2, 3, 4 }, before);

            await using var v2 = await LanceTable.OpenAsync(path, version: 2);
            int[] after = await ReadInt32ColumnAsync(v2, "x");
            Assert.Equal(new[] { 3, 4 }, after);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Delete_EmptyMap_NoOp()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2 });
                await ds.FinishAsync();
            }
            long ver = await LanceDatasetWriter.DeleteRowsAsync(path,
                new Dictionary<ulong, IReadOnlyList<int>>());
            Assert.Equal(1L, ver);  // unchanged

            // No deletions directory contents.
            string delDir = Path.Combine(path, "_deletions");
            if (Directory.Exists(delDir))
                Assert.Empty(Directory.GetFiles(delDir));
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Delete_RejectsBadFragmentId()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1 });
                await ds.FinishAsync();
            }
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await LanceDatasetWriter.DeleteRowsAsync(path,
                    new Dictionary<ulong, IReadOnlyList<int>>
                    {
                        [42] = new[] { 0 },  // fragment 42 doesn't exist
                    }));
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Delete_RejectsOffsetOutOfRange()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2 });
                await ds.FinishAsync();
            }
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
                await LanceDatasetWriter.DeleteRowsAsync(path,
                    new Dictionary<ulong, IReadOnlyList<int>>
                    {
                        [0] = new[] { 5 },  // fragment has 2 rows; 5 is out of range
                    }));
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Delete_CrossValidatedAgainstPylance()
    {
        // Verify the deletion-file format against pylance: write 8 rows,
        // delete rows 1, 3, 5; confirm pylance sees 5 surviving rows.
        if (!IsPythonAvailable()) return;
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x",
                    new[] { 10, 11, 12, 13, 14, 15, 16, 17 });
                await ds.FinishAsync();
            }
            await LanceDatasetWriter.DeleteRowsAsync(path,
                new Dictionary<ulong, IReadOnlyList<int>>
                {
                    [0] = new[] { 1, 3, 5 },
                });

            string script = "import sys, json, lance\n" +
                $"ds = lance.dataset(r'{path}')\n" +
                "out = { 'rows': ds.count_rows(), 'data': ds.to_table().to_pydict()['x'] }\n" +
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
            Assert.Equal(5, json.RootElement.GetProperty("rows").GetInt32());
            int[] expected = { 10, 12, 14, 16, 17 };
            int i = 0;
            foreach (var v in json.RootElement.GetProperty("data").EnumerateArray())
                Assert.Equal(expected[i++], v.GetInt32());
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task DeleteByPredicate_RemovesMatchingRows()
    {
        // Two-fragment dataset with int values 1..4 and 5..8. DeleteAsync
        // with `x > 3` should delete row 4 from fragment 0 (offset 3) and
        // every row from fragment 1 (offsets 0..3).
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3, 4 });
                await ds.NewFragmentAsync();
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 5, 6, 7, 8 });
                await ds.FinishAsync();
            }

            var pred = EngineeredWood.Expressions.Expressions.GreaterThan("x", LiteralValue.Of(3));
            long newVersion = await LanceDatasetWriter.DeleteAsync(path, pred);
            Assert.Equal(2L, newVersion);

            await using var table = await LanceTable.OpenAsync(path);
            int[] survivors = await ReadInt32ColumnAsync(table, "x");
            Assert.Equal(new[] { 1, 2, 3 }, survivors);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task DeleteByPredicate_NoMatches_NoOp()
    {
        // Predicate matches no rows → no version bump, no deletion files.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3 });
                await ds.FinishAsync();
            }
            var pred = EngineeredWood.Expressions.Expressions.GreaterThan("x", LiteralValue.Of(100));
            long version = await LanceDatasetWriter.DeleteAsync(path, pred);
            Assert.Equal(1L, version);  // unchanged

            string delDir = Path.Combine(path, "_deletions");
            if (Directory.Exists(delDir))
                Assert.Empty(Directory.GetFiles(delDir));
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task DeleteByPredicate_StringEqual_AcrossFragments()
    {
        // string column; delete where name = "alice".
        string path = MakeTempDatasetPath();
        try
        {
            var sb1 = new StringArray.Builder();
            sb1.Append("alice").Append("bob").Append("carol");
            var sb2 = new StringArray.Builder();
            sb2.Append("alice").Append("dave");

            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteColumnAsync("name", sb1.Build());
                await ds.NewFragmentAsync();
                await ds.FileWriter.WriteColumnAsync("name", sb2.Build());
                await ds.FinishAsync();
            }

            var pred = EngineeredWood.Expressions.Expressions.Equal("name", LiteralValue.Of("alice"));
            await LanceDatasetWriter.DeleteAsync(path, pred);

            await using var table = await LanceTable.OpenAsync(path);
            var names = new List<string>();
            await foreach (var batch in table.ReadAsync())
            {
                int idx = batch.Schema.GetFieldIndex("name");
                var arr = (StringArray)batch.Column(idx);
                for (int i = 0; i < arr.Length; i++) names.Add(arr.GetString(i)!);
            }
            Assert.Equal(new[] { "bob", "carol", "dave" }, names);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task DeleteByPredicate_RejectsUnknownColumn()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1 });
                await ds.FinishAsync();
            }
            var pred = EngineeredWood.Expressions.Expressions.Equal("nope", LiteralValue.Of(0));
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await LanceDatasetWriter.DeleteAsync(path, pred));
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task DeleteByPredicate_SkipsAlreadyDeletedRows()
    {
        // Pre-mark row 0 deleted. Then DeleteAsync with `x < 100` matches
        // every row but skips row 0 since it's already deleted. The new
        // deletion file should reference rows 1..4 only.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 10, 20, 30, 40, 50 });
                await ds.FinishAsync();
            }
            await LanceDatasetWriter.DeleteRowsAsync(path,
                new Dictionary<ulong, IReadOnlyList<int>> { [0] = new[] { 0 } });

            var pred = EngineeredWood.Expressions.Expressions.LessThan("x", LiteralValue.Of(100));
            await LanceDatasetWriter.DeleteAsync(path, pred);

            await using var table = await LanceTable.OpenAsync(path);
            int[] survivors = await ReadInt32ColumnAsync(table, "x");
            Assert.Empty(survivors);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Update_SetSingleColumn_ToLiteral()
    {
        // SET x = 99 WHERE x > 2 — over a 5-row dataset, rows 3 & 4 (values
        // 3 and 4) should be replaced with 99 each.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 0, 1, 2, 3, 4 });
                await ds.FinishAsync();
            }

            var pred = Expressions.Expressions.GreaterThan("x", LiteralValue.Of(2));
            var assignments = new Dictionary<string, Expression>
            {
                ["x"] = new LiteralExpression(LiteralValue.Of(99)),
            };
            var (rowsUpdated, version) = await LanceDatasetWriter.UpdateAsync(path, pred, assignments);
            Assert.Equal(2L, rowsUpdated);
            Assert.Equal(2L, version);

            await using var table = await LanceTable.OpenAsync(path);
            int[] survivors = await ReadInt32ColumnAsync(table, "x");
            // Original survivors: 0, 1, 2; new fragment: 99, 99 (the rewritten rows).
            // Reading via fragment order gives [0, 1, 2, 99, 99].
            Assert.Equal(new[] { 0, 1, 2, 99, 99 }, survivors);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Update_NoMatches_NoOp()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3 });
                await ds.FinishAsync();
            }
            var pred = Expressions.Expressions.GreaterThan("x", LiteralValue.Of(100));
            var assignments = new Dictionary<string, Expression>
            {
                ["x"] = new LiteralExpression(LiteralValue.Of(0)),
            };
            var (rowsUpdated, version) = await LanceDatasetWriter.UpdateAsync(path, pred, assignments);
            Assert.Equal(0L, rowsUpdated);
            Assert.Equal(1L, version);  // current version
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Update_PreservesUnassignedColumns()
    {
        // Two columns, only one in the assignment. The other should
        // round-trip its original values for the rewritten rows.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("id", new[] { 1, 2, 3 });
                var sb = new StringArray.Builder();
                sb.Append("alice").Append("bob").Append("carol");
                await ds.FileWriter.WriteColumnAsync("name", sb.Build());
                await ds.FinishAsync();
            }

            var pred = Expressions.Expressions.GreaterThan("id", LiteralValue.Of(1));
            var assignments = new Dictionary<string, Expression>
            {
                ["name"] = new LiteralExpression(LiteralValue.Of("UPDATED")),
            };
            await LanceDatasetWriter.UpdateAsync(path, pred, assignments);

            await using var table = await LanceTable.OpenAsync(path);
            var ids = new List<int>();
            var names = new List<string>();
            await foreach (var batch in table.ReadAsync())
            {
                int idIdx = batch.Schema.GetFieldIndex("id");
                int nameIdx = batch.Schema.GetFieldIndex("name");
                var idArr = (Int32Array)batch.Column(idIdx);
                var nameArr = (StringArray)batch.Column(nameIdx);
                for (int i = 0; i < batch.Length; i++)
                {
                    ids.Add(idArr.GetValue(i)!.Value);
                    names.Add(nameArr.GetString(i)!);
                }
            }
            // Order: original survivors (id=1) first, then rewrite fragment
            // (id=2 and id=3, both with name "UPDATED").
            Assert.Equal(new[] { 1, 2, 3 }, ids);
            Assert.Equal(new[] { "alice", "UPDATED", "UPDATED" }, names);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Update_RejectsUnknownColumn()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1 });
                await ds.FinishAsync();
            }
            var pred = Expressions.Expressions.Equal("x", LiteralValue.Of(1));
            var assignments = new Dictionary<string, Expression>
            {
                ["nope"] = new LiteralExpression(LiteralValue.Of(0)),
            };
            await Assert.ThrowsAsync<ArgumentException>(async () =>
                await LanceDatasetWriter.UpdateAsync(path, pred, assignments));
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Update_PreviousVersionStillReadable()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 10, 20, 30 });
                await ds.FinishAsync();
            }

            var pred = Expressions.Expressions.GreaterThanOrEqual("x", LiteralValue.Of(20));
            var assignments = new Dictionary<string, Expression>
            {
                ["x"] = new LiteralExpression(LiteralValue.Of(0)),
            };
            await LanceDatasetWriter.UpdateAsync(path, pred, assignments);

            await using var v1 = await LanceTable.OpenAsync(path, version: 1);
            int[] before = await ReadInt32ColumnAsync(v1, "x");
            Assert.Equal(new[] { 10, 20, 30 }, before);

            await using var v2 = await LanceTable.OpenAsync(path, version: 2);
            int[] after = await ReadInt32ColumnAsync(v2, "x");
            Assert.Equal(new[] { 10, 0, 0 }, after);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Update_AcrossMultipleFragments()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3 });
                await ds.NewFragmentAsync();
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 4, 5, 6 });
                await ds.FinishAsync();
            }
            var pred = Expressions.Expressions.LessThan("x", LiteralValue.Of(5));
            var assignments = new Dictionary<string, Expression>
            {
                ["x"] = new LiteralExpression(LiteralValue.Of(-1)),
            };
            var (rowsUpdated, _) = await LanceDatasetWriter.UpdateAsync(path, pred, assignments);
            Assert.Equal(4L, rowsUpdated);  // fragment 0 all 3 rows + fragment 1 row 4 only

            await using var table = await LanceTable.OpenAsync(path);
            int[] survivors = await ReadInt32ColumnAsync(table, "x");
            // Surviving originals: from frag0 nothing, from frag1: 5, 6.
            // Rewrite fragment: -1 four times.
            Assert.Equal(new[] { 5, 6, -1, -1, -1, -1 }, survivors);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Update_CrossValidatedAgainstPylance()
    {
        if (!IsPythonAvailable()) return;
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("id", new[] { 1, 2, 3, 4 });
                var sb = new StringArray.Builder();
                sb.Append("a").Append("b").Append("c").Append("d");
                await ds.FileWriter.WriteColumnAsync("tag", sb.Build());
                await ds.FinishAsync();
            }
            var pred = Expressions.Expressions.GreaterThanOrEqual("id", LiteralValue.Of(3));
            var assignments = new Dictionary<string, Expression>
            {
                ["tag"] = new LiteralExpression(LiteralValue.Of("ZZ")),
            };
            await LanceDatasetWriter.UpdateAsync(path, pred, assignments);

            string script = "import sys, json, lance\n" +
                $"ds = lance.dataset(r'{path}')\n" +
                "t = ds.to_table()\n" +
                "out = sorted(zip(t['id'].to_pylist(), t['tag'].to_pylist()))\n" +
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

            // Sorted (id, tag) pairs: ids 1..4, tags a/b/ZZ/ZZ.
            var json = System.Text.Json.JsonDocument.Parse(stdout);
            var pairs = json.RootElement.EnumerateArray().ToArray();
            Assert.Equal(4, pairs.Length);
            Assert.Equal(1, pairs[0][0].GetInt32());
            Assert.Equal("a", pairs[0][1].GetString());
            Assert.Equal(2, pairs[1][0].GetInt32());
            Assert.Equal("b", pairs[1][1].GetString());
            Assert.Equal(3, pairs[2][0].GetInt32());
            Assert.Equal("ZZ", pairs[2][1].GetString());
            Assert.Equal(4, pairs[3][0].GetInt32());
            Assert.Equal("ZZ", pairs[3][1].GetString());
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task TimeTravel_OpensVersionAtOrBeforeTimestamp()
    {
        // Three commits at distinct timestamps. Reading "as of" each
        // commit's timestamp should land on that exact version; reading
        // between commits returns the earlier version.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1 });
                await ds.FinishAsync();
            }
            // Capture the timestamp of v1 by reading the table now.
            DateTimeOffset afterV1;
            await using (var t = await LanceTable.OpenAsync(path))
                afterV1 = t.CommitTimestamp;
            await Task.Delay(20);

            await using (var ds = await LanceDatasetWriter.OverwriteAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 2 });
                await ds.FinishAsync();
            }
            DateTimeOffset afterV2;
            await using (var t = await LanceTable.OpenAsync(path))
                afterV2 = t.CommitTimestamp;
            await Task.Delay(20);

            await using (var ds = await LanceDatasetWriter.OverwriteAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 3 });
                await ds.FinishAsync();
            }

            // Open as-of the v1 timestamp → should return v1.
            await using (var t = await LanceTable.OpenAsync(path, asOf: afterV1))
            {
                Assert.Equal(1UL, t.Version);
                int[] data = await ReadInt32ColumnAsync(t, "x");
                Assert.Equal(new[] { 1 }, data);
            }

            // Open between v1 and v2 (just slightly after v1) → still v1.
            var betweenV1V2 = afterV1.AddMilliseconds(1);
            // (As long as betweenV1V2 < afterV2.)
            if (betweenV1V2 < afterV2)
            {
                await using var t = await LanceTable.OpenAsync(path, asOf: betweenV1V2);
                Assert.Equal(1UL, t.Version);
            }

            // Open as-of the v2 timestamp → should return v2.
            await using (var t = await LanceTable.OpenAsync(path, asOf: afterV2))
            {
                Assert.Equal(2UL, t.Version);
                int[] data = await ReadInt32ColumnAsync(t, "x");
                Assert.Equal(new[] { 2 }, data);
            }

            // Open as-of "right now" → should return latest (v3).
            await using (var t = await LanceTable.OpenAsync(path, asOf: DateTimeOffset.UtcNow))
            {
                Assert.Equal(3UL, t.Version);
            }
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task TimeTravel_BeforeAnyCommit_Throws()
    {
        // No version is old enough → throws with a helpful message.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1 });
                await ds.FinishAsync();
            }
            // A timestamp from years ago — no manifest is that old.
            var asOf = new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);
            await Assert.ThrowsAsync<LanceTableFormatException>(async () =>
                await LanceTable.OpenAsync(path, asOf: asOf));
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task TimeTravel_RejectsBothVersionAndAsOf()
    {
        // The two overloads can't coexist on a single call. (The public
        // API doesn't expose both; this is an internal-ish guard.)
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1 });
                await ds.FinishAsync();
            }
            // Sanity: both single-arg overloads work independently.
            await using var v1 = await LanceTable.OpenAsync(path, version: 1);
            Assert.Equal(1UL, v1.Version);
            await using var t1 = await LanceTable.OpenAsync(path, asOf: DateTimeOffset.UtcNow);
            Assert.Equal(1UL, t1.Version);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Compact_NoDeletions_NoOp()
    {
        // Fresh dataset, no deletion files anywhere → CompactAsync is a
        // no-op (no version bump, no source/new fragments).
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3 });
                await ds.FinishAsync();
            }
            var result = await LanceDatasetWriter.CompactAsync(path);
            Assert.Empty(result.SourceFragmentIds);
            Assert.Empty(result.NewFragmentIds);
            Assert.Equal(0, result.RowsRetained);
            Assert.Equal(1L, result.Version);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Compact_AfterDelete_ReplacesFragmentWithSurvivors()
    {
        // 1 fragment with 5 rows → delete 2 → compact rewrites the
        // remaining 3 into a new fragment, drops the old one.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 10, 20, 30, 40, 50 });
                await ds.FinishAsync();
            }
            await LanceDatasetWriter.DeleteRowsAsync(path,
                new Dictionary<ulong, IReadOnlyList<int>> { [0] = new[] { 1, 3 } });

            var result = await LanceDatasetWriter.CompactAsync(path);
            Assert.Single(result.SourceFragmentIds);
            Assert.Equal(0UL, result.SourceFragmentIds[0]);
            Assert.Single(result.NewFragmentIds);
            Assert.Equal(3, result.RowsRetained);
            Assert.Equal(3L, result.Version);

            await using var table = await LanceTable.OpenAsync(path);
            Assert.Equal(1, table.NumberOfFragments);
            int[] survivors = await ReadInt32ColumnAsync(table, "x");
            Assert.Equal(new[] { 10, 30, 50 }, survivors);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Compact_FullyDeletedFragment_DropsIt()
    {
        // Delete every row in a fragment; compaction drops the fragment
        // entirely (no new fragment produced) and the dataset becomes
        // empty.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3 });
                await ds.FinishAsync();
            }
            await LanceDatasetWriter.DeleteRowsAsync(path,
                new Dictionary<ulong, IReadOnlyList<int>> { [0] = new[] { 0, 1, 2 } });

            var result = await LanceDatasetWriter.CompactAsync(path);
            Assert.Single(result.SourceFragmentIds);
            Assert.Empty(result.NewFragmentIds);
            Assert.Equal(0, result.RowsRetained);

            await using var table = await LanceTable.OpenAsync(path);
            Assert.Equal(0, table.NumberOfFragments);
            Assert.Equal(0L, table.NumberOfRows);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Compact_LeavesUnaffectedFragmentsAlone()
    {
        // 2 fragments, only one has deletions. Compaction should leave
        // the clean one untouched and rewrite only the dirty one.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 100, 200, 300 });
                await ds.NewFragmentAsync();
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3, 4 });
                await ds.FinishAsync();
            }
            // Delete from fragment 1 only.
            await LanceDatasetWriter.DeleteRowsAsync(path,
                new Dictionary<ulong, IReadOnlyList<int>> { [1] = new[] { 0, 2 } });

            var result = await LanceDatasetWriter.CompactAsync(path);
            Assert.Single(result.SourceFragmentIds);
            Assert.Equal(1UL, result.SourceFragmentIds[0]);  // only fragment 1 was compacted
            Assert.Single(result.NewFragmentIds);
            Assert.Equal(2, result.RowsRetained);

            await using var table = await LanceTable.OpenAsync(path);
            // 2 fragments: clean fragment 0 (3 rows) + new compacted (2 rows).
            Assert.Equal(2, table.NumberOfFragments);
            int[] survivors = await ReadInt32ColumnAsync(table, "x");
            Assert.Equal(new[] { 100, 200, 300, 2, 4 }, survivors);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Compact_PreservesPreviousVersionRowCount()
    {
        // After compaction, v1 (pre-delete) and v2 (post-delete) should
        // still both be reachable with their original row counts.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3, 4 });
                await ds.FinishAsync();
            }
            await LanceDatasetWriter.DeleteRowsAsync(path,
                new Dictionary<ulong, IReadOnlyList<int>> { [0] = new[] { 0 } });
            var result = await LanceDatasetWriter.CompactAsync(path);
            Assert.Equal(3L, result.Version);

            await using var v1 = await LanceTable.OpenAsync(path, version: 1);
            Assert.Equal(4L, v1.NumberOfRows);
            await using var v2 = await LanceTable.OpenAsync(path, version: 2);
            Assert.Equal(3L, v2.NumberOfRows);
            await using var v3 = await LanceTable.OpenAsync(path, version: 3);
            Assert.Equal(3L, v3.NumberOfRows);  // same survivors, just repacked
            Assert.Equal(1, v3.NumberOfFragments);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Compact_ThenVacuum_FreesOldFiles()
    {
        // The whole maintenance lifecycle: write, delete, compact, vacuum.
        // After vacuum (RetainVersions=1), only the compacted data file
        // and its manifest remain.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 10, 20, 30, 40, 50 });
                await ds.FinishAsync();
            }
            await LanceDatasetWriter.DeleteRowsAsync(path,
                new Dictionary<ulong, IReadOnlyList<int>> { [0] = new[] { 0, 4 } });
            await LanceDatasetWriter.CompactAsync(path);

            // Pre-vacuum: 3 manifests (write + delete + compact); 2 data
            // files (original + compacted); 1 deletion file.
            string data = Path.Combine(path, "data");
            string deletions = Path.Combine(path, "_deletions");
            string versions = Path.Combine(path, "_versions");
            Assert.Equal(3, Directory.GetFiles(versions, "*.manifest").Length);
            Assert.Equal(2, Directory.GetFiles(data, "*.lance").Length);
            Assert.Equal(1, Directory.GetFiles(deletions, "*.arrow").Length);

            var vac = await LanceDatasetWriter.VacuumAsync(path);
            // The original data file is now orphaned (latest manifest only
            // references the compacted file). The deletion file is also
            // orphaned (only referenced by v2's manifest, which we just
            // pruned). Two manifests pruned (v1 + v2 → only v3 stays).
            Assert.Single(Directory.GetFiles(data, "*.lance"));
            Assert.Empty(Directory.GetFiles(deletions, "*.arrow"));
            Assert.Single(Directory.GetFiles(versions, "*.manifest"));
            Assert.Equal(1, vac.DataFilesDeleted.Count);
            Assert.Equal(2, vac.ManifestsDeleted.Count);

            await using var table = await LanceTable.OpenAsync(path);
            int[] survivors = await ReadInt32ColumnAsync(table, "x");
            Assert.Equal(new[] { 20, 30, 40 }, survivors);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Compact_CrossValidatedAgainstPylance()
    {
        if (!IsPythonAvailable()) return;
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x",
                    new[] { 100, 200, 300, 400, 500, 600 });
                await ds.FinishAsync();
            }
            await LanceDatasetWriter.DeleteRowsAsync(path,
                new Dictionary<ulong, IReadOnlyList<int>> { [0] = new[] { 1, 2, 4 } });
            await LanceDatasetWriter.CompactAsync(path);

            string script = "import sys, json, lance\n" +
                $"ds = lance.dataset(r'{path}')\n" +
                "out = { 'rows': ds.count_rows(), 'data': ds.to_table().to_pydict()['x'], " +
                "'frags': len(ds.get_fragments()) }\n" +
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
            Assert.Equal(3, json.RootElement.GetProperty("rows").GetInt32());
            Assert.Equal(1, json.RootElement.GetProperty("frags").GetInt32());
            int[] expected = { 100, 400, 600 };
            int i = 0;
            foreach (var v in json.RootElement.GetProperty("data").EnumerateArray())
                Assert.Equal(expected[i++], v.GetInt32());
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task DeleteByPredicate_CrossValidatedAgainstPylance()
    {
        if (!IsPythonAvailable()) return;
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x",
                    new[] { 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 });
                await ds.FinishAsync();
            }
            // Delete rows where x >= 15 (i.e., the last 5 values).
            var pred = EngineeredWood.Expressions.Expressions.GreaterThanOrEqual(
                "x", LiteralValue.Of(15));
            await LanceDatasetWriter.DeleteAsync(path, pred);

            string script = "import sys, json, lance\n" +
                $"ds = lance.dataset(r'{path}')\n" +
                "data = ds.to_table().to_pydict()['x']\n" +
                "out = { 'rows': len(data), 'data': data }\n" +
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
            Assert.Equal(5, json.RootElement.GetProperty("rows").GetInt32());
            int[] expected = { 10, 11, 12, 13, 14 };
            int i = 0;
            foreach (var v in json.RootElement.GetProperty("data").EnumerateArray())
                Assert.Equal(expected[i++], v.GetInt32());
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    private static async Task<int[]> ReadInt32ColumnAsync(LanceTable table, string col)
    {
        var values = new List<int>();
        await foreach (var batch in table.ReadAsync())
        {
            int idx = batch.Schema.GetFieldIndex(col);
            var arr = (Int32Array)batch.Column(idx);
            for (int i = 0; i < arr.Length; i++)
                values.Add(arr.GetValue(i)!.Value);
        }
        return values.ToArray();
    }

    [Fact]
    public async Task Vacuum_NoOlderVersions_DeletesNothing()
    {
        // Single-version dataset → vacuum can't free anything (we always
        // retain at least the latest). Result has empty lists.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3 });
                await ds.FinishAsync();
            }

            var result = await LanceDatasetWriter.VacuumAsync(path);
            Assert.Empty(result.DataFilesDeleted);
            Assert.Empty(result.ManifestsDeleted);
            Assert.Empty(result.TransactionsDeleted);
            Assert.Equal(0, result.BytesDeleted);

            // Reading still works.
            await using var table = await LanceTable.OpenAsync(path);
            Assert.Equal(3L, table.NumberOfRows);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Vacuum_AfterOverwrite_ReclaimsOldFragment()
    {
        // Overwrite leaves the v1 data file orphaned. Default vacuum
        // (RetainVersions = 1) should delete it along with v1's manifest
        // and transaction.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3, 4, 5 });
                await ds.FinishAsync();
            }
            await using (var ds = await LanceDatasetWriter.OverwriteAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 100 });
                await ds.FinishAsync();
            }

            // Pre-vacuum: 2 manifests, 2 data files, 2 transactions.
            string versions = Path.Combine(path, "_versions");
            string data = Path.Combine(path, "data");
            string txns = Path.Combine(path, "_transactions");
            Assert.Equal(2, Directory.GetFiles(versions, "*.manifest").Length);
            Assert.Equal(2, Directory.GetFiles(data, "*.lance").Length);
            Assert.Equal(2, Directory.GetFiles(txns, "*.txn").Length);

            var result = await LanceDatasetWriter.VacuumAsync(path);
            Assert.Single(result.DataFilesDeleted);
            Assert.Single(result.ManifestsDeleted);
            Assert.Single(result.TransactionsDeleted);
            Assert.True(result.BytesDeleted > 0);

            // Post-vacuum: only the v2 files remain.
            Assert.Single(Directory.GetFiles(versions, "*.manifest"));
            Assert.Single(Directory.GetFiles(data, "*.lance"));
            Assert.Single(Directory.GetFiles(txns, "*.txn"));

            // Reading still returns the latest data.
            await using var table = await LanceTable.OpenAsync(path);
            Assert.Equal(1L, table.NumberOfRows);
            await foreach (var batch in table.ReadAsync())
            {
                var arr = (Int32Array)batch.Column(0);
                Assert.Equal(100, arr.GetValue(0));
            }
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Vacuum_AfterAppend_KeepsAllReferencedFiles()
    {
        // Append PRESERVES old fragments — they're still referenced by the
        // latest manifest. Vacuum should leave them alone (only the v1
        // manifest + transaction get pruned, since v2's manifest also lists
        // the v1 data file).
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2, 3 });
                await ds.FinishAsync();
            }
            await using (var ds = await LanceDatasetWriter.AppendAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 4, 5 });
                await ds.FinishAsync();
            }

            var result = await LanceDatasetWriter.VacuumAsync(path);
            // Both data files are still live (latest manifest lists both).
            Assert.Empty(result.DataFilesDeleted);
            // v1 manifest and transaction become stale (v2 supersedes them).
            Assert.Single(result.ManifestsDeleted);
            Assert.Single(result.TransactionsDeleted);

            // Reading still works.
            await using var table = await LanceTable.OpenAsync(path);
            Assert.Equal(5L, table.NumberOfRows);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Vacuum_DryRun_ReportsButDoesNotDelete()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1, 2 });
                await ds.FinishAsync();
            }
            await using (var ds = await LanceDatasetWriter.OverwriteAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 99 });
                await ds.FinishAsync();
            }

            var result = await LanceDatasetWriter.VacuumAsync(path,
                new LanceVacuumOptions { RetainVersions = 1, DryRun = true });
            Assert.True(result.DryRun);
            Assert.Single(result.DataFilesDeleted);
            Assert.Single(result.ManifestsDeleted);
            Assert.Single(result.TransactionsDeleted);
            Assert.True(result.BytesDeleted > 0);

            // Files should still all be present.
            string versions = Path.Combine(path, "_versions");
            string data = Path.Combine(path, "data");
            string txns = Path.Combine(path, "_transactions");
            Assert.Equal(2, Directory.GetFiles(versions, "*.manifest").Length);
            Assert.Equal(2, Directory.GetFiles(data, "*.lance").Length);
            Assert.Equal(2, Directory.GetFiles(txns, "*.txn").Length);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Vacuum_RetainVersions_KeepsOlderHistory()
    {
        // Three versions; RetainVersions=2 should keep v3+v2 and only
        // prune v1's files.
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1 });
                await ds.FinishAsync();
            }
            await using (var ds = await LanceDatasetWriter.OverwriteAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 2 });
                await ds.FinishAsync();
            }
            await using (var ds = await LanceDatasetWriter.OverwriteAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 3 });
                await ds.FinishAsync();
            }

            var result = await LanceDatasetWriter.VacuumAsync(path,
                new LanceVacuumOptions { RetainVersions = 2 });
            // Only v1 prunes — v2 and v3 stay.
            Assert.Single(result.DataFilesDeleted);
            Assert.Single(result.ManifestsDeleted);
            Assert.Single(result.TransactionsDeleted);

            // v2 should still be reachable by version.
            await using var v2 = await LanceTable.OpenAsync(path, version: 2);
            Assert.Equal(1L, v2.NumberOfRows);
            // v3 latest.
            await using var v3 = await LanceTable.OpenAsync(path);
            Assert.Equal(1L, v3.NumberOfRows);

            // v1 manifest is gone — opening it should throw.
            await Assert.ThrowsAsync<LanceTableFormatException>(async () =>
                await LanceTable.OpenAsync(path, version: 1));
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Vacuum_RejectsRetainVersionsZero()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await using (var ds = await LanceDatasetWriter.CreateAsync(path))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", new[] { 1 });
                await ds.FinishAsync();
            }
            await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
                await LanceDatasetWriter.VacuumAsync(path,
                    new LanceVacuumOptions { RetainVersions = 0 }));
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
    }

    [Fact]
    public async Task Vacuum_RejectsMissingDataset()
    {
        string path = MakeTempDatasetPath();
        try
        {
            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                await LanceDatasetWriter.VacuumAsync(path));
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, recursive: true);
        }
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
    public async Task Dataset_ZstdCompression_RoundTrip()
    {
        // Pass-through test: dataset writer threads the ZSTD scheme into
        // every fragment's LanceFileWriter and reading via LanceTable still
        // returns the same data. Larger column to see meaningful compression.
        string path = MakeTempDatasetPath();
        try
        {
            const int N = 30_000;
            var values = new int[N];
            for (int i = 0; i < N; i++) values[i] = i % 100;

            await using (var ds = await LanceDatasetWriter.CreateAsync(path,
                compression: LanceCompressionScheme.Zstd))
            {
                await ds.FileWriter.WriteInt32ColumnAsync("x", values);
                await ds.FinishAsync();
            }

            // The data file should have shrunk noticeably from raw 4×N bytes.
            var dataFiles = Directory.GetFiles(Path.Combine(path, "data"), "*.lance");
            Assert.Single(dataFiles);
            long fileSize = new FileInfo(dataFiles[0]).Length;
            Assert.True(fileSize < N * 4 / 2,
                $"Expected compressed data file < half raw size; got {fileSize} vs {N * 4}.");

            await using var table = await LanceTable.OpenAsync(path);
            Assert.Equal((long)N, table.NumberOfRows);
            int rowCursor = 0;
            await foreach (var batch in table.ReadAsync())
            {
                var arr = (Int32Array)batch.Column(0);
                for (int i = 0; i < arr.Length; i++)
                    Assert.Equal(rowCursor++ % 100, arr.GetValue(i));
            }
            Assert.Equal(N, rowCursor);
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
