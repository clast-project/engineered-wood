// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.Lance.Tests.TestData;

namespace EngineeredWood.Lance.Tests;

/// <summary>
/// Phase 10 cross-validation: open every committed pylance-produced
/// <c>.lance</c> file in <c>TestData/</c>, parse the schema, and try to
/// read every top-level Arrow field. Catches regressions introduced by
/// later phases against the realistic pylance encoding matrix without
/// requiring a per-file assertion.
///
/// <para>Files that intentionally exercise unsupported encodings can be
/// added to <see cref="ExpectedReadFailures"/> with a reason. Today the
/// list is empty: every committed file decodes end-to-end.</para>
/// </summary>
public class CompatibilitySweepTests
{
    /// <summary>
    /// Files (or specific (file, columnIndex) pairs) whose read is expected
    /// to throw <see cref="NotImplementedException"/> because they hit a
    /// deferred encoding path. Empty today.
    /// </summary>
    private static readonly Dictionary<string, string> ExpectedReadFailures = new();

    [Fact]
    public void Every_TestData_File_Opens_Successfully()
    {
        var failures = new List<string>();
        foreach (var file in EnumerateTestDataFiles())
        {
            try
            {
                using var reader = LanceFileReader.OpenAsync(file).GetAwaiter().GetResult();
                _ = reader.Schema;
                _ = reader.NumberOfRows;
                _ = reader.NumberOfColumns;
            }
            catch (Exception ex)
            {
                failures.Add($"{Path.GetFileName(file)}: {ex.GetType().Name}: {ex.Message}");
            }
        }

        Assert.True(failures.Count == 0,
            "Expected every TestData/.lance file to open. Failures:\n" +
            string.Join("\n", failures));
    }

    [Fact]
    public async Task Every_TestData_Field_Decodes_To_NonNull_Array()
    {
        var failures = new List<string>();
        var unexpectedSuccesses = new List<string>();
        var expected = new HashSet<string>(ExpectedReadFailures.Keys, StringComparer.Ordinal);

        foreach (var file in EnumerateTestDataFiles())
        {
            string fileName = Path.GetFileName(file);
            await using var reader = await LanceFileReader.OpenAsync(file);

            for (int fieldIndex = 0; fieldIndex < reader.Schema.FieldsList.Count; fieldIndex++)
            {
                string key = $"{fileName}:{fieldIndex}";
                bool isExpectedToFail = expected.Contains(fileName) || expected.Contains(key);

                try
                {
                    var arr = await reader.ReadColumnAsync(fieldIndex);
                    Assert.NotNull(arr);
                    Assert.Equal((int)reader.NumberOfRows, arr.Length);

                    if (isExpectedToFail)
                        unexpectedSuccesses.Add(
                            $"{key}: expected to throw NotImplementedException but decoded successfully.");
                }
                catch (NotImplementedException ex) when (isExpectedToFail)
                {
                    // Expected: known-unsupported encoding path.
                    _ = ex;
                }
                catch (Exception ex)
                {
                    failures.Add(
                        $"{key}: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }

        Assert.True(failures.Count == 0,
            "Decode failures across the TestData sweep:\n" + string.Join("\n", failures));
        Assert.True(unexpectedSuccesses.Count == 0,
            "Files marked as expected failures actually succeeded — clean up the skip list:\n" +
            string.Join("\n", unexpectedSuccesses));
    }

    private static IEnumerable<string> EnumerateTestDataFiles()
    {
        string baseDir = Path.Combine(AppContext.BaseDirectory, "TestData");
        return Directory.EnumerateFiles(baseDir, "*.lance", SearchOption.TopDirectoryOnly)
            .OrderBy(p => p, StringComparer.Ordinal);
    }
}
