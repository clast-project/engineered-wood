// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Tests.Parquet;

/// <summary>
/// Helper to locate test data files from the parquet-testing submodule.
/// </summary>
internal static class TestData
{
    private static readonly string DataDirectory = FindDataDirectory();

    /// <summary>
    /// Returns the full path to a file in the parquet-testing/data/ directory.
    /// </summary>
    public static string GetPath(string fileName) =>
        Path.Combine(DataDirectory, fileName);

    /// <summary>
    /// Returns all .parquet files in the data directory.
    /// </summary>
    public static IEnumerable<string> GetAllParquetFiles() =>
        Directory.EnumerateFiles(DataDirectory, "*.parquet");

    /// <summary>
    /// Reads the raw Thrift footer bytes from a Parquet file.
    /// </summary>
    public static byte[] ReadFooterBytes(string fileName)
    {
        var path = GetPath(fileName);
        var fileBytes = File.ReadAllBytes(path);

        // Last 4 bytes must be PAR1 magic
        if (fileBytes.Length < 12)
            throw new InvalidOperationException($"File too small: {fileName}");

        var magic = fileBytes.AsSpan(fileBytes.Length - 4, 4);
        if (magic[0] != (byte)'P' || magic[1] != (byte)'A' ||
            magic[2] != (byte)'R' || magic[3] != (byte)'1')
            throw new InvalidOperationException($"Bad magic in {fileName}");

        int footerLength = BitConverter.ToInt32(fileBytes, fileBytes.Length - 8);
        int footerStart = fileBytes.Length - 8 - footerLength;
        if (footerStart < 4)
            throw new InvalidOperationException($"Invalid footer length in {fileName}");

        return fileBytes.AsSpan(footerStart, footerLength).ToArray();
    }

    private static string FindDataDirectory()
    {
        // Walk up from the test assembly output directory to find parquet-testing/data/
        var dir = AppContext.BaseDirectory;
        while (dir != null)
        {
            var candidate = Path.Combine(dir, "parquet-testing", "data");
            if (Directory.Exists(candidate))
                return candidate;
            dir = Path.GetDirectoryName(dir);
        }

        throw new DirectoryNotFoundException(
            "Could not find parquet-testing/data/ directory. " +
            "Ensure the parquet-testing submodule is initialized.");
    }
}
