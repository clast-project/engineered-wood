// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Lance.Table.Tests;

/// <summary>
/// Resolves the absolute path of a test fixture directory inside
/// <c>TestData/</c>. Each fixture is a Lance dataset directory tree
/// (with <c>_versions/</c>, <c>data/</c>, <c>_transactions/</c>) and
/// the whole tree is copied to the test output directory at build time.
/// </summary>
internal static class TestDataPath
{
    public static string Resolve(string datasetName)
    {
        var baseDir = AppContext.BaseDirectory;
        var path = Path.Combine(baseDir, "TestData", datasetName);
        if (!Directory.Exists(path))
            throw new DirectoryNotFoundException(
                $"Test data dataset not found: {path}. Regenerate via TestData/generate_test_data.py.");
        return path;
    }
}
