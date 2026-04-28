// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Lance.Tests.TestData;

/// <summary>
/// Resolves the absolute path of a <c>.lance</c> file in the tests'
/// <c>TestData</c> folder. The files are produced by
/// <c>generate_test_data.py</c> (pylance) and copied to the test output
/// directory at build time.
/// </summary>
internal static class TestDataPath
{
    public static string Resolve(string fileName)
    {
        var baseDir = AppContext.BaseDirectory;
        var path = Path.Combine(baseDir, "TestData", fileName);
        if (!File.Exists(path))
            throw new FileNotFoundException(
                $"Test data file not found: {path}. Regenerate with TestData/generate_test_data.py.",
                path);
        return path;
    }
}
