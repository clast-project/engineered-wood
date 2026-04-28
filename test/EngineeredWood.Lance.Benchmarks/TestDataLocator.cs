// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Lance.Benchmarks;

/// <summary>
/// Locates committed pylance-produced <c>.lance</c> files inside the
/// sibling test project's <c>TestData/</c> folder. Used by every
/// benchmark's <c>[GlobalSetup]</c>.
/// </summary>
internal static class TestDataLocator
{
    public static string Resolve(string fileName)
    {
        string? dir = AppContext.BaseDirectory;
        while (dir is not null)
        {
            string candidate = Path.Combine(
                dir, "test", "EngineeredWood.Lance.Tests", "TestData", fileName);
            if (File.Exists(candidate)) return candidate;
            dir = Path.GetDirectoryName(dir);
        }
        throw new FileNotFoundException(
            $"Lance test data file not found while walking up from {AppContext.BaseDirectory}: {fileName}");
    }
}
