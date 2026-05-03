// Copyright (c) Curt Hagenlocher. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Vortex.Tests.TestData;

/// <summary>
/// Resolves the absolute path of a <c>.vortex</c> file in the tests'
/// <c>TestData</c> folder. The files are produced by the Rust fixture-generator
/// crate under <c>test/EngineeredWood.Vortex.Tests/Rust/</c> and copied to the
/// test output directory at build time.
/// </summary>
internal static class TestDataPath
{
    public static string Resolve(string fileName)
    {
        var baseDir = AppContext.BaseDirectory;
        var path = Path.Combine(baseDir, "TestData", fileName);
        if (!File.Exists(path))
            throw new FileNotFoundException(
                $"Test data file not found: {path}. Regenerate via the Rust crate at test/EngineeredWood.Vortex.Tests/Rust.",
                path);
        return path;
    }
}
