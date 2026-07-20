// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Orc.Tests;

internal static class TestHelpers
{
    private static readonly string TestDataDir = Path.Combine(
        AppContext.BaseDirectory, "..", "..", "..", "TestData");

    public static string GetTestFilePath(string fileName) =>
        Path.GetFullPath(Path.Combine(TestDataDir, fileName));

    public static string GetTestDataDirectory() =>
        Path.GetFullPath(TestDataDir);
}
