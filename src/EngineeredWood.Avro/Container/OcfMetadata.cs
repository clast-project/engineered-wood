// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Avro.Container;

internal static class OcfMetadata
{
    /// <summary>
    /// The Avro specification reserves every metadata key that starts with <c>avro.</c>: the writer sets
    /// <c>avro.schema</c> and <c>avro.codec</c> itself, and a user value under such a key could contradict them.
    /// </summary>
    public static void ValidateUserKey(string key)
    {
        if (key is null)
            throw new ArgumentNullException(nameof(key));
        if (key.StartsWith("avro.", StringComparison.Ordinal))
            throw new ArgumentException(
                $"OCF metadata key '{key}' is reserved: keys starting with 'avro.' belong to the Avro format.",
                nameof(key));
    }
}
