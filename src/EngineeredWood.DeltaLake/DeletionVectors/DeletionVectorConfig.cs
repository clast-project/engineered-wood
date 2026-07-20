// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.DeletionVectors;

/// <summary>
/// Constants and utilities for Delta Lake deletion vectors. A deletion vector marks rows as logically
/// deleted while they stay physically present in the data file, so a DELETE (or UPDATE) can soft-delete a
/// subset of a file's rows without rewriting it.
///
/// <para>Deletion vectors are an OPT-IN table feature: enabling them sets the
/// <see cref="EnableKey"/> table property AND declares the <see cref="FeatureName"/> reader+writer feature
/// in the protocol (reader 3 / writer 7). The protocol declaration is what makes a conformant foreign
/// reader (Spark, delta-rs, delta-kernel) actually APPLY the deletion vector — without it, the reader
/// skips DV resolution and returns rows the table considers deleted. So a writer must never emit a
/// deletion vector on a table whose protocol does not declare the feature.</para>
/// </summary>
public static class DeletionVectorConfig
{
    /// <summary>Table property that enables deletion-vector writes.</summary>
    public const string EnableKey = "delta.enableDeletionVectors";

    /// <summary>The reader+writer table feature name (spec).</summary>
    public const string FeatureName = "deletionVectors";

    /// <summary>Returns true if deletion vectors are enabled for the table.</summary>
    public static bool IsEnabled(IReadOnlyDictionary<string, string>? configuration) =>
        configuration is not null &&
        configuration.TryGetValue(EnableKey, out string? value) &&
        string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);
}
