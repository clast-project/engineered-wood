// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;

namespace EngineeredWood.DeltaLake;

/// <summary>
/// The Delta <c>variantShredding</c> table feature (#671): whether a table's data files may store a variant
/// column in the SHREDDED Parquet layout, with part of each value hoisted into a typed <c>typed_value</c> child.
/// </summary>
/// <remarks>
/// <para>PROTOCOL.md, "Variant Shredding": the feature depends on <c>variantType</c>, is a reader-writer feature
/// listed in both <c>readerFeatures</c> and <c>writerFeatures</c>, and is ENABLED only when
/// <see cref="EnableKey"/> is <c>true</c>. "When Variant Shredding is not enabled, a variant column must not be
/// written as a shredded Variant." Once enabled, a table may mix shredded and unshredded files, so a writer that
/// never shreds is always conformant.</para>
///
/// <para>Two names exist. <see cref="PreviewFeatureName"/> is what delta-spark 4.0 wrote; <see cref="FeatureName"/>
/// arrived in delta-spark 4.1, which enables it from the property unless the table already has the preview name
/// (verified in delta-spark 4.3.1's <c>VariantShreddingTableFeature.metadataRequiresFeatureToBeEnabled</c>). Either
/// one lets a table shred. Only the GA name requires <c>variantType</c>.</para>
/// </remarks>
public static class VariantShreddingConfig
{
    /// <summary>The table property that enables shredded writes. Default <c>false</c>.</summary>
    public const string EnableKey = "delta.enableVariantShredding";

    /// <summary>The GA reader-writer feature name.</summary>
    public const string FeatureName = "variantShredding";

    /// <summary>The preview reader-writer feature name, which delta-spark 4.0 wrote.</summary>
    public const string PreviewFeatureName = "variantShredding-preview";

    /// <summary>True when <see cref="EnableKey"/> is <c>true</c> (any case).</summary>
    public static bool IsEnabled(IReadOnlyDictionary<string, string>? configuration) =>
        configuration is not null &&
        configuration.TryGetValue(EnableKey, out string? value) &&
        string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);

    /// <summary>True when <paramref name="protocol"/> lists either feature name as a writer feature.</summary>
    public static bool IsSupported(ProtocolAction? protocol) =>
        protocol?.WriterFeatures is { } features
        && (features.Contains(FeatureName) || features.Contains(PreviewFeatureName));

    /// <summary>
    /// True when a writer may shred a variant column of this table: the protocol supports the feature AND the
    /// metadata enables it. Spark's own write check reads the property alone, because Spark adds the feature to
    /// the protocol whenever the property is set; this asks both, since a table written by another engine can
    /// carry the property without the feature, and a reader that honours only the protocol would not expect a
    /// shredded file there.
    /// </summary>
    public static bool AllowsShreddedWrites(ProtocolAction? protocol, IReadOnlyDictionary<string, string>? configuration) =>
        IsEnabled(configuration) && IsSupported(protocol);
}
