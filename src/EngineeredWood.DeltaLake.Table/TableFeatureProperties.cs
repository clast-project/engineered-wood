// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using EngineeredWood.DeltaLake.Actions;

namespace EngineeredWood.DeltaLake.Table;

/// <summary>
/// <c>delta.feature.&lt;name&gt; = supported</c> table properties, which declare a table feature as supported in
/// the protocol without enabling it (#601). The rules follow delta-spark 4.0.0, measured:
/// <list type="bullet">
/// <item>The key is matched without regard to case, prefix and feature name alike, and so is the value, which
/// must be <c>supported</c> or <c>enabled</c> (<c>DELTA_UNSUPPORTED_FEATURE_STATUS</c> otherwise). A name that
/// is not a feature is refused with <c>DELTA_UNSUPPORTED_FEATURES_IN_CONFIG</c>.</item>
/// <item>The property is consumed, never stored in <c>metaData.configuration</c>.</item>
/// <item>The feature joins the protocol with the features it requires (<c>rowTracking</c> needs
/// <c>domainMetadata</c>, <c>icebergCompatV1</c>/<c>V2</c> need <c>columnMapping</c>). A feature set that some
/// legacy version describes exactly keeps the legacy version: <c>checkConstraints</c> alone is writer 3, while
/// <c>changeDataFeed</c> alone is writer 7, because writer 4 would also imply <c>generatedColumns</c>.</item>
/// <item><c>clustering</c> is refused: a clustered table is created with clustering columns.</item>
/// </list>
/// </summary>
internal static class TableFeatureProperties
{
    /// <summary>The property prefix, as Spark spells it. Matched without regard to case.</summary>
    public const string Prefix = "delta.feature.";

    private const string ClusteringFeature = "clustering";

    // The features this library can write, each with whether readers must understand it too and what it requires.
    // A feature Spark knows and this library cannot write is refused as unrecognised: it could not be honoured.
    private static readonly Dictionary<string, (bool ReaderWriter, string[] Requires)> Known =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["appendOnly"] = (false, []),
            ["invariants"] = (false, []),
            ["checkConstraints"] = (false, []),
            ["changeDataFeed"] = (false, []),
            ["generatedColumns"] = (false, []),
            ["columnMapping"] = (true, []),
            ["identityColumns"] = (false, []),
            ["deletionVectors"] = (true, []),
            ["rowTracking"] = (false, ["domainMetadata"]),
            ["timestampNtz"] = (true, []),
            ["domainMetadata"] = (false, []),
            ["v2Checkpoint"] = (true, []),
            ["icebergCompatV1"] = (false, ["columnMapping"]),
            ["icebergCompatV2"] = (false, ["columnMapping"]),
            ["inCommitTimestamp"] = (false, []),
            ["typeWidening"] = (true, []),
            ["vacuumProtocolCheck"] = (true, []),
            ["variantType"] = (true, []),
            [ClusteringFeature] = (false, ["domainMetadata"]),
        };

    /// <summary>True when <paramref name="key"/> is a <c>delta.feature.*</c> property.</summary>
    public static bool IsFeatureProperty(string key) =>
        key.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Removes every <c>delta.feature.*</c> property from <paramref name="configuration"/> and returns the
    /// features they declare, by their canonical names and in the order given, followed by the features those
    /// require. Refuses a property whose value is not <c>supported</c>/<c>enabled</c>, one naming no feature this
    /// library can write, and <c>clustering</c>.
    /// </summary>
    public static List<string> Extract(Dictionary<string, string>? configuration)
    {
        var features = new List<string>();
        if (configuration is null)
            return features;

        var keys = configuration.Keys.Where(IsFeatureProperty).ToList();
        foreach (string key in keys)
        {
            string name = key.Substring(Prefix.Length);
            string value = configuration[key];
            if (!string.Equals(value, "supported", StringComparison.OrdinalIgnoreCase)
                && !string.Equals(value, "enabled", StringComparison.OrdinalIgnoreCase))
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.UnsupportedFeatureStatus,
                    $"Expecting the status for table feature '{name}' (property '{key}') to be \"supported\", "
                    + $"but got \"{value}\".");
            }

            string? canonical = Known.Keys.FirstOrDefault(
                k => string.Equals(k, name, StringComparison.OrdinalIgnoreCase));
            if (canonical is null)
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.UnsupportedFeaturesInConfig,
                    $"Table property '{key}' names a table feature this implementation does not recognise or "
                    + "cannot write.");
            }
            if (canonical == ClusteringFeature)
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.ClusteringFeatureViaProperty,
                    $"The clustering table feature cannot be declared with '{key}'. Create the table with "
                    + "clustering columns instead.");
            }

            configuration.Remove(key);
            if (!features.Contains(canonical))
                features.Add(canonical);
        }

        for (int i = 0; i < features.Count; i++)
        {
            foreach (string required in Known[features[i]].Requires)
            {
                if (!features.Contains(required))
                    features.Add(required);
            }
        }
        return features;
    }

    /// <summary>
    /// <paramref name="protocol"/> with <paramref name="features"/> (from <see cref="Extract"/>) supported as
    /// well. The full feature sets are computed (a legacy version stands for the features it implies), then
    /// expressed as legacy versions where a legacy version describes a set exactly, as Spark does, and as named
    /// features otherwise. A side already in table-features mode stays there.
    /// </summary>
    public static ProtocolAction Declare(
        ProtocolAction protocol,
        IReadOnlyList<string> features,
        Func<int, List<string>> legacyWriterFeatures,
        Func<int, List<string>> legacyReaderFeatures)
    {
        var writer = protocol.MinWriterVersion >= 7
            ? new List<string>(protocol.WriterFeatures ?? [])
            : legacyWriterFeatures(protocol.MinWriterVersion);
        var reader = protocol.MinReaderVersion >= 3
            ? new List<string>(protocol.ReaderFeatures ?? [])
            : legacyReaderFeatures(protocol.MinReaderVersion);

        foreach (string feature in features)
        {
            if (!writer.Contains(feature))
                writer.Add(feature);
            if (Known[feature].ReaderWriter && !reader.Contains(feature))
                reader.Add(feature);
        }

        int writerVersion = 7;
        if (protocol.MinWriterVersion < 7)
        {
            for (int version = protocol.MinWriterVersion; version < 7; version++)
            {
                if (SameSet(writer, legacyWriterFeatures(version)))
                {
                    writerVersion = version;
                    break;
                }
            }
        }

        int readerVersion = 3;
        if (protocol.MinReaderVersion < 3)
        {
            for (int version = protocol.MinReaderVersion; version < 3; version++)
            {
                if (SameSet(reader, legacyReaderFeatures(version)))
                {
                    readerVersion = version;
                    break;
                }
            }
        }

        return new ProtocolAction
        {
            MinReaderVersion = readerVersion,
            MinWriterVersion = writerVersion,
            ReaderFeatures = readerVersion >= 3 ? reader : null,
            WriterFeatures = writerVersion >= 7 ? writer : null,
        };
    }

    private static bool SameSet(List<string> a, List<string> b) =>
        a.Count == b.Count && a.All(b.Contains);
}
