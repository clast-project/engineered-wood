// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
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

    /// <summary>The protocol-floor properties, as Spark spells them. Matched without regard to case.</summary>
    public const string MinReaderVersionKey = "delta.minReaderVersion";

    /// <inheritdoc cref="MinReaderVersionKey"/>
    public const string MinWriterVersionKey = "delta.minWriterVersion";

    /// <summary>True when <paramref name="key"/> is a <c>delta.feature.*</c> property.</summary>
    public static bool IsFeatureProperty(string key) =>
        key.StartsWith(Prefix, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// True when <paramref name="key"/> is a property Spark consumes into the protocol rather than storing:
    /// <c>delta.feature.*</c>, <c>delta.minReaderVersion</c> or <c>delta.minWriterVersion</c>.
    /// </summary>
    public static bool IsProtocolProperty(string key) =>
        IsFeatureProperty(key)
        || string.Equals(key, MinReaderVersionKey, StringComparison.OrdinalIgnoreCase)
        || string.Equals(key, MinWriterVersionKey, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Removes <c>delta.minReaderVersion</c> / <c>delta.minWriterVersion</c> from <paramref name="configuration"/>
    /// and returns the legacy protocol a new table starts from: <c>(1, 2)</c> when neither is given (#639), and
    /// whether either was.
    /// </summary>
    /// <remarks>
    /// <para>Measured against delta-spark 4.0.0. The two versions are a floor whose implied features the table
    /// supports, expressed as the smallest legacy protocol that implies exactly those, so a floor can come out
    /// LOWER than asked. Writer 7 (table features, none named) implies nothing, so it is <c>(1, 1)</c> and drops the
    /// <c>appendOnly</c>/<c>invariants</c> a default table has. <c>columnMapping</c> needs reader 2 as well as writer
    /// 5, so writer 5 at reader 1 is <c>(1, 4)</c>, and a reader version matters only with writer 5 or 6. A missing
    /// version takes the default's: writer 2, reader 1.</para>
    /// <para>An explicit floor also changes how the table's other needs are met. Without one, Spark raises a legacy
    /// version for a legacy feature the metadata needs (a CHECK constraint makes writer 3). With one, it names the
    /// feature instead (writer 7, <c>[checkConstraints]</c> at a floor of writer 1 or 7), and only the exact feature
    /// set is reduced back to a legacy version.</para>
    /// <para>The value must be one of 1-7 (writer) or 1-3 (reader), digits only. Spark refuses anything else with a
    /// bare <c>IllegalArgumentException</c>, no Delta error class.</para>
    /// </remarks>
    public static (int Reader, int Writer, bool Explicit) ExtractProtocolFloor(
        Dictionary<string, string>? configuration)
    {
        int? reader = TakeVersion(configuration, MinReaderVersionKey, 3);
        int? writer = TakeVersion(configuration, MinWriterVersionKey, 7);
        if (reader is null && writer is null)
            return (1, 2, false);

        int r = reader ?? 1;
        int w = writer ?? 2;
        if (w == 7)
            return (1, 1, true);
        if (w == 5 && r < 2)
            return (1, 4, true);
        return (w >= 5 && r >= 2 ? 2 : 1, w, true);
    }

    private static int? TakeVersion(Dictionary<string, string>? configuration, string name, int max)
    {
        if (configuration is null)
            return null;
        int? result = null;
        var keys = configuration.Keys
            .Where(k => string.Equals(k, name, StringComparison.OrdinalIgnoreCase))
            .ToList();
        foreach (string key in keys)
        {
            string value = configuration[key];
            if (!int.TryParse(value, NumberStyles.None, CultureInfo.InvariantCulture, out int version)
                || version < 1 || version > max)
            {
                throw new DeltaFormatException(
                    DeltaTableErrorCodes.InvalidProtocolVersionProperty,
                    $"{name} needs to be one of {string.Join(", ", Enumerable.Range(1, max))}, "
                    + $"but '{key}' is \"{value}\".");
            }
            configuration.Remove(key);
            result = Math.Max(result ?? 0, version);
        }
        return result;
    }

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
    /// features otherwise. A side already in table-features mode stays there, unless
    /// <paramref name="fromScratch"/>: then any legacy version from 1 up may describe the set, as for a table given an
    /// explicit protocol floor.
    /// </summary>
    public static ProtocolAction Declare(
        ProtocolAction protocol,
        IReadOnlyList<string> features,
        Func<int, List<string>> legacyWriterFeatures,
        Func<int, List<string>> legacyReaderFeatures,
        bool fromScratch = false)
    {
        var writer = protocol.MinWriterVersion >= 7
            ? new List<string>(protocol.WriterFeatures ?? [])
            : LegacyWriterFeaturesAtReader(protocol, legacyWriterFeatures);
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

        // columnMapping is implied by a legacy writer version only at reader 2, so a set without it compares against
        // the legacy sets without it: (1, 6) is the identity-columns protocol of a table with no column mapping.
        bool readerTwo = reader.Contains("columnMapping");
        int writerVersion = 7;
        if (protocol.MinWriterVersion < 7 || fromScratch)
        {
            for (int version = fromScratch ? 1 : protocol.MinWriterVersion; version < 7; version++)
            {
                var candidate = legacyWriterFeatures(version);
                if (!readerTwo)
                    candidate.Remove("columnMapping");
                if (SameSet(writer, candidate))
                {
                    writerVersion = version;
                    break;
                }
            }
        }

        int readerVersion = 3;
        if (protocol.MinReaderVersion < 3 || fromScratch)
        {
            for (int version = fromScratch ? 1 : protocol.MinReaderVersion; version < 3; version++)
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

    /// <summary>
    /// The writer features a legacy <paramref name="protocol"/> implies. <c>columnMapping</c> is implied only at
    /// reader 2 or above, as Spark reads it: a <c>(1, 6)</c> table (from a <c>delta.minWriterVersion</c> of 6) does
    /// not support it, and listing it at writer 7 without the reader side would be an invalid protocol.
    /// </summary>
    public static List<string> LegacyWriterFeaturesAtReader(
        ProtocolAction protocol, Func<int, List<string>> legacyWriterFeatures)
    {
        var features = legacyWriterFeatures(protocol.MinWriterVersion);
        if (protocol.MinReaderVersion < 2)
            features.Remove("columnMapping");
        return features;
    }

    private static bool SameSet(List<string> a, List<string> b) =>
        a.Count == b.Count && a.All(b.Contains);
}
