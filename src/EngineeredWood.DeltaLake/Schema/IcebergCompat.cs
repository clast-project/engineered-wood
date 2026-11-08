// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Schema;

/// <summary>
/// Iceberg compatibility versions supported by Delta Lake.
/// These table features constrain how data files and metadata are written
/// so that the table can be converted to Apache Iceberg format by an external tool.
/// </summary>
public enum IcebergCompatVersion
{
    /// <summary>No Iceberg compatibility constraints.</summary>
    None,

    /// <summary>
    /// V1: column mapping required, no deletion vectors, no Map/Array/Void types,
    /// partition columns materialized in Parquet files, numRecords required in stats.
    /// </summary>
    V1,

    /// <summary>
    /// V2: column mapping required, no deletion vectors, Map/Array types allowed with nested field IDs,
    /// partition columns materialized in Parquet files, numRecords required in stats.
    /// </summary>
    V2,
}

/// <summary>
/// Validation and configuration for the <c>icebergCompatV1</c> and
/// <c>icebergCompatV2</c> writer table features.
/// These features do not produce Iceberg metadata — they ensure that
/// the Delta table's Parquet files and schema are structured so that an
/// external converter (e.g. UniForm) can produce valid Iceberg metadata.
/// </summary>
public static class IcebergCompat
{
    /// <summary>Table property to enable Iceberg compatibility V1.</summary>
    public const string EnableV1Key = "delta.enableIcebergCompatV1";

    /// <summary>Table property to enable Iceberg compatibility V2.</summary>
    public const string EnableV2Key = "delta.enableIcebergCompatV2";

    /// <summary>
    /// Returns the active Iceberg compatibility version for the table,
    /// or <see cref="IcebergCompatVersion.None"/> if neither is enabled.
    /// V2 takes precedence if both are somehow enabled.
    /// </summary>
    public static IcebergCompatVersion GetVersion(IReadOnlyDictionary<string, string>? configuration)
    {
        if (configuration is null)
            return IcebergCompatVersion.None;

        if (configuration.TryGetValue(EnableV2Key, out string? v2) &&
            string.Equals(v2, "true", StringComparison.OrdinalIgnoreCase))
            return IcebergCompatVersion.V2;

        if (configuration.TryGetValue(EnableV1Key, out string? v1) &&
            string.Equals(v1, "true", StringComparison.OrdinalIgnoreCase))
            return IcebergCompatVersion.V1;

        return IcebergCompatVersion.None;
    }

    /// <summary>
    /// Validates that the table configuration and protocol satisfy the
    /// Iceberg compatibility constraints for the active version.
    /// Throws <see cref="DeltaFormatException"/> on violations.
    /// </summary>
    /// <param name="version">The version <paramref name="metadata"/> enables.</param>
    /// <param name="metadata">The table's metadata as the commit leaves it.</param>
    /// <param name="protocol">The table's protocol as the commit leaves it.</param>
    /// <param name="previousMetadata">The metadata the commit is based on, or null for a new table.</param>
    /// <param name="previousProtocol">The protocol the commit is based on, or null for a new table.</param>
    public static void Validate(
        IcebergCompatVersion version,
        Actions.MetadataAction metadata,
        Actions.ProtocolAction protocol,
        Actions.MetadataAction? previousMetadata = null,
        Actions.ProtocolAction? previousProtocol = null)
    {
        if (version == IcebergCompatVersion.None)
            return;

        ValidateColumnMapping(metadata.Configuration);
        ValidateSchema(metadata, version);

        ValidateNoDeletionVectors(version, metadata, protocol, previousMetadata, previousProtocol);
    }

    /// <summary>
    /// Returns true if partition columns must be materialized into Parquet
    /// data files for the given Iceberg compatibility version.
    /// </summary>
    public static bool RequiresPartitionMaterialization(IcebergCompatVersion version) =>
        version != IcebergCompatVersion.None;

    /// <summary>
    /// Returns true if stats must include <c>numRecords</c> for the given version.
    /// </summary>
    public static bool RequiresNumRecords(IcebergCompatVersion version) =>
        version != IcebergCompatVersion.None;

    private static void ValidateColumnMapping(IReadOnlyDictionary<string, string>? configuration)
    {
        var mode = ColumnMapping.GetMode(configuration);
        if (mode == ColumnMappingMode.None)
        {
            throw new DeltaFormatException(
                DeltaErrorCodes.IcebergCompatViolation,
                "Iceberg compatibility requires column mapping to be enabled " +
                "in 'name' or 'id' mode.");
        }
    }

    private static void ValidateSchema(Actions.MetadataAction metadata, IcebergCompatVersion version)
    {
        var schema = DeltaSchemaSerializer.Parse(metadata.SchemaString);
        ValidateFieldTypes(schema.Fields, version);
    }

    private static void ValidateFieldTypes(IReadOnlyList<StructField> fields, IcebergCompatVersion version)
    {
        foreach (var field in fields)
            ValidateFieldType(field, version);
    }

    private static void ValidateFieldType(StructField field, IcebergCompatVersion version)
    {
        switch (field.Type)
        {
            case ArrayType a when version == IcebergCompatVersion.V1:
                throw new DeltaFormatException(
                    DeltaErrorCodes.IcebergCompatViolation,
                    $"IcebergCompatV1 does not allow array types. " +
                    $"Field '{field.Name}' has type array.");

            case MapType m when version == IcebergCompatVersion.V1:
                throw new DeltaFormatException(
                    DeltaErrorCodes.IcebergCompatViolation,
                    $"IcebergCompatV1 does not allow map types. " +
                    $"Field '{field.Name}' has type map.");

            case PrimitiveType p when p.TypeName == "void":
                throw new DeltaFormatException(
                    DeltaErrorCodes.IcebergCompatViolation,
                    $"Iceberg compatibility does not allow void types. " +
                    $"Field '{field.Name}' has type void.");

            case StructType s:
                ValidateFieldTypes(s.Fields, version);
                break;

            case ArrayType a when version == IcebergCompatVersion.V2:
                ValidateFieldTypeRecursive(a.ElementType, field.Name + ".element", version);
                break;

            case MapType m when version == IcebergCompatVersion.V2:
                ValidateFieldTypeRecursive(m.KeyType, field.Name + ".key", version);
                ValidateFieldTypeRecursive(m.ValueType, field.Name + ".value", version);
                break;
        }
    }

    private static void ValidateFieldTypeRecursive(
        DeltaDataType type, string path, IcebergCompatVersion version)
    {
        switch (type)
        {
            case PrimitiveType p when p.TypeName == "void":
                throw new DeltaFormatException(
                    DeltaErrorCodes.IcebergCompatViolation,
                    $"Iceberg compatibility does not allow void types at '{path}'.");

            case StructType s:
                ValidateFieldTypes(s.Fields, version);
                break;

            case ArrayType a:
                ValidateFieldTypeRecursive(a.ElementType, path + ".element", version);
                break;

            case MapType m:
                ValidateFieldTypeRecursive(m.KeyType, path + ".key", version);
                ValidateFieldTypeRecursive(m.ValueType, path + ".value", version);
                break;
        }
    }

    // delta-spark's V1 and V2 both carry CheckDeletionVectorDisabled (read from the 4.0.0, 4.3.1 and 4.4.1 jars;
    // V3, which this library does not model, has none) (#744). It refuses a commit whose protocol supports
    // deletion vectors when they are WRITABLE, meaning supported and enabled by delta.enableDeletionVectors, either
    // in the snapshot the commit is based on or in the table as the commit leaves it. A table that declares the
    // feature with the property off passes. One whose base has them writable is refused even by the commit that
    // turns the property off. Spark's REORG ... APPLY (UPGRADE UNIFORM) checks only the new state, but this
    // library has no such operation.
    private static void ValidateNoDeletionVectors(
        IcebergCompatVersion version,
        Actions.MetadataAction metadata,
        Actions.ProtocolAction protocol,
        Actions.MetadataAction? previousMetadata,
        Actions.ProtocolAction? previousProtocol)
    {
        if (!SupportsDeletionVectors(protocol))
            return;

        bool writable = DeletionVectors.DeletionVectorConfig.IsEnabled(metadata.Configuration);
        bool wereWritable = previousMetadata is not null && previousProtocol is not null
            && SupportsDeletionVectors(previousProtocol)
            && DeletionVectors.DeletionVectorConfig.IsEnabled(previousMetadata.Configuration);
        if (writable || wereWritable)
        {
            throw new DeltaFormatException(
                DeltaErrorCodes.IcebergCompatViolation,
                $"IcebergCompat{version} requires that deletion vectors are not enabled: " +
                $"'{DeletionVectors.DeletionVectorConfig.EnableKey}' must be false, and must have been false " +
                "before this commit.");
        }
    }

    private static bool SupportsDeletionVectors(Actions.ProtocolAction protocol) =>
        (protocol.WriterFeatures is not null &&
         protocol.WriterFeatures.Contains(DeletionVectors.DeletionVectorConfig.FeatureName)) ||
        (protocol.ReaderFeatures is not null &&
         protocol.ReaderFeatures.Contains(DeletionVectors.DeletionVectorConfig.FeatureName));
}
