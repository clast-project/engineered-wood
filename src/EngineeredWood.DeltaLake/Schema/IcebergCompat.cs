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
    /// V2: column mapping required, Map/Array types allowed with nested field IDs,
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
    /// Metadata key for nested field IDs used by IcebergCompatV2.
    /// Stored on the ancestor <see cref="StructField"/> that contains
    /// Array or Map children.
    /// </summary>
    public const string NestedFieldIdsKey = "parquet.field.nested.ids";

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
    public static void Validate(
        IcebergCompatVersion version,
        Actions.MetadataAction metadata,
        Actions.ProtocolAction protocol)
    {
        if (version == IcebergCompatVersion.None)
            return;

        ValidateColumnMapping(metadata.Configuration);
        ValidateSchema(metadata, version);

        if (version == IcebergCompatVersion.V1)
            ValidateNoDeletionVectors(protocol);
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
                    $"IcebergCompatV1 does not allow array types. " +
                    $"Field '{field.Name}' has type array.");

            case MapType m when version == IcebergCompatVersion.V1:
                throw new DeltaFormatException(
                    $"IcebergCompatV1 does not allow map types. " +
                    $"Field '{field.Name}' has type map.");

            case PrimitiveType p when p.TypeName == "void":
                throw new DeltaFormatException(
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

    private static void ValidateNoDeletionVectors(Actions.ProtocolAction protocol)
    {
        bool hasDeletionVectors =
            (protocol.WriterFeatures is not null &&
             protocol.WriterFeatures.Contains("deletionVectors")) ||
            (protocol.ReaderFeatures is not null &&
             protocol.ReaderFeatures.Contains("deletionVectors"));

        if (hasDeletionVectors)
        {
            throw new DeltaFormatException(
                "IcebergCompatV1 requires that deletion vectors are not enabled. " +
                "The 'deletionVectors' feature must be removed from the protocol.");
        }
    }
}
