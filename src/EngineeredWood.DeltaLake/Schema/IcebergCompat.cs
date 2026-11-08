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

    /// <summary>
    /// V3 (delta-spark 4.3+, enforced from 4.4): column mapping AND row tracking required, deletion vectors
    /// allowed, V2's types plus variant, nested field ids, partition columns materialized in Parquet files,
    /// numRecords required in stats. It cannot be turned off once on.
    /// </summary>
    V3,
}

/// <summary>
/// Validation and configuration for the <c>icebergCompatV1</c>, <c>icebergCompatV2</c> and
/// <c>icebergCompatV3</c> writer table features.
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

    /// <summary>Table property to enable Iceberg compatibility V3.</summary>
    public const string EnableV3Key = "delta.enableIcebergCompatV3";

    /// <summary>The writer table feature <see cref="EnableV1Key"/> requires.</summary>
    public const string FeatureNameV1 = "icebergCompatV1";

    /// <summary>The writer table feature <see cref="EnableV2Key"/> requires.</summary>
    public const string FeatureNameV2 = "icebergCompatV2";

    /// <summary>The writer table feature <see cref="EnableV3Key"/> requires.</summary>
    public const string FeatureNameV3 = "icebergCompatV3";

    /// <summary>
    /// The Parquet field id an IcebergCompatV3 table gives the materialized row-id column: Iceberg's reserved id
    /// for <c>_row_id</c> (delta-spark's <c>IcebergConstants.ICEBERG_ROW_TRACKING_ROW_ID_FIELD_ID</c>).
    /// </summary>
    public const int RowIdFieldId = 2147483540;

    /// <summary>
    /// The Parquet field id an IcebergCompatV3 table gives the materialized row-commit-version column: Iceberg's
    /// reserved id for <c>_last_updated_sequence_number</c>.
    /// </summary>
    public const int RowCommitVersionFieldId = 2147483539;

    // delta-spark's DeltaColumnDefaults reads a column's default from this field-metadata key
    // (ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY).
    private const string CurrentDefaultKey = "CURRENT_DEFAULT";

    /// <summary>
    /// Returns the active Iceberg compatibility version for the table,
    /// or <see cref="IcebergCompatVersion.None"/> if none is enabled.
    /// The highest version wins if several are enabled; <see cref="Validate"/> refuses that when V3 is one of them.
    /// </summary>
    public static IcebergCompatVersion GetVersion(IReadOnlyDictionary<string, string>? configuration)
    {
        if (IsTrue(configuration, EnableV3Key))
            return IcebergCompatVersion.V3;
        if (IsTrue(configuration, EnableV2Key))
            return IcebergCompatVersion.V2;
        if (IsTrue(configuration, EnableV1Key))
            return IcebergCompatVersion.V1;
        return IcebergCompatVersion.None;
    }

    /// <summary>The writer table feature that <paramref name="version"/> requires.</summary>
    public static string FeatureName(IcebergCompatVersion version) => version switch
    {
        IcebergCompatVersion.V1 => FeatureNameV1,
        IcebergCompatVersion.V2 => FeatureNameV2,
        IcebergCompatVersion.V3 => FeatureNameV3,
        _ => throw new ArgumentOutOfRangeException(nameof(version), version, null),
    };

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
    /// <param name="altersTable">True when the commit changes an existing table in place, as delta-spark's
    /// <c>ALTER TABLE ... SET TBLPROPERTIES</c> does; false for a create, a replace or a restore.</param>
    public static void Validate(
        IcebergCompatVersion version,
        Actions.MetadataAction metadata,
        Actions.ProtocolAction protocol,
        Actions.MetadataAction? previousMetadata = null,
        Actions.ProtocolAction? previousProtocol = null,
        bool altersTable = false)
    {
        // delta-spark runs a version's checks when the commit enables it, and also when the commit turns it off.
        // Only V3 has a check that the second case can fail.
        ValidateNotDisabled(version, previousMetadata);

        if (version == IcebergCompatVersion.None)
            return;

        if (version == IcebergCompatVersion.V3)
        {
            ValidateV3(metadata, protocol, previousMetadata, previousProtocol, altersTable);
            return;
        }

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

    /// <summary>
    /// Returns true if array elements and map keys/values carry column ids
    /// (<c>delta.columnMapping.nested.ids</c>) under the given version: V2 and V3, as delta-spark assigns them
    /// whenever a version from 2 up is enabled.
    /// </summary>
    public static bool RequiresNestedIds(IcebergCompatVersion version) =>
        version is IcebergCompatVersion.V2 or IcebergCompatVersion.V3;

    /// <summary>
    /// The Parquet field ids that the materialized row-tracking columns carry in a data file of a table with
    /// <paramref name="configuration"/>, keyed by physical column name; null when they carry none. Only an
    /// IcebergCompatV3 table gives them ids: Iceberg's reserved row-lineage ids (<see cref="RowIdFieldId"/>,
    /// <see cref="RowCommitVersionFieldId"/>), as delta-spark 4.4 stamps them (measured, name and id mode).
    /// </summary>
    public static IReadOnlyDictionary<string, int>? RowTrackingFieldIds(
        IReadOnlyDictionary<string, string>? configuration)
    {
        if (GetVersion(configuration) != IcebergCompatVersion.V3)
            return null;
        var (rowId, rowCommitVersion) = RowTracking.RowTrackingConfig.TryGetMaterializedColumnNames(configuration);
        var ids = new Dictionary<string, int>(StringComparer.Ordinal);
        if (rowId is not null)
            ids[rowId] = RowIdFieldId;
        if (rowCommitVersion is not null)
            ids[rowCommitVersion] = RowCommitVersionFieldId;
        return ids.Count == 0 ? null : ids;
    }

    private static bool IsTrue(IReadOnlyDictionary<string, string>? configuration, string key) =>
        configuration is not null
        && configuration.TryGetValue(key, out string? value)
        && string.Equals(value, "true", StringComparison.OrdinalIgnoreCase);

    private static DeltaFormatException Violation(string message) =>
        new(DeltaErrorCodes.IcebergCompatViolation, message);

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
    // V3 has none, so this is never called for it) (#744). It refuses a commit whose protocol supports
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

    // ---- IcebergCompatV3 -------------------------------------------------------------------------------------
    //
    // Each rule below is delta-spark's IcebergCompatV3 check of the same name, read with javap from the 4.3.1 and
    // 4.4.1 jars (the two are the same apart from the geospatial types, which this library does not model) and
    // measured against Spark 4.2.0 + delta-spark 4.4.1. delta-spark 4.3.1 declares the feature but leaves V3 out of
    // IcebergCompat.knownVersions, so it enforces none of them (measured on Spark 4.1.2): this follows 4.4.

    // CheckCannotDisableCompat: an existing table that has V3 on must keep it, through ALTER, REPLACE and RESTORE
    // alike ("IcebergCompat cannot be disabled", measured for ALTER and REPLACE).
    private static void ValidateNotDisabled(IcebergCompatVersion version, Actions.MetadataAction? previousMetadata)
    {
        if (previousMetadata is not null
            && GetVersion(previousMetadata.Configuration) == IcebergCompatVersion.V3
            && version != IcebergCompatVersion.V3)
        {
            throw Violation(
                $"IcebergCompatV3 cannot be disabled: '{EnableV3Key}' must stay true on a table that has it.");
        }
    }

    private static void ValidateV3(
        Actions.MetadataAction metadata,
        Actions.ProtocolAction protocol,
        Actions.MetadataAction? previousMetadata,
        Actions.ProtocolAction? previousProtocol,
        bool altersTable)
    {
        var configuration = metadata.Configuration;

        // CheckOnlySingleVersionEnabled.
        if (IsTrue(configuration, EnableV1Key) || IsTrue(configuration, EnableV2Key))
        {
            throw Violation(
                "Only one IcebergCompat version can be enabled: IcebergCompatV3 is, so " +
                $"'{EnableV1Key}' and '{EnableV2Key}' must not be.");
        }

        // RequireColumnMapping: name or id.
        ValidateColumnMapping(configuration);

        // RequireRowTracking: delta.enableRowTracking = true, and the rowTracking feature the V3 feature requires.
        // Spark sets the property when the caller leaves it out; a caller's 'false' is refused (measured).
        if (!RowTracking.RowTrackingConfig.IsEnabled(configuration)
            || !RowTracking.RowTrackingConfig.IsSupported(protocol))
        {
            throw Violation(
                $"IcebergCompatV3 requires row tracking: '{RowTracking.RowTrackingConfig.EnableKey}' must be true " +
                $"and the protocol must support the '{RowTracking.RowTrackingConfig.FeatureName}' feature.");
        }

        // CheckNoRowTrackingBeforeUpgradeV3: Spark refuses an ALTER TABLE SET TBLPROPERTIES that turns V3 on for a
        // table whose row tracking was already enabled. Its rows' ids predate V3, and the materialized ones carry no
        // Iceberg field ids. Only that operation: a REPLACE or a RESTORE is not checked.
        if (altersTable
            && previousMetadata is not null
            && GetVersion(previousMetadata.Configuration) != IcebergCompatVersion.V3
            && RowTracking.RowTrackingConfig.IsSupported(previousProtocol)
            && RowTracking.RowTrackingConfig.IsEnabled(previousMetadata.Configuration))
        {
            throw Violation(
                "IcebergCompatV3 cannot be enabled on an existing table whose row tracking is already enabled. " +
                "Disable row tracking first.");
        }

        var schema = DeltaSchemaSerializer.Parse(metadata.SchemaString);

        // CheckTypeInV3AllowList: V2's allow-list plus variant, at every depth.
        foreach (var field in schema.Fields)
            ValidateV3Type(field.Type, field.Name);

        // CheckPartitionDataTypeInV2AllowList. The list holds DecimalType's companion object, which no decimal
        // column is, so a decimal partition column is refused (measured on 4.4.1).
        foreach (string partitionColumn in metadata.PartitionColumns)
        {
            var field = schema.Fields.FirstOrDefault(f => f.Name == partitionColumn);
            if (field?.Type is not PrimitiveType p || !IsV3PartitionType(p.TypeName))
            {
                throw Violation(
                    $"IcebergCompatV3 does not support the data type {field?.Type.ToString() ?? "(unknown)"} " +
                    $"for partition column '{partitionColumn}'.");
            }
        }

        // CheckNoPartitionEvolution: a partitioned table keeps its partition columns, through a REPLACE too. A
        // table that was not partitioned may become so.
        if (previousMetadata is { PartitionColumns.Count: > 0 }
            && !previousMetadata.PartitionColumns.SequenceEqual(metadata.PartitionColumns, StringComparer.Ordinal))
        {
            throw Violation(
                "IcebergCompatV3 does not allow changing the partition columns of a partitioned table: " +
                $"[{string.Join(", ", previousMetadata.PartitionColumns)}] to " +
                $"[{string.Join(", ", metadata.PartitionColumns)}].");
        }

        // CheckTypeWideningSupported: on a table that supports type widening, every type change recorded in the
        // schema must be one Iceberg can follow.
        if (protocol.WriterFeatures is { } writerFeatures
            && (writerFeatures.Contains("typeWidening") || writerFeatures.Contains("typeWidening-preview")))
        {
            ValidateV3TypeChanges(schema.Fields);
        }

        // CheckColumnDefaultIsLiteral: a top-level column's default must be a literal of its type.
        foreach (var field in schema.Fields)
        {
            if (field.Metadata is not null
                && field.Metadata.TryGetValue(CurrentDefaultKey, out string? defaultText)
                && !ColumnDefaultLiteral.IsLiteral(defaultText, field.Type))
            {
                throw Violation(
                    $"IcebergCompatV3 requires a column default to be a literal: column '{field.Name}' " +
                    $"defaults to '{defaultText}'.");
            }
        }
    }

    private static void ValidateV3Type(DeltaDataType type, string path)
    {
        switch (type)
        {
            case StructType s:
                foreach (var field in s.Fields)
                    ValidateV3Type(field.Type, path + "." + field.Name);
                break;
            case ArrayType a:
                ValidateV3Type(a.ElementType, path + ".element");
                break;
            case MapType m:
                ValidateV3Type(m.KeyType, path + ".key");
                ValidateV3Type(m.ValueType, path + ".value");
                break;
            case PrimitiveType p when !IsV3Type(p.TypeName):
                throw Violation($"IcebergCompatV3 does not support the data type {p.TypeName} at '{path}'.");
        }
    }

    // CheckTypeInV2AllowList's primitives, which CheckTypeInV3AllowList extends with variant.
    private static bool IsV3Type(string typeName) =>
        typeName == "variant" || IsV3PartitionType(typeName) || IsDecimal(typeName);

    private static bool IsV3PartitionType(string typeName) => typeName is
        "byte" or "short" or "integer" or "long" or "float" or "double" or "string" or "binary" or "boolean"
        or "timestamp" or "timestamp_ntz" or "date";

    private static bool IsDecimal(string typeName) =>
        typeName.StartsWith("decimal(", StringComparison.Ordinal);

    private static void ValidateV3TypeChanges(IReadOnlyList<StructField> fields)
    {
        foreach (var field in fields)
        {
            foreach (var change in TypeWidening.GetTypeChanges(field))
            {
                // Spark refuses a change only when Delta supports it and Iceberg does not
                // (CheckTypeWideningSupported: `isTypeChangeSupported(from, to) && !isTypeChangeSupportedByIceberg(...)`,
                // javap on 4.4.1). A recorded change Delta does not support, string to integer say, is not this
                // check's to refuse. TypeWidening's table is delta-spark 4.4.1's isTypeChangeSupported: wider
                // integers, float to double, date to timestamp_ntz, integer to double, and the decimal widenings.
                bool deltaSupports = TypeWidening.IsSupported(change.FromType, change.ToType)
                    || TypeWidening.IsDecimalWidening(change.FromType, change.ToType);
                if (deltaSupports && !IsIcebergTypeChange(change.FromType, change.ToType))
                {
                    throw Violation(
                        $"IcebergCompatV3 is incompatible with a type change applied to column '{field.Name}': " +
                        $"{change.FromType} to {change.ToType}.");
                }
            }
            ValidateV3TypeChanges(NestedFields(field.Type));
        }
    }

    private static IReadOnlyList<StructField> NestedFields(DeltaDataType type) => type switch
    {
        StructType s => s.Fields,
        ArrayType a => NestedFields(a.ElementType),
        MapType m => [.. NestedFields(m.KeyType), .. NestedFields(m.ValueType)],
        _ => [],
    };

    // delta-spark's TypeWidening.isTypeChangeSupportedByIceberg: an integer no narrower, float to double, and a
    // decimal at the same scale with no fewer digits.
    private static bool IsIcebergTypeChange(string from, string to)
    {
        if (from == to)
            return true;
        if (IntegralWidth(from) is int fromWidth && IntegralWidth(to) is int toWidth)
            return fromWidth <= toWidth;
        if (from == "float" && to == "double")
            return true;
        if (ColumnDefaultLiteral.TryParseDecimalType(from, out int fromPrecision, out int fromScale)
            && ColumnDefaultLiteral.TryParseDecimalType(to, out int toPrecision, out int toScale))
        {
            return fromScale == toScale && fromPrecision <= toPrecision;
        }
        return false;
    }

    private static int? IntegralWidth(string type) => type switch
    {
        "byte" => 1,
        "short" => 2,
        "integer" => 4,
        "long" => 8,
        _ => null,
    };
}
