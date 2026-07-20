// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Schema;

/// <summary>
/// Column mapping modes supported by Delta Lake.
/// </summary>
public enum ColumnMappingMode
{
    /// <summary>No column mapping; logical names equal physical names.</summary>
    None,

    /// <summary>
    /// Columns are resolved by their <c>delta.columnMapping.id</c> metadata,
    /// which maps to the Parquet <c>field_id</c>.
    /// </summary>
    Id,

    /// <summary>
    /// Columns are resolved by their <c>delta.columnMapping.physicalName</c> metadata,
    /// which is the physical column name in the Parquet file.
    /// </summary>
    Name,
}

/// <summary>
/// Utilities for Delta Lake column mapping.
/// Column mapping enables renaming and dropping columns without rewriting data files.
/// </summary>
public static class ColumnMapping
{
    public const string ModeKey = "delta.columnMapping.mode";
    public const string MaxColumnIdKey = "delta.columnMapping.maxColumnId";
    public const string FieldIdKey = "delta.columnMapping.id";
    public const string PhysicalNameKey = "delta.columnMapping.physicalName";

    /// <summary>
    /// Gets the column mapping mode from table configuration.
    /// </summary>
    public static ColumnMappingMode GetMode(IReadOnlyDictionary<string, string>? configuration)
    {
        if (configuration is null ||
            !configuration.TryGetValue(ModeKey, out string? mode))
            return ColumnMappingMode.None;

        return mode switch
        {
            "none" => ColumnMappingMode.None,
            "id" => ColumnMappingMode.Id,
            "name" => ColumnMappingMode.Name,
            _ => throw new DeltaLake.DeltaFormatException(
                $"Unknown column mapping mode: {mode}"),
        };
    }

    /// <summary>
    /// Gets the physical name for a field: the <c>delta.columnMapping.physicalName</c> metadata value whenever
    /// column mapping is enabled (BOTH <c>name</c> and <c>id</c> mode — per the Delta protocol, writers must
    /// write data files using the physical column names in both modes; <c>id</c> mode additionally stamps the
    /// parquet <c>field_id</c>, which is what id-mode READERS resolve by). Without column mapping, the logical
    /// name is the physical name.
    /// </summary>
    public static string GetPhysicalName(StructField field, ColumnMappingMode mode)
    {
        if (mode != ColumnMappingMode.None &&
            field.Metadata is not null &&
            field.Metadata.TryGetValue(PhysicalNameKey, out string? physicalName))
        {
            return physicalName;
        }

        return field.Name;
    }

    /// <summary>
    /// Gets the column mapping ID for a field. Returns null if not set.
    /// </summary>
    public static int? GetFieldId(StructField field)
    {
        if (field.Metadata is not null &&
            field.Metadata.TryGetValue(FieldIdKey, out string? idStr) &&
            int.TryParse(idStr, out int id))
        {
            return id;
        }

        return null;
    }

    /// <summary>
    /// Assigns column mapping metadata (IDs and physical names) to a schema
    /// that doesn't have them yet. Used when creating a new column-mapped table.
    /// Returns the updated schema and the maximum assigned column ID.
    /// </summary>
    public static (StructType Schema, int MaxColumnId) AssignColumnMapping(
        StructType schema, int startId = 0)
    {
        int nextId = startId;
        var fields = new List<StructField>();

        foreach (var field in schema.Fields)
        {
            var (mappedField, lastId) = AssignFieldMapping(field, nextId);
            fields.Add(mappedField);
            nextId = lastId;
        }

        return (new StructType { Fields = fields }, nextId);
    }

    private static (StructField Field, int NextId) AssignFieldMapping(
        StructField field, int nextId)
    {
        nextId++;
        int fieldId = nextId;
        string physicalName = GeneratePhysicalName(fieldId);

        // Recursively assign to nested types
        DeltaDataType mappedType = field.Type;
        switch (field.Type)
        {
            case StructType st:
            {
                var (mapped, lastId) = AssignColumnMapping(st, nextId);
                mappedType = mapped;
                nextId = lastId;
                break;
            }
            case ArrayType at:
            {
                nextId++;
                var elementField = new StructField
                {
                    Name = "element",
                    Type = at.ElementType,
                    Nullable = at.ContainsNull,
                };
                var (mappedElement, lastId) = AssignFieldMapping(elementField, nextId);
                mappedType = new ArrayType
                {
                    ElementType = mappedElement.Type,
                    ContainsNull = at.ContainsNull,
                };
                nextId = lastId;
                break;
            }
            case MapType mt:
            {
                nextId++;
                var keyField = new StructField
                {
                    Name = "key",
                    Type = mt.KeyType,
                    Nullable = false,
                };
                var (mappedKey, lastId1) = AssignFieldMapping(keyField, nextId);
                nextId = lastId1;

                var valueField = new StructField
                {
                    Name = "value",
                    Type = mt.ValueType,
                    Nullable = mt.ValueContainsNull,
                };
                var (mappedValue, lastId2) = AssignFieldMapping(valueField, nextId);
                nextId = lastId2;

                mappedType = new MapType
                {
                    KeyType = mappedKey.Type,
                    ValueType = mappedValue.Type,
                    ValueContainsNull = mt.ValueContainsNull,
                };
                break;
            }
        }

        var metadata = CopyMetadata(field.Metadata);
        metadata[FieldIdKey] = fieldId.ToString();
        metadata[PhysicalNameKey] = physicalName;

        return (new StructField
        {
            Name = field.Name,
            Type = mappedType,
            Nullable = field.Nullable,
            Metadata = metadata,
        }, nextId);
    }

    /// <summary>
    /// Generates a UUID-based physical name for a column.
    /// Delta Lake uses <c>col-{uuid}</c> format.
    /// </summary>
    private static string GeneratePhysicalName(int fieldId) =>
        $"col-{Guid.NewGuid():N}";

    /// <summary>
    /// Builds a mapping from physical column names to logical column names for the given schema.
    /// Populated whenever column mapping is enabled (<c>name</c> OR <c>id</c> mode — data files use
    /// physical names in both); empty without mapping.
    /// </summary>
    public static Dictionary<string, string> BuildPhysicalToLogicalMap(
        StructType schema, ColumnMappingMode mode)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        if (mode == ColumnMappingMode.None)
            return map;

        foreach (var field in schema.Fields)
        {
            string physicalName = GetPhysicalName(field, mode);
            map[physicalName] = field.Name;
        }

        return map;
    }

    /// <summary>
    /// Builds a mapping from logical column names to physical column names for the given schema.
    /// Populated whenever column mapping is enabled (<c>name</c> OR <c>id</c> mode — data files use
    /// physical names in both); empty without mapping.
    /// </summary>
    public static Dictionary<string, string> BuildLogicalToPhysicalMap(
        StructType schema, ColumnMappingMode mode)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        if (mode == ColumnMappingMode.None)
            return map;

        foreach (var field in schema.Fields)
        {
            string physicalName = GetPhysicalName(field, mode);
            map[field.Name] = physicalName;
        }

        return map;
    }

    /// <summary>
    /// Builds a mapping from field_id to logical column name.
    /// Used in <c>id</c> mode to resolve columns by Parquet field_id.
    /// </summary>
    public static Dictionary<int, string> BuildFieldIdToLogicalMap(StructType schema)
    {
        var map = new Dictionary<int, string>();
        foreach (var field in schema.Fields)
        {
            int? fieldId = GetFieldId(field);
            if (fieldId.HasValue)
                map[fieldId.Value] = field.Name;
        }
        return map;
    }

    /// <summary>
    /// Builds a mapping from logical column name to field_id.
    /// Used in <c>id</c> mode for column projection.
    /// </summary>
    public static Dictionary<string, int> BuildLogicalToFieldIdMap(StructType schema)
    {
        var map = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var field in schema.Fields)
        {
            int? fieldId = GetFieldId(field);
            if (fieldId.HasValue)
                map[field.Name] = fieldId.Value;
        }
        return map;
    }

    /// <summary>
    /// Adds <c>PARQUET:field_id</c> metadata to Arrow fields so the Parquet writer
    /// sets the field_id in the Parquet schema. Used in both <c>id</c> and <c>name</c>
    /// column mapping modes.
    /// </summary>
    public static Apache.Arrow.RecordBatch SetParquetFieldIds(
        Apache.Arrow.RecordBatch batch, StructType deltaSchema,
        ColumnMappingMode mode)
    {
        if (mode == ColumnMappingMode.None)
            return batch;

        var fields = new List<Apache.Arrow.Field>();
        bool anyChanged = false;

        for (int i = 0; i < batch.Schema.FieldsList.Count; i++)
        {
            var arrowField = batch.Schema.FieldsList[i];

            // Find the matching Delta field by name (post-rename if needed)
            int? fieldId = FindFieldIdForArrowField(arrowField.Name, deltaSchema, mode);

            if (fieldId.HasValue)
            {
                var meta = CopyMetadata(arrowField.Metadata);
                meta["PARQUET:field_id"] = fieldId.Value.ToString();
                fields.Add(new Apache.Arrow.Field(arrowField.Name, arrowField.DataType,
                    arrowField.IsNullable, meta));
                anyChanged = true;
            }
            else
            {
                fields.Add(arrowField);
            }
        }

        if (!anyChanged)
            return batch;

        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            builder.Field(f);

        var columns = new Apache.Arrow.IArrowArray[batch.ColumnCount];
        for (int i = 0; i < batch.ColumnCount; i++)
            columns[i] = batch.Column(i);

        return new Apache.Arrow.RecordBatch(builder.Build(), columns, batch.Length);
    }

    private static int? FindFieldIdForArrowField(
        string arrowFieldName, StructType deltaSchema, ColumnMappingMode mode)
    {
        foreach (var field in deltaSchema.Fields)
        {
            // With column mapping (either mode) the Arrow batch was renamed to PHYSICAL names before writing.
            string matchName = GetPhysicalName(field, mode);

            if (matchName == arrowFieldName)
                return GetFieldId(field);
        }
        return null;
    }

    /// <summary>
    /// Renames a RecordBatch using a field_id → logical name map.
    /// Matches batch columns by their Parquet field_id (from Arrow metadata
    /// or from a pre-resolved name map).
    /// </summary>
    public static Apache.Arrow.RecordBatch RenameByFieldId(
        Apache.Arrow.RecordBatch batch,
        Dictionary<int, string> fieldIdToLogical,
        EngineeredWood.Parquet.Schema.SchemaDescriptor parquetSchema)
    {
        // Build a map from Parquet column name → field_id using the Parquet schema
        var nameToFieldId = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var child in parquetSchema.Root.Children)
        {
            if (child.Element.FieldId.HasValue)
                nameToFieldId[child.Name] = child.Element.FieldId.Value;
        }

        var fields = new List<Apache.Arrow.Field>();
        bool anyRenamed = false;

        for (int i = 0; i < batch.Schema.FieldsList.Count; i++)
        {
            var field = batch.Schema.FieldsList[i];

            if (nameToFieldId.TryGetValue(field.Name, out int fieldId) &&
                fieldIdToLogical.TryGetValue(fieldId, out string? logicalName) &&
                logicalName != field.Name)
            {
                fields.Add(new Apache.Arrow.Field(logicalName, field.DataType, field.IsNullable));
                anyRenamed = true;
            }
            else
            {
                fields.Add(field);
            }
        }

        if (!anyRenamed)
            return batch;

        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            builder.Field(f);

        var columns = new Apache.Arrow.IArrowArray[batch.ColumnCount];
        for (int i = 0; i < batch.ColumnCount; i++)
            columns[i] = batch.Column(i);

        return new Apache.Arrow.RecordBatch(builder.Build(), columns, batch.Length);
    }

    /// <summary>
    /// Gets the maximum column mapping ID from the schema.
    /// </summary>
    public static int GetMaxColumnId(StructType schema)
    {
        int maxId = 0;

        foreach (var field in schema.Fields)
            maxId = Math.Max(maxId, GetMaxColumnIdRecursive(field));

        return maxId;
    }

    private static int GetMaxColumnIdRecursive(StructField field)
    {
        int maxId = GetFieldId(field) ?? 0;

        switch (field.Type)
        {
            case StructType st:
                foreach (var child in st.Fields)
                    maxId = Math.Max(maxId, GetMaxColumnIdRecursive(child));
                break;
        }

        return maxId;
    }

    /// <summary>
    /// Renames Arrow RecordBatch columns from physical names to logical names.
    /// </summary>
    public static Apache.Arrow.RecordBatch RenameColumns(
        Apache.Arrow.RecordBatch batch,
        Dictionary<string, string> physicalToLogical)
    {
        if (physicalToLogical.Count == 0)
            return batch;

        var fields = new List<Apache.Arrow.Field>();
        bool anyRenamed = false;

        for (int i = 0; i < batch.Schema.FieldsList.Count; i++)
        {
            var field = batch.Schema.FieldsList[i];
            if (physicalToLogical.TryGetValue(field.Name, out string? logicalName) &&
                logicalName != field.Name)
            {
                fields.Add(new Apache.Arrow.Field(logicalName, field.DataType, field.IsNullable));
                anyRenamed = true;
            }
            else
            {
                fields.Add(field);
            }
        }

        if (!anyRenamed)
            return batch;

        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            builder.Field(f);

        var columns = new Apache.Arrow.IArrowArray[batch.ColumnCount];
        for (int i = 0; i < batch.ColumnCount; i++)
            columns[i] = batch.Column(i);

        return new Apache.Arrow.RecordBatch(builder.Build(), columns, batch.Length);
    }

    /// <summary>
    /// Renames Arrow RecordBatch columns from logical names to physical names.
    /// </summary>
    public static Apache.Arrow.RecordBatch RenameToPhysical(
        Apache.Arrow.RecordBatch batch,
        Dictionary<string, string> logicalToPhysical)
    {
        if (logicalToPhysical.Count == 0)
            return batch;

        var fields = new List<Apache.Arrow.Field>();
        bool anyRenamed = false;

        for (int i = 0; i < batch.Schema.FieldsList.Count; i++)
        {
            var field = batch.Schema.FieldsList[i];
            if (logicalToPhysical.TryGetValue(field.Name, out string? physicalName) &&
                physicalName != field.Name)
            {
                fields.Add(new Apache.Arrow.Field(physicalName, field.DataType, field.IsNullable));
                anyRenamed = true;
            }
            else
            {
                fields.Add(field);
            }
        }

        if (!anyRenamed)
            return batch;

        var builder = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            builder.Field(f);

        var columns = new Apache.Arrow.IArrowArray[batch.ColumnCount];
        for (int i = 0; i < batch.ColumnCount; i++)
            columns[i] = batch.Column(i);

        return new Apache.Arrow.RecordBatch(builder.Build(), columns, batch.Length);
    }

    /// <summary>
    /// Copies metadata from an IReadOnlyDictionary to a mutable Dictionary.
    /// Needed because netstandard2.0's Dictionary constructor doesn't accept IReadOnlyDictionary.
    /// </summary>
    private static Dictionary<string, string> CopyMetadata(
        IReadOnlyDictionary<string, string>? source)
    {
        if (source is null)
            return new Dictionary<string, string>();

        var result = new Dictionary<string, string>(source.Count);
        foreach (var kvp in source)
            result[kvp.Key] = kvp.Value;
        return result;
    }
}
