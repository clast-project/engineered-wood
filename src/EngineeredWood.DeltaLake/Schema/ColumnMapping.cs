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
    /// The field-metadata key under which IcebergCompatV2 records the column ids of the array elements and map
    /// keys/values below a <see cref="StructField"/>: a JSON object from each one's path to its id. See
    /// <see cref="AssignNestedIds"/>.
    /// </summary>
    public const string NestedIdsKey = "delta.columnMapping.nested.ids";

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
    /// Returns <paramref name="schema"/> with every struct field, at every depth, renamed to its physical name —
    /// the shape a data file's columns have. Fields keep their type, nullability and metadata. Unchanged when
    /// <paramref name="mode"/> is <see cref="ColumnMappingMode.None"/>.
    /// </summary>
    public static StructType ToPhysicalSchema(StructType schema, ColumnMappingMode mode)
    {
        if (mode == ColumnMappingMode.None)
            return schema;
        return PhysicalStruct(schema, mode);
    }

    private static StructType PhysicalStruct(StructType schema, ColumnMappingMode mode) => new()
    {
        Fields = schema.Fields.Select(f => new StructField
        {
            Name = GetPhysicalName(f, mode),
            Type = PhysicalType(f.Type, mode),
            Nullable = f.Nullable,
            Metadata = f.Metadata,
        }).ToList(),
    };

    private static DeltaDataType PhysicalType(DeltaDataType type, ColumnMappingMode mode) => type switch
    {
        StructType st => PhysicalStruct(st, mode),
        ArrayType at => new ArrayType
        {
            ElementType = PhysicalType(at.ElementType, mode),
            ContainsNull = at.ContainsNull,
        },
        MapType mt => new MapType
        {
            KeyType = PhysicalType(mt.KeyType, mode),
            ValueType = PhysicalType(mt.ValueType, mode),
            ValueContainsNull = mt.ValueContainsNull,
        },
        _ => type,
    };

    /// <summary>
    /// The Arrow field-metadata key that carries a Parquet <c>field_id</c>, both into the Parquet writer and
    /// out of the Parquet reader.
    /// </summary>
    public const string ParquetFieldIdKey = "PARQUET:field_id";

    /// <summary>
    /// Gets the Parquet <c>field_id</c> an Arrow field carries under <see cref="ParquetFieldIdKey"/>, or null.
    /// </summary>
    public static int? GetParquetFieldId(Apache.Arrow.Field field)
    {
        if (field.Metadata is not null &&
            field.Metadata.TryGetValue(ParquetFieldIdKey, out string? idStr) &&
            int.TryParse(idStr, System.Globalization.NumberStyles.Integer,
                System.Globalization.CultureInfo.InvariantCulture, out int id))
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

        var (mappedType, lastId) = AssignTypeMapping(field.Type, nextId);

        var metadata = FieldMetadata.With(
            field.Metadata,
            (FieldIdKey, fieldId.ToString(System.Globalization.CultureInfo.InvariantCulture)),
            (PhysicalNameKey, physicalName));

        return (new StructField
        {
            Name = field.Name,
            Type = mappedType,
            Nullable = field.Nullable,
            Metadata = metadata,
        }, lastId);
    }

    // Only struct fields take an id here. An array element or map key/value is no StructField, and its id, when
    // the table needs one, is IcebergCompatV2's nested id (AssignNestedIds), recorded on the ancestor field.
    private static (DeltaDataType Type, int NextId) AssignTypeMapping(DeltaDataType type, int nextId)
    {
        switch (type)
        {
            case StructType st:
                return AssignColumnMapping(st, nextId);
            case ArrayType at:
            {
                var (element, lastId) = AssignTypeMapping(at.ElementType, nextId);
                return (new ArrayType { ElementType = element, ContainsNull = at.ContainsNull }, lastId);
            }
            case MapType mt:
            {
                var (key, afterKey) = AssignTypeMapping(mt.KeyType, nextId);
                var (value, lastId) = AssignTypeMapping(mt.ValueType, afterKey);
                return (new MapType
                {
                    KeyType = key,
                    ValueType = value,
                    ValueContainsNull = mt.ValueContainsNull,
                }, lastId);
            }
            default:
                return (type, nextId);
        }
    }

    /// <summary>
    /// The nested ids <paramref name="field"/> records under <see cref="NestedIdsKey"/>, by path, in the order
    /// they appear; empty when it records none.
    /// </summary>
    /// <exception cref="DeltaLake.DeltaFormatException">The value is not a JSON object of integer ids.</exception>
    public static IReadOnlyList<KeyValuePair<string, int>> GetNestedIds(StructField field)
    {
        if (field.Metadata is null || !field.Metadata.TryGetValue(NestedIdsKey, out string? json))
            return [];

        var ids = new List<KeyValuePair<string, int>>();
        try
        {
            using var doc = System.Text.Json.JsonDocument.Parse(json);
            if (doc.RootElement.ValueKind == System.Text.Json.JsonValueKind.Object)
            {
                bool wellFormed = true;
                foreach (var entry in doc.RootElement.EnumerateObject())
                {
                    if (entry.Value.ValueKind != System.Text.Json.JsonValueKind.Number
                        || !entry.Value.TryGetInt32(out int id))
                    {
                        wellFormed = false;
                        break;
                    }
                    ids.Add(new KeyValuePair<string, int>(entry.Name, id));
                }
                if (wellFormed)
                    return ids;
            }
        }
        catch (System.Text.Json.JsonException)
        {
        }

        throw new DeltaLake.DeltaFormatException(
            $"Field '{field.Name}' has a '{NestedIdsKey}' that is not a JSON object of integer column ids.");
    }

    /// <summary>
    /// Gives every array element and map key/value in <paramref name="schema"/> a column id, as IcebergCompatV2
    /// requires (PROTOCOL.md, "Writer Requirements for IcebergCompatV2"), continuing past
    /// <paramref name="startId"/> or the highest id <paramref name="schema"/> already records, whichever is
    /// higher. Returns the schema and the new maximum column id; the same schema instance when every one already
    /// has an id.
    /// </summary>
    /// <remarks>
    /// <para>The ids are recorded under <see cref="NestedIdsKey"/> on the nearest ancestor
    /// <see cref="StructField"/>, keyed by path: that field's PHYSICAL name, then <c>element</c>, <c>key</c> or
    /// <c>value</c> for each level, joined with dots. <c>m: map&lt;string, array&lt;int&gt;&gt;</c> records
    /// <c>&lt;physical&gt;.key</c>, <c>&lt;physical&gt;.value</c> and <c>&lt;physical&gt;.value.element</c>. A
    /// struct below an array or map starts over: its fields record their own.</para>
    /// <para>This is delta-spark's and delta-kernel's algorithm (<c>rewriteFieldIdsForIceberg</c>), read from
    /// their jars: an id is assigned before descending, a path that already has an id keeps it, and the map is
    /// written only on a field that has something to record (Kernel's behaviour; Spark writes an empty one on
    /// every field).</para>
    /// </remarks>
    public static (StructType Schema, int MaxColumnId) AssignNestedIds(StructType schema, int startId)
    {
        // A caller's high-water mark can lag the schema (one carrying ids of its own); an id at or below an
        // existing one could duplicate it.
        int nextId = Math.Max(startId, GetMaxColumnId(schema));
        var result = AssignNestedIdsIn(schema, ref nextId);
        return (result, nextId);
    }

    private static StructType AssignNestedIdsIn(StructType schema, ref int nextId)
    {
        List<StructField>? fields = null;
        for (int i = 0; i < schema.Fields.Count; i++)
        {
            var field = schema.Fields[i];
            var ids = GetNestedIds(field).ToList();
            int recorded = ids.Count;
            var type = AssignNestedIdsIn(
                field.Type, GetPhysicalName(field, ColumnMappingMode.Name), ids, ref nextId);
            if (ReferenceEquals(type, field.Type) && ids.Count == recorded)
                continue;

            fields ??= [.. schema.Fields];
            fields[i] = new StructField
            {
                Name = field.Name,
                Type = type,
                Nullable = field.Nullable,
                Metadata = ids.Count == recorded
                    ? field.Metadata
                    : FieldMetadata.With(field.Metadata, (NestedIdsKey, NestedIdsJson(ids))),
            };
        }

        return fields is null ? schema : new StructType { Fields = fields };
    }

    private static DeltaDataType AssignNestedIdsIn(
        DeltaDataType type, string path, List<KeyValuePair<string, int>> ids, ref int nextId)
    {
        switch (type)
        {
            case StructType st:
                return AssignNestedIdsIn(st, ref nextId);
            case ArrayType at:
            {
                string elementPath = Claim(path + ".element", ids, ref nextId);
                var element = AssignNestedIdsIn(at.ElementType, elementPath, ids, ref nextId);
                return ReferenceEquals(element, at.ElementType)
                    ? type
                    : new ArrayType { ElementType = element, ContainsNull = at.ContainsNull };
            }
            case MapType mt:
            {
                string keyPath = Claim(path + ".key", ids, ref nextId);
                var key = AssignNestedIdsIn(mt.KeyType, keyPath, ids, ref nextId);
                string valuePath = Claim(path + ".value", ids, ref nextId);
                var value = AssignNestedIdsIn(mt.ValueType, valuePath, ids, ref nextId);
                return ReferenceEquals(key, mt.KeyType) && ReferenceEquals(value, mt.ValueType)
                    ? type
                    : new MapType { KeyType = key, ValueType = value, ValueContainsNull = mt.ValueContainsNull };
            }
            default:
                return type;
        }
    }

    private static string Claim(string path, List<KeyValuePair<string, int>> ids, ref int nextId)
    {
        if (!ids.Any(entry => string.Equals(entry.Key, path, StringComparison.Ordinal)))
            ids.Add(new KeyValuePair<string, int>(path, ++nextId));
        return path;
    }

    private static string NestedIdsJson(List<KeyValuePair<string, int>> ids)
    {
        using var buffer = new MemoryStream();
        using (var writer = new System.Text.Json.Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            foreach (var entry in ids)
                writer.WriteNumber(entry.Key, entry.Value);
            writer.WriteEndObject();
        }
        return System.Text.Encoding.UTF8.GetString(buffer.ToArray());
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
    /// Renames a RecordBatch's top-level columns using a field_id → logical name map.
    /// A column's field_id is the <c>PARQUET:field_id</c> in its Arrow metadata (which the Parquet reader
    /// supplies), or else the id <paramref name="parquetSchema"/> gives the column of that name — for a batch
    /// from a source that does not carry ids into Arrow. A column with neither is left as it is.
    /// </summary>
    public static Apache.Arrow.RecordBatch RenameByFieldId(
        Apache.Arrow.RecordBatch batch,
        Dictionary<int, string> fieldIdToLogical,
        EngineeredWood.Parquet.Schema.SchemaDescriptor? parquetSchema)
    {
        Dictionary<string, int>? nameToFieldId = null;
        if (parquetSchema is not null)
        {
            nameToFieldId = new Dictionary<string, int>(StringComparer.Ordinal);
            foreach (var child in parquetSchema.Root.Children)
            {
                if (child.Element.FieldId.HasValue)
                    nameToFieldId[child.Name] = child.Element.FieldId.Value;
            }
        }

        var fields = new List<Apache.Arrow.Field>();
        bool anyRenamed = false;

        for (int i = 0; i < batch.Schema.FieldsList.Count; i++)
        {
            var field = batch.Schema.FieldsList[i];

            int? fieldId = GetParquetFieldId(field);
            if (fieldId is null && nameToFieldId is not null && nameToFieldId.TryGetValue(field.Name, out int byName))
                fieldId = byName;

            if (fieldId is { } id &&
                fieldIdToLogical.TryGetValue(id, out string? logicalName) &&
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

    // A field's nested ids count too: they come from the same sequence as column ids (AssignNestedIds).
    private static int GetMaxColumnIdRecursive(StructField field)
    {
        int maxId = Math.Max(GetFieldId(field) ?? 0, GetMaxColumnIdRecursive(field.Type));
        foreach (var entry in GetNestedIds(field))
            maxId = Math.Max(maxId, entry.Value);
        return maxId;
    }

    // Through array elements and map keys/values too: a struct nested in either carries ids of its own,
    // and a maxColumnId below them lets the next ADD COLUMN reuse one.
    private static int GetMaxColumnIdRecursive(DeltaDataType type)
    {
        switch (type)
        {
            case StructType st:
                int maxId = 0;
                foreach (var child in st.Fields)
                    maxId = Math.Max(maxId, GetMaxColumnIdRecursive(child));
                return maxId;
            case ArrayType at:
                return GetMaxColumnIdRecursive(at.ElementType);
            case MapType mt:
                return Math.Max(GetMaxColumnIdRecursive(mt.KeyType), GetMaxColumnIdRecursive(mt.ValueType));
            default:
                return 0;
        }
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
