// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg;

/// <summary>
/// Builds a schema evolution transaction for an Iceberg table, supporting column additions, drops,
/// renames, type promotions, and identifier field changes. Call <see cref="Apply"/> to produce updated metadata.
/// </summary>
public sealed class SchemaUpdate
{
    private readonly TableMetadata _metadata;
    private int _nextColumnId;
    private readonly List<NestedField> _addedColumns = [];
    private readonly HashSet<string> _droppedColumns = [];
    private readonly Dictionary<string, string> _renamedColumns = []; // old -> new
    private readonly Dictionary<string, IcebergType> _typeUpdates = [];
    private readonly HashSet<string> _madeOptional = [];
    private string[]? _identifierFieldNames;

    public SchemaUpdate(TableMetadata metadata)
    {
        _metadata = metadata;
        _nextColumnId = metadata.LastColumnId;
    }

    /// <summary>Adds a new column to the schema.</summary>
    public SchemaUpdate AddColumn(string name, IcebergType type, bool required = false, string? doc = null)
    {
        _nextColumnId++;
        _addedColumns.Add(new NestedField(_nextColumnId, name, type, required, doc));
        return this;
    }

    /// <summary>Removes a column from the schema by name.</summary>
    public SchemaUpdate DropColumn(string name)
    {
        _droppedColumns.Add(name);
        return this;
    }

    /// <summary>Renames an existing column.</summary>
    public SchemaUpdate RenameColumn(string oldName, string newName)
    {
        _renamedColumns[oldName] = newName;
        return this;
    }

    /// <summary>Promotes a column to a wider type (e.g. int to long, float to double).</summary>
    public SchemaUpdate UpdateColumnType(string name, IcebergType newType)
    {
        _typeUpdates[name] = newType;
        return this;
    }

    /// <summary>Makes a required column optional.</summary>
    public SchemaUpdate MakeOptional(string name)
    {
        _madeOptional.Add(name);
        return this;
    }

    /// <summary>Sets the identifier (primary key) fields for the new schema by column name.</summary>
    public SchemaUpdate SetIdentifierFields(params string[] fieldNames)
    {
        _identifierFieldNames = fieldNames;
        return this;
    }

    /// <summary>Applies all pending changes and returns updated table metadata with a new schema version.</summary>
    public TableMetadata Apply()
    {
        var currentSchema = _metadata.Schemas.First(
            s => s.SchemaId == _metadata.CurrentSchemaId);

        var fields = new List<NestedField>();

        foreach (var field in currentSchema.Fields)
        {
            if (_droppedColumns.Contains(field.Name))
                continue;

            var updated = field;

            if (_renamedColumns.TryGetValue(field.Name, out var newName))
                updated = updated with { Name = newName };

            if (_typeUpdates.TryGetValue(field.Name, out var newType))
            {
                ValidateTypePromotion(field.Type, newType);
                updated = updated with { Type = newType };
            }

            if (_madeOptional.Contains(field.Name) || _madeOptional.Contains(updated.Name))
                updated = updated with { IsRequired = false };

            fields.Add(updated);
        }

        fields.AddRange(_addedColumns);

        // Resolve identifier fields by name
        IReadOnlyList<int>? identifierFieldIds = null;
        if (_identifierFieldNames is not null)
        {
            identifierFieldIds = _identifierFieldNames
                .Select(name => fields.FirstOrDefault(f => f.Name == name)
                    ?? throw new ArgumentException($"Identifier field not found: {name}"))
                .Select(f => f.Id)
                .ToList();
        }

        var newSchemaId = _metadata.Schemas.Max(s => s.SchemaId) + 1;
        var newSchema = new Schema(newSchemaId, fields, identifierFieldIds);

        var schemas = _metadata.Schemas.ToList();
        schemas.Add(newSchema);

        return _metadata with
        {
            LastUpdatedMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            LastColumnId = _nextColumnId,
            CurrentSchemaId = newSchemaId,
            Schemas = schemas,
        };
    }

    internal static void ValidateTypePromotion(IcebergType from, IcebergType to)
    {
        var valid = (from, to) switch
        {
            // v2 promotions
            (IntegerType, LongType) => true,
            (FloatType, DoubleType) => true,
            (DecimalType d1, DecimalType d2) =>
                d2.Precision > d1.Precision && d2.Scale == d1.Scale,
            // v3 promotions
            (UnknownType, _) => true, // unknown promotes to any type
            (DateType, TimestampType) => true,
            (DateType, TimestampNsType) => true,
            _ => false,
        };

        if (!valid)
            throw new ArgumentException(
                $"Cannot promote type {from.GetType().Name} to {to.GetType().Name}");
    }
}
