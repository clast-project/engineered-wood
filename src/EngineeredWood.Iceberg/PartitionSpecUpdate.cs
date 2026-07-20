// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.Iceberg;

/// <summary>
/// Evolves a table's partition spec. Follows Iceberg's partition evolution rules:
/// - New fields get new, never-reused field IDs
/// - "Removing" a field replaces its transform with void (the field stays in the spec)
/// - Old data files keep their old partition layout; only new writes use the new spec
/// - All historical specs are preserved in metadata
/// </summary>
public sealed class PartitionSpecUpdate
{
    private readonly TableMetadata _metadata;
    private int _nextPartitionFieldId;
    private readonly List<(string SourceColumn, Transform Transform, string? Name)> _addedFields = [];
    private readonly HashSet<string> _removedFieldNames = [];

    public PartitionSpecUpdate(TableMetadata metadata)
    {
        _metadata = metadata;
        _nextPartitionFieldId = metadata.LastPartitionId;
    }

    /// <summary>
    /// Add a partition field by source column name.
    /// The partition field name defaults to "{column}_{transform}" if not specified.
    /// </summary>
    public PartitionSpecUpdate AddField(string sourceColumn, Transform transform, string? name = null)
    {
        _addedFields.Add((sourceColumn, transform, name));
        return this;
    }

    /// <summary>
    /// Remove a partition field by name. The field's transform becomes void —
    /// the field is retained in the spec so existing data files remain valid.
    /// </summary>
    public PartitionSpecUpdate RemoveField(string partitionFieldName)
    {
        _removedFieldNames.Add(partitionFieldName);
        return this;
    }

    /// <summary>
    /// Applies all pending changes and returns updated table metadata with a new partition spec.
    /// Returns the original metadata unchanged if no fields were modified.
    /// </summary>
    public TableMetadata Apply()
    {
        var currentSpec = _metadata.PartitionSpecs.First(
            s => s.SpecId == _metadata.DefaultSpecId);

        var schema = _metadata.Schemas.First(
            s => s.SchemaId == _metadata.CurrentSchemaId);

        var fields = new List<PartitionField>();

        // Carry forward existing fields, voiding any that were removed
        foreach (var field in currentSpec.Fields)
        {
            if (_removedFieldNames.Contains(field.Name))
            {
                if (field.Transform is VoidTransform)
                    throw new ArgumentException($"Partition field already removed: {field.Name}");
                fields.Add(field with { Transform = Transform.Void });
            }
            else
            {
                fields.Add(field);
            }
        }

        // Add new partition fields
        foreach (var (sourceColumn, transform, explicitName) in _addedFields)
        {
            var schemaField = schema.Fields.FirstOrDefault(f => f.Name == sourceColumn)
                ?? throw new ArgumentException($"Source column not found: {sourceColumn}");

            _nextPartitionFieldId++;
            var name = explicitName ?? $"{sourceColumn}_{TransformLabel(transform)}";
            fields.Add(new PartitionField(schemaField.Id, _nextPartitionFieldId, name, transform));
        }

        // Check if the spec actually changed
        if (SpecEquals(currentSpec.Fields, fields))
            return _metadata;

        // If the result is all void transforms, it's effectively unpartitioned.
        // Still create a new spec to preserve history.

        var newSpecId = _metadata.PartitionSpecs.Max(s => s.SpecId) + 1;
        var newSpec = new PartitionSpec(newSpecId, fields);

        var specs = _metadata.PartitionSpecs.ToList();
        specs.Add(newSpec);

        return _metadata with
        {
            LastUpdatedMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            DefaultSpecId = newSpecId,
            PartitionSpecs = specs,
            LastPartitionId = _nextPartitionFieldId,
        };
    }

    private static string TransformLabel(Transform t) => t switch
    {
        IdentityTransform => "identity",
        BucketTransform b => $"bucket_{b.NumBuckets}",
        TruncateTransform tr => $"truncate_{tr.Width}",
        YearTransform => "year",
        MonthTransform => "month",
        DayTransform => "day",
        HourTransform => "hour",
        VoidTransform => "void",
        _ => "unknown",
    };

    private static bool SpecEquals(IReadOnlyList<PartitionField> a, IReadOnlyList<PartitionField> b)
    {
        if (a.Count != b.Count) return false;
        for (int i = 0; i < a.Count; i++)
        {
            if (a[i] != b[i]) return false;
        }
        return true;
    }
}
