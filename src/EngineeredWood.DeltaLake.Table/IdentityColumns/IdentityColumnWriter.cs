// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Schema;
using DeltaStructType = EngineeredWood.DeltaLake.Schema.StructType;

namespace EngineeredWood.DeltaLake.Table.IdentityColumns;

/// <summary>
/// Handles identity column value generation during writes.
/// For columns with <c>allowExplicitInsert = false</c>, generates values automatically.
/// For columns with <c>allowExplicitInsert = true</c>, validates user-provided values.
/// </summary>
internal static class IdentityColumnWriter
{
    /// <summary>
    /// Processes a RecordBatch for identity columns. For auto-generated columns,
    /// adds or replaces the column with generated values. For explicit-insert columns,
    /// tracks the high water mark from user values.
    /// Returns the processed batch and a list of (fieldName, newHighWaterMark) updates.
    /// </summary>
    public static (RecordBatch Batch, List<(string Name, long HighWaterMark)> Updates)
        ProcessBatch(
            RecordBatch batch,
            DeltaStructType deltaSchema,
            ref Dictionary<string, IdentityColumnConfig> activeConfigs)
    {
        var updates = new List<(string Name, long HighWaterMark)>();
        var identityCols = new List<(int SchemaIdx, StructField Field, IdentityColumnConfig Config)>();

        for (int i = 0; i < deltaSchema.Fields.Count; i++)
        {
            var field = deltaSchema.Fields[i];
            var config = IdentityColumn.GetConfig(field);
            if (config is not null)
            {
                // Use the live config from activeConfigs if available (has updated HWM)
                if (activeConfigs.TryGetValue(field.Name, out var liveConfig))
                    config = liveConfig;
                identityCols.Add((i, field, config));
            }
        }

        if (identityCols.Count == 0)
            return (batch, updates);

        var columns = new List<IArrowArray>();
        var fields = new List<Field>();
        int batchColIdx = 0;

        for (int schemaIdx = 0; schemaIdx < deltaSchema.Fields.Count; schemaIdx++)
        {
            var deltaField = deltaSchema.Fields[schemaIdx];
            var identityInfo = identityCols.Find(x => x.SchemaIdx == schemaIdx);

            if (identityInfo != default && identityInfo.Config is not null)
            {
                var config = identityInfo.Config;

                if (config.AllowExplicitInsert)
                {
                    // User provides values — find the column in the batch
                    int colIdx = batch.Schema.GetFieldIndex(deltaField.Name);
                    if (colIdx >= 0)
                    {
                        var col = batch.Column(colIdx);
                        columns.Add(col);
                        fields.Add(batch.Schema.FieldsList[colIdx]);
                        batchColIdx = colIdx + 1;

                        // Track high water mark from user values
                        if (col is Int64Array int64)
                        {
                            long? hwm = IdentityColumn.FindExplicitHighWaterMark(
                                int64, config.Step);
                            if (hwm.HasValue)
                            {
                                long newHwm = config.HighWaterMark.HasValue
                                    ? (config.Step > 0
                                        ? Math.Max(hwm.Value, config.HighWaterMark.Value)
                                        : Math.Min(hwm.Value, config.HighWaterMark.Value))
                                    : hwm.Value;
                                updates.Add((deltaField.Name, newHwm));
                                activeConfigs[deltaField.Name] = config with
                                    { HighWaterMark = newHwm };
                            }
                        }
                    }
                    else
                    {
                        // Column not provided — generate values even in explicit mode
                        var (generated, newHwm) = GenerateColumn(config, batch.Length);
                        columns.Add(generated);
                        fields.Add(new Field(deltaField.Name, Int64Type.Default,
                            deltaField.Nullable));
                        updates.Add((deltaField.Name, newHwm));
                        activeConfigs[deltaField.Name] = config with
                            { HighWaterMark = newHwm };
                    }
                }
                else
                {
                    // Auto-generate values — don't use any user-provided column
                    var (generated, newHwm) = GenerateColumn(config, batch.Length);
                    columns.Add(generated);
                    fields.Add(new Field(deltaField.Name, Int64Type.Default,
                        deltaField.Nullable));
                    updates.Add((deltaField.Name, newHwm));
                    activeConfigs[deltaField.Name] = config with
                        { HighWaterMark = newHwm };

                    // Skip the user column if it was provided
                    int colIdx = batch.Schema.GetFieldIndex(deltaField.Name);
                    if (colIdx >= 0)
                        batchColIdx = colIdx + 1;
                }
            }
            else
            {
                // Regular column — take from batch
                int colIdx = batch.Schema.GetFieldIndex(deltaField.Name);
                if (colIdx >= 0)
                {
                    columns.Add(batch.Column(colIdx));
                    fields.Add(batch.Schema.FieldsList[colIdx]);
                }
            }
        }

        var schemaBuilder = new Apache.Arrow.Schema.Builder();
        foreach (var f in fields)
            schemaBuilder.Field(f);

        var resultBatch = new RecordBatch(schemaBuilder.Build(), columns, batch.Length);
        return (resultBatch, updates);
    }

    private static (Int64Array Array, long NewHighWaterMark) GenerateColumn(
        IdentityColumnConfig config, int count)
    {
        var (values, newHwm) = IdentityColumn.GenerateValues(config, count);

        var builder = new Int64Array.Builder();
        foreach (long v in values)
            builder.Append(v);

        return (builder.Build(), newHwm);
    }
}
