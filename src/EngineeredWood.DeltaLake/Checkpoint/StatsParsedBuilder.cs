// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using EngineeredWood.DeltaLake.Actions;
using EngineeredWood.DeltaLake.Schema;
using DeltaStructType = EngineeredWood.DeltaLake.Schema.StructType;
using ArrowStructType = Apache.Arrow.Types.StructType;

namespace EngineeredWood.DeltaLake.Checkpoint;

/// <summary>
/// Builds the <c>stats_parsed</c> struct column for checkpoint files.
/// The struct contains <c>numRecords</c>, <c>minValues</c>, <c>maxValues</c>,
/// and <c>nullCount</c> sub-structs matching the table schema's leaf columns.
/// </summary>
internal static class StatsParsedBuilder
{
    /// <summary>
    /// Builds the Arrow type for the <c>stats_parsed</c> struct based on the table schema.
    /// </summary>
    public static ArrowStructType BuildStatsType(DeltaStructType deltaSchema)
    {
        var statFields = new List<Field>();

        // numRecords: long
        statFields.Add(new Field("numRecords", Int64Type.Default, true));

        // minValues: struct with one field per leaf column (using the column's type)
        var minFields = BuildValueFields(deltaSchema);
        if (minFields.Count > 0)
            statFields.Add(new Field("minValues", new ArrowStructType(minFields), true));

        // maxValues: same structure
        var maxFields = BuildValueFields(deltaSchema);
        if (maxFields.Count > 0)
            statFields.Add(new Field("maxValues", new ArrowStructType(maxFields), true));

        // nullCount: struct with one long field per leaf column
        var nullCountFields = deltaSchema.Fields
            .Select(f => new Field(f.Name, Int64Type.Default, true))
            .ToList();
        if (nullCountFields.Count > 0)
            statFields.Add(new Field("nullCount", new ArrowStructType(nullCountFields), true));

        return new ArrowStructType(statFields);
    }

    /// <summary>
    /// Builds the <c>stats_parsed</c> struct array from a list of actions.
    /// Only non-null for rows that are <see cref="AddFile"/> actions with stats.
    /// </summary>
    public static StructArray BuildStatsColumn(
        List<DeltaAction> actions, int count, DeltaStructType deltaSchema)
    {
        var statsType = BuildStatsType(deltaSchema);
        var columnNames = deltaSchema.Fields.Select(f => f.Name).ToList();

        // Build numRecords array
        var numRecordsBuilder = new Int64Array.Builder();

        // Build per-column min/max/nullCount arrays
        var minBuilders = CreateColumnBuilders(deltaSchema);
        var maxBuilders = CreateColumnBuilders(deltaSchema);
        var nullCountBuilders = new List<Int64Array.Builder>();
        for (int c = 0; c < columnNames.Count; c++)
            nullCountBuilders.Add(new Int64Array.Builder());

        for (int i = 0; i < count; i++)
        {
            if (actions[i] is AddFile add && add.Stats is not null)
            {
                ParseStats(add.Stats, columnNames,
                    numRecordsBuilder, minBuilders, maxBuilders, nullCountBuilders);
            }
            else
            {
                numRecordsBuilder.AppendNull();
                foreach (var b in minBuilders) AppendNull(b);
                foreach (var b in maxBuilders) AppendNull(b);
                foreach (var b in nullCountBuilders) b.AppendNull();
            }
        }

        // Assemble the struct
        var children = new List<IArrowArray>();

        // numRecords
        children.Add(numRecordsBuilder.Build());

        // minValues struct
        if (minBuilders.Count > 0)
        {
            var minArrays = minBuilders.Select(b => Build(b)).ToArray();
            var minFields = BuildValueFields(deltaSchema);
            children.Add(new StructArray(
                new ArrowStructType(minFields), count,
                minArrays, ArrowBuffer.Empty, 0));
        }

        // maxValues struct
        if (maxBuilders.Count > 0)
        {
            var maxArrays = maxBuilders.Select(b => Build(b)).ToArray();
            var maxFields = BuildValueFields(deltaSchema);
            children.Add(new StructArray(
                new ArrowStructType(maxFields), count,
                maxArrays, ArrowBuffer.Empty, 0));
        }

        // nullCount struct
        if (nullCountBuilders.Count > 0)
        {
            var nullCountArrays = nullCountBuilders
                .Select(b => (IArrowArray)b.Build()).ToArray();
            var nullCountFields = columnNames
                .Select(n => new Field(n, Int64Type.Default, true)).ToList();
            children.Add(new StructArray(
                new ArrowStructType(nullCountFields), count,
                nullCountArrays, ArrowBuffer.Empty, 0));
        }

        return new StructArray(statsType, count, children, ArrowBuffer.Empty, 0);
    }

    private static void ParseStats(
        string statsJson,
        List<string> columnNames,
        Int64Array.Builder numRecords,
        List<IArrowArrayBuilder> minBuilders,
        List<IArrowArrayBuilder> maxBuilders,
        List<Int64Array.Builder> nullCountBuilders)
    {
        try
        {
            using var doc = JsonDocument.Parse(statsJson);
            var root = doc.RootElement;

            // numRecords
            if (root.TryGetProperty("numRecords", out var nr))
                numRecords.Append(nr.GetInt64());
            else
                numRecords.AppendNull();

            // minValues
            JsonElement? minObj = root.TryGetProperty("minValues", out var mv)
                ? mv : null;
            for (int c = 0; c < columnNames.Count; c++)
            {
                if (minObj.HasValue &&
                    minObj.Value.TryGetProperty(columnNames[c], out var minVal))
                    AppendValue(minBuilders[c], minVal);
                else
                    AppendNull(minBuilders[c]);
            }

            // maxValues
            JsonElement? maxObj = root.TryGetProperty("maxValues", out var xv)
                ? xv : null;
            for (int c = 0; c < columnNames.Count; c++)
            {
                if (maxObj.HasValue &&
                    maxObj.Value.TryGetProperty(columnNames[c], out var maxVal))
                    AppendValue(maxBuilders[c], maxVal);
                else
                    AppendNull(maxBuilders[c]);
            }

            // nullCount
            JsonElement? ncObj = root.TryGetProperty("nullCount", out var nc)
                ? nc : null;
            for (int c = 0; c < columnNames.Count; c++)
            {
                if (ncObj.HasValue &&
                    ncObj.Value.TryGetProperty(columnNames[c], out var ncVal))
                    nullCountBuilders[c].Append(ncVal.GetInt64());
                else
                    nullCountBuilders[c].AppendNull();
            }
        }
        catch
        {
            // If stats can't be parsed, fill with nulls
            numRecords.AppendNull();
            foreach (var b in minBuilders) AppendNull(b);
            foreach (var b in maxBuilders) AppendNull(b);
            foreach (var b in nullCountBuilders) b.AppendNull();
        }
    }

    private static List<Field> BuildValueFields(DeltaStructType deltaSchema)
    {
        return deltaSchema.Fields
            .Select(f => new Field(f.Name, DeltaTypeToArrowStatsType(f.Type), true))
            .ToList();
    }

    /// <summary>
    /// Maps Delta types to the Arrow type used in stats structs.
    /// Most types use their native representation; complex types are skipped.
    /// </summary>
    private static IArrowType DeltaTypeToArrowStatsType(DeltaDataType type) => type switch
    {
        PrimitiveType p => p.TypeName switch
        {
            "long" => Int64Type.Default,
            "integer" => Int32Type.Default,
            "short" => Int16Type.Default,
            "byte" => Int8Type.Default,
            "float" => FloatType.Default,
            "double" => DoubleType.Default,
            "string" => StringType.Default,
            "boolean" => BooleanType.Default,
            "date" => Date32Type.Default,
            "timestamp" or "timestamp_ntz" => Int64Type.Default, // micros as long
            _ when p.TypeName.StartsWith("decimal") => DoubleType.Default, // approx
            _ => StringType.Default,
        },
        _ => StringType.Default, // Complex types use string representation
    };

    private static List<IArrowArrayBuilder> CreateColumnBuilders(DeltaStructType deltaSchema)
    {
        var builders = new List<IArrowArrayBuilder>();
        foreach (var field in deltaSchema.Fields)
        {
            var arrowType = DeltaTypeToArrowStatsType(field.Type);
            builders.Add(CreateBuilder(arrowType));
        }
        return builders;
    }

    private static IArrowArrayBuilder CreateBuilder(IArrowType type) => type switch
    {
        Int64Type => new Int64Array.Builder(),
        Int32Type => new Int32Array.Builder(),
        Int16Type => new Int16Array.Builder(),
        Int8Type => new Int8Array.Builder(),
        FloatType => new FloatArray.Builder(),
        DoubleType => new DoubleArray.Builder(),
        StringType => new StringArray.Builder(),
        BooleanType => new BooleanArray.Builder(),
        Date32Type => new Date32Array.Builder(),
        _ => new StringArray.Builder(),
    };

    private static void AppendValue(IArrowArrayBuilder builder, JsonElement value)
    {
        switch (builder)
        {
            case Int64Array.Builder b:
                if (value.ValueKind == JsonValueKind.Number) b.Append(value.GetInt64());
                else b.AppendNull();
                break;
            case Int32Array.Builder b:
                if (value.ValueKind == JsonValueKind.Number) b.Append(value.GetInt32());
                else b.AppendNull();
                break;
            case Int16Array.Builder b:
                if (value.ValueKind == JsonValueKind.Number) b.Append(value.GetInt16());
                else b.AppendNull();
                break;
            case Int8Array.Builder b:
                if (value.ValueKind == JsonValueKind.Number) b.Append(value.GetSByte());
                else b.AppendNull();
                break;
            case FloatArray.Builder b:
                if (value.ValueKind == JsonValueKind.Number) b.Append(value.GetSingle());
                else b.AppendNull();
                break;
            case DoubleArray.Builder b:
                if (value.ValueKind == JsonValueKind.Number) b.Append(value.GetDouble());
                else b.AppendNull();
                break;
            case StringArray.Builder b:
                if (value.ValueKind == JsonValueKind.String) b.Append(value.GetString()!);
                else if (value.ValueKind == JsonValueKind.Null) b.AppendNull();
                else b.Append(value.GetRawText());
                break;
            case BooleanArray.Builder b:
                if (value.ValueKind is JsonValueKind.True or JsonValueKind.False)
                    b.Append(value.GetBoolean());
                else b.AppendNull();
                break;
            case Date32Array.Builder b:
                if (value.ValueKind == JsonValueKind.String &&
                    DateTimeOffset.TryParse(value.GetString(), out var dto))
                    b.Append(dto.DateTime);
                else b.AppendNull();
                break;
            default:
                AppendNull(builder);
                break;
        }
    }

    private static void AppendNull(IArrowArrayBuilder builder)
    {
        switch (builder)
        {
            case Int64Array.Builder b: b.AppendNull(); break;
            case Int32Array.Builder b: b.AppendNull(); break;
            case Int16Array.Builder b: b.AppendNull(); break;
            case Int8Array.Builder b: b.AppendNull(); break;
            case FloatArray.Builder b: b.AppendNull(); break;
            case DoubleArray.Builder b: b.AppendNull(); break;
            case StringArray.Builder b: b.AppendNull(); break;
            case BooleanArray.Builder b: b.AppendNull(); break;
            case Date32Array.Builder b: b.AppendNull(); break;
        }
    }

    private static IArrowArray Build(IArrowArrayBuilder builder) => builder switch
    {
        Int64Array.Builder b => b.Build(),
        Int32Array.Builder b => b.Build(),
        Int16Array.Builder b => b.Build(),
        Int8Array.Builder b => b.Build(),
        FloatArray.Builder b => b.Build(),
        DoubleArray.Builder b => b.Build(),
        StringArray.Builder b => b.Build(),
        BooleanArray.Builder b => b.Build(),
        Date32Array.Builder b => b.Build(),
        _ => new StringArray.Builder().Build(),
    };
}
