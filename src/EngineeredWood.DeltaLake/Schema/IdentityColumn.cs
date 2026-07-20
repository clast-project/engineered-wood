// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

namespace EngineeredWood.DeltaLake.Schema;

/// <summary>
/// Configuration for a Delta Lake identity column.
/// Identity columns auto-generate unique, monotonically increasing values.
/// </summary>
public sealed record IdentityColumnConfig
{
    /// <summary>Starting value for the identity column.</summary>
    public required long Start { get; init; }

    /// <summary>Increment to the next identity value. Cannot be 0.</summary>
    public required long Step { get; init; }

    /// <summary>
    /// The highest (or lowest for negative step) value generated.
    /// Null if no values have been generated yet.
    /// </summary>
    public long? HighWaterMark { get; init; }

    /// <summary>
    /// When true, users may provide their own values.
    /// When false, values are always auto-generated.
    /// </summary>
    public required bool AllowExplicitInsert { get; init; }
}

/// <summary>
/// Utilities for Delta Lake identity columns.
/// </summary>
public static class IdentityColumn
{
    public const string StartKey = "delta.identity.start";
    public const string StepKey = "delta.identity.step";
    public const string HighWaterMarkKey = "delta.identity.highWaterMark";
    public const string AllowExplicitInsertKey = "delta.identity.allowExplicitInsert";

    /// <summary>
    /// Gets the identity column configuration from a field's metadata.
    /// Returns null if the field is not an identity column.
    /// </summary>
    public static IdentityColumnConfig? GetConfig(StructField field)
    {
        if (field.Metadata is null ||
            !field.Metadata.ContainsKey(StartKey))
            return null;

        if (!long.TryParse(field.Metadata[StartKey], out long start))
            return null;

        long step = 1;
        if (field.Metadata.TryGetValue(StepKey, out string? stepStr))
            long.TryParse(stepStr, out step);

        long? highWaterMark = null;
        if (field.Metadata.TryGetValue(HighWaterMarkKey, out string? hwmStr) &&
            long.TryParse(hwmStr, out long hwm))
            highWaterMark = hwm;

        bool allowExplicit = false;
        if (field.Metadata.TryGetValue(AllowExplicitInsertKey, out string? aeStr))
            bool.TryParse(aeStr, out allowExplicit);

        return new IdentityColumnConfig
        {
            Start = start,
            Step = step,
            HighWaterMark = highWaterMark,
            AllowExplicitInsert = allowExplicit,
        };
    }

    /// <summary>
    /// Returns true if the field is an identity column.
    /// </summary>
    public static bool IsIdentityColumn(StructField field) =>
        field.Metadata is not null && field.Metadata.ContainsKey(StartKey);

    /// <summary>
    /// Creates identity column metadata for a new field.
    /// </summary>
    public static Dictionary<string, string> CreateMetadata(
        long start = 1, long step = 1, bool allowExplicitInsert = false)
    {
        if (step == 0)
            throw new ArgumentException("Identity column step cannot be 0.", nameof(step));

        return new Dictionary<string, string>
        {
            [StartKey] = start.ToString(),
            [StepKey] = step.ToString(),
            [AllowExplicitInsertKey] = allowExplicitInsert.ToString().ToLowerInvariant(),
        };
    }

    /// <summary>
    /// Updates the high water mark in a field's metadata.
    /// Returns a new field with the updated metadata.
    /// </summary>
    public static StructField UpdateHighWaterMark(StructField field, long highWaterMark)
    {
        var metadata = new Dictionary<string, string>();
        if (field.Metadata is not null)
            foreach (var kvp in field.Metadata)
                metadata[kvp.Key] = kvp.Value;

        metadata[HighWaterMarkKey] = highWaterMark.ToString();

        return new StructField
        {
            Name = field.Name,
            Type = field.Type,
            Nullable = field.Nullable,
            Metadata = metadata,
        };
    }

    /// <summary>
    /// Computes the next identity value after the given high water mark.
    /// </summary>
    public static long NextValue(IdentityColumnConfig config)
    {
        if (config.HighWaterMark is null)
            return config.Start;

        return checked(config.HighWaterMark.Value + config.Step);
    }

    /// <summary>
    /// Generates <paramref name="count"/> sequential identity values
    /// starting from the next value after the current high water mark.
    /// Returns the values and the new high water mark.
    /// </summary>
    public static (long[] Values, long NewHighWaterMark) GenerateValues(
        IdentityColumnConfig config, int count)
    {
        if (count <= 0)
            return ([], config.HighWaterMark ?? config.Start);

        long start = NextValue(config);
        var values = new long[count];

        for (int i = 0; i < count; i++)
            values[i] = checked(start + (long)i * config.Step);

        long newHwm = values[count - 1];
        return (values, newHwm);
    }

    /// <summary>
    /// Finds the highest (or lowest for negative step) value in an array
    /// of explicitly provided identity values.
    /// </summary>
    public static long? FindExplicitHighWaterMark(
        Apache.Arrow.Int64Array array, long step)
    {
        long? hwm = null;
        for (int i = 0; i < array.Length; i++)
        {
            if (array.IsNull(i)) continue;
            long val = array.GetValue(i)!.Value;

            if (hwm is null)
                hwm = val;
            else if (step > 0 && val > hwm.Value)
                hwm = val;
            else if (step < 0 && val < hwm.Value)
                hwm = val;
        }
        return hwm;
    }
}
