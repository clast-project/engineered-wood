// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.Json.Serialization;

namespace EngineeredWood.Iceberg.Serialization;

internal sealed class TransformConverter : JsonConverter<Transform>
{
    public override bool CanConvert(Type typeToConvert) =>
        typeof(Transform).IsAssignableFrom(typeToConvert);

    public override Transform Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        => Parse(reader.GetString());

    public override void Write(Utf8JsonWriter writer, Transform value, JsonSerializerOptions options)
        => writer.WriteStringValue(Format(value));

    internal static Transform Parse(string? value)
    {
        if (value is null)
            throw new JsonException("Transform cannot be null");

        if (value.StartsWith("bucket[", StringComparison.Ordinal))
        {
            var n = int.Parse(value[7..^1]);
            return new BucketTransform(n);
        }

        if (value.StartsWith("truncate[", StringComparison.Ordinal))
        {
            var w = int.Parse(value[9..^1]);
            return new TruncateTransform(w);
        }

        return value switch
        {
            "identity" => Transform.Identity,
            "year" => Transform.Year,
            "month" => Transform.Month,
            "day" => Transform.Day,
            "hour" => Transform.Hour,
            "void" => Transform.Void,
            // v3 readers must accept transforms they do not know; keep the name so it round-trips.
            _ => new UnknownTransform(value),
        };
    }

    internal static string Format(Transform value)
    {
        return value switch
        {
            IdentityTransform => "identity",
            BucketTransform b => $"bucket[{b.NumBuckets}]",
            TruncateTransform t => $"truncate[{t.Width}]",
            YearTransform => "year",
            MonthTransform => "month",
            DayTransform => "day",
            HourTransform => "hour",
            VoidTransform => "void",
            UnknownTransform u => u.Name,
            _ => throw new JsonException($"Unknown transform: {value.GetType().Name}")
        };
    }
}
