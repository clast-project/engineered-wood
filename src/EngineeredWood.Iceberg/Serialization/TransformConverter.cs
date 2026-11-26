// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text.Json;
using System.Text.RegularExpressions;
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

    private static readonly Regex WidthPattern =
        new(@"^(bucket|truncate)\[([0-9]+)\]\z", RegexOptions.CultureInvariant);

    internal static Transform Parse(string? value)
    {
        if (value is null)
            throw new JsonException("Transform cannot be null");

        // Only an exact "bucket[N]" / "truncate[W]" with a positive width is the known transform, as in Java,
        // which refuses a width of 0. Anything else with the same prefix ("bucket[16,2]", "bucket[0]") is kept
        // as a transform this library does not know: it still reads and round-trips, and no file can be
        // committed under it.
        var m = WidthPattern.Match(value);
        if (m.Success && int.TryParse(m.Groups[2].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var width)
            && width > 0)
        {
            switch (m.Groups[1].Value)
            {
                case "bucket": return new BucketTransform(width);
                case "truncate": return new TruncateTransform(width);
            }
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
