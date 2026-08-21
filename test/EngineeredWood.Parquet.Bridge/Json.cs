// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text;

namespace EngineeredWood.Parquet.Bridge;

/// <summary>
/// The few JSON shapes the bridge protocol needs, written by hand.
/// </summary>
/// <remarks>
/// The control messages are three fixed object shapes. Hand-writing them keeps the bridge free of
/// a serializer and its reflection, which matters because this executable is meant to be
/// copy-adaptable by other implementations.
/// </remarks>
public static class Json
{
    /// <summary>Encodes a JSON string literal, including the quotes.</summary>
    /// <param name="value">The text to encode.</param>
    public static string String(string value)
    {
        var builder = new StringBuilder(value.Length + 2);
        builder.Append('"');
        foreach (char character in value)
        {
            switch (character)
            {
                case '"': builder.Append("\\\""); break;
                case '\\': builder.Append("\\\\"); break;
                case '\b': builder.Append("\\b"); break;
                case '\f': builder.Append("\\f"); break;
                case '\n': builder.Append("\\n"); break;
                case '\r': builder.Append("\\r"); break;
                case '\t': builder.Append("\\t"); break;
                default:
                    if (character < ' ')
                        builder.Append(CultureInfo.InvariantCulture, $"\\u{(int)character:x4}");
                    else
                        builder.Append(character);
                    break;
            }
        }

        builder.Append('"');
        return builder.ToString();
    }

    /// <summary>Encodes a flat object of profile options. Values may be strings, integers, or booleans.</summary>
    /// <param name="values">The option names and values to encode.</param>
    public static string Object(IReadOnlyDictionary<string, object> values) =>
        "{" + string.Join(",", values.Select(entry => $"{String(entry.Key)}:{Value(entry.Value)}")) + "}";

    private static string Value(object value) => value switch
    {
        string text => String(text),
        bool flag => flag ? "true" : "false",
        int number => number.ToString(CultureInfo.InvariantCulture),
        long number => number.ToString(CultureInfo.InvariantCulture),
        _ => throw new ArgumentException(
            $"writer profile options must be a string, integer, or boolean: {value.GetType().Name}",
            nameof(value)),
    };
}
