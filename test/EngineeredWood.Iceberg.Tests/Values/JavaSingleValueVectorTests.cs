// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.Expressions;
using EngineeredWood.Iceberg.Serialization;
using EngineeredWood.Iceberg.Values;

namespace EngineeredWood.Iceberg.Tests.Values;

/// <summary>
/// The binary and JSON single-value codecs against what Iceberg Java 1.12 itself writes and accepts,
/// recorded in <c>TestData/value-codecs/single-value-vectors.json</c> by <c>SingleValueOracle.java</c>.
/// </summary>
public class JavaSingleValueVectorTests
{
    /// <summary>
    /// Parse cases where EngineeredWood deliberately answers differently from Java, and why. Every other
    /// case must agree: Java accepts and EW reads the same value, or Java refuses and EW refuses.
    /// </summary>
    private static readonly Dictionary<(string Type, string Json), string> Divergences = new()
    {
        // Java does not check a decimal default against its precision; ten digits do not fit decimal(9, 2).
        [("decimal(9, 2)", "\"12345678.90\"")] = "refused: more digits than the precision",
        // Java reads an infinity, then writes "Infinity", which it cannot read back.
        [("float", "1e40")] = "refused: out of range for a float",
        // A legal Iceberg date that .NET's date types cannot hold.
        [("date", "\"+10000-01-01\"")] = "unsupported: outside the years 1 to 9999",
    };

    public static TheoryData<string> EncodeCases() => CaseIds("encode");

    public static TheoryData<string> ParseCases() => CaseIds("parse");

    /// <summary>
    /// Java's bytes decode, and re-encode to the same bytes. Java's JSON text reads as the same value, and
    /// EW writes the same text, except hex case (Java writes upper case, EW the spec's lower case).
    /// </summary>
    [Theory]
    [MemberData(nameof(EncodeCases))]
    public void EncodeCase(string id)
    {
        var c = Case(id);
        var type = ParseType(c.GetProperty("type").GetString()!);
        byte[] javaBinary = FromHex(c.GetProperty("binary").GetString()!);

        var decoded = SingleValueBinary.Decode(type, javaBinary);
        Assert.Equal(IcebergLiterals.KindOf(type), decoded.Type);
        Assert.Equal(javaBinary, SingleValueBinary.Encode(type, decoded));

        string javaJson = c.GetProperty("json").GetString()!;
        using var doc = JsonDocument.Parse(javaJson);
        if (decoded.Type is LiteralValue.Kind.Float or LiteralValue.Kind.Double && IsNonFinite(decoded))
        {
            // Java writes "NaN" / "Infinity" strings it then refuses to read; EW refuses both ways.
            Assert.Throws<FormatException>(() => SingleValueJson.Parse(type, doc.RootElement));
            Assert.Throws<ArgumentException>(() => SingleValueJson.ToJsonString(type, decoded));
            return;
        }

        var parsed = SingleValueJson.Parse(type, doc.RootElement);
        Assert.Equal(decoded, parsed);

        string written = SingleValueJson.ToJsonString(type, decoded);
        if (type is FloatType or DoubleType)
        {
            // Any shortest round-tripping text is right; Java 17's Float.toString is not always shortest.
            using var back = JsonDocument.Parse(written);
            Assert.Equal(decoded, SingleValueJson.Parse(type, back.RootElement));
        }
        else if (type is StringType)
        {
            // System.Text.Json escapes a character outside the BMP even when relaxed; Java writes it raw.
            using var back = JsonDocument.Parse(written);
            Assert.Equal(doc.RootElement.GetString(), back.RootElement.GetString());
        }
        else
        {
            Assert.Equal(javaJson, written, ignoreCase: type is FixedType or BinaryType);
        }
    }

    /// <summary>EW accepts exactly the JSON texts Java accepts, reading the same value.</summary>
    [Theory]
    [MemberData(nameof(ParseCases))]
    public void ParseCase(string id)
    {
        var c = Case(id);
        string typeName = c.GetProperty("type").GetString()!;
        string jsonText = c.GetProperty("json").GetString()!;
        var type = ParseType(typeName);
        using var doc = JsonDocument.Parse(jsonText);

        if (Divergences.TryGetValue((typeName, jsonText), out string? why))
        {
            Assert.True(c.TryGetProperty("binary", out _), $"Java now refuses {jsonText} too; drop the divergence");
            var e = Record.Exception(() => SingleValueJson.Parse(type, doc.RootElement));
            Assert.True(e is FormatException or NotSupportedException, $"expected {why}, got {e?.GetType().Name ?? "a value"}");
            return;
        }

        if (c.TryGetProperty("error", out var javaError))
        {
            var e = Record.Exception(() => SingleValueJson.Parse(type, doc.RootElement));
            Assert.True(e is FormatException, $"Java refused {jsonText} ({javaError.GetString()}), EW gave {e?.GetType().Name ?? "a value"}");
            return;
        }

        var value = SingleValueJson.Parse(type, doc.RootElement);
        Assert.Equal(c.GetProperty("binary").GetString(), ToHex(SingleValueBinary.Encode(type, value)));
        Assert.Equal(
            c.GetProperty("rewritten").GetString(),
            SingleValueJson.ToJsonString(type, value),
            ignoreCase: type is FixedType or BinaryType);
    }

    [Fact]
    public void VectorsCameFromTheCorpusIcebergVersion()
    {
        var root = Vectors().RootElement;
        Assert.Equal("1.12.0", root.GetProperty("iceberg_version").GetString());
        Assert.True(root.GetProperty("cases").GetArrayLength() > 150);
    }

    // ── Plumbing ──

    private static JsonDocument? s_vectors;

    private static JsonDocument Vectors() =>
        s_vectors ??= JsonDocument.Parse(File.ReadAllText(
            Path.Combine(AppContext.BaseDirectory, "TestData", "value-codecs", "single-value-vectors.json")));

    private static TheoryData<string> CaseIds(string op)
    {
        var data = new TheoryData<string>();
        int i = 0;
        foreach (var c in Vectors().RootElement.GetProperty("cases").EnumerateArray())
        {
            if (c.GetProperty("op").GetString() == op)
                data.Add($"{i}: {c.GetProperty("type").GetString()} {(op == "parse" ? c.GetProperty("json").GetString() : c.GetProperty("java_value").GetString())}");
            i++;
        }

        return data;
    }

    private static JsonElement Case(string id) =>
        Vectors().RootElement.GetProperty("cases")[int.Parse(id.Substring(0, id.IndexOf(':')), System.Globalization.CultureInfo.InvariantCulture)];

    internal static IcebergType ParseType(string name) =>
        IcebergJsonSerializer.Deserialize<IcebergType>(JsonSerializer.Serialize(name));

    private static bool IsNonFinite(LiteralValue v) => v.Type == LiteralValue.Kind.Float
        ? float.IsNaN(v.AsFloat) || float.IsInfinity(v.AsFloat)
        : double.IsNaN(v.AsDouble) || double.IsInfinity(v.AsDouble);

    internal static byte[] FromHex(string hex)
    {
        var bytes = new byte[hex.Length / 2];
        for (int i = 0; i < bytes.Length; i++)
            bytes[i] = Convert.ToByte(hex.Substring(2 * i, 2), 16);
        return bytes;
    }

    internal static string ToHex(byte[] bytes) =>
        string.Concat(bytes.Select(b => b.ToString("x2", System.Globalization.CultureInfo.InvariantCulture)));
}
