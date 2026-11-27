// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using System.Text.RegularExpressions;

namespace EngineeredWood.Iceberg.Tests.Interop;

/// <summary>
/// The engines that read an Iceberg table by its metadata file, behind one shape, so a test can ask each of them
/// the same question. Every driver reports a scan the same way: <c>columns</c> (name, type) and <c>rows</c> of
/// JSON-native values, with timestamps as integers in the column's own unit.
/// </summary>
internal sealed class IcebergReader
{
    private readonly Action _require;
    private readonly Func<string, bool, JsonElement> _scan;

    private IcebergReader(string name, Action require, Func<string, bool, JsonElement> scan, bool readsRowLineage)
    {
        Name = name;
        _require = require;
        _scan = scan;
        ReadsRowLineage = readsRowLineage;
    }

    public static readonly IcebergReader IcebergGoReader = new(
        "iceberg-go", IcebergGo.Require,
        (md, lineage) => IcebergGo.Invoke("scan", new { metadata_location = md, row_lineage = lineage }),
        readsRowLineage: true);

    public static readonly IcebergReader DuckDb = new(
        "duckdb", DuckDbIceberg.Require, DuckDbIceberg.Scan, readsRowLineage: true);

    /// <summary>pyiceberg 0.12 cannot select the lineage columns (see <see cref="PyIceberg"/>).</summary>
    public static readonly IcebergReader PyIcebergReader = new(
        "pyiceberg", PyIceberg.Require, (md, _) => PyIceberg.Scan(md), readsRowLineage: false);

    public static IcebergReader Named(string name) => name switch
    {
        "iceberg-go" => IcebergGoReader,
        "duckdb" => DuckDb,
        "pyiceberg" => PyIcebergReader,
        _ => throw new ArgumentOutOfRangeException(nameof(name), name, null),
    };

    public string Name { get; }

    public bool ReadsRowLineage { get; }

    public void Require() => _require();

    /// <summary>Every row of the table as this reader reports it, lineage columns included when asked for and
    /// <see cref="ReadsRowLineage"/>.</summary>
    public ScanRows Scan(string metadataLocation, bool rowLineage) =>
        ScanRows.From(_scan(metadataLocation, rowLineage && ReadsRowLineage));

    public override string ToString() => Name;
}

/// <summary>One reader's rows, comparable with another's regardless of row and column order.</summary>
internal sealed class ScanRows
{
    private ScanRows(
        IReadOnlyList<string> columns, IReadOnlyDictionary<string, string> types,
        IReadOnlyList<IReadOnlyDictionary<string, JsonElement>> rows)
    {
        Columns = columns;
        Types = types;
        Rows = rows;
    }

    public IReadOnlyList<string> Columns { get; }

    /// <summary>Each column's type as the reader reported it, in one logical spelling (see
    /// <see cref="LogicalType"/>), so readers built on pyarrow and on arrow-go compare.</summary>
    public IReadOnlyDictionary<string, string> Types { get; }

    /// <summary>The columns and their logical types, in name order, as one comparable list.</summary>
    public IReadOnlyList<string> Schema =>
        Columns.OrderBy(c => c, StringComparer.Ordinal).Select(c => $"{c}: {Types[c]}").ToList();

    public IReadOnlyList<IReadOnlyDictionary<string, JsonElement>> Rows { get; }

    public static ScanRows From(JsonElement result)
    {
        var columns = result.GetProperty("columns").EnumerateArray().ToList();
        var names = columns.Select(c => c.GetProperty("name").GetString()!).ToList();
        var types = columns.ToDictionary(
            c => c.GetProperty("name").GetString()!, c => LogicalType(c.GetProperty("type").GetString()!));
        var rows = result.GetProperty("rows").EnumerateArray()
            .Select(row => (IReadOnlyDictionary<string, JsonElement>)names
                .Zip(row.EnumerateArray(), (n, v) => (n, v)).ToDictionary(x => x.n, x => x.v.Clone()))
            .ToList();
        return new ScanRows(names, types, rows);
    }

    private static readonly Regex Timestamp = new(@"^timestamp\[(s|ms|us|ns)(?:, tz=(.+))?\]$");
    private static readonly Regex Decimal = new(@"^decimal(?:128|256)?\((\d+), ?(\d+)\)$");
    private static readonly Regex FixedBinary = new(@"^fixed_size_binary\[(\d+)\]$");
    private static readonly Regex Time = new(@"^time(?:32|64)\[(s|ms|us|ns)\]$");

    /// <summary>
    /// An Arrow type's name, as pyarrow (DuckDB, pyiceberg) or arrow-go (iceberg-go) prints it, in one logical
    /// spelling: <c>string</c> and <c>large_string</c> and <c>utf8</c> are all <c>string</c>, and a timestamp keeps
    /// its unit and whether it has a zone (<c>timestamp[us]</c>, <c>timestamptz[us]</c>), which values alone would
    /// not show. A zone's name is dropped: a <c>timestamptz</c> value is an instant, and readers name the zone
    /// differently (MEASURED: on a machine in America/Los_Angeles one reader reports <c>tz=America/Los_Angeles</c>
    /// where the others report <c>tz=UTC</c>, for the same instants). A name it does not know fails the test, so a
    /// new type is mapped deliberately.
    /// </summary>
    public static string LogicalType(string arrowType)
    {
        string t = arrowType.Trim();
        switch (t)
        {
            case "bool": case "boolean": return "boolean";
            case "int8": case "int16": case "int32": case "int64": return t;
            case "float": case "float32": return "float32";
            case "double": case "float64": return "float64";
            case "string": case "large_string": case "utf8": case "large_utf8": case "string_view": return "string";
            case "binary": case "large_binary": case "binary_view": return "binary";
            case "date32": case "date32[day]": return "date";
        }

        if (Timestamp.Match(t) is { Success: true } ts)
            return (ts.Groups[2].Success ? "timestamptz[" : "timestamp[") + ts.Groups[1].Value + "]";
        if (Decimal.Match(t) is { Success: true } d)
            return $"decimal({d.Groups[1].Value}, {d.Groups[2].Value})";
        if (FixedBinary.Match(t) is { Success: true } f)
            return $"fixed[{f.Groups[1].Value}]";
        if (Time.Match(t) is { Success: true } tm)
            return $"time[{tm.Groups[1].Value}]";

        throw new Xunit.Sdk.XunitException(
            $"A reader reported the column type '{arrowType}', which {nameof(LogicalType)} does not map; add it.");
    }

    public IEnumerable<long> Ids => Rows.Select(r => r["id"].GetInt64()).OrderBy(x => x);

    /// <summary>Each row as one string, its columns in name order and each value in one spelling (the drivers
    /// differ in how they escape a string), sorted.</summary>
    public IReadOnlyList<string> Canonical(IEnumerable<string>? columns = null)
    {
        var names = (columns ?? Columns).OrderBy(n => n, StringComparer.Ordinal).ToList();
        return Rows
            .Select(r => string.Join(", ", names.Select(n => $"{n}={Value(r[n])}")))
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();
    }

    private static string Value(JsonElement v) => v.ValueKind switch
    {
        JsonValueKind.String => JsonSerializer.Serialize(v.GetString()),
        _ => v.GetRawText(),
    };
}
