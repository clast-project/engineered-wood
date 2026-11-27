// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;

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
    private ScanRows(IReadOnlyList<string> columns, IReadOnlyList<IReadOnlyDictionary<string, JsonElement>> rows)
    {
        Columns = columns;
        Rows = rows;
    }

    public IReadOnlyList<string> Columns { get; }

    public IReadOnlyList<IReadOnlyDictionary<string, JsonElement>> Rows { get; }

    public static ScanRows From(JsonElement result)
    {
        var names = result.GetProperty("columns").EnumerateArray().Select(c => c.GetProperty("name").GetString()!).ToList();
        var rows = result.GetProperty("rows").EnumerateArray()
            .Select(row => (IReadOnlyDictionary<string, JsonElement>)names
                .Zip(row.EnumerateArray(), (n, v) => (n, v)).ToDictionary(x => x.n, x => x.v.Clone()))
            .ToList();
        return new ScanRows(names, rows);
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
