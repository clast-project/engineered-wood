// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.Expressions;
using EngineeredWood.Iceberg.Values;

namespace EngineeredWood.Iceberg.Tests.Corpus;

/// <summary>
/// The iceberg-go corpus (<c>TestData/corpus/iceberg-go</c>): a second writer's tables, including the
/// timestamptz_ns columns the Spark corpus cannot carry.
/// </summary>
public class IcebergGoCorpusTests
{
    private static readonly IcebergCorpus Corpus = IcebergCorpus.IcebergGo;

    [Fact]
    public void CorpusIsFromAFixedIcebergGo()
    {
        Assert.StartsWith("v0.8.", Corpus.Info.GetProperty("iceberg_go_version").GetString());
        Assert.Equal(["v3_dv", "v3_part", "v2_upgrade", "v3_cow", "v3_eqdel"], Corpus.Tables);
    }

    [Theory]
    [MemberData(nameof(IcebergCorpus.IcebergGoTables), MemberType = typeof(IcebergCorpus))]
    public void MetadataReads(string table)
    {
        var go = Corpus.Expected(table).GetProperty("describe");
        var md = Corpus.Metadata(table);

        Assert.Equal(go.GetProperty("format_version").GetInt32(), md.FormatVersion);
        Assert.Equal(go.GetProperty("current_snapshot_id").GetInt64(), md.CurrentSnapshotId);
        Assert.Equal(go.GetProperty("current_schema_id").GetInt32(), md.CurrentSchemaId);
        Assert.Equal(go.GetProperty("last_sequence_number").GetInt64(), md.LastSequenceNumber);
        Assert.Equal(
            go.GetProperty("snapshots").EnumerateArray().Select(s => s.GetProperty("snapshot_id").GetInt64()),
            md.Snapshots.Select(s => s.SnapshotId));
        if (md.FormatVersion >= 3)
            Assert.Equal(go.GetProperty("next_row_id").GetInt64(), md.NextRowId);
    }

    /// <summary>v3_dv adds <c>score int</c> with an initial default of 42, which go reads for every old row.</summary>
    [Fact]
    public void DefaultReadsAsGoReadsIt()
    {
        var md = Corpus.Metadata("v3_dv");
        var score = md.Schemas.Single(s => s.SchemaId == md.CurrentSchemaId).Fields.Single(f => f.Name == "score");
        var initial = SingleValueJson.Parse(score.Type, score.InitialDefault!.Value);
        Assert.Equal(LiteralValue.Of(42), initial);

        var scan = Corpus.Expected("v3_dv").GetProperty("scan");
        int column = scan.GetProperty("columns").EnumerateArray().TakeWhile(c => c.GetProperty("name").GetString() != "score").Count();
        Assert.All(scan.GetProperty("rows").EnumerateArray(), row => Assert.Equal(42, row[column].GetInt32()));
    }

    /// <summary>
    /// Every bound go wrote decodes, nanosecond timestamps included, the bounded columns are the ones go
    /// reports, and the bounds of the live data files enclose every value go scans.
    /// </summary>
    [Theory]
    [MemberData(nameof(IcebergCorpus.IcebergGoTables), MemberType = typeof(IcebergCorpus))]
    public void BoundsDecodeAndEncloseTheRows(string table)
    {
        var md = Corpus.Metadata(table);
        var schema = md.Schemas.Single(s => s.SchemaId == md.CurrentSchemaId);
        var current = md.Snapshots.Single(s => s.SnapshotId == md.CurrentSnapshotId);
        var files = Corpus.ReadFileBounds(current);

        var goBoundIds = Corpus.Expected(table).GetProperty("describe").GetProperty("manifests").EnumerateArray()
            .SelectMany(m => m.GetProperty("entries").EnumerateArray())
            .Where(e => e.GetProperty("status").GetString() != "DELETED")
            .ToDictionary(e => e.GetProperty("file_path").GetString()!, e => e.GetProperty("lower_bound_ids").EnumerateArray().Select(i => i.GetInt32()).OrderBy(i => i).ToArray());

        var lowest = new Dictionary<int, LiteralValue>();
        var highest = new Dictionary<int, LiteralValue>();
        foreach (var file in files)
        {
            if (goBoundIds.TryGetValue(file.FilePath, out var ids))
                Assert.Equal(ids, file.Lower.Keys.OrderBy(i => i).ToArray());
            if (file.Content != 0)
                continue;

            foreach (var (id, bytes) in file.Lower)
            {
                // A copy-on-write rewrite also bounds the reserved lineage columns it materializes
                // (_row_id 2147483540, _last_updated_sequence_number 2147483539), which no schema lists.
                if (schema.Fields.FirstOrDefault(f => f.Id == id) is not { } bounded)
                {
                    Assert.True(id >= 2147483539, $"{table}: bound for unknown field {id}");
                    Assert.True(SingleValueBinary.TryDecodeBound(IcebergType.Long, bytes, out _));
                    continue;
                }

                var type = bounded.Type;
                Assert.True(SingleValueBinary.TryDecodeBound(type, bytes, out var lower), $"{table} field {id} lower bound declined");
                Assert.True(SingleValueBinary.TryDecodeBound(type, file.Upper[id], out var upper), $"{table} field {id} upper bound declined");
                Assert.True(lower.CompareTo(upper) <= 0);
                if (type is TimestamptzNsType)
                    Assert.Equal(LiteralValue.Kind.TimestampNanos, lower.Type);
                if (!lowest.TryGetValue(id, out var lo) || lower.CompareTo(lo) < 0)
                    lowest[id] = lower;
                if (!highest.TryGetValue(id, out var hi) || upper.CompareTo(hi) > 0)
                    highest[id] = upper;
            }
        }

        var scan = Corpus.Expected(table).GetProperty("scan");
        var columns = scan.GetProperty("columns").EnumerateArray().Select(c => c.GetProperty("name").GetString()!).ToList();
        foreach (var field in schema.Fields.Where(f => lowest.ContainsKey(f.Id)))
        {
            int c = columns.IndexOf(field.Name);
            foreach (var row in scan.GetProperty("rows").EnumerateArray())
            {
                var v = GoLiteral(field.Type, row[c]);
                Assert.True(lowest[field.Id].CompareTo(v) <= 0 && v.CompareTo(highest[field.Id]) <= 0,
                    $"{table} {field.Name} = {v} outside [{lowest[field.Id]}, {highest[field.Id]}]");
            }
        }

        Assert.NotEmpty(lowest);
    }

    /// <summary>go's scan values: integers, strings, and timestamps as integers in the column's own unit.</summary>
    private static LiteralValue GoLiteral(IcebergType type, JsonElement v) => type switch
    {
        LongType => LiteralValue.Of(v.GetInt64()),
        IntegerType => LiteralValue.Of(v.GetInt32()),
        StringType => LiteralValue.Of(v.GetString()!),
        TimestamptzNsType or TimestampNsType => LiteralValue.TimestampNanosOf(v.GetInt64()),
        TimestamptzType or TimestampType => LiteralValue.Of(new DateTimeOffset(1970, 1, 1, 0, 0, 0, TimeSpan.Zero).AddTicks(v.GetInt64() * 10)),
        _ => throw new NotSupportedException(type.ToString()),
    };
}
