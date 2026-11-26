// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Text.Json;
using EngineeredWood.Iceberg.Expressions;
using EngineeredWood.Iceberg.Manifest;
using EngineeredWood.Iceberg.Values;
using Ex = EngineeredWood.Iceberg.Expressions.Expressions;

namespace EngineeredWood.Iceberg.Tests.Corpus;

/// <summary>
/// Column bounds read from Spark's manifests are decoded for planning (<see cref="DataFile.ColumnLowerBounds"/>),
/// and they are the values Spark's own <c>files</c> table decodes (<c>readable_metrics</c>).
/// </summary>
public class DecodedBoundsCorpusTests
{
    private static readonly IcebergCorpus Corpus = IcebergCorpus.Spark;

    [Theory]
    [MemberData(nameof(IcebergCorpus.SparkTables), MemberType = typeof(IcebergCorpus))]
    public async Task DecodedBoundsAreSparksReadableMetrics(string table)
    {
        var md = Corpus.Metadata(table);
        var schema = md.Schemas.Single(s => s.SchemaId == md.CurrentSchemaId);
        var files = (await LiveFilesAsync(md)).Where(f => f.Content == FileContent.Data)
            .ToDictionary(f => f.FilePath);

        int compared = 0;
        foreach (var sf in Corpus.Expected(table).GetProperty("files").EnumerateArray())
        {
            // Iceberg 1.12 cannot render readable_metrics for a uuid column; the corpus drops it there.
            if (sf.GetProperty("content").GetInt32() != 0 || !sf.TryGetProperty("readable_metrics", out var metrics))
                continue;
            var file = files[sf.GetProperty("file_path").GetString()!];
            foreach (var column in metrics.EnumerateObject())
            {
                var field = schema.Fields.SingleOrDefault(f => f.Name == column.Name);
                if (field?.Type is not PrimitiveType type || type is VariantType)
                    continue;
                string where = $"{table}: {file.FilePath} {column.Name}";
                compared += Compare(column.Value, "lower_bound", type, field.Id, file.ColumnLowerBounds, where);
                compared += Compare(column.Value, "upper_bound", type, field.Id, file.ColumnUpperBounds, where);
            }
        }

        // Every table with bounds compares something, unless the corpus had to drop readable_metrics for it
        // (recorded as files_error).
        if (files.Values.Any(f => f.LowerBounds is { Count: > 0 }) && !Corpus.Expected(table).TryGetProperty("files_error", out _))
            Assert.True(compared > 0, $"{table}: no bounds compared");
    }

    [Fact]
    public async Task PlanningPrunesSparkFilesByTheirDecodedBounds()
    {
        var md = Corpus.Metadata("t_bucket");
        var fs = new CorpusFileSystem(Corpus);
        var files = (await LiveFilesAsync(md)).Where(f => f.Content == FileContent.Data).ToList();
        var schema = md.Schemas.Single(s => s.SchemaId == md.CurrentSchemaId);
        int idField = schema.Fields.Single(f => f.Name == "id").Id;
        Assert.True(files.Count > 1);

        long Bound(IReadOnlyDictionary<int, EngineeredWood.Expressions.LiteralValue>? bounds) =>
            bounds![idField] is var v && v.Type == EngineeredWood.Expressions.LiteralValue.Kind.Int64 ? v.AsInt64 : v.AsInt32;

        foreach (var file in files)
        {
            var result = await new TableScan(md, fs).Filter(Ex.Equal("id", Bound(file.ColumnLowerBounds))).PlanFilesAsync();
            Assert.Contains(result.DataFiles, f => f.FilePath == file.FilePath);
            Assert.True(result.DataFiles.Count < files.Count, "a point filter prunes some files");
        }

        long max = files.Max(f => Bound(f.ColumnUpperBounds));
        Assert.Empty((await new TableScan(md, fs).Filter(Ex.GreaterThan("id", max)).PlanFilesAsync()).DataFiles);
    }

    private static int Compare(JsonElement metric, string key, IcebergType type, int fieldId,
        IReadOnlyDictionary<int, EngineeredWood.Expressions.LiteralValue>? decoded, string where)
    {
        if (!metric.TryGetProperty(key, out var json) || json.ValueKind == JsonValueKind.Null)
        {
            Assert.True(decoded is null || !decoded.ContainsKey(fieldId), $"{where} {key}: Spark has none");
            return 0;
        }
        Assert.True(decoded is not null && decoded.TryGetValue(fieldId, out _), $"{where} {key}: EW decoded none");
        var expected = SingleValueBinary.Encode(type, SparkCorpusTests.SparkLiteral(type, json));
        Assert.True(SingleValueBinary.Encode(type, decoded![fieldId]).AsSpan().SequenceEqual(expected), $"{where} {key}");
        return 1;
    }

    private static async Task<List<DataFile>> LiveFilesAsync(TableMetadata md)
    {
        var fs = new CorpusFileSystem(Corpus);
        var current = md.Snapshots.Single(s => s.SnapshotId == md.CurrentSnapshotId);
        var result = new List<DataFile>();
        foreach (var mle in await ManifestIO.ReadSnapshotManifestListAsync(fs, md, current, default))
        {
            foreach (var entry in await ManifestIO.ReadSnapshotManifestAsync(fs, md, current, mle, default))
            {
                if (entry.Status != ManifestEntryStatus.Deleted)
                    result.Add(entry.DataFile);
            }
        }
        return result;
    }
}
