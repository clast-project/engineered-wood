// Copyright (c) clast-project. All rights reserved.
// Licensed under the Apache License, Version 2.0. See LICENSE in the project root for license information.

using System.Globalization;
using System.Text.Json;
using EngineeredWood.Expressions;
using EngineeredWood.Iceberg.Manifest;
using EngineeredWood.Iceberg.Values;

namespace EngineeredWood.Iceberg.Tests.Corpus;

/// <summary>
/// The Spark 4.1 + Iceberg 1.12 corpus (<c>TestData/corpus/spark</c>) against EngineeredWood, and against
/// what Spark itself reads back from it.
/// </summary>
public class SparkCorpusTests
{
    private static readonly IcebergCorpus Corpus = IcebergCorpus.Spark;

    [Fact]
    public void CorpusIsFromTheReferenceVersions()
    {
        Assert.Equal("1.12.0", Corpus.Info.GetProperty("iceberg_version").GetString());
        Assert.StartsWith("4.1.", Corpus.Info.GetProperty("spark_version").GetString());
        Assert.Empty(Corpus.Info.GetProperty("failed").EnumerateArray());
        Assert.Equal(13, Corpus.Tables.Count);
    }

    /// <summary>EW reads every table's current metadata, and sees the snapshots and refs Spark does.</summary>
    [Theory]
    [MemberData(nameof(IcebergCorpus.SparkTables), MemberType = typeof(IcebergCorpus))]
    public void MetadataReads(string table)
    {
        var expected = Corpus.Expected(table);
        var md = Corpus.Metadata(table);

        Assert.Equal(expected.GetProperty("format_version").GetInt32(), md.FormatVersion);
        var snapshots = expected.GetProperty("snapshots").EnumerateArray().ToList();
        Assert.Equal(
            snapshots.Select(s => s.GetProperty("snapshot_id").GetInt64()).OrderBy(x => x),
            md.Snapshots.Select(s => s.SnapshotId).OrderBy(x => x));
        var refs = expected.GetProperty("refs").EnumerateArray()
            .ToDictionary(r => r.GetProperty("name").GetString()!, r => r.GetProperty("snapshot_id").GetInt64());
        Assert.Equal(refs.Keys.OrderBy(k => k, StringComparer.Ordinal), md.Refs.Keys.OrderBy(k => k, StringComparer.Ordinal));
        foreach (var (name, id) in refs)
            Assert.Equal(id, md.Refs[name].SnapshotId);
        Assert.Equal(refs["main"], md.CurrentSnapshotId);

        if (md.FormatVersion >= 3)
        {
            // Every v3 snapshot carries first-row-id; one from before a v2 -> v3 upgrade does not.
            Assert.NotNull(md.NextRowId);
            Assert.NotNull(md.Snapshots.Single(s => s.SnapshotId == md.CurrentSnapshotId).FirstRowId);
        }
    }

    /// <summary>
    /// Every default Java wrote validates, reads as a value, and writes back as Java's text; and the
    /// value is what Spark reads for the rows written before the column existed.
    /// </summary>
    [Fact]
    public void DefaultsReadAsJavaWroteThem()
    {
        var md = Corpus.Metadata("defaults");
        var schema = md.Schemas.Single(s => s.SchemaId == md.CurrentSchemaId);
        var oldRow = Corpus.Expected("defaults").GetProperty("rows").EnumerateArray()
            .Single(r => r.GetProperty("id").GetInt64() == 0);

        int checkedDefaults = 0;
        foreach (var field in schema.Fields)
        {
            foreach (var json in new[] { field.InitialDefault, field.WriteDefault })
            {
                if (json is not { } j)
                    continue;
                SingleValueJson.Validate(field.Type, j);
                var value = SingleValueJson.Parse(field.Type, j);
                string written = SingleValueJson.ToJsonString(field.Type, value);
                if (field.Type is FloatType or DoubleType)
                    Assert.Equal(value, SingleValueJson.Parse(field.Type, JsonDocument.Parse(written).RootElement));
                else
                    Assert.Equal(j.GetRawText(), written, ignoreCase: field.Type is FixedType or BinaryType);
                checkedDefaults++;
            }

            if (field.InitialDefault is { } initial)
            {
                var value = SingleValueJson.Parse(field.Type, initial);
                var spark = SparkLiteral(field.Type, oldRow.GetProperty(field.Name));
                Assert.True(value.Equals(spark), $"{field.Name}: default {value} but Spark read {spark}");
            }
        }

        Assert.Equal(13 + 13, checkedDefaults); // every added column carries both defaults
    }

    /// <summary>
    /// Each bound in the <c>files</c> metadata table, decoded by <see cref="SingleValueBinary.TryDecodeBound"/>,
    /// is the value Spark's own <c>readable_metrics</c> shows for it.
    /// </summary>
    [Theory]
    [MemberData(nameof(IcebergCorpus.SparkTables), MemberType = typeof(IcebergCorpus))]
    public void BoundsDecodeAsSparkReadsThem(string table)
    {
        var md = Corpus.Metadata(table);
        var schema = md.Schemas.Single(s => s.SchemaId == md.CurrentSchemaId);
        int compared = 0;
        foreach (var file in Corpus.Expected(table).GetProperty("files").EnumerateArray())
        {
            if (!file.TryGetProperty("readable_metrics", out var readable))
                continue; // uuid columns: Spark could not render them (see generate_corpus.py)
            foreach (var metric in readable.EnumerateObject())
            {
                if (schema.Fields.FirstOrDefault(f => f.Name == metric.Name) is not { } field)
                    continue; // a nested field's dotted path
                foreach (var (boundsKey, readableKey) in new[] { ("lower_bounds", "lower_bound"), ("upper_bounds", "upper_bound") })
                {
                    // to_json leaves a null field out.
                    var sparkValue = metric.Value.TryGetProperty(readableKey, out var v) ? v : default;
                    bool hasRaw = file.TryGetProperty(boundsKey, out var raw) && raw.ValueKind == JsonValueKind.Object
                        && raw.TryGetProperty(field.Id.ToString(CultureInfo.InvariantCulture), out var bytes64);
                    if (!hasRaw)
                    {
                        Assert.True(sparkValue.ValueKind is JsonValueKind.Undefined or JsonValueKind.Null);
                        continue;
                    }

                    byte[] bytes = Convert.FromBase64String(raw.GetProperty(field.Id.ToString(CultureInfo.InvariantCulture)).GetString()!);
                    bool decoded = SingleValueBinary.TryDecodeBound(field.Type, bytes, out var bound);
                    if (field.Type is VariantType)
                    {
                        Assert.False(decoded); // variant bounds have their own encoding (G20)
                        continue;
                    }

                    Assert.True(decoded, $"{table} {field.Name} {boundsKey}: declined {Convert.ToBase64String(bytes)}");
                    if (sparkValue.ValueKind == JsonValueKind.Undefined)
                        continue; // a delete file's bounds, which readable_metrics leaves out
                    var expected = SparkLiteral(field.Type, sparkValue);
                    Assert.True(bound.Equals(expected), $"{table} {field.Name} {boundsKey}: {bound} != Spark's {expected}");
                    compared++;
                }
            }
        }

        if (table.StartsWith("t_", StringComparison.Ordinal) && table != "t_javatypes")
            Assert.True(compared > 100, $"only {compared} bounds compared");
    }

    /// <summary>The bound bytes in the manifests are the ones the <c>files</c> table reports.</summary>
    [Theory]
    [MemberData(nameof(IcebergCorpus.SparkTables), MemberType = typeof(IcebergCorpus))]
    public void ManifestBoundsAreTheFilesTablesBounds(string table)
    {
        var md = Corpus.Metadata(table);
        var current = md.Snapshots.Single(s => s.SnapshotId == md.CurrentSnapshotId);
        if (table == "t_javatypes")
        {
            // KNOWN DEFECT in Iceberg Java (apache/iceberg#17501, 1.8.0 through main), not in EngineeredWood.Avro:
            // its manifest writer encodes a plain Avro fixed (the identity(fixed[4]) partition value) as
            // length-prefixed bytes, and its reader decodes it the same way, so only Java reads its own file.
            // A conforming reader (fastavro too) takes the length as the fixed's first byte: the first entry's
            // 08 00 01 02 03 reads as fixed 08000102, then 03 as the next union's index, -2. Reading these manifests needs the Iceberg manifest codec to decode
            // fixed the Java way; when it does, delete the guard so the comparison below runs for this table too.
            var e = Assert.Throws<InvalidDataException>(() => Corpus.ReadFileBounds(current));
            Assert.Contains("union branch index -2 ", e.Message);
            return;
        }

        var fromManifests = Corpus.ReadFileBounds(current).Where(f => f.Content == 0).ToDictionary(f => f.FilePath);
        var fromSpark = Corpus.Expected(table).GetProperty("files").EnumerateArray()
            .Where(f => f.GetProperty("content").GetInt32() == 0).ToList();

        Assert.Equal(fromSpark.Count, fromManifests.Count);
        foreach (var file in fromSpark)
        {
            var bounds = fromManifests[file.GetProperty("file_path").GetString()!];
            AssertSameBounds(file, "lower_bounds", bounds.Lower);
            AssertSameBounds(file, "upper_bounds", bounds.Upper);
        }
    }

    /// <summary>
    /// The phase 0 exit criterion, held open for the manifest codec rewrite: EW reads every corpus manifest.
    /// Until #586 lands, EW refuses another writer's manifest schema (#775), and this passes on the refusal.
    /// </summary>
    [Theory]
    [MemberData(nameof(IcebergCorpus.SparkTables), MemberType = typeof(IcebergCorpus))]
    public async Task ManifestsRead_PendingIssue586(string table)
    {
        var md = Corpus.Metadata(table);
        var current = md.Snapshots.Single(s => s.SnapshotId == md.CurrentSnapshotId);
        var fs = new CorpusFileSystem(Corpus);
        try
        {
            var list = await ManifestIO.ReadManifestListAsync(fs, current.ManifestList);
            foreach (var manifest in list)
                await ManifestIO.ReadManifestAsync(fs, manifest.ManifestPath);
        }
        catch (NotSupportedException e) when (e.Message.Contains("issues/586", StringComparison.Ordinal))
        {
            return;
        }

        Assert.Fail("EW read Spark's manifests, so #586 has landed: replace this test with a field-by-field "
            + "comparison against expected/<table>.json (files, manifests) and drop the PendingIssue586 suffix.");
    }

    /// <summary>The corpus holds what it is meant to cover; a regenerated corpus that loses a case fails here.</summary>
    [Fact]
    public void CorpusCoversThePlan()
    {
        var transforms = new HashSet<string>(StringComparer.Ordinal);
        foreach (var t in Corpus.Tables.Where(t => t.StartsWith("t_", StringComparison.Ordinal)))
        {
            using var doc = JsonDocument.Parse(Corpus.MetadataText(t));
            foreach (var spec in doc.RootElement.GetProperty("partition-specs").EnumerateArray())
                foreach (var field in spec.GetProperty("fields").EnumerateArray())
                    transforms.Add(TransformKind(field.GetProperty("transform").GetString()!));
        }

        Assert.Superset(new HashSet<string> { "identity", "bucket", "truncate", "year", "month", "day", "hour", "void" }, transforms);

        // Several deletion vectors in one Puffin file, for different data files.
        var dvs = Files("dv_multi").Where(f => f.GetProperty("content").GetInt32() == 1).ToList();
        Assert.Contains(dvs.GroupBy(f => f.GetProperty("file_path").GetString()), g =>
            g.Count() >= 3 && g.Select(f => f.GetProperty("referenced_data_file").GetString()).Distinct().Count() == g.Count());

        // v2 position deletes, and equality deletes scoped by partition.
        Assert.Contains(Files("v2_posdel"), f => f.GetProperty("content").GetInt32() == 1
            && !f.TryGetProperty("referenced_data_file", out _) || f.GetProperty("file_format").GetString() == "PARQUET");
        Assert.Contains(Files("v2_eqdel"), f => f.GetProperty("content").GetInt32() == 2
            && f.GetProperty("equality_ids").GetArrayLength() == 1);

        // The upgraded table: v2 history, a v3 present, DVs after the upgrade.
        Assert.Equal(3, Corpus.Metadata("v2_to_v3").FormatVersion);
        Assert.Contains(Files("v2_to_v3"), f => f.TryGetProperty("referenced_data_file", out _));

        // Variant, shredded and not.
        Assert.All(new[] { "variant", "variant_shred" }, t =>
            Assert.Contains(Corpus.Metadata(t).Schemas.Last().Fields, f => f.Type is VariantType));
        Assert.Equal("true", Corpus.Metadata("variant_shred").Properties["write.parquet.shred-variants"]);

        // Branches and tags, and a compaction that carried lineage.
        Assert.Superset(new HashSet<string> { "main", "b1", "t1" }, new HashSet<string>(Corpus.Metadata("branches").Refs.Keys));
        Assert.Contains(Corpus.Expected("compacted").GetProperty("snapshots").EnumerateArray(),
            s => s.GetProperty("operation").GetString() == "replace");

        // No nanosecond timestamps: Spark cannot read them.
        foreach (var t in Corpus.Tables)
            Assert.DoesNotContain("timestamp_ns", Corpus.MetadataText(t), StringComparison.Ordinal);
    }

    /// <summary>The lineage Spark reads back is complete: every live row of a v3 table has a row id.</summary>
    [Theory]
    [MemberData(nameof(IcebergCorpus.SparkTables), MemberType = typeof(IcebergCorpus))]
    public void LineageIsRecordedForV3Tables(string table)
    {
        var expected = Corpus.Expected(table);
        if (expected.GetProperty("format_version").GetInt32() < 3)
            return;
        var lineage = expected.GetProperty("lineage");
        Assert.Equal(JsonValueKind.Array, lineage.ValueKind);
        Assert.Equal(expected.GetProperty("rows").GetArrayLength(), lineage.GetArrayLength());
        var rowIds = lineage.EnumerateArray().Select(r => r.GetProperty("_row_id").GetInt64()).ToList();
        Assert.Equal(rowIds.Count, rowIds.Distinct().Count());
    }

    // ── Helpers ──

    private static IEnumerable<JsonElement> Files(string table) =>
        Corpus.Expected(table).GetProperty("files").EnumerateArray();

    private static string TransformKind(string transform)
    {
        int bracket = transform.IndexOf('[');
        return bracket < 0 ? transform : transform.Substring(0, bracket);
    }

    private static void AssertSameBounds(JsonElement file, string key, IReadOnlyDictionary<int, byte[]> manifest)
    {
        var expected = new Dictionary<int, string>();
        if (file.TryGetProperty(key, out var map) && map.ValueKind == JsonValueKind.Object)
            foreach (var p in map.EnumerateObject())
                expected[int.Parse(p.Name, CultureInfo.InvariantCulture)] = p.Value.GetString()!;
        Assert.Equal(
            expected.OrderBy(e => e.Key),
            manifest.ToDictionary(e => e.Key, e => Convert.ToBase64String(e.Value)).OrderBy(e => e.Key));
    }

    /// <summary>A value as Spark's <c>to_json</c> wrote it, as the literal the codec decodes it to.</summary>
    internal static LiteralValue SparkLiteral(IcebergType type, JsonElement json)
    {
        if (json.ValueKind == JsonValueKind.Null)
            return LiteralValue.Null;
        switch (type)
        {
            case BooleanType:
                return LiteralValue.Of(json.GetBoolean());
            case IntegerType:
                return LiteralValue.Of(json.GetInt32());
            case LongType:
                return LiteralValue.Of(json.GetInt64());
            case FloatType:
                return LiteralValue.Of(float.Parse(json.GetString()!, CultureInfo.InvariantCulture));
            case DoubleType:
                return LiteralValue.Of(double.Parse(json.GetString()!, CultureInfo.InvariantCulture));
            case DecimalType d:
                Assert.True(SingleValueJson.TryParseDecimalText(json.GetString()!, out var unscaled, out int scale));
                return IcebergLiterals.Coerce(d, LiteralValue.HighPrecisionDecimalOf(unscaled, scale));
            case BinaryType or FixedType:
                return LiteralValue.Of(Convert.FromBase64String(json.GetString()!));
            case UuidType:
                return LiteralValue.Of(Guid.Parse(json.GetString()!));
            case StringType:
                return LiteralValue.Of(json.GetString()!);
            default:
                // Dates and timestamps: Spark's text is ISO 8601, which the JSON single-value reader takes.
                return SingleValueJson.Parse(type, json);
        }
    }
}
