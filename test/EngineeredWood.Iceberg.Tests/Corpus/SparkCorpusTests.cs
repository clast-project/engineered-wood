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
    /// The phase 0 exit criterion: EW reads every corpus manifest list and manifest, field by field, as Spark's
    /// <c>manifests</c> and <c>files</c> metadata tables report them. Partition values and partition-summary
    /// bounds are compared as Appendix D bytes, so both sides go through the same codec.
    /// </summary>
    [Theory]
    [MemberData(nameof(IcebergCorpus.SparkTables), MemberType = typeof(IcebergCorpus))]
    public async Task ManifestsMatchSpark(string table)
    {
        var md = Corpus.Metadata(table);
        var expected = Corpus.Expected(table);
        var current = md.Snapshots.Single(s => s.SnapshotId == md.CurrentSnapshotId);
        var fs = Corpus.FileSystem;

        var list = await ManifestIO.ReadSnapshotManifestListAsync(fs, md, current, default);
        var sparkManifests = expected.GetProperty("manifests").EnumerateArray()
            .ToDictionary(m => m.GetProperty("path").GetString()!);
        Assert.Equal(sparkManifests.Keys.OrderBy(k => k, StringComparer.Ordinal),
            list.Select(m => m.ManifestPath).OrderBy(k => k, StringComparer.Ordinal));

        var files = new List<(ManifestEntry Entry, int SpecId)>();
        foreach (var mle in list)
        {
            var sm = sparkManifests[mle.ManifestPath];
            string where = $"{table}: {mle.ManifestPath}";
            Assert.True(sm.GetProperty("length").GetInt64() == mle.ManifestLength, where + " length");
            Assert.True(sm.GetProperty("partition_spec_id").GetInt32() == mle.PartitionSpecId, where + " spec id");
            Assert.True(sm.GetProperty("content").GetInt32() == (int)mle.Content, where + " content");
            Assert.True(sm.GetProperty("added_snapshot_id").GetInt64() == mle.AddedSnapshotId, where + " added snapshot");
            // Spark splits the counts by content: a data manifest's are data-file counts, a delete manifest's delete-file counts.
            string kind = mle.Content == ManifestContent.Data ? "data" : "delete";
            Assert.True(sm.GetProperty($"added_{kind}_files_count").GetInt32() == mle.AddedDataFilesCount, where + " added count");
            Assert.True(sm.GetProperty($"existing_{kind}_files_count").GetInt32() == mle.ExistingDataFilesCount, where + " existing count");
            Assert.True(sm.GetProperty($"deleted_{kind}_files_count").GetInt32() == mle.DeletedDataFilesCount, where + " deleted count");

            var spec = md.PartitionSpecs.Single(s => s.SpecId == mle.PartitionSpecId);
            var partitionType = PartitionTypes.Resolve(spec, md);
            var summaries = sm.GetProperty("partition_summaries").EnumerateArray().ToList();
            Assert.True(summaries.Count == (mle.Partitions?.Count ?? 0), where + " partition summary count");
            for (int i = 0; i < summaries.Count; i++)
            {
                var ss = summaries[i];
                var es = mle.Partitions![i];
                var type = partitionType[i].Type;
                Assert.True(ss.GetProperty("contains_null").GetBoolean() == es.ContainsNull, $"{where} summary {i} contains_null");
                if (ss.TryGetProperty("contains_nan", out var nan) && nan.ValueKind != JsonValueKind.Null)
                    Assert.True(nan.GetBoolean() == es.ContainsNan, $"{where} summary {i} contains_nan");
                var transform = spec.Fields[i].Transform;
                AssertSummaryBound(ss, "lower_bound", type, transform, es.LowerBound, $"{where} summary {i}");
                AssertSummaryBound(ss, "upper_bound", type, transform, es.UpperBound, $"{where} summary {i}");
            }

            foreach (var entry in await ManifestIO.ReadSnapshotManifestAsync(fs, md, current, mle, default))
            {
                if (entry.Status != ManifestEntryStatus.Deleted)
                    files.Add((entry, mle.PartitionSpecId));
            }
        }

        // The files table lists the live files; a DV is one blob of its Puffin file, so it is keyed by offset too.
        static string Key(string path, long? offset) => offset is { } o ? $"{path}@{o}" : path;
        var sparkFiles = expected.GetProperty("files").EnumerateArray()
            .ToDictionary(f => Key(f.GetProperty("file_path").GetString()!, OptionalInt64(f, "content_offset")));
        Assert.Equal(sparkFiles.Keys.OrderBy(k => k, StringComparer.Ordinal),
            files.Select(f => Key(f.Entry.DataFile.FilePath, f.Entry.DataFile.ContentOffset)).OrderBy(k => k, StringComparer.Ordinal));

        foreach (var (entry, specId) in files)
        {
            var df = entry.DataFile;
            var sf = sparkFiles[Key(df.FilePath, df.ContentOffset)];
            string where = $"{table}: {df.FilePath}";
            Assert.True(sf.GetProperty("content").GetInt32() == (int)df.Content, where + " content");
            Assert.True(sf.GetProperty("file_format").GetString() == df.FileFormat.ToString().ToUpperInvariant(), where + " format");
            Assert.True(sf.GetProperty("spec_id").GetInt32() == specId, where + " spec id");
            Assert.True(sf.GetProperty("record_count").GetInt64() == df.RecordCount, where + " record count");
            Assert.True(sf.GetProperty("file_size_in_bytes").GetInt64() == df.FileSizeInBytes, where + " size");
            AssertCounts(sf, "column_sizes", df.ColumnSizes, where);
            AssertCounts(sf, "value_counts", df.ValueCounts, where);
            AssertCounts(sf, "null_value_counts", df.NullValueCounts, where);
            AssertCounts(sf, "nan_value_counts", df.NanValueCounts, where);
            AssertSameBounds(sf, "lower_bounds", df.LowerBounds ?? new Dictionary<int, byte[]>());
            AssertSameBounds(sf, "upper_bounds", df.UpperBounds ?? new Dictionary<int, byte[]>());
            Assert.True(OptionalInt64List(sf, "split_offsets").SequenceEqual(df.SplitOffsets ?? []), where + " split offsets");
            Assert.True(OptionalInt64List(sf, "equality_ids").Select(x => (int)x).SequenceEqual(df.EqualityIds ?? []), where + " equality ids");
            Assert.True(OptionalInt64(sf, "sort_order_id") == df.SortOrderId, where + " sort order");
            Assert.True(OptionalInt64(sf, "first_row_id") == df.FirstRowId, where + " first row id (inherited)");
            Assert.True(OptionalString(sf, "referenced_data_file") == df.ReferencedDataFile, where + " referenced data file");
            Assert.True(OptionalInt64(sf, "content_offset") == df.ContentOffset, where + " content offset");
            Assert.True(OptionalInt64(sf, "content_size_in_bytes") == df.ContentSizeInBytes, where + " content size");
            Assert.True(!sf.TryGetProperty("key_metadata", out var km) || km.ValueKind == JsonValueKind.Null, where + " key metadata");

            // The partition tuple: Spark's to_json leaves out null fields.
            var sparkPartition = sf.TryGetProperty("partition", out var sp) && sp.ValueKind == JsonValueKind.Object
                ? sp.EnumerateObject().ToDictionary(p => p.Name, p => p.Value)
                : [];
            foreach (var value in df.Partition?.Values ?? [])
            {
                if (value.Value is null)
                {
                    Assert.False(sparkPartition.ContainsKey(value.Name), $"{where} partition {value.Name} is null");
                    continue;
                }
                Assert.True(sparkPartition.TryGetValue(value.Name, out var json), $"{where} partition {value.Name}");
                Assert.True(
                    SingleValueBinary.Encode(value.Type, SparkLiteral(value.Type, json))
                        .AsSpan().SequenceEqual(SingleValueBinary.Encode(value.Type, PhysicalLiteral(value.Type, value.Value))),
                    $"{where} partition {value.Name}");
            }
            Assert.Equal(sparkPartition.Count, df.Partition?.Values.Count(v => v.Value is not null) ?? 0);
        }
    }

    // A partition-summary bound as Spark's manifests table shows it: the value's human string, or "null" when the
    // field has no non-null value.
    private static void AssertSummaryBound(
        JsonElement summary, string key, IcebergType type, Transform transform, byte[]? bound, string where)
    {
        string? text = summary.TryGetProperty(key, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;
        if (text is null or "null")
        {
            Assert.True(bound is null, $"{where} {key} is null");
            return;
        }
        Assert.True(bound is not null, $"{where} {key} present");

        // Java's human string of a temporal transform's int result is calendar text, not the number:
        // year "1982", month "1982-03", hour "1982-03-04-05".
        LiteralValue expected = transform switch
        {
            YearTransform => LiteralValue.Of(int.Parse(text, CultureInfo.InvariantCulture) - 1970),
            MonthTransform => LiteralValue.Of(MonthsFromEpoch(text)),
            HourTransform => LiteralValue.Of(HoursFromEpoch(text)),
            _ => SparkLiteral(type, type is IntegerType or LongType
                ? JsonDocument.Parse(text).RootElement
                : JsonDocument.Parse(JsonSerializer.Serialize(text)).RootElement),
        };
        Assert.True(SingleValueBinary.Encode(type, expected).AsSpan().SequenceEqual(bound), $"{where} {key} {text}");

        static int MonthsFromEpoch(string t)
        {
            var parts = t.Split('-');
            return (int.Parse(parts[0], CultureInfo.InvariantCulture) - 1970) * 12 + int.Parse(parts[1], CultureInfo.InvariantCulture) - 1;
        }

        static int HoursFromEpoch(string t)
        {
            var at = DateTime.ParseExact(t, "yyyy-MM-dd-HH", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
            return checked((int)((at - new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc)).Ticks / TimeSpan.TicksPerHour));
        }
    }

    /// <summary>A partition value in its physical form (see <see cref="PartitionData"/>) as a literal.</summary>
    private static LiteralValue PhysicalLiteral(IcebergType type, object value) => (type, value) switch
    {
        (BooleanType, bool b) => LiteralValue.Of(b),
        (IntegerType, int i) => LiteralValue.Of(i),
        (LongType, long l) => LiteralValue.Of(l),
        (StringType, string s) => LiteralValue.Of(s),
        (BinaryType or FixedType, byte[] b) => LiteralValue.Of(b),
        (DateType, int days) => IcebergLiterals.DateLiteral(days),
        (DecimalType d, System.Numerics.BigInteger unscaled) =>
            IcebergLiterals.Coerce(d, LiteralValue.HighPrecisionDecimalOf(unscaled, d.Scale)),
        _ => throw new NotSupportedException($"The corpus comparison has no case for a {type} partition value."),
    };

    private static void AssertCounts(JsonElement file, string key, IReadOnlyDictionary<int, long>? actual, string where)
    {
        var expected = file.TryGetProperty(key, out var map) && map.ValueKind == JsonValueKind.Object
            ? map.EnumerateObject().ToDictionary(p => int.Parse(p.Name, CultureInfo.InvariantCulture), p => p.Value.GetInt64())
            : [];
        // Spark shows an absent map and an empty one alike.
        Assert.True(expected.OrderBy(e => e.Key).SequenceEqual((actual ?? new Dictionary<int, long>()).OrderBy(e => e.Key)),
            $"{where} {key}");
    }

    private static long? OptionalInt64(JsonElement e, string key) =>
        e.TryGetProperty(key, out var v) && v.ValueKind == JsonValueKind.Number ? v.GetInt64() : null;

    private static string? OptionalString(JsonElement e, string key) =>
        e.TryGetProperty(key, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

    private static IEnumerable<long> OptionalInt64List(JsonElement e, string key) =>
        e.TryGetProperty(key, out var v) && v.ValueKind == JsonValueKind.Array ? v.EnumerateArray().Select(x => x.GetInt64()) : [];

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
